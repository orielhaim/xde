//! Dedicated control-plane thread.
//!
//! The loop owns the pure `Controller` (and through it the `WorldModel`), a
//! `TimerQueue`, the resolver, the shard mailboxes and the per-job reply
//! channels. It feeds observations into the controller and executes the
//! returned actions. All scheduling intelligence lives in `crate::core::controller`;
//! this file is routing and I/O glue only.

pub(crate) mod checkpoint;

use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::Arc,
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crate::core::{
    Error, Result,
    context::JobContext,
    controller::{Action, Controller, Observation, ResumeEvidence},
    ids::{ConnectionId, JobId},
    timers::TimerQueue,
    world::ConnectionStatus,
};
use crate::net::ConnectTarget;
use crate::storage::{FileDestinationCoordinator, Journal, JournalPayload};
use flume::{Receiver, Sender, TrySendError};

use crate::shard::ShardCommand;

/// The outcome delivered to a waiting `Job`.
#[derive(Debug, Clone)]
pub struct JobOutcome {
    pub bytes: u64,
    /// Read-back digest of the committed artifact (BLAKE3/SHA-256), present
    /// when the job requested integrity computation or verification.
    pub digest: Option<crate::core::spec::Digest>,
    /// Bytes that came from a previous run's journal instead of the network.
    pub resumed_bytes: u64,
}

pub struct ControlCommand {
    pub spec: Box<crate::core::JobSpec>,
    pub destination_key: crate::core::world::DestinationKey,
    pub final_path: Option<PathBuf>,
    /// Application-provided shared destination (custom sink). When set,
    /// `final_path`/coordinator/journal/resume are all bypassed.
    pub shared_destination:
        Option<std::sync::Arc<dyn crate::storage::DynDestination + Send + Sync>>,
    /// Application-provided credential refresher for this job, if any.
    /// Not `Debug`: refreshers may hold secrets; they are never logged.
    pub refresher: Option<std::sync::Arc<dyn crate::core::credentials::SourceRefresher>>,
    pub progress: Option<crate::progress::ProgressPublisher>,
    /// Admission reply: JobId or the lease-conflict error.
    pub admit_tx: Sender<Result<JobId>>,
    /// Final outcome channel.
    pub result_tx: Sender<Result<JobOutcome>>,
}

enum ControlRequest {
    Submit(Box<ControlCommand>),
    Shutdown,
    Pause {
        job: JobId,
        reply: Sender<Result<()>>,
    },
    Resume {
        job: JobId,
        reply: Sender<Result<()>>,
    },
}

/// Per-job execution state owned by the control thread.
struct JobState {
    result_tx: Sender<Result<JobOutcome>>,
    admission: Option<(Box<crate::core::JobSpec>, Option<Box<ResumeEvidence>>)>,
    context: JobContext,
    destination: DestinationState,
    part_path: PathBuf,
    checkpoints: checkpoint::Checkpoints,
    /// Application-provided credential refresher, if any.
    refresher: Option<std::sync::Arc<dyn crate::core::credentials::SourceRefresher>>,
    progress: Option<crate::progress::ProgressPublisher>,
    provenance: std::sync::Arc<std::sync::Mutex<crate::core::ArtifactProvenance>>,
}

enum DestinationState {
    File(Box<FileState>),
    Custom(Arc<dyn crate::storage::DynDestination + Send + Sync>),
    Committing,
}

struct FileState {
    coordinator: FileDestinationCoordinator,
    journal: Journal,
}

impl DestinationState {
    fn journal(&self) -> Option<&Journal> {
        match self {
            Self::File(file) => Some(&file.journal),
            _ => None,
        }
    }
    fn journal_mut(&mut self) -> Option<&mut Journal> {
        match self {
            Self::File(file) => Some(&mut file.journal),
            _ => None,
        }
    }
}

/// Handle held by the public Engine.
#[derive(Clone)]
pub struct ControlHandle {
    tx: Sender<ControlRequest>,
    cancel_tx: Sender<(JobId, Sender<()>)>,
    status_tx: flume::Sender<StatusRequest>,
}

/// One snapshot query; carries its own reply channel.
enum StatusRequest {
    Job {
        job: JobId,
        reply: flume::Sender<Option<crate::snapshot::JobSnapshot>>,
    },
    Engine {
        reply: flume::Sender<crate::snapshot::EngineSnapshot>,
    },
}

impl std::fmt::Debug for ControlHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlHandle").finish()
    }
}

impl ControlHandle {
    pub fn submit(&self, cmd: ControlCommand) -> Result<()> {
        self.tx
            .try_send(ControlRequest::Submit(Box::new(cmd)))
            .map_err(|e| match e {
                TrySendError::Full(_) => Error::Runtime(crate::core::RuntimeError::EngineBusy),
                TrySendError::Disconnected(_) => {
                    Error::Runtime(crate::core::RuntimeError::EngineGone)
                }
            })
    }

    pub fn shutdown(&self) -> Result<()> {
        self.tx
            .send(ControlRequest::Shutdown)
            .map_err(|_| Error::Runtime(crate::core::RuntimeError::EngineGone))
    }

    pub(crate) fn pause(&self, job: JobId, resume: bool) -> Result<()> {
        let (reply, done) = flume::bounded(1);
        let command = if resume {
            ControlRequest::Resume { job, reply }
        } else {
            ControlRequest::Pause { job, reply }
        };
        self.tx
            .try_send(command)
            .map_err(|_| Error::Runtime(crate::core::RuntimeError::EngineBusy))?;
        done.recv()
            .map_err(|_| Error::Runtime(crate::core::RuntimeError::EngineGone))?
    }

    pub(crate) fn cancel(&self, job: JobId) -> Result<()> {
        let (ack, done) = flume::bounded(1);
        self.cancel_tx
            .try_send((job, ack))
            .map_err(|error| match error {
                TrySendError::Full(_) => Error::Runtime(crate::core::RuntimeError::EngineBusy),
                TrySendError::Disconnected(_) => {
                    Error::Runtime(crate::core::RuntimeError::EngineGone)
                }
            })?;
        done.recv()
            .map_err(|_| Error::Runtime(crate::core::RuntimeError::EngineGone))
    }

    pub(crate) fn job_snapshot(&self, job: JobId) -> Option<crate::snapshot::JobSnapshot> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.status_tx
            .try_send(StatusRequest::Job {
                job,
                reply: reply_tx,
            })
            .ok()?;
        reply_rx.recv().ok().flatten()
    }

    pub(crate) fn engine_snapshot(&self) -> Option<crate::snapshot::EngineSnapshot> {
        let (reply_tx, reply_rx) = flume::bounded(1);
        self.status_tx
            .try_send(StatusRequest::Engine { reply: reply_tx })
            .ok()?;
        reply_rx.recv().ok()
    }
}
struct DeferredDispatch {
    shard: usize,
    command: DeferredCommand,
}

#[derive(Clone)]
enum DeferredCommand {
    Release {
        job: JobId,
        part_path: PathBuf,
    },
    Close(ConnectionId),
    Checkpoint {
        job: JobId,
        id: u64,
        work: Arc<checkpoint::Work>,
        completed: Sender<checkpoint::Completed>,
    },
}
impl DeferredCommand {
    fn command(&self) -> ShardCommand {
        match self {
            Self::Release { job, part_path } => ShardCommand::ReleaseJob {
                job: *job,
                part_path: part_path.clone(),
            },
            Self::Close(connection) => ShardCommand::CloseConnection {
                connection: *connection,
            },
            Self::Checkpoint {
                job,
                id,
                work,
                completed,
            } => ShardCommand::Checkpoint {
                job: *job,
                id: *id,
                work: work.clone(),
                completed: completed.clone(),
            },
        }
    }
}

struct Loop {
    deferred_dispatch: VecDeque<DeferredDispatch>,
    controller: Controller,
    pending: VecDeque<Observation>,
    timers: TimerQueue,
    jobs: HashMap<JobId, JobState>,
    obs_tx: Sender<Observation>,
    checkpoint_tx: Sender<checkpoint::Completed>,
    shard_txs: Vec<Sender<ShardCommand>>,
    status_rx: flume::Receiver<StatusRequest>,
    memory: crate::storage::MemoryBudget,
    resolver: crate::resolve::ResolverHandle,
    /// Public telemetry sink. Absent when nobody subscribed.
    events: Option<crate::core::events::EventSink>,
    /// The network context detected at engine construction.
    current_network_context: Option<crate::core::ids::NetworkContextId>,
}

/// Arguments to the control loop, grouped so the thread entry point has one
/// coherent parameter.
struct LoopArgs {
    shards: usize,
    limits: crate::core::policy::EngineLimits,
    cmd_rx: Receiver<ControlRequest>,
    cancel_rx: Receiver<(JobId, Sender<()>)>,
    obs_rx: Receiver<Observation>,
    obs_tx: Sender<Observation>,
    shard_txs: Vec<Sender<ShardCommand>>,
    resolver: crate::resolve::ResolverHandle,
    events: Option<crate::core::events::EventSink>,
    network_context: Option<crate::core::world::NetworkContextKey>,
    status_rx: flume::Receiver<StatusRequest>,
    memory: crate::storage::MemoryBudget,
    /// Persistent learning (opt-in): when set, transport evidence is loaded
    /// from this file at startup and written back on shutdown.
    profile_path: Option<PathBuf>,
}

pub struct ControlPlane;

impl ControlPlane {
    /// Spawn the control thread with persistent-learning storage. Returns
    /// the submit handle and join handle.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_with_profile(
        shards: usize,
        limits: crate::core::policy::EngineLimits,
        obs_rx: Receiver<Observation>,
        obs_tx: Sender<Observation>,
        shard_txs: Vec<Sender<ShardCommand>>,
        resolver: crate::resolve::ResolverHandle,
        events: Option<crate::core::events::EventSink>,
        network_context: Option<crate::core::world::NetworkContextKey>,
        memory: crate::storage::MemoryBudget,
        profile_path: Option<PathBuf>,
    ) -> (ControlHandle, JoinHandle<()>) {
        let (cmd_tx, cmd_rx) = flume::bounded(64);
        let (cancel_tx, cancel_rx) = flume::bounded(256);
        let (status_tx, status_rx) = flume::bounded::<StatusRequest>(64);
        let handle = ControlHandle {
            tx: cmd_tx,
            cancel_tx,
            status_tx,
        };
        let args = LoopArgs {
            shards,
            limits,
            cmd_rx,
            cancel_rx,
            obs_rx,
            obs_tx,
            shard_txs,
            resolver,
            events,
            network_context,
            status_rx,
            memory,
            profile_path,
        };
        let join = thread::Builder::new()
            .name("xde-control".into())
            .spawn(move || run_loop(args))
            .expect("control thread spawn");
        (handle, join)
    }
}

fn run_loop(args: LoopArgs) {
    let LoopArgs {
        shards,
        limits,
        cmd_rx,
        cancel_rx,
        obs_rx,
        obs_tx,
        shard_txs,
        resolver,
        events,
        network_context,
        status_rx,
        memory,
        profile_path,
    } = args;
    let (checkpoint_tx, checkpoint_rx) = flume::bounded(64);
    let mut lp = Loop {
        controller: Controller::with_shards(shards),
        pending: VecDeque::new(),
        deferred_dispatch: VecDeque::new(),
        timers: TimerQueue::new(),
        jobs: HashMap::new(),
        obs_tx,
        checkpoint_tx,
        shard_txs,
        resolver,
        events,
        current_network_context: None,
        status_rx,
        memory,
    };
    lp.controller.set_engine_limits(&limits);
    if let Some(ctx) = network_context {
        let id = lp.controller.world.set_network_context(ctx);
        lp.current_network_context = Some(id);
    }
    // Persistent learning (opt-in): seed the model with evidence from
    // earlier runs. With no profile path configured this is a strict no-op
    // and the engine performs zero persistence I/O.
    if let Some(profile) = load_profile(&profile_path) {
        lp.controller.world.import_profiles(&profile);
    }

    enum Input {
        Command(std::result::Result<ControlRequest, flume::RecvError>),
        Cancel(std::result::Result<(JobId, Sender<()>), flume::RecvError>),
        Status(std::result::Result<StatusRequest, flume::RecvError>),
        Observation(std::result::Result<Observation, flume::RecvError>),
        Dispatch(usize, std::result::Result<(), flume::SendError<()>>),
        Checkpoint(std::result::Result<checkpoint::Completed, flume::RecvError>),
    }
    let mut shutdown = false;
    loop {
        if shutdown && lp.jobs.is_empty() && lp.deferred_dispatch.is_empty() {
            break;
        }
        let input = if let Some(observation) = lp.pending.pop_front() {
            Some(Input::Observation(Ok(observation)))
        } else {
            let mut selector = flume::Selector::new()
                .recv(&cmd_rx, Input::Command)
                .recv(&cancel_rx, Input::Cancel)
                .recv(&lp.status_rx, Input::Status)
                .recv(&obs_rx, Input::Observation)
                .recv(&checkpoint_rx, Input::Checkpoint);
            let mut selected = vec![false; lp.shard_txs.len()];
            for (index, cleanup) in lp.deferred_dispatch.iter().enumerate() {
                if selected[cleanup.shard] {
                    continue;
                }
                selected[cleanup.shard] = true;
                selector = selector.send(
                    &lp.shard_txs[cleanup.shard],
                    cleanup.command.command(),
                    move |result| Input::Dispatch(index, result.map_err(|_| flume::SendError(()))),
                );
            }
            if let Some(deadline) = lp.timers.next_deadline() {
                selector.wait_deadline(deadline).ok()
            } else {
                Some(selector.wait())
            }
        };
        match input {
            Some(Input::Checkpoint(Ok(completion))) => checkpoint::complete(&mut lp, completion),
            Some(Input::Checkpoint(Err(_))) => unreachable!("control owns checkpoint ingress"),
            Some(Input::Dispatch(index, result)) => {
                let pending = lp
                    .deferred_dispatch
                    .remove(index)
                    .expect("selected dispatch exists");
                if result.is_err()
                    && let DeferredCommand::Checkpoint { job, id, .. } = pending.command
                {
                    checkpoint::complete(
                        &mut lp,
                        checkpoint::Completed {
                            job,
                            id,
                            result: Err(Error::journal("checkpoint shard disconnected")),
                        },
                    );
                }
            }

            Some(Input::Command(Ok(ControlRequest::Pause { job, reply }))) => {
                let allowed = lp.controller.world.jobs.get(job).is_some_and(|j| {
                    matches!(
                        j.phase,
                        crate::core::world::JobPhase::Created
                            | crate::core::world::JobPhase::Transferring
                            | crate::core::world::JobPhase::Paused
                    )
                });
                if allowed {
                    let actions = lp
                        .controller
                        .handle(Observation::JobPaused { job }, Instant::now());
                    route_actions(&mut lp, &actions);
                    let _ = reply.send(Ok(()));
                } else {
                    let _ = reply.send(Err(Error::Config(
                        "job cannot pause during preparation, probing, commit or teardown".into(),
                    )));
                }
            }
            Some(Input::Command(Ok(ControlRequest::Resume { job, reply }))) => {
                let allowed = !shutdown
                    && lp
                        .controller
                        .world
                        .jobs
                        .get(job)
                        .is_some_and(|j| j.phase == crate::core::world::JobPhase::Paused);
                if allowed {
                    let actions = lp
                        .controller
                        .handle(Observation::JobResumed { job }, Instant::now());
                    route_actions(&mut lp, &actions);
                    let _ = reply.send(Ok(()));
                } else {
                    let _ = reply.send(Err(Error::Config("job is not paused".into())));
                }
            }
            Some(Input::Command(Ok(ControlRequest::Submit(cmd)))) if !shutdown => {
                admit_command(&mut lp, *cmd);
            }
            Some(Input::Command(Ok(ControlRequest::Submit(cmd)))) => {
                let _ = cmd.admit_tx.send(Err(Error::EngineGone));
                let _ = cmd.result_tx.send(Err(Error::EngineGone));
            }
            Some(Input::Command(Ok(ControlRequest::Shutdown) | Err(_))) => {
                if !shutdown {
                    shutdown = true;
                    for job in lp.jobs.keys().copied().collect::<Vec<_>>() {
                        let actions = lp
                            .controller
                            .handle(Observation::JobCancelled { job }, Instant::now());
                        route_actions(&mut lp, &actions);
                    }
                }
            }
            Some(Input::Cancel(Ok((job, ack)))) => {
                let actions = lp
                    .controller
                    .handle(Observation::JobCancelled { job }, Instant::now());
                route_actions(&mut lp, &actions);
                let _ = ack.send(());
            }
            Some(Input::Status(Ok(request))) => match request {
                StatusRequest::Job { job, reply } => {
                    let _ = reply.send(build_snapshot(&lp, job));
                }
                StatusRequest::Engine { reply } => {
                    let _ = reply.send(build_engine_snapshot(&lp));
                }
            },
            Some(Input::Observation(Ok(obs))) => {
                let prepared_job = match &obs {
                    Observation::DestinationPrepared { job, .. } => Some(*job),
                    _ => None,
                };
                let progress_job = match &obs {
                    Observation::Probed { job, .. }
                    | Observation::AssignmentVerified { job, .. } => Some(*job),
                    _ => None,
                };
                emit_observation_events(&lp, &obs);
                record_progress(&mut lp, &obs);
                let actions = lp.controller.handle(obs, Instant::now());
                if let Some(job) = progress_job {
                    publish_progress(&lp, job);
                }
                route_actions(&mut lp, &actions);
                if let Some(job) = prepared_job
                    && lp
                        .controller
                        .world
                        .jobs
                        .get(job)
                        .is_some_and(|j| j.phase == crate::core::world::JobPhase::Created)
                    && let Some((spec, resume)) = lp
                        .jobs
                        .get_mut(&job)
                        .and_then(|state| state.admission.take())
                {
                    let actions = lp.controller.handle(
                        Observation::JobAdmitted { job, spec, resume },
                        Instant::now(),
                    );
                    route_actions(&mut lp, &actions);
                }
            }
            Some(Input::Observation(Err(_))) => break,
            Some(Input::Cancel(Err(_)) | Input::Status(Err(_))) | None => {}
        }
        for event in lp.timers.drain_expired(Instant::now()) {
            if let crate::core::timers::TimerEvent::CheckpointDue(job) = &event {
                checkpoint::due(&mut lp, *job, Instant::now());
            }
            let actions = lp
                .controller
                .handle(Observation::TimerExpired { event }, Instant::now());
            route_actions(&mut lp, &actions);
        }
    }

    save_profile(&profile_path, lp.controller.world.export_profiles());
}

// ---------------------------------------------------------------------------
// Persistent learning I/O (opt-in). Every helper treats None as a strict
// no-op: a default-configured engine never touches the filesystem for
// profiles. Profiles are optimization hints - corrupt or incompatible files
// are discarded, never fatal.
// ---------------------------------------------------------------------------

fn load_profile(path: &Option<PathBuf>) -> Option<crate::core::profile::PersistedProfile> {
    let path = path.as_ref()?;
    let bytes = std::fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn save_profile(path: &Option<PathBuf>, profile: crate::core::profile::PersistedProfile) {
    let Some(path) = path.as_ref() else { return };
    let Ok(bytes) = serde_json::to_vec(&profile) else {
        return;
    };
    // Atomic rename so a crash never leaves a torn profile.
    let tmp = path.with_extension("json.tmp");
    if std::fs::write(&tmp, bytes).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

fn admit_command(lp: &mut Loop, cmd: ControlCommand) {
    if cmd.final_path.is_none() && cmd.shared_destination.is_none() {
        let _ = cmd
            .admit_tx
            .send(Err(Error::Config("job has no destination".into())));
        return;
    }

    // Open the file coordinator before admission so a bad path fails fast.
    let coordinator = cmd.final_path.as_ref().map(|path| {
        FileDestinationCoordinator::open_blocking(crate::storage::FileDestinationOptions::new(
            path.clone(),
        ))
    });
    let coordinator = match coordinator {
        Some(Ok(c)) => Some(c),
        Some(Err(e)) => {
            let msg = e.to_string();
            let _ = cmd.admit_tx.send(Err(e));
            let _ = cmd.result_tx.send(Err(Error::destination(msg)));
            return;
        }
        None => None,
    };

    let spec = *cmd.spec;
    if lp.jobs.len() >= lp.controller.max_jobs()
        || lp.deferred_dispatch.len() >= lp.controller.max_jobs().saturating_mul(lp.shard_txs.len())
    {
        let msg = "engine job limit reached".to_string();
        let _ = cmd.admit_tx.send(Err(Error::destination(msg.clone())));
        let _ = cmd.result_tx.send(Err(Error::destination(msg)));
        return;
    }
    let job = match lp
        .controller
        .world
        .admit_job(&spec, cmd.destination_key.clone())
    {
        Ok(job) => job,
        Err(e) => {
            let msg = match &e {
                crate::core::world::DestinationLeaseError::Busy(k) => {
                    format!("destination '{k}' is in use by another active job")
                }
                crate::core::world::DestinationLeaseError::NoSource => {
                    "job has no source".to_string()
                }
            };
            let _ = cmd.admit_tx.send(Err(Error::destination(msg.clone())));
            let _ = cmd.result_tx.send(Err(Error::destination(msg)));
            return;
        }
    };
    let part_path = coordinator
        .as_ref()
        .map(|c| c.part_path().to_path_buf())
        .unwrap_or_default();

    // Resume discovery: locate `.part` + journal, validate format and
    // geometry, and compute the ranges that can safely seed the plan.
    let (destination, resume) = match coordinator {
        Some(coordinator) => {
            let journal = discover_resume(&coordinator, &spec);
            let resume = if !journal.payload().durable.is_empty() {
                Some(Box::new(ResumeEvidence::Ranges {
                    durable: journal.payload().durable.clone(),
                    total: journal.payload().total,
                    etag: journal.payload().fingerprint.etag.clone(),
                    last_modified: journal.payload().fingerprint.last_modified_unix.and_then(
                        |unix| std::time::UNIX_EPOCH.checked_add(Duration::from_secs(unix)),
                    ),
                    urls: vec![journal.payload().fingerprint.final_url.clone()],
                }))
            } else if !matches!(journal.path().try_exists(), Ok(false)) {
                Some(Box::new(ResumeEvidence::ResetRequired))
            } else {
                None
            };
            (
                DestinationState::File(Box::new(FileState {
                    coordinator,
                    journal,
                })),
                resume,
            )
        }
        None => (
            DestinationState::Custom(
                cmd.shared_destination
                    .clone()
                    .expect("validated destination"),
            ),
            None,
        ),
    };

    lp.jobs.insert(
        job,
        JobState {
            result_tx: cmd.result_tx.clone(),
            admission: None,
            context: JobContext::with_durability(spec.deadline, spec.persistence),
            destination,
            part_path,
            checkpoints: Default::default(),
            refresher: cmd.refresher,
            progress: cmd.progress,
            provenance: std::sync::Arc::new(std::sync::Mutex::new(
                crate::core::ArtifactProvenance::new(),
            )),
        },
    );
    // Shared custom destination: register its capabilities so the
    // controller gates segmentation/parallelism honestly, and hand the
    // destination to the shard that will write it.
    if let Some(dest) = &cmd.shared_destination
        && let Err(error) = lp
            .controller
            .world
            .set_sink_properties(job, dest.caps(), dest.hints())
    {
        let actions = lp.controller.handle(
            Observation::DestinationFailed {
                job,
                failure: crate::core::controller::DestinationFailure::from_error(&error),
            },
            Instant::now(),
        );
        route_actions(lp, &actions);
        let _ = cmd.admit_tx.send(Err(error));
        return;
    }
    if let Some(deadline) = spec.deadline {
        lp.timers
            .schedule(deadline, crate::core::timers::TimerEvent::JobDeadline(job));
    }
    let resumed = resume.as_deref().map_or(0, |evidence| match evidence {
        ResumeEvidence::Ranges { durable, .. } => durable.covered_len(),
        ResumeEvidence::ResetRequired => 0,
    });
    let _ = cmd.admit_tx.send(Ok(job));
    emit(
        lp,
        crate::core::events::Event::Started {
            job,
            total: None,
            resumed_bytes: resumed,
        },
    );

    if let Some(dest) = cmd.shared_destination {
        lp.jobs.get_mut(&job).expect("admitted job").admission = Some((Box::new(spec), resume));
        let actions = lp
            .controller
            .handle(Observation::DestinationPreparing { job }, Instant::now());
        route_actions(lp, &actions);
        send_shard(lp, 0, ShardCommand::PrepareDestination { job, dest });
        return;
    }

    let actions = lp.controller.handle(
        Observation::JobAdmitted {
            job,
            spec: Box::new(spec),
            resume,
        },
        Instant::now(),
    );
    route_actions(lp, &actions);
}

/// Load and validate a previous run's journal for this destination.
///
/// A corrupt or incompatible journal starts empty. Durable ranges that do not
/// fit inside an existing `.part` are dropped conservatively.
fn discover_resume(
    coordinator: &FileDestinationCoordinator,
    spec: &crate::core::JobSpec,
) -> Journal {
    let journal_path = coordinator.journal_path();
    let part_path = coordinator.part_path();
    let fresh = |spec: &crate::core::JobSpec, path: std::path::PathBuf| -> Journal {
        // No previous run (or an unreadable one): start a journal seeded
        // with this run's source identity.
        let src = spec.sources.first().expect("admission requires a source");
        let fingerprint = crate::core::representation::JournaledFingerprint {
            content_length: None,
            etag: None,
            last_modified_unix: None,
            final_url: src.url.to_string(),
            redirect_chain: Vec::new(),
            header_identity: src.representation_fingerprint(),
            content_coding: None,
        };
        let mut payload = JournalPayload::new(fingerprint, None, spec.integrity.overlap_bytes);
        payload.source_urls.push(src.url.to_string());
        Journal::create(path, payload)
    };

    let mut journal = match Journal::load_blocking(&journal_path) {
        Ok(Some(j)) => j,
        Ok(None) => return fresh(spec, journal_path.clone()),
        // Corrupt/torn/incompatible version: resume from nothing.
        Err(_) => return fresh(spec, journal_path),
    };

    // The partial artifact must exist and cover every durable range.
    if !part_path.exists() {
        return fresh(spec, journal_path.clone());
    }
    let part_len = std::fs::metadata(part_path).map(|m| m.len()).unwrap_or(0);
    if part_len == 0 {
        return fresh(spec, journal_path);
    }
    let payload = journal.payload_mut();
    if payload.durable.iter().any(|range| range.end > part_len)
        || payload.fingerprint.header_identity != spec.sources[0].representation_fingerprint()
    {
        payload.durable.clear();
    }
    // durable ⊆ completed by construction; after trimming, completed keeps
    // exactly the ranges that remain both written and durable.
    payload.completed = payload.durable.clone();
    payload.bytes_written = payload.completed.covered_len();

    // Record source identity so the controller can judge representation
    // agreement when the probe answers.
    if let Some(src) = spec.sources.first() {
        payload.fingerprint.header_identity = src.representation_fingerprint();
        if !payload.source_urls.contains(&src.url.to_string()) {
            payload.source_urls.push(src.url.to_string());
        }
    }
    journal
}

fn route_actions(lp: &mut Loop, actions: &[Action]) {
    for action in actions {
        if checkpoint::defer(lp, action) {
            continue;
        }
        tracing::trace!(target: "xde::engine", ?action, "control action");
        match action {
            Action::Resolve { origin, host, port } => {
                if !lp
                    .resolver
                    .resolve(*origin, host.clone(), *port, lp.obs_tx.clone())
                {
                    lp.pending.push_back(Observation::DispatchFailed {
                        operation: crate::core::controller::DispatchOperation::Resolve,
                        job: None,
                        assignment: None,
                        connection: None,
                        origin: Some(*origin),
                    });
                }
            }
            Action::OpenConnection {
                connection,
                origin,
                endpoint,
                shard,
                prefer_h3,
                alt_port,
            } => {
                let Some(mut target) = build_target(lp, *origin, *endpoint) else {
                    continue;
                };
                if let Some(job) = lp
                    .controller
                    .world
                    .connections
                    .get(*connection)
                    .and_then(|c| c.owner)
                    .and_then(|job| lp.controller.world.jobs.get(job))
                {
                    target.http_version = job.policy.http_version;
                }
                if let Some(port) = alt_port {
                    target.addr.set_port(*port);
                }
                send_shard(
                    lp,
                    *shard,
                    ShardCommand::OpenConnection {
                        connection: *connection,
                        target,
                        prefer_h3: *prefer_h3,
                    },
                );
            }
            Action::Probe {
                job,
                connection,
                source: probe_source,
            } => {
                // Resolve the probe target: an explicit mirror id, or the
                // job's currently active source.
                let resolved = probe_source.or_else(|| {
                    lp.controller
                        .world
                        .jobs
                        .get(*job)
                        .and_then(|j| j.active_source())
                });
                let Some(sid) = resolved else { continue };
                let Some(context) = build_source_context(lp, *job, sid) else {
                    continue;
                };
                let Some(ctx) = lp.jobs.get(job).map(|s| s.context.clone()) else {
                    continue;
                };
                // Route to the shard that owns the connection.
                send_shard(
                    lp,
                    shard_of_connection(lp, *connection),
                    ShardCommand::Probe {
                        job: *job,
                        connection: *connection,
                        source_id: sid,
                        source: context,
                        context: ctx,
                    },
                );
            }
            Action::StartAssignment {
                job,
                assignment,
                connection,
                range,
                overlap,
                frontier,
            } => {
                let Some(state) = lp.jobs.get(job) else {
                    continue;
                };
                let _ = assignment;
                emit(
                    lp,
                    crate::core::events::Event::WorkerAdded {
                        job: *job,
                        range: *range,
                        connection: *connection,
                    },
                );
                let source = match source_for_connection(lp, *job, *connection) {
                    Some(s) => s,
                    None => continue,
                };
                let attempt = lp
                    .controller
                    .world
                    .jobs
                    .get(*job)
                    .and_then(|j| j.plan.as_ref())
                    .and_then(|p| p.assignment(assignment.assignment))
                    .map(|a| a.attempt)
                    .unwrap_or(0);
                let lock = lp
                    .controller
                    .world
                    .jobs
                    .get(*job)
                    .map(|j| j.rep_lock.clone())
                    .unwrap_or_default();
                send_shard(
                    lp,
                    shard_of_connection(lp, *connection),
                    ShardCommand::StartRange {
                        control: lp.controller.world.jobs[*job]
                            .plan
                            .as_ref()
                            .unwrap()
                            .assignment(assignment.assignment)
                            .unwrap()
                            .control
                            .clone(),
                        job: *job,
                        assignment: *assignment,
                        attempt,
                        range: *range,
                        overlap: *overlap,
                        is_resume: lp
                            .controller
                            .world
                            .jobs
                            .get(*job)
                            .is_some_and(|j| j.integrity.verify_on_resume && j.resumed_bytes > 0),
                        frontier: *frontier,
                        connection: *connection,
                        source_id: source,
                        source: build_source_context(lp, *job, source).unwrap_or_else(|| {
                            Arc::new(crate::http::SourceContext {
                                url: url::Url::parse("about:invalid").expect("static"),
                                if_range: None,
                                allow_compressed: false,
                                extra_headers: Default::default(),
                                deadline: None,
                            })
                        }),
                        destination: match &state.destination {
                            DestinationState::Custom(dest) => Some(dest.clone()),
                            _ => None,
                        },
                        part_path: state.part_path.clone(),
                        context: state.context.clone(),
                        rep_lock: lock,
                        provenance: state.provenance.clone(),
                    },
                );
            }
            Action::StartFullBody {
                job,
                assignment,
                connection,
                frontier,
            } => {
                let Some(state) = lp.jobs.get(job) else {
                    continue;
                };
                let source = match source_for_connection(lp, *job, *connection) {
                    Some(s) => s,
                    None => continue,
                };
                let rep_lock = lp
                    .controller
                    .world
                    .jobs
                    .get(*job)
                    .map(|j| j.rep_lock.clone())
                    .unwrap_or_default();
                send_shard(
                    lp,
                    shard_of_connection(lp, *connection),
                    ShardCommand::StartFullBody {
                        control: lp.controller.world.jobs[*job]
                            .plan
                            .as_ref()
                            .unwrap()
                            .assignment(assignment.assignment)
                            .unwrap()
                            .control
                            .clone(),
                        job: *job,
                        attempt: 0,
                        assignment: *assignment,
                        connection: *connection,
                        frontier: *frontier,
                        source: build_source_context(lp, *job, source).unwrap_or_else(|| {
                            Arc::new(crate::http::SourceContext {
                                url: url::Url::parse("about:invalid").expect("static"),
                                if_range: None,
                                allow_compressed: false,
                                extra_headers: Default::default(),
                                deadline: None,
                            })
                        }),
                        destination: match &state.destination {
                            DestinationState::Custom(dest) => Some(dest.clone()),
                            _ => None,
                        },
                        part_path: state.part_path.clone(),
                        context: state.context.clone(),
                        source_id: source,
                        rep_lock,
                        provenance: state.provenance.clone(),
                    },
                );
            }
            Action::SampleMirror {
                job,
                source,
                connection,
                range,
            } => {
                let Some(state) = lp.jobs.get(job) else {
                    continue;
                };
                let Some(context) = build_source_context(lp, *job, *source) else {
                    continue;
                };
                let rep_lock = lp
                    .controller
                    .world
                    .jobs
                    .get(*job)
                    .map(|j| j.rep_lock.clone())
                    .unwrap_or_default();
                send_shard(
                    lp,
                    shard_of_connection(lp, *connection),
                    ShardCommand::SampleMirror {
                        job: *job,
                        source_id: *source,
                        connection: *connection,
                        range: *range,
                        source: context,
                        part_path: state.part_path.clone(),
                        context: state.context.clone(),
                        rep_lock,
                    },
                );
            }
            Action::ReportSourceQuarantined {
                job,
                sources,
                reason,
            } => {
                emit(
                    lp,
                    crate::core::events::Event::SourceQuarantined {
                        job: *job,
                        sources: sources.clone(),
                        reason: reason.clone(),
                    },
                );
            }
            Action::TruncateAssignment {
                job,
                assignment,
                connection,
                new_end,
            } => {
                send_shard(
                    lp,
                    shard_of_connection(lp, *connection),
                    ShardCommand::TruncateAssignment {
                        job: *job,
                        assignment: *assignment,
                        connection: *connection,
                        new_end: *new_end,
                    },
                );
            }
            Action::CloseConnection { connection } => {
                tracing::info!(target: "xde::engine", ?connection, "close connection");
                if let Some(shard) = lp.controller.connection_shard(*connection)
                    && matches!(
                        lp.shard_txs[shard].try_send(ShardCommand::CloseConnection {
                            connection: *connection
                        }),
                        Err(TrySendError::Full(_))
                    )
                {
                    lp.deferred_dispatch.push_back(DeferredDispatch {
                        shard,
                        command: DeferredCommand::Close(*connection),
                    });
                }
            }
            Action::CommitDestination {
                job,
                final_length,
                integrity,
            } => {
                let Some(state) = lp.jobs.get_mut(job) else {
                    continue;
                };
                let durability = state.context.durability();
                let destination =
                    std::mem::replace(&mut state.destination, DestinationState::Committing);
                let FileState {
                    coordinator,
                    journal,
                } = match destination {
                    DestinationState::Custom(dest) => {
                        send_shard(
                            lp,
                            0,
                            ShardCommand::CommitSharedDestination {
                                dest,
                                job: *job,
                                final_length: *final_length,
                                integrity: *integrity,
                            },
                        );
                        continue;
                    }
                    DestinationState::File(file) => *file,
                    DestinationState::Committing => {
                        lp.pending.push_back(Observation::DestinationFailed {
                            job: *job,
                            failure: crate::core::controller::DestinationFailure {
                                kind: crate::core::controller::DestinationFailureKind::DestinationError,
                            },
                        });
                        continue;
                    }
                };
                let part_path = state.part_path.clone();
                let lanes_closed = close_lanes_everywhere(lp, &part_path);
                send_shard(
                    lp,
                    0,
                    ShardCommand::CommitDestination {
                        job: *job,
                        part_path,
                        coordinator,
                        final_length: *final_length,
                        integrity: *integrity,
                        durability,
                        journal,
                        lanes_closed,
                    },
                );
            }
            Action::CancelTransfer { job } => {
                if let Some(state) = lp.jobs.get(job) {
                    state.context.cancel();
                }
            }
            Action::ResetTransfer { job } => {
                if let Some(state) = lp.jobs.get_mut(job) {
                    state.context = JobContext::with_durability(
                        state.context.deadline(),
                        state.context.durability(),
                    );
                    *state.provenance.lock().expect("provenance lock") =
                        crate::core::ArtifactProvenance::new();
                }
            }
            Action::DrainDestination { job } => checkpoint::drain(lp, *job),
            Action::FailJob { job, error } => {
                let err = checkpoint::failure_error(error);
                finish_job(lp, *job, Err(err));
            }
            Action::CompleteJob {
                job,
                total_bytes,
                digest,
                resumed_bytes,
            } => finish_job(
                lp,
                *job,
                Ok(JobOutcome {
                    bytes: *total_bytes,
                    digest: *digest,
                    resumed_bytes: *resumed_bytes,
                }),
            ),
            Action::ResetResumeData { job } => checkpoint::reset(lp, *job),
            Action::RequestCredentialRefresh {
                job,
                url,
                status,
                attempt,
            } => {
                let (job, status, attempt) = (*job, *status, *attempt);
                let outcome = match lp.jobs.get_mut(&job) {
                    Some(state) => match state.refresher.as_ref() {
                        Some(r) => {
                            let req = crate::core::credentials::RefreshRequest {
                                url: url.clone(),
                                status,
                                attempt,
                            };
                            match r.refresh(&req) {
                                Some(crate::core::credentials::RefreshedSource {
                                    url,
                                    headers,
                                }) => Ok((url, headers)),
                                None => Err(status),
                            }
                        }
                        None => Err(status),
                    },
                    None => continue,
                };
                let obs = match outcome {
                    Ok((new_url, headers)) => Observation::SourceRefreshed {
                        job,
                        url: new_url,
                        headers,
                    },
                    Err(status) => Observation::CredentialRefreshFailed { job, status },
                };
                let actions = lp.controller.handle(obs, Instant::now());
                route_actions(lp, &actions);
            }
            Action::ScheduleTimer { at, event } => {
                if let crate::core::timers::TimerEvent::RetryReady {
                    assignment,
                    range,
                    attempt,
                    ..
                } = event
                {
                    emit(
                        lp,
                        crate::core::events::Event::Retrying {
                            job: assignment.job,
                            range: Some(*range),
                            attempt: *attempt,
                            delay: at.saturating_duration_since(Instant::now()),
                            reason: "retry backoff".into(),
                        },
                    );
                }
                lp.timers.schedule(*at, event.clone());
            }
        }
    }
}

fn send_shard(lp: &mut Loop, shard: usize, cmd: ShardCommand) {
    use crate::core::controller::{
        DestinationFailure, DestinationFailureKind, DispatchOperation as Op,
    };
    let (operation, job, assignment, connection) = match &cmd {
        ShardCommand::Checkpoint { .. } => unreachable!("checkpoint admission owns its completion"),
        ShardCommand::OpenConnection { connection, .. } => {
            (Op::OpenConnection, None, None, Some(*connection))
        }
        ShardCommand::Probe {
            job, connection, ..
        } => (Op::Probe, Some(*job), None, Some(*connection)),
        ShardCommand::StartRange {
            job,
            assignment,
            connection,
            ..
        } => (
            Op::StartAssignment,
            Some(*job),
            Some(*assignment),
            Some(*connection),
        ),
        ShardCommand::StartFullBody {
            job,
            assignment,
            connection,
            ..
        } => (
            Op::StartFullBody,
            Some(*job),
            Some(*assignment),
            Some(*connection),
        ),
        ShardCommand::TruncateAssignment {
            job,
            assignment,
            connection,
            ..
        } => (
            Op::TruncateAssignment,
            Some(*job),
            Some(*assignment),
            Some(*connection),
        ),
        ShardCommand::SampleMirror {
            job, connection, ..
        } => (Op::SampleMirror, Some(*job), None, Some(*connection)),
        ShardCommand::CommitDestination { job, .. }
        | ShardCommand::CommitSharedDestination { job, .. } => {
            (Op::CommitDestination, Some(*job), None, None)
        }
        ShardCommand::PrepareDestination { job, .. } => {
            (Op::PrepareDestination, Some(*job), None, None)
        }
        ShardCommand::CloseConnection { connection } => {
            (Op::CloseConnection, None, None, Some(*connection))
        }
        ShardCommand::ReleaseJob { job, .. } => (Op::CloseLane, Some(*job), None, None),
        ShardCommand::CloseLane { .. } => (Op::CloseLane, None, None, None),
    };
    let failed = lp
        .shard_txs
        .get(shard)
        .is_none_or(|tx| tx.try_send(cmd).is_err());
    if !failed {
        return;
    }
    let observation = if operation == Op::CommitDestination {
        Observation::DestinationFailed {
            job: job.expect("commit belongs to a job"),
            failure: DestinationFailure {
                kind: DestinationFailureKind::DestinationError,
            },
        }
    } else {
        Observation::DispatchFailed {
            operation,
            job,
            assignment,
            connection,
            origin: None,
        }
    };
    lp.pending.push_back(observation);
}

/// Publish telemetry for an observation. Events are derived from real runtime
/// state here, once, before the controller consumes the observation.
fn emit_observation_events(lp: &Loop, obs: &Observation) {
    use crate::core::events::{Event, Protocol};
    match obs {
        Observation::ConnectionReady {
            connection,
            protocol,
            ..
        } => {
            let Some(c) = lp.controller.world.connections.get(*connection) else {
                return;
            };
            let job = c.owner;
            if let Some(job) = job {
                emit(
                    lp,
                    Event::ConnectionOpened {
                        job,
                        connection: *connection,
                        protocol: *protocol,
                        shard: c.shard,
                    },
                );
            }
        }
        Observation::Probed {
            job,
            connection,
            supports_ranges,
            total_length,
            ..
        } => {
            let protocol = lp
                .controller
                .world
                .connections
                .get(*connection)
                .map(|c| c.protocol)
                .unwrap_or(Protocol::Http1_1);
            emit(
                lp,
                Event::SourceProbed {
                    job: *job,
                    source: lp
                        .controller
                        .world
                        .jobs
                        .get(*job)
                        .and_then(|j| j.active_source())
                        .unwrap_or_default(),
                    supports_ranges: *supports_ranges,
                    total: *total_length,
                    protocol,
                    ttfb: Duration::ZERO,
                },
            );
        }
        Observation::AssignmentVerified {
            job,
            connection,
            range,
            sample,
            ..
        } => {
            // Telemetry carries the *receive* rate: pure network evidence,
            // never polluted by destination or memory stalls.
            emit(
                lp,
                Event::WorkerFinished {
                    job: *job,
                    connection: *connection,
                    range: *range,
                    sample: sample.clone(),
                },
            );
        }
        _ => {}
    }
}

/// Build a compact public projection of one job's state. Reads only the
/// WorldModel + job bookkeeping; never exposes the model itself.
/// Compact engine-global projection for UI polling.
fn build_engine_snapshot(lp: &Loop) -> crate::snapshot::EngineSnapshot {
    let mut active_jobs = 0usize;
    let mut active_streams = 0usize;
    for (_, j) in lp.controller.world.jobs.iter() {
        if j.activated && !j.phase.is_draining() && j.phase != crate::core::world::JobPhase::Paused
        {
            active_jobs += 1;
        }
        active_streams += j.plan.as_ref().map_or(0, |p| p.in_flight());
    }
    let mut protocol_counts: std::collections::BTreeMap<String, usize> = Default::default();
    let mut origins: Vec<crate::core::ids::OriginId> = Vec::new();
    for (_, c) in lp.controller.world.connections.iter() {
        *protocol_counts
            .entry(format!("{:?}", c.protocol))
            .or_insert(0) += 1;
        if !origins.contains(&c.origin) {
            origins.push(c.origin);
        }
    }
    let protocol_counts: Vec<_> = protocol_counts.into_iter().collect();
    let protocol_counts: Vec<(crate::core::events::Protocol, usize)> = protocol_counts
        .into_iter()
        .map(|(name, count)| {
            let p = match name.as_str() {
                "Http3" => crate::core::events::Protocol::Http3,
                "Http2" => crate::core::events::Protocol::Http2,
                _ => crate::core::events::Protocol::Http1_1,
            };
            (p, count)
        })
        .collect();
    crate::snapshot::EngineSnapshot {
        controller: lp.controller.stats,
        queued_jobs: lp
            .controller
            .world
            .jobs
            .values()
            .filter(|j| {
                !j.activated
                    && matches!(
                        j.phase,
                        crate::core::world::JobPhase::Created
                            | crate::core::world::JobPhase::Probing
                            | crate::core::world::JobPhase::Transferring
                    )
            })
            .count(),
        active_jobs,
        physical_connections: lp.controller.reserved_connections(),
        active_streams,
        protocol_counts,
        active_origins: origins.len(),
        memory_used: lp.memory.used(),
        memory_limit: lp.memory.limit(),
    }
}

fn build_snapshot(lp: &Loop, job: JobId) -> Option<crate::snapshot::JobSnapshot> {
    let j = lp.controller.world.jobs.get(job)?;
    // Only connections currently owned by this job enter its projection.
    let mut active_connections = 0usize;
    let mut protocols = Vec::new();
    for (_, c) in lp.controller.world.connections.iter() {
        if c.owner == Some(job) && c.status == ConnectionStatus::Ready {
            active_connections += 1;
            if !protocols.contains(&c.protocol) {
                protocols.push(c.protocol);
            }
        }
    }
    let receive_rate_bps = j.plan.as_ref().and_then(|p| {
        let stats = p.rate_stats();
        stats.is_warm().then(|| stats.mean())
    });
    Some(crate::snapshot::JobSnapshot {
        desired: j.planner.desired(),
        granted: j.grant,
        queued: !j.activated
            && matches!(
                j.phase,
                crate::core::world::JobPhase::Created
                    | crate::core::world::JobPhase::Probing
                    | crate::core::world::JobPhase::Transferring
            ),
        globally_constrained: lp.controller.globally_constrained(job),
        opening_connections: lp
            .controller
            .world
            .connections
            .values()
            .filter(|c| c.owner == Some(job) && c.status == ConnectionStatus::Connecting)
            .count(),
        consumed_bytes: j.useful_bytes.saturating_add(
            j.plan
                .iter()
                .flat_map(|p| p.iter_assignments())
                .map(|(_, a)| a.control.bytes())
                .sum::<u64>(),
        ),
        phase: (&j.phase).into(),
        total_length: j.total_length,
        verified_bytes: j.plan.as_ref().map_or(0, |p| p.bytes_done()),
        resumed_bytes: j.resumed_bytes,
        receive_rate_bps,
        active_connections,
        active_streams: j.plan.as_ref().map_or(0, |p| p.in_flight()),
        protocols,
    })
}

fn publish_progress(lp: &Loop, job: JobId) {
    let Some(state) = lp.jobs.get(&job) else {
        return;
    };
    let Some(publisher) = &state.progress else {
        return;
    };
    let Some(snapshot) = build_snapshot(lp, job) else {
        return;
    };
    publisher.publish(crate::DownloadProgress::new(
        snapshot.verified_bytes,
        snapshot.total_length,
        snapshot.receive_rate_bps,
    ));
}

fn emit(lp: &Loop, event: crate::core::events::Event) {
    if let Some(sink) = &lp.events {
        sink.emit(event);
    }
}

/// Journal bookkeeping driven by real observations, before the controller
/// sees them.
fn record_progress(lp: &mut Loop, obs: &Observation) {
    match obs {
        Observation::Probed {
            job,
            etag,
            last_modified,
            total_length,
            ..
        } => {
            let Some(state) = lp.jobs.get_mut(job) else {
                return;
            };
            let Some(journal) = state.destination.journal_mut() else {
                return;
            };
            {
                let fp = &mut journal.payload_mut().fingerprint;
                fp.etag = etag.clone();
                fp.last_modified_unix = last_modified.map(|t| {
                    t.duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs())
                        .unwrap_or(0)
                });
            }
            journal.payload_mut().total = *total_length;
            journal.payload_mut().fingerprint.content_length = *total_length;
        }
        Observation::AssignmentVerified {
            job,
            assignment,
            range,
            ..
        } => {
            if !lp.controller.has_assignment(*assignment)
                || lp
                    .controller
                    .world
                    .jobs
                    .get(*job)
                    .is_none_or(|j| j.phase.is_draining())
            {
                return;
            }
            checkpoint::record(lp, *job, *range);
        }
        _ => {}
    }
}

/// Copy the controller's view of the representation into the journal.
fn sync_fingerprint(node: Option<&crate::core::world::JobNode>, journal: &mut Journal) {
    let Some(node) = node else { return };
    let payload = journal.payload_mut();
    payload.total = node.total_length.or(payload.total);
    payload.fingerprint.content_length = node.total_length;
    if let Some(etag) = &node.fingerprint_etag {
        payload.fingerprint.etag.get_or_insert_with(|| etag.clone());
    }
}

fn close_lanes_everywhere(lp: &Loop, part_path: &std::path::Path) -> Vec<Receiver<()>> {
    lp.shard_txs
        .iter()
        .map(|tx| {
            let (ack, done) = flume::bounded(1);
            let _ = tx.try_send(ShardCommand::CloseLane {
                part_path: part_path.to_path_buf(),
                ack,
            });
            done
        })
        .collect()
}

fn shard_of_connection(lp: &Loop, conn: ConnectionId) -> usize {
    // The world model allocated the connection with its shard; commands for
    // a connection always route to the shard that owns the socket.
    lp.controller
        .world
        .connections
        .get(conn)
        .map_or(0, |c| c.shard)
}

fn build_target(
    lp: &Loop,
    origin: crate::core::ids::OriginId,
    endpoint: crate::core::ids::EndpointId,
) -> Option<ConnectTarget> {
    let o = lp.controller.world.origins.get(origin)?;
    let e = lp.controller.world.endpoints.get(endpoint)?;
    let key = &o.key;
    Some(ConnectTarget {
        http_version: crate::HttpVersionPolicy::Auto,
        key: key.clone(),
        addr: e.address,
        sni: key.host.to_string(),
        tls: key.scheme == "https",
    })
}

fn build_source_context(
    lp: &Loop,
    job: JobId,
    source: crate::core::SourceId,
) -> Option<Arc<crate::http::SourceContext>> {
    let j = lp.controller.world.jobs.get(job)?;
    // Always read the URL/headers straight from the controller's source
    // node: a stale prepared cache would re-issue probes against a
    // pre-redirect URL and trigger the loop detector (or worse, a 404).
    let s = lp.controller.world.sources.get(source)?;
    let url = url::Url::parse(&s.url).ok()?;
    let if_range = j
        .fingerprint_etag
        .as_ref()
        .filter(|e| !e.starts_with("W/") && e.starts_with('"'))
        .cloned();
    Some(Arc::new(crate::http::SourceContext {
        url,
        if_range,
        allow_compressed: j.policy.allow_compressed,
        extra_headers: s.headers.clone(),
        deadline: j.deadline,
    }))
}

/// Resolve which of a job's sources a connection serves: an explicitly
/// tagged mirror, else the active source whose origin owns the connection,
/// else the job's primary.
fn source_for_connection(
    lp: &Loop,
    job: JobId,
    connection: crate::core::ConnectionId,
) -> Option<crate::core::SourceId> {
    let conn_origin = lp
        .controller
        .world
        .connections
        .get(connection)
        .map(|c| c.origin);
    let j = lp.controller.world.jobs.get(job)?;
    if let Some(sid) = lp
        .controller
        .world
        .connections
        .get(connection)
        .and_then(|c| c.serving_source)
    {
        return Some(sid);
    }
    j.active_sources
        .iter()
        .copied()
        .find(|sid| {
            lp.controller
                .world
                .sources
                .get(*sid)
                .is_some_and(|s| Some(s.origin) == conn_origin)
        })
        .or_else(|| j.active_source())
}

/// Terminal bookkeeping: reply to the waiter and drop state.
fn finish_job(lp: &mut Loop, job: JobId, outcome: Result<JobOutcome>) {
    let ok = outcome.is_ok();
    if let Some(state) = lp.jobs.remove(&job) {
        for (shard, tx) in lp.shard_txs.iter().enumerate() {
            if matches!(
                tx.try_send(ShardCommand::ReleaseJob {
                    job,
                    part_path: state.part_path.clone()
                }),
                Err(TrySendError::Full(_))
            ) {
                lp.deferred_dispatch.push_back(DeferredDispatch {
                    shard,
                    command: DeferredCommand::Release {
                        job,
                        part_path: state.part_path.clone(),
                    },
                });
            }
        }
        use crate::core::events::Event;
        match &outcome {
            Ok(o) => emit(
                lp,
                Event::Completed {
                    job,
                    bytes: o.bytes,
                    duration: Duration::ZERO,
                    average_rate: crate::core::units::Rate::FLOOR,
                },
            ),
            Err(Error::Cancelled) => emit(lp, Event::Cancelled { job }),
            Err(e) => emit(
                lp,
                Event::Failed {
                    job,
                    error: e.to_string().into(),
                },
            ),
        }
        if let Some(publisher) = &state.progress {
            let final_progress = outcome
                .as_ref()
                .ok()
                .map(|result| crate::DownloadProgress::new(result.bytes, Some(result.bytes), None));
            publisher.close(final_progress);
        }
        let _ = state.result_tx.send(outcome);
        if ok {
            state.context.finish();
        } else {
            state.context.cancel();
        }
    }
}

#[cfg(test)]
mod profile_tests {
    use super::*;

    #[test]
    fn no_profile_path_means_no_io() {
        // The default configuration is `None`; both helpers are strict
        // no-ops, so a default engine performs zero persistence I/O.
        let none: Option<PathBuf> = None;
        assert!(load_profile(&none).is_none());
        let world = crate::core::WorldModel::new();
        save_profile(&none, world.export_profiles()); // must not touch disk
    }

    #[test]
    fn corrupt_profile_is_discarded_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("profile.json");
        std::fs::write(&path, b"{ not valid json !!!").unwrap();
        assert!(
            load_profile(&Some(path.clone())).is_none(),
            "corrupt profiles load as nothing"
        );
        // Wrong format version loads as nothing too (import_profiles guards).
        let wrong = r#"{"format_version":999,"origins":[]}"#;
        std::fs::write(&path, wrong).unwrap();
        let parsed = load_profile(&Some(path)).expect("valid json parses");
        let mut world = crate::core::WorldModel::new();
        world.import_profiles(&parsed); // must be a silent no-op
    }
}
