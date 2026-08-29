//! Dedicated control-plane thread.
//!
//! The loop owns the pure `Controller` (and through it the `WorldModel`), a
//! `TimerQueue`, the resolver, the shard mailboxes and the per-job reply
//! channels. It feeds observations into the controller and executes the
//! returned actions. All scheduling intelligence lives in `crate::core::controller`;
//! this file is routing and I/O glue only.

use std::{
    collections::HashMap,
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
    pub destination_key: String,
    pub final_path: Option<PathBuf>,
    /// Application-provided shared destination (custom sink). When set,
    /// `final_path`/coordinator/journal/resume are all bypassed.
    pub shared_destination:
        Option<std::sync::Arc<dyn crate::storage::DynDestination + Send + Sync>>,
    /// Application-provided credential refresher for this job, if any.
    /// Not `Debug`: refreshers may hold secrets; they are never logged.
    pub refresher: Option<std::sync::Arc<dyn crate::core::credentials::SourceRefresher>>,
    /// Graceful shutdown request (spec fields unused when set).
    pub shutdown: bool,
    /// Admission reply: JobId or the lease-conflict error.
    pub admit_tx: Sender<Result<JobId>>,
    /// Final outcome channel.
    pub result_tx: Sender<Result<JobOutcome>>,
}

/// Per-job execution state owned by the control thread.
struct JobState {
    result_tx: Sender<Result<JobOutcome>>,
    context: JobContext,
    coordinator: Option<FileDestinationCoordinator>,
    part_path: PathBuf,
    /// This destination's crash-recovery journal, loaded at admission and
    /// updated as verified ranges arrive. Persisted per the durability
    /// policy; removed on successful commit.
    journal: Option<Journal>,
    /// Shared custom destination, when the job was admitted with one.
    shared: Option<Arc<dyn crate::storage::DynDestination + Send + Sync>>,
    /// Application-provided credential refresher, if any.
    refresher: Option<std::sync::Arc<dyn crate::core::credentials::SourceRefresher>>,
    provenance: std::sync::Arc<std::sync::Mutex<crate::core::ArtifactProvenance>>,
}

/// Handle held by the public Engine.
#[derive(Clone)]
pub struct ControlHandle {
    tx: Sender<ControlCommand>,
    cancel_tx: Sender<JobId>,
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
        self.tx.try_send(cmd).map_err(|e| match e {
            TrySendError::Full(_) => Error::Runtime(crate::core::RuntimeError::EngineBusy),
            TrySendError::Disconnected(_) => Error::Runtime(crate::core::RuntimeError::EngineGone),
        })
    }

    pub fn shutdown(&self) -> Result<()> {
        let (admit_tx, _admit_rx) = flume::bounded(1);
        let (result_tx, result_rx) = flume::bounded(1);
        let sent = self.tx.send_timeout(
            ControlCommand {
                spec: Box::new(crate::core::JobSpec::new(
                    url::Url::parse("about:xde-shutdown").expect("static url"),
                )),
                destination_key: String::new(),
                final_path: None,
                shared_destination: None,
                refresher: None,
                shutdown: true,
                admit_tx,
                result_tx,
            },
            std::time::Duration::from_secs(10),
        );
        drop(result_rx);
        sent.map_err(|e| match e {
            flume::SendTimeoutError::Timeout(_) => {
                Error::Runtime(crate::core::RuntimeError::EngineBusy)
            }
            flume::SendTimeoutError::Disconnected(_) => {
                Error::Runtime(crate::core::RuntimeError::EngineGone)
            }
        })
    }

    pub(crate) fn cancel(&self, job: JobId) -> Result<()> {
        self.cancel_tx
            .try_send(job)
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
struct Loop {
    controller: Controller,
    timers: TimerQueue,
    jobs: HashMap<JobId, JobState>,
    obs_tx: Sender<Observation>,
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
    cmd_rx: Receiver<ControlCommand>,
    cancel_rx: Receiver<JobId>,
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
        let (status_tx, status_rx) = flume::unbounded::<StatusRequest>();
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
    let mut lp = Loop {
        controller: Controller::with_shards(shards),
        timers: TimerQueue::new(),
        jobs: HashMap::new(),
        // The loop holds no clone of obs_tx: dropping the engine disconnects
        // this receiver, which is the shutdown signal.
        obs_tx,
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

    let mut shutdown = false;
    loop {
        // 1. Admissions (synchronous so submit() gets its JobId).
        while let Ok(cmd) = cmd_rx.try_recv() {
            if cmd.shutdown {
                shutdown = true;
                continue;
            }
            admit_command(&mut lp, cmd);
        }
        // Explicit graceful shutdown: cancel every active job. The
        // cancellation path checkpoints journals, so resumable state is
        // persisted before the loop exits. Any job unknown to the
        // controller (mid-cancellation already) is answered by the drain
        // after the loop.
        let mut terminate = false;
        if shutdown {
            for job in lp.jobs.keys().copied().collect::<Vec<_>>() {
                tracing::info!(target: "xde::engine", ?job, "shutdown cancel");
                let actions = lp
                    .controller
                    .handle(Observation::JobCancelled { job }, Instant::now());
                route_actions(&mut lp, &actions);
            }
            terminate = true;
        }
        // 1b. Cancellation requests become observations.
        while let Ok(job) = cancel_rx.try_recv() {
            tracing::info!(target: "xde::engine", ?job, "cancel requested");
            if let Some(state) = lp.jobs.get(&job) {
                state.context.cancel();
            }
            let actions = lp
                .controller
                .handle(Observation::JobCancelled { job }, Instant::now());
            route_actions(&mut lp, &actions);
        }

        // 1c. Snapshot queries: bounded drain so a polling client cannot
        // starve observations.
        for _ in 0..16 {
            let Ok(req) = lp.status_rx.try_recv() else {
                break;
            };
            match req {
                StatusRequest::Job { job, reply } => {
                    let _ = reply.send(build_snapshot(&lp, job));
                }
                StatusRequest::Engine { reply } => {
                    let _ = reply.send(build_engine_snapshot(&lp));
                }
            }
        }

        // 2. Wait for the next observation or timer deadline.
        let timeout = lp
            .timers
            .next_deadline()
            .map(|d| d.saturating_duration_since(Instant::now()))
            .unwrap_or(Duration::from_millis(50));
        match obs_rx.recv_timeout(timeout.max(Duration::from_millis(1))) {
            Ok(obs) => {
                emit_observation_events(&lp, &obs);
                record_progress(&mut lp, &obs);
                let actions = lp.controller.handle(obs, Instant::now());
                route_actions(&mut lp, &actions);
            }
            // Engine dropped: deterministic shutdown.
            Err(flume::RecvTimeoutError::Disconnected) => break,
            Err(flume::RecvTimeoutError::Timeout) => {}
        }

        // 3. Expired timers become observations.
        for event in lp.timers.drain_expired(Instant::now()) {
            if let crate::core::timers::TimerEvent::CheckpointDue(job) = &event {
                checkpoint_journal(&mut lp, *job);
            }
            let actions = lp
                .controller
                .handle(Observation::TimerExpired { event }, Instant::now());
            route_actions(&mut lp, &actions);
        }

        // 4. Both upstream mailboxes gone (engine dropped or shutdown
        // requested and answered): exit. Any remaining job state is drained
        // after the loop, which cancels contexts and answers every waiter
        // explicitly rather than dropping the reply channel.
        if terminate || (cmd_rx.is_disconnected() && obs_rx.is_disconnected()) {
            break;
        }
    }

    // Deterministic shutdown: fail every remaining waiter explicitly rather
    // than dropping the reply channel (which would surface as EngineGone).
    for (_, state) in lp.jobs.drain() {
        state.context.cancel();
        let _ = state
            .result_tx
            .send(Err(Error::Runtime(crate::core::RuntimeError::EngineGone)));
        // Coordinator drops here: its own Drop releases the OS lock file.
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
    if lp.controller.world.jobs.len() >= lp.controller.max_jobs() {
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
        Ok((job, _artifact)) => job,
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
    if let Err(e) = lp.controller.world.set_destination_part_path(
        job,
        coordinator.as_ref().map(|c| c.part_path().to_path_buf()),
    ) {
        let msg = e.to_string();
        let _ = cmd.admit_tx.send(Err(e));
        let _ = cmd.result_tx.send(Err(Error::destination(msg)));
        return;
    }

    let part_path = coordinator
        .as_ref()
        .map(|c| c.part_path().to_path_buf())
        .unwrap_or_default();

    // Resume discovery: locate `.part` + journal, validate format and
    // geometry, and compute the ranges that can safely seed the plan.
    let discovered = coordinator.as_ref().map(|c| discover_resume(c, &spec));
    let (journal, resume) = match discovered {
        Some(Ok(Some(j))) => {
            let evidence = ResumeEvidence {
                durable: j.payload().durable.clone(),
                total: j.payload().total,
                etag: j.payload().fingerprint.etag.clone(),
                last_modified: j.payload().fingerprint.last_modified_unix.and_then(|unix| {
                    std::time::UNIX_EPOCH.checked_add(std::time::Duration::from_secs(unix))
                }),
                urls: vec![j.payload().fingerprint.final_url.clone()],
            };
            (Some(j), Some(Box::new(evidence)))
        }
        _ => (None, None),
    };

    lp.jobs.insert(
        job,
        JobState {
            result_tx: cmd.result_tx.clone(),
            context: JobContext::with_durability(spec.deadline, spec.persistence),
            coordinator,
            part_path,
            journal,
            shared: cmd.shared_destination.clone(),
            refresher: cmd.refresher,
            provenance: std::sync::Arc::new(std::sync::Mutex::new(
                crate::core::ArtifactProvenance::new(),
            )),
        },
    );
    // Shared custom destination: register its capabilities so the
    // controller gates segmentation/parallelism honestly, and hand the
    // destination to the shard that will write it.
    if let Some(dest) = &cmd.shared_destination {
        if let Err(e) = lp.controller.world.set_sink_properties(
            job,
            crate::storage::DynDestination::caps(dest.as_ref()),
            crate::storage::DynDestination::hints(dest.as_ref()),
        ) {
            let _ = cmd.admit_tx.send(Err(e));
            return;
        }
        // Attach on every shard: connections (and their fetch tasks) may
        // land on any of them.
        for tx in &lp.shard_txs {
            let _ = tx.try_send(ShardCommand::AttachDestination {
                job,
                dest: cmd.shared_destination.clone().unwrap(),
            });
        }
    }
    let resumed = resume.as_ref().map_or(0, |r| r.durable.covered_len());
    let _ = cmd.admit_tx.send(Ok(job));
    emit(
        lp,
        crate::core::events::Event::Started {
            job,
            total: None,
            resumed_bytes: resumed,
        },
    );

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
/// Returns `Ok(None)` when there is nothing to resume. A corrupt or
/// incompatible journal is discarded, not fatal. Durable ranges that do not
/// fit inside an existing `.part` are dropped conservatively.
fn discover_resume(
    coordinator: &FileDestinationCoordinator,
    spec: &crate::core::JobSpec,
) -> Result<Option<Journal>> {
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
        Ok(None) => return Ok(Some(fresh(spec, journal_path.clone()))),
        // Corrupt/torn/incompatible version: resume from nothing.
        Err(_) => return Ok(Some(fresh(spec, journal_path))),
    };

    // The partial artifact must exist and cover every durable range.
    if !part_path.exists() {
        return Ok(Some(fresh(spec, journal_path.clone())));
    }
    let part_len = std::fs::metadata(part_path).map(|m| m.len()).unwrap_or(0);
    if part_len == 0 {
        return Ok(Some(fresh(spec, journal_path)));
    }
    let payload = journal.payload_mut();
    let surviving: Vec<crate::core::ranges::ByteRange> = payload
        .durable
        .iter()
        .filter(|r| r.end <= part_len)
        .collect();
    payload.durable.clear();
    for r in surviving {
        payload.durable.insert(r);
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
    journal.mark_dirty();
    Ok(Some(journal))
}

fn route_actions(lp: &mut Loop, actions: &[Action]) {
    for action in actions {
        tracing::trace!(target: "xde::engine", ?action, "control action");
        match action {
            Action::Resolve { origin, host, port } => {
                lp.resolver
                    .resolve(*origin, host.clone(), *port, lp.obs_tx.clone());
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
                // The world model already removed the node; find which shard
                // owned it from the pre-removal record kept by the caller of
                // remove_connection. We broadcast close; shards ignore
                // unknown ids.
                for tx in &lp.shard_txs {
                    let _ = tx.try_send(ShardCommand::CloseConnection {
                        connection: *connection,
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
                // Shared custom destination: finalize on the shard that
                // holds it (flush → read-back digest → commit).
                if state.shared.is_some() {
                    let (ack_tx, ack_rx) = flume::bounded(1);
                    send_shard(
                        lp,
                        0,
                        ShardCommand::CommitSharedDestination {
                            job: *job,
                            final_length: *final_length,
                            integrity: *integrity,
                            ack: ack_tx,
                        },
                    );
                    // Bounded wait: commit is the job's terminal step and
                    // every downstream path (outcome reply, journal cleanup)
                    // needs the digest verdict.
                    let result = ack_rx.recv_timeout(Duration::from_secs(30));
                    let obs = match result {
                        Ok(Ok(digest)) => Observation::DestinationCommitted {
                            job: *job,
                            final_length: *final_length,
                            digest,
                        },
                        Ok(Err(e)) => Observation::DestinationFailed {
                            job: *job,
                            failure: crate::core::controller::DestinationFailure::from_error(&e),
                        },
                        Err(_) => Observation::DestinationFailed {
                            job: *job,
                            failure: crate::core::controller::DestinationFailure {
                                kind:
                                    crate::core::controller::DestinationFailureKind::DestinationError,
                            },
                        },
                    };
                    let _ = lp.obs_tx.send(obs);
                    continue;
                }
                let durability = state.context.durability();
                let Some(coordinator) = state.coordinator.take() else {
                    // Non-file destination: nothing to finalize.
                    let _ = lp.obs_tx.send(Observation::DestinationCommitted {
                        job: *job,
                        final_length: *final_length,
                        digest: None,
                    });
                    continue;
                };
                // Every shard that opened a lane against the `.part` must
                // drop its handle before the rename; a Windows rename with
                // any writer open fails outright.
                let part_path = state.part_path.clone();
                close_lanes_everywhere(lp, &part_path);
                // The finalize itself runs on shard 0.
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
                    },
                );
            }
            Action::DiscardDestination { job } => {
                if let Some(state) = lp.jobs.remove(job) {
                    state.context.cancel();
                    // Coordinator drops: lock file released, `.part` left for
                    // a future resume decision.
                }
            }
            Action::FailJob { job, error } => {
                let err = match error.as_str() {
                    crate::core::controller::CANCELLED_SENTINEL => Error::Cancelled,
                    crate::core::controller::DEADLINE_SENTINEL => Error::DeadlineExceeded,
                    _ => Error::destination(error.clone()),
                };
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
            Action::ResetResumeData { job } => {
                if let Some(state) = lp.jobs.get_mut(job)
                    && let Some(journal) = state.journal.as_mut()
                {
                    journal.clear_ranges();
                    // Persist immediately: this is a correctness write, not
                    // a throughput optimization.
                    if let Err(error) = journal.persist_blocking() {
                        tracing::error!(
                            target: "xde::engine",
                            ?job,
                            %error,
                            "resume wipe failed"
                        );
                    }
                }
            }
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

fn send_shard(lp: &Loop, shard: usize, cmd: ShardCommand) {
    let Some(tx) = lp.shard_txs.get(shard) else {
        return;
    };
    if let Err(e) = tx.try_send(cmd) {
        let is_full = matches!(e, flume::TrySendError::Full(_));
        let _ = lp.obs_tx.send(Observation::DispatchFailed {
            operation: if is_full {
                crate::core::controller::DispatchOperation::StartAssignment
            } else {
                crate::core::controller::DispatchOperation::CloseConnection
            },
            job: None,
            assignment: None,
            connection: None,
            origin: None,
        });
    }
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
            // Attribute the connection to whichever job is waiting to probe
            // or transfer on its origin; connections are engine-owned pools.
            let job = lp
                .controller
                .world
                .jobs
                .iter()
                .find(|(_, j)| j.origin == c.origin)
                .map(|(id, _)| id);
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
            job, range, sample, ..
        } => {
            // Telemetry carries the *receive* rate: pure network evidence,
            // never polluted by destination or memory stalls.
            emit(
                lp,
                Event::WorkerFinished {
                    job: *job,
                    range: *range,
                    rate: crate::core::units::Rate::from_bps(sample.receive_rate()),
                    receive_active: sample.receive_active,
                    destination_blocked: sample.destination_blocked,
                    next_pending: sample.next_pending,
                    max_frame_gap: sample.max_frame_gap,
                    send_ready: sample.send_ready,
                    headers: sample.headers,
                    data_frames: sample.data_frames,
                    dest_accepts: sample.dest_accepts,
                    copy_count: sample.copy_count,
                    copied_bytes: sample.copied_bytes,
                    avg_frame: sample.avg_frame,
                    frame_p50: sample.frame_p50,
                    frame_p90: sample.frame_p90,
                    io_reads_submitted: sample.io_reads_submitted,
                    io_reads_completed: sample.io_reads_completed,
                    zero_read: sample.zero_read,
                    max_zero_read: sample.max_zero_read,
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
        if !j.phase.is_terminal() && j.phase != crate::core::world::JobPhase::Cancelling {
            active_jobs += 1;
        }
        active_streams += j.plan.as_ref().map_or(0, |p| p.in_flight());
    }
    let mut physical_connections = 0usize;
    let mut protocol_counts: std::collections::BTreeMap<String, usize> = Default::default();
    let mut origins: Vec<crate::core::ids::OriginId> = Vec::new();
    for (_, c) in lp.controller.world.connections.iter() {
        physical_connections += 1;
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
        active_jobs,
        physical_connections,
        active_streams,
        protocol_counts,
        active_origins: origins.len(),
        memory_used: lp.memory.used(),
        memory_limit: lp.memory.limit(),
    }
}

fn build_snapshot(lp: &Loop, job: JobId) -> Option<crate::snapshot::JobSnapshot> {
    let j = lp.controller.world.jobs.get(job)?;
    // Live connections for this origin (job shares engine-owned pools).
    let mut active_connections = 0usize;
    let mut protocols = Vec::new();
    for (_, c) in lp.controller.world.connections.iter() {
        if c.origin == j.origin && c.status == ConnectionStatus::Ready {
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
        phase: j.phase.into(),
        total_length: j.total_length,
        verified_bytes: j.plan.as_ref().map_or(0, |p| p.bytes_done()),
        resumed_bytes: j.resumed_bytes,
        receive_rate_bps,
        active_connections,
        active_streams: j.plan.as_ref().map_or(0, |p| p.in_flight()),
        protocols,
    })
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
            let Some(journal) = state.journal.as_mut() else {
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
        Observation::AssignmentVerified { job, range, .. } => {
            let durability = match lp.controller.world.jobs.get(*job) {
                Some(j) => j.durability,
                None => return,
            };
            match durability {
                crate::core::spec::Durability::CrashSafe => {
                    // Every verified piece must survive a crash before the
                    // next one starts: fsync here is the contract.
                    checkpoint_journal_at(lp, *job, |journal| {
                        journal.record_completed(*range);
                        journal.mark_completed_durable();
                    });
                }
                crate::core::spec::Durability::Periodic(interval) => {
                    // Record in memory; persist on the periodic timer. The
                    // control thread must NEVER fsync per piece - that would
                    // serialize the whole engine on disk latency.
                    record_only(lp, *job, *range);
                    schedule_checkpoint(lp, *job, interval);
                }
                crate::core::spec::Durability::Relaxed => {
                    record_only(lp, *job, *range);
                }
            }
        }
        _ => {}
    }
}

/// Record a verified range in the in-memory journal without persisting.
fn record_only(lp: &mut Loop, job: JobId, range: crate::core::ranges::ByteRange) {
    if let Some(state) = lp.jobs.get_mut(&job)
        && let Some(journal) = state.journal.as_mut()
    {
        journal.record_completed(range);
    }
}

/// Persist one job's journal with the current fingerprint evidence, then
/// mark everything recorded so far durable.
fn checkpoint_journal(lp: &mut Loop, job: JobId) -> bool {
    checkpoint_journal_at(lp, job, |journal| journal.mark_completed_durable())
}

fn checkpoint_journal_at(lp: &mut Loop, job: JobId, mutate: impl FnOnce(&mut Journal)) -> bool {
    let persisted = {
        let Some(state) = lp.jobs.get_mut(&job) else {
            return false;
        };
        let Some(journal) = state.journal.as_mut() else {
            return false;
        };
        mutate(journal);
        sync_fingerprint(lp.controller.world.jobs.get(job), journal);
        journal.persist_blocking().is_ok()
    };
    if persisted {
        emit(
            lp,
            crate::core::events::Event::Checkpointed {
                job,
                bytes_done: lp
                    .jobs
                    .get(&job)
                    .and_then(|s| s.journal.as_ref())
                    .map_or(0, |j| j.payload().bytes_written),
                durable: true,
            },
        );
        true
    } else {
        false
    }
}

fn schedule_checkpoint(lp: &mut Loop, job: JobId, interval: std::time::Duration) {
    let at = Instant::now() + interval;
    lp.timers
        .schedule(at, crate::core::timers::TimerEvent::CheckpointDue(job));
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

/// Broadcast a lane close to every shard and wait for all acknowledgements.
/// Bounded: a misbehaving shard delays the commit by at most `timeout`.
fn close_lanes_everywhere(lp: &Loop, part_path: &std::path::Path) {
    let timeout = Duration::from_secs(5);
    let (ack_tx, ack_rx) = flume::bounded(lp.shard_txs.len());
    for tx in &lp.shard_txs {
        let _ = tx.try_send(ShardCommand::CloseLane {
            part_path: part_path.to_path_buf(),
            ack: ack_tx.clone(),
        });
    }
    drop(ack_tx);
    let mut remaining = lp.shard_txs.len();
    let deadline = Instant::now() + timeout;
    while remaining > 0 {
        let now = Instant::now();
        if now >= deadline {
            return;
        }
        match ack_rx.recv_timeout(deadline - now) {
            Ok(()) => remaining -= 1,
            Err(flume::RecvTimeoutError::Timeout | flume::RecvTimeoutError::Disconnected) => return,
        }
    }
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
    if ok {
        // Published successfully: the journal's job is done.
        if let Some(state) = lp.jobs.get(&job)
            && let Some(journal) = state.journal.as_ref()
        {
            let _ = std::fs::remove_file(journal.path());
        }
    } else {
        // Failure/cancellation: persist verified progress so a later run can
        // resume. Best effort - losing it only costs re-downloaded bytes.
        checkpoint_journal(lp, job);
    }
    if let Some(state) = lp.jobs.remove(&job) {
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
