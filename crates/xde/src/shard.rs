//! Resident shard service.
//!
//! One `ShardService` runs per Compio shard. It owns every non-Send resource
//! used on that shard, keyed by IDs the control plane's WorldModel allocated:
//!
//! - `connections`: established transports (`PhysicalConnection`) +
//!   an async session pool per connection. H1 pools hold one slot (the
//!   physical connection serializes requests); H2 pools hold many clones of
//!   the multiplexed sender.
//! - `lanes`: `FileLane`s opened on *this* shard against shared `.part`
//!   files. Shared as `Rc` because compio tasks are thread-local by design.
//!
//! H1 fetches run as Compio tasks. H2 stream work is admitted onto the
//! connection handle and polled from the command loop with the same waker
//! as `recv_async`, so h2 I/O is never a cold sibling of the shard waiter.
//! Cancellation
//! propagates through the job's `JobContext`, which drops the fetch future;
//! a dropped H1 response leaves unread body bytes, so its session slot is
//! intentionally not returned to the pool.

use std::{collections::HashMap, future::Future, rc::Rc, sync::Arc, task::Context};

use crate::core::{
    Error, Result,
    context::JobContext,
    controller::{ConnectFailure, ConnectionState, DestinationFailure, Observation, ProbeFailure},
    disposition::{Disposition, classify_transport_error},
    ids::{AssignmentRef, ConnectionId, JobId},
    ranges::ByteRange,
};
use crate::http::{
    FullBodyFetch, RangeFetch, SourceContext,
    fetch::{ChunkSink, fetch_full_body, fetch_range},
    probe_source,
};
use crate::integrity::hash::Hasher as IntegrityHasher;
use crate::net::h2::H2Handle;
use crate::net::{ConnectTarget, Connector, HttpSession, PhysicalConnection, WireProtocol};
use crate::storage::{
    CapacityCredits, CapacityGuard, DestinationCapacityRegistry, FileDestinationCoordinator,
    FileLane, FlushLevel, MemoryBudget,
};
use flume::{Receiver, Sender};

/// A command from the control loop to this shard. Not `Debug`: the shared
/// destination handle has no meaningful debug rendering and must never be
/// logged.
pub enum ShardCommand {
    OpenConnection {
        connection: ConnectionId,
        target: ConnectTarget,
        /// Dial HTTP/3 over the shard's QUIC endpoint instead of TCP. On
        /// failure the control plane falls back to a TCP dial.
        prefer_h3: bool,
    },
    Probe {
        job: JobId,
        connection: ConnectionId,
        /// Which mirror this probe belongs to; echoed back in observations.
        source_id: crate::core::SourceId,
        source: Arc<SourceContext>,
        context: JobContext,
    },
    StartRange {
        job: JobId,
        assignment: AssignmentRef,
        attempt: u32,
        range: ByteRange,
        overlap: u32,
        is_resume: bool,
        /// Continues the verified contiguous prefix; may consume the memory
        /// budget's forward-progress reserve.
        frontier: bool,
        connection: ConnectionId,
        source: Arc<SourceContext>,
        /// Which mirror serves this assignment; attribution key for
        /// cross-source consistency checks.
        source_id: crate::core::SourceId,
        part_path: std::path::PathBuf,
        context: JobContext,
        rep_lock: std::sync::Arc<std::sync::Mutex<crate::core::RepresentationLock>>,
        provenance: std::sync::Arc<std::sync::Mutex<crate::core::ArtifactProvenance>>,
    },
    StartFullBody {
        job: JobId,
        attempt: u32,
        assignment: AssignmentRef,
        connection: ConnectionId,
        /// Single-stream transfers always advance the frontier.
        frontier: bool,
        source: Arc<SourceContext>,
        part_path: std::path::PathBuf,
        context: JobContext,
        source_id: crate::core::SourceId,
        rep_lock: std::sync::Arc<std::sync::Mutex<crate::core::RepresentationLock>>,
        provenance: std::sync::Arc<std::sync::Mutex<crate::core::ArtifactProvenance>>,
    },
    CloseConnection {
        connection: ConnectionId,
    },
    /// Register the job's shared custom destination on this shard so fetch
    /// tasks can write into it. Broadcast at admission.
    AttachDestination {
        job: JobId,
        dest: Arc<dyn crate::storage::DynDestination + Send + Sync>,
    },
    /// Finalize a shared-destination job: flush, optional digest
    /// verification via read-back, then `commit`.
    CommitSharedDestination {
        job: JobId,
        final_length: u64,
        integrity: Option<crate::core::spec::DigestCheck>,
        ack: Sender<Result<Option<crate::core::spec::Digest>>>,
    },
    /// Drop this shard's lane for `.part` and acknowledge. The control plane
    /// broadcasts this before a commit so no open handle anywhere blocks the
    /// final rename (a hard requirement on Windows).
    CloseLane {
        part_path: std::path::PathBuf,
        ack: Sender<()>,
    },
    /// Stop a running request at `new_end` instead of its original range
    /// end: the scheduler gave its tail to another worker. The fetch observes
    /// this through its sink's `end_offset` and finishes cleanly.
    TruncateAssignment {
        job: JobId,
        assignment: AssignmentRef,
        connection: ConnectionId,
        new_end: u64,
    },
    /// Mirror equivalence sampling: fetch `range` from this mirror's
    /// connection and compare byte-for-byte against what is already in the
    /// destination. No bytes are written.
    SampleMirror {
        job: JobId,
        source_id: crate::core::SourceId,
        connection: ConnectionId,
        range: ByteRange,
        source: Arc<SourceContext>,
        part_path: std::path::PathBuf,
        context: JobContext,
        rep_lock: std::sync::Arc<std::sync::Mutex<crate::core::RepresentationLock>>,
    },
    /// Close this shard's lane (releasing the `.part` handle) and run the
    /// final commit. The coordinator moves here because the rename must
    /// happen only after every handle on this shard is closed - on Windows
    /// a rename with an open writer fails outright.
    CommitDestination {
        job: JobId,
        part_path: std::path::PathBuf,
        coordinator: FileDestinationCoordinator,
        final_length: u64,
        /// Integrity proof required before the rename is performed.
        integrity: Option<crate::core::spec::DigestCheck>,
        durability: crate::core::spec::Durability,
    },
}

enum ConnPool {
    H1 {
        conn: std::cell::RefCell<Option<PhysicalConnection>>,
        event: event_listener::Event,
        dead: std::cell::Cell<bool>,
    },
    Multiplexed {
        conn: std::cell::RefCell<Option<PhysicalConnection>>,
        dead: std::cell::Cell<bool>,
    },
}

impl ConnPool {
    fn new(conn: PhysicalConnection) -> Rc<Self> {
        match conn.protocol() {
            crate::net::WireProtocol::Http1 => Rc::new(Self::H1 {
                conn: std::cell::RefCell::new(Some(conn)),
                event: event_listener::Event::new(),
                dead: std::cell::Cell::new(false),
            }),
            _ => Rc::new(Self::Multiplexed {
                conn: std::cell::RefCell::new(Some(conn)),
                dead: std::cell::Cell::new(false),
            }),
        }
    }

    async fn acquire(self: &Rc<Self>) -> Option<HttpSession> {
        match &**self {
            Self::H1 { conn, event, dead } => {
                if dead.get() {
                    return None;
                }
                loop {
                    if dead.get() {
                        return None;
                    }
                    if let Some(c) = conn.borrow_mut().as_mut()
                        && let Some(s) = c.open_session()
                    {
                        return Some(s);
                    }
                    if conn.borrow().is_none() {
                        return None;
                    }
                    let listener = event.listen();
                    if let Some(c) = conn.borrow_mut().as_mut()
                        && let Some(s) = c.open_session()
                    {
                        return Some(s);
                    }
                    if dead.get() || conn.borrow().is_none() {
                        return None;
                    }
                    listener.await;
                }
            }
            Self::Multiplexed { conn, dead } => {
                if dead.get() {
                    return None;
                }
                if let Some(c) = conn.borrow_mut().as_mut()
                    && let Some(s) = c.open_session()
                {
                    return Some(s);
                }
                None
            }
        }
    }

    fn start_work(self: &Rc<Self>, fut: impl std::future::Future<Output = ()> + 'static) {
        let handle = match &**self {
            Self::Multiplexed { conn, .. } => conn.borrow().as_ref().and_then(|c| c.h2_handle()),
            _ => None,
        };
        if let Some(handle) = handle {
            handle.admit(fut);
            return;
        }
        compio::runtime::spawn(fut).detach();
    }

    fn h2_handle(&self) -> Option<H2Handle> {
        match self {
            Self::Multiplexed { conn, .. } => conn.borrow().as_ref().and_then(|c| c.h2_handle()),
            Self::H1 { .. } => None,
        }
    }

    fn release(self: &Rc<Self>, session: HttpSession) {
        match &**self {
            Self::H1 { conn, event, .. } => {
                if let Some(c) = conn.borrow_mut().as_mut() {
                    c.return_session(session);
                }
                event.notify(1);
            }
            Self::Multiplexed { .. } => {}
        }
    }

    fn retire(self: &Rc<Self>) {
        match &**self {
            Self::H1 { conn, event, dead } => {
                if dead.replace(true) {
                    return;
                }
                event.notify(usize::MAX);
                let taken = conn.borrow_mut().take();
                if let Some(mut c) = taken {
                    compio::runtime::spawn(async move {
                        c.close().await;
                    })
                    .detach();
                }
            }
            Self::Multiplexed { conn, dead } => {
                if dead.replace(true) {
                    return;
                }
                let taken = conn.borrow_mut().take();
                if let Some(mut c) = taken {
                    compio::runtime::spawn(async move {
                        c.close().await;
                    })
                    .detach();
                }
            }
        }
    }

    async fn close(self: &Rc<Self>) {
        match &**self {
            Self::H1 { conn, event, dead } => {
                dead.set(true);
                event.notify(usize::MAX);
                let taken = conn.borrow_mut().take();
                if let Some(mut c) = taken {
                    c.close().await;
                }
            }
            Self::Multiplexed { conn, dead } => {
                dead.set(true);
                let taken = conn.borrow_mut().take();
                if let Some(mut c) = taken {
                    c.close().await;
                }
            }
        }
    }
}

fn poll_h2_pools(
    pools: &std::cell::RefCell<HashMap<ConnectionId, Rc<ConnPool>>>,
    cx: &mut Context<'_>,
) {
    let handles: Vec<H2Handle> = pools
        .borrow()
        .values()
        .filter_map(|p| p.h2_handle())
        .collect();
    for handle in handles {
        let _ = handle.poll_task(cx);
    }
}

/// Live end boundary for a running assignment. The scheduler shrinks it on
/// steal/rebalance; the fetch's sink observes it per chunk so the victim
/// stops exactly at the new boundary instead of duplicating the thief's
/// bytes. `u64::MAX` means "no truncation".
type EndFlag = Arc<std::sync::atomic::AtomicU64>;

fn end_flag_new(end: u64) -> EndFlag {
    Arc::new(std::sync::atomic::AtomicU64::new(end))
}

#[derive(Debug)]
pub struct ShardService {
    rx: Receiver<ShardCommand>,
    obs_tx: Sender<Observation>,
    connector: Arc<Connector>,
    memory: MemoryBudget,
    /// Shared destination-capacity registry: all shards' lanes against one
    /// destination consume from the same credit pool.
    capacity: Arc<DestinationCapacityRegistry>,
}

impl ShardService {
    pub fn new(
        shard_id: usize,
        rx: Receiver<ShardCommand>,
        obs_tx: Sender<Observation>,
        connector: Arc<Connector>,
        memory: MemoryBudget,
        capacity: Arc<DestinationCapacityRegistry>,
    ) -> Self {
        let _ = shard_id;
        Self {
            rx,
            obs_tx,
            connector,
            memory,
            capacity,
        }
    }

    pub async fn run(self) {
        let Self {
            rx,
            obs_tx,
            connector,
            memory,
            capacity,
        } = self;

        let pools: Rc<std::cell::RefCell<HashMap<ConnectionId, Rc<ConnPool>>>> =
            Rc::new(std::cell::RefCell::new(HashMap::new()));
        let mut lanes: HashMap<String, Rc<FileLane>> = HashMap::new();
        let mut shared_dests: HashMap<
            JobId,
            Arc<dyn crate::storage::DynDestination + Send + Sync>,
        > = HashMap::new();
        let quic_endpoint: Rc<std::cell::RefCell<Option<compio_quic::Endpoint>>> =
            Rc::new(std::cell::RefCell::new(None));
        // Live end boundaries for running assignments, observed by their
        // sinks; updated by TruncateAssignment. Rc/RefCell because tasks and
        // this loop share one shard thread.
        let end_flags: Rc<std::cell::RefCell<HashMap<AssignmentRef, EndFlag>>> =
            Rc::new(std::cell::RefCell::new(HashMap::new()));

        loop {
            let recv = rx.recv_async();
            let mut recv = std::pin::pin!(recv);
            let cmd = match std::future::poll_fn(|cx| {
                poll_h2_pools(&pools, cx);
                recv.as_mut().poll(cx)
            })
            .await
            {
                Ok(cmd) => cmd,
                Err(_) => break,
            };
            match cmd {
                ShardCommand::OpenConnection {
                    connection,
                    target,
                    prefer_h3,
                } => {
                    let pools = pools.clone();
                    let obs = obs_tx.clone();
                    let connector = connector.clone();
                    let quic_endpoint = quic_endpoint.clone();
                    compio::runtime::spawn(async move {
                        let res = if prefer_h3 {
                            if quic_endpoint.borrow().is_none() {
                                match compio_quic::Endpoint::client(("0.0.0.0", 0)).await {
                                    Ok(ep) => {
                                        quic_endpoint.borrow_mut().replace(ep);
                                    }
                                    Err(e) => {
                                        tracing::error!(
                                            target: "xde::net",
                                            ?connection,
                                            error = %e,
                                            "quic endpoint bind failed"
                                        );
                                        let _ = obs.send(Observation::ConnectionFailed {
                                            connection,
                                            kind: ConnectFailure::Other,
                                        });
                                        return;
                                    }
                                }
                            }
                            let endpoint = quic_endpoint
                                .borrow()
                                .as_ref()
                                .expect("just created")
                                .clone();
                            connector.connect_h3(&endpoint, &target).await
                        } else {
                            connector.connect(&target).await
                        };
                        match res {
                            Ok((conn, metrics)) => {
                                let pool = ConnPool::new(conn);
                                pools.borrow_mut().insert(connection, pool);
                                let _ = obs.send(Observation::ConnectionReady {
                                    connection,
                                    protocol: crate::core::events::Protocol::from(metrics.protocol),
                                    handshake: metrics.tcp_handshake + metrics.tls_handshake,
                                });
                            }
                            Err(e) => {
                                let _ = obs.send(Observation::ConnectionFailed {
                                    connection,
                                    kind: connect_failure_kind(&e),
                                });
                            }
                        }
                    })
                    .detach();
                }

                ShardCommand::Probe {
                    job,
                    connection,
                    source_id,
                    source,
                    context,
                } => {
                    let Some(pool) = pools.borrow().get(&connection).cloned() else {
                        let _ = obs_tx.send(Observation::ProbeFailed {
                            job,
                            source: source_id,
                            connection,
                            failure: ProbeFailure {
                                kind: crate::core::controller::ProbeFailureKind::Transport,
                                status: 0,
                            },
                            connection_state: ConnectionState::Gone,
                        });
                        continue;
                    };
                    let obs_tx = obs_tx.clone();
                    pool.clone().start_work(async move {
                        let probe_res = run_probe(pool, &source, &context).await;
                        let obs = match probe_res {
                            Ok(ProbeReport::Ready(probe, session_alive)) => Observation::Probed {
                                job,
                                source: source_id,
                                connection,
                                supports_ranges: probe.supports_ranges,
                                total_length: probe.total_length,
                                etag: probe.fingerprint.etag.as_ref().map(|e| e.to_string()),
                                last_modified: probe.fingerprint.last_modified,
                                reusable: session_alive && !probe.compressed,
                                alt_svc_h3: probe.alt_svc_h3_port,
                            },
                            Ok(ProbeReport::Redirected { status, location }) => {
                                Observation::ProbeRedirected {
                                    job,
                                    connection,
                                    status,
                                    location: crate::core::controller::RedirectTarget {
                                        url: location.to_string(),
                                    },
                                }
                            }
                            Err(e) => {
                                let state = if e.is_destination_error() {
                                    ConnectionState::Reusable
                                } else {
                                    ConnectionState::Poisoned
                                };
                                Observation::ProbeFailed {
                                    job,
                                    source: source_id,
                                    connection,
                                    failure: ProbeFailure::from_error(&e),
                                    connection_state: state,
                                }
                            }
                        };
                        let _ = obs_tx.send(obs);
                    });
                }

                ShardCommand::StartRange {
                    job,
                    assignment,
                    attempt,
                    range,
                    overlap,
                    is_resume,
                    frontier,
                    connection,
                    source,
                    source_id,
                    part_path,
                    context,
                    rep_lock,
                    provenance,
                } => {
                    // Shared-destination jobs bypass lanes entirely.
                    if let Some(dest) = shared_dests.get(&job).cloned() {
                        let Some(pool) = pools.borrow().get(&connection).cloned() else {
                            let _ = obs_tx.send(Observation::AssignmentFailed {
                                job,
                                assignment,
                                attempt,
                                disposition: Disposition::RetrySameRange {
                                    after: None,
                                    reason: "unknown connection",
                                },
                                connection: None,
                                connection_state: ConnectionState::Gone,
                                stream_health: crate::core::controller::StreamHealth::Failed,
                            });
                            continue;
                        };
                        let credits = capacity.entry(&format!("shared-{job:?}"));
                        let obs_tx = obs_tx.clone();
                        let end_flag = end_flag_new(range.end);
                        let aref = assignment;
                        end_flags.borrow_mut().insert(aref, end_flag.clone());
                        let memory = memory.clone();
                        let task_flags = end_flags.clone();
                        pool.clone().start_work(async move {
                            let outcome = run_shared(
                                pool, &*dest, &credits, &source, range, overlap, is_resume,
                                &context, &memory, frontier, &end_flag, rep_lock, provenance,
                                source_id,
                            )
                            .await;
                            task_flags.borrow_mut().remove(&aref);
                            let obs = match &outcome {
                                Ok(o) => Observation::AssignmentVerified {
                                    job,
                                    assignment,
                                    range: ByteRange::new(
                                        range.start,
                                        range.start + o.sample.bytes,
                                    ),
                                    sample: o.sample,
                                    connection,
                                    connection_reusable: o.connection_reusable,
                                },
                                Err(e) => {
                                    tracing::debug!(
                                        target: "xde::engine",
                                        ?job,
                                        ?assignment,
                                        ?connection,
                                        error = ?e,
                                        "shared assignment failed"
                                    );
                                    Observation::AssignmentFailed {
                                        job,
                                        assignment,
                                        attempt,
                                        disposition: classify_transport_error(&e.error),
                                        connection: Some(connection),
                                        connection_state: e.connection_state,
                                        stream_health:
                                            crate::core::controller::StreamHealth::Failed,
                                    }
                                }
                            };
                            let _ = obs_tx.send(obs);
                        });
                        continue;
                    }
                    let pool = pools.borrow().get(&connection).cloned();
                    let Some(pool) = pool else {
                        let _ = obs_tx.send(Observation::AssignmentFailed {
                            job,
                            assignment,
                            attempt: 0,
                            disposition: Disposition::RetrySameRange {
                                after: None,
                                reason: "unknown connection",
                            },
                            connection: None,
                            connection_state: ConnectionState::Gone,
                            stream_health: crate::core::controller::StreamHealth::Failed,
                        });
                        continue;
                    };
                    let lane_key = part_path.to_string_lossy().into_owned();
                    let lane = match lanes.get(&lane_key) {
                        Some(l) => l.clone(),
                        None => {
                            let opened = FileLane::open(part_path.to_path_buf()).await;
                            match opened {
                                Ok(l) => {
                                    let l = Rc::new(l);
                                    lanes.insert(lane_key.clone(), l.clone());
                                    l
                                }
                                Err(e) => {
                                    let _ = obs_tx.send(Observation::AssignmentFailed {
                                        job,
                                        assignment,
                                        attempt: 0,
                                        disposition: Disposition::Fatal {
                                            status: 0,
                                            reason: "destination unavailable",
                                        },
                                        connection: Some(connection),
                                        connection_state: ConnectionState::Poisoned,
                                        stream_health:
                                            crate::core::controller::StreamHealth::Failed,
                                    });
                                    let _ = e;
                                    continue;
                                }
                            }
                        }
                    };
                    let credits = capacity.entry(&lane_key);
                    let obs_tx = obs_tx.clone();
                    let memory = memory.clone();
                    let end_flag = end_flag_new(range.end);
                    let aref = assignment;
                    end_flags.borrow_mut().insert(aref, end_flag.clone());
                    let task_flags = end_flags.clone();
                    pool.clone().start_work(async move {
                        let outcome = run_range(
                            pool, &lane, &source, range, overlap, is_resume, &context, &memory,
                            &credits, frontier, &end_flag, source_id, rep_lock, provenance,
                        )
                        .await;
                        task_flags.borrow_mut().remove(&aref);
                        let obs = match outcome {
                            Ok(o) => {
                                let verified_range =
                                    ByteRange::new(range.start, range.start + o.sample.bytes);
                                Observation::AssignmentVerified {
                                    job,
                                    assignment,
                                    range: verified_range,
                                    sample: o.sample,
                                    connection,
                                    connection_reusable: o.connection_reusable,
                                }
                            }
                            Err(e) => {
                                tracing::debug!(
                                    target: "xde::engine",
                                    ?job,
                                    ?assignment,
                                    ?connection,
                                    error = ?e.error,
                                    "assignment failed"
                                );
                                Observation::AssignmentFailed {
                                    job,
                                    assignment,
                                    attempt,
                                    disposition: classify_transport_error(&e.error),
                                    connection: Some(connection),
                                    connection_state: e.connection_state,
                                    stream_health: crate::core::controller::StreamHealth::Failed,
                                }
                            }
                        };
                        let _ = obs_tx.send(obs);
                    });
                }

                ShardCommand::StartFullBody {
                    job,
                    attempt,
                    assignment,
                    connection,
                    frontier,
                    source,
                    part_path,
                    context,
                    source_id,
                    rep_lock,
                    provenance,
                } => {
                    // Shared-destination jobs bypass lanes entirely. A
                    // full-body fetch into a custom sink runs single-stream
                    // from offset 0 to EOF; the end flag stays at u64::MAX.
                    if let Some(dest) = shared_dests.get(&job).cloned() {
                        let Some(pool) = pools.borrow().get(&connection).cloned() else {
                            let _ = obs_tx.send(Observation::AssignmentFailed {
                                job,
                                assignment,
                                attempt,
                                disposition: Disposition::RetrySameRange {
                                    after: None,
                                    reason: "unknown connection",
                                },
                                connection: None,
                                connection_state: ConnectionState::Gone,
                                stream_health: crate::core::controller::StreamHealth::Failed,
                            });
                            continue;
                        };
                        let credits = capacity.entry(&format!("shared-{job:?}"));
                        let obs_tx = obs_tx.clone();
                        let end_flag = end_flag_new(u64::MAX);
                        let aref = assignment;
                        end_flags.borrow_mut().insert(aref, end_flag.clone());
                        let memory = memory.clone();
                        let task_flags = end_flags.clone();
                        pool.clone().start_work(async move {
                            let outcome = run_shared_full_body(
                                pool, &*dest, &credits, &source, &context, &memory, frontier,
                                &end_flag, rep_lock, provenance, source_id,
                            )
                            .await;
                            task_flags.borrow_mut().remove(&aref);
                            let obs = match &outcome {
                                Ok(o) => Observation::AssignmentVerified {
                                    job,
                                    assignment,
                                    range: ByteRange::new(0, o.sample.bytes),
                                    sample: o.sample,
                                    connection,
                                    connection_reusable: o.connection_reusable,
                                },
                                Err(e) => {
                                    tracing::debug!(
                                        target: "xde::engine",
                                        ?job,
                                        ?assignment,
                                        ?connection,
                                        error = ?e,
                                        "shared assignment failed"
                                    );
                                    Observation::AssignmentFailed {
                                        job,
                                        assignment,
                                        attempt,
                                        disposition: classify_transport_error(&e.error),
                                        connection: Some(connection),
                                        connection_state: e.connection_state,
                                        stream_health:
                                            crate::core::controller::StreamHealth::Failed,
                                    }
                                }
                            };
                            let _ = obs_tx.send(obs);
                        });
                        continue;
                    }
                    let pool = pools.borrow().get(&connection).cloned();
                    let Some(pool) = pool else {
                        let _ = obs_tx.send(Observation::AssignmentFailed {
                            job,
                            assignment,
                            attempt: 0,
                            disposition: Disposition::RetrySameRange {
                                after: None,
                                reason: "unknown connection",
                            },
                            connection: None,
                            connection_state: ConnectionState::Gone,
                            stream_health: crate::core::controller::StreamHealth::Failed,
                        });
                        continue;
                    };
                    let lane_key = part_path.to_string_lossy().into_owned();
                    let lane = match lanes.get(&lane_key) {
                        Some(l) => l.clone(),
                        None => {
                            let opened = FileLane::open(part_path.to_path_buf()).await;
                            match opened {
                                Ok(l) => {
                                    let l = Rc::new(l);
                                    lanes.insert(lane_key.clone(), l.clone());
                                    l
                                }
                                Err(e) => {
                                    let _ = obs_tx.send(Observation::AssignmentFailed {
                                        job,
                                        assignment,
                                        attempt: 0,
                                        disposition: Disposition::Fatal {
                                            status: 0,
                                            reason: "destination unavailable",
                                        },
                                        connection: Some(connection),
                                        connection_state: ConnectionState::Poisoned,
                                        stream_health:
                                            crate::core::controller::StreamHealth::Failed,
                                    });
                                    let _ = e;
                                    continue;
                                }
                            }
                        }
                    };
                    let credits = capacity.entry(&lane_key);
                    let obs_tx = obs_tx.clone();
                    let memory = memory.clone();
                    let end_flag = end_flag_new(u64::MAX);
                    let aref = assignment;
                    end_flags.borrow_mut().insert(aref, end_flag.clone());
                    let task_flags = end_flags.clone();
                    pool.clone().start_work(async move {
                        let fetch = FullBodyFetch { source };
                        let outcome = run_full_body(
                            pool, &fetch, &lane, &context, &memory, &credits, frontier, &end_flag,
                            rep_lock, provenance, source_id,
                        )
                        .await;
                        task_flags.borrow_mut().remove(&aref);
                        let obs = match outcome {
                            Ok(o) => Observation::AssignmentVerified {
                                job,
                                assignment,
                                range: ByteRange::new(0, o.sample.bytes),
                                sample: o.sample,
                                connection,
                                connection_reusable: o.connection_reusable,
                            },
                            Err(e) => {
                                tracing::debug!(
                                    target: "xde::engine",
                                    ?job,
                                    ?assignment,
                                    ?connection,
                                    error = ?e,
                                    "assignment failed"
                                );
                                Observation::AssignmentFailed {
                                    job,
                                    assignment,
                                    attempt,
                                    disposition: classify_transport_error(&e.error),
                                    connection: Some(connection),
                                    connection_state: e.connection_state,
                                    stream_health: crate::core::controller::StreamHealth::Failed,
                                }
                            }
                        };
                        let _ = obs_tx.send(obs);
                    });
                }

                ShardCommand::SampleMirror {
                    job,
                    source_id,
                    connection,
                    range,
                    source,
                    part_path,
                    context,
                    rep_lock,
                } => {
                    let Some(pool) = pools.borrow().get(&connection).cloned() else {
                        let _ = obs_tx.send(Observation::MirrorSampled {
                            job,
                            source: source_id,
                            connection,
                            matches: false,
                            reusable: false,
                        });
                        continue;
                    };
                    let lane = match lanes.get(&part_path.to_string_lossy().into_owned()) {
                        Some(l) => l.clone(),
                        None => match FileLane::open(part_path.clone()).await {
                            Ok(l) => {
                                let l = Rc::new(l);
                                lanes.insert(part_path.to_string_lossy().into_owned(), l.clone());
                                l
                            }
                            Err(_) => {
                                let _ = obs_tx.send(Observation::MirrorSampled {
                                    job,
                                    source: source_id,
                                    connection,
                                    matches: false,
                                    reusable: false,
                                });
                                continue;
                            }
                        },
                    };
                    let obs_tx = obs_tx.clone();
                    pool.clone().start_work(async move {
                        let outcome = run_mirror_sample(
                            pool, &lane, &source, range, &context, source_id, rep_lock,
                        )
                        .await;
                        let (matches, reusable) = match outcome {
                            Ok(o) => (o.matches, o.reusable),
                            Err(_) => (false, false),
                        };
                        let _ = obs_tx.send(Observation::MirrorSampled {
                            job,
                            source: source_id,
                            connection,
                            matches,
                            reusable,
                        });
                    });
                }

                ShardCommand::CloseLane { part_path, ack } => {
                    lanes.remove(&part_path.to_string_lossy().into_owned());
                    let _ = ack.send(());
                }

                ShardCommand::TruncateAssignment {
                    job: _,
                    assignment,
                    new_end,
                    ..
                } => {
                    if let Some(flag) = end_flags.borrow().get(&assignment) {
                        flag.store(new_end, std::sync::atomic::Ordering::Relaxed);
                    }
                }

                ShardCommand::CommitDestination {
                    job,
                    part_path,
                    coordinator,
                    final_length,
                    integrity,
                    durability,
                } => {
                    // Close this shard's lane so no handle keeps the `.part`
                    // open during the rename.
                    lanes.remove(&part_path.to_string_lossy().into_owned());
                    let result = coordinator
                        .finalize(final_length, integrity, durability)
                        .await;
                    let obs = match result {
                        Ok(digest) => Observation::DestinationCommitted {
                            job,
                            final_length,
                            digest,
                        },
                        Err(e) => Observation::DestinationFailed {
                            job,
                            failure: DestinationFailure::from_error(&e),
                        },
                    };
                    let _ = obs_tx.send(obs);
                }

                ShardCommand::AttachDestination { job, dest } => {
                    shared_dests.insert(job, dest);
                }

                ShardCommand::CommitSharedDestination {
                    job,
                    final_length,
                    integrity,
                    ack,
                } => {
                    let outcome = match shared_dests.get(&job) {
                        Some(dest) => commit_shared(dest.as_ref(), final_length, integrity).await,
                        None => Err(crate::core::Error::destination(
                            "shared destination missing at commit",
                        )),
                    };
                    shared_dests.remove(&job);
                    let _ = ack.send(outcome);
                }

                ShardCommand::CloseConnection { connection } => {
                    let pool = pools.borrow_mut().remove(&connection);
                    if let Some(pool) = pool {
                        pool.close().await;
                    }
                }
            }
        }

        let pools_drain: Vec<_> = pools.borrow_mut().drain().collect();
        for (_, pool) in pools_drain {
            pool.close().await;
        }
        lanes.clear();
    }
}

async fn run_probe(
    pool: Rc<ConnPool>,
    source: &Arc<SourceContext>,
    context: &JobContext,
) -> Result<ProbeReport> {
    let Some(mut session) = pool.acquire().await else {
        return Err(crate::core::Error::Transport(
            crate::core::error::TransportError::ConnectionRetired {
                reason: "pool retired".into(),
            },
        ));
    };
    let outcome = context
        .run(async {
            probe_source(
                &mut session,
                &source.url,
                &source.extra_headers,
                source.allow_compressed,
                source.deadline,
            )
            .await
        })
        .await;
    // A cleanly drained probe (tiny 206 body, redirect response) leaves the
    // session reusable regardless of protocol. Anything else - full 200
    // body left unread, Connection: close, transport error, cancellation -
    // makes the H1 slot unrecoverable and the whole pool is retired.
    let reusable = matches!(&outcome,
        Ok(crate::http::ProbeOutcome::Ready(p)) if p.connection_reusable && !p.compressed
    ) || matches!(
        &outcome,
        Ok(crate::http::ProbeOutcome::Redirect {
            connection_close: false,
            ..
        })
    );
    if reusable {
        pool.release(session);
    } else if !context.is_cancelled() {
        pool.retire();
    }
    match outcome? {
        crate::http::ProbeOutcome::Ready(p) => Ok(ProbeReport::Ready(Box::new(*p), reusable)),
        crate::http::ProbeOutcome::Redirect {
            status, location, ..
        } => Ok(ProbeReport::Redirected { status, location }),
    }
}

/// What one probe attempt produced.
enum ProbeReport {
    Ready(Box<crate::http::probe::ProbeResult>, bool),
    Redirected { status: u16, location: url::Url },
}

#[allow(clippy::too_many_arguments)]
async fn run_range(
    pool: Rc<ConnPool>,
    lane: &Rc<FileLane>,
    source: &Arc<SourceContext>,
    range: ByteRange,
    overlap: u32,
    is_resume: bool,
    context: &JobContext,
    memory: &MemoryBudget,
    credits: &Arc<CapacityCredits>,
    frontier: bool,
    end_flag: &EndFlag,
    source_id: crate::core::SourceId,
    rep_lock: std::sync::Arc<std::sync::Mutex<crate::core::RepresentationLock>>,
    provenance: std::sync::Arc<std::sync::Mutex<crate::core::ArtifactProvenance>>,
) -> std::result::Result<crate::http::fetch::RangeFetchOutcome, RangeRunError> {
    let Some(mut session) = pool.acquire().await else {
        return Err(RangeRunError {
            error: crate::core::Error::Transport(
                crate::core::error::TransportError::ConnectionRetired {
                    reason: "pool retired".into(),
                },
            ),
            connection_state: ConnectionState::Gone,
        });
    };
    let fetch = RangeFetch {
        source: source.clone(),
        range,
        overlap,
        is_resume,
    };
    let crash_safe = matches!(
        context.durability(),
        crate::core::spec::Durability::CrashSafe
    );
    let mut sink = LaneSink {
        lane: lane.clone(),
        capacity: credits.clone(),
        frontier,
        crash_safe,
        dynamic_end: Some(end_flag.clone()),
        provenance,
        source_id,
    };
    let outcome = context
        .run(async {
            fetch_range(
                &mut session,
                memory,
                &fetch,
                &mut sink,
                context,
                rep_lock.as_ref(),
            )
            .await
        })
        .await;
    // Release on clean completion; any other ending (truncation, error,
    // cancellation) leaves the H1 slot unrecoverable and retires the pool.
    let protocol = session.protocol();
    let connection_state = match outcome.as_ref() {
        Ok(result) if result.connection_reusable => ConnectionState::Reusable,
        Ok(_) => ConnectionState::Poisoned,
        Err(e) => failure_connection_state(protocol, e),
    };
    if connection_state == ConnectionState::Reusable {
        pool.release(session);
    } else {
        pool.retire();
    }
    outcome.map_err(|error| RangeRunError {
        error,
        connection_state,
    })
}

#[derive(Debug)]
struct RangeRunError {
    error: crate::core::Error,
    connection_state: ConnectionState,
}

/// Classify the physical connection independently from the failed logical
/// stream. H2/H3 status, EOF, overlap, and destination errors end one stream
/// but do not retire sibling streams; socket/protocol failures still retire
/// the physical connection.
fn failure_connection_state(protocol: WireProtocol, error: &crate::core::Error) -> ConnectionState {
    use crate::core::error::{Error, HttpError, TransportError};
    if protocol == WireProtocol::Http1 {
        return ConnectionState::Poisoned;
    }
    match error {
        Error::Transport(TransportError::ConnectionRetired { .. }) => ConnectionState::Gone,
        Error::Transport(
            TransportError::Io(_) | TransportError::ConnectTimeout | TransportError::Tls(_),
        )
        | Error::Http(HttpError::Protocol(_)) => ConnectionState::Poisoned,
        _ => ConnectionState::Reusable,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_full_body(
    pool: Rc<ConnPool>,
    fetch: &FullBodyFetch,
    lane: &Rc<FileLane>,
    context: &JobContext,
    memory: &MemoryBudget,
    credits: &Arc<CapacityCredits>,
    frontier: bool,
    end_flag: &EndFlag,
    rep_lock: std::sync::Arc<std::sync::Mutex<crate::core::RepresentationLock>>,
    provenance: std::sync::Arc<std::sync::Mutex<crate::core::ArtifactProvenance>>,
    source_id: crate::core::SourceId,
) -> std::result::Result<crate::http::fetch::FullBodyFetchOutcome, RangeRunError> {
    let Some(mut session) = pool.acquire().await else {
        return Err(RangeRunError {
            error: crate::core::Error::Transport(
                crate::core::error::TransportError::ConnectionRetired {
                    reason: "pool retired".into(),
                },
            ),
            connection_state: ConnectionState::Gone,
        });
    };
    let crash_safe = matches!(
        context.durability(),
        crate::core::spec::Durability::CrashSafe
    );
    let mut sink = LaneSink {
        lane: lane.clone(),
        capacity: credits.clone(),
        frontier,
        crash_safe,
        dynamic_end: Some(end_flag.clone()),
        provenance,
        source_id,
    };
    let outcome = context
        .run(async {
            fetch_full_body(
                &mut session,
                memory,
                fetch,
                &mut sink,
                context,
                rep_lock.as_ref(),
            )
            .await
        })
        .await;
    let protocol = session.protocol();
    let connection_state = match outcome.as_ref() {
        Ok(result) if result.connection_reusable => ConnectionState::Reusable,
        Ok(_) => ConnectionState::Poisoned,
        Err(e) => failure_connection_state(protocol, e),
    };
    if connection_state == ConnectionState::Reusable {
        pool.release(session);
    } else {
        pool.retire();
    }
    outcome.map_err(|error| RangeRunError {
        error,
        connection_state,
    })
}

struct LaneSink {
    lane: Rc<FileLane>,
    /// Whether per-piece fsync is required (CrashSafe durability only).
    crash_safe: bool,
    /// Shared destination-capacity pool for this `.part`; every write holds
    /// credits while in flight, so N shard lanes respect one ceiling.
    capacity: Arc<CapacityCredits>,
    /// Whether this assignment advances the artifact frontier.
    frontier: bool,
    /// Live scheduler boundary. `Some` while the assignment runs; the fetch
    /// stops at this offset when the tail is stolen or rebalanced.
    dynamic_end: Option<EndFlag>,
    /// Engine-wide first-writer provenance for this artifact. Every shard
    /// receives the same registry, so mirror writes cannot bypass one another.
    provenance: std::sync::Arc<std::sync::Mutex<crate::core::ArtifactProvenance>>,
    /// The mirror this assignment's bytes come from.
    source_id: crate::core::SourceId,
}

impl LaneSink {
    /// Compare the already-written portion of this chunk against what this
    /// source delivers. Blocks first written by ANOTHER source are the
    /// attribution boundary: a disagreement quarantines THIS source.
    async fn check_cross_source(&mut self, chunk_offset: u64, bytes: &[u8]) -> Result<()> {
        let chunk_range = ByteRange::new(chunk_offset, chunk_offset + bytes.len() as u64);
        let conflicts = self
            .provenance
            .lock()
            .map_err(|_| crate::core::Error::protocol("artifact provenance poisoned"))?
            .foreign_spans(chunk_range, self.source_id);
        for conflict in conflicts {
            let existing = self
                .lane
                .read_back(conflict.start, conflict.len() as usize)
                .await?;
            let mine =
                &bytes[(conflict.start - chunk_offset) as usize..][..conflict.len() as usize];
            if existing.as_slice() != mine {
                return Err(crate::core::Error::OverlapMismatch {
                    offset: conflict.start,
                });
            }
        }
        Ok(())
    }
    fn record_written(&mut self, chunk_offset: u64, len: usize) {
        if let Ok(mut provenance) = self.provenance.lock() {
            provenance.record(
                ByteRange::new(chunk_offset, chunk_offset + len as u64),
                self.source_id,
            );
        }
    }
}

impl ChunkSink for LaneSink {
    async fn accept(&mut self, chunk: crate::storage::TransferChunk) -> Result<()> {
        let need_cross = self
            .provenance
            .lock()
            .map(|p| p.has_multiple_writers())
            .unwrap_or(false);
        if need_cross {
            self.check_cross_source(chunk.offset(), chunk.as_slice())
                .await?;
        }
        let offset = chunk.offset();
        let n = chunk.len();
        let guard = CapacityGuard::acquire(&self.capacity, n as u64).await;
        self.lane.write_chunk(chunk).await?;
        drop(guard);
        self.record_written(offset, n);
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        if self.crash_safe {
            self.lane.flush(FlushLevel::Data).await
        } else {
            Ok(())
        }
    }

    fn end_offset(&self) -> Option<u64> {
        self.dynamic_end
            .as_ref()
            .map(|f| f.load(std::sync::atomic::Ordering::Relaxed))
    }

    fn progress_class(&self, _offset: u64) -> crate::storage::ProgressClass {
        if self.frontier {
            crate::storage::ProgressClass::Frontier
        } else {
            crate::storage::ProgressClass::Speculative
        }
    }
}

/// What one mirror sampling attempt produced.
struct MirrorSampleOutcome {
    matches: bool,
    reusable: bool,
}

/// Fetch `range` from the mirror and compare every byte against the
/// destination's existing content. Nothing is written.
async fn run_mirror_sample(
    pool: Rc<ConnPool>,
    lane: &Rc<FileLane>,
    source: &Arc<SourceContext>,
    range: ByteRange,
    context: &JobContext,
    _source_id: crate::core::SourceId,
    rep_lock: std::sync::Arc<std::sync::Mutex<crate::core::RepresentationLock>>,
) -> Result<MirrorSampleOutcome> {
    let Some(mut session) = pool.acquire().await else {
        return Err(crate::core::Error::Transport(
            crate::core::error::TransportError::ConnectionRetired {
                reason: "pool retired".into(),
            },
        ));
    };
    let fetch = RangeFetch {
        source: source.clone(),
        range,
        overlap: 0,
        is_resume: false,
    };

    let mut sink = SampleSink { lane: lane.clone() };
    let outcome = context
        .run(async {
            fetch_range(
                &mut session,
                &MemoryBudget::new(u64::MAX),
                &fetch,
                &mut sink,
                context,
                rep_lock.as_ref(),
            )
            .await
        })
        .await;
    let reusable = outcome.as_ref().is_ok_and(|o| o.connection_reusable);
    if reusable {
        pool.release(session);
    } else if !context.is_cancelled() {
        pool.retire();
    }
    Ok(MirrorSampleOutcome {
        matches: outcome.is_ok(),
        reusable,
    })
}

/// Read-only sink that verifies mirror bytes against destination content.
struct SampleSink {
    lane: Rc<FileLane>,
}

impl ChunkSink for SampleSink {
    async fn accept(&mut self, chunk: crate::storage::TransferChunk) -> Result<()> {
        let offset = chunk.offset();
        let len = chunk.len();
        let existing = self.lane.read_back(offset, len).await?;
        if existing.as_slice() != chunk.as_slice() {
            return Err(crate::core::Error::OverlapMismatch { offset });
        }
        Ok(())
    }

    fn progress_class(&self, _offset: u64) -> crate::storage::ProgressClass {
        crate::storage::ProgressClass::Speculative
    }
}

/// Map a connect error onto the typed failure the controller reasons about.
fn connect_failure_kind(e: &crate::core::Error) -> ConnectFailure {
    use crate::core::error::{Error, TransportError};
    match e {
        Error::DeadlineExceeded => ConnectFailure::Timeout,
        Error::Transport(TransportError::ConnectTimeout) => ConnectFailure::Timeout,
        Error::Transport(TransportError::Tls(_)) => ConnectFailure::Tls,
        Error::Transport(TransportError::Io(io)) => match io.kind() {
            std::io::ErrorKind::ConnectionRefused => ConnectFailure::Refused,
            _ => ConnectFailure::Other,
        },
        _ => ConnectFailure::Other,
    }
}

/// What one shared-destination assignment produced.
struct SharedOutcome {
    sample: crate::core::metrics::TransferSample,
    connection_reusable: bool,
}

/// Execute one ranged fetch writing into a SHARED custom destination.
/// Mirrors [`run_range`]'s measurement semantics, but the sink calls
/// `DynDestination::write_chunk_dyn` instead of a file lane. Cross-source
/// overlap verification does not apply here; correctness is enforced at
/// commit via read-back digest when the destination supports READ_BACK.
#[allow(clippy::too_many_arguments)]
async fn run_shared(
    pool: Rc<ConnPool>,
    dest: &dyn crate::storage::DynDestination,
    credits: &Arc<CapacityCredits>,
    source: &Arc<SourceContext>,
    range: ByteRange,
    overlap: u32,
    is_resume: bool,
    context: &JobContext,
    memory: &MemoryBudget,
    frontier: bool,
    end_flag: &EndFlag,
    rep_lock: std::sync::Arc<std::sync::Mutex<crate::core::RepresentationLock>>,
    provenance: std::sync::Arc<std::sync::Mutex<crate::core::ArtifactProvenance>>,
    source_id: crate::core::SourceId,
) -> std::result::Result<SharedOutcome, RangeRunError> {
    let Some(mut session) = pool.acquire().await else {
        return Err(RangeRunError {
            error: crate::core::Error::Transport(
                crate::core::error::TransportError::ConnectionRetired {
                    reason: "pool retired".into(),
                },
            ),
            connection_state: ConnectionState::Gone,
        });
    };
    let fetch = RangeFetch {
        source: source.clone(),
        range,
        overlap,
        is_resume,
    };
    let mut sink = SharedSink {
        dest,
        credits: credits.clone(),
        frontier,
        end_flag: end_flag.clone(),
        provenance: Some(provenance),
        source_id,
    };

    let outcome = context
        .run(async {
            fetch_range(
                &mut session,
                memory,
                &fetch,
                &mut sink,
                context,
                rep_lock.as_ref(),
            )
            .await
        })
        .await;

    // H2/H3 senders are clone-based and survive clean completions; anything
    // else poisons the pool exactly like the file path.
    let protocol = session.protocol();
    let connection_state = match outcome.as_ref() {
        Ok(result) if result.connection_reusable => ConnectionState::Reusable,
        Ok(_) => ConnectionState::Poisoned,
        Err(e) => failure_connection_state(protocol, e),
    };
    if connection_state == ConnectionState::Reusable {
        pool.release(session);
    } else if !context.is_cancelled() {
        pool.retire();
    }
    outcome
        .map(|o| SharedOutcome {
            sample: o.sample,
            connection_reusable: o.connection_reusable,
        })
        .map_err(|error| RangeRunError {
            error,
            connection_state,
        })
}

/// Sink that writes into an application-provided destination. Destination
/// capacity credits bound in-flight bytes - a slow sink blocks HERE, which
/// makes backpressure observable as `destination_blocked` instead of
/// unbounded buffering.
struct SharedSink<'a> {
    dest: &'a dyn crate::storage::DynDestination,
    credits: Arc<CapacityCredits>,
    frontier: bool,
    end_flag: EndFlag,
    provenance: Option<std::sync::Arc<std::sync::Mutex<crate::core::ArtifactProvenance>>>,
    source_id: crate::core::SourceId,
}

impl ChunkSink for SharedSink<'_> {
    async fn accept(&mut self, chunk: crate::storage::TransferChunk) -> Result<()> {
        if let Some(provenance) = &self.provenance {
            let range = ByteRange::new(chunk.offset(), chunk.offset() + chunk.len() as u64);
            let conflicts = provenance
                .lock()
                .map_err(|_| crate::core::Error::protocol("artifact provenance poisoned"))?
                .foreign_spans(range, self.source_id);
            for conflict in conflicts {
                let existing = self
                    .dest
                    .read_back_dyn(conflict.start, conflict.len() as usize)
                    .await?;
                let start = (conflict.start - chunk.offset()) as usize;
                if existing.as_slice() != &chunk.as_slice()[start..][..conflict.len() as usize] {
                    return Err(crate::core::Error::OverlapMismatch {
                        offset: conflict.start,
                    });
                }
            }
        }
        let guard = CapacityGuard::acquire(&self.credits, chunk.len() as u64).await;
        let completion = self.dest.write_chunk_dyn(chunk).await?;
        drop(guard);
        if let Some(provenance) = &self.provenance
            && let Ok(mut provenance) = provenance.lock()
        {
            provenance.record(completion.range, self.source_id);
        }
        Ok(())
    }

    fn end_offset(&self) -> Option<u64> {
        Some(self.end_flag.load(std::sync::atomic::Ordering::Relaxed))
    }

    fn progress_class(&self, _offset: u64) -> crate::storage::ProgressClass {
        if self.frontier {
            crate::storage::ProgressClass::Frontier
        } else {
            crate::storage::ProgressClass::Speculative
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_shared_full_body(
    pool: Rc<ConnPool>,
    dest: &dyn crate::storage::DynDestination,
    credits: &Arc<CapacityCredits>,
    source: &Arc<SourceContext>,
    context: &JobContext,
    memory: &MemoryBudget,
    frontier: bool,
    end_flag: &EndFlag,
    rep_lock: std::sync::Arc<std::sync::Mutex<crate::core::RepresentationLock>>,
    provenance: std::sync::Arc<std::sync::Mutex<crate::core::ArtifactProvenance>>,
    source_id: crate::core::SourceId,
) -> std::result::Result<SharedOutcome, RangeRunError> {
    let Some(mut session) = pool.acquire().await else {
        return Err(RangeRunError {
            error: crate::core::Error::Transport(
                crate::core::error::TransportError::ConnectionRetired {
                    reason: "pool retired".into(),
                },
            ),
            connection_state: ConnectionState::Gone,
        });
    };
    let fetch = FullBodyFetch {
        source: source.clone(),
    };
    let mut sink = SharedSink {
        dest,
        credits: credits.clone(),
        frontier,
        end_flag: end_flag.clone(),
        provenance: Some(provenance),
        source_id,
    };
    let outcome = context
        .run(async {
            fetch_full_body(
                &mut session,
                memory,
                &fetch,
                &mut sink,
                context,
                rep_lock.as_ref(),
            )
            .await
        })
        .await;
    let protocol = session.protocol();
    let connection_state = match outcome.as_ref() {
        Ok(result) if result.connection_reusable => ConnectionState::Reusable,
        Ok(_) => ConnectionState::Poisoned,
        Err(e) => failure_connection_state(protocol, e),
    };
    if connection_state == ConnectionState::Reusable {
        pool.release(session);
    } else if !context.is_cancelled() {
        pool.retire();
    }
    outcome
        .map(|o| SharedOutcome {
            sample: o.sample,
            connection_reusable: o.connection_reusable,
        })
        .map_err(|error| RangeRunError {
            error,
            connection_state,
        })
}

/// Flush + optional digest verification + commit for a shared destination.
async fn commit_shared(
    dest: &dyn crate::storage::DynDestination,
    final_length: u64,
    integrity: Option<crate::core::spec::DigestCheck>,
) -> Result<Option<crate::core::spec::Digest>> {
    dest.flush_dyn(crate::core::sink::FlushLevel::Data).await?;
    let digest = match integrity {
        None => None,
        Some(check) => {
            let bytes = dest.read_back_dyn(0, final_length as usize).await?;
            let mut hasher = IntegrityHasher::new(check.kind);
            hasher.update(&bytes);
            let d = hasher.finalize();
            if let Some(expected) = check.expected
                && d != expected
            {
                return Err(Error::Integrity(
                    "digest mismatch on shared destination".into(),
                ));
            }
            Some(crate::core::spec::Digest {
                kind: check.kind,
                value: d,
            })
        }
    };
    dest.commit_dyn(crate::core::sink::CommitOutcome::Success { final_length })
        .await?;
    Ok(digest)
}
