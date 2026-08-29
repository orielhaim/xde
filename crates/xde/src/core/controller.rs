//! The pure control-plane state machine.
//!
//! `Engine` → `ControlLoop` → `Controller`. The controller owns the
//! authoritative `WorldModel` and reacts to `Observation`s by mutating the
//! model and emitting `Action`s. No I/O, no sleeps, no DNS, no sockets: every
//! effect is an `Action` the control loop routes to its executor (resolver,
//! shard, coordinator, timer queue). This makes the scheduling brain
//! deterministic and testable without a network.
//!
//! The core mechanism is the *pump*: any progress event (connection ready,
//! assignment finished, timer fired) causes the controller to walk ready
//! connections of each active job and claim new assignments until every
//! connection is at its stream capacity or no unclaimed range remains. One
//! pump serves H1 (capacity 1), H2 multiplexing (capacity N on one physical
//! connection), and multi-shard jobs (the same job's plan feeds assignments
//! across several shards' connections).

use std::time::{Duration, Instant};

use crate::core::{
    disposition::Disposition,
    ids::{AssignId, AssignmentRef, ConnectionId, EndpointId, JobId, OriginId, SourceId},
    policy::RetryPolicy,
    ranges::ByteRange,
    segment::Claim,
    spec::JobSpec,
    timers::TimerEvent,
    units::Rate,
    world::{ConnectionStatus, JobPhase, WorldModel},
};
/// Evidence carried from an earlier run's journal, used to seed the
/// SegmentPlan and to judge whether previously downloaded bytes are still
/// valid for the representation the server serves now.
#[derive(Debug, Clone, Default)]
pub struct ResumeEvidence {
    /// Ranges whose bytes were durably written *and* HTTP-verified before
    /// the previous run ended.
    pub durable: crate::core::ranges::RangeSet,
    pub total: Option<u64>,
    pub etag: Option<String>,
    pub last_modified: Option<std::time::SystemTime>,
    /// Source URLs the previous run used (final URL included).
    pub urls: Vec<String>,
}

/// Discovery metadata from an HTTPS/SVCB record: transport hints the engine
/// may use for protocol/endpoint selection. Purely advisory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HttpsRecordInfo {
    pub priority: u16,
    /// SVCB target name (`.` = same authority).
    pub target: String,
    pub alpn: Vec<String>,
    pub port: Option<u16>,
    pub ipv4_hint: Vec<std::net::IpAddr>,
    pub ipv6_hint: Vec<std::net::IpAddr>,
    /// Encrypted ClientHello present (advisory; not usable without support).
    pub ech: bool,
    /// Record TTL as reported by DNS.
    pub ttl: std::time::Duration,
}

#[derive(Debug, Clone)]
pub enum Observation {
    /// Application submitted a job. Admission is synchronous in the caller.
    JobAdmitted {
        job: JobId,
        spec: Box<JobSpec>,
        /// Recoverable local progress from a previous run of the same
        /// destination, if any.
        resume: Option<Box<ResumeEvidence>>,
    },
    JobCancelled {
        job: JobId,
    },
    /// Resolver answered.
    Resolved {
        origin: OriginId,
        endpoints: Vec<std::net::SocketAddr>,
        failed: bool,
        /// HTTPS/SVCB discovery hints, when the zone publishes them.
        https_records: Vec<HttpsRecordInfo>,
        /// Answer served from this resolver's cache.
        from_cache: bool,
    },
    /// A connection finished its handshake.
    ConnectionReady {
        connection: ConnectionId,
        handshake: Duration,
        /// Negotiated application protocol (ALPN result).
        protocol: crate::core::events::Protocol,
    },
    /// A connection attempt failed at socket level.
    ConnectionFailed {
        connection: ConnectionId,
        kind: ConnectFailure,
    },
    /// The shard dropped a connection (retirement, close, shutdown).
    ConnectionGone {
        connection: ConnectionId,
    },
    /// A state-changing command never reached its shard (mailbox full or
    /// shard gone). The controller must roll back the corresponding claim
    /// rather than wait forever.
    DispatchFailed {
        operation: DispatchOperation,
        job: Option<JobId>,
        assignment: Option<AssignmentRef>,
        connection: Option<ConnectionId>,
        origin: Option<OriginId>,
    },
    /// Probe answered.
    Probed {
        job: JobId,
        connection: ConnectionId,
        /// The source that answered.
        source: SourceId,
        supports_ranges: bool,
        total_length: Option<u64>,
        etag: Option<String>,
        last_modified: Option<std::time::SystemTime>,
        /// The probe response was consumed; the physical connection is clean.
        reusable: bool,
        /// `Alt-Svc` advertised HTTP/3 on this UDP port (discovery hint).
        alt_svc_h3: Option<u16>,
    },
    /// The probe was redirected. The controller owns the decision to follow:
    /// same-origin redirects re-probe the new URL; cross-origin ones go
    /// through origin creation and DNS with origin-scoped credentials
    /// stripped.
    ProbeRedirected {
        job: JobId,
        connection: ConnectionId,
        status: u16,
        location: RedirectTarget,
    },
    /// A mirror candidate reproduced (or failed to reproduce) an
    /// already-verified byte window sampled from the destination.
    MirrorSampled {
        job: JobId,
        source: SourceId,
        connection: ConnectionId,
        matches: bool,
        reusable: bool,
    },
    /// Probe failed, classified for policy.
    ProbeFailed {
        job: JobId,
        connection: ConnectionId,
        /// The source that failed.
        source: SourceId,
        failure: ProbeFailure,
        connection_state: ConnectionState,
    },
    /// A range assignment completed with HTTP verification.
    AssignmentVerified {
        job: JobId,
        assignment: AssignmentRef,
        range: ByteRange,
        /// Where the request's time went (receive vs memory vs destination).
        sample: crate::core::metrics::TransferSample,
        /// The connection that ran this assignment.
        connection: ConnectionId,
        /// The connection can carry another request (H1 body fully drained
        /// and the physical connection still alive).
        connection_reusable: bool,
    },
    /// A range assignment failed; already classified into a Disposition.
    AssignmentFailed {
        job: JobId,
        assignment: AssignmentRef,
        attempt: u32,
        disposition: Disposition,
        /// The connection that ran this assignment, when the shard still has
        /// a node for it. Poisoned connections are closed by the controller.
        connection: Option<ConnectionId>,
        connection_state: ConnectionState,
        stream_health: StreamHealth,
    },
    /// Destination finalized (rename done). Terminal for the job.
    DestinationCommitted {
        job: JobId,
        final_length: u64,
        /// Read-back digest of the committed artifact, when integrity
        /// verification was requested.
        digest: Option<crate::core::spec::Digest>,
    },
    /// Destination finalize or discard failed.
    DestinationFailed {
        job: JobId,
        failure: DestinationFailure,
    },
    RateLimited {
        origin: OriginId,
        retry_after: Option<Duration>,
    },
    TimerExpired {
        event: TimerEvent,
    },
    /// The application's refresher produced updated source information.
    SourceRefreshed {
        job: JobId,
        /// Replacement URL, if the refresher rotated it.
        url: Option<String>,
        /// Replacement headers (may include credential headers; never logged).
        headers: Option<http::HeaderMap>,
    },
    /// The refresher could not produce usable credentials. Terminal.
    CredentialRefreshFailed {
        job: JobId,
        status: u16,
    },
}

/// The state-changing command whose delivery failed. Keeping this explicit
/// lets the controller roll back only the state that the missing command
/// would have changed; a failed truncate, for example, is a correctness
/// failure rather than an ordinary range retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchOperation {
    Resolve,
    OpenConnection,
    Probe,
    StartAssignment,
    StartFullBody,
    SampleMirror,
    TruncateAssignment,
    CloseConnection,
    AttachDestination,
    CloseLane,
    CommitDestination,
}

/// Post-failure state of the connection that ran a failed assignment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Unread response bytes remain; must be closed.
    Poisoned,
    /// Clean failure (e.g. 4xx); connection reusable.
    Reusable,
    /// Connection is already gone from the shard.
    Gone,
}

/// Health of one logical request stream. A truncated H2/H3 stream is not a
/// poisoned physical connection; the two lifecycles must be reported
/// independently so one stolen range cannot retire healthy multiplexed work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamHealth {
    Completed,
    Truncated,
    Failed,
}

/// A redirect target reported by a probe. Kept as a typed newtype so the
/// controller can parse and classify it exactly once.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedirectTarget {
    pub url: String,
}

/// Why a connection attempt failed at socket level. The controller derives
/// retry semantics from the kind, never from an untyped flag.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectFailure {
    Dns,
    Refused,
    Timeout,
    Tls,
    Other,
}

impl ConnectFailure {
    /// DNS/refused/timeouts are transient by nature; TLS identity failures
    /// and unknown socket errors are retried only under the normal backoff
    /// budget, which the controller applies uniformly.
    pub fn retryable(self) -> bool {
        !matches!(self, ConnectFailure::Tls)
    }
}

/// Typed probe failure: enough semantic information for policy decisions,
/// none of the raw user-facing error tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeFailure {
    pub kind: ProbeFailureKind,
    pub status: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeFailureKind {
    /// 401/407-style credential rejection; refresh may apply.
    Credentials,
    /// Origin-wide rate limiting / overload.
    RateLimited { retry_after: Option<Duration> },
    /// The transport or protocol layer failed; retry with backoff.
    Transport,
}

impl ProbeFailure {
    pub fn from_error(error: &crate::core::Error) -> Self {
        use crate::core::error::{Error, HttpError};
        match error {
            Error::Http(HttpError::Dispositioned(d, status)) => match d.as_ref() {
                crate::core::Disposition::RefreshCredentials { .. } => Self {
                    kind: ProbeFailureKind::Credentials,
                    status: *status,
                },
                crate::core::Disposition::BackOffOrigin { after, .. } => Self {
                    kind: ProbeFailureKind::RateLimited {
                        retry_after: *after,
                    },
                    status: *status,
                },
                _ => Self {
                    kind: ProbeFailureKind::Transport,
                    status: *status,
                },
            },
            Error::Http(HttpError::RangeUnsupported { status })
            | Error::Http(HttpError::InvalidStatus(status)) => Self {
                kind: ProbeFailureKind::Transport,
                status: *status,
            },
            _ => Self {
                kind: ProbeFailureKind::Transport,
                status: 0,
            },
        }
    }
}

/// Typed destination-side failure. Distinguishing these matters: a digest
/// mismatch invalidates the artifact, ENOSPC must never trigger a network
/// retry, and lease conflicts are admission bugs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DestinationFailure {
    pub kind: DestinationFailureKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DestinationFailureKind {
    /// The artifact's computed digest did not match the expected value.
    DigestMismatch,
    /// No space / quota on the destination device.
    NoSpace,
    /// The destination rejected or failed an operation.
    DestinationError,
}

impl DestinationFailure {
    pub fn from_error(error: &crate::core::Error) -> Self {
        let kind = match error {
            crate::core::Error::Integrity(_) => DestinationFailureKind::DigestMismatch,
            crate::core::Error::Destination(crate::core::error::DestinationError::Enospc) => {
                DestinationFailureKind::NoSpace
            }
            _ => DestinationFailureKind::DestinationError,
        };
        Self { kind }
    }
}

/// Commands the controller emits. Each action has exactly one executor:
/// resolver service, shard service, destination coordinator, or the control
/// loop itself (timers).
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Resolve {
        origin: OriginId,
        host: String,
        port: u16,
    },
    /// Open a connection on the named shard. The shard stores the real
    /// transport under `connection` and reports back.
    OpenConnection {
        connection: ConnectionId,
        origin: OriginId,
        endpoint: EndpointId,
        shard: usize,
        /// Dial over QUIC (HTTP/3) instead of TCP. The controller sets this
        /// only when discovery advertises h3 and the origin's H3 evidence
        /// is not in backoff; a failed QUIC dial falls back to TCP.
        prefer_h3: bool,
        /// Alt-Svc UDP port for H3 dials; applied to the endpoint address.
        alt_port: Option<u16>,
    },
    CloseConnection {
        connection: ConnectionId,
    },
    Probe {
        job: JobId,
        connection: ConnectionId,
        /// Which mirror this probe interrogates. None = the job's
        /// currently active source.
        source: Option<SourceId>,
    },
    /// Execute one range fetch for this assignment on this connection.
    /// `frontier`: the assignment starts at the verified contiguous prefix,
    /// so it may consume the memory budget's forward-progress reserve.
    StartAssignment {
        job: JobId,
        assignment: AssignmentRef,
        connection: ConnectionId,
        range: ByteRange,
        overlap: u32,
        frontier: bool,
    },
    /// Full-body fetch (no range support / unknown length / single-stream).
    /// `assignment` is the plan's single-stream worker tracking progress.
    StartFullBody {
        job: JobId,
        assignment: AssignmentRef,
        connection: ConnectionId,
        frontier: bool,
    },
    /// Ask a mirror to reproduce an already-verified byte window. Agreement
    /// is the equivalence evidence that unlocks simultaneous transfer.
    /// The control plane attaches the destination's `.part` path and job
    /// context when routing.
    SampleMirror {
        job: JobId,
        source: SourceId,
        connection: ConnectionId,
        range: ByteRange,
    },
    /// Wipe the destination journal: its representation evidence no longer
    /// matches what the server serves.
    ResetResumeData {
        job: JobId,
    },
    /// Telemetry: one or more mirrors were quarantined after serving
    /// content inconsistent with verified artifact data.
    ReportSourceQuarantined {
        job: JobId,
        sources: Vec<SourceId>,
        reason: String,
    },
    /// Tell a running request to stop at `new_end` instead of its original
    /// range end: its tail was given to another worker (steal or straggler
    /// rebalance). Without this the victim and its replacement would
    /// download the same bytes twice.
    TruncateAssignment {
        job: JobId,
        assignment: AssignmentRef,
        connection: ConnectionId,
        new_end: u64,
    },
    /// Ask the application's refresher for updated source information. The
    /// control loop executes the callback and reports back with
    /// `Observation::SourceRefreshed` or `Observation::CredentialRefreshFailed`.
    RequestCredentialRefresh {
        job: JobId,
        url: String,
        status: u16,
        attempt: u32,
    },
    CommitDestination {
        job: JobId,
        final_length: u64,
        /// Integrity proof required before the rename. `None` when the job
        /// asked for no digest at all.
        integrity: Option<crate::core::spec::DigestCheck>,
    },
    DiscardDestination {
        job: JobId,
    },
    FailJob {
        job: JobId,
        error: String,
    },
    CompleteJob {
        job: JobId,
        total_bytes: u64,
        digest: Option<crate::core::spec::Digest>,
        resumed_bytes: u64,
    },
    ScheduleTimer {
        at: Instant,
        event: TimerEvent,
    },
}

const RESUME_ESTIMATE: Rate = Rate::COLD_START;

/// How long to wait before trying the next-ranked endpoint when the first
/// has not completed its handshake (Happy-Eyeballs stagger).
pub const CONNECT_STAGGER: Duration = Duration::from_millis(250);

/// Sentinel failure text for user-initiated cancellation, recognized by the
/// control loop when translating `FailJob` into a public `Error::Cancelled`.
pub const CANCELLED_SENTINEL: &str = "cancelled";

/// Sentinel for jobs that exceeded their deadline.
pub const DEADLINE_SENTINEL: &str = "deadline exceeded";

struct TopologyExperimentView {
    variable: crate::core::world::TopologyVariable,
    conn: Option<ConnectionId>,
    baseline: f64,
    opened_at: Instant,
    measure_start_at: Option<Instant>,
    measure_start: u64,
    already_measured: bool,
}

/// Everything one adaptive tick measured, grouped so the experiment
/// evaluator reads a single coherent snapshot of the window.
struct DecisionInputs {
    rate: f64,
    stall_fraction: f64,
    experiment: Option<TopologyExperimentView>,
    remaining: Option<u64>,
    has_work: bool,
    ceiling: usize,
    live: usize,
    multiplexed: bool,
    handshake: Duration,
    stream_ceiling: u32,
    max_streams: u32,
}

/// A tail cut order: the running request on `connection` must stop at
/// `new_end` because its remaining bytes were handed to another worker.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TailCut {
    job: JobId,
    victim: AssignId,
    connection: ConnectionId,
    new_end: u64,
}

/// One successfully claimed piece of work for a connection.
struct ClaimedPiece {
    assign: AssignId,
    range: ByteRange,
    overlap: u32,
    frontier: bool,
    /// Present when this claim stole the tail of a running assignment.
    tail_cut: Option<TailCut>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ConnectionRuntimeState {
    in_flight: usize,
    ewma: Option<crate::core::ewma::EwmaWithVariance>,
}

/// The deterministic controller. Owns the WorldModel.
#[derive(Debug)]
pub struct Controller {
    pub world: WorldModel,
    retry: RetryPolicy,
    epoch: Instant,
    engine_max_active_assignments: u32,
    engine_max_connections: u16,
    engine_max_jobs: usize,
    active_assignment_count: usize,
    shard_load: Vec<usize>,
    origin_live_conns: slotmap::SecondaryMap<OriginId, usize>,
    origin_verified: slotmap::SecondaryMap<OriginId, u64>,
    origin_avg_handshake: slotmap::SecondaryMap<OriginId, Duration>,
    origin_generation: slotmap::SecondaryMap<OriginId, u64>,
    origin_last_invalidation: slotmap::SecondaryMap<OriginId, Instant>,
    connection_state: slotmap::SecondaryMap<ConnectionId, ConnectionRuntimeState>,
    origin_state: slotmap::SecondaryMap<OriginId, (Duration, Duration)>,
    origin_admission: slotmap::SecondaryMap<OriginId, Instant>,
}

impl Default for Controller {
    fn default() -> Self {
        Self::with_shards(1)
    }
}

impl Controller {
    pub fn new() -> Self {
        Self::with_shards(1)
    }

    pub fn with_shards(shards: usize) -> Self {
        let shards = shards.max(1);
        Self {
            world: WorldModel::new(),
            retry: RetryPolicy::default(),
            epoch: Instant::now(),
            engine_max_active_assignments: crate::core::policy::EngineLimits::default()
                .max_active_assignments,
            active_assignment_count: 0,
            shard_load: vec![0; shards],
            origin_live_conns: slotmap::SecondaryMap::new(),
            origin_verified: slotmap::SecondaryMap::new(),
            origin_avg_handshake: slotmap::SecondaryMap::new(),
            origin_generation: slotmap::SecondaryMap::new(),
            origin_last_invalidation: slotmap::SecondaryMap::new(),
            connection_state: slotmap::SecondaryMap::new(),
            origin_state: slotmap::SecondaryMap::new(),
            origin_admission: slotmap::SecondaryMap::new(),
            engine_max_connections: crate::core::policy::EngineLimits::default()
                .max_physical_connections,
            engine_max_jobs: crate::core::policy::EngineLimits::default().max_jobs,
        }
    }

    /// Apply the engine's global ceilings (called once at construction by the
    /// control plane).
    pub fn set_engine_limits(&mut self, limits: &crate::core::policy::EngineLimits) {
        self.engine_max_active_assignments = limits.max_active_assignments.max(1);
        self.engine_max_connections = limits.max_physical_connections.max(1);
        self.engine_max_jobs = limits.max_jobs.max(1);
    }

    pub fn max_jobs(&self) -> usize {
        self.engine_max_jobs
    }

    fn place_shard(&self) -> usize {
        self.shard_load
            .iter()
            .enumerate()
            .min_by_key(|(idx, n)| (**n, *idx))
            .map_or(0, |(idx, _)| idx)
    }

    fn inc_shard_load(&mut self, shard: usize) {
        if shard < self.shard_load.len() {
            self.shard_load[shard] += 1;
        }
    }

    fn dec_shard_load(&mut self, shard: usize) {
        if shard < self.shard_load.len() {
            self.shard_load[shard] = self.shard_load[shard].saturating_sub(1);
        }
    }

    #[allow(clippy::match_like_matches_macro)]
    pub fn handle(&mut self, obs: Observation, now: Instant) -> Vec<Action> {
        let mut actions = Vec::new();
        match obs {
            Observation::JobAdmitted { job, spec, resume } => {
                let origin_for_gen = self
                    .world
                    .jobs
                    .get(job)
                    .map(|j| j.origin)
                    .unwrap_or_default();
                let min_peer = self
                    .world
                    .jobs
                    .iter()
                    .filter(|(id, peer)| {
                        *id != job
                            && peer.origin == origin_for_gen
                            && peer.phase == JobPhase::Transferring
                    })
                    .map(|(_, peer)| peer.virt_service)
                    .fold(f64::INFINITY, f64::min);
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.integrity = spec.integrity.clone();
                    j.resume = resume.map(|b| *b);
                    if min_peer.is_finite() {
                        j.virt_service = min_peer;
                    }
                }
                self.bump_generation(origin_for_gen, now);
                let origin = self.world.jobs.get(job).map(|j| j.origin);
                if let Some(origin) = origin
                    && let Some(o) = self.world.origins.get(origin)
                {
                    let (host, port) = (o.key.host.to_string(), o.key.port);
                    let already_resolved = !o.endpoints.is_empty();
                    if already_resolved {
                        self.ensure_probes(&mut actions, origin, now);
                    } else {
                        actions.push(Action::Resolve { origin, host, port });
                    }
                }
            }

            Observation::JobCancelled { job } => {
                let origin = self.world.jobs.get(job).map(|j| j.origin);
                let active_cnt = self.active_assignment_count;
                let Some(j) = self.world.jobs.get_mut(job) else {
                    return actions;
                };
                if j.phase.is_terminal() {
                    return actions;
                }
                let inflight = j.plan.as_ref().map(|p| p.in_flight()).unwrap_or(0);
                j.phase = JobPhase::Cancelling;
                j.pending_terminal = Some(Err(CANCELLED_SENTINEL.into()));
                if let Some(o) = origin {
                    self.bump_generation(o, now);
                }
                if inflight == 0 {
                    self.active_assignment_count = active_cnt;
                }
                self.try_complete_drain(&mut actions, job);
            }

            Observation::Resolved {
                origin,
                endpoints,
                failed,
                https_records,
                from_cache: _,
            } => {
                // Protocol discovery: SVCB alpn=h3 is a hint the controller
                // may use for protocol experiments.
                let advertises_h3 = https_records
                    .iter()
                    .any(|r| r.alpn.iter().any(|p| p == "h3"));
                if let Some(o) = self.world.origins.get_mut(origin) {
                    o.advertises_h3 = advertises_h3 || o.advertises_h3;
                }
                self.world.note_endpoints(origin, &endpoints);

                let job = self.waiting_probe_job(origin);
                let Some(job) = job else {
                    // No primary probe waiting: dial any pending MIRROR
                    // probes that were blocked on this resolution.
                    let pending: Vec<(JobId, SourceId)> = self
                        .world
                        .jobs
                        .iter()
                        .filter(|(_, j)| j.phase == JobPhase::Transferring)
                        .flat_map(|(id, j)| {
                            j.probing_sources
                                .iter()
                                .copied()
                                .filter(|sid| {
                                    self.world
                                        .sources
                                        .get(*sid)
                                        .is_some_and(|s| s.origin == origin)
                                })
                                .map(move |sid| (id, sid))
                        })
                        .collect();
                    for (mjob, sid) in pending {
                        if failed || endpoints.is_empty() {
                            // Mirror unreachable: drop the candidate.
                            if let Some(j) = self.world.jobs.get_mut(mjob) {
                                j.probing_sources.retain(|s| s != &sid);
                                j.failed_sources.push(sid);
                            }
                            continue;
                        }
                        let Some(ep) = self.world.select_endpoint(origin, None, now, |_| false)
                        else {
                            continue;
                        };
                        tracing::info!(target: "xde::mirror", ?mjob, ?sid, "dialing resolved mirror");
                        let shard = self.place_shard();
                        self.dial_connection(&mut actions, origin, ep, shard, true, Some(sid), now);
                    }
                    return actions;
                };
                if failed || endpoints.is_empty() {
                    self.retry_resolve(&mut actions, job, now);
                    return actions;
                }
                if let Some(endpoint) = self.world.select_endpoint(origin, None, now, |_| false) {
                    let shard = self.place_shard();
                    self.dial_connection(&mut actions, origin, endpoint, shard, true, None, now);
                    // Happy-Eyeballs-style stagger: if this connection is not
                    // ready shortly, try the next-ranked address family.
                    actions.push(Action::ScheduleTimer {
                        at: now + CONNECT_STAGGER,
                        event: TimerEvent::EndpointStagger { origin, rank: 1 },
                    });
                    if let Some(j) = self.world.jobs.get_mut(job) {
                        j.phase = JobPhase::Probing;
                        j.resolve_attempts = 0;
                    }
                    self.ensure_probes(&mut actions, origin, now);
                } else {
                    self.retry_resolve(&mut actions, job, now);
                }
            }

            Observation::ConnectionReady {
                connection,
                handshake,
                protocol,
            } => {
                let shard_for_load = self.world.connections.get(connection).map(|c| c.shard);
                if let Some(c) = self.world.connections.get_mut(connection) {
                    c.status = ConnectionStatus::Ready;
                    c.protocol = protocol;
                }
                if let Some(origin) = self.world.connections.get(connection).map(|c| c.origin) {
                    let avg = self
                        .origin_avg_handshake
                        .get(origin)
                        .copied()
                        .unwrap_or(handshake);
                    let blended = if avg.is_zero() {
                        handshake
                    } else {
                        avg.mul_f32(0.7) + handshake.mul_f32(0.3)
                    };
                    self.origin_avg_handshake.insert(origin, blended);
                }
                let _ = shard_for_load;
                // A working handshake proves reachability now: clear the
                // stale failure penalty so old failures never shadow
                // healthy endpoints.
                self.world.clear_endpoint_failures(connection);
                // A working H3 handshake proves UDP/QUIC reachability
                // now: clear stale H3 failure evidence.
                let (ready_origin, is_h3) = self
                    .world
                    .connections
                    .get(connection)
                    .map(|c| {
                        (
                            Some(c.origin),
                            c.protocol == crate::core::events::Protocol::Http3,
                        )
                    })
                    .unwrap_or((None, false));
                if is_h3
                    && let Some(origin) = ready_origin
                    && let Some(o) = self.world.origins.get_mut(origin)
                {
                    o.h3_failures = 0;
                    o.h3_retry_after = None;
                }
                // Cold-start endpoint racing: retire other CONNECTING
                // dials to this origin ONLY while no connection is ready
                // yet. Once one is ready, sibling dials belong to
                // deliberate scale-out (multi-connection aggregation),
                // not to a lost race, and must be allowed to finish.
                if let Some(c) = self.world.connections.get(connection) {
                    let had_ready = self.world.connections.iter().any(|(id, other)| {
                        other.origin == c.origin
                            && other.status == ConnectionStatus::Ready
                            && id != connection
                    });
                    if !had_ready {
                        let winner_job = match c.intent {
                            crate::core::world::ConnectionIntent::Probe { job, .. }
                            | crate::core::world::ConnectionIntent::Mirror { job, .. } => Some(job),
                            crate::core::world::ConnectionIntent::Pool { .. } => None,
                        };
                        let losers: Vec<ConnectionId> = self
                            .world
                            .connections
                            .iter()
                            .filter(|(id, other)| {
                                other.origin == c.origin
                                    && other.status == ConnectionStatus::Connecting
                                    && *id != connection
                                    && match (winner_job, other.intent) {
                                        (
                                            Some(winner),
                                            crate::core::world::ConnectionIntent::Probe {
                                                job, ..
                                            }
                                            | crate::core::world::ConnectionIntent::Mirror {
                                                job,
                                                ..
                                            },
                                        ) if job != winner => false,
                                        _ => true,
                                    }
                            })
                            .map(|(id, _)| id)
                            .collect();
                        for loser in losers {
                            actions.push(Action::CloseConnection { connection: loser });
                            self.drop_connection(loser);
                        }
                    }
                }
                // Record handshake latency as endpoint evidence.
                let (origin, endpoint) = match self.world.connections.get(connection) {
                    Some(c) => (c.origin, c.endpoint),
                    None => return actions,
                };
                if let Some(e) = self.world.endpoints.get_mut(endpoint) {
                    e.handshake_ewma = Some(match e.handshake_ewma {
                        Some(prev) => prev.mul_f32(0.7) + handshake.mul_f32(0.3),
                        None => handshake,
                    });
                }
                // A connection dialed for a specific mirror probes THAT
                // source; the job continues transferring on its active
                // sources meanwhile.
                let serving = self
                    .world
                    .connections
                    .get(connection)
                    .and_then(|c| c.serving_source);
                if let Some(sid) = serving {
                    let owner = self
                        .world
                        .jobs
                        .iter()
                        .find(|(_, j)| {
                            j.phase == JobPhase::Transferring && j.sources.contains(&sid)
                        })
                        .map(|(id, _)| id);
                    if let Some(job) = owner {
                        actions.push(Action::Probe {
                            job,
                            connection,
                            source: Some(sid),
                        });
                        return actions;
                    }
                    // Nobody needs this mirror anymore.
                    actions.push(Action::CloseConnection { connection });
                    self.drop_connection(connection);
                    return actions;
                }
                // Honor the dial intent first: a connection opened for job B
                // must probe B even if job A is still Probing on this origin.
                let intent_job =
                    self.world
                        .connections
                        .get(connection)
                        .and_then(|c| match c.intent {
                            crate::core::world::ConnectionIntent::Probe { job, .. } => Some(job),
                            _ => None,
                        });
                if let Some(job) = intent_job
                    && self.world.jobs.get(job).is_some_and(|j| {
                        j.origin == origin
                            && matches!(j.phase, JobPhase::Created | JobPhase::Probing)
                    })
                {
                    if let Some(j) = self.world.jobs.get_mut(job) {
                        j.phase = JobPhase::Probing;
                    }
                    actions.push(Action::Probe {
                        job,
                        connection,
                        source: None,
                    });
                    return actions;
                }
                if let Some(job) = self.waiting_probe_job(origin) {
                    if let Some(j) = self.world.jobs.get_mut(job) {
                        j.phase = JobPhase::Probing;
                    }
                    actions.push(Action::Probe {
                        job,
                        connection,
                        source: None,
                    });
                    return actions;
                }
                self.ensure_probes(&mut actions, origin, now);
                self.pump(&mut actions, origin, now);
            }

            Observation::ConnectionFailed { connection, kind } => {
                // H3 failure evidence: remember contextually that QUIC did
                // not work to this origin and back off exponentially, so a
                // stale advertisement or blocked UDP is never retried for
                // every range. A TCP replacement dials immediately.
                let (origin_of_conn, was_h3) = self
                    .world
                    .connections
                    .get(connection)
                    .map(|c| (Some(c.origin), c.prefer_h3))
                    .unwrap_or((None, false));
                if let Some(o) = origin_of_conn.and_then(|o| self.world.origins.get_mut(o))
                    && was_h3
                {
                    o.h3_failures += 1;
                    let backoff =
                        Duration::from_secs(30u64.saturating_mul(1 << o.h3_failures.min(5)));
                    o.h3_retry_after = Some(now + backoff);
                }
                self.world.note_endpoint_failure(connection);
                if let Some(o) = origin_of_conn {
                    self.bump_generation(o, now);
                }
                let intent = self.world.connections.get(connection).map(|c| c.intent);
                self.drop_connection(connection);
                // H3 dial failed and we have no ready connection: open a
                // TCP replacement so the job isn't blocked. After the
                // fallback is in flight, the intent-based handler below
                // would otherwise kill the job for the H3 TLS error - the
                // replacement is the authoritative path now, so we skip
                // it for the H3+Probe case.
                let mut h3_fallback_opened = false;
                if was_h3
                    && let Some(origin) = origin_of_conn
                    && !self.world.has_ready_connection(origin)
                {
                    self.open_one_connection(&mut actions, origin, now, false);
                    h3_fallback_opened = true;
                }
                if h3_fallback_opened
                    && matches!(
                        intent,
                        Some(crate::core::world::ConnectionIntent::Probe { .. })
                    )
                {
                    return actions;
                }
                match intent {
                    Some(crate::core::world::ConnectionIntent::Probe { job, source }) => {
                        self.on_intent_connect_failed(
                            &mut actions,
                            job,
                            Some(source),
                            kind,
                            origin_of_conn,
                            now,
                        );
                    }
                    Some(crate::core::world::ConnectionIntent::Mirror { job, source }) => {
                        if let Some(j) = self.world.jobs.get_mut(job) {
                            j.probing_sources.retain(|s| *s != source);
                            j.failed_sources.push(source);
                        }
                    }
                    Some(crate::core::world::ConnectionIntent::Pool { origin }) => {
                        if !kind.retryable() {
                            let jobs: Vec<_> = self
                                .world
                                .jobs
                                .iter()
                                .filter(|(_, j)| j.origin == origin && j.phase == JobPhase::Probing)
                                .map(|(id, _)| id)
                                .collect();
                            for job in jobs {
                                self.begin_fail(&mut actions, job, "connection failed: tls".into());
                            }
                        } else {
                            self.open_one_connection(&mut actions, origin, now, true);
                        }
                    }
                    None => {}
                }
            }

            Observation::DispatchFailed {
                operation,
                job,
                assignment,
                connection,
                origin,
            } => {
                match operation {
                    DispatchOperation::StartAssignment | DispatchOperation::StartFullBody => {
                        if let Some(assign) = assignment {
                            self.release_assignment_slot(connection);
                            self.handle_assignment_disposition(
                                &mut actions,
                                assign.job,
                                Some(assign.assignment),
                                ByteRange::new(0, 0),
                                Disposition::RetrySameRange {
                                    after: Some(Duration::ZERO),
                                    reason: "assignment dispatch failed",
                                },
                                0,
                                connection,
                                now,
                            );
                        }
                    }
                    DispatchOperation::TruncateAssignment => {
                        if let Some(job) = job {
                            self.begin_fail(
                                &mut actions,
                                job,
                                "failed to stop an assignment during range rebalancing".into(),
                            );
                        }
                    }
                    DispatchOperation::Probe | DispatchOperation::OpenConnection => {
                        if let Some(conn) = connection {
                            let intent = self.world.connections.get(conn).map(|c| c.intent);
                            self.drop_connection(conn);
                            match intent {
                                Some(crate::core::world::ConnectionIntent::Probe {
                                    job, ..
                                }) => {
                                    self.retry_resolve(&mut actions, job, now);
                                }
                                Some(crate::core::world::ConnectionIntent::Mirror {
                                    job,
                                    source,
                                }) => {
                                    if let Some(j) = self.world.jobs.get_mut(job) {
                                        j.probing_sources.retain(|s| *s != source);
                                        j.failed_sources.push(source);
                                    }
                                }
                                _ => {}
                            }
                        } else if let Some(job) = job {
                            self.retry_resolve(&mut actions, job, now);
                        } else if let Some(origin) = origin {
                            let jobs: Vec<_> = self
                                .world
                                .jobs
                                .iter()
                                .filter(|(_, j)| j.origin == origin && j.phase == JobPhase::Probing)
                                .map(|(id, _)| id)
                                .collect();
                            for job in jobs {
                                self.retry_resolve(&mut actions, job, now);
                            }
                        }
                    }
                    DispatchOperation::Resolve => {
                        if let Some(origin) = origin {
                            let jobs: Vec<_> = self
                                .world
                                .jobs
                                .iter()
                                .filter(|(_, j)| j.origin == origin && !j.phase.is_terminal())
                                .map(|(id, _)| id)
                                .collect();
                            for job in jobs {
                                self.retry_resolve(&mut actions, job, now);
                            }
                        }
                    }
                    DispatchOperation::SampleMirror => {
                        if let Some(assign) = assignment
                            .or_else(|| job.map(|job| AssignmentRef::new(job, AssignId::default())))
                        {
                            let _ = assign;
                        }
                    }
                    DispatchOperation::CloseConnection
                    | DispatchOperation::AttachDestination
                    | DispatchOperation::CloseLane
                    | DispatchOperation::CommitDestination => {
                        if let Some(job) = job {
                            self.begin_fail(
                                &mut actions,
                                job,
                                "state-changing command dispatch failed".into(),
                            );
                        }
                    }
                }
                if let Some(job) = job.or_else(|| assignment.map(|a| a.job))
                    && let Some(origin) = self.world.jobs.get(job).map(|j| j.origin)
                {
                    self.pump(&mut actions, origin, now);
                }
            }

            Observation::ConnectionGone { connection } => {
                // Any assignments riding this connection fail on the shard
                // side; here we only drop the node.
                self.drop_connection(connection);
            }

            Observation::Probed {
                job,
                source,
                supports_ranges,
                total_length,
                etag,
                last_modified,
                reusable,
                connection,
                alt_svc_h3,
            } => {
                let Some(j) = self.world.jobs.get_mut(job) else {
                    return actions;
                };
                // Mirror verification probe: this source was dialed
                // explicitly by the mixer. Length agreement alone is weak;
                // the mirror must also REPRODUCE an already-verified byte
                // window before it joins the shared plan.
                if j.probing_sources.contains(&source) {
                    let agrees = match (total_length, j.total_length) {
                        (Some(a), Some(b)) => a == b,
                        _ => false,
                    };
                    j.probing_sources.retain(|s| s != &source);
                    if !agrees || !supports_ranges {
                        if let Some(j) = self.world.jobs.get_mut(job) {
                            j.failed_sources.push(source);
                        }
                        actions.push(Action::ReportSourceQuarantined {
                            job,
                            sources: vec![source],
                            reason: if supports_ranges {
                                "length mismatch across mirrors".into()
                            } else {
                                "mirror lacks range support".into()
                            },
                        });
                        tracing::warn!(target: "xde::mirror", ?job, ?source, "mirror rejected");
                        if !reusable {
                            actions.push(Action::CloseConnection { connection });
                            self.drop_connection(connection);
                        }
                        return actions;
                    }
                    // Sample an already-verified window near the frontier.
                    let prefix = j
                        .plan
                        .as_ref()
                        .map(|p| p.completed().contiguous_prefix())
                        .unwrap_or(0);
                    const SAMPLE_LEN: u64 = 128 * 1024;
                    if prefix == 0 {
                        // Nothing verified yet: keep the candidate warm and
                        // retry sampling on a later tick.
                        j.probing_sources.push(source);
                        return actions;
                    }
                    let start = prefix.saturating_sub(SAMPLE_LEN);
                    tracing::info!(target: "xde::mirror", ?job, ?source, ?start, "sampling mirror against verified data");
                    if let Some(j) = self.world.jobs.get_mut(job) {
                        j.probing_sources.push(source);
                    }
                    actions.push(Action::SampleMirror {
                        job,
                        source,
                        connection,
                        range: ByteRange::new(start, prefix),
                    });
                    return actions;
                }
                j.supports_ranges = supports_ranges;
                j.total_length = total_length.or(j.total_length);
                j.fingerprint_etag = etag.clone();
                j.fingerprint_last_modified = last_modified;
                if let Ok(mut lock) = j.rep_lock.lock() {
                    let _ = lock.validate(etag.as_deref(), None, total_length);
                }
                // Alt-Svc discovery: the origin advertises HTTP/3 on this
                // UDP port. Evidence only - dials still verify by working.
                if let Some(port) = alt_svc_h3
                    && let Some(o) = self.world.origins.get_mut(j.origin)
                {
                    o.advertises_h3 = true;
                    o.h3_alt_port = Some(port);
                }
                // Judge resume evidence against what the server just told us.
                // Strong agreement (URL + validators + length) seeds verified
                // ranges; anything weaker re-downloads conservatively.
                let mut seeded = crate::core::ranges::RangeSet::new();
                let mut resumed_bytes = 0u64;
                if let Some(r) = j.resume.take() {
                    let url_ok = r.urls.last().is_some_and(|u| {
                        Some(u.as_str()) == j.redirect_chain.last().map(|s| s.as_str())
                    });
                    let etag_agrees = match (&r.etag, &etag) {
                        (Some(a), Some(b)) => a == b,
                        (None, None) => true,
                        _ => false,
                    };
                    let lm_agrees = match (&r.last_modified, &last_modified) {
                        (Some(a), Some(b)) => a == b,
                        (None, None) => true,
                        _ => false,
                    };
                    let length_agrees = match (&r.total, &total_length) {
                        (Some(a), Some(b)) => a == b,
                        (None, _) => true,
                        _ => false,
                    };
                    let strong_etag = etag
                        .as_deref()
                        .is_some_and(|e| e.starts_with('"') && !e.starts_with("W/"));
                    let validators_ok = if strong_etag {
                        etag_agrees
                    } else {
                        etag_agrees && lm_agrees
                    };
                    let trust_journal = url_ok
                        && length_agrees
                        && validators_ok
                        && (strong_etag || !j.integrity.verify_on_resume);
                    if trust_journal {
                        for range in r.durable.iter() {
                            if total_length.is_none_or(|t| range.end <= t) {
                                seeded.insert(range);
                                resumed_bytes += range.len();
                            }
                        }
                    } else if url_ok
                        && length_agrees
                        && validators_ok
                        && j.integrity.verify_on_resume
                    {
                        // Weak/missing validators: keep bytes on disk but
                        // re-fetch so overlap verification can confirm them.
                    } else {
                        actions.push(Action::ResetResumeData { job });
                    }
                }
                j.resumed_bytes = resumed_bytes;
                let mut plan = crate::core::segment::SegmentPlan::new(
                    j.total_length,
                    seeded,
                    j.policy.segmentation,
                    j.urgency.piece_duration_target(),
                    j.integrity.overlap_bytes,
                );
                if !supports_ranges {
                    // The source serves only full representations: one
                    // ordered stream, even though we know the length.
                    plan.set_single_stream(true);
                }
                if !crate::core::sink::supports_segmentation(j.sink_caps) {
                    // The destination cannot take non-contiguous writes
                    // (e.g. a raw sequential sink without reorder buffering):
                    // one ordered stream regardless of source capabilities.
                    plan.set_single_stream(true);
                }
                plan.clamp_max_piece(j.sink_hints.max_operation_bytes);
                let already_complete = plan.is_complete();
                j.plan = Some(plan);
                j.phase = JobPhase::Transferring;
                let origin = j.origin;
                let init_conns = j.policy.initial_physical_connections.max(1) as usize;
                let init_streams = u32::from(
                    j.policy
                        .initial_streams_per_connection
                        .max(1)
                        .min(j.policy.transport.max_streams_per_connection),
                );
                let discovered_h3 = alt_svc_h3;
                let _ = j;
                self.origin_admission.insert(origin, now);
                if let Some(o) = self.world.origins.get_mut(origin) {
                    o.adaptive_target_conns = o.adaptive_target_conns.max(init_conns);
                    o.adaptive_target_streams = o.adaptive_target_streams.max(init_streams);
                }
                if !reusable {
                    actions.push(Action::CloseConnection { connection });
                    self.drop_connection(connection);
                }
                if discovered_h3.is_some() {
                    let has_h3 = self.world.connections.iter().any(|(_, c)| {
                        c.origin == origin
                            && (c.prefer_h3 || c.protocol == crate::core::events::Protocol::Http3)
                    });
                    if !has_h3
                        && let Some(ep) = self.world.select_endpoint(origin, None, now, |_| false)
                    {
                        let shard = self.place_shard();
                        self.dial_connection(&mut actions, origin, ep, shard, true, None, now);
                    }
                }
                self.scale_connections(&mut actions, Some(origin), now);
                self.ensure_probes(&mut actions, origin, now);
                self.start_adaptive(&mut actions, origin, now);
                if already_complete {
                    if let Some(j) = self.world.jobs.get_mut(job) {
                        j.phase = JobPhase::Committing;
                        let len = j
                            .plan
                            .as_ref()
                            .and_then(|p| p.total())
                            .or(j.total_length)
                            .unwrap_or(0);
                        let integrity = match (j.integrity.expected, j.integrity.compute) {
                            (Some(exp), _) => Some(crate::core::spec::DigestCheck {
                                kind: exp.kind(),
                                expected: Some(*exp.bytes()),
                            }),
                            (None, Some(kind)) => Some(crate::core::spec::DigestCheck {
                                kind,
                                expected: None,
                            }),
                            (None, None) => None,
                        };
                        actions.push(Action::CommitDestination {
                            job,
                            final_length: len,
                            integrity,
                        });
                    }
                } else {
                    self.pump(&mut actions, origin, now);
                }
                return actions;
            }

            Observation::ProbeRedirected {
                job,
                connection,
                status,
                location,
            } => self.handle_probe_redirect(&mut actions, job, connection, status, &location, now),

            Observation::ProbeFailed {
                job,
                source,
                connection,
                failure,
                connection_state,
            } => {
                // A failed MIRROR probe just drops that candidate; the job
                // continues on its active sources.
                let mirror_probe = self
                    .world
                    .jobs
                    .get(job)
                    .is_some_and(|j| j.probing_sources.contains(&source));
                if mirror_probe {
                    if let Some(j) = self.world.jobs.get_mut(job) {
                        j.probing_sources.retain(|s| s != &source);
                        j.failed_sources.push(source);
                    }
                    tracing::warn!(target: "xde::mirror", ?job, ?source, "mirror probe failed");
                    return actions;
                }
                match connection_state {
                    ConnectionState::Poisoned | ConnectionState::Gone => {
                        actions.push(Action::CloseConnection { connection });
                        self.drop_connection(connection);
                    }
                    ConnectionState::Reusable => {}
                }
                let disp = match failure.kind {
                    ProbeFailureKind::Credentials => Disposition::RefreshCredentials {
                        status: failure.status,
                    },
                    ProbeFailureKind::RateLimited { retry_after } => Disposition::BackOffOrigin {
                        after: retry_after,
                        reason: "probe rate limited",
                    },
                    ProbeFailureKind::Transport => Disposition::RetrySameRange {
                        after: None,
                        reason: "probe failed",
                    },
                };
                self.handle_assignment_disposition(
                    &mut actions,
                    job,
                    None,
                    ByteRange::new(0, 0),
                    disp,
                    0,
                    Some(connection),
                    now,
                );
            }

            Observation::AssignmentVerified {
                job,
                assignment,
                range,
                sample,
                connection,
                connection_reusable,
            } => {
                let assignment = assignment.assignment;
                self.release_assignment_slot(Some(connection));
                if let Some(origin) = self.world.connections.get(connection).map(|c| c.origin) {
                    self.bump_verified(origin, sample.bytes);
                    if let Some(j) = self.world.jobs.get_mut(job) {
                        let w = eff_weight(j, now);
                        j.virt_service += (sample.bytes as f64) / w.max(1e-9);
                    }
                }
                // Accumulate stall evidence for the adaptive brain: time
                // this request spent blocked on memory/destination vs its
                // wall clock. Reset at each measurement window.
                let origin_now = self.world.connections.get(connection).map(|c| c.origin);
                if let Some(origin) = origin_now {
                    if let Some(e) = self.origin_state.get_mut(origin) {
                        e.0 += sample.memory_blocked + sample.destination_blocked;
                        e.1 += sample.response_wall;
                    } else {
                        self.origin_state.insert(
                            origin,
                            (
                                sample.memory_blocked + sample.destination_blocked,
                                sample.response_wall,
                            ),
                        );
                    }
                }
                // Track this connection's live receive rate for piece sizing.
                if sample.stall_fraction() < 0.5 {
                    let entry = if let Some(s) = self.connection_state.get_mut(connection) {
                        s
                    } else {
                        self.connection_state.insert(connection, Default::default());
                        self.connection_state.get_mut(connection).unwrap()
                    };
                    if entry.ewma.is_none() {
                        entry.ewma = Some(crate::core::ewma::EwmaWithVariance::new(
                            Duration::from_secs(4),
                        ));
                    }
                    if let Some(ewma) = entry.ewma.as_mut() {
                        ewma.observe(sample.receive_rate(), sample.response_wall);
                    }
                }
                // Feed endpoint learning. The world model itself rejects
                // samples dominated by destination/memory stalls: a rate
                // measured while waiting on disk is not network capacity.
                self.world.note_endpoint_sample(connection, sample, now);
                // H1 cannot carry another request after a poisoned or
                // truncated response. H2/H3 stream health is independent of
                // the physical connection: closing it here would collapse
                // multiplexed fleets back to one dial after every steal.
                let protocol = self.world.connections.get(connection).map(|c| c.protocol);
                let h1 = matches!(protocol, Some(crate::core::events::Protocol::Http1_1));
                if (h1 && !connection_reusable) || self.retired_and_drained(connection) {
                    actions.push(Action::CloseConnection { connection });
                    self.drop_connection(connection);
                    self.connection_state.remove(connection);
                }
                if let Some(j) = self.world.jobs.get_mut(job)
                    && let Some(plan) = j.plan.as_mut()
                {
                    plan.on_response_verified(assignment, range, now);
                    plan.finish_verified(assignment);
                    let complete = plan.is_complete();
                    let total = plan.total();
                    if complete {
                        j.phase = JobPhase::Committing;
                        let len = total.unwrap_or_else(|| plan.bytes_done());
                        // What the commit must prove: expected digest wins,
                        // otherwise a compute-only request.
                        let integrity = match (j.integrity.expected, j.integrity.compute) {
                            (Some(exp), _) => Some(crate::core::spec::DigestCheck {
                                kind: exp.kind(),
                                expected: Some(*exp.bytes()),
                            }),
                            (None, Some(kind)) => Some(crate::core::spec::DigestCheck {
                                kind,
                                expected: None,
                            }),
                            (None, None) => None,
                        };
                        actions.push(Action::CommitDestination {
                            job,
                            final_length: len,
                            integrity,
                        });
                    }
                }
                if self
                    .world
                    .jobs
                    .get(job)
                    .is_some_and(|j| j.phase.is_draining())
                {
                    self.try_complete_drain(&mut actions, job);
                    return actions;
                }
                let needs_work = self
                    .world
                    .jobs
                    .get(job)
                    .and_then(|j| j.plan.as_ref())
                    .is_some_and(|p| p.admits_worker());
                if let Some(origin) = self.world.jobs.get(job).map(|j| j.origin) {
                    self.consider_early_connection_scale(&mut actions, origin, now, &sample);
                    if needs_work {
                        self.scale_connections(&mut actions, Some(origin), now);
                        self.pump(&mut actions, origin, now);
                    }
                }
                self.world.assert_invariants();
            }

            Observation::AssignmentFailed {
                job,
                assignment,
                attempt,
                disposition,
                connection,
                connection_state,
                ..
            } => {
                let assignment = assignment.assignment;
                self.release_assignment_slot(connection);
                if let ConnectionState::Poisoned | ConnectionState::Gone = connection_state
                    && let Some(conn) = connection
                {
                    actions.push(Action::CloseConnection { connection: conn });
                    self.drop_connection(conn);
                }
                self.handle_assignment_disposition(
                    &mut actions,
                    job,
                    Some(assignment),
                    ByteRange::new(0, 0),
                    disposition,
                    attempt,
                    connection,
                    now,
                );
                // Keep other workers going - across every origin the job
                // still has active sources on.
                let origins: Vec<_> = self
                    .world
                    .jobs
                    .get(job)
                    .map(|j| {
                        j.active_sources
                            .iter()
                            .filter_map(|sid| self.world.sources.get(*sid).map(|s| s.origin))
                            .collect()
                    })
                    .unwrap_or_default();
                for origin in origins {
                    self.scale_connections(&mut actions, Some(origin), now);
                    self.pump(&mut actions, origin, now);
                }
            }

            Observation::MirrorSampled {
                job,
                source,
                connection,
                matches,
                reusable,
            } => {
                let Some(j) = self.world.jobs.get_mut(job) else {
                    return actions;
                };
                j.probing_sources.retain(|s| s != &source);
                j.sampling_sources.retain(|s| s != &source);
                if !matches {
                    j.failed_sources.push(source);
                    actions.push(Action::ReportSourceQuarantined {
                        job,
                        sources: vec![source],
                        reason: "mirror bytes disagreed with verified artifact window".into(),
                    });
                    tracing::warn!(target: "xde::mirror", ?job, ?source, "mirror sampled inconsistent; quarantined");
                    if !reusable {
                        actions.push(Action::CloseConnection { connection });
                        self.drop_connection(connection);
                    }
                    return actions;
                }
                if !j.active_sources.contains(&source) {
                    j.active_sources.push(source);
                }
                tracing::info!(target: "xde::mirror", ?job, ?source, "mirror verified by sampling; activated");
                // The sampling connection stays in the pool as a worker for
                // this mirror. Pump it.
                let origin = self.world.connections.get(connection).map(|c| c.origin);
                if let Some(origin) = origin {
                    self.pump(&mut actions, origin, now);
                }
            }

            Observation::DestinationCommitted {
                job,
                final_length,
                digest,
            } => self.finish_job(&mut actions, job, Ok((final_length, digest)), None),

            Observation::DestinationFailed { job, failure } => {
                let message = match failure.kind {
                    crate::core::controller::DestinationFailureKind::DigestMismatch => {
                        "integrity verification failed; artifact not published".to_string()
                    }
                    crate::core::controller::DestinationFailureKind::NoSpace => {
                        "destination has no space left".to_string()
                    }
                    crate::core::controller::DestinationFailureKind::DestinationError => {
                        "destination failed during commit".to_string()
                    }
                };
                self.finish_job(&mut actions, job, Err(message), None);
            }

            Observation::RateLimited {
                origin,
                retry_after,
            } => {
                let delay = retry_after.unwrap_or(Duration::from_secs(5));
                if let Some(o) = self.world.origins.get_mut(origin) {
                    o.cooldown_until = Some(now + delay);
                }
                self.bump_generation(origin, now);
                actions.push(Action::ScheduleTimer {
                    at: now + delay,
                    event: TimerEvent::OriginCooldownExpired(origin),
                });
            }

            Observation::SourceRefreshed { job, url, headers } => {
                let Some(j) = self.world.jobs.get_mut(job) else {
                    return actions;
                };
                j.refresh_attempts += 1;
                let Some(source_id) = j.active_source() else {
                    return actions;
                };
                let origin_before = j.origin;
                if let Some(s) = self.world.sources.get_mut(source_id) {
                    if let Some(u) = url {
                        s.url = u;
                    }
                    if let Some(h) = headers {
                        s.headers = h;
                    }
                    s.header_identity = crate::core::spec::SourceRequest::fingerprint_of(
                        &url::Url::parse(&s.url)
                            .unwrap_or_else(|_| url::Url::parse("about:invalid").expect("static")),
                        &s.headers,
                    );
                }
                j.phase = JobPhase::Probing;
                let origin = self.world.jobs.get(job).map(|j| j.origin);
                if let Some(origin) = origin {
                    if origin != origin_before {
                        self.bump_generation(origin_before, now);
                        self.bump_generation(origin, now);
                    } else {
                        self.bump_generation(origin, now);
                    }
                    self.open_one_connection(&mut actions, origin, now, true);
                }
            }

            Observation::CredentialRefreshFailed { job, status } => {
                self.finish_job(
                    &mut actions,
                    job,
                    Err(format!(
                        "credentials rejected ({status}) and refresh failed"
                    )),
                    None,
                );
            }

            Observation::TimerExpired { event } => match event {
                TimerEvent::JobDeadline(job) => {
                    self.finish_job(&mut actions, job, Err(DEADLINE_SENTINEL.into()), None);
                }
                TimerEvent::RetryReady { assignment, .. } => {
                    let job = assignment.job;
                    if let Some(j) = self.world.jobs.get_mut(job)
                        && let Some(plan) = j.plan.as_mut()
                    {
                        plan.release_due(now);
                    }
                    if let Some(origin) = self.world.jobs.get(job).map(|j| j.origin) {
                        if self
                            .world
                            .origins
                            .get(origin)
                            .is_none_or(|o| !o.is_cooled_down(now))
                            && !self.world.has_ready_connection(origin)
                        {
                            self.open_one_connection(&mut actions, origin, now, true);
                        }
                        self.pump(&mut actions, origin, now);
                    }
                }
                TimerEvent::ResolveRetry { origin, host, port } => {
                    actions.push(Action::Resolve { origin, host, port });
                }
                TimerEvent::OriginCooldownExpired(origin) => {
                    if let Some(o) = self.world.origins.get_mut(origin) {
                        o.cooldown_until = None;
                    }
                    self.bump_generation(origin, now);
                    self.ensure_probes(&mut actions, origin, now);
                    if self.waiting_probe_job(origin).is_some()
                        && !self.world.has_ready_connection(origin)
                    {
                        self.open_one_connection(&mut actions, origin, now, true);
                    }
                    self.pump(&mut actions, origin, now);
                }
                TimerEvent::CheckpointDue(_) => {}
                TimerEvent::AdaptiveTick { origin } => {
                    self.adaptive_tick(&mut actions, origin, now);
                }
                TimerEvent::RebalanceTick { origin } => {
                    self.rebalance_tick(&mut actions, origin, now);
                }
                TimerEvent::DrainConnection { connection } => {
                    if self.retired_and_drained(connection) {
                        actions.push(Action::CloseConnection { connection });
                        self.drop_connection(connection);
                        self.connection_state.remove(connection);
                    } else if self
                        .world
                        .connections
                        .get(connection)
                        .is_some_and(|c| c.status == ConnectionStatus::Retired)
                    {
                        actions.push(Action::ScheduleTimer {
                            at: now + Self::DRAIN_POLL,
                            event: TimerEvent::DrainConnection { connection },
                        });
                    }
                }
                TimerEvent::EndpointStagger { origin, rank } => {
                    // Still nothing ready on this origin? Race the next
                    // ranked endpoint. Ready connections cancel the race by
                    // retiring losers at ConnectionReady time.
                    if self.world.has_ready_connection(origin)
                        || self
                            .world
                            .origins
                            .get(origin)
                            .is_some_and(|o| o.is_cooled_down(now))
                    {
                        // Race already won or origin cooling down: stop.
                    } else {
                        let ranked = self.world.ranked_endpoints(origin, now, None);
                        if let Some(&endpoint) = ranked.get(rank) {
                            let shard = self.place_shard();
                            self.dial_connection(
                                &mut actions,
                                origin,
                                endpoint,
                                shard,
                                false,
                                None,
                                now,
                            );
                            if rank + 1 < ranked.len() {
                                actions.push(Action::ScheduleTimer {
                                    at: now + CONNECT_STAGGER,
                                    event: TimerEvent::EndpointStagger {
                                        origin,
                                        rank: rank + 1,
                                    },
                                });
                            }
                        }
                    }
                }
            },
        }
        actions
    }

    // ------------------------------------------------------------------
    // Redirects
    // ------------------------------------------------------------------

    /// Follow one hop of a probe redirect. Same-origin hops re-probe in place;
    /// cross-origin hops re-enter admission at the Resolve step with
    /// origin-scoped credentials stripped. Downgrades from https to http and
    /// loops past the policy limit fail the job.
    fn handle_probe_redirect(
        &mut self,
        actions: &mut Vec<Action>,
        job: JobId,
        connection: ConnectionId,
        _status: u16,
        location: &RedirectTarget,
        _now: Instant,
    ) {
        let Some(j) = self.world.jobs.get_mut(job) else {
            return;
        };
        let max_redirects = j.policy.max_redirects;
        let allow_downgrade = j.policy.redirects.allow_https_to_http;
        let forward_credentials = j.policy.redirects.forward_credentials_cross_origin;

        if j.redirect_chain.len() >= usize::from(max_redirects.max(1)) {
            self.finish_job(
                actions,
                job,
                Err(format!("redirect limit exceeded at {location:?}")),
                None,
            );
            return;
        }
        let Ok(url) = url::Url::parse(&location.url) else {
            self.finish_job(
                actions,
                job,
                Err("redirect Location was not a valid URL".into()),
                None,
            );
            return;
        };
        let Some(new_key) = crate::core::ids::TransportOriginKey::from_url(&url) else {
            self.finish_job(
                actions,
                job,
                Err(format!("redirect to unsupported scheme '{}'", url.scheme())),
                None,
            );
            return;
        };

        // Loop detection: an exact URL repeat is a loop regardless of depth.
        {
            let j = self.world.jobs.get(job).expect("checked above");
            let mut chain = j.redirect_chain.clone();
            let before = chain.len();
            chain.push(url.to_string());
            chain.sort_unstable();
            chain.dedup();
            if chain.len() != before + 1 {
                self.finish_job(actions, job, Err("redirect loop detected".into()), None);
                return;
            }
        }

        let Some(source_id) = self.world.jobs.get(job).and_then(|j| j.active_source()) else {
            return;
        };
        let current_origin = self.world.jobs.get(job).map(|j| j.origin);

        // Security default: never follow https -> http unless the policy
        // explicitly allows the downgrade.
        if let Some(key) = current_origin.and_then(|o| self.world.origins.get(o))
            && key.key.scheme == "https"
            && new_key.scheme == "http"
            && !allow_downgrade
        {
            self.finish_job(
                actions,
                job,
                Err("insecure redirect downgrade from https to http rejected".into()),
                None,
            );
            return;
        }

        let old_key = current_origin.and_then(|o| self.world.origins.get(o).map(|n| n.key.clone()));
        match old_key {
            Some(key) if key.same_origin(&new_key) => {
                // Same origin: update the request target and re-probe. The
                // redirect response body was drained by the shard, so a fresh
                // Probe action on the same connection is always safe to issue;
                // if the session was retired, the probe fails and retries on
                // a replacement connection.
                if let Some(s) = self.world.sources.get_mut(source_id) {
                    s.url = url.to_string();
                }
                let j = self.world.jobs.get_mut(job).expect("checked above");
                j.redirect_chain.push(url.to_string());
                actions.push(Action::Probe {
                    job,
                    connection,
                    source: None,
                });
            }
            _ => {
                // Cross-origin: strip origin-scoped credentials unless
                // explicitly forwarded, reset representation evidence, and
                // re-enter admission at the Resolve step.
                if !forward_credentials && let Some(s) = self.world.sources.get_mut(source_id) {
                    s.headers.remove(http::header::AUTHORIZATION);
                    s.headers.remove(http::header::PROXY_AUTHORIZATION);
                    s.headers.remove(http::header::COOKIE);
                    let filtered: http::HeaderMap = s
                        .headers
                        .drain()
                        .filter_map(|(name, value)| {
                            let name = name?;
                            (!value.is_sensitive()).then_some((name, value))
                        })
                        .collect();
                    let _ = std::mem::replace(&mut s.headers, filtered);
                    s.header_identity =
                        crate::core::spec::SourceRequest::fingerprint_of(&url, &s.headers);
                }
                let new_origin = self.world.get_or_create_origin(new_key.clone());
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.origin = new_origin;
                    if let Ok(mut lock) = j.rep_lock.lock() {
                        *lock = Default::default();
                    }
                    j.phase = JobPhase::Created;
                    j.resolve_attempts = 0;
                    j.redirect_chain.push(url.to_string());
                }
                if let Some(s) = self.world.sources.get_mut(source_id) {
                    s.url = url.to_string();
                    s.origin = new_origin;
                }
                // The old connection belongs to the previous origin; retire it.
                actions.push(Action::CloseConnection { connection });
                if current_origin.is_some() {
                    self.drop_connection(connection);
                }
                actions.push(Action::Resolve {
                    origin: new_origin,
                    host: new_key.host.to_string(),
                    port: new_key.port,
                });
            }
        }
    }

    // ------------------------------------------------------------------
    // Adaptive topology
    // ------------------------------------------------------------------

    /// Relative gain below which a measurement is indistinguishable from
    /// noise even without history.
    const ADAPTIVE_NOISE_FLOOR: f64 = 0.05;
    /// Below this many remaining bytes, don't bother experimenting.
    const ADAPTIVE_MIN_REMAINING: u64 = 256 * 1024;
    /// Measurement window per tick.
    const ADAPTIVE_WINDOW: Duration = Duration::from_millis(1500);
    /// How long to let a topology change ramp before measuring its effect.
    const ADAPTIVE_RAMP: Duration = Duration::from_millis(800);
    /// Cooldown after any revert, so one bad experiment cannot thrash.
    const ADAPTIVE_COOLDOWN: Duration = Duration::from_secs(3);
    /// Hard cap on adaptive connections per origin regardless of policy.
    const ADAPTIVE_MAX_CONNS: usize = 8;
    /// A window whose samples spent more than this fraction of wall time
    /// blocked on memory/destination says nothing about network capacity.
    const STALL_EXPERIMENT_GUARD: f64 = 0.5;
    /// How long to wait before re-checking whether a retired connection has
    /// drained its last assignment.
    const DRAIN_POLL: Duration = Duration::from_millis(500);
    /// Medium-timescale straggler rebalance cadence.
    const REBALANCE_INTERVAL: Duration = Duration::from_millis(1000);

    /// True when the connection was retired by a revert decision and has no
    /// assignments left, i.e. it can be closed without aborting work.
    fn retired_and_drained(&self, conn: ConnectionId) -> bool {
        self.world
            .connections
            .get(conn)
            .is_some_and(|c| c.status == ConnectionStatus::Retired)
            && self
                .connection_state
                .get(conn)
                .map(|s| s.in_flight)
                .unwrap_or(0)
                == 0
    }

    /// Open a second connection before the first 1.5s window if the path is
    /// receive-bound and enough work remains. Waiting for a full window lets
    /// a bandwidth-capped transfer finish on one socket.
    fn consider_early_connection_scale(
        &mut self,
        actions: &mut Vec<Action>,
        origin: OriginId,
        now: Instant,
        sample: &crate::core::metrics::TransferSample,
    ) {
        if sample.bytes == 0 || sample.response_wall.is_zero() {
            return;
        }
        let stall = (sample.memory_blocked + sample.destination_blocked).as_secs_f64()
            / sample.response_wall.as_secs_f64().max(1e-9);
        if stall > Self::STALL_EXPERIMENT_GUARD {
            return;
        }
        if self
            .world
            .origins
            .get(origin)
            .is_some_and(|o| o.is_cooled_down(now) || o.topology_experiment.is_some())
        {
            return;
        }
        let remaining: u64 = self
            .world
            .jobs
            .iter()
            .filter(|(_, j)| j.origin == origin && j.phase == JobPhase::Transferring)
            .map(|(_, j)| j.bytes_remaining_estimate())
            .sum();
        if remaining <= Self::ADAPTIVE_MIN_REMAINING {
            return;
        }
        let rate = sample.receive_rate();
        if !rate.is_finite() || rate <= 0.0 {
            return;
        }
        if remaining as f64 / rate < 0.4 {
            return;
        }
        self.open_parallel_connection(actions, origin, now, rate);
    }

    fn open_parallel_connection(
        &mut self,
        actions: &mut Vec<Action>,
        origin: OriginId,
        now: Instant,
        baseline_rate_bps: f64,
    ) {
        if self
            .world
            .origins
            .get(origin)
            .is_some_and(|o| o.is_cooled_down(now) || o.topology_experiment.is_some())
        {
            return;
        }
        let remaining: u64 = self
            .world
            .jobs
            .iter()
            .filter(|(_, j)| j.origin == origin && j.phase == JobPhase::Transferring)
            .map(|(_, j)| j.bytes_remaining_estimate())
            .sum();
        if remaining <= Self::ADAPTIVE_MIN_REMAINING {
            return;
        }
        let live = self.world.live_connections_for_origin(origin);
        let ceiling = self.adaptive_ceiling(origin);
        if live < 1 || live >= ceiling || live >= Self::ADAPTIVE_MAX_CONNS {
            return;
        }
        if self.world.connections.len() >= self.engine_max_connections as usize {
            return;
        }
        let Some(ep) = self.world.select_endpoint(origin, None, now, |_| false) else {
            return;
        };
        let shard = self.place_shard();
        let conn = self.dial_connection(actions, origin, ep, shard, true, None, now);
        if let Some(o) = self.world.origins.get_mut(origin) {
            o.topology_experiment = Some(crate::core::world::TopologyExperiment {
                variable: crate::core::world::TopologyVariable::Connection,
                conn: Some(conn),
                baseline_rate_bps,
                opened_at: now,
                measure_start_at: None,
                measure_start_bytes: 0,
                measured_rate_bps: None,
            });
            o.adaptive_target_conns = o.adaptive_target_conns.max(live + 1);
        }
        self.pump(actions, origin, now);
    }

    /// Forget a connection everywhere: world node, in-flight counter and
    /// rate evidence. The single removal path for the controller.
    fn drop_connection(&mut self, conn: ConnectionId) {
        let shard = self.world.connections.get(conn).map(|c| c.shard);
        let origin = self.world.connections.get(conn).map(|c| c.origin);
        self.world.remove_connection(conn);
        self.connection_state.remove(conn);
        if let Some(s) = shard {
            self.dec_shard_load(s);
        }
        if let Some(o) = origin
            && let Some(cnt) = self.origin_live_conns.get_mut(o)
        {
            *cnt = cnt.saturating_sub(1);
        }
        for (_, j) in self.world.jobs.iter_mut() {
            if let Some(plan) = j.plan.as_mut() {
                for (_, a) in plan.iter_assignments_mut() {
                    if a.connection == Some(conn) {
                        a.connection = None;
                    }
                }
            }
        }
    }

    /// Should the next dial to this origin try HTTP/3? Requires discovery
    /// (HTTPS/SVCB alpn=h3), no active H3 backoff, and a policy that does
    /// not forbid protocol experiments. Failures are remembered
    /// contextually: broken advertisements or blocked UDP are not retried
    /// for every range.
    fn prefer_h3_for(&self, origin: OriginId, now: Instant) -> bool {
        let http1_only = self.world.jobs.iter().any(|(_, j)| {
            j.origin == origin
                && matches!(
                    j.policy.http_version,
                    crate::core::policy::HttpVersionPolicy::Http1Only
                )
        });
        if http1_only {
            return false;
        }
        self.world.origins.get(origin).is_some_and(|o| {
            o.advertises_h3 && o.h3_retry_after.is_none_or(|t| now >= t) && o.h3_failures < 5
        })
    }

    /// Allocate + dispatch one connection. `allow_h3` gates the QUIC path;
    /// `serving` tags the connection with a specific mirror when dialed for
    /// one.
    #[allow(clippy::too_many_arguments)]
    /// Issue (or re-issue) the equivalence sample for a mirror that is
    /// dialed and probed but whose sampling was blocked because no verified
    /// window existed yet.
    fn retry_pending_mirror_sample(
        &mut self,
        actions: &mut Vec<Action>,
        job: JobId,
        _now: Instant,
    ) {
        let Some(j) = self.world.jobs.get(job) else {
            return;
        };
        const SAMPLE_LEN: u64 = 128 * 1024;
        let Some(plan) = j.plan.as_ref() else {
            return;
        };
        let prefix = plan.completed().contiguous_prefix();
        if prefix < SAMPLE_LEN {
            return;
        }
        let probing = j.probing_sources.clone();
        let sampling = j.sampling_sources.clone();
        for sid in probing {
            if sampling.contains(&sid) {
                continue;
            }
            let conn = self
                .world
                .connections
                .iter()
                .find(|(_, c)| c.status == ConnectionStatus::Ready && c.serving_source == Some(sid))
                .map(|(id, _)| id);
            let Some(connection) = conn else { continue };
            if let Some(j) = self.world.jobs.get_mut(job) {
                j.sampling_sources.push(sid);
            }
            let start = prefix - SAMPLE_LEN;
            tracing::info!(target: "xde::mirror", ?job, ?sid, ?start, "sampling mirror against verified data");
            actions.push(Action::SampleMirror {
                job,
                source: sid,
                connection,
                range: ByteRange::new(start, prefix),
            });
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn dial_connection(
        &mut self,
        actions: &mut Vec<Action>,
        origin: OriginId,
        endpoint: EndpointId,
        shard: usize,
        allow_h3: bool,
        serving: Option<SourceId>,
        now: Instant,
    ) -> ConnectionId {
        let prefer_h3 = allow_h3 && self.prefer_h3_for(origin, now);
        // H3 dials go to the Alt-Svc-advertised UDP port when present;
        // TCP dials use the resolved address unchanged.
        let alt_port = self
            .world
            .origins
            .get(origin)
            .and_then(|o| o.h3_alt_port)
            .filter(|_| prefer_h3);
        let conn = self.world.allocate_connection(origin, endpoint, shard);
        self.inc_shard_load(shard);
        {
            let cnt = self.origin_live_conns.get(origin).copied().unwrap_or(0);
            self.origin_live_conns.insert(origin, cnt + 1);
        }
        let intent = match serving {
            Some(source) => {
                let job = self
                    .world
                    .jobs
                    .iter()
                    .find(|(_, j)| {
                        j.probing_sources.contains(&source) || j.sampling_sources.contains(&source)
                    })
                    .map(|(id, _)| id);
                match job {
                    Some(job) => crate::core::world::ConnectionIntent::Mirror { job, source },
                    None => crate::core::world::ConnectionIntent::Pool { origin },
                }
            }
            None => {
                let probe_job = self.waiting_probe_job(origin);
                match probe_job.and_then(|job| {
                    self.world.jobs.get(job).and_then(|j| {
                        j.active_source()
                            .or_else(|| j.sources.first().copied())
                            .map(|source| (job, source))
                    })
                }) {
                    Some((job, source)) => {
                        crate::core::world::ConnectionIntent::Probe { job, source }
                    }
                    None => crate::core::world::ConnectionIntent::Pool { origin },
                }
            }
        };
        if let Some(c) = self.world.connections.get_mut(conn) {
            c.prefer_h3 = prefer_h3;
            c.serving_source = serving;
            c.intent = intent;
        }
        actions.push(Action::OpenConnection {
            connection: conn,
            origin,
            endpoint,
            shard,
            prefer_h3,
            alt_port,
        });
        conn
    }

    /// Noise floor (B/s) for the origin's window-rate series: median absolute
    /// deviation of recent windows, or the relative floor applied to the
    /// baseline while the history is still cold.
    fn noise_floor_bps(&self, origin: OriginId, baseline: f64) -> f64 {
        let Some(o) = self.world.origins.get(origin) else {
            return baseline.max(1.0) * Self::ADAPTIVE_NOISE_FLOOR;
        };
        if o.window_rates.len() < 4 {
            return baseline.max(1.0) * Self::ADAPTIVE_NOISE_FLOOR;
        }
        let mut sorted = o.window_rates.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = sorted[sorted.len() / 2];
        let mut devs: Vec<f64> = o.window_rates.iter().map(|r| (r - median).abs()).collect();
        devs.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        devs[devs.len() / 2].max(baseline.max(1.0) * Self::ADAPTIVE_NOISE_FLOOR)
    }

    /// Stall fraction observed across verified samples in the current
    /// window. High values mean the destination/memory subsystem, not the
    /// network, is pacing the transfer.
    fn window_stall_fraction(&self, origin: OriginId) -> f64 {
        self.origin_state
            .get(origin)
            .map(|(blocked, wall)| {
                if wall.is_zero() {
                    0.0
                } else {
                    (blocked.as_secs_f64() / wall.as_secs_f64()).clamp(0.0, 1.0)
                }
            })
            .unwrap_or(0.0)
    }

    /// One adaptive tick. Full experiment lifecycle:
    ///
    /// ```text
    /// Idle → Opening/Adjusting → Settling(ramp) → Measuring(window)
    ///      → Keep | Revert(drains safely) | Inconclusive(revert + cooldown)
    /// ```
    ///
    /// Decisions are marginal-utility judgments over real TransferSample
    /// evidence: gain must exceed the measured noise floor of recent
    /// windows AND pay the setup cost of the added resource projected over
    /// the remaining artifact size; windows dominated by destination or
    /// memory stalls never judge network topology (a slow disk is not a
    /// slow network).
    fn adaptive_tick(&mut self, actions: &mut Vec<Action>, origin: OriginId, now: Instant) {
        use crate::core::world::ConnectionStatus;
        let Some(o) = self.world.origins.get(origin) else {
            return;
        };
        if !o.adaptive_active || o.is_cooled_down(now) {
            return;
        }
        // Stop when no active jobs remain on this origin.
        let has_active = self.world.jobs.iter().any(|(_, j)| {
            j.origin == origin && matches!(j.phase, JobPhase::Probing | JobPhase::Transferring)
        });
        if !has_active {
            if let Some(o2) = self.world.origins.get_mut(origin) {
                o2.adaptive_active = false;
                o2.topology_experiment = None;
            }
            self.origin_state.remove(origin);
            return;
        }

        // In revert cooldown: wait it out, then resume exploring.
        if let Some(until) = o.experiment_cooldown_until {
            if now < until {
                actions.push(Action::ScheduleTimer {
                    at: until,
                    event: TimerEvent::AdaptiveTick { origin },
                });
                return;
            }
            if let Some(o2) = self.world.origins.get_mut(origin) {
                o2.experiment_cooldown_until = None;
            }
        }

        // Retired connections pending drain: close them once empty.
        let drained: Vec<ConnectionId> = self
            .world
            .connections
            .iter()
            .filter(|(_, c)| c.status == ConnectionStatus::Retired)
            .map(|(id, _)| id)
            .filter(|&id| self.retired_and_drained(id))
            .collect();
        for conn in drained {
            actions.push(Action::CloseConnection { connection: conn });
            self.drop_connection(conn);
            self.connection_state.remove(conn);
        }
        // Re-poll connections still draining.
        for (id, _) in self
            .world
            .connections
            .iter()
            .filter(|(_, c)| c.status == ConnectionStatus::Retired)
        {
            actions.push(Action::ScheduleTimer {
                at: now + Self::DRAIN_POLL,
                event: TimerEvent::DrainConnection { connection: id },
            });
        }

        let live_conns: Vec<ConnectionId> = self
            .world
            .connections
            .iter()
            .filter(|(_, c)| c.origin == origin && c.status == ConnectionStatus::Ready)
            .map(|(id, _)| id)
            .collect();
        let live = live_conns.len();

        // Measure the current window; gather decision inputs.
        let d = {
            let total_verified = self.origin_verified_bytes(origin);
            let stall_fraction = self.window_stall_fraction(origin);
            self.origin_state.remove(origin);
            let ceiling = self.adaptive_ceiling(origin);
            let multiplexed = self.world.has_multiplexed_ready_connection(origin);
            let handshake = live_conns
                .iter()
                .filter_map(|&c| self.world.connections.get(c))
                .filter_map(|c| self.world.endpoints.get(c.endpoint))
                .filter_map(|e| e.handshake_ewma)
                .sum::<Duration>();
            let max_streams = self
                .world
                .jobs
                .iter()
                .filter(|(_, j)| j.origin == origin)
                .map(|(_, j)| u32::from(j.policy.transport.max_streams_per_connection))
                .max()
                .unwrap_or(1);
            let (rate, experiment, stream_ceiling) = {
                let Some(o) = self.world.origins.get_mut(origin) else {
                    return;
                };
                let elapsed = now.saturating_duration_since(o.last_window_at);
                if elapsed < Self::ADAPTIVE_WINDOW {
                    return; // not enough data yet for a clean window
                }
                let delta = total_verified.saturating_sub(o.last_window_bytes);
                let rate = delta as f64 / elapsed.as_secs_f64().max(1e-9);
                o.window_rates.push(rate);
                if o.window_rates.len() > 8 {
                    o.window_rates.remove(0);
                }
                o.last_window_bytes = total_verified;
                o.last_window_at = now;

                let experiment = o
                    .topology_experiment
                    .as_ref()
                    .map(|e| TopologyExperimentView {
                        variable: e.variable,
                        conn: e.conn,
                        baseline: e.baseline_rate_bps,
                        opened_at: e.opened_at,
                        measure_start_at: e.measure_start_at,
                        measure_start: e.measure_start_bytes,
                        already_measured: e.measured_rate_bps.is_some(),
                    });
                (rate, experiment, o.adaptive_target_streams)
            };

            let remaining: Option<u64> = {
                let sum: u64 = self
                    .world
                    .jobs
                    .iter()
                    .filter(|(_, j)| j.origin == origin && j.phase == JobPhase::Transferring)
                    .filter_map(|(_, j)| j.plan.as_ref())
                    .filter_map(|p| p.bytes_remaining())
                    .fold(0u64, |acc, v| acc.saturating_add(v));
                if sum == 0 { None } else { Some(sum) }
            };

            let has_work = self
                .world
                .jobs
                .iter()
                .filter(|(_, j)| j.origin == origin && j.phase == JobPhase::Transferring)
                .any(|(_, j)| {
                    j.plan.as_ref().is_some_and(|p| {
                        p.admits_worker()
                            && p.bytes_remaining()
                                .is_none_or(|r| r > Self::ADAPTIVE_MIN_REMAINING)
                    })
                });

            DecisionInputs {
                rate,
                stall_fraction,
                experiment,
                remaining,
                has_work,
                ceiling,
                live,
                multiplexed,
                handshake,
                stream_ceiling,
                max_streams,
            }
        };

        // Always schedule the next tick while active.
        actions.push(Action::ScheduleTimer {
            at: now + Self::ADAPTIVE_WINDOW,
            event: TimerEvent::AdaptiveTick { origin },
        });

        self.evaluate_experiment(&d, actions, origin, now);
    }

    /// Judge the in-flight experiment, or - when idle - pick the next one.
    fn evaluate_experiment(
        &mut self,
        d: &DecisionInputs,
        actions: &mut Vec<Action>,
        origin: OriginId,
        now: Instant,
    ) {
        use crate::core::world::TopologyVariable;

        if let Some(inv) = self.origin_last_invalidation.get(origin).copied()
            && now.saturating_duration_since(inv) < Self::ADAPTIVE_WINDOW * 2
        {
            tracing::debug!(target: "xde::adaptive", ?origin, "confounded window: recent invalidation");
            return;
        }
        if d.stall_fraction > Self::STALL_EXPERIMENT_GUARD && d.experiment.is_some() {
            tracing::debug!(target: "xde::adaptive", ?origin, stall=d.stall_fraction, "stall-dominated measurement window");
            return;
        }

        if let Some(view) = &d.experiment {
            let TopologyExperimentView {
                variable,
                conn: exp_conn,
                baseline,
                opened_at,
                measure_start_at,
                measure_start,
                already_measured,
            } = *view;
            if now.saturating_duration_since(opened_at) < Self::ADAPTIVE_RAMP {
                return;
            }

            if !already_measured {
                let cursor = self.origin_verified_bytes(origin);
                if let Some(o) = self.world.origins.get_mut(origin)
                    && let Some(exp) = o.topology_experiment.as_mut()
                {
                    exp.measure_start_at = Some(now);
                    exp.measure_start_bytes = cursor;
                    exp.measured_rate_bps = Some(0.0);
                }
                tracing::debug!(
                    target: "xde::adaptive",
                    ?origin,
                    cursor,
                    "measurement window opened"
                );
                return;
            }

            let measured_bytes = self
                .origin_verified_bytes(origin)
                .saturating_sub(measure_start);
            let elapsed = measure_start_at
                .map(|at| now.saturating_duration_since(at).as_secs_f64())
                .unwrap_or(Self::ADAPTIVE_WINDOW.as_secs_f64())
                .max(1e-9);
            let measured_rate = measured_bytes as f64 / elapsed;
            let noise = self.noise_floor_bps(origin, baseline);
            let gain = measured_rate - baseline;
            let remaining = d.remaining.unwrap_or(0);
            let avg_handshake = self
                .origin_avg_handshake
                .get(origin)
                .copied()
                .unwrap_or(d.handshake);
            let setup_cost_bytes = match variable {
                TopologyVariable::Streams => 0.0,
                TopologyVariable::Connection => {
                    avg_handshake.as_secs_f64() * measured_rate.max(1.0)
                }
            };
            let projected_secs = remaining as f64 / measured_rate.max(1024.0);
            let projected_gain_bytes = gain * projected_secs;
            let worth_the_cost = projected_gain_bytes > 2.0 * setup_cost_bytes;
            let clearly_worse = gain < -noise;
            let keep = gain > noise && worth_the_cost && !clearly_worse;

            tracing::info!(
                target: "xde::adaptive",
                ?origin,
                ?variable,
                ?exp_conn,
                baseline_bps = baseline,
                measured_bps = measured_rate,
                noise_bps = noise,
                gain_bps = gain,
                stall_fraction = d.stall_fraction,
                remaining,
                setup_cost_bytes,
                keep,
                "topology experiment complete"
            );

            if keep {
                if let Some(o) = self.world.origins.get_mut(origin) {
                    o.topology_experiment = None;
                    o.last_experiment_variable = Some(variable);
                }
            } else {
                self.revert(variable, exp_conn, actions, origin, now);
            }
            return;
        }

        // --- Idle ---
        if !d.has_work {
            return;
        }
        // Destination/memory pressure: scaling the network cannot help.
        if d.stall_fraction > Self::STALL_EXPERIMENT_GUARD {
            tracing::debug!(
                target: "xde::adaptive",
                ?origin,
                stall = d.stall_fraction,
                "skipping topology experiment: destination/memory bound window"
            );
            return;
        }
        if self
            .world
            .origins
            .get(origin)
            .is_some_and(|o| o.is_cooled_down(now))
        {
            return;
        }

        let first = TopologyVariable::next_after(
            self.world
                .origins
                .get(origin)
                .and_then(|o| o.last_experiment_variable),
        );
        let candidates = [
            first,
            match first {
                TopologyVariable::Connection => TopologyVariable::Streams,
                TopologyVariable::Streams => TopologyVariable::Connection,
            },
        ];

        for candidate in candidates {
            match candidate {
                TopologyVariable::Connection => {
                    if d.live >= d.ceiling || d.live >= Self::ADAPTIVE_MAX_CONNS || d.live < 1 {
                        continue;
                    }
                    // Engine-global ceiling: a job-local optimum must not
                    // exhaust the machine's connection budget.
                    if self.world.connections.len() >= self.engine_max_connections as usize {
                        continue;
                    }
                    let ep = self.world.select_endpoint(origin, None, now, |_| false);
                    let Some(ep) = ep else { continue };
                    let shard = self.place_shard();
                    let conn = self.dial_connection(actions, origin, ep, shard, true, None, now);
                    if let Some(o) = self.world.origins.get_mut(origin) {
                        o.topology_experiment = Some(crate::core::world::TopologyExperiment {
                            variable: TopologyVariable::Connection,
                            conn: Some(conn),
                            baseline_rate_bps: d.rate,
                            opened_at: now,
                            measure_start_at: None,
                            measure_start_bytes: 0,
                            measured_rate_bps: None,
                        });
                        o.adaptive_target_conns += 1;
                    }
                    return;
                }
                TopologyVariable::Streams => {
                    // Streams experiments need a multiplexed connection and
                    // headroom under both the policy ceiling and the global
                    // slot budget per connection.
                    let live_now = self.world.live_connections_for_origin(origin).max(1) as u32;
                    let slots_per_conn = self.engine_max_active_assignments / live_now;
                    if !d.multiplexed
                        || d.stream_ceiling >= d.max_streams
                        || d.stream_ceiling >= slots_per_conn.max(1)
                    {
                        continue;
                    }
                    if let Some(o) = self.world.origins.get_mut(origin) {
                        o.topology_experiment = Some(crate::core::world::TopologyExperiment {
                            variable: TopologyVariable::Streams,
                            conn: None,
                            baseline_rate_bps: d.rate,
                            opened_at: now,
                            measure_start_at: None,
                            measure_start_bytes: 0,
                            measured_rate_bps: None,
                        });
                        o.adaptive_target_streams += 1;
                    }
                    self.pump(actions, origin, now);
                    return;
                }
            }
        }
    }

    /// Undo an experiment. Connections retire FIRST and close only after
    /// their in-flight assignments drain, so reverting never aborts useful
    /// work mid-transfer.
    fn revert(
        &mut self,
        variable: crate::core::world::TopologyVariable,
        exp_conn: Option<ConnectionId>,
        actions: &mut Vec<Action>,
        origin: OriginId,
        now: Instant,
    ) {
        use crate::core::world::{ConnectionStatus, TopologyVariable};
        match variable {
            TopologyVariable::Connection => {
                if let Some(conn) = exp_conn {
                    if self.retired_and_drained(conn) {
                        actions.push(Action::CloseConnection { connection: conn });
                        self.drop_connection(conn);
                        self.connection_state.remove(conn);
                    } else {
                        // Stop new claims; AssignmentVerified or the
                        // DrainConnection timer closes it once empty.
                        if let Some(c) = self.world.connections.get_mut(conn) {
                            c.status = ConnectionStatus::Retired;
                        }
                        actions.push(Action::ScheduleTimer {
                            at: now + Self::DRAIN_POLL,
                            event: TimerEvent::DrainConnection { connection: conn },
                        });
                    }
                }
                if let Some(o) = self.world.origins.get_mut(origin) {
                    o.adaptive_target_conns = o.adaptive_target_conns.saturating_sub(1).max(1);
                }
            }
            TopologyVariable::Streams => {
                if let Some(o) = self.world.origins.get_mut(origin) {
                    o.adaptive_target_streams = o.adaptive_target_streams.saturating_sub(1).max(1);
                }
            }
        }
        if let Some(o) = self.world.origins.get_mut(origin) {
            o.topology_experiment = None;
            o.last_experiment_variable = Some(variable);
            o.experiment_cooldown_until = Some(now + Self::ADAPTIVE_COOLDOWN);
        }
    }

    fn origin_verified_bytes(&self, origin: OriginId) -> u64 {
        self.origin_verified.get(origin).copied().unwrap_or(0)
    }

    fn bump_verified(&mut self, origin: OriginId, bytes: u64) {
        let cur = self.origin_verified.get(origin).copied().unwrap_or(0);
        self.origin_verified
            .insert(origin, cur.saturating_add(bytes));
    }

    /// The maximum useful connection count for this origin based on job
    /// policies. Never exceeds any single job's ceiling.
    fn adaptive_ceiling(&self, origin: OriginId) -> usize {
        self.world
            .jobs
            .iter()
            .filter(|(_, j)| {
                j.origin == origin && matches!(j.phase, JobPhase::Probing | JobPhase::Transferring)
            })
            .map(|(_, j)| usize::from(j.policy.transport.max_physical_connections))
            .max()
            .unwrap_or(1)
    }

    /// Start the adaptive loop for an origin after its first successful
    /// probe. Called once per origin.
    pub fn start_adaptive(&mut self, actions: &mut Vec<Action>, origin: OriginId, now: Instant) {
        let Some(o) = self.world.origins.get(origin) else {
            return;
        };
        if !o.adaptive_active
            && let Some(o) = self.world.origins.get_mut(origin)
        {
            o.adaptive_active = true;
            o.last_window_at = now;
        }
        // Admission/probe are not topology confounders. Clearing here lets
        // the first measurement window actually judge +1 conn/+1 stream.
        self.origin_last_invalidation.remove(origin);
        // Slow timescale: topology experiments.
        actions.push(Action::ScheduleTimer {
            at: now + Self::ADAPTIVE_WINDOW + Self::ADAPTIVE_RAMP,
            event: TimerEvent::AdaptiveTick { origin },
        });
        // Medium timescale: straggler rebalance / work redistribution.
        actions.push(Action::ScheduleTimer {
            at: now + Self::REBALANCE_INTERVAL,
            event: TimerEvent::RebalanceTick { origin },
        });
    }

    /// Medium-timescale control (~1s): shrink stragglers to a fair share of
    /// their own throughput and hand their tails to whoever is faster. The
    /// truncations ride to the owning connections so no bytes are fetched
    /// twice.
    fn rebalance_tick(&mut self, actions: &mut Vec<Action>, origin: OriginId, now: Instant) {
        use crate::core::world::JobPhase;
        tracing::trace!(
            target: "xde::adaptive",
            ?origin,
            jobs = self.world.jobs.len(),
            conns = self.world.connections.len(),
            "rebalance tick"
        );
        // Keep ticking while any job on this origin transfers.
        let active = self
            .world
            .jobs
            .iter()
            .any(|(_, j)| j.origin == origin && j.phase == JobPhase::Transferring);
        if active {
            actions.push(Action::ScheduleTimer {
                at: now + Self::REBALANCE_INTERVAL,
                event: TimerEvent::RebalanceTick { origin },
            });
        }
        let job_ids: Vec<JobId> = self
            .world
            .jobs
            .iter()
            .filter(|(_, j)| j.origin == origin && j.phase == JobPhase::Transferring)
            .map(|(id, _)| id)
            .collect();
        for job in job_ids {
            let cuts = match self.world.jobs.get_mut(job).and_then(|j| j.plan.as_mut()) {
                Some(plan) => plan.rebalance_stragglers(now),
                None => continue,
            };
            for (victim, new_end) in cuts {
                tracing::debug!(
                    target: "xde::segment", ?job, ?victim, new_end,
                    "straggler rebalanced"
                );
                let conn = self
                    .world
                    .jobs
                    .get(job)
                    .and_then(|j| j.plan.as_ref())
                    .and_then(|p| p.assignment(victim))
                    .and_then(|a| a.connection);
                if let Some(conn) = conn {
                    actions.push(Action::TruncateAssignment {
                        job,
                        assignment: AssignmentRef::new(job, victim),
                        connection: conn,
                        new_end,
                    });
                }
            }
        }

        // Mirror mixing: with strong artifact equivalence, activate one
        // additional source per tick while headroom remains. The claim path
        // then distributes ranges across mirrors by live rate - fast
        // sources naturally receive proportionally more work.
        if self.world.connections.len() < self.engine_max_connections as usize {
            let candidates: Vec<JobId> = self
                .world
                .jobs
                .iter()
                .filter(|(_, j)| {
                    j.phase == JobPhase::Transferring
                        && j.strong_equivalence()
                        && (j.unused_sources().next().is_some() || !j.probing_sources.is_empty())
                })
                .map(|(id, _)| id)
                .collect();
            for job in candidates {
                // A mirror may already be dialed+probed but still waiting
                // for enough verified bytes to sample against.
                self.retry_pending_mirror_sample(actions, job, now);
                let Some(sid) = self
                    .world
                    .jobs
                    .get(job)
                    .and_then(|j| j.unused_sources().next())
                else {
                    continue;
                };
                // Large remaining work is what makes an extra mirror worth
                // its probe.
                if self.world.jobs.get(job).is_none_or(|j| {
                    j.bytes_remaining_estimate() <= Self::ADAPTIVE_MIN_REMAINING.saturating_mul(4)
                }) {
                    continue;
                }
                let Some(origin) = self.world.sources.get(sid).map(|s| s.origin) else {
                    continue;
                };
                if self
                    .world
                    .origins
                    .get(origin)
                    .is_some_and(|o| o.is_cooled_down(now))
                {
                    continue;
                }
                // Mark early so ticks don't double-probe.
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.probing_sources.push(sid);
                }
                let resolved = self
                    .world
                    .origins
                    .get(origin)
                    .is_some_and(|o| !o.endpoints.is_empty());
                if !resolved {
                    // Resolver never ran for this mirror: request it now.
                    // The Resolved handler dials the pending probe.
                    if let Some(o) = self.world.origins.get(origin) {
                        let (host, port) = (o.key.host.to_string(), o.key.port);
                        actions.push(Action::Resolve { origin, host, port });
                    }
                    continue;
                }
                let Some(ep) = self.world.select_endpoint(origin, None, now, |_| false) else {
                    continue;
                };
                tracing::info!(target: "xde::mirror", ?job, ?sid, "probing mirror for simultaneous transfer");
                let shard = self.place_shard();
                self.dial_connection(actions, origin, ep, shard, true, Some(sid), now);
            }
        }
        if self
            .world
            .jobs
            .iter()
            .any(|(_, j)| j.origin == origin && j.phase == JobPhase::Transferring)
        {
            self.pump(actions, origin, now);
        }
    }

    fn waiting_probe_job(&self, origin: OriginId) -> Option<JobId> {
        self.world
            .jobs
            .iter()
            .find(|(id, j)| {
                j.origin == origin
                    && j.phase == JobPhase::Created
                    && !self.job_has_inflight_probe(*id)
            })
            .map(|(id, _)| id)
            .or_else(|| {
                self.world
                    .jobs
                    .iter()
                    .find(|(id, j)| {
                        j.origin == origin
                            && j.phase == JobPhase::Probing
                            && !self.job_has_inflight_probe(*id)
                    })
                    .map(|(id, _)| id)
            })
    }

    fn job_has_inflight_probe(&self, job: JobId) -> bool {
        self.world.connections.iter().any(|(_, c)| {
            matches!(
                c.intent,
                crate::core::world::ConnectionIntent::Probe { job: j, .. } if j == job
            ) && matches!(
                c.status,
                ConnectionStatus::Connecting | ConnectionStatus::Ready
            )
        })
    }

    /// Dial or reuse a connection so every Created job on this origin can probe.
    /// Shared-origin jobs must not wait for the first transfer to finish.
    #[allow(clippy::while_let_loop)]
    fn ensure_probes(&mut self, actions: &mut Vec<Action>, origin: OriginId, now: Instant) {
        if self
            .world
            .origins
            .get(origin)
            .is_some_and(|o| o.is_cooled_down(now) || o.endpoints.is_empty())
        {
            return;
        }
        loop {
            let Some(job) = self.waiting_probe_job(origin) else {
                break;
            };
            let idle = self.world.connections.iter().find(|(cid, c)| {
                c.origin == origin
                    && c.status == ConnectionStatus::Ready
                    && c.serving_source.is_none()
                    && self.in_flight_on(*cid) == 0
            });
            if let Some((connection, _)) = idle {
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.phase = JobPhase::Probing;
                }
                if let Some(c) = self.world.connections.get_mut(connection) {
                    let source = self
                        .world
                        .jobs
                        .get(job)
                        .and_then(|j| j.active_source().or_else(|| j.sources.first().copied()));
                    if let Some(source) = source {
                        c.intent = crate::core::world::ConnectionIntent::Probe { job, source };
                    }
                }
                actions.push(Action::Probe {
                    job,
                    connection,
                    source: None,
                });
                continue;
            }
            let Some(ep) = self.world.select_endpoint(origin, None, now, |_| false) else {
                break;
            };
            let shard = self.place_shard();
            self.dial_connection(actions, origin, ep, shard, true, None, now);
            if let Some(j) = self.world.jobs.get_mut(job) {
                j.phase = JobPhase::Probing;
            }
            if !self.job_has_inflight_probe(job) {
                break;
            }
        }
    }

    fn is_runnable(&self, job: JobId, origin: OriginId) -> bool {
        let Some(j) = self.world.jobs.get(job) else {
            return false;
        };
        if j.phase != JobPhase::Transferring || j.phase.is_draining() {
            return false;
        }
        if j.origin != origin
            && !j.active_sources.iter().any(|sid| {
                self.world
                    .sources
                    .get(*sid)
                    .is_some_and(|s| s.origin == origin)
            })
        {
            return false;
        }
        let Some(plan) = j.plan.as_ref() else {
            return false;
        };
        if !plan.admits_worker() {
            return false;
        }
        let ceiling = usize::from(j.policy.transport.max_active_assignments)
            .min(usize::from(j.sink_hints.max_parallel_writes.max(1)));
        if plan.in_flight() >= ceiling {
            return false;
        }
        true
    }

    fn pick_wfq_job(&self, origin: OriginId, now: Instant) -> Option<JobId> {
        if self.active_assignment_count >= self.engine_max_active_assignments as usize {
            return None;
        }
        let mut best: Option<(JobId, f64)> = None;
        for (id, j) in self.world.jobs.iter() {
            if !self.is_runnable(id, origin) {
                continue;
            }
            let w = eff_weight(j, now).max(1e-9);
            let score = j.virt_service;
            let is_better = match best {
                None => true,
                Some((_, s)) => score < s || (score == s && id < best.unwrap().0),
            };
            if is_better {
                let _ = w;
                best = Some((id, score));
            }
        }
        best.map(|(id, _)| id)
    }

    fn pump(&mut self, actions: &mut Vec<Action>, origin: OriginId, now: Instant) {
        let conns: Vec<ConnectionId> = self
            .world
            .origins
            .get(origin)
            .map(|o| o.connections.to_vec())
            .unwrap_or_default();
        loop {
            if self.active_assignment_count >= self.engine_max_active_assignments as usize {
                break;
            }
            let best = conns
                .iter()
                .copied()
                .filter(|&c| {
                    self.world
                        .connections
                        .get(c)
                        .is_some_and(|n| n.status == ConnectionStatus::Ready)
                })
                .filter(|&c| self.in_flight_on(c) < self.stream_capacity(c))
                .min_by_key(|&c| {
                    self.connection_state
                        .get(c)
                        .map(|s| s.in_flight)
                        .unwrap_or(0)
                });
            let Some(conn) = best else { break };
            if !self.claim_one(actions, conn, now) {
                break;
            }
        }
    }

    /// Assignments currently claimed but not finished on this connection.
    fn in_flight_on(&self, conn: ConnectionId) -> usize {
        self.connection_state
            .get(conn)
            .map(|s| s.in_flight)
            .unwrap_or(0)
    }

    /// Best available speed estimate for sizing this connection's next
    /// piece: its own measured receive rate, falling back to the origin's
    /// recent window median, falling back to a conservative floor.
    fn connection_rate_estimate(&self, conn: ConnectionId) -> Rate {
        if let Some(ewma) = self
            .connection_state
            .get(conn)
            .and_then(|s| s.ewma.as_ref())
            && ewma.is_warm()
        {
            return Rate::from_bps(ewma.mean());
        }
        let origin = self.world.connections.get(conn).map(|c| c.origin);
        if let Some(origin) = origin
            && let Some(o) = self.world.origins.get(origin)
            && !o.window_rates.is_empty()
        {
            let mut rates = o.window_rates.clone();
            rates.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            return Rate::from_bps(rates[rates.len() / 2]);
        }
        RESUME_ESTIMATE
    }

    /// How many concurrent assignments this connection may carry: one for
    /// H1 (the physical connection serializes requests), the origin's
    /// adaptive stream target for multiplexed protocols. Retired
    /// connections take nothing new while they drain.
    fn stream_capacity(&self, conn: ConnectionId) -> usize {
        let Some(c) = self.world.connections.get(conn) else {
            return 0;
        };
        if c.status != ConnectionStatus::Ready {
            return 0;
        }
        match c.protocol {
            crate::core::events::Protocol::Http1_1 => 1,
            crate::core::events::Protocol::Http2 | crate::core::events::Protocol::Http3 => self
                .world
                .origins
                .get(c.origin)
                .map_or(1, |o| o.adaptive_target_streams.max(1) as usize),
        }
    }

    /// Claim at most one assignment on this connection for the best eligible
    /// job (WFQ by virt_service). Returns true when a claim was dispatched.
    fn claim_one(
        &mut self,
        actions: &mut Vec<Action>,
        connection: ConnectionId,
        now: Instant,
    ) -> bool {
        let Some(c) = self.world.connections.get(connection) else {
            return false;
        };
        if c.status != ConnectionStatus::Ready {
            return false;
        }
        let origin = c.origin;
        let Some(job) = self.pick_wfq_job(origin, now) else {
            return false;
        };

        let supports_ranges = self.world.jobs.get(job).is_some_and(|j| j.supports_ranges);
        let Some(claim) = self.claim_on_plan(job, connection, now) else {
            return false;
        };
        if let Some(s) = self.connection_state.get_mut(connection) {
            s.in_flight += 1;
        } else {
            self.connection_state.insert(
                connection,
                ConnectionRuntimeState {
                    in_flight: 1,
                    ..Default::default()
                },
            );
        }
        if let Some(cut) = claim.tail_cut {
            // The stolen tail must stop being fetched by its previous
            // owner, or both would download the same bytes.
            actions.push(Action::TruncateAssignment {
                job: cut.job,
                assignment: AssignmentRef::new(cut.job, cut.victim),
                connection: cut.connection,
                new_end: cut.new_end,
            });
        }
        let (assign, range, overlap, frontier) =
            (claim.assign, claim.range, claim.overlap, claim.frontier);
        self.active_assignment_count += 1;
        if !supports_ranges {
            // Full-representation source: the claimed assignment tracks
            // progress, but the wire request carries no Range header.
            actions.push(Action::StartFullBody {
                job,
                assignment: AssignmentRef::new(job, assign),
                connection,
                frontier,
            });
            return true;
        }
        actions.push(Action::StartAssignment {
            job,
            assignment: AssignmentRef::new(job, assign),
            connection,
            range,
            overlap,
            frontier,
        });
        true
    }

    fn release_assignment_slot(&mut self, connection: Option<ConnectionId>) {
        self.active_assignment_count = self.active_assignment_count.saturating_sub(1);
        if let Some(connection) = connection
            && let Some(state) = self.connection_state.get_mut(connection)
        {
            state.in_flight = state.in_flight.saturating_sub(1);
        }
    }

    /// Claim one piece from the job's plan, sized for THIS connection's
    /// measured speed: a 200 MiB/s worker gets proportionally larger pieces
    /// than a 40 MiB/s one, and steal cut-points use both workers' rates.
    /// Returns the claim plus, when a tail was stolen from another
    /// connection, the truncation order for that victim.
    fn claim_on_plan(
        &mut self,
        job: JobId,
        connection: ConnectionId,
        now: Instant,
    ) -> Option<ClaimedPiece> {
        let est = self.connection_rate_estimate(connection);
        let max_conns = self
            .world
            .jobs
            .get(job)
            .map(|j| j.policy.transport.max_physical_connections)
            .unwrap_or(1);
        let solo = max_conns <= 1 && self.stream_capacity(connection) <= 1;
        let j = self.world.jobs.get_mut(job)?;
        let plan = j.plan.as_mut()?;
        let prefix_before = plan.completed().contiguous_prefix();
        match plan.claim_with(est, now, solo, max_conns.max(1)) {
            Claim::Fresh(assign) => {
                let (range, overlap) = plan
                    .assignment(assign)
                    .map(|a| (a.wire_range(), a.overlap))
                    .expect("just-created assignment exists in plan");
                plan.assignment_mut(assign)
                    .expect("just-created")
                    .connection = Some(connection);
                // A fresh claim that continues the verified contiguous
                // prefix advances the artifact frontier.
                let frontier = range.start <= prefix_before;
                Some(ClaimedPiece {
                    assign,
                    range,
                    overlap,
                    frontier,
                    tail_cut: None,
                })
            }
            Claim::Stolen { new, from } => {
                let (range, overlap) = plan
                    .assignment(new)
                    .map(|a| (a.wire_range(), a.overlap))
                    .expect("just-created assignment exists in plan");
                let cut = plan.assignment(new).map(|a| a.range.start);
                plan.assignment_mut(new).expect("just-created").connection = Some(connection);
                let victim_conn = plan.assignment(from).and_then(|a| a.connection);
                // Stolen tails are speculative by definition.
                Some(ClaimedPiece {
                    assign: new,
                    range,
                    overlap,
                    frontier: false,
                    tail_cut: cut.zip(victim_conn).map(|(new_end, conn)| TailCut {
                        job,
                        victim: from,
                        connection: conn,
                        new_end,
                    }),
                })
            }
            Claim::Saturated | Claim::Complete => None,
        }
    }

    /// Open one connection on the best shard, unless the origin is cooling
    /// down. Replacement path for a spent connection. `allow_h3` is true by
    /// default; pass `false` when the caller knows the H3 path is broken
    /// (e.g. right after a TLS handshake failure) so the fallback doesn't
    /// trip the same error.
    fn open_one_connection(
        &mut self,
        actions: &mut Vec<Action>,
        origin: OriginId,
        now: Instant,
        allow_h3: bool,
    ) {
        if self
            .world
            .origins
            .get(origin)
            .is_some_and(|o| o.is_cooled_down(now))
        {
            return;
        }
        if let Some(ep) = self.world.select_endpoint(origin, None, now, |_| false) {
            let shard = self.place_shard();
            self.dial_connection(actions, origin, ep, shard, allow_h3, None, now);
        }
    }

    /// Open connections until the origin reaches its jobs' desired initial
    /// concurrency, spreading them across shards. Never exceeds any job's
    /// `max_physical_connections`, and never opens during an origin cooldown.
    fn scale_connections(
        &mut self,
        actions: &mut Vec<Action>,
        origin: Option<OriginId>,
        now: Instant,
    ) {
        let Some(origin) = origin else { return };
        if self
            .world
            .origins
            .get(origin)
            .is_some_and(|o| o.is_cooled_down(now))
        {
            return;
        }
        let Some((target, max)) = self
            .world
            .jobs
            .iter()
            .filter(|(_, j)| {
                j.origin == origin
                    && matches!(
                        j.phase,
                        JobPhase::Created | JobPhase::Probing | JobPhase::Transferring
                    )
            })
            .fold(None::<(usize, usize)>, |acc, (_, j)| {
                let want = (
                    acc.map_or(
                        usize::from(j.policy.initial_physical_connections),
                        |(t, _): (usize, usize)| {
                            t.max(usize::from(j.policy.initial_physical_connections))
                        },
                    ),
                    acc.map_or(
                        usize::from(j.policy.transport.max_physical_connections),
                        |(_, m): (usize, usize)| {
                            m.max(usize::from(j.policy.transport.max_physical_connections))
                        },
                    ),
                );
                Some(want)
            })
        else {
            return;
        };
        let target = target.min(max).max(1);
        // The adaptive loop refines this over time; scale_connections only
        // handles the initial ramp before measurements are available.
        let effective_target = {
            let adaptive = self
                .world
                .origins
                .get(origin)
                .map_or(usize::MAX, |o| o.adaptive_target_conns);
            target.min(adaptive.max(1))
        };
        while self.world.live_connections_for_origin(origin) < effective_target {
            let Some(ep) = self.world.select_endpoint(origin, None, now, |_| false) else {
                return;
            };
            let shard = self.place_shard();
            self.dial_connection(actions, origin, ep, shard, true, None, now);
        }
    }

    // ------------------------------------------------------------------
    // Failure handling
    // ------------------------------------------------------------------

    #[allow(clippy::too_many_arguments)]
    fn handle_assignment_disposition(
        &mut self,
        actions: &mut Vec<Action>,
        job: JobId,
        assignment: Option<AssignId>,
        range: ByteRange,
        disposition: Disposition,
        attempt: u32,
        connection: Option<ConnectionId>,
        now: Instant,
    ) {
        let max_attempts = self
            .world
            .jobs
            .get(job)
            .map(|j| j.policy.retry.max_attempts_per_range)
            .unwrap_or(6);
        let retry_plan = match &disposition {
            Disposition::RetrySameRange { after, .. } => {
                let next = attempt.saturating_add(1);
                let delay = after.unwrap_or_else(|| {
                    self.retry
                        .backoff(next, &mut || jitter_for(now, self.epoch))
                });
                Some((delay, next))
            }
            _ => None,
        };
        if let Some(assign) = assignment
            && let Some(j) = self.world.jobs.get_mut(job)
            && let Some(plan) = j.plan.as_mut()
        {
            let abandoned = plan.abandon(assign);
            if let (Some((delay, next)), Some(a)) = (retry_plan, abandoned)
                && next <= max_attempts
            {
                plan.defer(a.range, now + delay, next);
            }
        }
        if self
            .world
            .jobs
            .get(job)
            .is_some_and(|j| j.phase.is_draining())
        {
            self.try_complete_drain(actions, job);
        }
        match disposition {
            Disposition::Accept => {}
            Disposition::RetrySameRange { reason, .. } => {
                let _ = reason;
                let Some((delay, next)) = retry_plan else {
                    return;
                };
                match assignment {
                    Some(assign) => {
                        if next > max_attempts {
                            self.begin_fail(actions, job, "range retry budget exhausted".into());
                        } else {
                            actions.push(Action::ScheduleTimer {
                                at: now + delay,
                                event: TimerEvent::RetryReady {
                                    assignment: AssignmentRef::new(job, assign),
                                    range,
                                    attempt: next,
                                },
                            });
                        }
                    }
                    // Probe failure: retry the whole probe on a fresh
                    // connection after backoff.
                    None => {
                        let origin = self.world.jobs.get(job).map(|j| j.origin);
                        if let Some(origin) = origin {
                            if let Some(o) = self.world.origins.get_mut(origin) {
                                o.cooldown_until = Some(now + delay);
                            }
                            actions.push(Action::ScheduleTimer {
                                at: now + delay,
                                event: TimerEvent::OriginCooldownExpired(origin),
                            });
                        }
                    }
                }
            }
            Disposition::BackOffOrigin { after, reason } => {
                let _ = reason;
                let delay = after.unwrap_or_else(|| {
                    self.retry
                        .backoff(attempt + 3, &mut || jitter_for(now, self.epoch))
                });
                let origin = self.world.jobs.get(job).map(|j| j.origin);
                if let Some(origin) = origin {
                    if let Some(o) = self.world.origins.get_mut(origin) {
                        o.cooldown_until = Some(now + delay);
                    }
                    actions.push(Action::ScheduleTimer {
                        at: now + delay,
                        event: TimerEvent::OriginCooldownExpired(origin),
                    });
                }
            }
            Disposition::RefreshCredentials { status } => {
                // Ask the application (via the control loop) for refreshed
                // source information. Bounded by a small attempt budget so
                // a broken refresher cannot loop forever.
                let attempts = self
                    .world
                    .jobs
                    .get(job)
                    .map(|j| j.refresh_attempts)
                    .unwrap_or(0);
                if attempts >= 3 {
                    self.finish_job(
                        actions,
                        job,
                        Err(format!(
                            "credentials rejected ({status}); refresh budget exhausted"
                        )),
                        None,
                    );
                    return;
                }
                let url = self
                    .world
                    .sources
                    .get(
                        self.world
                            .jobs
                            .get(job)
                            .and_then(|j| j.active_source())
                            .unwrap_or_default(),
                    )
                    .map(|s| s.url.clone())
                    .unwrap_or_default();
                actions.push(Action::RequestCredentialRefresh {
                    job,
                    url,
                    status,
                    attempt: attempts,
                });
            }
            Disposition::InvalidateArtifact { reason } => {
                // Everything local is void.
                actions.push(Action::DiscardDestination { job });
                self.finish_job(
                    actions,
                    job,
                    Err(format!("artifact invalidated: {reason}")),
                    None,
                );
            }
            Disposition::FullBodyForRangeRequest => {
                // Downgrade to full-body mode: keep verified ranges, switch
                // the plan to single-stream semantics.
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.supports_ranges = false;
                    let completed = j
                        .plan
                        .as_ref()
                        .map(|p| p.completed().clone())
                        .unwrap_or_default();
                    let mut plan = crate::core::segment::SegmentPlan::new(
                        j.total_length,
                        completed,
                        j.policy.segmentation,
                        j.urgency.piece_duration_target(),
                        0,
                    );
                    plan.set_single_stream(true);
                    j.plan = Some(plan);
                }
                self.open_one_connection(
                    actions,
                    self.world
                        .jobs
                        .get(job)
                        .map(|j| j.origin)
                        .unwrap_or_default(),
                    now,
                    true,
                );
            }
            Disposition::RecheckLength => {
                // Re-probe via a fresh connection.
                if let Some(origin) = self.world.jobs.get(job).map(|j| j.origin) {
                    if let Some(j) = self.world.jobs.get_mut(job) {
                        j.phase = JobPhase::Probing;
                    }
                    self.open_one_connection(actions, origin, now, true);
                }
            }
            Disposition::RangeCapabilityLost => {
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.supports_ranges = false;
                }
            }
            Disposition::QuarantineSource { reason } => {
                // The connection's source served bytes inconsistent with
                // verified artifact data. Quarantine every active source on
                // that connection's origin, drain its unverified work, and
                // let healthy mirrors re-claim it. Independently verified
                // ranges from healthy sources are untouched.
                let conn_origin = connection
                    .and_then(|c| self.world.connections.get(c))
                    .map(|c| c.origin);
                let quarantined: Vec<SourceId> = match (self.world.jobs.get(job), conn_origin) {
                    (Some(j), Some(origin)) => j
                        .active_sources
                        .iter()
                        .copied()
                        .filter(|sid| {
                            self.world
                                .sources
                                .get(*sid)
                                .is_some_and(|s| s.origin == origin)
                        })
                        .collect(),
                    _ => Vec::new(),
                };
                if quarantined.is_empty() {
                    // Cannot attribute the inconsistency to a specific
                    // mirror: representation trust is broken, fail the job.
                    self.finish_job(
                        actions,
                        job,
                        Err(format!("artifact inconsistent across sources: {reason}")),
                        None,
                    );
                    return;
                }
                for sid in &quarantined {
                    if let Some(j) = self.world.jobs.get_mut(job) {
                        j.active_sources.retain(|s| s != sid);
                        if !j.failed_sources.contains(sid) {
                            j.failed_sources.push(*sid);
                        }
                    }
                    tracing::warn!(
                        target: "xde::mirror", ?job, ?sid, reason,
                        "source quarantined"
                    );
                }
                actions.push(Action::ReportSourceQuarantined {
                    job,
                    sources: quarantined.clone(),
                    reason: reason.to_string(),
                });
                // Drain the quarantined connections' in-flight assignments:
                // truncate each so the fetch stops at its next chunk, then
                // abandon in the plan so healthy sources re-claim.
                if let Some(conn) = connection {
                    let victims: Vec<(JobId, AssignId)> = self
                        .world
                        .jobs
                        .iter()
                        .flat_map(|(j, node)| {
                            node.plan.iter().flat_map(move |plan| {
                                plan.iter_assignments().filter_map(move |(a, asg)| {
                                    (asg.connection == Some(conn)).then_some((j, a))
                                })
                            })
                        })
                        .collect();
                    for (v_job, v_assign) in victims {
                        let new_end = self
                            .world
                            .jobs
                            .get_mut(v_job)
                            .and_then(|j| j.plan.as_mut())
                            .map(|p| p.abandon(v_assign).map(|a| a.cursor()))
                            .unwrap_or(None);
                        if let Some(end) = new_end {
                            actions.push(Action::TruncateAssignment {
                                job: v_job,
                                assignment: AssignmentRef::new(v_job, v_assign),
                                connection: conn,
                                new_end: end,
                            });
                        }
                        if let Some(s) = self.connection_state.get_mut(conn) {
                            s.in_flight = s.in_flight.saturating_sub(1);
                        }
                        if let Some(o) = self.world.jobs.get(job).map(|j| j.origin) {
                            self.bump_generation(o, now);
                        }
                    }
                }
                // Failover: if nothing active remains, try the next unused
                // source; with none left, the job fails.
                let has_active = self
                    .world
                    .jobs
                    .get(job)
                    .is_some_and(|j| !j.active_sources.is_empty());
                if !has_active && !self.activate_next_source(actions, job, now) {
                    self.finish_job(
                        actions,
                        job,
                        Err(format!("all sources failed or were quarantined: {reason}")),
                        None,
                    );
                }
            }
            Disposition::Fatal { status, reason } => {
                // A fatal PROBE failure on a job with unused mirrors is a
                // source failure, not an artifact failure: fail over.
                if assignment.is_none() && self.activate_next_source(actions, job, now) {
                    return;
                }
                self.finish_job(
                    actions,
                    job,
                    Err(format!("fatal ({status}): {reason}")),
                    None,
                );
            }
        }
    }

    /// Point the job at its next untried source and re-enter admission.
    /// Returns false when no candidate remains.
    fn activate_next_source(
        &mut self,
        actions: &mut Vec<Action>,
        job: JobId,
        _now: Instant,
    ) -> bool {
        let Some(sid) = self
            .world
            .jobs
            .get(job)
            .and_then(|j| j.unused_sources().next())
        else {
            return false;
        };
        let Some(origin) = self.world.sources.get(sid).map(|s| s.origin) else {
            return false;
        };
        let Some(o) = self.world.origins.get(origin) else {
            return false;
        };
        let (host, port) = (o.key.host.to_string(), o.key.port);
        if let Some(j) = self.world.jobs.get_mut(job) {
            j.active_sources = vec![sid];
            // The job's origin anchor must follow the active source:
            // waiting_probe_job, pump eligibility and snapshot projections
            // all match on it.
            j.origin = origin;
            j.phase = JobPhase::Created;
            j.resolve_attempts = 0;
        }
        tracing::info!(target: "xde::mirror", ?job, ?sid, "failing over to next source");
        actions.push(Action::Resolve { origin, host, port });
        true
    }

    fn retry_resolve(&mut self, actions: &mut Vec<Action>, job: JobId, now: Instant) {
        let attempts = self
            .world
            .jobs
            .get(job)
            .map(|j| j.resolve_attempts)
            .unwrap_or(0);
        if self
            .world
            .jobs
            .get(job)
            .is_none_or(|j| attempts >= j.policy.retry.max_attempts_per_range.max(3))
        {
            self.finish_job(actions, job, Err("resolution failed".into()), None);
            return;
        }
        let (origin, host, port) = {
            let j = self.world.jobs.get(job).expect("checked above");
            let o = self.world.origins.get(j.origin).expect("job origin exists");
            (j.origin, o.key.host.to_string(), o.key.port)
        };
        if let Some(j) = self.world.jobs.get_mut(job) {
            j.resolve_attempts += 1;
        }
        let delay = self
            .retry
            .backoff(attempts, &mut || jitter_for(now, self.epoch));
        actions.push(Action::ScheduleTimer {
            at: now + delay,
            event: TimerEvent::ResolveRetry { origin, host, port },
        });
    }

    fn begin_fail(&mut self, actions: &mut Vec<Action>, job: JobId, error: String) {
        let Some(j) = self.world.jobs.get_mut(job) else {
            return;
        };
        if j.phase.is_terminal() || j.phase.is_draining() {
            return;
        }
        j.phase = JobPhase::Failing;
        j.pending_terminal = Some(Err(error));
        self.try_complete_drain(actions, job);
    }

    fn try_complete_drain(&mut self, actions: &mut Vec<Action>, job: JobId) {
        let Some(j) = self.world.jobs.get(job) else {
            return;
        };
        if !j.phase.is_draining() {
            return;
        }
        let in_flight = j.plan.as_ref().map_or(0, |p| p.in_flight());
        if in_flight > 0 {
            return;
        }
        let outcome = j.pending_terminal.clone().unwrap_or(Err("drained".into()));
        self.finish_job(actions, job, outcome, None);
    }

    fn on_intent_connect_failed(
        &mut self,
        actions: &mut Vec<Action>,
        job: JobId,
        source: Option<SourceId>,
        kind: ConnectFailure,
        origin_of_failed: Option<OriginId>,
        now: Instant,
    ) {
        if !kind.retryable() {
            self.begin_fail(actions, job, "connection failed: tls".into());
            return;
        }
        let has_mirror = self.world.jobs.get(job).is_some_and(|j| {
            source.is_some_and(|sid| {
                self.world
                    .sources
                    .get(sid)
                    .is_some_and(|s| Some(s.origin) == origin_of_failed)
            }) && j.unused_sources().next().is_some()
        });
        if has_mirror && self.activate_next_source(actions, job, now) {
            return;
        }
        self.retry_resolve(actions, job, now);
    }

    /// Terminal transition: release lease, reply to waiter, remove the node.
    fn finish_job(
        &mut self,
        actions: &mut Vec<Action>,
        job: JobId,
        outcome: Result<(u64, Option<crate::core::spec::Digest>), String>,
        resumed: Option<u64>,
    ) {
        let Some(mut j) = self.world.jobs.remove(job) else {
            return;
        };
        j.phase = match &outcome {
            Ok(_) => JobPhase::Completed,
            Err(_) => JobPhase::Failed,
        };
        // Release the lease directly: the job node is already removed.
        if let Some(d) = self.world.destinations.get_mut(j.destination)
            && d.leased_by == Some(job)
        {
            d.leased_by = None;
        }
        let origin = j.origin;
        match outcome {
            Ok((bytes, digest)) => {
                let resumed_bytes = j.resumed_bytes;
                actions.push(Action::CompleteJob {
                    job,
                    total_bytes: bytes,
                    digest,
                    resumed_bytes,
                })
            }
            Err(e) => actions.push(Action::FailJob { job, error: e }),
        }
        let _ = resumed;
        self.ensure_probes(actions, origin, Instant::now());
        self.world.assert_invariants();
    }
}

/// Full-jitter source derived from the *injected* observation timestamp
/// relative to the controller's own epoch, not the wall clock: the control
/// loop stays deterministic and replayable while retries remain spread out.
/// Deadline pressure multiplier for the slot allocator: 1.0 without a
/// deadline, growing as the required average rate exceeds a nominal single-
/// stream rate. Bounded so one dying job cannot flatten all others.
fn deadline_pressure(deadline: Option<Instant>, remaining_bytes: u64, now: Instant) -> f64 {
    const NOMINAL_STREAM_RATE: f64 = 10.0 * 1024.0 * 1024.0;
    let Some(dl) = deadline else { return 1.0 };
    if remaining_bytes == 0 {
        return 1.0;
    }
    let left = dl.saturating_duration_since(now);
    if left.is_zero() {
        return 8.0;
    }
    let required = remaining_bytes as f64 / left.as_secs_f64();
    (required / NOMINAL_STREAM_RATE).clamp(1.0, 8.0)
}

fn eff_weight(j: &crate::core::world::JobNode, now: Instant) -> f64 {
    let remaining = j
        .plan
        .as_ref()
        .and_then(|p| p.bytes_remaining())
        .unwrap_or(0);
    j.priority.weight() * j.urgency.allocator_bias() * deadline_pressure(j.deadline, remaining, now)
}

impl Controller {
    fn bump_generation(&mut self, origin: OriginId, now: Instant) {
        let g = self.origin_generation.get(origin).copied().unwrap_or(0);
        self.origin_generation.insert(origin, g + 1);
        self.origin_last_invalidation.insert(origin, now);
    }
}

fn jitter_for(now: Instant, epoch: Instant) -> f64 {
    let nanos = now.saturating_duration_since(epoch).subsec_nanos();
    ((nanos % 997) as f64 + 1.0) / 998.0
}

#[cfg(test)]
#[path = "controller_tests.rs"]
mod tests;
