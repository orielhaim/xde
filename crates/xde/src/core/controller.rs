//! The pure control-plane state machine.
//!
//! `Engine` Ã¢â€ â€™ `ControlLoop` Ã¢â€ â€™ `Controller`. The controller owns the
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
    world::{ConnectionStatus, JobFailure, JobPhase, WorldModel},
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
    DestinationPreparing {
        job: JobId,
    },
    DestinationPrepared {
        job: JobId,
        result: Result<(), DestinationFailure>,
    },
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
    JobPaused {
        job: JobId,
    },
    JobResumed {
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
        sample: Box<crate::core::metrics::TransferSample>,
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
    PrepareDestination,
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
    FailJob {
        job: JobId,
        error: JobFailure,
    },
    CancelTransfer {
        job: JobId,
    },
    ResetTransfer {
        job: JobId,
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

enum ClaimedPiece {
    Transfer {
        assign: AssignId,
        range: ByteRange,
        overlap: u32,
        frontier: bool,
    },
    Yield {
        assignment: AssignId,
        connection: ConnectionId,
        new_end: u64,
    },
}

#[derive(Debug, Clone, Copy, Default)]
struct ConnectionRuntimeState {
    in_flight: usize,
    ewma: Option<crate::core::ewma::EwmaWithVariance>,
}

#[derive(Debug, Clone, Copy)]
struct ConnectionReservation {
    owner: JobId,
    origin: OriginId,
    shard: usize,
}

/// The deterministic controller. Owns the WorldModel.
#[derive(Debug)]
pub struct Controller {
    pub world: WorldModel,
    pub stats: crate::snapshot::ControllerStats,
    retry: RetryPolicy,
    epoch: Instant,
    limits: crate::core::policy::EngineLimits,
    contended: bool,
    active_assignment_count: usize,
    shard_load: Vec<usize>,
    connection_state: slotmap::SecondaryMap<ConnectionId, ConnectionRuntimeState>,
    // Closing reservations can coexist with a newer arena generation.
    reservations: std::collections::HashMap<ConnectionId, ConnectionReservation>,
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
            stats: crate::snapshot::ControllerStats {
                planner_bytes_per_job: std::mem::size_of::<crate::core::planner::JobPlanner>(),
                assignment_control_bytes: std::mem::size_of::<
                    crate::core::segment::AssignmentControl,
                >(),
                ..Default::default()
            },
            limits: Default::default(),
            contended: false,
            retry: RetryPolicy::default(),
            epoch: Instant::now(),
            active_assignment_count: 0,
            shard_load: vec![0; shards],
            connection_state: slotmap::SecondaryMap::new(),
            reservations: std::collections::HashMap::new(),
        }
    }

    /// Apply the engine's global ceilings (called once at construction by the
    /// control plane).
    pub fn set_engine_limits(&mut self, limits: &crate::core::policy::EngineLimits) {
        self.limits = *limits;
    }

    pub fn max_jobs(&self) -> usize {
        self.limits.max_jobs
    }

    fn refresh_grants(&mut self, now: Instant) {
        self.stats.allocation_passes += 1;
        use crate::core::allocator::{Demand, ResourceGrant, allocate};
        let demand: Vec<_> = self
            .world
            .jobs
            .iter()
            .filter(|(_, j)| {
                matches!(
                    j.phase,
                    JobPhase::Created
                        | JobPhase::Probing
                        | JobPhase::Transferring
                        | JobPhase::Committing
                )
            })
            .map(|(job, j)| {
                let target = j.planner.desired();
                let multiplexed = self.world.connections.values().any(|c| {
                    c.owner == Some(job)
                        && c.status == ConnectionStatus::Ready
                        && c.protocol != crate::core::events::Protocol::Http1_1
                });
                let segmented =
                    j.supports_ranges && crate::core::sink::supports_segmentation(j.sink_caps);
                let connections = match j.phase {
                    JobPhase::Committing => 0,
                    JobPhase::Transferring if segmented => u16::from(target.connections),
                    _ => 1,
                };
                let assignments = if connections == 0 {
                    0
                } else if segmented {
                    (u32::from(connections)
                        * if multiplexed {
                            u32::from(target.streams_per_connection)
                        } else {
                            1
                        })
                    .min(u32::from(j.policy.transport.max_active_assignments))
                    .min(j.sink_hints.max_parallel_writes.max(1) as u32) as u16
                } else {
                    1
                };
                Demand {
                    job,
                    order: j.admission_order,
                    origin: j.origin,
                    connections,
                    assignments,
                    weight: j.priority.weight(),
                    service: j.virt_service
                        + j.plan
                            .iter()
                            .flat_map(|p| p.iter_assignments())
                            .map(|(_, a)| {
                                a.control
                                    .end
                                    .load(std::sync::atomic::Ordering::Relaxed)
                                    .saturating_sub(a.range.start)
                                    .saturating_sub(a.control.bytes())
                                    .min(self.limits.fair_quantum_bytes)
                            })
                            .sum::<u64>() as f64
                            / j.priority.weight(),
                    active: j.activated,
                    productivity: j.planner.productivity(now),
                }
            })
            .collect();
        let grants = allocate(&demand, self.limits);
        self.contended = demand.len() > 1
            && demand.iter().any(|d| {
                let granted = grants
                    .iter()
                    .find(|(job, _)| *job == d.job)
                    .map_or(ResourceGrant::default(), |(_, g)| *g);
                granted.connections < d.connections || granted.assignments < d.assignments
            });
        for (job, j) in self.world.jobs.iter_mut() {
            let next = grants
                .iter()
                .find(|(id, _)| *id == job)
                .map_or(ResourceGrant::default(), |(_, grant)| *grant);
            let requested = demand.iter().find(|d| d.job == job);
            if requested.is_some_and(|d| {
                (next.connections < j.grant.connections && next.connections < d.connections)
                    || (next.assignments < j.grant.assignments && next.assignments < d.assignments)
            }) {
                j.planner.invalidate();
            }
            j.grant = next;
        }
    }

    fn reconcile(&mut self, actions: &mut Vec<Action>, now: Instant) {
        self.refresh_grants(now);
        let competition =
            self.contended && matches!(self.limits.scheduling, crate::SchedulingPolicy::FairShare);
        if competition {
            for (_, j) in self.world.jobs.iter_mut() {
                if !j.supports_ranges {
                    continue;
                }
                if let Some(plan) = j.plan.as_mut() {
                    for (_, a) in plan.iter_assignments_mut() {
                        let end = a
                            .range
                            .start
                            .saturating_add(a.control.bytes())
                            .saturating_add(self.limits.fair_quantum_bytes);
                        if end < a.range.end {
                            a.control
                                .end
                                .fetch_min(end, std::sync::atomic::Ordering::Relaxed);
                            a.truncated = true;
                        }
                    }
                }
            }
        }
        for (job, j) in self.world.jobs.iter_mut() {
            let target = j.planner.desired();
            if !j.planner.adaptive()
                || !j.supports_ranges
                || j.grant.connections < u16::from(target.connections)
                || u32::from(j.grant.assignments)
                    < target
                        .slots()
                        .min(u32::from(j.policy.transport.max_active_assignments))
            {
                continue;
            }
            let keep: Vec<_> = self
                .world
                .connections
                .iter()
                .filter(|(_, c)| c.owner == Some(job) && c.status == ConnectionStatus::Ready)
                .take(usize::from(target.connections))
                .map(|(id, _)| id)
                .collect();
            let mut slots = vec![0usize; keep.len()];
            if let Some(plan) = j.plan.as_mut() {
                for (_, a) in plan.iter_assignments_mut() {
                    let retained = a
                        .connection
                        .and_then(|c| keep.iter().position(|id| *id == c))
                        .is_some_and(|index| {
                            slots[index] += 1;
                            slots[index] <= usize::from(target.streams_per_connection)
                        });
                    if !retained {
                        // Keep the range reservation until the sink acknowledges its actual end.
                        let end = a
                            .range
                            .start
                            .saturating_add(a.control.bytes())
                            .saturating_add(j.policy.segmentation.min_piece);
                        if end < a.range.end {
                            a.control
                                .end
                                .fetch_min(end, std::sync::atomic::Ordering::Relaxed);
                            a.truncated = true;
                        }
                    }
                }
            }
        }
        // Objects already executing keep their reservation until they drain.
        // New admissions use the reduced grant immediately.
        let mut owned = slotmap::SecondaryMap::<JobId, usize>::new();
        for c in self.world.connections.values() {
            if let Some(owner) = c.owner {
                *owned.entry(owner).unwrap().or_insert(0) += 1;
            }
        }
        let idle: Vec<_> = self
            .world
            .connections
            .iter()
            .filter(|(connection, c)| {
                c.status == ConnectionStatus::Ready
                    && self.in_flight_on(*connection) == 0
                    && !self.control_busy(*connection)
            })
            .map(|(id, _)| id)
            .collect();
        for connection in idle {
            let c = &self.world.connections[connection];
            let Some(owner) = c.owner else {
                continue;
            };
            let limit = self
                .world
                .jobs
                .get(owner)
                .map_or(0, |j| usize::from(j.grant.connections));
            if owned[owner] <= limit {
                continue;
            }
            owned[owner] -= 1;
            let recipient = self
                .world
                .jobs
                .iter()
                .filter(|(job, j)| {
                    *job != owner
                        && j.origin == c.origin
                        && j.grant.assignments > 0
                        && usize::from(j.grant.connections) > owned.get(*job).copied().unwrap_or(0)
                        && c.serving_source.is_none()
                        && Self::protocol_allowed(&j.policy, c.protocol)
                })
                .min_by(|(a, x), (b, y)| x.virt_service.total_cmp(&y.virt_service).then(a.cmp(b)))
                .map(|(job, _)| job);
            if let Some(job) = recipient {
                let c = &mut self.world.connections[connection];
                c.owner = Some(job);
                c.intent = crate::core::world::ConnectionIntent::Pool { origin: c.origin };
                if let Some(r) = self.reservations.get_mut(&connection) {
                    r.owner = job;
                }
                *owned.entry(job).unwrap().or_insert(0) += 1;
            } else {
                actions.push(Action::CloseConnection { connection });
                self.drop_connection(connection);
            }
        }
        let activate: Vec<_> = self
            .world
            .jobs
            .iter()
            .filter(|(_, j)| !j.activated && j.grant.connections > 0)
            .map(|(job, _)| job)
            .collect();
        for job in activate {
            let j = &mut self.world.jobs[job];
            j.activated = true;
            let origin = j.origin;
            if let Some(o) = self.world.origins.get(origin)
                && o.endpoints.is_empty()
            {
                actions.push(Action::Resolve {
                    origin,
                    host: o.key.host.to_string(),
                    port: o.key.port,
                });
            }
        }
        let candidates: Vec<_> = self
            .world
            .jobs
            .iter()
            .filter(|(job, j)| {
                j.phase == JobPhase::Transferring
                    && j.strong_equivalence()
                    && j.probing_sources.is_empty()
                    && j.sampling_sources.is_empty()
                    && j.plan
                        .as_ref()
                        .is_some_and(|p| p.completed().contiguous_prefix() >= 128 * 1024)
                    && self
                        .reservations
                        .values()
                        .filter(|r| r.owner == *job)
                        .count()
                        < usize::from(j.grant.connections)
            })
            .filter_map(|(job, j)| j.unused_sources().next().map(|source| (job, source)))
            .collect();
        for (job, source) in candidates {
            let origin = self.world.sources[source].origin;
            if self.world.origins[origin].is_cooled_down(now) {
                continue;
            }
            self.world.jobs[job].probing_sources.push(source);
            if let Some(endpoint) = self.world.select_endpoint(origin, None, now, |_| false) {
                self.dial_connection(
                    Some(job),
                    actions,
                    origin,
                    endpoint,
                    self.place_shard(),
                    true,
                    Some(source),
                    now,
                );
            } else {
                let o = &self.world.origins[origin];
                actions.push(Action::Resolve {
                    origin,
                    host: o.key.host.to_string(),
                    port: o.key.port,
                });
            }
        }
        let connections = &self.world.connections;
        let source_demand: Vec<_> = self
            .world
            .jobs
            .iter()
            .filter(|(_, j)| j.phase == JobPhase::Transferring)
            .flat_map(|(job, j)| {
                j.probing_sources
                    .iter()
                    .chain(
                        j.active_sources
                            .iter()
                            .filter(|source| **source != j.primary_source()),
                    )
                    .copied()
                    .filter(move |source| {
                        !connections
                            .values()
                            .any(|c| c.owner == Some(job) && c.serving_source == Some(*source))
                    })
                    .map(move |source| (job, source))
            })
            .collect();
        for (job, source) in source_demand {
            let origin = self.world.sources[source].origin;
            if self.world.origins[origin].is_cooled_down(now) {
                continue;
            }
            if let Some(endpoint) = self.world.select_endpoint(origin, None, now, |_| false) {
                self.dial_connection(
                    Some(job),
                    actions,
                    origin,
                    endpoint,
                    self.place_shard(),
                    true,
                    Some(source),
                    now,
                );
            }
        }
        let origins: Vec<_> = self.world.origins.keys().collect();
        for origin in origins {
            self.ensure_probes(actions, origin, now);
            self.scale_connections(actions, Some(origin), now);
        }
        if let Some(origin) = self.world.origins.keys().next() {
            self.pump(actions, origin, now);
        }
    }

    fn control_busy(&self, connection: ConnectionId) -> bool {
        self.world
            .connections
            .get(connection)
            .is_some_and(|c| match c.intent {
                crate::core::world::ConnectionIntent::Probe { job, .. } => self
                    .world
                    .jobs
                    .get(job)
                    .is_some_and(|j| j.phase == JobPhase::Probing),
                crate::core::world::ConnectionIntent::Mirror { job, source } => {
                    self.world.jobs.get(job).is_some_and(|j| {
                        j.probing_sources.contains(&source) || j.sampling_sources.contains(&source)
                    })
                }
                _ => false,
            })
    }

    fn probe_count(&self) -> usize {
        self.world
            .connections
            .keys()
            .filter(|id| self.control_busy(*id))
            .count()
    }

    fn protocol_allowed(policy: &crate::TransferPolicy, protocol: crate::Protocol) -> bool {
        use crate::{HttpVersionPolicy as H, Protocol as P};
        match policy.http_version {
            H::Auto => true,
            H::Http1Only => protocol == P::Http1_1,
            H::Http2PriorKnowledge => protocol == P::Http2,
            H::Http3Only => protocol == P::Http3,
        }
    }

    fn occupied_slots(&self) -> usize {
        self.active_assignment_count + self.probe_count()
    }

    pub fn reserved_connections(&self) -> usize {
        self.reservations.len()
    }

    pub fn globally_constrained(&self, job: JobId) -> bool {
        let Some(j) = self.world.jobs.get(job) else {
            return false;
        };
        let target = j.planner.desired();
        let owned = self
            .reservations
            .values()
            .filter(|r| r.owner == job)
            .count();
        j.grant.connections < u16::from(target.connections)
            || j.grant.assignments == 0
            || (owned < usize::from(j.grant.connections)
                && (self.reservations.len() >= usize::from(self.limits.max_physical_connections)
                    || self
                        .reservations
                        .values()
                        .filter(|r| r.origin == j.origin)
                        .count()
                        >= usize::from(self.limits.max_connections_per_origin)))
    }

    pub fn connection_shard(&self, connection: ConnectionId) -> Option<usize> {
        self.reservations
            .get(&connection)
            .map(|r| r.shard)
            .or_else(|| self.world.connections.get(connection).map(|c| c.shard))
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
        self.stats.observations += 1;
        #[cfg(feature = "diagnostics")]
        let started = Instant::now();
        let result = self.handle_reconciled(obs, now);
        #[cfg(feature = "diagnostics")]
        {
            self.stats.active_time += started.elapsed();
        }
        result
    }

    fn handle_reconciled(&mut self, obs: Observation, now: Instant) -> Vec<Action> {
        self.refresh_grants(now);
        let control_result = match &obs {
            Observation::Probed {
                job, connection, ..
            }
            | Observation::ProbeFailed {
                job, connection, ..
            }
            | Observation::ProbeRedirected {
                job, connection, ..
            }
            | Observation::MirrorSampled {
                job, connection, ..
            } => Some((*job, *connection)),
            _ => None,
        };
        if let Some((job, connection)) = control_result
            && self
                .world
                .connections
                .get(connection)
                .is_none_or(|c| c.owner != Some(job))
        {
            let mut actions = Vec::new();
            self.reconcile(&mut actions, now);
            return actions;
        }
        if let Some((job, connection)) = control_result
            && self
                .world
                .jobs
                .get(job)
                .is_some_and(|j| j.phase.is_draining() || j.phase == JobPhase::Paused)
        {
            let mut actions = vec![Action::CloseConnection { connection }];
            if let Some(j) = self.world.jobs.get_mut(job)
                && let Some(source) = self
                    .world
                    .connections
                    .get(connection)
                    .and_then(|c| c.serving_source)
            {
                j.probing_sources.retain(|sid| *sid != source);
                j.sampling_sources.retain(|sid| *sid != source);
            }
            self.drop_connection(connection);
            self.try_complete_drain(&mut actions, job);
            self.reconcile(&mut actions, now);
            return actions;
        }
        let mut actions = self.handle_event(obs, now);
        self.reconcile(&mut actions, now);
        actions
    }

    #[allow(clippy::match_like_matches_macro)]
    fn handle_event(&mut self, obs: Observation, now: Instant) -> Vec<Action> {
        let mut actions = Vec::new();
        match obs {
            Observation::DestinationPreparing { job } => {
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.phase = JobPhase::Preparing;
                }
            }
            Observation::DestinationPrepared { job, result } => {
                if let Some(j) = self.world.jobs.get_mut(job) {
                    let failure = match &j.phase {
                        JobPhase::Preparing => result.err().map(|error| {
                            format!("destination preparation failed: {:?}", error.kind).into()
                        }),
                        JobPhase::StoppingPreparation(error) => Some(error.clone()),
                        _ => return actions,
                    };
                    j.phase = JobPhase::Created;
                    if let Some(error) = failure {
                        self.begin_fail(&mut actions, job, error);
                    }
                }
            }
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
                    .filter(|(id, peer)| *id != job && peer.phase == JobPhase::Transferring)
                    .map(|(_, peer)| peer.virt_service)
                    .fold(f64::INFINITY, f64::min);
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.integrity = spec.integrity.clone();
                    j.resume = resume.map(|b| *b);
                    if min_peer.is_finite() {
                        j.virt_service = min_peer;
                    }
                }
                self.invalidate_origin_measurements(origin_for_gen, now);
            }

            Observation::JobPaused { job } => {
                let pending: Vec<_> = self
                    .world
                    .connections
                    .iter()
                    .filter(|(_, c)| {
                        c.owner == Some(job)
                            && c.serving_source.is_some_and(|source| {
                                self.world.jobs.get(job).is_some_and(|j| {
                                    j.probing_sources.contains(&source)
                                        || j.sampling_sources.contains(&source)
                                })
                            })
                    })
                    .map(|(id, _)| id)
                    .collect();
                for connection in pending {
                    actions.push(Action::CloseConnection { connection });
                    self.drop_connection(connection);
                }
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.probing_sources.clear();
                    j.sampling_sources.clear();
                }

                if let Some(j) = self.world.jobs.get_mut(job) {
                    match j.phase {
                        JobPhase::Created => {
                            j.phase = JobPhase::Paused;
                            j.activated = false;
                        }
                        JobPhase::Transferring => {
                            j.phase = JobPhase::Pausing;
                            j.activated = false;
                            j.planner.invalidate();
                            j.planner_due = None;
                            actions.push(Action::CancelTransfer { job });
                            self.try_complete_drain(&mut actions, job);
                        }
                        _ => {}
                    }
                }
            }
            Observation::JobResumed { job } => {
                let floor = self
                    .world
                    .jobs
                    .values()
                    .filter(|j| j.activated)
                    .map(|j| j.virt_service)
                    .fold(f64::INFINITY, f64::min);
                if let Some(j) = self.world.jobs.get_mut(job)
                    && j.phase == JobPhase::Paused
                {
                    j.phase = if j.plan.is_some() {
                        JobPhase::Transferring
                    } else {
                        JobPhase::Created
                    };
                    if floor.is_finite() {
                        j.virt_service = floor;
                    }
                    let origin = j.origin;
                    self.start_adaptive(&mut actions, origin, now);
                }
            }

            Observation::JobCancelled { job } => {
                self.begin_fail(&mut actions, job, JobFailure::Cancelled);
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

                if failed || endpoints.is_empty() {
                    let pending: Vec<_> = self
                        .world
                        .jobs
                        .iter()
                        .flat_map(|(job, j)| {
                            j.probing_sources
                                .iter()
                                .copied()
                                .filter(|source| self.world.sources[*source].origin == origin)
                                .map(move |source| (job, source))
                        })
                        .collect();
                    for (job, source) in pending {
                        let j = &mut self.world.jobs[job];
                        j.probing_sources.retain(|sid| *sid != source);
                        j.failed_sources.push(source);
                    }
                }
                let Some(job) = self.waiting_probe_job(origin) else {
                    return actions;
                };
                if failed || endpoints.is_empty() {
                    self.retry_resolve(&mut actions, job, now);
                    return actions;
                }
                if let Some(endpoint) = self.world.select_endpoint(origin, None, now, |_| false) {
                    let shard = self.place_shard();
                    self.dial_connection(
                        Some(job),
                        &mut actions,
                        origin,
                        endpoint,
                        shard,
                        true,
                        None,
                        now,
                    );
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
                if !self.world.connections.contains_key(connection) {
                    actions.push(Action::CloseConnection { connection });
                    return actions;
                }
                if let Some(c) = self.world.connections.get_mut(connection) {
                    c.status = ConnectionStatus::Ready;
                    c.protocol = protocol;
                }
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
                    let owner = self.world.connections[connection].owner.filter(|job| {
                        self.world
                            .jobs
                            .get(*job)
                            .is_some_and(|j| j.phase == JobPhase::Transferring)
                    });
                    if let Some(job) = owner {
                        if self.world.jobs[job].active_sources.contains(&sid) {
                            self.pump(&mut actions, origin, now);
                        } else if self.world.jobs[job].probing_sources.contains(&sid) {
                            actions.push(Action::Probe {
                                job,
                                connection,
                                source: Some(sid),
                            });
                        } else {
                            actions.push(Action::CloseConnection { connection });
                            self.drop_connection(connection);
                        }
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
                self.ensure_probes(&mut actions, origin, now);
                self.pump(&mut actions, origin, now);
            }

            Observation::ConnectionFailed { connection, kind } => {
                self.reservations.remove(&connection);
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
                    self.invalidate_origin_measurements(o, now);
                }
                let owner = self.world.connections.get(connection).and_then(|c| c.owner);
                let intent = self.world.connections.get(connection).map(|c| c.intent);
                self.drop_connection(connection);
                // H3 dial failed and we have no ready connection: open a
                // TCP replacement so the job isn't blocked. After the
                // fallback is in flight, the intent-based handler below
                // would otherwise kill the job for the H3 TLS error - the
                // replacement is the authoritative path now, so we skip
                // it for the H3+Probe case.
                if was_h3 && let Some(origin) = origin_of_conn {
                    let required: Vec<_> = self
                        .world
                        .jobs
                        .iter()
                        .filter(|(_, j)| {
                            j.origin == origin
                                && j.policy.http_version
                                    == crate::core::policy::HttpVersionPolicy::Http3Only
                        })
                        .map(|(id, _)| id)
                        .collect();
                    for job in required {
                        self.begin_fail(
                            &mut actions,
                            job,
                            "required HTTP/3 connection failed".into(),
                        );
                    }
                }
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
                        let active = self
                            .world
                            .jobs
                            .get(job)
                            .is_some_and(|j| j.active_sources.contains(&source));
                        if active && kind.retryable() {
                            let origin = self.world.sources[source].origin;
                            let at = now + self.world.jobs[job].policy.retry.base_backoff;
                            self.world.origins[origin].cooldown_until = Some(at);
                            actions.push(Action::ScheduleTimer {
                                at,
                                event: TimerEvent::OriginCooldownExpired(origin),
                            });
                        } else {
                            if let Some(j) = self.world.jobs.get_mut(job) {
                                j.probing_sources.retain(|s| *s != source);
                                j.active_sources.retain(|s| *s != source);
                                j.failed_sources.push(source);
                            }
                            if self
                                .world
                                .jobs
                                .get(job)
                                .is_some_and(|j| j.active_sources.is_empty())
                                && !self.activate_next_source(&mut actions, job, now)
                            {
                                self.begin_fail(
                                    &mut actions,
                                    job,
                                    "no usable source remains after connection failure".into(),
                                );
                            }
                        }
                        self.try_complete_drain(&mut actions, job);
                    }
                    Some(crate::core::world::ConnectionIntent::Pool { origin }) => {
                        if !kind.retryable() {
                            if let Some(job) = owner {
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
                            if !self.has_assignment(assign) {
                                return actions;
                            }
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
                            if operation == DispatchOperation::OpenConnection {
                                self.reservations.remove(&conn);
                            } else {
                                actions.push(Action::CloseConnection { connection: conn });
                            }
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
                                .filter(|(_, j)| j.origin == origin && !j.phase.is_draining())
                                .map(|(id, _)| id)
                                .collect();
                            for job in jobs {
                                self.retry_resolve(&mut actions, job, now);
                            }
                        }
                    }
                    DispatchOperation::SampleMirror => {
                        if let Some(connection) = connection {
                            if let Some(c) = self.world.connections.get(connection)
                                && let (Some(job), Some(source)) = (c.owner, c.serving_source)
                                && let Some(j) = self.world.jobs.get_mut(job)
                            {
                                j.probing_sources.retain(|sid| *sid != source);
                                j.sampling_sources.retain(|sid| *sid != source);
                                j.failed_sources.push(source);
                            }
                            actions.push(Action::CloseConnection { connection });
                            self.drop_connection(connection);
                        }
                    }
                    DispatchOperation::PrepareDestination => {
                        if let Some(job) = job {
                            actions.extend(self.handle(
                                Observation::DestinationPrepared {
                                    job,
                                    result: Err(DestinationFailure {
                                        kind: DestinationFailureKind::DestinationError,
                                    }),
                                },
                                now,
                            ));
                        }
                    }
                    DispatchOperation::CloseConnection
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
                self.reservations.remove(&connection);
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
                if !self.world.jobs.get(job).is_some_and(|j| {
                    matches!(j.phase, JobPhase::Created | JobPhase::Probing)
                        || j.probing_sources.contains(&source)
                }) {
                    return actions;
                }

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
                        actions.push(Action::CloseConnection { connection });
                        self.drop_connection(connection);
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
                j.plan_id.advance();
                j.plan = Some(plan);
                j.phase = JobPhase::Transferring;
                let origin = j.origin;
                let requires_h3 =
                    j.policy.http_version == crate::core::policy::HttpVersionPolicy::Http3Only;
                let discovered_h3 = alt_svc_h3;
                let _ = j;
                if !reusable
                    || (requires_h3
                        && self
                            .world
                            .connections
                            .get(connection)
                            .is_some_and(|c| c.protocol != crate::core::events::Protocol::Http3))
                {
                    actions.push(Action::CloseConnection { connection });
                    self.drop_connection(connection);
                }
                if requires_h3
                    && !self.prefer_h3_for(origin, now)
                    && !self.world.connections.iter().any(|(_, c)| {
                        c.origin == origin && c.protocol == crate::core::events::Protocol::Http3
                    })
                {
                    self.begin_fail(
                        &mut actions,
                        job,
                        "HTTP/3 is required but no usable endpoint was discovered".into(),
                    );
                    return actions;
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
                        self.dial_connection(
                            Some(job),
                            &mut actions,
                            origin,
                            ep,
                            shard,
                            true,
                            None,
                            now,
                        );
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
                    actions.push(Action::CloseConnection { connection });
                    self.drop_connection(connection);
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
                if !self.has_assignment(assignment) {
                    return actions;
                }
                let assignment = assignment.assignment;
                self.release_assignment_slot(Some(connection));
                if self
                    .world
                    .jobs
                    .get(job)
                    .is_some_and(|j| j.phase.is_draining())
                {
                    if let Some(plan) = self.world.jobs.get_mut(job).and_then(|j| j.plan.as_mut()) {
                        plan.abandon(assignment);
                    }
                    self.try_complete_drain(&mut actions, job);
                    return actions;
                }
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.virt_service += sample.bytes as f64 / j.priority.weight();
                }
                if let Some(j) = self.world.jobs.get_mut(job) {
                    j.useful_bytes = j.useful_bytes.saturating_add(sample.bytes);
                    j.local_blocked += sample.memory_wait + sample.destination_blocked;
                    j.sample_wall += sample.response_wall;
                }
                // Track this connection's live receive rate for piece sizing.
                if sample.stall_fraction() < 0.5 && self.world.connections.contains_key(connection)
                {
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
                self.world.note_endpoint_sample(connection, *sample, now);
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
                    .is_some_and(|p| p.admits_worker(now));
                if let Some(origin) = self.world.jobs.get(job).map(|j| j.origin)
                    && needs_work
                {
                    self.scale_connections(&mut actions, Some(origin), now);
                    self.pump(&mut actions, origin, now);
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
                if !self.has_assignment(assignment) {
                    return actions;
                }
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
                    actions.push(Action::CloseConnection { connection });
                    self.drop_connection(connection);
                    return actions;
                }
                if !j.active_sources.contains(&source) {
                    j.active_sources.push(source);
                }
                tracing::info!(target: "xde::mirror", ?job, ?source, "mirror verified by sampling; activated");
                if !reusable {
                    actions.push(Action::CloseConnection { connection });
                    self.drop_connection(connection);
                    return actions;
                }
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
            } => self.complete_committed_job(&mut actions, job, final_length, digest),

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
                if self
                    .world
                    .jobs
                    .get(job)
                    .is_some_and(|j| j.phase == JobPhase::Committing)
                {
                    self.remove_job(&mut actions, job);
                    actions.push(Action::FailJob {
                        job,
                        error: JobFailure::Failed(message),
                    });
                } else {
                    self.begin_fail(&mut actions, job, JobFailure::Failed(message));
                }
            }

            Observation::RateLimited {
                origin,
                retry_after,
            } => {
                let delay = retry_after.unwrap_or(Duration::from_secs(5));
                if let Some(o) = self.world.origins.get_mut(origin) {
                    o.cooldown_until = Some(now + delay);
                }
                self.invalidate_origin_measurements(origin, now);
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
                let transferring = j.plan.is_some();
                j.phase = if transferring {
                    JobPhase::Transferring
                } else {
                    JobPhase::Probing
                };
                let origin = self.world.jobs.get(job).map(|j| j.origin);
                if let Some(origin) = origin {
                    if origin != origin_before {
                        self.invalidate_origin_measurements(origin_before, now);
                        self.invalidate_origin_measurements(origin, now);
                    } else {
                        self.invalidate_origin_measurements(origin, now);
                    }
                    self.open_one_connection(&mut actions, origin, now, true);
                    if transferring {
                        self.pump(&mut actions, origin, now);
                    }
                }
            }

            Observation::CredentialRefreshFailed { job, status } => {
                self.begin_fail(
                    &mut actions,
                    job,
                    JobFailure::Failed(format!(
                        "credentials rejected ({status}) and refresh failed"
                    )),
                );
            }

            Observation::TimerExpired { event } => match event {
                TimerEvent::JobDeadline(job) => {
                    self.begin_fail(&mut actions, job, JobFailure::DeadlineExceeded);
                }
                TimerEvent::RetryReady { assignment, .. } => {
                    let job = assignment.job;
                    if !self.world.jobs.get(job).is_some_and(|j| {
                        j.plan_id == assignment.plan && j.phase == JobPhase::Transferring
                    }) {
                        return actions;
                    }
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
                    self.invalidate_origin_measurements(origin, now);
                    self.ensure_probes(&mut actions, origin, now);
                    if self.waiting_probe_job(origin).is_some()
                        && !self.world.has_ready_connection(origin)
                    {
                        self.open_one_connection(&mut actions, origin, now, true);
                    }
                    self.pump(&mut actions, origin, now);
                }
                TimerEvent::CheckpointDue(_) => {}
                TimerEvent::AdaptiveTick { job } => {
                    self.adaptive_tick(&mut actions, job, now);
                    if self
                        .world
                        .jobs
                        .get(job)
                        .is_some_and(|j| j.phase == JobPhase::Transferring)
                    {
                        self.rebalance_job(&mut actions, job, now);
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
                                None,
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
            self.begin_fail(
                actions,
                job,
                JobFailure::Failed(format!("redirect limit exceeded at {location:?}")),
            );
            return;
        }
        let Ok(url) = url::Url::parse(&location.url) else {
            self.begin_fail(
                actions,
                job,
                JobFailure::Failed("redirect Location was not a valid URL".into()),
            );
            return;
        };
        let Some(new_key) = crate::core::ids::TransportOriginKey::from_url(&url) else {
            self.begin_fail(
                actions,
                job,
                JobFailure::Failed(format!("redirect to unsupported scheme '{}'", url.scheme())),
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
                self.begin_fail(
                    actions,
                    job,
                    JobFailure::Failed("redirect loop detected".into()),
                );
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
            self.begin_fail(
                actions,
                job,
                JobFailure::Failed(
                    "insecure redirect downgrade from https to http rejected".into(),
                ),
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

    /// Forget a connection everywhere: world node, in-flight counter and
    /// rate evidence. The single removal path for the controller.
    fn drop_connection(&mut self, conn: ConnectionId) {
        let shard = self.world.connections.get(conn).map(|c| c.shard);
        self.world.remove_connection(conn);
        self.connection_state.remove(conn);
        if let Some(s) = shard {
            self.dec_shard_load(s);
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
        requested_job: Option<JobId>,
        actions: &mut Vec<Action>,
        origin: OriginId,
        endpoint: EndpointId,
        shard: usize,
        allow_h3: bool,
        serving: Option<SourceId>,
        now: Instant,
    ) -> Option<ConnectionId> {
        self.refresh_grants(now);
        let owner = self
            .world
            .jobs
            .iter()
            .filter(|(id, j)| {
                requested_job.is_none_or(|wanted| wanted == *id)
                    && !j.phase.is_draining()
                    && (serving.is_some() || j.probing_sources.is_empty())
                    && j.grant.connections > 0
                    && (serving.is_some_and(|sid| {
                        j.probing_sources.contains(&sid) || j.active_sources.contains(&sid)
                    }) || j.origin == origin)
            })
            .filter(|(id, j)| {
                self.reservations
                    .values()
                    .filter(|r| r.owner == *id)
                    .count()
                    < usize::from(j.grant.connections)
            })
            .min_by(|(a, x), (b, y)| x.virt_service.total_cmp(&y.virt_service).then(a.cmp(b)))
            .map(|(id, _)| id)?;
        if self.reservations.len() >= usize::from(self.limits.max_physical_connections)
            || self
                .reservations
                .values()
                .filter(|r| r.origin == origin)
                .count()
                >= usize::from(self.limits.max_connections_per_origin)
        {
            return None;
        }
        let probing = serving.is_some()
            || self
                .world
                .jobs
                .get(owner)
                .is_some_and(|j| matches!(j.phase, JobPhase::Created | JobPhase::Probing));
        if probing
            && (self.world.jobs[owner].grant.assignments == 0
                || self.occupied_slots() >= self.limits.max_active_assignments as usize)
        {
            return None;
        }
        let prefer_h3 = allow_h3
            && self.prefer_h3_for(origin, now)
            && matches!(
                self.world.jobs[owner].policy.http_version,
                crate::HttpVersionPolicy::Auto | crate::HttpVersionPolicy::Http3Only
            );
        // H3 dials go to the Alt-Svc-advertised UDP port when present;
        // TCP dials use the resolved address unchanged.
        let alt_port = self
            .world
            .origins
            .get(origin)
            .and_then(|o| o.h3_alt_port)
            .filter(|_| prefer_h3);
        let conn = self.world.allocate_connection(origin, endpoint, shard);
        self.reservations.insert(
            conn,
            ConnectionReservation {
                owner,
                origin,
                shard,
            },
        );
        self.stats.peak_reserved_connections = self
            .stats
            .peak_reserved_connections
            .max(self.reservations.len());
        self.inc_shard_load(shard);
        let intent = match serving {
            Some(source) => crate::core::world::ConnectionIntent::Mirror { job: owner, source },
            None => {
                let probe_job = self
                    .world
                    .jobs
                    .get(owner)
                    .filter(|j| matches!(j.phase, JobPhase::Created | JobPhase::Probing))
                    .map(|_| owner);
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
            c.owner = Some(owner);
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
        Some(conn)
    }

    fn adaptive_tick(&mut self, actions: &mut Vec<Action>, job: JobId, now: Instant) {
        use crate::core::planner::{Constraint, Evidence, JobPlanner};
        let Some(j) = self.world.jobs.get(job) else {
            return;
        };
        if j.phase != JobPhase::Transferring {
            self.world.jobs[job].planner_due = None;
            return;
        }
        if j.planner_due.is_some_and(|due| now < due) {
            return;
        }
        self.world.jobs[job].planner_due = Some(now + JobPlanner::WINDOW);
        self.stats.planner_evaluations += 1;
        let j = &self.world.jobs[job];
        let desired = j.planner.desired();
        let ready: Vec<_> = self
            .world
            .connections
            .iter()
            .filter(|(_, c)| c.owner == Some(job) && c.status == ConnectionStatus::Ready)
            .map(|(_, c)| c)
            .collect();
        let multiplexed = ready
            .iter()
            .any(|c| c.protocol != crate::core::events::Protocol::Http1_1);
        let effective_streams = if multiplexed {
            desired.streams_per_connection
        } else {
            1
        };
        let wanted = (u32::from(desired.connections) * u32::from(effective_streams))
            .min(u32::from(j.policy.transport.max_active_assignments));
        let (active_bytes, active_wait, active_wall) =
            j.plan.iter().flat_map(|p| p.iter_assignments()).fold(
                (0u64, Duration::ZERO, Duration::ZERO),
                |(bytes, wait, wall), (_, a)| {
                    (
                        bytes.saturating_add(a.control.bytes()),
                        wait + a.control.local_wait(),
                        wall + now.saturating_duration_since(a.started),
                    )
                },
            );
        let blocked_total = j.local_blocked + active_wait;
        let wall_total = j.sample_wall + active_wall;
        let blocked_delta = blocked_total.saturating_sub(j.previous_local.0);
        let wall_delta = wall_total.saturating_sub(j.previous_local.1);
        let constraint =
            if !j.supports_ranges || !crate::core::sink::supports_segmentation(j.sink_caps) {
                Constraint::Source
            } else if j.sink_hints.max_parallel_writes < wanted as u16 {
                Constraint::Local
            } else if j.grant.connections < u16::from(desired.connections)
                || u32::from(j.grant.assignments) < wanted
            {
                Constraint::Global
            } else if blocked_delta > wall_delta / 2 {
                Constraint::Local
            } else {
                Constraint::None
            };
        match constraint {
            Constraint::Global => self.stats.planner_global_limited += 1,
            Constraint::Local => self.stats.planner_local_limited += 1,
            Constraint::Source => self.stats.planner_source_limited += 1,
            Constraint::None => {}
        }
        let assignments_settled = j.plan.as_ref().is_none_or(|plan| {
            plan.in_flight() <= usize::from(j.grant.assignments)
                && plan.iter_assignments().all(|(_, a)| {
                    a.connection.is_none_or(|connection| {
                        self.in_flight_on(connection) <= usize::from(effective_streams)
                    })
                })
        });
        if ready.len() != usize::from(j.grant.connections) || !assignments_settled {
            self.stats.planner_unsettled += 1;
            self.world.jobs[job].planner.suspend();
            actions.push(Action::ScheduleTimer {
                at: now + JobPlanner::WINDOW,
                event: TimerEvent::AdaptiveTick { job },
            });
            return;
        }
        let setup = ready
            .iter()
            .filter_map(|c| self.world.endpoints[c.endpoint].handshake_ewma)
            .max()
            .unwrap_or_default();
        use std::hash::{Hash, Hasher};
        let mut context = std::hash::DefaultHasher::new();
        for (peer, node) in self.world.jobs.iter().filter(|(peer, _)| *peer != job) {
            (
                peer,
                std::mem::discriminant(&node.phase),
                node.priority.weight().to_bits(),
            )
                .hash(&mut context);
        }
        let evidence = Evidence {
            now,
            useful_bytes: j.useful_bytes.saturating_add(active_bytes),
            engine_bytes: self
                .world
                .jobs
                .values()
                .map(|node| {
                    let active = node
                        .plan
                        .iter()
                        .flat_map(|p| p.iter_assignments())
                        .map(|(_, a)| u128::from(a.control.bytes()))
                        .sum::<u128>();
                    (u128::from(node.useful_bytes) + active) * (node.priority.weight() as u128)
                })
                .sum(),
            remaining: j
                .plan
                .as_ref()
                .and_then(|p| p.bytes_remaining())
                .map(|left| left.saturating_sub(active_bytes)),
            constraint,
            multiplexed,
            setup,
            workload_context: context.finish(),
        };
        let granted_connections: u32 = self
            .world
            .jobs
            .values()
            .map(|j| u32::from(j.grant.connections))
            .sum();
        let granted_assignments: u32 = self
            .world
            .jobs
            .values()
            .map(|j| u32::from(j.grant.assignments))
            .sum();
        let free_slots = self
            .limits
            .max_active_assignments
            .saturating_sub(granted_assignments);
        let free_connections =
            u32::from(self.limits.max_physical_connections).saturating_sub(granted_connections);
        let origin_grants: u32 = self
            .world
            .jobs
            .values()
            .filter(|peer| peer.origin == j.origin)
            .map(|peer| u32::from(peer.grant.connections))
            .sum();
        let origin_free =
            u32::from(self.limits.max_connections_per_origin).saturating_sub(origin_grants);
        let mut opportunities = j.policy.transport;
        // Headroom is an offer to explore, not an allocation. The same ledger
        // must still grant and reserve any resulting request.
        opportunities.max_physical_connections = opportunities.max_physical_connections.min(
            (u32::from(desired.connections) + free_connections.min(origin_free).min(free_slots))
                .min(u32::from(u8::MAX)) as u8,
        );
        opportunities.max_streams_per_connection = opportunities.max_streams_per_connection.min(
            (u32::from(desired.streams_per_connection)
                + free_slots / u32::from(desired.connections))
            .min(u32::from(u16::MAX)) as u16,
        );
        let j = &mut self.world.jobs[job];
        j.previous_local = (blocked_total, wall_total);
        if j.planner.observe(evidence, opportunities) {
            self.stats.planner_topology_changes += 1;
        }
        actions.push(Action::ScheduleTimer {
            at: now + JobPlanner::WINDOW,
            event: TimerEvent::AdaptiveTick { job },
        });
    }

    pub fn start_adaptive(&mut self, actions: &mut Vec<Action>, origin: OriginId, now: Instant) {
        let jobs: Vec<_> = self
            .world
            .jobs
            .iter()
            .filter(|(_, j)| {
                j.origin == origin && j.phase == JobPhase::Transferring && j.planner.adaptive()
            })
            .map(|(id, _)| id)
            .collect();
        for job in jobs {
            let j = &self.world.jobs[job];
            let connection = self
                .world
                .connections
                .values()
                .find(|c| c.owner == Some(job) && c.status == ConnectionStatus::Ready);
            let multiplexed = j.policy.http_version == crate::HttpVersionPolicy::Http3Only
                || connection.is_some_and(|c| c.protocol != crate::Protocol::Http1_1);
            let setup = connection
                .and_then(|c| self.world.endpoints[c.endpoint].handshake_ewma)
                .unwrap_or_default();
            let prior = connection
                .and_then(|c| {
                    self.world.endpoint_evidence(
                        c.endpoint,
                        c.network_context,
                        Some(c.protocol),
                        now,
                    )
                })
                .filter(|e| e.samples >= 3)
                .map(|e| e.median_bps);
            let remaining = j.plan.as_ref().and_then(|p| p.bytes_remaining());
            let segmented =
                j.supports_ranges && crate::core::sink::supports_segmentation(j.sink_caps);
            let mut limits = j.policy.transport;
            let free_connections = u32::from(self.limits.max_physical_connections).saturating_sub(
                self.world
                    .jobs
                    .values()
                    .map(|j| u32::from(j.grant.connections))
                    .sum(),
            );
            let free_assignments = self.limits.max_active_assignments.saturating_sub(
                self.world
                    .jobs
                    .values()
                    .map(|j| u32::from(j.grant.assignments))
                    .sum(),
            );
            let free_origin = u32::from(self.limits.max_connections_per_origin).saturating_sub(
                self.world
                    .jobs
                    .values()
                    .filter(|peer| peer.origin == origin)
                    .map(|peer| u32::from(peer.grant.connections))
                    .sum(),
            );
            limits.max_physical_connections = limits.max_physical_connections.min(
                (u32::from(j.grant.connections) + free_connections.min(free_origin))
                    .min(u32::from(self.limits.max_connections_per_origin))
                    .min(u32::from(u8::MAX)) as u8,
            );
            limits.max_active_assignments = limits.max_active_assignments.min(
                (u32::from(j.grant.assignments) + free_assignments).min(u32::from(u16::MAX)) as u16,
            );
            limits.max_active_assignments = limits
                .max_active_assignments
                .min(j.sink_hints.max_parallel_writes.max(1));
            let j = &mut self.world.jobs[job];
            if segmented {
                j.planner
                    .start(remaining, multiplexed, setup, prior, limits);
            }
            if j.planner_due.is_none() {
                let at = now + crate::core::planner::JobPlanner::WINDOW;
                j.planner_due = Some(at);
                actions.push(Action::ScheduleTimer {
                    at,
                    event: TimerEvent::AdaptiveTick { job },
                });
            }
        }
        // First claims must use the cold topology grant, not the probe slot.
        self.refresh_grants(now);
    }

    fn rebalance_job(&mut self, actions: &mut Vec<Action>, job: JobId, now: Instant) {
        let Some(plan) = self.world.jobs.get_mut(job).and_then(|j| j.plan.as_mut()) else {
            return;
        };
        let cuts = plan.rebalance_stragglers(now);
        for (victim, new_end) in cuts {
            if let Some(connection) = self.world.jobs[job]
                .plan
                .as_ref()
                .and_then(|p| p.assignment(victim))
                .and_then(|a| a.connection)
            {
                actions.push(Action::TruncateAssignment {
                    job,
                    assignment: self.assignment_ref(job, victim),
                    connection,
                    new_end,
                });
            }
        }
        self.retry_pending_mirror_sample(actions, job, now);
    }

    fn waiting_probe_job(&self, origin: OriginId) -> Option<JobId> {
        self.world
            .jobs
            .iter()
            .find(|(id, j)| {
                j.origin == origin
                    && j.grant.connections > 0
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
            if self.world.jobs[job].grant.assignments == 0
                || self.occupied_slots() >= self.limits.max_active_assignments as usize
            {
                break;
            }
            let idle = self.world.connections.iter().find(|(cid, c)| {
                c.origin == origin
                    && c.owner == Some(job)
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
            if self
                .dial_connection(Some(job), actions, origin, ep, shard, true, None, now)
                .is_none()
            {
                break;
            }
            if let Some(j) = self.world.jobs.get_mut(job) {
                j.phase = JobPhase::Probing;
            }
            if !self.job_has_inflight_probe(job) {
                break;
            }
        }
    }

    fn is_runnable(&self, job: JobId, origin: OriginId, now: Instant) -> bool {
        let Some(j) = self.world.jobs.get(job) else {
            return false;
        };
        if j.grant.assignments == 0 || j.phase != JobPhase::Transferring || j.phase.is_draining() {
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
        if !plan.admits_worker(now) {
            return false;
        }
        let ceiling = usize::from(j.grant.assignments)
            .min(usize::from(j.sink_hints.max_parallel_writes.max(1)));
        if plan.in_flight() >= ceiling {
            return false;
        }
        true
    }

    fn pick_wfq_job(&self, connection: ConnectionId, now: Instant) -> Option<JobId> {
        if self.occupied_slots() >= self.limits.max_active_assignments as usize {
            return None;
        }
        let mut best: Option<(JobId, f64)> = None;
        for (id, j) in self.world.jobs.iter() {
            if !self.can_assign(id, connection, now) {
                continue;
            }
            let w = eff_weight(j, now).max(1e-9);
            let pending: u64 = j
                .plan
                .iter()
                .flat_map(|p| p.iter_assignments())
                .map(|(_, a)| a.remaining().min(j.policy.segmentation.max_piece))
                .sum();
            let score = j.virt_service + pending as f64 / w;
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

    fn can_assign(&self, job: JobId, connection: ConnectionId, now: Instant) -> bool {
        let Some(c) = self.world.connections.get(connection) else {
            return false;
        };
        let j = &self.world.jobs[job];
        if c.owner != Some(job)
            || c.status != ConnectionStatus::Ready
            || !self.is_runnable(job, c.origin, now)
            || self.in_flight_on(connection) >= self.stream_capacity(connection)
        {
            return false;
        }
        let protocol_ok = Self::protocol_allowed(&j.policy, c.protocol);
        if !protocol_ok
            || c.serving_source
                .is_some_and(|s| !j.active_sources.contains(&s))
        {
            return false;
        }
        let plan = j.plan.as_ref().expect("runnable job has a plan");
        let on_connection = plan
            .iter_assignments()
            .filter(|(_, a)| a.connection == Some(connection))
            .count();
        if on_connection >= usize::from(j.planner.desired().streams_per_connection) {
            return false;
        }
        let used: std::collections::HashSet<_> = plan
            .iter_assignments()
            .filter_map(|(_, a)| a.connection)
            .collect();
        used.contains(&connection) || used.len() < usize::from(j.grant.connections)
    }

    fn pump(&mut self, actions: &mut Vec<Action>, _origin: OriginId, now: Instant) {
        while self.occupied_slots() < self.limits.max_active_assignments as usize {
            let candidate = self
                .world
                .connections
                .iter()
                .filter_map(|(conn, _)| {
                    let job = self.pick_wfq_job(conn, now)?;
                    let j = &self.world.jobs[job];
                    let pending: u64 = j
                        .plan
                        .iter()
                        .flat_map(|p| p.iter_assignments())
                        .map(|(_, a)| a.remaining().min(j.policy.segmentation.max_piece))
                        .sum();
                    Some((
                        conn,
                        job,
                        j.virt_service + pending as f64 / eff_weight(j, now).max(1e-9),
                    ))
                })
                .min_by(|a, b| {
                    a.2.total_cmp(&b.2)
                        .then_with(|| a.1.cmp(&b.1))
                        .then_with(|| self.in_flight_on(a.0).cmp(&self.in_flight_on(b.0)))
                });
            let Some((conn, _, _)) = candidate else { break };
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
            crate::core::events::Protocol::Http2 | crate::core::events::Protocol::Http3 => c
                .owner
                .and_then(|job| self.world.jobs.get(job))
                .map_or(0, |j| {
                    usize::from(j.planner.desired().streams_per_connection)
                }),
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
        let Some(job) = self.pick_wfq_job(connection, now) else {
            return false;
        };

        let supports_ranges = self.world.jobs.get(job).is_some_and(|j| j.supports_ranges);
        let Some(claim) = self.claim_on_plan(job, connection, now) else {
            return false;
        };
        let (assign, range, overlap, frontier) = match claim {
            ClaimedPiece::Transfer {
                assign,
                range,
                overlap,
                frontier,
            } => (assign, range, overlap, frontier),
            ClaimedPiece::Yield {
                assignment,
                connection,
                new_end,
            } => {
                actions.push(Action::TruncateAssignment {
                    job,
                    assignment: self.assignment_ref(job, assignment),
                    connection,
                    new_end,
                });
                return true;
            }
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
        self.active_assignment_count += 1;
        if !supports_ranges {
            // Full-representation source: the claimed assignment tracks
            // progress, but the wire request carries no Range header.
            actions.push(Action::StartFullBody {
                job,
                assignment: self.assignment_ref(job, assign),
                connection,
                frontier,
            });
            return true;
        }
        actions.push(Action::StartAssignment {
            job,
            assignment: self.assignment_ref(job, assign),
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

    /// Prepare a duration-sized assignment or a request for an incumbent to yield.
    fn claim_on_plan(
        &mut self,
        job: JobId,
        connection: ConnectionId,
        now: Instant,
    ) -> Option<ClaimedPiece> {
        let est = self.connection_rate_estimate(connection);
        let j = self.world.jobs.get(job)?;
        let slots = usize::from(j.grant.assignments).max(1);
        let solo = slots == 1;
        let competing =
            self.contended && matches!(self.limits.scheduling, crate::SchedulingPolicy::FairShare);
        let quantum = match (competing, j.planner.assignment_quantum()) {
            (true, Some(learning)) => Some(learning.min(self.limits.fair_quantum_bytes.max(1))),
            (true, None) => Some(self.limits.fair_quantum_bytes.max(1)),
            (false, learning) => learning,
        };
        let j = self.world.jobs.get_mut(job)?;
        let plan = j.plan.as_mut()?;
        let prefix_before = plan.completed().contiguous_prefix();
        match plan.claim_bounded(est, now, solo, slots, quantum) {
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
                Some(ClaimedPiece::Transfer {
                    assign,
                    range,
                    overlap,
                    frontier,
                })
            }
            Claim::Yield {
                assignment,
                new_end,
            } => {
                let connection = plan.assignment(assignment)?.connection?;
                Some(ClaimedPiece::Yield {
                    assignment,
                    connection,
                    new_end,
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
            self.dial_connection(None, actions, origin, ep, shard, allow_h3, None, now);
        }
    }

    fn scale_connections(
        &mut self,
        actions: &mut Vec<Action>,
        origin: Option<OriginId>,
        now: Instant,
    ) {
        let Some(origin) = origin else {
            return;
        };
        if self
            .world
            .origins
            .get(origin)
            .is_none_or(|o| o.is_cooled_down(now))
        {
            return;
        }
        while let Some(ep) = self.world.select_endpoint(origin, None, now, |_| false) {
            let shard = self.place_shard();
            if self
                .dial_connection(None, actions, origin, ep, shard, true, None, now)
                .is_none()
            {
                break;
            }
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
            if let Some(a) = &abandoned {
                j.virt_service += a.control.bytes() as f64 / j.priority.weight();
            }
            j.planner.invalidate();
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
            return;
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
                                    assignment: self.assignment_ref(job, assign),
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
                    self.begin_fail(
                        actions,
                        job,
                        JobFailure::Failed(format!(
                            "credentials rejected ({status}); refresh budget exhausted"
                        )),
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
                self.begin_fail(
                    actions,
                    job,
                    JobFailure::Failed(format!("artifact invalidated: {reason}")),
                );
            }
            Disposition::FullBodyForRangeRequest => {
                if self.world.jobs.get(job).is_some_and(|j| {
                    !j.sink_caps
                        .contains(crate::core::sink::DestinationCaps::IDEMPOTENT_REWRITE)
                }) {
                    self.begin_fail(
                        actions,
                        job,
                        "destination cannot rewrite after range support is lost".into(),
                    );
                    return;
                }
                let Some(j) = self.world.jobs.get_mut(job) else {
                    return;
                };
                j.phase = JobPhase::Restarting;
                actions.push(Action::CancelTransfer { job });
                self.try_complete_drain(actions, job);
            }
            Disposition::RecheckLength => {
                if self
                    .world
                    .jobs
                    .get(job)
                    .and_then(|j| j.plan.as_ref())
                    .is_some_and(|p| p.in_flight() > 0)
                {
                    self.begin_fail(
                        actions,
                        job,
                        "range length contradicted during active transfer".into(),
                    );
                    return;
                }
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
                    self.begin_fail(
                        actions,
                        job,
                        JobFailure::Failed(format!(
                            "artifact inconsistent across sources: {reason}"
                        )),
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
                                assignment: self.assignment_ref(v_job, v_assign),
                                connection: conn,
                                new_end: end,
                            });
                        }
                        if let Some(s) = self.connection_state.get_mut(conn) {
                            s.in_flight = s.in_flight.saturating_sub(1);
                        }
                        if let Some(o) = self.world.jobs.get(job).map(|j| j.origin) {
                            self.invalidate_origin_measurements(o, now);
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
                    self.begin_fail(
                        actions,
                        job,
                        JobFailure::Failed(format!(
                            "all sources failed or were quarantined: {reason}"
                        )),
                    );
                }
            }
            Disposition::Fatal { status, reason } => {
                // A fatal PROBE failure on a job with unused mirrors is a
                // source failure, not an artifact failure: fail over.
                if assignment.is_none() && self.activate_next_source(actions, job, now) {
                    return;
                }
                self.begin_fail(
                    actions,
                    job,
                    JobFailure::Failed(format!("fatal ({status}): {reason}")),
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
            self.begin_fail(actions, job, JobFailure::Failed("resolution failed".into()));
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

    fn assignment_ref(&self, job: JobId, assignment: AssignId) -> AssignmentRef {
        let plan = self.world.jobs[job].plan_id;
        AssignmentRef::new(job, plan, assignment)
    }

    pub fn has_assignment(&self, reference: AssignmentRef) -> bool {
        self.world.jobs.get(reference.job).is_some_and(|j| {
            j.plan_id == reference.plan
                && j.plan
                    .as_ref()
                    .is_some_and(|p| p.assignment(reference.assignment).is_some())
        })
    }

    fn begin_fail(&mut self, actions: &mut Vec<Action>, job: JobId, error: JobFailure) {
        let Some(j) = self.world.jobs.get_mut(job) else {
            return;
        };
        if matches!(j.phase, JobPhase::Preparing) {
            j.phase = JobPhase::StoppingPreparation(error);
            actions.push(Action::CancelTransfer { job });
            return;
        }
        if matches!(
            j.phase,
            JobPhase::Stopping(_) | JobPhase::StoppingPreparation(_) | JobPhase::Committing
        ) {
            return;
        }
        j.phase = JobPhase::Stopping(error);
        actions.push(Action::CancelTransfer { job });
        self.try_complete_drain(actions, job);
    }

    fn try_complete_drain(&mut self, actions: &mut Vec<Action>, job: JobId) {
        let Some(j) = self.world.jobs.get(job) else {
            return;
        };
        if !j.phase.is_draining() || matches!(j.phase, JobPhase::StoppingPreparation(_)) {
            return;
        }
        let in_flight = j.plan.as_ref().map_or(0, |p| p.in_flight());
        if in_flight > 0
            || self
                .world
                .connections
                .iter()
                .any(|(id, c)| c.owner == Some(job) && self.control_busy(id))
        {
            return;
        }
        match j.phase.clone() {
            JobPhase::Stopping(error) => {
                self.remove_job(actions, job);
                actions.push(Action::FailJob { job, error });
            }
            JobPhase::Pausing => {
                let j = &mut self.world.jobs[job];
                j.plan_id.advance();
                j.phase = JobPhase::Paused;
                j.planner_due = None;
                actions.push(Action::ResetTransfer { job });
            }
            JobPhase::Restarting => {
                let j = self.world.jobs.get_mut(job).expect("drained job exists");
                j.plan_id.advance();
                let mut plan = crate::core::segment::SegmentPlan::new(
                    j.total_length,
                    crate::core::ranges::RangeSet::new(),
                    j.policy.segmentation,
                    j.urgency.piece_duration_target(),
                    0,
                );
                plan.set_single_stream(true);
                j.plan = Some(plan);
                j.resumed_bytes = 0;
                j.supports_ranges = false;
                j.phase = JobPhase::Transferring;
                let origin = j.origin;
                actions.push(Action::ResetTransfer { job });
                actions.push(Action::ResetResumeData { job });
                let _ = origin;
            }
            _ => unreachable!("only draining jobs can finish a drain"),
        }
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

    fn complete_committed_job(
        &mut self,
        actions: &mut Vec<Action>,
        job: JobId,
        bytes: u64,
        digest: Option<crate::core::spec::Digest>,
    ) {
        let Some(j) = self.world.jobs.get(job) else {
            return;
        };
        if j.phase != JobPhase::Committing
            || !j
                .plan
                .as_ref()
                .is_some_and(|p| p.is_complete() && p.total().unwrap_or(p.bytes_done()) == bytes)
        {
            return;
        }
        let resumed_bytes = j.resumed_bytes;
        self.remove_job(actions, job);
        actions.push(Action::CompleteJob {
            job,
            total_bytes: bytes,
            digest,
            resumed_bytes,
        });
    }

    fn remove_job(&mut self, actions: &mut Vec<Action>, job: JobId) {
        let Some(j) = self.world.jobs.remove(job) else {
            return;
        };
        if let Some(d) = self.world.destinations.get_mut(j.destination)
            && d.leased_by == Some(job)
        {
            d.leased_by = None;
        }
        let unused: Vec<_> = self
            .world
            .connections
            .iter()
            .filter(|(_, c)| {
                !self.world.jobs.iter().any(|(_, job)| {
                    job.sources
                        .iter()
                        .any(|source| self.world.sources[*source].origin == c.origin)
                })
            })
            .map(|(id, _)| id)
            .collect();
        for connection in unused {
            actions.push(Action::CloseConnection { connection });
            self.drop_connection(connection);
        }
        self.world.assert_invariants();
    }
}

impl Controller {
    fn invalidate_origin_measurements(&mut self, origin: OriginId, _now: Instant) {
        for (_, j) in self
            .world
            .jobs
            .iter_mut()
            .filter(|(_, j)| j.origin == origin)
        {
            j.planner.invalidate();
        }
    }
}
fn eff_weight(j: &crate::core::world::JobNode, _now: Instant) -> f64 {
    j.priority.weight()
}

fn jitter_for(now: Instant, epoch: Instant) -> f64 {
    let nanos = now.saturating_duration_since(epoch).subsec_nanos();
    ((nanos % 997) as f64 + 1.0) / 998.0
}

#[cfg(test)]
#[path = "controller_tests.rs"]
mod tests;
