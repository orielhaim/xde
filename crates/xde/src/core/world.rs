//! The authoritative control-plane model.
//!
//! One `WorldModel` per engine. Every entity ID in the engine is allocated
//! here and nowhere else: a stale `ConnectionId` from a removed connection can
//! never alias a fresh one because there is exactly one generational key
//! space per ID type.
//!
//! Shard storage never re-mints these IDs. A shard keeps its non-Send
//! resources keyed by IDs this model already allocated.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use slotmap::{DenseSlotMap, SecondaryMap, SlotMap};

use crate::core::{
    ids::{
        ArtifactId, ConnectionId, DestinationId, EndpointId, JobId, NetworkContextId, OriginId,
        PathId, SourceId, TransportOriginKey,
    },
    policy::TransferPolicy,
    representation::RepresentationLock,
    segment::SegmentPlan,
    spec::{Durability, IntegritySpec, JobSpec, Priority, Urgency},
};

/// Observable properties of the network environment a transfer runs in.
/// Kept coarse on purpose: stable enough to accumulate evidence, sensitive
/// enough to distinguish meaningful changes (Wi-Fi vs Ethernet, VPN vs
/// direct).
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct NetworkContextKey {
    /// OS interface index of the default-route interface, when known.
    pub iface_index: Option<u32>,
    /// Interface name, when known.
    pub iface_name: Option<String>,
    /// Address family of the preferred local address.
    pub family: Option<AddressFamily>,
    /// Default gateway address, when known.
    pub gateway: Option<std::net::IpAddr>,
    /// Interface MTU.
    pub mtu: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AddressFamily {
    V4,
    V6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobPhase {
    /// Admitted, resolution pending.
    Created,
    /// Probe in flight on an open connection.
    Probing,
    /// Fetching bytes.
    Transferring,
    /// All ranges verified; final integrity pass / commit in progress.
    Committing,
    Completed,
    Cancelling,
    /// Terminal failure in progress: no new claims, drain in-flight work,
    /// then release the destination lease.
    Failing,
    Failed,
}

impl JobPhase {
    pub fn is_terminal(self) -> bool {
        matches!(self, JobPhase::Completed | JobPhase::Failed)
    }

    /// No new work may be claimed.
    pub fn is_draining(self) -> bool {
        matches!(self, JobPhase::Cancelling | JobPhase::Failing)
    }
}

/// Everything the controller knows about one admitted job. The SegmentPlan is
/// *the* assignment bookkeeping for the job: it allocates AssignIds, tracks
/// claimed/completed ranges, and is consulted by every scheduling decision.
#[derive(Debug)]
pub struct JobNode {
    pub id: JobId,
    pub artifact: ArtifactId,
    /// Every source that can serve this artifact, in application-declared
    /// preference order. One artifact, many sources.
    pub sources: Vec<SourceId>,
    /// Sources currently eligible to serve requests. Starts with the
    /// primary; mirrors join when the mixer activates them (strong
    /// equivalence only) and leave when quarantined.
    pub active_sources: Vec<SourceId>,
    pub origin: OriginId,
    pub destination: DestinationId,
    pub phase: JobPhase,
    pub priority: Priority,
    pub urgency: Urgency,
    pub deadline: Option<Instant>,
    pub durability: Durability,
    pub integrity: IntegritySpec,
    pub policy: TransferPolicy,

    // Probe results (authoritative once Probing completes).
    pub fingerprint_etag: Option<String>,
    pub fingerprint_last_modified: Option<std::time::SystemTime>,
    pub supports_ranges: bool,
    pub total_length: Option<u64>,

    /// Cross-request representation consistency evidence.
    pub rep_lock: Arc<Mutex<RepresentationLock>>,

    /// Range plan + assignments. Allocated lazily when transfer begins;
    /// `None` until then (probe-only jobs never need one).
    pub plan: Option<SegmentPlan>,

    /// Retry attempt counter per logical failure class, reset on progress.
    pub resolve_attempts: u32,

    /// Redirect history: `chain[0]` is the originally requested URL.
    pub redirect_chain: Vec<String>,

    /// Recoverable local progress from a previous run, consumed (taken) at
    /// probe time when the plan is first created.
    pub resume: Option<crate::core::controller::ResumeEvidence>,
    /// Bytes seeded from the journal into the current plan.
    pub resumed_bytes: u64,
    /// Credential refresh attempts so far (bounded by the controller).
    pub refresh_attempts: u32,
    /// Sources that failed probing or were quarantined. Never claim work
    /// from these again this run.
    pub failed_sources: Vec<SourceId>,
    /// Mirror probes currently in flight (sources dialed for verification
    /// but not yet activated into `active_sources`).
    pub probing_sources: Vec<SourceId>,
    /// Sources whose equivalence sample is in flight.
    pub sampling_sources: Vec<SourceId>,
    /// What the destination backing this job can do. File destinations get
    /// the full capability set; shared custom destinations report their own
    /// caps, which gate segmented transfer and parallelism.
    pub sink_caps: crate::core::sink::DestinationCaps,
    pub sink_hints: crate::core::sink::DestinationHints,
    /// Set when the job is draining toward a terminal result. Applied once
    /// in-flight assignments hit zero.
    pub pending_terminal: Option<Result<(u64, Option<crate::core::spec::Digest>), String>>,
    /// WFQ virtual service: normalized bytes delivered (bytes / eff_weight).
    /// Jobs with smaller virt_service are picked first.
    pub virt_service: f64,
}

/// Capability set of the built-in file destination: everything a positional
/// `.part` file provides.
pub fn file_sink_caps() -> crate::core::sink::DestinationCaps {
    use crate::core::sink::DestinationCaps as C;
    C::RANDOM_ACCESS
        | C::PARALLEL_WRITES
        | C::OUT_OF_ORDER
        | C::IDEMPOTENT_REWRITE
        | C::SPARSE
        | C::DURABLE_COMMIT
        | C::READ_BACK
        | C::PREALLOCATE
}

impl JobNode {
    /// The application's preferred source.
    pub fn primary_source(&self) -> SourceId {
        self.sources[0]
    }

    /// The source currently serving requests, if any remain eligible.
    pub fn active_source(&self) -> Option<SourceId> {
        self.active_sources.first().copied()
    }

    /// Strong artifact equivalence: with an expected whole-file digest,
    /// ranges fetched from ANY equivalent source verify against the same
    /// truth, so simultaneous multi-source transfer is safe. Without one,
    /// mirrors may only serve as failover.
    pub fn strong_equivalence(&self) -> bool {
        self.integrity.expected.is_some()
    }

    /// Bytes still needed, best-effort: unknown length reads as "large".
    pub fn bytes_remaining_estimate(&self) -> u64 {
        self.plan
            .as_ref()
            .and_then(|p| p.bytes_remaining())
            .unwrap_or(u64::MAX)
    }

    /// Sources declared but neither active, probing, nor failed - the
    /// mixer's activation candidates, and failover's last resort.
    pub fn unused_sources<'a>(&'a self) -> impl Iterator<Item = SourceId> + 'a {
        self.sources.iter().copied().filter(|s| {
            !self.active_sources.contains(s)
                && !self.probing_sources.contains(s)
                && !self.failed_sources.contains(s)
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionStatus {
    Connecting,
    Ready,
    /// H1 connection with unread response bytes; not reusable.
    Poisoned,
    Retired,
}

#[derive(Debug)]
pub struct ConnectionNode {
    pub id: ConnectionId,
    pub origin: OriginId,
    pub endpoint: EndpointId,
    pub shard: usize,
    /// Negotiated application protocol (set at handshake; ALPN result).
    pub protocol: crate::core::events::Protocol,
    pub status: ConnectionStatus,
    pub opened_at: Instant,
    /// The network context this connection was established under.
    pub network_context: Option<NetworkContextId>,
    /// The path this connection currently uses (TCP: exactly one).
    pub path: Option<PathId>,
    /// This dial was requested over QUIC (HTTP/3). Failure classification
    /// and fallback depend on knowing the intended transport.
    pub prefer_h3: bool,
    /// The specific mirror this connection serves, when dialed for one.
    /// `None` for the job's primary flow; connections tagged with a source
    /// only carry requests for that source.
    pub serving_source: Option<SourceId>,
    /// Why this connection exists. Attribution for handshake failure is
    /// O(1) from this field, never a scan of probing jobs.
    pub intent: ConnectionIntent,
}

/// Why a physical connection was dialed. Handshake success and failure
/// attribute to this intent, not to "any job currently Probing".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionIntent {
    Probe { job: JobId, source: SourceId },
    Pool { origin: OriginId },
    Mirror { job: JobId, source: SourceId },
}

/// A network path: the (context, local interface, remote endpoint) tuple
/// that performance evidence should be scoped by. For TCP one connection
/// maps to one path; QUIC connections may later map to several.
#[derive(Debug)]
pub struct PathNode {
    pub id: PathId,
    pub connection: ConnectionId,
    pub endpoint: EndpointId,
    pub network_context: Option<NetworkContextId>,
    pub established_at: Instant,
}

#[derive(Debug)]
pub struct OriginNode {
    pub key: TransportOriginKey,
    pub endpoints: Vec<EndpointId>,
    pub connections: Vec<ConnectionId>,
    /// No new connections or requests to this origin until this instant.
    pub cooldown_until: Option<Instant>,
    /// Learned range-support behavior. `None` = unprobed.
    pub supports_ranges: Option<bool>,
    /// Endpoints observed to fail repeatedly are skipped by selection.
    pub endpoint_failures: SecondaryMap<EndpointId, u32>,
    /// The origin's DNS HTTPS/SVCB records advertise HTTP/3 (alpn=h3).
    /// Discovery signal only - the controller still probes before trusting.
    pub advertises_h3: bool,
    /// Adaptive topology state: how many physical connections the
    /// controller currently believes are useful for this origin.
    pub adaptive_target_conns: usize,
    /// Adaptive topology state: how many concurrent streams the controller
    /// currently allows per multiplexed (H2/H3) connection. H1 connections
    /// always carry exactly one request regardless of this value.
    pub adaptive_target_streams: u32,
    /// Cumulative verified bytes when the current measurement window opened.
    pub last_window_bytes: u64,
    pub last_window_at: Instant,
    /// Recent aggregate window rates (B/s), oldest first, capped - the
    /// dispersion of this series is the noise floor for keep/revert
    /// decisions.
    pub window_rates: Vec<f64>,
    /// Whether the adaptive loop is running for this origin.
    pub adaptive_active: bool,
    /// Active topology experiment: the variable under test, its baseline
    /// rate, and when it started. Cleared on keep or revert.
    pub topology_experiment: Option<TopologyExperiment>,
    /// The variable adjusted by the previous experiment; the next one picks
    /// the other variable first so neither dominates exploration.
    pub last_experiment_variable: Option<TopologyVariable>,
    /// No new experiments until this instant (hysteresis after revert).
    pub experiment_cooldown_until: Option<Instant>,
    /// Failed H3 dials to this origin. Drives exponential retry backoff:
    /// broken advertisements or blocked UDP must not be re-probed for
    /// every range.
    pub h3_failures: u32,
    /// No H3 dials until this instant (set after a failed H3 attempt).
    pub h3_retry_after: Option<Instant>,
    /// UDP port where the origin advertised HTTP/3 via Alt-Svc or SVCB.
    pub h3_alt_port: Option<u16>,
}

/// Which control knob an experiment adjusts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyVariable {
    /// One more physical connection to this origin.
    Connection,
    /// One more concurrent stream per multiplexed connection.
    Streams,
}

impl TopologyVariable {
    /// The variable to try next, given what was tried last.
    pub fn next_after(last: Option<TopologyVariable>) -> Self {
        match last {
            Some(TopologyVariable::Connection) => TopologyVariable::Streams,
            Some(TopologyVariable::Streams) => TopologyVariable::Connection,
            None => TopologyVariable::Connection,
        }
    }
}

/// One adaptive topology experiment in flight.
#[derive(Debug)]
pub struct TopologyExperiment {
    pub variable: TopologyVariable,
    pub conn: Option<ConnectionId>,
    pub baseline_rate_bps: f64,
    pub opened_at: Instant,
    pub measure_start_at: Option<Instant>,
    pub measure_start_bytes: u64,
    pub measured_rate_bps: Option<f64>,
}

impl OriginNode {
    pub fn is_cooled_down(&self, now: Instant) -> bool {
        self.cooldown_until.is_some_and(|until| until > now)
    }
}

#[derive(Debug)]
pub struct EndpointNode {
    pub address: SocketAddr,
    pub handshake_ewma: Option<Duration>,
    /// Recent verified-transfer samples, newest last. Evidence is queried
    /// at decision time through [`WorldModel::endpoint_evidence`] - there is
    /// deliberately no cached scalar hint, because a scalar computed under
    /// one network context or protocol would otherwise rank decisions made
    /// under another forever.
    pub samples: Vec<ContextualSample>,
}

/// One verified transfer observation attached to an endpoint.
#[derive(Debug, Clone)]
pub struct ContextualSample {
    pub network_context: Option<NetworkContextId>,
    pub protocol: crate::core::events::Protocol,
    /// Pure receive-active goodput in bytes/sec.
    pub receive_rate_bps: f64,
    pub recorded_at: Instant,
}

/// Aggregated evidence for one endpoint under one (context, protocol) key.
#[derive(Debug, Clone, Copy)]
pub struct EvidenceSummary {
    /// Median receive rate in bytes/sec.
    pub median_bps: f64,
    /// Median absolute deviation in bytes/sec - uncertainty proxy.
    pub dispersion_bps: f64,
    /// Fresh matching sample count.
    pub samples: usize,
    pub last_seen: Instant,
}

/// How many recent samples we keep per endpoint.
const MAX_SAMPLES_PER_ENDPOINT: usize = 8;

/// Samples older than this no longer influence ranking.
const SAMPLE_TTL: Duration = Duration::from_secs(600);

#[derive(Debug)]
pub struct ArtifactNode {
    pub expected_length: Option<u64>,
}

#[derive(Debug)]
pub struct SourceNode {
    pub artifact: ArtifactId,
    pub url: String,
    /// Headers that can change the answer (never credentials).
    pub headers: http::HeaderMap,
    pub header_identity: [u8; 32],
    pub origin: OriginId,
}

#[derive(Debug)]
pub struct DestinationNode {
    pub key: String,
    /// Job currently holding the exclusive lease, if any.
    pub leased_by: Option<JobId>,
}

/// Single source of truth for engine entities and their relationships.
///
/// Invariants (checked by `assert_invariants`, enabled in debug builds):
/// - every `JobId`/`ConnectionId`/`AssignId` was minted by *this* struct;
/// - each destination is leased by at most one active job;
/// - each connection appears in exactly one origin's `connections` list;
/// - assignment ranges claimed by a job's plan lie within the job's total.
#[derive(Debug, Default)]
pub struct WorldModel {
    pub jobs: DenseSlotMap<JobId, JobNode>,
    pub artifacts: SlotMap<ArtifactId, ArtifactNode>,
    pub sources: SlotMap<SourceId, SourceNode>,
    pub origins: SlotMap<OriginId, OriginNode>,
    pub endpoints: SlotMap<EndpointId, EndpointNode>,
    pub connections: DenseSlotMap<ConnectionId, ConnectionNode>,
    /// Network paths. ConnectionId != PathId by construction.
    pub paths: SlotMap<PathId, PathNode>,
    /// Known network environments.
    pub network_contexts: SlotMap<NetworkContextId, NetworkContextKey>,
    /// The context the engine currently operates in.
    pub current_context: Option<NetworkContextId>,
    pub destinations: SlotMap<DestinationId, DestinationNode>,
    pub destination_index: HashMap<String, DestinationId>,
    pub origin_index: HashMap<TransportOriginKey, OriginId>,
    pending_learning: Option<crate::core::profile::PersistedProfile>,
}

impl WorldModel {
    pub fn new() -> Self {
        Self::default()
    }

    // ---------------------------------------------------------------
    // Admission
    // ---------------------------------------------------------------

    /// Admit a job: allocate artifact/source/origin, acquire the destination
    /// lease. Returns Err(DestinationError) on lease conflict so the caller
    /// can fail the job before any I/O happens.
    pub fn admit_job(
        &mut self,
        spec: &JobSpec,
        destination_key: String,
    ) -> Result<(JobId, ArtifactId), DestinationLeaseError> {
        let dest_id = match self.destination_index.get(&destination_key) {
            Some(&id) => {
                let node = self
                    .destinations
                    .get_mut(id)
                    .expect("index points into slotmap");
                if node.leased_by.is_some() {
                    return Err(DestinationLeaseError::Busy(destination_key));
                }
                id
            }
            None => {
                let id = self.destinations.insert(DestinationNode {
                    key: destination_key.clone(),
                    leased_by: None,
                });
                self.destination_index.insert(destination_key.clone(), id);
                id
            }
        };

        if spec.sources.is_empty() {
            return Err(DestinationLeaseError::NoSource);
        }
        let artifact = self.artifacts.insert(ArtifactNode {
            expected_length: None,
        });

        // One artifact, many sources: every declared source gets a node,
        // all pointing at the same artifact. Origins are shared when
        // mirrors resolve to the same host.
        let mut source_ids = Vec::with_capacity(spec.sources.len());
        let mut primary_origin = None;
        for src in &spec.sources {
            let origin_id = self.get_or_create_origin(src.origin_key());
            let source_id = self.sources.insert(SourceNode {
                artifact, // known above; no patching needed
                url: src.url.to_string(),
                headers: src.headers.clone(),
                header_identity: src.representation_fingerprint(),
                origin: origin_id,
            });
            source_ids.push(source_id);
            if primary_origin.is_none() {
                primary_origin = Some(origin_id);
            }
        }
        let origin_id = primary_origin.expect("checked non-empty above");

        let job = self.jobs.insert_with_key(|id| JobNode {
            id,
            artifact,
            active_sources: vec![source_ids[0]],
            sources: source_ids,
            origin: origin_id,
            destination: dest_id,
            phase: JobPhase::Created,
            priority: spec.priority,
            urgency: spec.urgency,
            deadline: spec.deadline,
            durability: spec.persistence,
            integrity: spec.integrity.clone(),
            policy: spec.policy.clone(),
            fingerprint_etag: None,
            fingerprint_last_modified: None,
            supports_ranges: false,
            total_length: None,
            rep_lock: Arc::new(Mutex::new(RepresentationLock::default())),
            plan: None,
            resolve_attempts: 0,
            // chain[0] is the originally requested URL.
            redirect_chain: vec![spec.sources[0].url.to_string()],
            resume: None,
            resumed_bytes: 0,
            refresh_attempts: 0,
            failed_sources: Vec::new(),
            probing_sources: Vec::new(),
            sampling_sources: Vec::new(),
            sink_caps: crate::core::world::file_sink_caps(),
            sink_hints: Default::default(),
            pending_terminal: None,
            virt_service: 0.0,
        });

        if let Some(d) = self.destinations.get_mut(dest_id) {
            d.leased_by = Some(job);
        }

        Ok((job, artifact))
    }

    pub fn release_destination(&mut self, job: JobId) {
        let dest = self.jobs.get(job).map(|j| j.destination);
        if let Some(dest) = dest
            && let Some(d) = self.destinations.get_mut(dest)
            && d.leased_by == Some(job)
        {
            d.leased_by = None;
        }
    }

    /// Record where this job's `.part` lives so fetch commands can carry it.
    /// Returns an error only if the job does not exist.
    pub fn set_destination_part_path(
        &mut self,
        job: JobId,
        _path: Option<std::path::PathBuf>,
    ) -> Result<(), crate::core::Error> {
        if self.jobs.contains_key(job) {
            Ok(())
        } else {
            Err(crate::core::Error::Config(
                "job vanished during admission".into(),
            ))
        }
    }

    /// Record what the job's destination can do. Called at admission for
    /// shared custom destinations; file jobs keep the full-capability
    /// default.
    pub fn set_sink_properties(
        &mut self,
        job: JobId,
        caps: crate::core::sink::DestinationCaps,
        hints: crate::core::sink::DestinationHints,
    ) -> Result<(), crate::core::Error> {
        let Some(j) = self.jobs.get_mut(job) else {
            return Err(crate::core::Error::Config(
                "job vanished during admission".into(),
            ));
        };
        j.sink_caps = caps;
        j.sink_hints = hints;
        Ok(())
    }

    pub fn get_or_create_origin(&mut self, key: TransportOriginKey) -> OriginId {
        if let Some(&id) = self.origin_index.get(&key) {
            return id;
        }
        let id = self.origins.insert(OriginNode {
            key: key.clone(),
            endpoints: Vec::new(),
            connections: Vec::new(),
            cooldown_until: None,
            supports_ranges: None,
            endpoint_failures: SecondaryMap::new(),
            advertises_h3: false,
            adaptive_target_conns: 1,
            adaptive_target_streams: 1,
            last_window_bytes: 0,
            last_window_at: Instant::now(),
            window_rates: Vec::new(),
            adaptive_active: false,
            topology_experiment: None,
            h3_failures: 0,
            h3_retry_after: None,
            h3_alt_port: None,
            last_experiment_variable: None,
            experiment_cooldown_until: None,
        });
        self.origin_index.insert(key, id);
        self.apply_pending_learning(id);
        id
    }

    /// Record resolver output. Returns only genuinely new endpoints so the
    /// caller does not open duplicate connections to known addresses.
    pub fn note_endpoints(&mut self, origin: OriginId, addrs: &[SocketAddr]) -> Vec<EndpointId> {
        let o = self.origins.get_mut(origin).expect("origin exists");
        let mut fresh = Vec::new();
        for addr in addrs {
            let known = o
                .endpoints
                .iter()
                .any(|&e| self.endpoints.get(e).is_some_and(|n| n.address == *addr));
            if !known {
                let e = self.endpoints.insert(EndpointNode {
                    address: *addr,
                    handshake_ewma: None,
                    samples: Vec::new(),
                });
                o.endpoints.push(e);
                fresh.push(e);
            }
        }
        if !fresh.is_empty() {
            self.apply_pending_learning(origin);
        }
        fresh
    }

    /// Best endpoint for a new connection. Decision-time evidence query:
    /// fewest failures, then context+protocol-scoped receive evidence
    /// (median descending), then handshake latency, then stable ordering.
    /// `protocol == None` (Auto) aggregates all protocols within the current
    /// context - cold exploration; H1 evidence never ranks an explicit H2
    /// request and vice versa.
    pub fn select_endpoint(
        &self,
        origin: OriginId,
        protocol: Option<crate::core::events::Protocol>,
        now: Instant,
        exclude: impl Fn(EndpointId) -> bool,
    ) -> Option<EndpointId> {
        let o = self.origins.get(origin)?;
        let ctx = self.current_context;
        o.endpoints
            .iter()
            .copied()
            .filter(|&e| !exclude(e))
            .min_by_key(|&e| {
                let failures = o.endpoint_failures.get(e).copied().unwrap_or(0);
                let median = self
                    .endpoint_evidence(e, ctx, protocol, now)
                    .map(|s| s.median_bps)
                    .unwrap_or(0.0);
                let hs = self.endpoints.get(e).and_then(|n| n.handshake_ewma);
                (
                    failures,
                    std::cmp::Reverse(median.to_bits()),
                    hs.unwrap_or(Duration::ZERO),
                    self.endpoints.get(e).map_or(0, |n| n.address.port() as u64),
                )
            })
    }

    // ---------------------------------------------------------------
    // Connections
    // ---------------------------------------------------------------

    /// Allocate a ConnectionId in Connecting state, plus its initial Path.
    /// This is the only place a ConnectionId/PathId pair is ever created.
    pub fn allocate_connection(
        &mut self,
        origin: OriginId,
        endpoint: EndpointId,
        shard: usize,
    ) -> ConnectionId {
        // Protocol is unknown until the handshake negotiates it; the
        // controller records the result at ConnectionReady time.
        let id = self.connections.insert_with_key(|id| ConnectionNode {
            id,
            origin,
            endpoint,
            shard,
            protocol: crate::core::events::Protocol::Http1_1,
            status: ConnectionStatus::Connecting,
            opened_at: Instant::now(),
            network_context: self.current_context,
            path: None,
            prefer_h3: false,
            serving_source: None,
            intent: ConnectionIntent::Pool { origin },
        });
        if let Some(o) = self.origins.get_mut(origin) {
            o.connections.push(id);
        }
        // One path per TCP connection for now; QUIC may add more later.
        let pid = self.paths.insert_with_key(|pid| PathNode {
            id: pid,
            connection: id,
            endpoint,
            network_context: self.current_context,
            established_at: Instant::now(),
        });
        if let Some(c) = self.connections.get_mut(id) {
            c.path = Some(pid);
        }
        id
    }

    /// Any ready connection for this origin, regardless of shard.
    pub fn has_ready_connection(&self, origin: OriginId) -> bool {
        self.connections
            .iter()
            .any(|(_, c)| c.origin == origin && c.status == ConnectionStatus::Ready)
    }

    /// Any ready connection for this origin multiplexing-capable (H2/H3).
    pub fn has_multiplexed_ready_connection(&self, origin: OriginId) -> bool {
        use crate::core::events::Protocol;
        self.connections.iter().any(|(_, c)| {
            c.origin == origin
                && c.status == ConnectionStatus::Ready
                && matches!(c.protocol, Protocol::Http2 | Protocol::Http3)
        })
    }

    pub fn retire_connection(&mut self, id: ConnectionId) {
        if let Some(c) = self.connections.get_mut(id) {
            c.status = ConnectionStatus::Retired;
        }
        if let Some(o) = self
            .connections
            .get(id)
            .map(|c| c.origin)
            .and_then(|o| self.origins.get_mut(o))
        {
            o.connections.retain(|&x| x != id);
        }
    }

    pub fn remove_connection(&mut self, id: ConnectionId) {
        let origin = self.connections.get(id).map(|c| c.origin);
        if let Some(c) = self.connections.get(id)
            && let Some(pid) = c.path
        {
            self.paths.remove(pid);
        }
        self.connections.remove(id);
        if let Some(o) = origin
            && let Some(node) = self.origins.get_mut(o)
        {
            node.connections.retain(|&x| x != id);
        }
    }

    /// Register (or look up) the engine's current network environment.
    pub fn set_network_context(&mut self, key: NetworkContextKey) -> NetworkContextId {
        if let Some(existing) = self.current_context
            && self.network_contexts.get(existing) == Some(&key)
        {
            return existing;
        }
        // Reuse an identical known context instead of minting a new one.
        for (id, k) in self.network_contexts.iter() {
            if *k == key {
                self.current_context = Some(id);
                return id;
            }
        }
        let id = self.network_contexts.insert(key);
        self.current_context = Some(id);
        id
    }

    /// Endpoints of this origin ordered by preference for a dial under the
    /// current context: failures, then fresh context-scoped evidence, then
    /// handshake latency. Used by endpoint racing.
    pub fn ranked_endpoints(
        &self,
        origin: OriginId,
        now: Instant,
        protocol: Option<crate::core::events::Protocol>,
    ) -> Vec<EndpointId> {
        let Some(o) = self.origins.get(origin) else {
            return Vec::new();
        };
        let ctx = self.current_context;
        let mut eps = o.endpoints.clone();
        eps.sort_by_key(|&e| {
            let failures = o.endpoint_failures.get(e).copied().unwrap_or(0);
            let median = self
                .endpoint_evidence(e, ctx, protocol, now)
                .map(|s| s.median_bps)
                .unwrap_or(0.0);
            let hs = self.endpoints.get(e).and_then(|n| n.handshake_ewma);
            (
                failures,
                std::cmp::Reverse(median.to_bits()),
                hs.unwrap_or(Duration::ZERO),
                self.endpoints.get(e).map_or(0, |n| n.address.port() as u64),
            )
        });
        eps
    }

    /// Record one verified-transfer sample against an endpoint, scoped by
    /// network context and protocol. Samples dominated by destination or
    /// memory stalls are rejected here: a rate measured while waiting on
    /// disk is NOT endpoint capacity. `now` drives recency decay.
    pub fn note_endpoint_sample(
        &mut self,
        connection: ConnectionId,
        sample: crate::core::metrics::TransferSample,
        now: Instant,
    ) {
        if sample.bytes == 0 || sample.stall_fraction() >= 0.5 {
            return;
        }
        let receive_rate_bps = sample.receive_rate();
        let Some(c) = self.connections.get(connection) else {
            return;
        };
        let (endpoint, context) = (c.endpoint, c.network_context);
        let protocol = c.protocol;
        let Some(e) = self.endpoints.get_mut(endpoint) else {
            return;
        };
        e.samples.push(ContextualSample {
            network_context: context,
            protocol,
            receive_rate_bps,
            recorded_at: now,
        });
        let cutoff = now.checked_sub(SAMPLE_TTL);
        e.samples.retain(|s| match cutoff {
            Some(c) => s.recorded_at >= c,
            None => true,
        });
        if e.samples.len() > MAX_SAMPLES_PER_ENDPOINT {
            let drop_n = e.samples.len() - MAX_SAMPLES_PER_ENDPOINT;
            e.samples.drain(..drop_n);
        }
    }

    /// Decision-time evidence query: aggregate fresh samples for one
    /// endpoint under one (context, protocol) key. `protocol == None`
    /// aggregates all protocols within the context (cold exploration).
    /// There is no cached scalar anywhere in this path - stale evidence
    /// simply stops matching.
    pub fn endpoint_evidence(
        &self,
        endpoint: EndpointId,
        context: Option<NetworkContextId>,
        protocol: Option<crate::core::events::Protocol>,
        now: Instant,
    ) -> Option<EvidenceSummary> {
        let cutoff = now.checked_sub(SAMPLE_TTL);
        let mut rates: Vec<(f64, Instant)> = self
            .endpoints
            .get(endpoint)?
            .samples
            .iter()
            .filter(|s| s.network_context == context)
            .filter(|s| protocol.is_none_or(|p| s.protocol == p))
            .filter(|s| cutoff.is_none_or(|c| s.recorded_at >= c))
            .map(|s| (s.receive_rate_bps, s.recorded_at))
            .collect();
        if rates.is_empty() {
            return None;
        }
        rates.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        let median = rates[rates.len() / 2].0;
        let last_seen = rates.iter().map(|(_, t)| *t).max().unwrap();
        let deviations: Vec<f64> = rates.iter().map(|(r, _)| (r - median).abs()).collect();
        let mut dev = deviations.clone();
        dev.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Some(EvidenceSummary {
            median_bps: median,
            dispersion_bps: dev[dev.len() / 2],
            samples: rates.len(),
            last_seen,
        })
    }

    /// A successful handshake proves reachability *now*: clear any stale
    /// failure penalty so old failures do not shadow healthy endpoints.
    pub fn clear_endpoint_failures(&mut self, id: ConnectionId) {
        let (origin, endpoint) = match self.connections.get(id) {
            Some(c) => (c.origin, c.endpoint),
            None => return,
        };
        if let Some(o) = self.origins.get_mut(origin)
            && o.endpoint_failures.get(endpoint).is_some_and(|f| *f > 0)
        {
            o.endpoint_failures.remove(endpoint);
        }
    }

    pub fn note_endpoint_failure(&mut self, id: ConnectionId) {
        let (origin, endpoint) = match self.connections.get(id) {
            Some(c) => (c.origin, c.endpoint),
            None => return,
        };
        if let Some(o) = self.origins.get_mut(origin)
            && let Some(entry) = o.endpoint_failures.entry(endpoint)
        {
            *entry.or_insert(0) += 1;
        }
    }

    pub fn live_connections_for_origin(&self, origin: OriginId) -> usize {
        self.connections
            .iter()
            .filter(|(_, c)| c.origin == origin && c.status != ConnectionStatus::Retired)
            .count()
    }

    // ---------------------------------------------------------------
    // Debug invariant checking
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // Persistent learning
    // ---------------------------------------------------------------

    /// Export transport evidence keyed semantically (origin identity +
    /// stable context signature). Process-local IDs never leave the model.
    pub fn export_profiles(&self) -> crate::core::profile::PersistedProfile {
        use crate::core::profile::{PersistedEndpointSample, PersistedOrigin, PersistedProfile};
        let ctx_signature = self
            .current_context
            .and_then(|id| self.network_contexts.get(id))
            .map(context_signature)
            .unwrap_or_default();
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let now_instant = std::time::Instant::now();
        let mut origins = Vec::new();
        for (_, o) in self.origins.iter() {
            let mut samples = Vec::new();
            for &ep in &o.endpoints {
                let Some(node) = self.endpoints.get(ep) else {
                    continue;
                };
                for s in &node.samples {
                    samples.push(PersistedEndpointSample {
                        addr: node.address,
                        protocol: s.protocol.as_str().to_string(),
                        rate_bps: s.receive_rate_bps,
                        recorded_at_unix: now_unix.saturating_sub(
                            now_instant
                                .saturating_duration_since(s.recorded_at)
                                .as_secs(),
                        ),
                    });
                }
            }
            if samples.is_empty() && o.supports_ranges.is_none() && o.h3_alt_port.is_none() {
                continue;
            }
            origins.push(PersistedOrigin {
                scheme: o.key.scheme.to_string(),
                host: o.key.host.to_string(),
                port: o.key.port,
                context_signature: ctx_signature.clone(),
                supports_ranges: o.supports_ranges,
                h3_alt_port: o.h3_alt_port,
                samples,
            });
        }
        PersistedProfile {
            format_version: crate::core::profile::PROFILE_FORMAT_VERSION,
            origins,
        }
    }

    /// Import a profile written by an earlier engine run. The document is
    /// kept as pending semantic evidence and applied to any origin that
    /// already exists *and* to origins/endpoints created later.
    pub fn import_profiles(&mut self, profile: &crate::core::profile::PersistedProfile) {
        if profile.format_version != crate::core::profile::PROFILE_FORMAT_VERSION {
            return;
        }
        self.pending_learning = Some(profile.clone());
        let ids: Vec<OriginId> = self.origins.keys().collect();
        for id in ids {
            self.apply_pending_learning(id);
        }
    }

    fn apply_pending_learning(&mut self, origin_id: OriginId) {
        use crate::core::events::Protocol;
        let Some(profile) = self.pending_learning.clone() else {
            return;
        };
        let ctx_signature = self
            .current_context
            .and_then(|id| self.network_contexts.get(id))
            .map(context_signature)
            .unwrap_or_default();
        let Some(o) = self.origins.get(origin_id) else {
            return;
        };
        let key = o.key.clone();
        for po in &profile.origins {
            if po.context_signature != ctx_signature {
                continue;
            }
            if po.scheme != key.scheme.as_str()
                || po.host != key.host.as_str()
                || po.port != key.port
            {
                continue;
            }
            for ps in &po.samples {
                let endpoint = self
                    .origins
                    .get(origin_id)
                    .into_iter()
                    .flat_map(|o| o.endpoints.iter().copied())
                    .find(|&ep| self.endpoints.get(ep).is_some_and(|n| n.address == ps.addr));
                let Some(endpoint) = endpoint else { continue };
                let protocol = match ps.protocol.as_str() {
                    "h1" | "http/1.1" => Protocol::Http1_1,
                    "h2" => Protocol::Http2,
                    "h3" => Protocol::Http3,
                    _ => continue,
                };
                let age_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0)
                    .saturating_sub(ps.recorded_at_unix);
                let recorded_at = std::time::Instant::now()
                    .checked_sub(Duration::from_secs(age_secs.min(86_400)))
                    .unwrap_or_else(std::time::Instant::now);
                let Some(node) = self.endpoints.get_mut(endpoint) else {
                    continue;
                };
                let already = node.samples.iter().any(|s| {
                    s.protocol == protocol && (s.receive_rate_bps - ps.rate_bps).abs() < 1.0
                });
                if already {
                    continue;
                }
                node.samples.push(ContextualSample {
                    network_context: self.current_context,
                    protocol,
                    receive_rate_bps: ps.rate_bps,
                    recorded_at,
                });
            }
            if let Some(o) = self.origins.get_mut(origin_id) {
                o.supports_ranges = o.supports_ranges.or(po.supports_ranges);
                o.h3_alt_port = o.h3_alt_port.or(po.h3_alt_port);
                if po.h3_alt_port.is_some() {
                    o.advertises_h3 = true;
                }
            }
        }
    }

    pub fn assert_invariants(&self) {
        for (_, job) in self.jobs.iter() {
            debug_assert!(
                self.destinations.contains_key(job.destination),
                "job points at missing destination"
            );
            debug_assert!(self.artifacts.contains_key(job.artifact));
            debug_assert!(job.sources.iter().all(|s| self.sources.contains_key(*s)));
            if let Some(plan) = &job.plan
                && let Some(total) = plan.total()
            {
                debug_assert!(plan.completed().covered_len() <= total);
            }
        }
        // At most one active job per destination lease.
        for (dest_id, dest) in self.destinations.iter() {
            let holders: Vec<_> = self
                .jobs
                .iter()
                .filter(|(_, j)| j.destination == dest_id)
                .collect();
            if dest.leased_by.is_some() || !holders.is_empty() {
                debug_assert!(
                    holders.len() <= 1,
                    "more than one job holds lease on {}",
                    dest.key
                );
            }
        }
        // Every connection belongs to exactly one origin list.
        for (_, conn) in self.connections.iter() {
            if let Some(o) = self.origins.get(conn.origin) {
                debug_assert!(
                    o.connections.contains(&conn.id),
                    "connection {:?} missing from its origin's list",
                    conn.id
                );
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DestinationLeaseError {
    Busy(String),
    NoSource,
}

/// Stable, human-inspectable signature of a network context. Evidence is
/// only reused across engine runs when this string matches, so switching
/// from Wi-Fi to Ethernet (or a different gateway) never inherits stale
/// rates.
fn context_signature(key: &NetworkContextKey) -> String {
    format!(
        "iface={:?} family={:?} gw={:?} mtu={:?}",
        key.iface_name, key.family, key.gateway, key.mtu
    )
}
