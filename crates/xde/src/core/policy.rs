use std::time::Duration;

use crate::core::units::MIB;

/// Per-job resource ceilings and transfer strategy. Fixed topology is a requested target.
#[derive(Debug, Clone)]
pub struct TransferPolicy {
    pub transport: TransportLimits,
    pub topology: TopologyPolicy,
    pub segmentation: SegmentationPolicy,
    pub retry: RetryPolicy,
    pub max_redirects: u8,
    pub redirects: RedirectPolicy,
    pub allow_compressed: bool,
    pub http_version: HttpVersionPolicy,
}

impl Default for TransferPolicy {
    fn default() -> Self {
        Self {
            transport: TransportLimits::default(),
            topology: TopologyPolicy::Adaptive,
            segmentation: SegmentationPolicy::default(),
            retry: RetryPolicy::default(),
            max_redirects: 10,
            redirects: RedirectPolicy::default(),
            allow_compressed: false,
            http_version: HttpVersionPolicy::Auto,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedirectPolicy {
    pub allow_https_to_http: bool,
    pub forward_credentials_cross_origin: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HttpVersionPolicy {
    #[default]
    Auto,
    Http1Only,
    /// Cleartext HTTP/2 prior knowledge for origins explicitly configured for
    /// h2c. Auto mode never guesses this against an ordinary HTTP server.
    Http2PriorKnowledge,
    /// Payload must use HTTP/3. TCP requests may discover Alt-Svc first.
    Http3Only,
}

impl TransferPolicy {
    pub fn builder() -> TransferPolicyBuilder {
        TransferPolicyBuilder(Self::default())
    }
}

#[derive(Debug, Clone)]
pub struct TransferPolicyBuilder(TransferPolicy);

impl TransferPolicyBuilder {
    pub fn max_physical_connections(mut self, v: u8) -> Self {
        self.0.transport.max_physical_connections = v.max(1);
        self
    }
    pub fn max_streams_per_connection(mut self, v: u16) -> Self {
        self.0.transport.max_streams_per_connection = v.max(1);
        self
    }
    pub fn topology(mut self, topology: TopologyPolicy) -> Self {
        self.0.topology = topology;
        self
    }
    pub fn allow_compressed(mut self, v: bool) -> Self {
        self.0.allow_compressed = v;
        self
    }
    pub fn build(self) -> TransferPolicy {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportLimits {
    pub max_physical_connections: u8,
    pub max_streams_per_connection: u16,
    pub max_active_assignments: u16,
}

impl Default for TransportLimits {
    fn default() -> Self {
        Self {
            max_physical_connections: 8,
            max_streams_per_connection: 16,
            max_active_assignments: 8,
        }
    }
}

/// Global Engine-wide resource limits.
/// These act as ceilings across all jobs. Engine limits must NEVER raise a job's stricter ceiling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EngineLimits {
    pub scheduling: SchedulingPolicy,
    /// Range admission quantum when jobs compete. Transport buffering is separate.
    pub fair_quantum_bytes: u64,
    pub memory_bytes: u64,
    pub max_jobs: usize,
    pub max_physical_connections: u16,
    pub max_connections_per_origin: u16,
    /// Engine-wide ceiling on concurrently active assignments across ALL
    /// jobs. This is the opportunity budget the fair allocator distributes.
    pub max_active_assignments: u32,
}

impl Default for EngineLimits {
    fn default() -> Self {
        Self {
            scheduling: SchedulingPolicy::FairShare,
            fair_quantum_bytes: 8 * MIB,
            memory_bytes: 256 * 1024 * 1024,
            max_jobs: 128,
            max_physical_connections: 64,
            max_connections_per_origin: 16,
            max_active_assignments: 32,
        }
    }
}

impl From<TransferPolicyBuilder> for TransferPolicy {
    fn from(b: TransferPolicyBuilder) -> Self {
        b.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SegmentationPolicy {
    pub min_piece: u64,
    pub max_piece: u64,
    /// Below this many bytes remaining, splitting gets aggressive to cut tail latency.
    pub tail_threshold: u64,
    pub tail_min_piece: u64,
    /// Piece boundaries snap to this, so writes stay aligned for the sink.
    pub alignment: u64,
    /// A worker slower than `straggler_z` stddevs below the fleet mean is a straggler.
    pub straggler_z: f64,
    /// ...or a worker below this fraction of the fleet mean is a straggler
    /// regardless of dispersion. Pure z-scores cannot flag anyone in small
    /// fleets (with two workers the max z is ~1 by symmetry), and small
    /// heterogeneous fleets are exactly where stragglers hurt most.
    pub straggler_share: f64,
    /// ...but only after it has been running at least this long.
    pub straggler_grace: Duration,
}

impl Default for SegmentationPolicy {
    fn default() -> Self {
        Self {
            min_piece: 256 * 1024,
            max_piece: 512 * MIB,
            tail_threshold: 64 * MIB,
            tail_min_piece: 64 * 1024,
            alignment: 64 * 1024,
            straggler_z: 2.0,
            straggler_share: 0.5,
            straggler_grace: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts_per_range: u32,
    pub base_backoff: Duration,
    pub max_backoff: Duration,
    /// Full jitter, because synchronized retries against one CDN are a DoS on yourself.
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts_per_range: 6,
            base_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(30),
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Decorrelated-ish exponential backoff. The algorithm is worth borrowing
    /// from `backon`; the mechanism is not, because our retries are semantic.
    pub fn backoff(&self, attempt: u32, rng: &mut impl FnMut() -> f64) -> Duration {
        let exp = self.base_backoff.as_secs_f64() * 2f64.powi(attempt.min(16) as i32);
        let capped = exp.min(self.max_backoff.as_secs_f64());
        let v = if self.jitter { capped * rng() } else { capped };
        Duration::from_secs_f64(v.max(0.001))
    }
}

/// A target, subject to protocol capabilities and engine grants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Topology {
    pub connections: u8,
    pub streams_per_connection: u16,
}
impl Topology {
    pub const fn new(connections: u8, streams_per_connection: u16) -> Self {
        Self {
            connections,
            streams_per_connection,
        }
    }
    pub fn slots(self) -> u32 {
        u32::from(self.connections) * u32::from(self.streams_per_connection)
    }
    pub fn clamped(self, limits: TransportLimits) -> Self {
        Self {
            connections: self
                .connections
                .max(1)
                .min(limits.max_physical_connections.max(1)),
            streams_per_connection: self
                .streams_per_connection
                .max(1)
                .min(limits.max_streams_per_connection.max(1)),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TopologyPolicy {
    #[default]
    Adaptive,
    Fixed(Topology),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SchedulingPolicy {
    #[default]
    FairShare,
    /// Priority order, FIFO within a priority. Active jobs run to completion.
    Queue {
        max_active_jobs: std::num::NonZeroUsize,
    },
}
