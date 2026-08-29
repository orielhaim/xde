use slotmap::new_key_type;

new_key_type! {
    /// Generational job handle. A stale JobId can never alias a fresh job.
    pub struct JobId;
    pub struct ArtifactId;
    pub struct SourceId;
    pub struct OriginId;
    pub struct EndpointId;
    pub struct ConnectionId;
    pub struct PathId;
    pub struct SessionId;
    pub struct StreamId;
    pub struct AssignId;
    pub struct DestinationId;
    pub struct NetworkContextId;
}

/// Stable, serializable identity of an origin (scheme + host + port + the
/// request headers that can change the answer).
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct TransportOriginKey {
    pub scheme: compact_str::CompactString,
    pub host: compact_str::CompactString,
    pub port: u16,
}

impl TransportOriginKey {
    pub fn authority(&self) -> String {
        let default = matches!(
            (self.scheme.as_str(), self.port),
            ("https", 443) | ("http", 80)
        );
        if default {
            self.host.to_string()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// The origin a URL belongs to. Non-HTTP URLs yield `None`; the engine
    /// never follows redirects out of the http(s) schemes.
    pub fn from_url(url: &url::Url) -> Option<Self> {
        let scheme = url.scheme();
        if !matches!(scheme, "http" | "https") {
            return None;
        }
        Some(Self {
            scheme: scheme.into(),
            host: url.host_str().unwrap_or_default().into(),
            port: url.port_or_known_default().unwrap_or(80),
        })
    }

    pub fn same_origin(&self, other: &Self) -> bool {
        self == other
    }
}

/// A globally-unique reference to an assignment: `AssignId` is only unique
/// within a job's `SegmentPlan`, so any cross-job map or protocol message
/// must key on this pair instead of the bare `AssignId`.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct AssignmentRef {
    pub job: JobId,
    pub assignment: AssignId,
}

impl AssignmentRef {
    pub fn new(job: JobId, assignment: AssignId) -> Self {
        Self { job, assignment }
    }
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct EndpointKey {
    pub origin: TransportOriginKey,
    pub address: std::net::SocketAddr,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct RepresentationKey {
    pub url: compact_str::CompactString,
    pub context_fingerprint: [u8; 32],
}
