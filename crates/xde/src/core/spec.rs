use std::time::{Duration, Instant};

use compact_str::CompactString;
use http::{HeaderMap, HeaderName, HeaderValue};
use smallvec::SmallVec;
use url::Url;

use crate::core::policy::TransferPolicy;

/// What the application wants. Never how.
#[derive(Debug, Clone)]
pub struct JobSpec {
    pub sources: SmallVec<[SourceRequest; 4]>,
    pub integrity: IntegritySpec,
    pub priority: Priority,
    pub urgency: Urgency,
    pub deadline: Option<Instant>,
    pub persistence: Durability,
    pub policy: TransferPolicy,
    pub label: Option<CompactString>,
}

impl JobSpec {
    pub fn new(url: Url) -> Self {
        Self {
            sources: smallvec::smallvec![SourceRequest::new(url)],
            integrity: IntegritySpec::default(),
            priority: Priority::Normal,
            urgency: Urgency::ThroughputSensitive,
            deadline: None,
            persistence: Durability::default(),
            policy: TransferPolicy::default(),
            label: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceRequest {
    pub url: Url,
    pub headers: HeaderMap,
    /// Application-declared hint. The engine still probes; this only orders
    /// the initial candidate list.
    pub weight: u8,
}

impl SourceRequest {
    pub fn new(url: Url) -> Self {
        Self {
            url,
            headers: HeaderMap::new(),
            weight: 128,
        }
    }

    pub fn origin_key(&self) -> crate::core::ids::TransportOriginKey {
        let scheme = self.url.scheme();
        let host = self.url.host_str().unwrap_or("localhost");
        let port = self.url.port_or_known_default().unwrap_or(80);
        crate::core::ids::TransportOriginKey {
            scheme: scheme.into(),
            host: host.into(),
            port,
        }
    }

    pub fn header(mut self, name: HeaderName, value: HeaderValue) -> crate::core::Result<Self> {
        reject_managed_header(&name)?;
        self.headers.append(name, value);
        Ok(self)
    }

    pub fn try_header(mut self, name: &str, value: &str) -> crate::core::Result<Self> {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|error| crate::core::Error::Config(format!("invalid header name: {error}")))?;
        let value = HeaderValue::from_str(value).map_err(|error| {
            crate::core::Error::Config(format!("invalid header value: {error}"))
        })?;
        reject_managed_header(&name)?;
        self.headers.append(name, value);
        Ok(self)
    }

    /// Headers can change the response, so they are part of source identity.
    /// Representation context separates cache-influencing headers from authorization/session credentials.
    pub fn representation_fingerprint(&self) -> [u8; 32] {
        Self::fingerprint_of(&self.url, &self.headers)
    }

    /// Fingerprint of a request target plus its representation-affecting
    /// headers. Credential headers are excluded so a refresh does not look
    /// like a representation change.
    pub fn fingerprint_of(url: &Url, headers: &HeaderMap) -> [u8; 32] {
        let mut entries: Vec<_> = headers
            .iter()
            .filter(|(name, _)| {
                !matches!(
                    name.as_str(),
                    "authorization" | "proxy-authorization" | "cookie"
                )
            })
            .map(|(name, value)| (name.as_str().as_bytes(), value.as_bytes()))
            .collect();
        entries.sort_unstable();
        let mut hasher = blake3::Hasher::new_derive_key("xde representation context v1");
        hasher.update(url.as_str().as_bytes());
        for (name, value) in entries {
            hasher.update(&(name.len() as u64).to_le_bytes());
            hasher.update(name);
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value);
        }
        *hasher.finalize().as_bytes()
    }
}

fn reject_managed_header(name: &HeaderName) -> crate::core::Result<()> {
    if matches!(
        name.as_str(),
        "host"
            | "range"
            | "if-range"
            | "content-length"
            | "accept-encoding"
            | "transfer-encoding"
            | "connection"
            | "keep-alive"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "upgrade"
    ) {
        return Err(crate::core::Error::Config(format!(
            "header {name} is managed by XDE"
        )));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Priority {
    Background = 0,
    Low = 1,
    #[default]
    Normal = 2,
    High = 3,
    Critical = 4,
}

impl Priority {
    /// Weight used by the max-min allocator. Deliberately super-linear so
    /// Critical actually dominates instead of politely queueing.
    pub fn weight(self) -> f64 {
        match self {
            Priority::Background => 0.25,
            Priority::Low => 1.0,
            Priority::Normal => 4.0,
            Priority::High => 12.0,
            Priority::Critical => 40.0,
        }
    }
}

/// Orthogonal to priority. A 2MB manifest and a 90GB ISO can both be Critical
/// and still want completely different treatment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Urgency {
    /// Minimize time-to-first-byte and time-to-complete for a small object.
    LatencySensitive,
    #[default]
    /// Maximize sustained throughput; a few hundred ms of ramp is fine.
    ThroughputSensitive,
}

impl Urgency {
    pub fn piece_duration_target(self) -> Duration {
        match self {
            // Short pieces: more scheduling overhead, far better work stealing.
            Urgency::LatencySensitive => Duration::from_millis(800),
            Urgency::ThroughputSensitive => Duration::from_millis(3000),
        }
    }
    pub fn allocator_bias(self) -> f64 {
        match self {
            Urgency::LatencySensitive => 1.6,
            Urgency::ThroughputSensitive => 1.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// No guarantees. Fastest. On crash, resume falls back to overlap re-verification.
    Relaxed,
    /// Checkpoint + fdatasync every interval.
    Periodic(Duration),
    /// Every completed piece is durable before it enters the journal.
    CrashSafe,
}

impl Default for Durability {
    fn default() -> Self {
        Durability::Periodic(Duration::from_secs(2))
    }
}

#[derive(Debug, Clone, Default)]
pub struct IntegritySpec {
    pub expected: Option<ExpectedDigest>,
    /// Hash computed over the finished artifact even without an expected value,
    /// so the caller gets a checksum for free.
    pub compute: Option<HashKind>,
    /// Bytes of overlap requested between adjacent ranges. 0 disables the check.
    pub overlap_bytes: u32,
    /// Re-verify the boundary bytes of every completed range when resuming.
    pub verify_on_resume: bool,
}

impl IntegritySpec {
    pub fn strict(expected: ExpectedDigest) -> Self {
        Self {
            expected: Some(expected),
            compute: None,
            overlap_bytes: 4096,
            verify_on_resume: true,
        }
    }
    pub fn overlap_only(bytes: u32) -> Self {
        Self {
            expected: None,
            compute: None,
            overlap_bytes: bytes,
            verify_on_resume: true,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashKind {
    Blake3,
    Sha256,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectedDigest {
    Blake3([u8; 32]),
    Sha256([u8; 32]),
}

/// A computed artifact digest. The same 32-byte shape covers BLAKE3 and
/// SHA-256; `kind` keeps the algorithms distinguishable forever.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Digest {
    pub kind: HashKind,
    pub value: [u8; 32],
}

impl Digest {
    pub fn hex(&self) -> String {
        self.value.iter().map(|b| format!("{b:02x}")).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DigestCheck {
    pub kind: HashKind,
    /// `None`: compute and report only. `Some`: a mismatch fails the commit
    /// and the artifact is NOT published.
    pub expected: Option<[u8; 32]>,
}

impl From<ExpectedDigest> for Digest {
    fn from(e: ExpectedDigest) -> Self {
        match e {
            ExpectedDigest::Blake3(v) => Self {
                kind: HashKind::Blake3,
                value: v,
            },
            ExpectedDigest::Sha256(v) => Self {
                kind: HashKind::Sha256,
                value: v,
            },
        }
    }
}

impl ExpectedDigest {
    pub fn kind(&self) -> HashKind {
        match self {
            ExpectedDigest::Blake3(_) => HashKind::Blake3,
            ExpectedDigest::Sha256(_) => HashKind::Sha256,
        }
    }
    pub fn bytes(&self) -> &[u8; 32] {
        match self {
            ExpectedDigest::Blake3(b) | ExpectedDigest::Sha256(b) => b,
        }
    }
    pub fn parse_hex(kind: HashKind, s: &str) -> Option<Self> {
        let s = s.trim();
        if s.len() != 64 {
            return None;
        }
        let mut out = [0u8; 32];
        for (i, chunk) in s.as_bytes().as_chunks::<2>().0.iter().enumerate() {
            let hi = (chunk[0] as char).to_digit(16)? as u8;
            let lo = (chunk[1] as char).to_digit(16)? as u8;
            out[i] = (hi << 4) | lo;
        }
        Some(match kind {
            HashKind::Blake3 => ExpectedDigest::Blake3(out),
            HashKind::Sha256 => ExpectedDigest::Sha256(out),
        })
    }
}
