use std::time::Duration;

use smallvec::SmallVec;

use crate::core::units::Rate;

/// Beta-ish confidence: successes and failures, not a bool.
/// A single failure against one CDN edge should not permanently disable H2.
#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Confidence {
    pub yes: u32,
    pub no: u32,
}

impl Confidence {
    pub const UNKNOWN: Confidence = Confidence { yes: 0, no: 0 };

    pub fn observe(&mut self, ok: bool) {
        if ok {
            self.yes = self.yes.saturating_add(1);
        } else {
            self.no = self.no.saturating_add(1);
        }
        // Bounded memory: decay once we have enough evidence, so the profile
        // tracks the origin's *current* behavior, not its 2023 behavior.
        if self.yes + self.no > 64 {
            self.yes = self.yes.div_ceil(2);
            self.no = self.no.div_ceil(2);
        }
    }

    /// Laplace-smoothed probability.
    pub fn p(&self) -> f32 {
        (self.yes as f32 + 1.0) / (self.yes as f32 + self.no as f32 + 2.0)
    }

    pub fn is_confident_yes(&self) -> bool {
        self.yes >= 2 && self.p() > 0.8
    }
    pub fn is_confident_no(&self) -> bool {
        self.no >= 3 && self.p() < 0.2
    }
    pub fn samples(&self) -> u32 {
        self.yes + self.no
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum ValidatorBehavior {
    #[default]
    Unknown,
    /// Strong ETag, stable across requests. `If-Range` is trustworthy.
    StrongStable,
    /// Weak ETag only. Usable as a hint, never as a resume guarantee.
    WeakOnly,
    /// ETag changes between identical requests (load-balanced backends).
    /// Fall back to Last-Modified + Content-Length and re-verify overlaps.
    Unstable,
    /// No validators at all. Resume is best-effort with overlap checking only.
    Absent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct RateLimitEvent {
    pub at_unix_secs: u64,
    pub status: u16,
    pub retry_after_secs: Option<u32>,
    pub concurrency_at_event: u16,
}

/// Ordered endpoint candidates. Not "the IP" - a ranked list, because racing
/// on handshake alone is the wrong objective for a 30GB transfer.
#[derive(Debug, Clone, Default)]
pub struct EndpointCandidates {
    pub entries: SmallVec<[EndpointCandidate; 8]>,
}

#[derive(Debug, Clone, Copy)]
pub struct EndpointCandidate {
    pub addr: std::net::SocketAddr,
    pub historical_rate: Option<Rate>,
    pub historical_handshake: Option<Duration>,
    pub score: f32,
}

impl EndpointCandidates {
    /// Sort by expected *throughput*, with handshake as a tiebreak.
    /// A v6 address 4ms slower to connect but 1.5x faster to stream wins here,
    /// which is exactly why generic Happy Eyeballs is the wrong tool.
    pub fn rank(&mut self) {
        self.entries.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    pub fn score_of(rate: Option<Rate>, hs: Option<Duration>) -> f32 {
        let r = rate.map(|r| r.bps()).unwrap_or(0.0);
        let throughput_term = if r > 0.0 {
            (r / 1_000_000.0).ln_1p() as f32
        } else {
            0.0
        };
        let hs_term = hs
            .map(|d| 1.0 / (1.0 + d.as_secs_f32() * 10.0))
            .unwrap_or(0.5);
        throughput_term * 3.0 + hs_term
    }
}

// ---------------------------------------------------------------------------
// Persistent learning
// ---------------------------------------------------------------------------

/// Version of [`PersistedProfile`] on disk. Loaders reject anything else so
/// format evolution never misinterprets old evidence.
pub const PROFILE_FORMAT_VERSION: u32 = 1;

/// One persisted endpoint observation. Semantic keys only: origin identity,
/// address, protocol, and the network context's *stable signature* - never
/// process-local IDs.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PersistedEndpointSample {
    pub addr: std::net::SocketAddr,
    pub protocol: String,
    /// Receive-active goodput in bytes/sec.
    pub rate_bps: f64,
    /// Unix seconds when the sample was taken; consumers apply recency.
    pub recorded_at_unix: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PersistedOrigin {
    pub scheme: String,
    pub host: String,
    pub port: u16,
    /// The network context signature this evidence was gathered under.
    /// Evidence only applies when the current context matches.
    pub context_signature: String,
    pub supports_ranges: Option<bool>,
    /// UDP port where HTTP/3 was advertised and last worked.
    pub h3_alt_port: Option<u16>,
    pub samples: Vec<PersistedEndpointSample>,
}

/// The whole on-disk learning profile: a compact versioned JSON document.
/// Secrets never enter this structure; it holds transport evidence only.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct PersistedProfile {
    pub format_version: u32,
    pub origins: Vec<PersistedOrigin>,
}
