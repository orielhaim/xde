//! Semantic credential/source refresh.
//!
//! Applications provide a [`SourceRefresher`] when a source's access may
//! expire (rotating bearer tokens, signed URLs). The engine invokes it on
//! 401 / eligible 403 and applies the result to the job's source. This is a
//! semantic interface: the refresher returns *where to ask now*, never raw
//! secret material for logs or profiles.

use http::HeaderMap;

/// Why the engine asks for refreshed source information.
#[derive(Debug, Clone)]
pub struct RefreshRequest {
    /// The URL that was rejected.
    pub url: String,
    /// HTTP status that triggered the refresh (401, 403, ...).
    pub status: u16,
    /// Zero-based count of prior refreshes for this job.
    pub attempt: u32,
}

/// What a refresher may change about the source.
///
/// A credential change does NOT prove the representation changed; the
/// engine still validates identity through normal probing and resume
/// evidence before trusting previously downloaded bytes.
#[derive(Debug, Clone, Default)]
pub struct RefreshedSource {
    /// Replacement request target (e.g. a freshly signed URL). `None` keeps
    /// the current URL.
    pub url: Option<String>,
    /// Replacement representation-affecting headers. Credential headers
    /// (Authorization/Cookie) may be included here; they are never logged,
    /// traced, emitted in events, or written to journals/profiles.
    pub headers: Option<HeaderMap>,
}

/// Application-provided refresh capability. Called from the engine's control
/// thread; implementations should be quick and non-blocking where possible.
pub trait SourceRefresher: Send + Sync {
    fn refresh(&self, request: &RefreshRequest) -> Option<RefreshedSource>;
}

/// Adapter helper for closures.
impl<F> SourceRefresher for F
where
    F: Fn(&RefreshRequest) -> Option<RefreshedSource> + Send + Sync,
{
    fn refresh(&self, request: &RefreshRequest) -> Option<RefreshedSource> {
        self(request)
    }
}
