use std::time::Duration;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("transport io: {0}")]
    Io(#[from] std::io::Error),

    #[error("transport connection retired: {reason}")]
    ConnectionRetired { reason: String },

    #[error("transport connect timeout")]
    ConnectTimeout,

    #[error("transport handshake failed: {0}")]
    HandshakeFailed(String),

    #[error("tls error: {0}")]
    Tls(String),
}

#[derive(Debug, thiserror::Error)]
pub enum HttpError {
    #[error("http protocol violation: {0}")]
    Protocol(String),

    #[error("server refused range request (status {status})")]
    RangeUnsupported { status: u16 },

    #[error("oversized body: received {received} bytes, expected {expected}")]
    OversizedBody { received: u64, expected: u64 },

    #[error("premature eof: received {received} bytes, expected {expected}")]
    PrematureEof { received: u64, expected: u64 },

    #[error("invalid http status: {0}")]
    InvalidStatus(u16),

    #[error("unexpected http status: {status} ({message})")]
    UnexpectedStatus { status: u16, message: String },

    #[error("unexpected 206 Partial Content received")]
    UnexpectedPartialContent,

    #[error("compressed response to identity request")]
    CompressedPayload,

    #[error("rate limited by origin, retry after {0:?}")]
    RateLimited(Option<Duration>),

    #[error("range capability lost; source returned full body or unexpected status")]
    RangeCapabilityLost,

    /// The server answered a status whose controller disposition is not
    /// Accept. Carries the typed classification so the shard can hand it
    /// back to the control plane unchanged.
    #[error("http status {1} requires controller action")]
    Dispositioned(Box<crate::core::disposition::Disposition>, u16),
}

#[derive(Debug, thiserror::Error)]
pub enum DestinationError {
    #[error("destination io: {0}")]
    Io(#[from] std::io::Error),

    #[error("destination rejected operation: {0}")]
    Rejected(String),

    #[error("no space left on device")]
    Enospc,

    #[error("destination lease conflict: {0}")]
    LeaseConflict(String),

    #[error("non-idempotent destination cannot safely retry range {0}")]
    NonIdempotentRetry(String),
}

#[derive(Debug, thiserror::Error)]
pub enum PersistenceError {
    #[error("persistence io: {0}")]
    Io(#[from] std::io::Error),

    #[error("journal is corrupt or unreadable: {0}")]
    Corrupt(String),
}

#[derive(Debug, thiserror::Error)]
pub enum ResolverError {
    #[error("resolver io: {0}")]
    Io(#[from] std::io::Error),

    #[error("domain not found: {0}")]
    NotFound(String),

    #[error("dns resolution timed out")]
    Timeout,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Transport(#[from] TransportError),

    #[error(transparent)]
    Http(#[from] HttpError),

    #[error(transparent)]
    Destination(#[from] DestinationError),

    #[error(transparent)]
    Persistence(#[from] PersistenceError),

    #[error(transparent)]
    Resolver(#[from] ResolverError),

    #[error("representation changed remotely; local state invalidated ({reason})")]
    RepresentationChanged { reason: String },

    #[error("integrity mismatch: {0}")]
    Integrity(String),

    #[error("overlap verification failed at offset {offset}")]
    OverlapMismatch { offset: u64 },

    #[error("credentials expired and no provider could refresh them")]
    CredentialsExpired,

    #[error("job cancelled")]
    Cancelled,

    #[error("transfer deadline exceeded")]
    DeadlineExceeded,

    #[error("transfer stalled: {reason}")]
    Stalled { reason: String },

    #[error("engine shut down")]
    EngineGone,

    #[error("configuration error: {0}")]
    Config(String),

    #[error(transparent)]
    Runtime(#[from] RuntimeError),

    #[error(transparent)]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

/// Runtime initialization / mailbox / shard lifecycle failures.
///
/// These are not network-retryable: they mean the engine itself could not
/// accept the work (shards failed to start, the control mailbox is full, the
/// control loop has exited). I/O from transport, destination, persistence
/// and resolver layers stays in its own typed domain.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("runtime initialization failed: {0}")]
    Init(String),

    #[error("engine control mailbox is full")]
    EngineBusy,

    #[error("engine control loop has exited")]
    EngineGone,

    #[error("runtime thread creation failed: {0}")]
    ThreadCreate(String),

    #[error("runtime join failed: {0}")]
    Join(String),
}

impl Error {
    pub fn protocol(m: impl Into<String>) -> Self {
        Self::Http(HttpError::Protocol(m.into()))
    }

    pub fn destination(m: impl Into<String>) -> Self {
        Self::Destination(DestinationError::Rejected(m.into()))
    }

    pub fn journal(m: impl Into<String>) -> Self {
        Self::Persistence(PersistenceError::Corrupt(m.into()))
    }

    pub fn is_destination_error(&self) -> bool {
        matches!(self, Error::Destination(_))
    }

    pub fn is_persistence_error(&self) -> bool {
        matches!(self, Error::Persistence(_))
    }

    /// Whether the *same* byte range may be retried against the network source.
    /// Destination errors (such as ENOSPC or write rejections) are NEVER network retryable.
    pub fn is_range_retryable(&self) -> bool {
        matches!(
            self,
            Error::Transport(TransportError::Io(_))
                | Error::Transport(TransportError::ConnectionRetired { .. })
                | Error::Transport(TransportError::ConnectTimeout)
                | Error::Http(HttpError::RateLimited(_))
                | Error::Http(HttpError::PrematureEof { .. })
                | Error::Http(HttpError::Protocol(_))
        )
    }

    /// Whether local state (journal + .part) must be thrown away.
    pub fn invalidates_state(&self) -> bool {
        matches!(
            self,
            Error::RepresentationChanged { .. } | Error::OverlapMismatch { .. }
        )
    }
}
