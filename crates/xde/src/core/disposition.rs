//! Status/error → disposition classification.
//!
//! Pure policy: given what the wire produced, what should the controller do?
//! Errors describe what happened; dispositions describe what to do next. The
//! distinction is load-bearing - ENOSPC *describes* a full disk, and its
//! disposition is fatal-for-job, never retry-on-network.

use std::time::Duration;

use crate::core::error::{Error, TransportError};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disposition {
    /// The response is what we asked for.
    Accept,
    /// Retry this exact range against the same source.
    RetrySameRange {
        after: Option<Duration>,
        reason: &'static str,
    },
    /// Retry, and cool down the origin - the limit is server-side, not local.
    BackOffOrigin {
        after: Option<Duration>,
        reason: &'static str,
    },
    /// Signed URL expired or auth rejected: application must intervene.
    RefreshCredentials { status: u16 },
    /// The artifact changed under us. Everything local is void.
    InvalidateArtifact { reason: &'static str },
    /// Re-probe the length; 416 sometimes just means "you already finished".
    RecheckLength,
    /// The server ignored our Range. Never write a 200 body at a resume offset.
    FullBodyForRangeRequest,
    /// Origin lost range capability mid-transfer on this worker.
    RangeCapabilityLost,
    /// This source served bytes inconsistent with the verified artifact
    /// (overlap/hash/representation conflict). Stop using it; its
    /// unverified work is drained and reassigned to healthy sources.
    QuarantineSource { reason: &'static str },
    /// Permanent for this source.
    Fatal { status: u16, reason: &'static str },
}

pub fn classify_status(
    status: u16,
    requested: Option<crate::core::ranges::ByteRange>,
    retry_after: Option<Duration>,
    is_resume: bool,
) -> Disposition {
    match status {
        // Range ignored. On a fresh job this is merely "no range support";
        // on a resume it would corrupt the artifact at the old offset.
        200 if requested.is_some() => {
            if is_resume {
                Disposition::FullBodyForRangeRequest
            } else {
                Disposition::Accept
            }
        }
        200 | 204 => Disposition::Accept,
        206 => Disposition::Accept,

        301 | 302 | 303 | 307 | 308 => Disposition::Fatal {
            status,
            reason: "redirect must be resolved before fetching",
        },

        401 | 403 => Disposition::RefreshCredentials { status },

        404 | 410 => Disposition::Fatal {
            status,
            reason: "resource gone",
        },

        408 => Disposition::RetrySameRange {
            after: None,
            reason: "request timeout",
        },

        412 => Disposition::InvalidateArtifact {
            reason: "precondition failed",
        },

        416 => Disposition::RecheckLength,

        429 => Disposition::BackOffOrigin {
            after: retry_after,
            reason: "rate limited",
        },

        500 | 502 | 504 => Disposition::RetrySameRange {
            after: None,
            reason: "upstream error",
        },
        503 => Disposition::BackOffOrigin {
            after: retry_after,
            reason: "service unavailable",
        },

        s if (500..600).contains(&s) => Disposition::RetrySameRange {
            after: None,
            reason: "server error",
        },
        s => Disposition::Fatal {
            status: s,
            reason: "unhandled status",
        },
    }
}

pub fn parse_retry_after(v: &str) -> Option<Duration> {
    if let Ok(secs) = v.trim().parse::<u64>() {
        return Some(Duration::from_secs(secs.min(3600)));
    }
    let when = crate::core::representation::parse_http_date(v)?;
    when.duration_since(std::time::SystemTime::now())
        .ok()
        .map(|d| d.min(Duration::from_secs(3600)))
}

/// Map an error into a disposition without pretending to know more than we do.
pub fn classify_transport_error(e: &Error) -> Disposition {
    match e {
        Error::Transport(TransportError::Io(io)) => match io.kind() {
            std::io::ErrorKind::ConnectionReset
            | std::io::ErrorKind::ConnectionAborted
            | std::io::ErrorKind::BrokenPipe
            | std::io::ErrorKind::UnexpectedEof
            | std::io::ErrorKind::TimedOut => Disposition::RetrySameRange {
                after: None,
                reason: "connection dropped",
            },
            std::io::ErrorKind::ConnectionRefused | std::io::ErrorKind::HostUnreachable => {
                Disposition::BackOffOrigin {
                    after: None,
                    reason: "origin unreachable",
                }
            }
            _ => Disposition::RetrySameRange {
                after: None,
                reason: "io error",
            },
        },
        Error::Transport(TransportError::ConnectionRetired { .. }) => Disposition::RetrySameRange {
            after: None,
            reason: "connection retired",
        },
        Error::Transport(TransportError::ConnectTimeout) => Disposition::BackOffOrigin {
            after: None,
            reason: "connect timeout",
        },
        Error::Http(crate::core::error::HttpError::RateLimited(after)) => {
            Disposition::BackOffOrigin {
                after: *after,
                reason: "rate limited",
            }
        }
        Error::Http(crate::core::error::HttpError::PrematureEof { .. }) => {
            Disposition::RetrySameRange {
                after: None,
                reason: "premature eof",
            }
        }
        Error::Http(crate::core::error::HttpError::RangeCapabilityLost) => {
            Disposition::FullBodyForRangeRequest
        }
        // The fetch layer already classified this status; pass it through.
        Error::Http(crate::core::error::HttpError::Dispositioned(d, _)) => (**d).clone(),
        Error::Http(crate::core::error::HttpError::Protocol(_)) => Disposition::RetrySameRange {
            after: None,
            reason: "protocol error",
        },
        Error::OverlapMismatch { .. } => Disposition::QuarantineSource {
            reason: "overlap mismatch against verified data",
        },
        Error::RepresentationChanged { .. } => Disposition::InvalidateArtifact {
            reason: "representation changed",
        },
        Error::Cancelled => Disposition::Fatal {
            status: 0,
            reason: "cancelled",
        },
        Error::DeadlineExceeded => Disposition::Fatal {
            status: 0,
            reason: "deadline exceeded",
        },
        // Destination/persistence/config errors are never network problems.
        _ => Disposition::Fatal {
            status: 0,
            reason: "unrecoverable",
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    #[test]
    fn destination_errors_are_never_network_retryable() {
        let err = Error::Destination(crate::core::error::DestinationError::Enospc);
        assert!(matches!(
            classify_transport_error(&err),
            Disposition::Fatal { .. }
        ));
    }

    #[rstest]
    #[case(200, Some(crate::core::ranges::ByteRange::new(100, 200)), true, "full")]
    #[case(
        200,
        Some(crate::core::ranges::ByteRange::new(0, 100)),
        false,
        "accept"
    )]
    #[case(404, None, false, "fatal")]
    #[case(429, None, false, "backoff")]
    #[case(416, None, false, "recheck")]
    #[case(401, None, false, "refresh")]
    fn status_matrix(
        #[case] status: u16,
        #[case] requested: Option<crate::core::ranges::ByteRange>,
        #[case] resume: bool,
        #[case] kind: &str,
    ) {
        let d = classify_status(status, requested, Some(Duration::from_secs(5)), resume);
        match kind {
            "full" => assert!(matches!(d, Disposition::FullBodyForRangeRequest)),
            "accept" => assert!(matches!(d, Disposition::Accept)),
            "fatal" => assert!(matches!(d, Disposition::Fatal { status: 404, .. })),
            "backoff" => assert!(matches!(d, Disposition::BackOffOrigin { .. })),
            "recheck" => assert_eq!(d, Disposition::RecheckLength),
            "refresh" => assert!(matches!(d, Disposition::RefreshCredentials { status: 401 })),
            _ => unreachable!(),
        }
    }
}
