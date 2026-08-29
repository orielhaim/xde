//! Representation identity.
//!
//! A URL is a terrible identifier for content: it can redirect, rotate across
//! CDN edges, or serve different bytes to different request headers. Everything
//! in this module answers one question - *are the bytes I am about to write
//! next to already-written bytes part of the same representation?* - and it
//! lives in core because the HTTP layer produces the evidence, the scheduler
//! enforces it, and the journal persists it.

use std::time::SystemTime;

use compact_str::CompactString;
use http::HeaderMap;
use smallvec::SmallVec;
use url::Url;

#[derive(Debug, Clone, PartialEq)]
pub struct RemoteFingerprint {
    pub content_length: Option<u64>,
    pub etag: Option<CompactString>,
    pub last_modified: Option<SystemTime>,
    pub final_url: Url,
    pub redirect_chain: SmallVec<[Url; 4]>,
    /// Non-identity content coding. A compressed response makes ordinary
    /// resume invalid: ranges address representation bytes, not payload bytes.
    pub content_coding: Option<CompactString>,
    /// Hint only. RFC 9110 allows Range to be tried without this header and
    /// promises nothing about the next response even when it is present.
    pub accept_ranges_hint: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidatorStrength {
    Strong,
    Weak,
    None,
}

impl RemoteFingerprint {
    pub fn from_headers(url: Url, headers: &HeaderMap) -> Self {
        let etag = headers
            .get(http::header::ETAG)
            .and_then(|v| v.to_str().ok())
            .map(CompactString::from);

        let last_modified = headers
            .get(http::header::LAST_MODIFIED)
            .and_then(|v| v.to_str().ok())
            .and_then(parse_http_date);

        let content_length = headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());

        let content_coding = headers
            .get(http::header::CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.trim().eq_ignore_ascii_case("identity"))
            .map(CompactString::from);

        let accept_ranges_hint = headers
            .get(http::header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.split(',').any(|t| t.trim().eq_ignore_ascii_case("bytes")));

        Self {
            content_length,
            etag,
            last_modified,
            final_url: url,
            redirect_chain: SmallVec::new(),
            content_coding,
            accept_ranges_hint,
        }
    }

    pub fn validator_strength(&self) -> ValidatorStrength {
        match &self.etag {
            Some(e) if !e.starts_with("W/") && e.starts_with('"') => ValidatorStrength::Strong,
            Some(_) => ValidatorStrength::Weak,
            None if self.last_modified.is_some() => ValidatorStrength::Weak,
            None => ValidatorStrength::None,
        }
    }

    /// The value for `If-Range`. RFC 9110 forbids a weak entity-tag here, and
    /// a plain Last-Modified date is not necessarily a strong validator either;
    /// weak resume relies on overlap verification instead.
    pub fn if_range_value(&self) -> Option<String> {
        match self.validator_strength() {
            ValidatorStrength::Strong => self.etag.as_ref().map(|e| e.to_string()),
            ValidatorStrength::Weak | ValidatorStrength::None => None,
        }
    }

    pub fn is_compressed(&self) -> bool {
        self.content_coding.is_some()
    }

    pub fn to_journal(&self, header_identity: [u8; 32]) -> JournaledFingerprint {
        JournaledFingerprint {
            content_length: self.content_length,
            etag: self.etag.as_ref().map(|e| e.to_string()),
            last_modified_unix: self.last_modified.and_then(|t| {
                t.duration_since(SystemTime::UNIX_EPOCH)
                    .ok()
                    .map(|d| d.as_secs())
            }),
            final_url: self.final_url.to_string(),
            redirect_chain: self.redirect_chain.iter().map(|u| u.to_string()).collect(),
            header_identity,
            content_coding: self.content_coding.as_ref().map(|c| c.to_string()),
        }
    }

    pub fn compare_with_journal(
        &self,
        j: &JournaledFingerprint,
        header_identity: [u8; 32],
    ) -> ValidatorMatch {
        let Ok(previous_url) = Url::parse(&j.final_url) else {
            return ValidatorMatch::Mismatch;
        };
        if self.final_url.scheme() != previous_url.scheme()
            || self.final_url.host_str() != previous_url.host_str()
            || self.final_url.port_or_known_default() != previous_url.port_or_known_default()
        {
            return ValidatorMatch::Mismatch;
        }
        self.to_journal(header_identity).matches(j)
    }
}

/// Serializable evidence about the remote representation, persisted beside the
/// `.part`. A transaction log entry, not a progress percentage.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct JournaledFingerprint {
    pub content_length: Option<u64>,
    pub etag: Option<String>,
    pub last_modified_unix: Option<u64>,
    pub final_url: String,
    pub redirect_chain: Vec<String>,
    /// Hash of the identity-relevant request headers.
    pub header_identity: [u8; 32],
    pub content_coding: Option<String>,
}

impl JournaledFingerprint {
    /// Is this the same representation we were downloading before?
    pub fn matches(&self, other: &JournaledFingerprint) -> ValidatorMatch {
        if self.header_identity != other.header_identity {
            return ValidatorMatch::Unknown;
        }
        match (&self.etag, &other.etag) {
            (Some(a), Some(b)) if a == b && !a.starts_with("W/") => ValidatorMatch::Strong,
            (Some(a), Some(b)) if a == b => ValidatorMatch::Weak,
            (Some(_), Some(_)) => ValidatorMatch::Mismatch,
            _ => match (self.last_modified_unix, other.last_modified_unix) {
                (Some(a), Some(b)) if a == b => match (self.content_length, other.content_length) {
                    (Some(x), Some(y)) if x == y => ValidatorMatch::Weak,
                    (Some(_), Some(_)) => ValidatorMatch::Mismatch,
                    _ => ValidatorMatch::Weak,
                },
                _ => match (self.content_length, other.content_length) {
                    (Some(x), Some(y)) if x != y => ValidatorMatch::Mismatch,
                    _ => ValidatorMatch::Unknown,
                },
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidatorMatch {
    /// Strong validator matched. Resume freely.
    Strong,
    /// Weak evidence. Resume, but verify overlaps.
    Weak,
    /// No usable validators. Resume only with overlap verification, or restart.
    Unknown,
    /// Definitely a different representation. Discard local state.
    Mismatch,
}

pub fn parse_http_date(s: &str) -> Option<SystemTime> {
    httpdate::parse_http_date(s).ok()
}

pub fn format_http_date(t: SystemTime) -> String {
    httpdate::fmt_http_date(t)
}

/// Validates that parallel range requests against an origin continue to see
/// the same underlying representation. Held per job once the first response
/// establishes the evidence.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepresentationLock {
    pub etag: Option<String>,
    pub last_modified: Option<SystemTime>,
    pub total_length: Option<u64>,
}

impl RepresentationLock {
    pub fn new(
        etag: Option<String>,
        last_modified: Option<SystemTime>,
        total_length: Option<u64>,
    ) -> Self {
        Self {
            etag,
            last_modified,
            total_length,
        }
    }

    pub fn validate(
        &mut self,
        etag: Option<&str>,
        last_modified: Option<SystemTime>,
        total_length: Option<u64>,
    ) -> Result<(), crate::core::error::Error> {
        match (&self.etag, etag) {
            (Some(expected), Some(actual)) if expected != actual => {
                return Err(crate::core::Error::RepresentationChanged {
                    reason: format!(
                        "ETag mismatch across range requests: expected {expected}, got {actual}"
                    ),
                });
            }
            (None, Some(actual)) => self.etag = Some(actual.to_string()),
            _ => {}
        }

        match (&self.last_modified, last_modified) {
            (Some(expected), Some(actual)) if *expected != actual => {
                return Err(crate::core::Error::RepresentationChanged {
                    reason: "Last-Modified timestamp changed across range requests".into(),
                });
            }
            (None, Some(actual)) => self.last_modified = Some(actual),
            _ => {}
        }

        match (&self.total_length, total_length) {
            (Some(expected), Some(actual)) if *expected != actual => {
                return Err(crate::core::Error::RepresentationChanged {
                    reason: format!("total length changed from {expected} to {actual}"),
                });
            }
            (None, Some(actual)) => self.total_length = Some(actual),
            _ => {}
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strong_etags_are_usable_for_if_range_weak_are_not() {
        let mut headers = HeaderMap::new();
        headers.insert(http::header::ETAG, "\"v1\"".parse().unwrap());
        let fp = RemoteFingerprint::from_headers(Url::parse("https://x.test/a").unwrap(), &headers);
        assert_eq!(fp.validator_strength(), ValidatorStrength::Strong);
        assert_eq!(fp.if_range_value().as_deref(), Some("\"v1\""));

        headers.insert(http::header::ETAG, "W/\"v1\"".parse().unwrap());
        let fp = RemoteFingerprint::from_headers(Url::parse("https://x.test/a").unwrap(), &headers);
        assert_eq!(fp.validator_strength(), ValidatorStrength::Weak);
        assert!(fp.if_range_value().is_none());
    }

    #[test]
    fn representation_lock_detects_etag_drift() {
        let mut lock = RepresentationLock::new(Some("\"etag-1\"".into()), None, Some(1000));
        assert!(lock.validate(Some("\"etag-1\""), None, Some(1000)).is_ok());
        assert!(lock.validate(Some("\"etag-2\""), None, Some(1000)).is_err());
    }

    #[test]
    fn representation_lock_detects_length_drift() {
        let mut lock = RepresentationLock::new(Some("\"etag-1\"".into()), None, Some(1000));
        assert!(lock.validate(Some("\"etag-1\""), None, Some(2000)).is_err());
    }
}
