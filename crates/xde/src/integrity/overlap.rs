use crate::core::ranges::ByteRange;

/// Overlap verification, taken from IDM largely as-is because it is the most
/// elegant trick in that whole design.
///
/// Adjacent ranges are requested with a few bytes of overlap and compared;
/// resume re-reads a few bytes backwards and compares against what is already
/// on disk. One mismatch re-downloads that piece. Mismatches on several
/// connections drop the whole job to single-connection mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OverlapVerdict {
    /// Boundary bytes agree.
    Match,
    /// Nothing to compare (start of file, or no prior data).
    Skipped,
    /// Disagreement; re-fetch this piece.
    Mismatch { offset: u64 },
}

#[derive(Debug, Default)]
pub struct OverlapGuard {
    mismatches: u32,
    distinct_connections_failed: u32,
    /// Above this many failures across different connections, the source is
    /// serving inconsistent bytes and parallelism is not safe.
    degrade_threshold: u32,
}

impl OverlapGuard {
    pub fn new(degrade_threshold: u32) -> Self {
        Self {
            mismatches: 0,
            distinct_connections_failed: 0,
            degrade_threshold,
        }
    }

    /// Compare the overlap prefix against what we already have.
    pub fn check(&mut self, at: u64, existing: &[u8], incoming: &[u8]) -> OverlapVerdict {
        let n = existing.len().min(incoming.len());
        if n == 0 {
            return OverlapVerdict::Skipped;
        }
        if existing[..n] == incoming[..n] {
            return OverlapVerdict::Match;
        }
        let first_bad = existing[..n]
            .iter()
            .zip(&incoming[..n])
            .position(|(a, b)| a != b)
            .unwrap_or(0) as u64;
        self.mismatches += 1;
        tracing::warn!(
            target: "xde::integrity",
            offset = at + first_bad,
            "overlap mismatch"
        );
        OverlapVerdict::Mismatch {
            offset: at + first_bad,
        }
    }

    pub fn note_connection_failed(&mut self) {
        self.distinct_connections_failed += 1;
    }

    /// Should we fall back to a single connection?
    pub fn should_degrade(&self) -> bool {
        self.distinct_connections_failed >= self.degrade_threshold
    }

    pub fn mismatches(&self) -> u32 {
        self.mismatches
    }

    /// The prefix to re-read on resume for a range that starts at `start`.
    pub fn resume_probe(start: u64, overlap: u32) -> Option<ByteRange> {
        if start == 0 || overlap == 0 {
            return None;
        }
        let n = (overlap as u64).min(start);
        Some(ByteRange::new(start - n, start))
    }
}

/// Validates that parallel range requests against an origin continue to see
/// the same underlying representation (ETag, Last-Modified, total length).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RepresentationLock {
    pub etag: Option<String>,
    pub last_modified: Option<std::time::SystemTime>,
    pub total_length: Option<u64>,
}

impl RepresentationLock {
    pub fn new(
        etag: Option<String>,
        last_modified: Option<std::time::SystemTime>,
        total_length: Option<u64>,
    ) -> Self {
        Self {
            etag,
            last_modified,
            total_length,
        }
    }

    #[allow(clippy::collapsible_if)]
    pub fn validate(
        &mut self,
        etag: Option<&str>,
        last_modified: Option<std::time::SystemTime>,
        total_length: Option<u64>,
    ) -> crate::core::error::Result<()> {
        if let Some(expected_etag) = &self.etag {
            if let Some(actual_etag) = etag {
                if expected_etag != actual_etag {
                    return Err(crate::core::Error::RepresentationChanged {
                        reason: format!(
                            "ETag mismatch across range requests: expected {expected_etag}, got {actual_etag}"
                        ),
                    });
                }
            }
        } else if let Some(actual_etag) = etag {
            self.etag = Some(actual_etag.to_string());
        }

        if let Some(expected_lm) = self.last_modified {
            if let Some(actual_lm) = last_modified {
                if expected_lm != actual_lm {
                    return Err(crate::core::Error::RepresentationChanged {
                        reason: "Last-Modified timestamp changed across range requests".into(),
                    });
                }
            }
        } else if let Some(actual_lm) = last_modified {
            self.last_modified = Some(actual_lm);
        }

        if let Some(expected_len) = self.total_length {
            if let Some(actual_len) = total_length {
                if expected_len != actual_len {
                    return Err(crate::core::Error::RepresentationChanged {
                        reason: format!("total length changed from {expected_len} to {actual_len}"),
                    });
                }
            }
        } else if let Some(actual_len) = total_length {
            self.total_length = Some(actual_len);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn overlap_guard_identifies_mismatches() {
        let mut guard = OverlapGuard::new(2);
        let existing = b"abcdef";
        let incoming_good = b"abcdef";
        let incoming_bad = b"abcxyz";

        assert_eq!(
            guard.check(100, existing, incoming_good),
            OverlapVerdict::Match
        );
        assert_eq!(
            guard.check(100, existing, incoming_bad),
            OverlapVerdict::Mismatch { offset: 103 }
        );
        assert_eq!(guard.mismatches(), 1);
    }
}
