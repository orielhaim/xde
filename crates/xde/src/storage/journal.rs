use std::{
    path::{Path, PathBuf},
    time::SystemTime,
};

use crate::core::{
    error::{Error, Result},
    ranges::RangeSet,
};
use serde::{Deserialize, Serialize};

// Canonical identity/policy types live in the core representation module.
pub use crate::core::representation::{JournaledFingerprint, ValidatorMatch};

const MAGIC: [u8; 8] = *b"xdeJRNL1";
const FORMAT_VERSION: u16 = 2;

// JournaledFingerprint and ValidatorMatch are canonical in
// the core representation module and re-exported above.

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct JournalPayload {
    pub format_version: u16,
    pub fingerprint: JournaledFingerprint,
    pub completed: RangeSet,
    /// Subset of `completed` known to survive a crash.
    pub durable: RangeSet,
    pub total: Option<u64>,
    pub bytes_written: u64,
    pub created_unix: u64,
    pub updated_unix: u64,
    /// Overlap width in force when these ranges were written, so a policy
    /// change between runs cannot silently weaken verification.
    pub overlap_bytes: u32,
    pub source_urls: Vec<String>,
}

impl JournalPayload {
    pub fn new(fingerprint: JournaledFingerprint, total: Option<u64>, overlap_bytes: u32) -> Self {
        let now = unix_now();
        Self {
            format_version: FORMAT_VERSION,
            fingerprint,
            completed: RangeSet::new(),
            durable: RangeSet::new(),
            total,
            bytes_written: 0,
            created_unix: now,
            updated_unix: now,
            overlap_bytes,
            source_urls: Vec::new(),
        }
    }
}

/// On-disk framing: magic, version, length, crc32c, payload.
/// crc32c is the right tool here - we only need to know whether the record was
/// torn, and SSE4.2 makes it free. No crypto requirement at this layer.
#[derive(Debug)]
pub struct Journal {
    path: PathBuf,
    payload: JournalPayload,
    dirty: bool,
}

impl Journal {
    pub fn path_for(part_path: &Path) -> PathBuf {
        let mut p = part_path.to_path_buf().into_os_string();
        p.push(".state");
        PathBuf::from(p)
    }

    pub fn create(path: PathBuf, payload: JournalPayload) -> Self {
        Self {
            path,
            payload,
            dirty: true,
        }
    }

    /// Load an existing journal. A corrupt journal is not fatal: it means we
    /// resume from nothing, not that the job fails.
    pub async fn load(path: &Path) -> Result<Option<Self>> {
        let path = path.to_path_buf();
        compio::runtime::spawn_blocking(move || Self::load_blocking(&path))
            .await
            .map_err(|_| Error::journal("journal loader panicked"))?
    }

    /// Synchronous variant used by the control thread at admission time.
    pub fn load_blocking(path: &Path) -> Result<Option<Self>> {
        let raw = match std::fs::read(path) {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(Error::Persistence(
                    crate::core::error::PersistenceError::Io(e),
                ));
            }
        };
        if raw.len() < 8 + 2 + 4 + 4 {
            return Err(Error::journal("truncated header"));
        }
        if raw[..8] != MAGIC {
            return Err(Error::journal("bad magic"));
        }
        let version = u16::from_le_bytes([raw[8], raw[9]]);
        if version != FORMAT_VERSION {
            return Err(Error::journal(format!("unsupported version {version}")));
        }
        let len = u32::from_le_bytes([raw[10], raw[11], raw[12], raw[13]]) as usize;
        let crc = u32::from_le_bytes([raw[14], raw[15], raw[16], raw[17]]);
        let body = raw
            .get(18..18 + len)
            .ok_or_else(|| Error::journal("truncated body"))?;
        if crc32c::crc32c(body) != crc {
            return Err(Error::journal("crc mismatch: record torn"));
        }
        let payload: JournalPayload =
            postcard::from_bytes(body).map_err(|e| Error::journal(e.to_string()))?;
        Ok(Some(Self {
            path: path.to_path_buf(),
            payload,
            dirty: false,
        }))
    }

    pub fn payload(&self) -> &JournalPayload {
        &self.payload
    }
    pub fn path(&self) -> &Path {
        &self.path
    }
    pub fn payload_mut(&mut self) -> &mut JournalPayload {
        self.dirty = true;
        &mut self.payload
    }
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Force the next `persist` to rewrite the record even if the in-memory
    /// mutation helpers above were not used.
    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    pub fn record_completed(&mut self, r: crate::core::ranges::ByteRange) {
        self.payload.completed.insert(r);
        self.payload.bytes_written = self.payload.completed.covered_len();
        self.payload.updated_unix = unix_now();
        self.dirty = true;
    }

    pub fn mark_completed_durable(&mut self) {
        self.payload.durable = self.payload.completed.clone();
        self.dirty = true;
    }

    pub fn invalidate(&mut self, r: crate::core::ranges::ByteRange) {
        self.payload.completed.remove(r);
        self.payload.durable.remove(r);
        self.payload.bytes_written = self.payload.completed.covered_len();
        self.dirty = true;
    }

    pub fn clear_ranges(&mut self) {
        self.payload.completed = RangeSet::new();
        self.payload.durable = RangeSet::new();
        self.payload.bytes_written = 0;
        self.dirty = true;
    }

    /// Serialize + write + fsync + atomic rename on the calling thread. The
    /// control plane calls this from its own thread; the write is small
    /// (bounded by range count) and checkpoint-cadence limited.
    pub fn persist_blocking(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        let path = self.path.clone();
        Self::persist_blocking_inner(path, &self.payload)?;
        self.dirty = false;
        Ok(())
    }

    fn persist_blocking_inner(path: PathBuf, payload: &JournalPayload) -> Result<()> {
        let body = postcard::to_allocvec(payload).map_err(|e| Error::journal(e.to_string()))?;
        let crc = crc32c::crc32c(&body);

        let mut out = Vec::with_capacity(18 + body.len());
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        out.extend_from_slice(&(body.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc.to_le_bytes());
        out.extend_from_slice(&body);

        let tmp = {
            let mut p = path.clone().into_os_string();
            p.push(".tmp");
            PathBuf::from(p)
        };
        use std::io::Write as _;
        let mut f = std::fs::File::create(&tmp)
            .map_err(|e| Error::Persistence(crate::core::error::PersistenceError::Io(e)))?;
        f.write_all(&out)
            .map_err(|e| Error::Persistence(crate::core::error::PersistenceError::Io(e)))?;
        f.sync_all()
            .map_err(|e| Error::Persistence(crate::core::error::PersistenceError::Io(e)))?;
        drop(f);
        std::fs::rename(&tmp, &path)
            .map_err(|e| Error::Persistence(crate::core::error::PersistenceError::Io(e)))?;
        sync_parent(&path)?;
        Ok(())
    }

    pub async fn remove(self) -> Result<()> {
        compio::runtime::spawn_blocking(move || match std::fs::remove_file(&self.path) {
            Ok(()) => sync_parent(&self.path),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(Error::Persistence(
                crate::core::error::PersistenceError::Io(e),
            )),
        })
        .await
        .map_err(|_| Error::journal("journal remover panicked"))?
    }
}

fn sync_parent(path: &Path) -> Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)
            .map_err(|e| Error::Persistence(crate::core::error::PersistenceError::Io(e)))?
            .sync_all()
            .map_err(|e| Error::Persistence(crate::core::error::PersistenceError::Io(e)))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ranges::ByteRange;

    fn fingerprint() -> JournaledFingerprint {
        JournaledFingerprint {
            content_length: Some(8),
            etag: Some("\"strong\"".into()),
            last_modified_unix: None,
            final_url: "https://example.test/artifact".into(),
            redirect_chain: Vec::new(),
            header_identity: [0; 32],
            content_coding: None,
        }
    }

    #[test]
    fn written_ranges_are_not_crash_recoverable_until_marked_durable() {
        let mut journal = Journal::create(
            PathBuf::from("unused"),
            JournalPayload::new(fingerprint(), Some(8), 0),
        );
        journal.record_completed(ByteRange::new(0, 8));
        assert_eq!(journal.payload().completed.covered_len(), 8);
        assert_eq!(journal.payload().durable.covered_len(), 0);

        journal.mark_completed_durable();
        assert_eq!(journal.payload().durable.covered_len(), 8);
        journal.invalidate(ByteRange::new(2, 6));
        for range in journal.payload().durable.iter() {
            assert!(journal.payload().completed.contains_range(range));
        }
    }
}
