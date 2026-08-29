//! Coordinator + shard-local file lane.
//!
//! The coordinator is Send-safe control-plane state: it owns the canonical
//! paths, the OS lock file (held for the lifetime of the transfer), the
//! expected/final length, and the commit state. It performs no async I/O
//! itself except through `spawn_blocking` for the final rename.
//!
//! A `FileLane` is the shard-resident writer. It owns a `compio::fs::File`
//! opened on *its* shard and performs positional `write_all_at` into the
//! shared `.part`. The coordinator preallocates once before any lane writes;
//! lanes never preallocate.

use std::path::{Path, PathBuf};

use crate::core::error::{DestinationError, Error, Result};
use compio::fs::File as CompioFile;
use compio::{
    buf::BufResult,
    io::{AsyncReadAtExt, AsyncWriteAtExt},
};
use compio_buf::{IntoInner, IoBuf};

pub use crate::storage::file::journal_path_for;
use crate::storage::{
    destination::{DestinationCaps, DestinationHints, FlushLevel, WriteCompletion},
    file::{FileDestinationOptions, part_path_for},
    transfer::TransferChunk,
};

/// Send-safe control-plane owner of a file destination's lifecycle.
///
/// Holds the OS lock for the entire transfer (cross-process exclusion) and
/// the canonical paths. The actual `compio::fs::File` writers live on the
/// shards that use them; the coordinator never touches the `.part` bytes
/// directly except for the final truncate/rename/sync.
///
/// Preallocation runs here, via `spawn_blocking`, exactly once, before any
/// lane begins writing.
#[derive(Debug)]
pub struct FileDestinationCoordinator {
    final_path: PathBuf,
    part_path: PathBuf,
    lock_path: PathBuf,
    opts: FileDestinationOptions,
    /// The OS lock file. Held until `finalize` consumes the coordinator.
    /// `Option` so `finalize` can take it for the blocking rename task.
    lock_file: Option<std::fs::File>,
}

impl FileDestinationCoordinator {
    /// Open the coordinator: create parent dirs, acquire the OS lock on
    /// `<final>.part.lock`, and open the `.part` for writing. The lock is
    /// held for the lifetime of the coordinator.
    ///
    /// This runs the blocking setup (lock + preallocate) via `spawn_blocking`
    /// so it can be awaited from an async context without blocking the I/O
    /// shard. The returned coordinator is Send-safe and may be cloned by Arc.
    pub async fn open(opts: FileDestinationOptions) -> Result<Self> {
        let final_path = opts.final_path.clone();
        let part_path = part_path_for(&final_path);
        let lock_path = lock_path_for(&final_path);

        // Ensure parent dir exists.
        if let Some(parent) = part_path.parent()
            && !parent.as_os_str().is_empty()
        {
            let parent = parent.to_path_buf();
            compio::runtime::spawn_blocking(move || {
                std::fs::create_dir_all(parent)
                    .map_err(|e| Error::Destination(DestinationError::Io(e)))
            })
            .await
            .map_err(|_| Error::destination("directory creation task panicked"))??;
        }

        // Acquire the OS lock on the dedicated lock file. Held until finalize.
        let lock_path_clone = lock_path.clone();
        let lock_file = compio::runtime::spawn_blocking(move || -> Result<std::fs::File> {
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&lock_path_clone)
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            // try_lock: fail fast if another process holds it.
            file.try_lock().map_err(|e| match e {
                std::fs::TryLockError::WouldBlock => {
                    Error::Destination(DestinationError::LeaseConflict(
                        "destination is locked by another process".into(),
                    ))
                }
                std::fs::TryLockError::Error(io_err) => {
                    Error::Destination(DestinationError::Io(io_err))
                }
            })?;
            // The lock is held for the lifetime of the returned File. We do
            // NOT call unlock() here; that happens when the coordinator is
            // dropped/finalized.
            Ok(file)
        })
        .await
        .map_err(|_| Error::destination("lock task panicked"))??;

        Ok(Self {
            final_path,
            part_path,
            lock_path,
            opts,
            lock_file: Some(lock_file),
        })
    }

    pub fn final_path(&self) -> &Path {
        &self.final_path
    }
    pub fn part_path(&self) -> &Path {
        &self.part_path
    }
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }
    pub fn journal_path(&self) -> PathBuf {
        journal_path_for(&self.final_path)
    }

    /// Open the coordinator from a synchronous context (the control thread).
    /// All setup here is plain std I/O: parent dirs, lock file acquisition.
    /// No compio runtime is required or created.
    pub fn open_blocking(opts: FileDestinationOptions) -> Result<Self> {
        let final_path = opts.final_path.clone();
        let part_path = part_path_for(&final_path);
        let lock_path = lock_path_for(&final_path);

        if let Some(parent) = part_path.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
        }

        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
        file_try_lock(&lock_file)?;

        Ok(Self {
            final_path,
            part_path,
            lock_path,
            opts,
            lock_file: Some(lock_file),
        })
    }

    /// Preallocate the `.part` to `size` bytes. Runs via `spawn_blocking`
    /// because `rustix::fs::fallocate` is synchronous. Called exactly once
    /// before any lane begins writing.
    pub async fn preallocate(&self, size: u64) -> Result<()> {
        if !self.opts.preallocate {
            return Ok(());
        }
        let part_path = self.part_path.clone();
        compio::runtime::spawn_blocking(move || -> Result<()> {
            #[cfg(unix)]
            {
                use std::os::fd::AsRawFd;
                let file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&part_path)
                    .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
                let fd = unsafe { rustix::fd::BorrowedFd::borrow_raw(file.as_raw_fd()) };
                match rustix::fs::fallocate(fd, rustix::fs::FallocateFlags::empty(), 0, size) {
                    Ok(()) => return Ok(()),
                    Err(rustix::io::Errno::OPNOTSUPP) | Err(rustix::io::Errno::NOSYS) => {
                        tracing::debug!(
                            target: "xde::file",
                            "fallocate unsupported, falling back to set_len"
                        );
                    }
                    Err(e) => {
                        return Err(Error::Destination(DestinationError::Io(e.into())));
                    }
                }
            }
            #[cfg(not(unix))]
            {
                let _ = part_path;
            }
            // Fallback: create/truncate to size via std.
            let _ = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(false)
                .open(&part_path)
                .and_then(|f| f.set_len(size));
            Ok(())
        })
        .await
        .map_err(|_| Error::destination("preallocate task panicked"))?
    }

    /// Truncate the `.part` to 0 bytes. Used when beginning a Fresh artifact.
    pub async fn truncate_part(&self) -> Result<()> {
        let part_path = self.part_path.clone();
        compio::runtime::spawn_blocking(move || -> Result<()> {
            let f = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&part_path)
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            f.set_len(0)
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            Ok(())
        })
        .await
        .map_err(|_| Error::destination("truncate task panicked"))?
    }

    /// Final sync + optional integrity verification + atomic rename +
    /// parent-dir sync. Consumes the coordinator (and releases the OS lock
    /// by dropping the lock file).
    ///
    /// When `check` is present, the materialized `.part` is hashed by
    /// read-back *before* the rename, so a digest mismatch can never be
    /// reported as a successful commit: the job fails and the artifact is
    /// not published.
    pub async fn finalize(
        self,
        final_length: u64,
        check: Option<DigestCheck>,
        durability: crate::core::spec::Durability,
    ) -> Result<Option<crate::core::spec::Digest>> {
        let Self {
            final_path,
            part_path,
            lock_path,
            lock_file,
            ..
        } = self;
        // Drop the OS lock file handle before the blocking rename: on Windows
        // the rename might need the lock file to be closed.
        drop(lock_file);
        compio::runtime::spawn_blocking(move || -> Result<Option<crate::core::spec::Digest>> {
            // Truncate the .part to the exact final length.
            let f = std::fs::OpenOptions::new()
                .write(true)
                .read(true)
                .open(&part_path)
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            f.set_len(final_length)
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            if matches!(durability, crate::core::spec::Durability::CrashSafe) {
                f.sync_all()
                    .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            }

            // Integrity pass over the successfully written bytes.
            let digest = match check {
                None => None,
                Some(check) => {
                    let computed = hash_read_back(&f, final_length, check.kind)?;
                    if let Some(expected) = check.expected
                        && expected != computed.value
                    {
                        return Err(Error::Integrity(format!(
                            "{:?} digest mismatch: expected {}, got {}",
                            check.kind,
                            hex(&expected),
                            hex(&computed.value),
                        )));
                    }
                    Some(computed)
                }
            };
            drop(f);

            #[cfg(windows)]
            {
                use std::os::windows::ffi::OsStrExt;
                // MoveFileExW with MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH:
                // atomic replace of an existing destination without a racy
                // remove/rename pair, with write-through so the metadata hit
                // the disk before we report success.
                let to_wide = |p: &Path| -> Vec<u16> {
                    p.as_os_str()
                        .encode_wide()
                        .chain(std::iter::once(0))
                        .collect()
                };
                let from = to_wide(&part_path);
                let to = to_wide(&final_path);
                // SAFETY: both pointers are NUL-terminated wide strings for
                // the duration of the call; MoveFileExW does not retain them.
                let ok = unsafe {
                    windows_sys::Win32::Storage::FileSystem::MoveFileExW(
                        from.as_ptr(),
                        to.as_ptr(),
                        windows_sys::Win32::Storage::FileSystem::MOVEFILE_REPLACE_EXISTING
                            | if matches!(durability, crate::core::spec::Durability::CrashSafe) {
                                windows_sys::Win32::Storage::FileSystem::MOVEFILE_WRITE_THROUGH
                            } else {
                                0
                            },
                    )
                };
                if ok == 0 {
                    return Err(Error::Destination(DestinationError::Io(
                        std::io::Error::last_os_error(),
                    )));
                }
            }
            #[cfg(not(windows))]
            {
                std::fs::rename(&part_path, &final_path)
                    .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            }
            if matches!(durability, crate::core::spec::Durability::CrashSafe) {
                sync_parent_dir(&final_path)?;
            }
            // Remove the lock file now that the transfer is done.
            let _ = std::fs::remove_file(&lock_path);
            Ok(digest)
        })
        .await
        .map_err(|_| Error::destination("finalize task panicked"))?
    }

    /// Discard the `.part` and the lock file. Used on failed/aborted transfers.
    pub async fn discard(self) -> Result<()> {
        let part_path = self.part_path;
        let lock_path = self.lock_path;
        compio::runtime::spawn_blocking(move || -> Result<()> {
            for p in [part_path.as_path(), lock_path.as_path()] {
                match std::fs::remove_file(p) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => {
                        return Err(Error::Destination(DestinationError::Io(e)));
                    }
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| Error::destination("discard task panicked"))?
    }
}

/// Integrity proof the commit must satisfy before publishing the artifact.
pub use crate::core::spec::DigestCheck;

/// Read-back hash of the finalized artifact. Deliberately a final read, not
/// incremental streaming: the hash then provably describes the bytes that
/// were successfully written, with no out-of-order buffering in between.
fn hash_read_back(
    f: &std::fs::File,
    len: u64,
    kind: crate::core::spec::HashKind,
) -> Result<crate::core::spec::Digest> {
    use crate::core::spec::{Digest, HashKind};
    use std::io::{BufRead, BufReader, Read, Seek};
    let mut f = f
        .try_clone()
        .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
    f.seek(std::io::SeekFrom::Start(0))
        .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
    let mut limited = BufReader::with_capacity(1024 * 1024, f).take(len);
    let value: [u8; 32] = match kind {
        HashKind::Blake3 => {
            let mut h = blake3::Hasher::new();
            std::io::copy(&mut limited, &mut h)
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            *h.finalize().as_bytes()
        }
        HashKind::Sha256 => {
            use sha2::Digest as _;
            let mut h = sha2::Sha256::new();
            loop {
                match limited.fill_buf() {
                    Ok([]) => break,
                    Ok(buf) => {
                        let n = buf.len();
                        h.update(buf);
                        limited.consume(n);
                    }
                    Err(e) => return Err(Error::Destination(DestinationError::Io(e))),
                }
            }
            h.finalize().into()
        }
    };
    Ok(Digest { kind, value })
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// A shard-local writer into the shared `.part`.
///
/// Each lane opens its own `compio::fs::File` against the same `.part` path
/// on its own shard. Positional `write_all_at` keeps writes independent of any
/// shared cursor. The lane never owns the lock or the final rename; that is
/// the coordinator's job.
#[derive(Debug)]
pub struct FileLane {
    file: CompioFile,
    part_path: PathBuf,
}

impl FileLane {
    /// Open a `compio::fs::File` against the shared `.part` on the current
    /// shard. Must be called from the shard that will own this lane.
    pub async fn open(part_path: PathBuf) -> Result<Self> {
        let mut open = compio::fs::OpenOptions::new();
        open.read(true).write(true).create(true);
        let file = open
            .open(&part_path)
            .await
            .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
        Ok(Self { file, part_path })
    }

    pub fn caps(&self) -> DestinationCaps {
        DestinationCaps::RANDOM_ACCESS
            | DestinationCaps::PARALLEL_WRITES
            | DestinationCaps::OUT_OF_ORDER
            | DestinationCaps::IDEMPOTENT_REWRITE
            | DestinationCaps::SPARSE
            | DestinationCaps::DURABLE_COMMIT
            | DestinationCaps::READ_BACK
    }

    pub fn hints(&self) -> DestinationHints {
        DestinationHints {
            alignment: 1,
            max_operation_bytes: 256 * 1024 * 1024,
            max_inflight_bytes: 256 * 1024 * 1024,
            max_parallel_writes: 8,
        }
    }

    pub fn part_path(&self) -> &Path {
        &self.part_path
    }

    pub async fn write_chunk(&self, chunk: TransferChunk) -> Result<WriteCompletion> {
        let len = chunk.len() as u64;
        if len == 0 {
            let (off, payload) = chunk.into_parts();
            return Ok(WriteCompletion {
                range: crate::core::ranges::ByteRange::new(off, off),
                payload,
            });
        }
        let (offset, payload) = chunk.into_parts();
        let mut file = self.file.clone();
        let BufResult(res, payload) = file.write_all_at(payload, offset).await;
        res.map_err(|e| Error::Destination(DestinationError::Io(e)))?;
        Ok(WriteCompletion {
            range: crate::core::ranges::ByteRange::new(offset, offset + len),
            payload,
        })
    }

    pub async fn read_back(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let file = self.file.clone();
        let buf = Vec::with_capacity(len);
        let BufResult(res, slice) = file.read_exact_at(buf.slice(..len), offset).await;
        let mut buf = slice.into_inner();
        res.map_err(|e| Error::Destination(DestinationError::Io(e)))?;
        // `read_exact_at` fills the slice; make the length visible.
        unsafe { buf.set_len(len) };
        Ok(buf)
    }

    pub async fn flush(&self, level: FlushLevel) -> Result<()> {
        match level {
            FlushLevel::Buffered => Ok(()),
            FlushLevel::Data | FlushLevel::Full => self
                .file
                .sync_data()
                .await
                .map_err(|e| Error::Destination(DestinationError::Io(e))),
        }
    }
}

/// `<final>.part.lock` - a dedicated lock file separate from the `.part`
/// itself, so the `.part` can be truncated/reopened without losing the lock.
pub fn lock_path_for(final_path: &Path) -> PathBuf {
    let mut s = part_path_for(final_path).into_os_string();
    s.push(".lock");
    PathBuf::from(s)
}

/// Cross-process exclusive lock via std's `try_lock`. Held for the lifetime
/// of the returned guard's file handle; never unlocked early.
fn file_try_lock(file: &std::fs::File) -> Result<()> {
    match file.try_lock() {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => Err(Error::Destination(
            DestinationError::LeaseConflict("destination is locked by another process".into()),
        )),
        Err(std::fs::TryLockError::Error(io)) => Err(Error::Destination(DestinationError::Io(io))),
    }
}

/// A rename is only durable once the directory entry is durable.
pub fn sync_parent_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        let parent = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        let dir =
            std::fs::File::open(parent).map_err(|e| Error::Destination(DestinationError::Io(e)))?;
        dir.sync_all()
            .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
    }
    // On Windows, MoveFileEx/ReplaceFile semantics differ and there is no
    // directory handle to sync in the same sense; the rename is
    // metadata-journaled. Full Windows crash-safe semantics is a deferred
    // issue pending windows-sys integration.
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
