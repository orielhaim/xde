use std::{
    cell::RefCell,
    path::{Path, PathBuf},
    rc::Rc,
};

use crate::core::error::{DestinationError, Error, Result};
use compio::{
    buf::BufResult,
    fs::{File, OpenOptions},
    io::AsyncReadAtExt,
};
use compio_buf::{IntoInner, IoBuf};

use crate::storage::{
    destination::{
        CommitOutcome, DestinationCaps, DestinationHints, FlushLevel, RandomAccessDestination,
        WriteCompletion,
    },
    transfer::TransferChunk,
};

#[derive(Debug, Clone)]
pub struct FileDestinationOptions {
    /// Final path. We always write to `<final>.part` and rename at the end.
    pub final_path: PathBuf,
    /// Real preallocation via `fallocate`, not just `set_len`. `set_len` can
    /// leave you with a logically sparse file that has no storage behind it.
    pub preallocate: bool,
    /// Tell the kernel we are streaming, so it stops trying to be clever.
    pub advise_sequential: bool,
    pub max_parallel_writes: u16,
    pub max_inflight_bytes: u64,
}

impl FileDestinationOptions {
    pub fn new(final_path: impl Into<PathBuf>) -> Self {
        Self {
            final_path: final_path.into(),
            preallocate: true,
            advise_sequential: false,
            max_parallel_writes: 16,
            max_inflight_bytes: 256 * 1024 * 1024,
        }
    }
}

/// Positional writes straight into `artifact.part`. No per-connection temp files
/// and no merge pass at the end; the merge pass is pure I/O amplification when
/// the OS already gives you `pwrite`.
#[derive(Debug)]
pub struct FileDestination {
    /// compio's `File` is cheaply clonable and positional (no shared cursor),
    /// which is exactly what a segmented writer wants. `write_at` needs
    /// `&mut self`, so each call takes a fresh clone - the clone is an Arc bump,
    /// not a dup().
    file: File,
    part_path: PathBuf,
    final_path: PathBuf,
    opts: FileDestinationOptions,
    /// Highest offset+len we have written; used to size the final truncate when
    /// the server never told us the length.
    high_water: Rc<RefCell<u64>>,
    preallocated: Rc<RefCell<bool>>,
}

impl FileDestination {
    /// Open the `.part` for writing. The OS lock is owned by
    /// `FileDestinationCoordinator`, not here; `FileDestination` is a raw
    /// handle used by tests and the storage crate's own helpers.
    pub async fn open(opts: FileDestinationOptions) -> Result<Self> {
        let part_path = part_path_for(&opts.final_path);
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

        let mut open = OpenOptions::new();
        open.read(true).write(true).create(true);

        let file = open
            .open(&part_path)
            .await
            .map_err(|e| Error::Destination(DestinationError::Io(e)))?;

        let existing_len = file
            .metadata()
            .await
            .map_err(|e| Error::Destination(DestinationError::Io(e)))?
            .len();
        let this = Self {
            file,
            part_path,
            final_path: opts.final_path.clone(),
            opts,
            high_water: Rc::new(RefCell::new(existing_len)),
            preallocated: Rc::new(RefCell::new(false)),
        };

        if this.opts.advise_sequential {
            this.advise_sequential();
        }
        Ok(this)
    }

    pub fn part_path(&self) -> &Path {
        &self.part_path
    }
    pub fn final_path(&self) -> &Path {
        &self.final_path
    }

    /// Bytes already present in the `.part` from a previous run.
    pub async fn existing_len(&self) -> Result<u64> {
        Ok(self
            .file
            .metadata()
            .await
            .map_err(|e| Error::Destination(DestinationError::Io(e)))?
            .len())
    }

    #[cfg(unix)]
    fn advise_sequential(&self) {
        use std::os::fd::AsRawFd;
        // Best effort: a failed hint is not an error worth surfacing.
        let fd = unsafe { rustix::fd::BorrowedFd::borrow_raw(self.file.as_raw_fd()) };
        let _ = rustix::fs::fadvise(fd, 0, None, rustix::fs::Advice::Sequential);
    }
    #[cfg(not(unix))]
    fn advise_sequential(&self) {}

    #[cfg(unix)]
    fn fallocate(&self, size: u64) -> Result<bool> {
        use std::os::fd::AsRawFd;
        let fd = unsafe { rustix::fd::BorrowedFd::borrow_raw(self.file.as_raw_fd()) };
        match rustix::fs::fallocate(fd, rustix::fs::FallocateFlags::empty(), 0, size) {
            Ok(()) => Ok(true),
            // Not all filesystems support it (and macOS only has the posix form).
            Err(rustix::io::Errno::OPNOTSUPP) | Err(rustix::io::Errno::NOSYS) => {
                tracing::debug!(target: "xde::file", "fallocate unsupported, falling back to set_len");
                Ok(false)
            }
            Err(e) => Err(Error::Destination(
                crate::core::error::DestinationError::Io(e.into()),
            )),
        }
    }
    #[cfg(not(unix))]
    fn fallocate(&self, _size: u64) -> Result<bool> {
        Ok(false)
    }

    #[cfg(unix)]
    async fn fdatasync(&self) -> Result<()> {
        // compio's `sync_data` maps to fdatasync where available.
        self.file
            .sync_data()
            .await
            .map_err(|e| Error::Destination(crate::core::error::DestinationError::Io(e)))
    }
    #[cfg(not(unix))]
    async fn fdatasync(&self) -> Result<()> {
        self.file
            .sync_data()
            .await
            .map_err(|e| Error::Destination(crate::core::error::DestinationError::Io(e)))
    }

    async fn full_sync(&self) -> Result<()> {
        #[cfg(target_vendor = "apple")]
        {
            use std::os::fd::AsRawFd;
            // On macOS, fsync alone does not guarantee the data reached stable
            // storage. F_FULLFSYNC does.
            let fd = self.file.as_raw_fd();
            let owned = unsafe { rustix::fd::BorrowedFd::borrow_raw(fd) };
            if rustix::fs::fcntl_fullfsync(owned).is_ok() {
                return Ok(());
            }
        }
        self.file
            .sync_all()
            .await
            .map_err(|e| Error::Destination(crate::core::error::DestinationError::Io(e)))
    }

    /// After a successful transfer: sync the data, rename atomically, then sync
    /// the parent directory so the rename itself survives a crash.
    async fn finalize(&self, expected_len: Option<u64>) -> Result<()> {
        if let Some(len) = expected_len {
            // Preallocation may have left a tail of zeroes past the real end.
            self.file
                .set_len(len)
                .await
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
        } else {
            let hw = *self.high_water.borrow();
            self.file
                .set_len(hw)
                .await
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
        }
        self.full_sync().await?;
        let part = self.part_path.clone();
        let final_path = self.final_path.clone();
        compio::runtime::spawn_blocking(move || -> Result<()> {
            #[cfg(windows)]
            {
                // Windows MoveFileEx semantics: rename fails if target exists; use Replace semantics
                // Do a best-effort remove-then-rename; not atomic but correct.
                // A true atomic replace would use windows-sys MoveFileExW with MOVEFILE_REPLACE_EXISTING
                let _ = std::fs::remove_file(&final_path);
                std::fs::rename(&part, &final_path)
                    .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            }
            #[cfg(not(windows))]
            {
                std::fs::rename(&part, &final_path)
                    .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            }
            sync_parent_dir(&final_path)
        })
        .await
        .map_err(|_| Error::destination("artifact finalization task panicked"))??;
        Ok(())
    }
}

impl RandomAccessDestination for FileDestination {
    fn caps(&self) -> DestinationCaps {
        DestinationCaps::RANDOM_ACCESS
            | DestinationCaps::PARALLEL_WRITES
            | DestinationCaps::OUT_OF_ORDER
            | DestinationCaps::IDEMPOTENT_REWRITE
            | DestinationCaps::SPARSE
            | DestinationCaps::DURABLE_COMMIT
            | DestinationCaps::READ_BACK
            | DestinationCaps::PREALLOCATE
    }

    fn hints(&self) -> DestinationHints {
        DestinationHints {
            alignment: 1,
            max_operation_bytes: self.opts.max_inflight_bytes,
            max_inflight_bytes: self.opts.max_inflight_bytes,
            max_parallel_writes: self.opts.max_parallel_writes,
        }
    }

    async fn write_chunk(&self, chunk: TransferChunk) -> Result<WriteCompletion> {
        let payload = chunk.retained_payload();
        let (offset, data) = chunk.into_parts();
        let len = data.len() as u64;
        if len == 0 {
            return Ok(WriteCompletion {
                range: crate::core::ranges::ByteRange::new(offset, offset),
                payload,
            });
        }

        let mut file = self.file.clone();
        {
            use compio::io::AsyncWriteAtExt;
            let BufResult(res, _) = file.write_all_at(data, offset).await;
            res.map_err(|e| Error::Destination(DestinationError::Io(e)))?;
        }

        {
            let mut hw = self.high_water.borrow_mut();
            *hw = (*hw).max(offset + len);
        }
        Ok(WriteCompletion {
            range: crate::core::ranges::ByteRange::new(offset, offset + len),
            payload,
        })
    }

    async fn begin(&self, spec: crate::storage::destination::BeginArtifact) -> Result<()> {
        if spec.mode == crate::storage::destination::ArtifactMode::Fresh {
            self.file
                .set_len(0)
                .await
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
            *self.high_water.borrow_mut() = 0;
            *self.preallocated.borrow_mut() = false;
        }
        if let Some(size) = spec.expected_length {
            self.preallocate(size).await?;
        }
        Ok(())
    }

    async fn preallocate(&self, size: u64) -> Result<()> {
        if !self.opts.preallocate || *self.preallocated.borrow() {
            return Ok(());
        }
        if !self.fallocate(size)? {
            // set_len is the fallback, with the caveat that on most Unix
            // filesystems it only moves i_size and reserves nothing.
            self.file
                .set_len(size)
                .await
                .map_err(|e| Error::Destination(DestinationError::Io(e)))?;
        }
        *self.preallocated.borrow_mut() = true;
        tracing::debug!(target: "xde::file", size, path = ?self.part_path, "preallocated");
        Ok(())
    }

    async fn flush(&self, level: FlushLevel) -> Result<()> {
        match level {
            FlushLevel::Buffered => Ok(()),
            FlushLevel::Data => self.fdatasync().await,
            FlushLevel::Full => self.full_sync().await,
        }
    }

    async fn commit(&self, outcome: CommitOutcome) -> Result<()> {
        match outcome {
            CommitOutcome::Success { final_length } => self.finalize(Some(final_length)).await,
            CommitOutcome::Suspend => {
                // Leave `.part` exactly where it is; the journal describes it.
                self.fdatasync().await
            }
            CommitOutcome::Discard => {
                let path = self.part_path.clone();
                compio::runtime::spawn_blocking(move || match std::fs::remove_file(path) {
                    Ok(()) => Ok(()),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(error) => Err(Error::Destination(
                        crate::core::error::DestinationError::Io(error),
                    )),
                })
                .await
                .map_err(|_| Error::destination("partial removal task panicked"))?
            }
        }
    }

    async fn read_back(&self, offset: u64, len: usize) -> Result<Vec<u8>> {
        if len == 0 {
            return Ok(Vec::new());
        }
        let file = self.file.clone();
        let buf = Vec::with_capacity(len);
        let BufResult(res, slice) = file.read_exact_at(buf.slice(..len), offset).await;
        let mut buf = slice.into_inner();
        res.map_err(|e| Error::Destination(crate::core::error::DestinationError::Io(e)))?;
        // `read_exact_at` fills the slice; make the length visible.
        unsafe { buf.set_len(len) };
        Ok(buf)
    }
}

/// `foo.iso` -> `foo.iso.part`. Keeping the original extension in the name
/// means file managers still show a sensible icon mid-download.
pub fn part_path_for(final_path: &Path) -> PathBuf {
    let mut s = final_path.as_os_str().to_os_string();
    s.push(".part");
    PathBuf::from(s)
}

pub fn journal_path_for(final_path: &Path) -> PathBuf {
    let mut s = part_path_for(final_path).into_os_string();
    s.push(".state");
    PathBuf::from(s)
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
    // On Windows, ReplaceFile/MoveFileEx semantics differ and there is no
    // directory handle to sync in the same sense; the rename is metadata-journaled.
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
