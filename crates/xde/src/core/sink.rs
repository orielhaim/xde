//! Destination capability vocabulary.
//!
//! Pure data shared by the controller and the storage backends. No I/O here.

use bitflags::bitflags;

bitflags! {
    /// What the destination can actually do. This is what makes one engine serve
    /// a downloader, an updater, a proxy-transfer engine, an S3 relay, an
    /// installer or a streaming pipeline without forking the core.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct DestinationCaps: u32 {
        const RANDOM_ACCESS      = 1 << 0;
        const PARALLEL_WRITES    = 1 << 1;
        const OUT_OF_ORDER       = 1 << 2;
        const IDEMPOTENT_REWRITE = 1 << 3;
        const SPARSE             = 1 << 4;
        const DURABLE_COMMIT     = 1 << 5;
        /// Can read written bytes back; required for overlap verification on resume.
        const READ_BACK          = 1 << 6;
        /// Knows its own size ahead of time and wants preallocation.
        const PREALLOCATE        = 1 << 7;
        /// Adapter accepts random offsets but must receive them contiguously.
        const CONTIGUOUS_SUBMISSION = 1 << 8;
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DestinationHints {
    pub alignment: usize,
    pub max_operation_bytes: u64,
    pub max_inflight_bytes: u64,
    pub max_parallel_writes: u16,
}

impl Default for DestinationHints {
    fn default() -> Self {
        Self {
            alignment: 4096,
            max_operation_bytes: 256 * 1024 * 1024,
            max_inflight_bytes: 256 * 1024 * 1024,
            max_parallel_writes: 8,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushLevel {
    /// Hand it to the OS, no sync.
    Buffered,
    /// fdatasync: contents durable, metadata maybe not.
    Data,
    /// fsync (+ F_FULLFSYNC on macOS): everything durable.
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactMode {
    Fresh,
    Resume,
}

#[derive(Debug, Clone)]
pub struct BeginArtifact {
    pub mode: ArtifactMode,
    pub expected_length: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitOutcome {
    /// Finish the artifact: sync, atomic rename, sync parent metadata with exact final length.
    Success { final_length: u64 },
    /// Keep the partial state for a later resume.
    Suspend,
    /// Throw it away.
    Discard,
}

/// Convenience: does this destination let the segmented scheduler operate
/// (random offsets accepted in any arrival order)?
pub fn supports_segmentation(caps: DestinationCaps) -> bool {
    caps.contains(DestinationCaps::RANDOM_ACCESS) && caps.contains(DestinationCaps::OUT_OF_ORDER)
}
