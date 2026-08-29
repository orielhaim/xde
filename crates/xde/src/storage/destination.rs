//! Destination trait family.
//!
//! The capability/hint vocabulary lives in `crate::core::sink` (pure data the
//! controller reasons about) and is re-exported here; this module defines the
//! I/O traits over it.

use std::{future::Future, pin::Pin};

use crate::core::error::Result;
pub use crate::core::sink::{
    ArtifactMode, BeginArtifact, CommitOutcome, DestinationCaps, DestinationHints, FlushLevel,
};

use crate::storage::transfer::{BudgetedPayload, TransferChunk};

#[derive(Debug)]
pub struct WriteCompletion {
    pub range: crate::core::ranges::ByteRange,
    pub payload: BudgetedPayload,
}

pub type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// `impl AsyncWrite` is the wrong abstraction for a segmented transfer: the
/// bytes arrive in the order `900MB, 0, 3GB, 500MB`.
pub trait RandomAccessDestination {
    fn caps(&self) -> DestinationCaps;
    fn hints(&self) -> DestinationHints;

    /// Prepare the artifact for transfer (e.g. truncate stale contents if Fresh, preallocate).
    fn begin(&self, spec: BeginArtifact) -> impl Future<Output = Result<()>> {
        async move {
            if let Some(size) = spec.expected_length {
                self.preallocate(size).await?;
            }
            Ok(())
        }
    }

    /// Write at an absolute offset. The destination owns the chunk until the
    /// returned future completes; dropping it releases global byte credits.
    fn write_chunk(&self, chunk: TransferChunk) -> impl Future<Output = Result<WriteCompletion>>;

    fn preallocate(&self, size: u64) -> impl Future<Output = Result<()>>;
    fn flush(&self, level: FlushLevel) -> impl Future<Output = Result<()>>;
    fn commit(&self, outcome: CommitOutcome) -> impl Future<Output = Result<()>>;

    /// Optional read-back used for resume overlap checks and final hashing.
    /// Destinations that advertise `READ_BACK` must override this method.
    fn read_back(&self, _offset: u64, _len: usize) -> impl Future<Output = Result<Vec<u8>>> {
        async {
            Err(crate::core::Error::destination(
                "destination does not support read-back",
            ))
        }
    }
}

/// For sinks that genuinely cannot seek (a pipe, a stdout, a chunked upload
/// that must go in order). The engine inserts a `ReorderBuffer` in front.
pub trait SequentialDestination {
    fn caps(&self) -> DestinationCaps;
    fn hints(&self) -> DestinationHints;

    fn push(&mut self, chunk: TransferChunk) -> impl Future<Output = Result<()>>;
    fn flush(&mut self, level: FlushLevel) -> impl Future<Output = Result<()>>;
    fn commit(&mut self, outcome: CommitOutcome) -> impl Future<Output = Result<()>>;
}

/// Object-safe façade. The engine stores `Arc<dyn DynDestination>` because a
/// job's destination is chosen at runtime, but every implementor writes the
/// clean `impl Trait` version above and gets this one for free via the blanket
/// impl below.
pub trait DynDestination {
    fn caps(&self) -> DestinationCaps;
    fn hints(&self) -> DestinationHints;

    fn begin_dyn(&self, spec: BeginArtifact) -> LocalBoxFuture<'_, Result<()>>;
    fn write_chunk_dyn(&self, chunk: TransferChunk) -> LocalBoxFuture<'_, Result<WriteCompletion>>;
    fn preallocate_dyn(&self, size: u64) -> LocalBoxFuture<'_, Result<()>>;
    fn flush_dyn(&self, level: FlushLevel) -> LocalBoxFuture<'_, Result<()>>;
    fn commit_dyn(&self, outcome: CommitOutcome) -> LocalBoxFuture<'_, Result<()>>;

    /// `Err(Error::Destination)` if `READ_BACK` is not in `caps()`.
    fn read_back_dyn(&self, offset: u64, len: usize) -> LocalBoxFuture<'_, Result<Vec<u8>>>;
}

impl<T> DynDestination for T
where
    T: RandomAccessDestination + 'static,
{
    fn caps(&self) -> DestinationCaps {
        RandomAccessDestination::caps(self)
    }
    fn hints(&self) -> DestinationHints {
        RandomAccessDestination::hints(self)
    }
    fn begin_dyn(&self, spec: BeginArtifact) -> LocalBoxFuture<'_, Result<()>> {
        Box::pin(self.begin(spec))
    }
    fn write_chunk_dyn(&self, chunk: TransferChunk) -> LocalBoxFuture<'_, Result<WriteCompletion>> {
        Box::pin(self.write_chunk(chunk))
    }
    fn preallocate_dyn(&self, size: u64) -> LocalBoxFuture<'_, Result<()>> {
        Box::pin(self.preallocate(size))
    }
    fn flush_dyn(&self, level: FlushLevel) -> LocalBoxFuture<'_, Result<()>> {
        Box::pin(self.flush(level))
    }
    fn commit_dyn(&self, outcome: CommitOutcome) -> LocalBoxFuture<'_, Result<()>> {
        Box::pin(RandomAccessDestination::commit(self, outcome))
    }
    fn read_back_dyn(&self, offset: u64, len: usize) -> LocalBoxFuture<'_, Result<Vec<u8>>> {
        Box::pin(RandomAccessDestination::read_back(self, offset, len))
    }
}

/// Convenience: does this destination let us run the segmented scheduler at all?
pub fn supports_segmentation(caps: DestinationCaps) -> bool {
    caps.contains(DestinationCaps::RANDOM_ACCESS) && caps.contains(DestinationCaps::OUT_OF_ORDER)
}
