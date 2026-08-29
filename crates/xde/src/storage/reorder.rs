use std::collections::BTreeMap;

use crate::core::error::Result;
use futures_util::lock::Mutex;

use crate::storage::{
    destination::{
        CommitOutcome, DestinationCaps, DestinationHints, FlushLevel, SequentialDestination,
    },
    transfer::TransferChunk,
};

/// Bridges out-of-order workers to an in-order sink. Holds a bounded amount of
/// data; when it is full the engine stops handing out credit, which is the same
/// backpressure path as everything else.
#[derive(Debug)]
pub struct SequentialAdapter<D> {
    inner: D,
    next_offset: u64,
    pending: BTreeMap<u64, TransferChunk>,
    buffered_bytes: u64,
    max_buffered: u64,
}

impl<D: SequentialDestination> SequentialAdapter<D> {
    pub fn new(inner: D, start_offset: u64, max_buffered: u64) -> Self {
        Self {
            inner,
            next_offset: start_offset,
            pending: BTreeMap::new(),
            buffered_bytes: 0,
            max_buffered,
        }
    }

    pub fn caps(&self) -> DestinationCaps {
        self.inner.caps()
    }
    pub fn hints(&self) -> DestinationHints {
        self.inner.hints()
    }
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
    pub fn has_room(&self, n: u64) -> bool {
        self.buffered_bytes + n <= self.max_buffered
    }

    /// Accept a chunk at an arbitrary offset; drains whatever became contiguous.
    /// Returns the leases whose bytes have already been pushed downstream.
    pub async fn accept(&mut self, mut chunk: TransferChunk) -> Result<()> {
        let offset = chunk.offset();
        if offset < self.next_offset {
            // Overlap re-delivery (retry or overlap prefix). Drop the part we
            // already pushed; if it is entirely behind us, drop the whole thing.
            let skip = (self.next_offset - offset) as usize;
            if skip >= chunk.len() {
                return Ok(());
            }
            chunk = chunk.trim_prefix(skip);
            self.push_now(chunk).await?;
        } else if offset == self.next_offset {
            self.push_now(chunk).await?;
        } else {
            if self.pending.contains_key(&offset) {
                return Err(crate::core::Error::destination(format!(
                    "duplicate sequential chunk at offset {offset}"
                )));
            }
            self.buffered_bytes += chunk.len() as u64;
            self.pending.insert(offset, chunk);
        }

        // Drain everything that is now contiguous.
        while let Some((&off, _)) = self.pending.iter().next() {
            if off != self.next_offset {
                break;
            }
            let chunk = self.pending.remove(&off).expect("just observed");
            self.buffered_bytes -= chunk.len() as u64;
            self.push_now(chunk).await?;
        }
        Ok(())
    }

    async fn push_now(&mut self, chunk: TransferChunk) -> Result<()> {
        let n = chunk.len() as u64;
        self.inner.push(chunk).await?;
        self.next_offset += n;
        Ok(())
    }

    pub async fn flush(&mut self, level: FlushLevel) -> Result<()> {
        self.inner.flush(level).await
    }

    pub async fn commit(&mut self, outcome: CommitOutcome) -> Result<()> {
        self.inner.commit(outcome).await
    }
}

/// Turns an ordered consumer into a bounded random-access destination. The
/// adapter retains budgeted chunks until their offsets become contiguous.
#[derive(Debug)]
pub struct ReorderingDestination<D> {
    adapter: Mutex<SequentialAdapter<D>>,
    max_buffered: u64,
    room: event_listener::Event,
}

impl<D: SequentialDestination> ReorderingDestination<D> {
    pub fn new(inner: D, max_buffered: u64) -> Self {
        Self {
            adapter: Mutex::new(SequentialAdapter::new(inner, 0, max_buffered.max(1))),
            max_buffered: max_buffered.max(1),
            room: event_listener::Event::new(),
        }
    }
}

impl<D: SequentialDestination + 'static> crate::storage::destination::RandomAccessDestination
    for ReorderingDestination<D>
{
    fn caps(&self) -> DestinationCaps {
        // Sequential underneath, but this adapter ACCEPTS out-of-order
        // offsets: it buffers non-contiguous chunks in a bounded BTreeMap and
        // drains contiguously. Segmented transfer is therefore safe; the
        // bound (max_buffered + hints.max_inflight_bytes) plus the engine's
        // destination-capacity credits keep memory bounded, and write_chunk
        // awaits `room` when the buffer is full - that IS the backpressure.
        DestinationCaps::RANDOM_ACCESS
            | DestinationCaps::PARALLEL_WRITES
            | DestinationCaps::OUT_OF_ORDER
            | DestinationCaps::CONTIGUOUS_SUBMISSION
    }

    fn hints(&self) -> DestinationHints {
        let mut hints = self
            .adapter
            .try_lock()
            .map_or_else(DestinationHints::default, |guard| guard.hints());
        hints.max_inflight_bytes = hints.max_inflight_bytes.min(self.max_buffered);
        hints
    }

    async fn write_chunk(&self, chunk: TransferChunk) -> Result<crate::storage::WriteCompletion> {
        let range = crate::core::ranges::ByteRange::new(
            chunk.offset(),
            chunk.offset() + chunk.len() as u64,
        );
        let payload = chunk.retained_payload();
        loop {
            let listener = self.room.listen();
            let mut adapter = self.adapter.lock().await;
            if chunk.offset() <= adapter.next_offset() || adapter.has_room(chunk.len() as u64) {
                adapter.accept(chunk).await?;
                self.room.notify(usize::MAX);
                return Ok(crate::storage::WriteCompletion { range, payload });
            }
            drop(adapter);
            listener.await;
        }
    }

    async fn begin(&self, _spec: crate::storage::destination::BeginArtifact) -> Result<()> {
        Ok(())
    }

    async fn preallocate(&self, _size: u64) -> Result<()> {
        Ok(())
    }

    async fn flush(&self, level: FlushLevel) -> Result<()> {
        self.adapter.lock().await.flush(level).await
    }

    async fn commit(&self, outcome: CommitOutcome) -> Result<()> {
        self.adapter.lock().await.commit(outcome).await
    }
}
