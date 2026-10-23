//! Explicit discard destinations for the full-engine diagnostic ladder.
//! These modes never claim to publish a file or verify a payload digest.
use crate::core::{context::JobContext, metrics::DestinationStats};
use crate::http::fetch::ChunkSink;
use crate::storage::capacity::{CapacityCredits, CapacityGuard};
use crate::{CommitOutcome, DestinationCaps, DestinationHints, FlushLevel, Result, TransferChunk};
use std::{sync::Arc, time::Instant};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stage {
    Receive,
    Ownership,
    Capacity,
    Batching,
}

pub struct DiscardDestination(pub Stage);
impl crate::RandomAccessDestination for DiscardDestination {
    fn diagnostic_stage(&self) -> Option<Stage> {
        Some(self.0)
    }
    fn caps(&self) -> DestinationCaps {
        DestinationCaps::RANDOM_ACCESS
            | DestinationCaps::PARALLEL_WRITES
            | DestinationCaps::OUT_OF_ORDER
            | DestinationCaps::IDEMPOTENT_REWRITE
    }
    fn hints(&self) -> DestinationHints {
        DestinationHints {
            alignment: 1,
            ..Default::default()
        }
    }
    async fn write_chunk(&self, _chunk: TransferChunk) -> Result<crate::WriteCompletion> {
        Err(crate::Error::Config(
            "diagnostic sink was not installed".into(),
        ))
    }
    async fn preallocate(&self, _: u64) -> Result<()> {
        Ok(())
    }
    async fn flush(&self, _: FlushLevel) -> Result<()> {
        Ok(())
    }
    async fn commit(&self, _: CommitOutcome) -> Result<()> {
        Ok(())
    }
}

pub(crate) struct Sink {
    stage: Stage,
    credits: Arc<CapacityCredits>,
    context: JobContext,
    stats: DestinationStats,
}
impl Sink {
    pub(crate) fn new(stage: Stage, credits: Arc<CapacityCredits>, context: JobContext) -> Self {
        Self {
            stage,
            credits,
            context,
            stats: Default::default(),
        }
    }
    async fn operation(&mut self, bytes: usize) -> Result<()> {
        let start = Instant::now();
        let guard = self
            .context
            .run(CapacityGuard::acquire(&self.credits, bytes as u64))
            .await?;
        self.stats.capacity_acquire += start.elapsed();
        self.stats.capacity_wait += guard.waited();
        self.stats.operations += 1;
        self.stats.bytes += bytes as u64;
        drop(guard);
        Ok(())
    }
}
impl ChunkSink for Sink {
    fn discard_received(&mut self, bytes: usize) -> bool {
        if self.stage == Stage::Receive {
            self.stats.bytes += bytes as u64;
            true
        } else {
            false
        }
    }
    fn destination_stats(&self) -> DestinationStats {
        self.stats
    }
    async fn accept(&mut self, chunk: TransferChunk) -> Result<()> {
        if self.stage == Stage::Ownership {
            self.stats.bytes += chunk.len() as u64;
            return Ok(());
        }
        self.operation(chunk.len()).await
    }
    async fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
