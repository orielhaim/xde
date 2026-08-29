use std::time::Duration;

use compact_str::CompactString;

use crate::core::{
    ids::{ConnectionId, JobId, OriginId, SourceId},
    ranges::ByteRange,
    units::Rate,
};

/// The public event stream. Deliberately about *decisions*, not about bytes:
/// nobody wants a span per 64KB.
#[derive(Debug, Clone)]
pub enum Event {
    Started {
        job: JobId,
        total: Option<u64>,
        resumed_bytes: u64,
    },
    SourceProbed {
        job: JobId,
        source: SourceId,
        supports_ranges: bool,
        total: Option<u64>,
        protocol: Protocol,
        ttfb: Duration,
    },
    /// One or more mirrors served content inconsistent with verified
    /// artifact data and were quarantined. Their unverified work was
    /// drained to healthy sources; verified ranges are untouched.
    SourceQuarantined {
        job: JobId,
        sources: Vec<SourceId>,
        reason: String,
    },
    ConnectionOpened {
        job: JobId,
        connection: ConnectionId,
        protocol: Protocol,
        /// Compio shard the physical connection lives on.
        shard: usize,
    },
    ConnectionReused {
        job: JobId,
        connection: ConnectionId,
        protocol: Protocol,
    },
    WorkerAdded {
        job: JobId,
        range: ByteRange,
        connection: ConnectionId,
    },
    WorkerFinished {
        job: JobId,
        range: ByteRange,
        rate: Rate,
        receive_active: Duration,
        destination_blocked: Duration,
        next_pending: Duration,
        max_frame_gap: Duration,
        send_ready: Duration,
        headers: Duration,
        data_frames: u64,
        dest_accepts: u64,
        copy_count: u64,
        copied_bytes: u64,
        avg_frame: u32,
        frame_p50: u32,
        frame_p90: u32,
        io_reads_submitted: u64,
        io_reads_completed: u64,
        zero_read: Duration,
        max_zero_read: Duration,
    },
    RangeSplit {
        job: JobId,
        original: ByteRange,
        kept: ByteRange,
        stolen: ByteRange,
        reason: SplitReason,
    },
    StragglerDetected {
        job: JobId,
        range: ByteRange,
        rate: Rate,
        fleet_median: Rate,
    },
    ConcurrencyChanged {
        job: JobId,
        origin: OriginId,
        connections: u8,
        streams: u16,
        reason: CompactString,
    },
    Progress {
        job: JobId,
        done: u64,
        total: Option<u64>,
        rate: Rate,
        eta: Option<Duration>,
    },
    Retrying {
        job: JobId,
        range: Option<ByteRange>,
        attempt: u32,
        delay: Duration,
        reason: CompactString,
    },
    CredentialRefreshRequired {
        job: JobId,
        source: SourceId,
        status: u16,
    },
    Checkpointed {
        job: JobId,
        bytes_done: u64,
        durable: bool,
    },
    IntegrityVerified {
        job: JobId,
        digest_hex: CompactString,
    },
    BottleneckChanged {
        job: Option<JobId>,
        description: CompactString,
        confidence: f32,
    },
    DestinationProfile {
        job: JobId,
        writes: u64,
        bytes: u64,
        total_latency: Duration,
        max_latency: Duration,
    },
    TransportProfile {
        job: JobId,
        physical_connections_opened: u16,
        peak_logical_streams: u16,
    },
    Completed {
        job: JobId,
        bytes: u64,
        duration: Duration,
        average_rate: Rate,
    },
    Failed {
        job: JobId,
        error: CompactString,
    },
    Cancelled {
        job: JobId,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    Http1_1,
    Http2,
    /// Reserved; not reachable in v0.1.
    Http3,
}

impl Protocol {
    pub fn as_str(&self) -> &'static str {
        match self {
            Protocol::Http1_1 => "http/1.1",
            Protocol::Http2 => "h2",
            Protocol::Http3 => "h3",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SplitReason {
    /// A worker freed up and stole work.
    WorkStealing,
    /// Straggler mitigation.
    Straggler,
    /// End-of-file aggression.
    TailCut,
}

/// Bounded diagnostic stream with one explicit consumer. Terminal job state is
/// delivered by `Job`, not through this telemetry path.
#[derive(Debug, Clone)]
pub struct EventSink {
    tx: flume::Sender<Event>,
}

/// Bounded diagnostic stream with one explicit consumer. Terminal job state is
/// delivered by `Job`, not through this telemetry path. Clones observe the
/// same stream (work-conserving competition), not independent broadcasts.
#[derive(Debug, Clone)]
pub struct EventStream {
    rx: flume::Receiver<Event>,
}
impl EventSink {
    pub fn channel() -> (EventSink, EventStream) {
        let (tx, rx) = flume::bounded(1024);
        (EventSink { tx }, EventStream { rx })
    }
    pub fn emit(&self, e: Event) {
        // A closed receiver is normal (nobody subscribed); never fail on it.
        let _ = self.tx.try_send(e);
    }
}

impl EventStream {
    pub fn try_next(&self) -> Option<Event> {
        self.rx.try_recv().ok()
    }
    pub fn try_recv_timeout(&self, timeout: Duration) -> Result<Event, flume::RecvTimeoutError> {
        self.rx.recv_timeout(timeout)
    }
    pub async fn next(&self) -> Option<Event> {
        self.rx.recv_async().await.ok()
    }
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.rx.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slotmap::SlotMap;

    #[test]
    fn diagnostic_channel_is_bounded() {
        let (sink, stream) = EventSink::channel();
        let mut ids = SlotMap::<JobId, ()>::with_key();
        let job = ids.insert(());
        for _ in 0..2048 {
            sink.emit(Event::Cancelled { job });
        }
        drop(sink);
        assert_eq!(stream.iter().take(2048).count(), 1024);
    }
}
