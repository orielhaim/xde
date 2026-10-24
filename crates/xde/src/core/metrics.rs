//! Metric semantics shared by transport, controller and telemetry.
//!
//! One rule: a rate measured while waiting on the destination is NOT
//! endpoint capacity. Every duration class is captured separately so future
//! learning consumes only receive-active evidence.

use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, Default)]
pub struct PayloadTiming {
    pub first_received: Option<Instant>,
    /// All payload has reached the sink's finish barrier. Excludes final
    /// artifact hashing and publication, but includes pending batch writes.
    pub sink_finished: Option<Instant>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DestinationStats {
    pub operations: u64,
    pub bytes: u64,
    pub copied_bytes: u64,
    pub capacity_wait: Duration,
    pub capacity_acquire: Duration,
    pub batch_copy: Duration,
    pub write_await: Duration,
    #[cfg(feature = "diagnostics")]
    pub write_poll: WritePollStats,
}

/// First-poll time includes synchronous submission work, not just kernel execution.
#[cfg(feature = "diagnostics")]
#[derive(Debug, Clone, Copy, Default)]
pub struct WritePollStats {
    pub first_ready: u64,
    pub first_pending: u64,
    pub first_poll: Duration,
    pub max_first_poll_ns: u64,
    pub max_await_ns: u64,
}

impl DestinationStats {
    pub(crate) async fn observe_write<F: std::future::Future>(&mut self, future: F) -> F::Output {
        let started = Instant::now();
        #[cfg(feature = "diagnostics")]
        let mut first = true;
        let future = std::pin::pin!(future);
        #[cfg(feature = "diagnostics")]
        let mut future = future;
        #[cfg(feature = "diagnostics")]
        let result = std::future::poll_fn(|cx| {
            let poll_started = first.then(Instant::now);
            let result = future.as_mut().poll(cx);
            if let Some(start) = poll_started {
                first = false;
                let elapsed = start.elapsed();
                self.write_poll.first_poll += elapsed;
                self.write_poll.max_first_poll_ns = self
                    .write_poll
                    .max_first_poll_ns
                    .max(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
                if result.is_ready() {
                    self.write_poll.first_ready += 1;
                } else {
                    self.write_poll.first_pending += 1;
                }
            }
            result
        })
        .await;
        #[cfg(not(feature = "diagnostics"))]
        let result = future.await;
        let elapsed = started.elapsed();
        self.write_await += elapsed;
        #[cfg(feature = "diagnostics")]
        {
            self.write_poll.max_await_ns = self
                .write_poll
                .max_await_ns
                .max(u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX));
        }
        result
    }
}

/// What one HTTP request's execution actually looked like, split by where
/// the time went.
#[derive(Debug, Clone, Copy, Default)]
pub struct TransferSample {
    /// Payload bytes delivered to the sink (excluding overlap prefix).
    pub bytes: u64,
    /// Request start -> response head received.
    pub ttfb: Duration,
    /// Full request-to-sink-finish wall clock.
    pub response_wall: Duration,
    /// Time spent receiving frames from the socket.
    pub receive_active: Duration,
    /// Time blocked acquiring transfer-memory credit.
    pub memory_blocked: Duration,
    pub memory_wait: Duration,
    /// Time blocked handing payload to the destination.
    pub destination_blocked: Duration,
    /// Time `next_data` spent in Poll::Pending (socket/driver, not dest).
    pub next_pending: Duration,
    /// Longest gap between successive DATA payloads.
    pub max_frame_gap: Duration,
    /// `SendRequest::ready` wait.
    pub send_ready: Duration,
    /// `send_request` until response heads.
    pub headers: Duration,
    pub data_frames: u64,
    /// `sink.accept` calls for this request.
    pub dest_accepts: u64,
    pub destination: DestinationStats,
    pub payload: PayloadTiming,
    /// XDE memcpy operations of payload bytes (0 on the normal H1/H2 path).
    pub copy_count: u64,
    /// Bytes those operations copied.
    pub copied_bytes: u64,
    pub frame_p50: u32,
    pub frame_p90: u32,
    pub avg_frame: u32,
    pub io_reads_submitted: u64,
    pub io_reads_completed: u64,
    /// Time `inflight_reads == 0` during this request's body.
    pub zero_read: Duration,
    pub max_zero_read: Duration,
}

impl TransferSample {
    /// Pure network/endpoint rate: bytes over receive-active time only.
    /// This is the only number endpoint learning may consume.
    pub fn receive_rate(&self) -> f64 {
        let secs = self.receive_active.as_secs_f64().max(1e-9);
        self.bytes as f64 / secs
    }

    /// End-to-end rate including every stall.
    pub fn effective_rate(&self) -> f64 {
        let secs = self.response_wall.as_secs_f64().max(1e-9);
        self.bytes as f64 / secs
    }

    /// Fraction of wall time spent stalled outside the network.
    pub fn stall_fraction(&self) -> f64 {
        if self.response_wall.is_zero() {
            return 0.0;
        }
        let stalled = self.memory_blocked + self.destination_blocked;
        (stalled.as_secs_f64() / self.response_wall.as_secs_f64()).clamp(0.0, 1.0)
    }
}

#[cfg(all(test, feature = "diagnostics"))]
mod tests {
    use super::*;

    #[test]
    fn file_write_poll_diagnostics_distinguish_ready_and_pending_operations() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let mut stats = DestinationStats::default();
            assert_eq!(stats.observe_write(std::future::ready(7)).await, 7);
            let mut pending = true;
            let value = stats
                .observe_write(std::future::poll_fn(|cx| {
                    if std::mem::take(&mut pending) {
                        cx.waker().wake_by_ref();
                        std::task::Poll::Pending
                    } else {
                        std::task::Poll::Ready(11)
                    }
                }))
                .await;
            assert_eq!(value, 11);
            assert_eq!(stats.write_poll.first_ready, 1);
            assert_eq!(stats.write_poll.first_pending, 1);
        });
    }
}
