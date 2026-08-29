//! Metric semantics shared by transport, controller and telemetry.
//!
//! One rule: a rate measured while waiting on the destination is NOT
//! endpoint capacity. Every duration class is captured separately so future
//! learning consumes only receive-active evidence.

use std::time::Duration;

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
