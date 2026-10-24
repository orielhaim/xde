use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

/// How the fixture answers HTTP requests.
#[derive(Debug, Clone)]
pub struct FixtureSpec {
    pub size: u64,
    pub ranges: RangeBehavior,
    pub etag: String,
    pub latency: Duration,
    /// Strict per-connection send cap. `None` = unlimited.
    pub per_connection_bps: Option<u64>,
    /// Shared application-egress pacing. This is not network RTT or congestion emulation.
    pub egress: Option<SharedEgress>,
    pub keep_alive: bool,
    /// Advertise HTTP/3 on this UDP port (H1 probe path).
    pub alt_svc_h3: Option<u16>,
    pub redirect: Option<Redirect>,
    /// Return this status instead of 200/206 (e.g. 429).
    pub status: Option<u16>,
    pub retry_after: Option<Duration>,
    /// XOR payload bytes starting at this offset (corrupt mirror).
    pub corrupt_from: Option<u64>,
    /// Drop the TCP connection after this many successful requests.
    pub close_after_requests: Option<u32>,
    /// Truncate the Nth request body after this many bytes, then reset.
    pub truncate: Option<Truncate>,
    /// RST the Nth request before sending a complete body.
    pub reset_nth: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct Redirect {
    pub status: u16,
    pub location: String,
}

#[derive(Debug, Clone, Copy)]
pub struct Truncate {
    pub nth_request: u32,
    pub after_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default)]
pub enum RangeBehavior {
    #[default]
    Supported,
    Ignored,
    LostAfter(u32),
}

impl RangeBehavior {
    pub fn honors(self, request: u32) -> bool {
        match self {
            Self::Supported => true,
            Self::Ignored => false,
            Self::LostAfter(last) => request <= last,
        }
    }
}

impl Default for FixtureSpec {
    fn default() -> Self {
        Self {
            size: 1024 * 1024,
            ranges: RangeBehavior::Supported,
            etag: "\"xde-fixture\"".into(),
            latency: Duration::ZERO,
            per_connection_bps: None,
            egress: None,
            keep_alive: true,
            alt_svc_h3: None,
            redirect: None,
            status: None,
            retry_after: None,
            corrupt_from: None,
            close_after_requests: None,
            truncate: None,
            reset_nth: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SharedEgress(Arc<EgressClock>);

#[derive(Debug)]
struct EgressClock {
    bytes_per_second: std::num::NonZeroU64,
    next: std::sync::Mutex<Option<std::time::Instant>>,
}

impl SharedEgress {
    pub(crate) fn session(&self) -> EgressSession {
        EgressSession {
            budget: self.clone(),
            credit: 0,
        }
    }
    pub fn new(bytes_per_second: std::num::NonZeroU64) -> Self {
        Self(Arc::new(EgressClock {
            bytes_per_second,
            next: Default::default(),
        }))
    }
    fn reserve(&self, bytes: usize, now: std::time::Instant) -> std::time::Instant {
        let mut next = self.0.next.lock().unwrap();
        let at = next.unwrap_or(now).max(now)
            + Duration::from_secs_f64(bytes as f64 / self.0.bytes_per_second.get() as f64);
        *next = Some(at);
        at
    }
    pub(crate) fn delay(&self, bytes: usize) -> Duration {
        let now = std::time::Instant::now();
        self.reserve(bytes, now).saturating_duration_since(now)
    }
}

#[derive(Debug)]
pub(crate) struct EgressSession {
    budget: SharedEgress,
    credit: usize,
}
impl EgressSession {
    pub(crate) fn delay(&mut self, take: usize, remaining: usize) -> Duration {
        let delay = if self.credit < take {
            // Amortize timer granularity; bound each request's burst to 1 MiB.
            self.credit = remaining.min(1024 * 1024);
            self.budget.delay(self.credit)
        } else {
            Duration::ZERO
        };
        self.credit -= take;
        delay
    }
}

#[cfg(test)]
mod pacing_tests {
    use super::*;
    #[test]
    fn shared_reservations_serialize_bytes_and_idle_time_creates_no_burst_credit() {
        let now = std::time::Instant::now();
        let a = SharedEgress::new(1024.try_into().unwrap());
        let b = a.clone();
        assert_eq!(a.reserve(256, now), now + Duration::from_millis(250));
        assert_eq!(b.reserve(768, now), now + Duration::from_secs(1));
        assert_eq!(
            a.reserve(1024, now + Duration::from_secs(10)),
            now + Duration::from_secs(11)
        );
    }
}

impl FixtureSpec {
    pub fn small() -> Self {
        Self {
            size: 256 * 1024,
            ..Self::default()
        }
    }

    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }
}

#[derive(Debug, Default)]
pub struct FixtureStats {
    pub accepts: AtomicUsize,
    pub requests: AtomicUsize,
    pub bytes_sent: AtomicU64,
    pub h2_peak_streams_per_connection: AtomicUsize,
    pub h3_connections: AtomicUsize,
    pub h3_requests: AtomicUsize,
    pub h3_peak_streams_per_connection: AtomicUsize,
    pub h3_payload_bytes: AtomicU64,
    conn_bytes: std::sync::Mutex<Vec<u64>>,
}

impl FixtureStats {
    pub fn record_conn_bytes(&self, n: u64) {
        self.conn_bytes.lock().expect("stats").push(n);
    }

    pub fn accepts(&self) -> usize {
        self.accepts.load(Ordering::Acquire)
    }

    pub fn requests(&self) -> usize {
        self.requests.load(Ordering::Acquire)
    }

    pub fn bytes_sent(&self) -> u64 {
        self.bytes_sent.load(Ordering::Acquire)
    }

    /// Bytes written on each accepted connection, in accept order.
    pub fn conn_bytes(&self) -> Vec<u64> {
        self.conn_bytes.lock().expect("stats").clone()
    }
}

pub type SharedStats = Arc<FixtureStats>;
