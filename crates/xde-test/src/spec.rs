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
    pub etag: String,
    pub latency: Duration,
    /// Strict per-connection send cap. `None` = unlimited.
    pub per_connection_bps: Option<u64>,
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

impl Default for FixtureSpec {
    fn default() -> Self {
        Self {
            size: 1024 * 1024,
            etag: "\"xde-fixture\"".into(),
            latency: Duration::ZERO,
            per_connection_bps: None,
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
