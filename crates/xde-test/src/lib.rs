//! Shared HTTP fixtures for XDE tests and the system benchmark harness.
//!
//! One server implementation per protocol. Tests configure behavior through
//! [`FixtureSpec`]; they do not copy listeners around.

pub mod h1;
pub mod h2;
pub mod h3;
pub mod payload;
pub mod spec;

pub use h1::{H1Server, spawn_h1};
pub use h2::{H2Server, spawn_h2c};
pub use h3::{DualH3, spawn_h1h3};
pub use spec::{FixtureSpec, FixtureStats, Redirect, SharedStats, Truncate};

use std::sync::OnceLock;
use std::time::Duration;

use tempfile::TempDir;
use xde::{Engine, EngineLimits, Job, JobOutcome, TransferPolicy};

pub(crate) fn fixture_bind_addr() -> std::net::SocketAddr {
    #[cfg(target_os = "linux")]
    {
        let probe = std::net::UdpSocket::bind((std::net::Ipv4Addr::UNSPECIFIED, 0))
            .expect("bind fixture address probe");
        let _ = probe.connect((std::net::Ipv4Addr::new(1, 1, 1, 1), 80));
        if let Ok(addr) = probe.local_addr()
            && !addr.ip().is_unspecified()
        {
            return std::net::SocketAddr::new(addr.ip(), 0);
        }
    }
    std::net::SocketAddr::from(([127, 0, 0, 1], 0))
}

pub fn init_tracing() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
            )
            .with_test_writer()
            .try_init();
    });
}

pub fn test_engine() -> Engine {
    init_tracing();
    Engine::builder()
        .shards(1)
        .danger_accept_invalid_certs(true)
        .build()
        .expect("engine")
}

pub fn test_engine_h2() -> Engine {
    init_tracing();
    Engine::builder()
        .shards(1)
        .h2_prior_knowledge(true)
        .danger_accept_invalid_certs(true)
        .build()
        .expect("engine")
}

pub fn test_engine_limited(limits: EngineLimits) -> Engine {
    init_tracing();
    Engine::builder()
        .shards(1)
        .limits(limits)
        .danger_accept_invalid_certs(true)
        .memory_limit(limits.memory_bytes)
        .build()
        .expect("engine")
}

pub struct DownloadEnv {
    pub dir: TempDir,
    pub path: std::path::PathBuf,
}

impl DownloadEnv {
    pub fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("out.bin");
        Self { dir, path }
    }
}

impl Default for DownloadEnv {
    fn default() -> Self {
        Self::new()
    }
}

pub fn conservative_policy() -> TransferPolicy {
    TransferPolicy {
        initial_physical_connections: 1,
        initial_streams_per_connection: 1,
        ..TransferPolicy::default()
    }
}

pub fn wait_job(job: Job) -> xde::Result<JobOutcome> {
    job.wait_blocking_progressing(Duration::from_secs(8), Duration::from_secs(120))
}

pub fn assert_bytes_match(path: &std::path::Path, size: u64) {
    let got = std::fs::read(path).expect("read artifact");
    assert_eq!(got.len() as u64, size, "artifact length");
    let expect = payload::bytes(size as usize);
    assert_eq!(got, expect, "artifact bytes");
}
