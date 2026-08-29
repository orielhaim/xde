//! System-benchmark helpers: fair curl pairing, histograms, JSON summaries.

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use hdrhistogram::Histogram;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Proto {
    H1,
    H2,
    H3,
}

impl Proto {
    pub fn curl_flag(self) -> &'static str {
        match self {
            Proto::H1 => "--http1.1",
            Proto::H2 => "--http2",
            Proto::H3 => "--http3",
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Proto::H1 => "h1",
            Proto::H2 => "h2",
            Proto::H3 => "h3",
        }
    }
}

#[derive(Debug, Serialize)]
pub struct Dist {
    pub median: f64,
    pub p10: f64,
    pub p90: f64,
    pub mean: f64,
    pub samples: u64,
}

pub fn summarize(samples_mib_s: &[f64]) -> Dist {
    let mut sorted = samples_mib_s.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = sorted.len();
    let pct = |p: f64| {
        if n == 0 {
            return 0.0;
        }
        let i = ((p / 100.0) * (n as f64 - 1.0)).round() as usize;
        sorted[i.min(n - 1)]
    };
    Dist {
        median: pct(50.0),
        p10: pct(10.0),
        p90: pct(90.0),
        mean: if n == 0 {
            0.0
        } else {
            sorted.iter().sum::<f64>() / n as f64
        },
        samples: n as u64,
    }
}

pub fn hdr_record(samples: &[f64]) -> Option<Histogram<u64>> {
    let mut h = Histogram::<u64>::new(3).ok()?;
    for s in samples {
        let scaled = (*s * 1000.0).max(1.0) as u64;
        let _ = h.record(scaled);
    }
    Some(h)
}

pub fn curl_path() -> PathBuf {
    if let Ok(p) = std::env::var("xde_CURL") {
        return PathBuf::from(p);
    }
    if let Ok(dir) = std::env::var("LOCALAPPDATA") {
        let winget = PathBuf::from(dir).join(r"Microsoft\WinGet\Links\curl.exe");
        if winget.exists() {
            return winget;
        }
    }
    PathBuf::from("curl")
}

pub fn curl_supports(proto: Proto) -> bool {
    let Ok(out) = Command::new(curl_path()).arg("--version").output() else {
        return false;
    };
    let text = String::from_utf8_lossy(&out.stdout);
    match proto {
        Proto::H1 => true,
        Proto::H2 => text.contains("HTTP2") || text.contains("http2"),
        Proto::H3 => {
            text.to_ascii_lowercase()
                .split_whitespace()
                .any(|t| t == "http3" || t == "h3")
                || text.contains("HTTP3")
        }
    }
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct XdePhases {
    /// `download().start()` until the job is admitted.
    pub admit_ms: f64,
    /// Admission until first `ConnectionOpened`.
    pub connect_ms: Option<f64>,
    /// Wall from job start until `SourceProbed` (includes connect).
    pub probe_ms: Option<f64>,
    /// HTTP TTFB reported on the probe itself.
    pub probe_ttfb_ms: Option<f64>,
    /// Admission until first `WorkerAdded` (range dispatched).
    pub first_byte_ms: Option<f64>,
    /// First `WorkerAdded` until last `WorkerFinished`.
    pub transfer_ms: Option<f64>,
    /// Last `WorkerFinished` until `Completed` (commit/rename).
    pub commit_ms: Option<f64>,
    /// `start()` return until `wait_blocking()` return. Throughput uses this.
    pub wait_ms: f64,
    /// `Engine::shutdown()` after the job is done. Never folded into throughput.
    pub shutdown_ms: f64,
    pub receive_ms: Option<f64>,
    pub dest_blocked_ms: Option<f64>,
    pub data_frames: Option<u64>,
    pub dest_accepts: Option<u64>,
    pub copy_count: Option<u64>,
    pub copied_bytes: Option<u64>,
    pub avg_frame: Option<u32>,
    pub frame_p50: Option<u32>,
    pub frame_p90: Option<u32>,
    pub next_pending_ms: Option<f64>,
    pub max_gap_ms: Option<f64>,
    pub send_ready_ms: Option<f64>,
    pub headers_ms: Option<f64>,
    pub io_sub: Option<u64>,
    pub io_cplt: Option<u64>,
    pub zero_read_ms: Option<f64>,
    pub max_zero_ms: Option<f64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct XdeRun {
    pub mib_s: f64,
    pub bytes: u64,
    pub phases: XdePhases,
}

struct EventClock {
    phases: XdePhases,
    first_worker: Option<Instant>,
    last_worker: Option<Instant>,
}

impl EventClock {
    fn apply(&mut self, ev: xde::Event, t0: Instant, now: Instant) {
        let since = now.saturating_duration_since(t0).as_secs_f64() * 1000.0;
        match ev {
            xde::Event::ConnectionOpened { .. } if self.phases.connect_ms.is_none() => {
                self.phases.connect_ms = Some(since);
            }
            xde::Event::SourceProbed { ttfb, .. } => {
                if self.phases.probe_ms.is_none() {
                    self.phases.probe_ms = Some(since);
                    self.phases.probe_ttfb_ms = Some(ttfb.as_secs_f64() * 1000.0);
                }
            }
            xde::Event::WorkerAdded { .. } => {
                if self.phases.first_byte_ms.is_none() {
                    self.phases.first_byte_ms = Some(since);
                }
                if self.first_worker.is_none() {
                    self.first_worker = Some(now);
                }
            }
            xde::Event::WorkerFinished {
                receive_active,
                destination_blocked,
                next_pending,
                max_frame_gap,
                send_ready,
                headers,
                data_frames,
                dest_accepts,
                copy_count,
                copied_bytes,
                avg_frame,
                frame_p50,
                frame_p90,
                io_reads_submitted,
                io_reads_completed,
                zero_read,
                max_zero_read,
                ..
            } => {
                self.last_worker = Some(now);
                self.phases.receive_ms = Some(receive_active.as_secs_f64() * 1000.0);
                self.phases.dest_blocked_ms = Some(destination_blocked.as_secs_f64() * 1000.0);
                self.phases.next_pending_ms = Some(next_pending.as_secs_f64() * 1000.0);
                self.phases.max_gap_ms = Some(max_frame_gap.as_secs_f64() * 1000.0);
                self.phases.send_ready_ms = Some(send_ready.as_secs_f64() * 1000.0);
                self.phases.headers_ms = Some(headers.as_secs_f64() * 1000.0);
                self.phases.data_frames = Some(data_frames);
                self.phases.dest_accepts = Some(dest_accepts);
                self.phases.copy_count = Some(copy_count);
                self.phases.copied_bytes = Some(copied_bytes);
                self.phases.avg_frame = Some(avg_frame);
                self.phases.frame_p50 = Some(frame_p50);
                self.phases.frame_p90 = Some(frame_p90);
                self.phases.io_sub = Some(io_reads_submitted);
                self.phases.io_cplt = Some(io_reads_completed);
                self.phases.zero_read_ms = Some(zero_read.as_secs_f64() * 1000.0);
                self.phases.max_zero_ms = Some(max_zero_read.as_secs_f64() * 1000.0);
            }
            xde::Event::Progress { done, .. }
                if done > 0 && self.phases.first_byte_ms.is_none() =>
            {
                self.phases.first_byte_ms = Some(since);
            }
            xde::Event::Completed { .. } => {
                if let (Some(a), Some(b)) = (self.first_worker, self.last_worker) {
                    self.phases.transfer_ms =
                        Some(b.saturating_duration_since(a).as_secs_f64() * 1000.0);
                }
                if let Some(b) = self.last_worker {
                    self.phases.commit_ms =
                        Some(now.saturating_duration_since(b).as_secs_f64() * 1000.0);
                }
            }
            _ => {}
        }
    }
}

pub fn run_curl_clean(url: &str, dest: &Path, proto: Proto) -> Result<f64, String> {
    if !curl_supports(proto) {
        return Err(format!("curl lacks {}", proto.name()));
    }
    let t0 = Instant::now();
    let mut cmd = Command::new(curl_path());
    cmd.args(["-sS", "-o", dest.to_str().unwrap(), proto.curl_flag(), url]);
    if proto == Proto::H2 {
        cmd.arg("--http2-prior-knowledge");
    }
    let status = cmd.status().map_err(|e| e.to_string())?;
    if !status.success() {
        return Err(format!("curl {status}"));
    }
    let secs = t0.elapsed().as_secs_f64().max(1e-9);
    let bytes = std::fs::metadata(dest).map_err(|e| e.to_string())?.len();
    Ok((bytes as f64 / secs) / (1024.0 * 1024.0))
}

pub fn run_xde(url: &str, dest: &Path, h2: bool, conns: u8) -> Result<f64, String> {
    Ok(run_xde_timed(url, dest, h2, conns)?.mib_s)
}

/// Throughput is `bytes / wait_blocking`. Shutdown is timed separately and
/// never included.
pub fn run_xde_timed(url: &str, dest: &Path, h2: bool, conns: u8) -> Result<XdeRun, String> {
    let mut b = xde::Engine::builder().shards(1);
    if h2 {
        b = b.h2_prior_knowledge(true);
    }
    let engine = b.build().map_err(|e| e.to_string())?;
    let policy = xde::TransferPolicy {
        initial_physical_connections: conns,
        transport: xde::TransportLimits {
            max_physical_connections: conns,
            max_streams_per_connection: 1,
            max_active_assignments: conns as u16,
        },
        initial_streams_per_connection: 1,
        ..Default::default()
    };
    let events = engine.events();
    let stop = Arc::new(AtomicBool::new(false));
    let t0 = Instant::now();
    let watcher = {
        let events = events.clone();
        let stop = stop.clone();
        thread::spawn(move || {
            let mut clock = EventClock {
                phases: XdePhases::default(),
                first_worker: None,
                last_worker: None,
            };
            while !stop.load(Ordering::Acquire) {
                if let Ok(ev) = events.try_recv_timeout(Duration::from_millis(2)) {
                    clock.apply(ev, t0, Instant::now());
                }
            }
            while let Some(ev) = events.try_next() {
                clock.apply(ev, t0, Instant::now());
            }
            clock.phases
        })
    };
    let job = engine
        .download(url)
        .to(dest)
        .policy(policy)
        .start()
        .map_err(|e| e.to_string())?;
    let admitted = Instant::now();
    let outcome = job.wait_blocking().map_err(|e| e.to_string())?;
    let waited = Instant::now();
    stop.store(true, Ordering::Release);
    let mut phases = watcher.join().unwrap_or_default();
    phases.admit_ms = admitted.saturating_duration_since(t0).as_secs_f64() * 1000.0;
    phases.wait_ms = waited.saturating_duration_since(admitted).as_secs_f64() * 1000.0;
    let shut0 = Instant::now();
    engine.shutdown().ok();
    phases.shutdown_ms = shut0.elapsed().as_secs_f64() * 1000.0;
    let secs = phases.wait_ms.max(1e-6) / 1000.0;
    Ok(XdeRun {
        mib_s: (outcome.bytes as f64 / secs) / (1024.0 * 1024.0),
        bytes: outcome.bytes,
        phases,
    })
}

pub fn format_phases(p: &XdePhases) -> String {
    format!(
        "admit={:.1}ms connect={} probe={} probe_ttfb={} dispatch={} transfer={} commit={} wait={:.1}ms shutdown={:.1}ms recv={} dest_blk={} nxt_pend={} max_gap={} ready={} hdr={} frames={} dest_acc={} copies={} copied={} avg_fr={} p50={} p90={} io_sub={} io_cplt={} zero={} max_zero={}",
        p.admit_ms,
        opt_ms(p.connect_ms),
        opt_ms(p.probe_ms),
        opt_ms(p.probe_ttfb_ms),
        opt_ms(p.first_byte_ms),
        opt_ms(p.transfer_ms),
        opt_ms(p.commit_ms),
        p.wait_ms,
        p.shutdown_ms,
        opt_ms(p.receive_ms),
        opt_ms(p.dest_blocked_ms),
        opt_ms(p.next_pending_ms),
        opt_ms(p.max_gap_ms),
        opt_ms(p.send_ready_ms),
        opt_ms(p.headers_ms),
        opt_u64(p.data_frames),
        opt_u64(p.dest_accepts),
        opt_u64(p.copy_count),
        opt_u64(p.copied_bytes),
        opt_u32(p.avg_frame),
        opt_u32(p.frame_p50),
        opt_u32(p.frame_p90),
        opt_u64(p.io_sub),
        opt_u64(p.io_cplt),
        opt_ms(p.zero_read_ms),
        opt_ms(p.max_zero_ms),
    )
}

fn opt_u64(v: Option<u64>) -> String {
    v.map(|x| x.to_string()).unwrap_or_else(|| "n/a".into())
}

fn opt_u32(v: Option<u32>) -> String {
    v.map(|x| x.to_string()).unwrap_or_else(|| "n/a".into())
}

fn opt_ms(v: Option<f64>) -> String {
    v.map(|x| format!("{x:.1}ms"))
        .unwrap_or_else(|| "n/a".into())
}

pub fn warmup_pause() {
    std::thread::sleep(Duration::from_millis(50));
}
