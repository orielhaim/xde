pub mod load;

use clap::ValueEnum;
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    io::Read,
    path::Path,
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use xde::{Event, Protocol};

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Proto {
    H1,
    H2,
    H3,
}
impl Proto {
    pub fn protocol(self) -> Protocol {
        match self {
            Self::H1 => Protocol::Http1_1,
            Self::H2 => Protocol::Http2,
            Self::H3 => Protocol::Http3,
        }
    }
    fn curl_version(self) -> &'static str {
        match self {
            Self::H1 => "1.1",
            Self::H2 => "2",
            Self::H3 => "3",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, ValueEnum, Serialize, PartialEq, Eq)]
pub enum Pipeline {
    A,
    B,
    C,
    D,
    #[default]
    E,
}
impl Pipeline {
    fn diagnostic(self) -> Option<xde::diagnostics::Stage> {
        use xde::diagnostics::Stage;
        match self {
            Self::A => Some(Stage::Receive),
            Self::B => Some(Stage::Ownership),
            Self::C => Some(Stage::Capacity),
            Self::D => Some(Stage::Batching),
            Self::E => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct Workload {
    pub protocol: Proto,
    pub bytes: u64,
    pub connections: u8,
    pub streams: u16,
    pub latency_ms: u64,
    pub per_connection_bps: Option<u64>,
}
impl Workload {
    pub fn validate(self) -> Result<Self, String> {
        if self.bytes == 0 || self.connections == 0 || self.streams == 0 {
            return Err("size, connections and streams must be positive".into());
        }
        if matches!(self.protocol, Proto::H1) && self.streams != 1 {
            return Err("HTTP/1.1 permits one active request per connection".into());
        }
        if self.per_connection_bps == Some(0) {
            return Err("bandwidth cap must be positive".into());
        }
        if !matches!(self.protocol, Proto::H1) && self.per_connection_bps.is_some() {
            return Err("only the H1 fixture implements per-connection bandwidth caps".into());
        }
        Ok(self)
    }
    pub fn policy(self) -> xde::TransferPolicy {
        xde::TransferPolicy {
            http_version: match self.protocol {
                Proto::H1 => xde::HttpVersionPolicy::Http1Only,
                Proto::H2 => xde::HttpVersionPolicy::Http2PriorKnowledge,
                Proto::H3 => xde::HttpVersionPolicy::Http3Only,
            },
            topology: xde::TopologyPolicy::Fixed(xde::Topology::new(
                self.connections,
                self.streams,
            )),
            transport: xde::TransportLimits {
                max_physical_connections: self.connections,
                max_streams_per_connection: self.streams,
                max_active_assignments: u16::from(self.connections).saturating_mul(self.streams),
            },
            ..Default::default()
        }
    }
}

#[derive(Default, Serialize)]
pub struct TransportEvidence {
    pub payload_protocols: Vec<String>,
    pub fixture_peak_streams_per_connection: Option<usize>,
    pub payload_connections: usize,
    pub peak_payload_connections: usize,
    pub peak_reserved_connections: Option<usize>,
    pub peak_assignments: usize,
    pub peak_streams_per_connection: usize,
    pub assignments_started: usize,
    pub assignments_finished: usize,
    pub receive_chunks: u64,
    pub sink_accepts: u64,
    pub destination_operations: u64,
    pub destination_bytes: u64,
    pub staging_copied_bytes: u64,
    pub response_validated_bytes: u64,
    pub payload_to_sink_finish_seconds: Option<f64>,
    pub missing_protocol_evidence: bool,
    pub memory_wait_seconds: f64,
    pub memory_acquisition_seconds: f64,
    pub capacity_acquisition_seconds: f64,
    pub capacity_wait_seconds: f64,
    pub batch_copy_seconds: f64,
    pub file_write_await_seconds: f64,
    pub requests: Vec<RequestEvidence>,
}

impl TransportEvidence {
    fn observe_active(&mut self, counts: impl Iterator<Item = usize>) {
        let (mut connections, mut assignments, mut streams) = (0, 0, 0);
        for count in counts {
            connections += usize::from(count > 0);
            assignments += count;
            streams = streams.max(count);
        }
        self.peak_payload_connections = self.peak_payload_connections.max(connections);
        self.peak_assignments = self.peak_assignments.max(assignments);
        self.peak_streams_per_connection = self.peak_streams_per_connection.max(streams);
    }
}

#[derive(Serialize)]
pub struct RequestEvidence {
    pub job: String,
    pub connection: String,
    pub payload_start_seconds: Option<f64>,
    pub payload_finish_seconds: Option<f64>,
    pub bytes: u64,
    pub ttfb_seconds: f64,
    pub response_seconds: f64,
    pub receive_seconds: f64,
    pub destination_seconds: f64,
    pub data_frames: u64,
    pub frame_p50: u32,
    pub frame_p90: u32,
    pub max_frame_gap_seconds: f64,
    pub next_pending_seconds: f64,
    pub io_reads_submitted: u64,
    pub io_reads_completed: u64,
}

#[derive(Serialize)]
pub struct Arm {
    pub bytes: u64,
    pub load: load::LoadObservation,
    pub diagnostic_only: bool,
    pub artifact_verified_bytes: Option<u64>,
    pub job_seconds: f64,
    pub job_mib_s: f64,
    pub payload_mib_s: Option<f64>,
    pub engine_build_seconds: Option<f64>,
    pub engine_shutdown_seconds: Option<f64>,
    pub curl_payload_approx_seconds: Option<f64>,
    pub transport: Option<TransportEvidence>,
}

pub struct Fixture {
    pub url: String,
    pub stats: std::sync::Arc<xde_test::FixtureStats>,
    shutdown: Option<Box<dyn FnOnce()>>,
}
impl Fixture {
    pub fn start(workload: Workload) -> Self {
        let spec = xde_test::FixtureSpec {
            size: workload.bytes,
            latency: Duration::from_millis(workload.latency_ms),
            per_connection_bps: workload.per_connection_bps,
            ..Default::default()
        };
        Self::with_spec(workload.protocol, spec)
    }
    pub fn with_spec(protocol: Proto, spec: xde_test::FixtureSpec) -> Self {
        let (url, stats, shutdown): (_, _, Box<dyn FnOnce()>) = match protocol {
            Proto::H1 => {
                let server = xde_test::spawn_h1(spec);
                (
                    server.url(),
                    server.stats.clone(),
                    Box::new(move || server.shutdown()),
                )
            }
            Proto::H2 => {
                let server = xde_test::spawn_h2c(spec);
                (
                    server.url(),
                    server.stats.clone(),
                    Box::new(move || server.shutdown()),
                )
            }
            Proto::H3 => {
                let server = xde_test::spawn_h1h3(spec);
                (
                    server.url(),
                    server.stats.clone(),
                    Box::new(move || server.shutdown()),
                )
            }
        };
        Self {
            url,
            stats,
            shutdown: Some(shutdown),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown();
        }
    }
}

pub fn verify_file(path: &Path, expected_length: u64) -> Result<(), String> {
    let mut file = std::fs::File::open(path).map_err(|e| e.to_string())?;
    if file.metadata().map_err(|e| e.to_string())?.len() != expected_length {
        return Err("published length mismatch".into());
    }
    let mut actual = vec![0; 1024 * 1024];
    let mut expected = vec![0; actual.len()];
    let mut offset = 0;
    loop {
        let len = file.read(&mut actual).map_err(|e| e.to_string())?;
        if len == 0 {
            break;
        }
        xde_test::payload::fill(&mut expected[..len], offset);
        if actual[..len] != expected[..len] {
            return Err(format!("published payload mismatch at block {offset}"));
        }
        offset += len as u64;
    }
    if offset != expected_length {
        return Err("published file changed during validation".into());
    }
    Ok(())
}

pub fn run_xde(url: &str, path: &Path, workload: Workload) -> Result<Arm, String> {
    run_xde_stage(url, path, workload, Pipeline::E)
}
pub fn run_xde_stage(
    url: &str,
    path: &Path,
    workload: Workload,
    pipeline: Pipeline,
) -> Result<Arm, String> {
    let build = Instant::now();
    let slots = u32::from(workload.connections) * u32::from(workload.streams);
    let engine = xde::Engine::builder()
        .shards(1)
        .limits(xde::EngineLimits {
            max_active_assignments: slots,
            max_physical_connections: u16::from(workload.connections),
            ..Default::default()
        })
        .h2_prior_knowledge(matches!(workload.protocol, Proto::H2))
        .danger_accept_invalid_certs(matches!(workload.protocol, Proto::H3))
        .build()
        .map_err(|e| e.to_string())?;
    let engine_build_seconds = build.elapsed().as_secs_f64();
    let events = engine.events();
    let monitor = load::Monitor::start();
    let start = Instant::now();
    let download = engine.download(url).policy(workload.policy());
    let download = if let Some(stage) = pipeline.diagnostic() {
        download.destination(std::sync::Arc::new(xde::diagnostics::DiscardDestination(
            stage,
        )))
    } else {
        download.to(path)
    };
    let result = download.start().and_then(|job| {
        job.wait_blocking_progressing(Duration::from_secs(15), Duration::from_secs(180))
    });
    let job_seconds = start.elapsed().as_secs_f64();
    let load = monitor.finish();
    let mut evidence = collect_events(events, start);
    evidence.peak_reserved_connections = Some(
        engine
            .snapshot()
            .map_err(|e| e.to_string())?
            .controller
            .peak_reserved_connections,
    );
    let shutdown = Instant::now();
    engine.shutdown().map_err(|e| e.to_string())?;
    let engine_shutdown_seconds = shutdown.elapsed().as_secs_f64();
    let result = result.map_err(|e| e.to_string())?;
    if result.bytes != workload.bytes {
        return Err("job byte count differs from workload".into());
    }
    if pipeline == Pipeline::E {
        verify_file(path, workload.bytes)?;
    }
    if evidence.missing_protocol_evidence
        || evidence.payload_protocols != vec![workload.protocol.protocol().as_str().to_owned()]
    {
        return Err("payload protocol could not be proved".into());
    }
    if evidence
        .peak_reserved_connections
        .is_some_and(|peak| peak > usize::from(workload.connections))
        || evidence.peak_payload_connections > usize::from(workload.connections)
        || evidence.peak_streams_per_connection > usize::from(workload.streams)
    {
        return Err("observed topology exceeded the requested ceiling".into());
    }
    if evidence.assignments_started != evidence.assignments_finished
        || evidence.response_validated_bytes != workload.bytes
    {
        return Err("incomplete or inconsistent transfer telemetry".into());
    }
    Ok(Arm {
        bytes: result.bytes,
        diagnostic_only: pipeline != Pipeline::E,
        artifact_verified_bytes: (pipeline == Pipeline::E).then_some(workload.bytes),
        load,
        job_seconds,
        job_mib_s: workload.bytes as f64 / job_seconds / 1048576.0,
        payload_mib_s: evidence
            .payload_to_sink_finish_seconds
            .filter(|seconds| *seconds > 0.0)
            .map(|seconds| workload.bytes as f64 / seconds / 1048576.0),
        engine_build_seconds: Some(engine_build_seconds),
        engine_shutdown_seconds: Some(engine_shutdown_seconds),
        curl_payload_approx_seconds: None,
        transport: Some(evidence),
    })
}

pub fn collect_events(events: xde::EventStream, reference: Instant) -> TransportEvidence {
    let mut evidence = TransportEvidence::default();
    let mut connections = HashMap::new();
    let mut active = HashMap::<_, usize>::new();
    let mut used = HashSet::new();
    let mut first_payload: Option<Instant> = None;
    let mut last_payload: Option<Instant> = None;
    while let Some(event) = events.try_next() {
        match event {
            Event::ConnectionOpened {
                connection,
                protocol,
                ..
            } => {
                connections.insert(connection, protocol);
            }
            Event::WorkerAdded { connection, .. } => {
                evidence.assignments_started += 1;
                used.insert(connection);
                match connections.get(&connection) {
                    Some(protocol) => {
                        let protocol = protocol.as_str().to_owned();
                        if !evidence.payload_protocols.contains(&protocol) {
                            evidence.payload_protocols.push(protocol);
                        }
                    }
                    None => evidence.missing_protocol_evidence = true,
                }
                *active.entry(connection).or_default() += 1;
                evidence.observe_active(active.values().copied());
            }
            Event::WorkerFinished {
                job,
                connection,
                range,
                sample,
                ..
            } => {
                evidence.memory_wait_seconds += sample.memory_wait.as_secs_f64();
                evidence.memory_acquisition_seconds += sample.memory_blocked.as_secs_f64();
                evidence.capacity_acquisition_seconds +=
                    sample.destination.capacity_acquire.as_secs_f64();
                evidence.capacity_wait_seconds += sample.destination.capacity_wait.as_secs_f64();
                evidence.batch_copy_seconds += sample.destination.batch_copy.as_secs_f64();
                evidence.file_write_await_seconds += sample.destination.write_await.as_secs_f64();
                evidence.requests.push(RequestEvidence {
                    job: format!("{job:?}"),
                    connection: format!("{connection:?}"),
                    payload_start_seconds: sample
                        .payload
                        .first_received
                        .map(|at| at.saturating_duration_since(reference).as_secs_f64()),
                    payload_finish_seconds: sample
                        .payload
                        .sink_finished
                        .map(|at| at.saturating_duration_since(reference).as_secs_f64()),
                    bytes: sample.bytes,
                    ttfb_seconds: sample.ttfb.as_secs_f64(),
                    response_seconds: sample.response_wall.as_secs_f64(),
                    receive_seconds: sample.receive_active.as_secs_f64(),
                    destination_seconds: sample.destination_blocked.as_secs_f64(),
                    data_frames: sample.data_frames,
                    frame_p50: sample.frame_p50,
                    frame_p90: sample.frame_p90,
                    max_frame_gap_seconds: sample.max_frame_gap.as_secs_f64(),
                    next_pending_seconds: sample.next_pending.as_secs_f64(),
                    io_reads_submitted: sample.io_reads_submitted,
                    io_reads_completed: sample.io_reads_completed,
                });
                evidence.assignments_finished += 1;
                evidence.response_validated_bytes += range.len();
                evidence.receive_chunks += sample.data_frames;
                evidence.sink_accepts += sample.dest_accepts;
                evidence.destination_operations += sample.destination.operations;
                evidence.destination_bytes += sample.destination.bytes;
                evidence.staging_copied_bytes += sample.destination.copied_bytes;
                if let Some(first) = sample.payload.first_received {
                    first_payload = Some(first_payload.map_or(first, |current| current.min(first)));
                }
                if let Some(last) = sample.payload.sink_finished {
                    last_payload = Some(last_payload.map_or(last, |current| current.max(last)));
                }
                match active.get_mut(&connection) {
                    Some(n) if *n > 0 => *n -= 1,
                    _ => evidence.missing_protocol_evidence = true,
                }
            }
            _ => {}
        }
    }
    evidence.payload_to_sink_finish_seconds = first_payload
        .zip(last_payload)
        .map(|(first, last)| last.saturating_duration_since(first).as_secs_f64());
    evidence.payload_connections = used.len();
    evidence.payload_protocols.sort();
    evidence
}

pub fn run_curl(url: &str, path: &Path, workload: Workload) -> Result<Arm, String> {
    if !matches!((workload.connections, workload.streams), (1, 1))
        || matches!(workload.protocol, Proto::H3)
    {
        return Err(
            "curl pairing currently requires H1/H2 with one connection and one stream".into(),
        );
    }
    let executable = std::env::var_os("XDE_CURL").unwrap_or_else(|| "curl".into());
    let flag = match workload.protocol {
        Proto::H1 => "--http1.1",
        Proto::H2 => "--http2-prior-knowledge",
        Proto::H3 => unreachable!(),
    };
    let monitor = load::Monitor::start();
    let mut child = Command::new(executable)
        .args([
            "--silent",
            "--show-error",
            "--fail",
            "--write-out",
            "%{json}",
            "--output",
        ])
        .arg(path)
        .arg(flag)
        .arg(url)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| e.to_string())?;
    if let Err(error) = monitor.register_child(&child) {
        let _ = child.kill();
        let _ = child.wait();
        return Err(error.to_string());
    }
    let status = match child.wait() {
        Ok(status) => status,
        Err(error) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(error.to_string());
        }
    };
    let load = monitor.finish();
    let mut output = String::new();
    child
        .stdout
        .take()
        .ok_or("missing curl stdout")?
        .read_to_string(&mut output)
        .map_err(|e| e.to_string())?;
    if !status.success() {
        return Err(format!("curl failed: {status}"));
    }
    let values: serde_json::Value = serde_json::from_str(&output).map_err(|e| e.to_string())?;
    if values["http_version"].as_str() != Some(workload.protocol.curl_version()) {
        return Err("curl negotiated the wrong protocol".into());
    }
    let seconds = values["time_total"]
        .as_f64()
        .filter(|n| *n > 0.0)
        .ok_or("invalid curl timing")?;
    let first = values["time_starttransfer"]
        .as_f64()
        .ok_or("missing curl TTFB")?;
    verify_file(path, workload.bytes)?;
    Ok(Arm {
        bytes: workload.bytes,
        diagnostic_only: false,
        artifact_verified_bytes: Some(workload.bytes),
        load,
        job_seconds: seconds,
        job_mib_s: workload.bytes as f64 / seconds / 1048576.0,
        payload_mib_s: (seconds > first)
            .then(|| workload.bytes as f64 / (seconds - first) / 1048576.0),
        engine_build_seconds: None,
        engine_shutdown_seconds: None,
        curl_payload_approx_seconds: Some((seconds - first).max(0.0)),
        transport: None,
    })
}

#[derive(Serialize)]
#[serde(tag = "status", content = "details", rename_all = "snake_case")]
pub enum Validity {
    Valid,
    Invalid(Vec<String>),
}
#[derive(Serialize)]
pub struct Trial {
    pub id: usize,
    pub warmup: bool,
    pub xde_first: bool,
    pub xde: Result<Arm, String>,
    pub curl: Option<Result<Arm, String>>,
    pub validity: Validity,
    pub paired_ratio: Option<f64>,
    pub measurement_notes: Vec<String>,
    pub pipeline: Pipeline,
}
pub fn trial(
    id: usize,
    warmup: bool,
    workload: Workload,
    paired: bool,
    pipeline: Pipeline,
) -> Trial {
    let fixture = Fixture::start(workload);
    let directory = tempfile::tempdir().expect("benchmark directory");
    let xde_first = id.is_multiple_of(2);
    let curl = || run_curl(&fixture.url, &directory.path().join("curl.bin"), workload);
    let (mut xde, curl) = if xde_first {
        let xde = run_xde_stage(
            &fixture.url,
            &directory.path().join("xde.bin"),
            workload,
            pipeline,
        );
        (xde, paired.then(curl))
    } else {
        let curl = paired.then(curl);
        (
            run_xde_stage(
                &fixture.url,
                &directory.path().join("xde.bin"),
                workload,
                pipeline,
            ),
            curl,
        )
    };
    let mut reasons = Vec::new();
    if let Ok(arm) = &mut xde
        && let Some(transport) = &mut arm.transport
    {
        let peak = match workload.protocol {
            Proto::H1 => 1,
            Proto::H2 => fixture
                .stats
                .h2_peak_streams_per_connection
                .load(std::sync::atomic::Ordering::Acquire),
            Proto::H3 => fixture
                .stats
                .h3_peak_streams_per_connection
                .load(std::sync::atomic::Ordering::Acquire),
        };
        transport.fixture_peak_streams_per_connection = Some(peak);
        if peak < usize::from(workload.streams)
            || transport.payload_connections < usize::from(workload.connections)
        {
            reasons.push("fixture/transport did not demonstrate requested payload topology".into());
        }
    }
    if let Err(e) = &xde {
        reasons.push(format!("XDE: {e}"));
    }
    if let Some(Err(e)) = &curl {
        reasons.push(format!("curl: {e}"));
    }
    let mut measurement_notes = Vec::new();
    for (name, arm) in std::iter::once(("XDE", &xde)).chain(curl.as_ref().map(|arm| ("curl", arm)))
    {
        if let Ok(arm) = arm {
            measurement_notes.extend(
                arm.load
                    .limitations()
                    .into_iter()
                    .map(|note| format!("{name}: {note}")),
            );
            if arm.job_seconds < 1.0 {
                measurement_notes.push(format!(
                    "{name}: sub-second transfer; timing and CPU sampling resolution are limited"
                ));
            }
        }
    }
    let validity = if reasons.is_empty() {
        Validity::Valid
    } else {
        Validity::Invalid(reasons)
    };
    let paired_ratio = if matches!(validity, Validity::Valid) && !warmup {
        xde.as_ref()
            .ok()
            .zip(curl.as_ref().and_then(|c| c.as_ref().ok()))
            .map(|(a, b)| a.job_mib_s / b.job_mib_s)
    } else {
        None
    };
    Trial {
        id,
        warmup,
        xde_first,
        xde,
        curl,
        validity,
        paired_ratio,
        measurement_notes,
        pipeline,
    }
}

#[derive(Serialize)]
pub struct Distribution {
    pub samples: usize,
    pub values: Vec<f64>,
    pub min: f64,
    pub p10: f64,
    pub median: f64,
    pub p90: f64,
    pub max: f64,
    pub mean: f64,
    pub stdev: Option<f64>,
}
pub fn distribution(values: Vec<f64>) -> Option<Distribution> {
    if values.is_empty() {
        return None;
    }
    let mut ordered = values.clone();
    ordered.sort_by(f64::total_cmp);
    let at = |p: f64| {
        let index = (ordered.len() - 1) as f64 * p;
        let lower = index.floor() as usize;
        let upper = index.ceil() as usize;
        ordered[lower] + (ordered[upper] - ordered[lower]) * index.fract()
    };
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    let stdev = (values.len() > 1).then(|| {
        (values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (values.len() - 1) as f64).sqrt()
    });
    Some(Distribution {
        samples: values.len(),
        min: ordered[0],
        p10: at(0.1),
        median: at(0.5),
        p90: at(0.9),
        max: ordered[ordered.len() - 1],
        mean,
        stdev,
        values,
    })
}
#[cfg(test)]
mod pipeline_tests {
    use super::*;
    #[test]
    fn replacements_do_not_inflate_concurrent_topology() {
        let mut evidence = TransportEvidence {
            payload_connections: 2,
            ..Default::default()
        };
        for counts in [[1, 0], [0, 0], [0, 1]] {
            evidence.observe_active(counts.into_iter());
        }
        assert_eq!(evidence.payload_connections, 2);
        assert_eq!(evidence.peak_payload_connections, 1);
        assert_eq!(evidence.peak_assignments, 1);
        evidence.observe_active([2, 1].into_iter());
        assert_eq!(evidence.peak_payload_connections, 2);
        assert_eq!(evidence.peak_assignments, 3);
        assert_eq!(evidence.peak_streams_per_connection, 2);
    }

    #[test]
    fn distribution_preserves_stalls_and_trial_order() {
        let result = distribution(vec![100.0, 1.0, 2.0]).unwrap();
        assert_eq!(result.values, [100.0, 1.0, 2.0]);
        assert_eq!(result.median, 2.0);
        assert_eq!(result.min, 1.0);
        assert_eq!(result.max, 100.0);
        assert!((result.p10 - 1.2).abs() < 1e-12);
        assert!((result.p90 - 80.4).abs() < 1e-12);
        assert_eq!(result.mean, 103.0 / 3.0);
        assert!(result.stdev.unwrap() > 50.0);
        assert!(distribution(vec![]).is_none());
        assert!(distribution(vec![1.0]).unwrap().stdev.is_none());
    }
    #[test]
    fn ladder_keeps_protocol_validation_and_changes_only_named_stages() {
        for (protocol, streams) in [
            (Proto::H1, 1),
            (Proto::H2, 1),
            (Proto::H3, 1),
            (Proto::H3, 4),
        ] {
            let workload = Workload {
                protocol,
                bytes: 4 * 1048576,
                connections: 1,
                streams,
                latency_ms: 5,
                per_connection_bps: None,
            };
            let fixture = Fixture::start(workload);
            let directory = tempfile::tempdir().unwrap();
            for pipeline in [
                Pipeline::A,
                Pipeline::B,
                Pipeline::C,
                Pipeline::D,
                Pipeline::E,
            ] {
                let path = directory.path().join(format!("{pipeline:?}.bin"));
                let arm = run_xde_stage(&fixture.url, &path, workload, pipeline).unwrap();
                let evidence = arm.transport.unwrap();
                assert_eq!(evidence.response_validated_bytes, workload.bytes);
                assert!(evidence.receive_chunks > 0);
                assert_eq!(path.exists(), pipeline == Pipeline::E);
                match pipeline {
                    Pipeline::A => {
                        assert_eq!(evidence.sink_accepts, 0);
                        assert_eq!(evidence.memory_wait_seconds, 0.0);
                    }
                    Pipeline::B => {
                        assert!(evidence.sink_accepts > 0);
                        assert_eq!(evidence.destination_operations, 0);
                    }
                    Pipeline::C => {
                        assert_eq!(evidence.destination_operations, evidence.sink_accepts);
                        assert_eq!(evidence.staging_copied_bytes, 0);
                    }
                    Pipeline::D | Pipeline::E => {
                        assert!(evidence.destination_operations > 0);
                        if matches!(protocol, Proto::H3) {
                            assert!(evidence.staging_copied_bytes > 0);
                            assert!(evidence.destination_operations < evidence.sink_accepts / 2);
                        }
                    }
                }
                if pipeline != Pipeline::E {
                    assert_eq!(evidence.file_write_await_seconds, 0.0);
                }
            }
        }
    }
}
