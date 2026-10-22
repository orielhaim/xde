//! Uses the production engine and existing fixtures/load recorder for concurrent jobs.
use clap::{Parser, ValueEnum};
use serde::Serialize;
use std::time::Instant;
use xde::{Engine, EngineLimits, Priority, SchedulingPolicy, Topology, TopologyPolicy};
use xde_bench::{Fixture, Proto, Workload, collect_events, load, verify_file};

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
enum Mode {
    Fixed,
    Adaptive,
    Single,
}

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum, default_value_t = Proto::H1)]
    protocol: Proto,
    #[arg(long, value_enum, default_value_t = Mode::Adaptive)]
    mode: Mode,
    #[arg(long, default_value_t = 1024)]
    size_mib: u64,
    #[arg(long, default_value_t = 2)]
    jobs: usize,
    #[arg(long, default_value_t = 4)]
    connections: u8,
    #[arg(long, default_value_t = 1)]
    streams: u16,
    #[arg(long, default_value_t = 8)]
    global_connections: u16,
    #[arg(long, default_value_t = 32)]
    assignments: u32,
    #[arg(long, default_value_t = 8)]
    quantum_mib: u64,
    #[arg(long)]
    per_connection_mib_s: Option<u64>,
    #[arg(long)]
    queue: Option<std::num::NonZeroUsize>,
    #[arg(long)]
    shared_origin: bool,
    /// Give the first job Critical weight, others Normal.
    #[arg(long)]
    weighted: bool,
    /// Shared server application-egress cap across fixtures, not a network RTT emulator.
    #[arg(long)]
    shared_egress_mib_s: Option<std::num::NonZeroU64>,
    /// Independent per-source egress caps, in job order.
    #[arg(long, value_delimiter = ',')]
    source_mib_s: Vec<std::num::NonZeroU64>,
}

#[derive(Serialize)]
struct Report {
    schema_version: u32,
    validity_policy: &'static str,
    source_id: &'static str,
    compiler: &'static str,
    build_target: &'static str,
    build_profile: &'static str,
    os: &'static str,
    compio_revision: &'static str,
    lock_blake3: String,
    mode: Mode,
    jobs: usize,
    job_ids: Vec<String>,
    shared_origin: bool,
    weighted: bool,
    shared_egress_mib_s: Option<std::num::NonZeroU64>,
    source_mib_s: Vec<std::num::NonZeroU64>,
    queue: Option<usize>,
    global_connections: u16,
    global_assignments: u32,
    quantum_bytes: u64,
    workload: Workload,
    completion_seconds: Vec<f64>,
    completion_offsets_seconds: Vec<f64>,
    job_mib_s: Vec<f64>,
    makespan_seconds: f64,
    aggregate_mib_s: f64,
    published_bytes: u64,
    controller_observations: u64,
    allocation_passes: u64,
    planner_evaluations: u64,
    planner_global_limited: u64,
    planner_local_limited: u64,
    planner_source_limited: u64,
    planner_unsettled: u64,
    planner_topology_changes: u64,
    controller_seconds: f64,
    planner_bytes_per_job: usize,
    assignment_control_bytes: usize,
    engine_memory_used_after_jobs: u64,
    load: load::LoadObservation,
    transport: xde_bench::TransportEvidence,
}

fn main() -> Result<(), String> {
    let args = Args::parse();
    if args.jobs == 0 || args.jobs > 128 {
        return Err("jobs must be in 1..=128".into());
    }
    let fixture_count = if args.shared_origin { 1 } else { args.jobs };
    if (!args.source_mib_s.is_empty() && args.source_mib_s.len() != fixture_count)
        || (args.shared_egress_mib_s.is_some() && !args.source_mib_s.is_empty())
    {
        return Err("supply one source cap per fixture, or one shared egress cap".into());
    }
    let workload = Workload {
        protocol: args.protocol,
        bytes: args.size_mib.checked_mul(1 << 20).ok_or("size overflow")?,
        connections: args.connections,
        streams: args.streams,
        latency_ms: 0,
        per_connection_bps: args
            .per_connection_mib_s
            .map(|v| v.checked_mul(1 << 20).ok_or("rate overflow"))
            .transpose()?,
    }
    .validate()?;
    let egress = |rate: std::num::NonZeroU64| -> Result<_, String> {
        let bytes = rate
            .get()
            .checked_mul(1 << 20)
            .ok_or("egress rate overflow")?;
        Ok(xde_test::spec::SharedEgress::new(bytes.try_into().unwrap()))
    };
    let shared_egress = args.shared_egress_mib_s.map(egress).transpose()?;
    let fixtures: Vec<_> = (0..fixture_count)
        .map(|i| {
            Ok(Fixture::with_spec(
                workload.protocol,
                xde_test::FixtureSpec {
                    size: workload.bytes,
                    per_connection_bps: workload.per_connection_bps,
                    egress: match args.source_mib_s.get(i) {
                        Some(rate) => Some(egress(*rate)?),
                        None => shared_egress.clone(),
                    },
                    ..Default::default()
                },
            ))
        })
        .collect::<Result<_, String>>()?;
    let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
    let engine = Engine::builder()
        .shards(1)
        .h2_prior_knowledge(matches!(args.protocol, Proto::H2))
        .danger_accept_invalid_certs(matches!(args.protocol, Proto::H3))
        .limits(EngineLimits {
            scheduling: args
                .queue
                .map_or(SchedulingPolicy::FairShare, |max_active_jobs| {
                    SchedulingPolicy::Queue { max_active_jobs }
                }),
            max_physical_connections: args.global_connections,
            max_active_assignments: args.assignments,
            fair_quantum_bytes: args
                .quantum_mib
                .checked_mul(1 << 20)
                .ok_or("quantum overflow")?,
            ..Default::default()
        })
        .build()
        .map_err(|e| e.to_string())?;
    let events = engine.events();
    let monitor = load::Monitor::start();
    let started = Instant::now();
    let mut workers = Vec::new();
    let mut paths = Vec::new();
    let mut job_ids = Vec::new();
    for i in 0..args.jobs {
        let mut policy = workload.policy();
        policy.topology = match args.mode {
            Mode::Fixed => policy.topology,
            Mode::Single => TopologyPolicy::Fixed(Topology::new(1, 1)),
            Mode::Adaptive => TopologyPolicy::Adaptive,
        };
        let path = directory.path().join(format!("job-{i}.bin"));
        let at = Instant::now();
        let job = engine
            .download(&fixtures[if args.shared_origin { 0 } else { i }].url)
            .policy(policy)
            .priority(if args.weighted && i == 0 {
                Priority::Critical
            } else {
                Priority::Normal
            })
            .to(&path)
            .start()
            .map_err(|e| e.to_string())?;
        job_ids.push(format!("{:?}", job.id()));
        workers.push(std::thread::spawn(move || {
            let outcome = job.wait_blocking();
            (
                outcome,
                at.elapsed().as_secs_f64(),
                started.elapsed().as_secs_f64(),
            )
        }));
        paths.push(path);
    }
    let mut completion_seconds = Vec::new();
    let mut completion_offsets_seconds = Vec::new();
    for worker in workers {
        let (outcome, seconds, offset) = worker.join().map_err(|_| "job waiter panicked")?;
        if outcome.map_err(|e| e.to_string())?.bytes != workload.bytes {
            return Err("wrong completed byte count".into());
        }
        completion_seconds.push(seconds);
        completion_offsets_seconds.push(offset);
    }
    let makespan_seconds = started.elapsed().as_secs_f64();
    let cpu = monitor.finish();
    let snapshot = engine.snapshot().map_err(|e| e.to_string())?;
    engine.shutdown().map_err(|e| e.to_string())?;
    let mut evidence = collect_events(events, started);
    evidence.peak_reserved_connections = Some(snapshot.controller.peak_reserved_connections);
    if snapshot.controller.peak_reserved_connections > usize::from(args.global_connections) {
        return Err("physical reservation ceiling exceeded".into());
    }
    evidence.fixture_peak_streams_per_connection = Some(
        fixtures
            .iter()
            .map(|f| match args.protocol {
                Proto::H1 => 1,
                Proto::H2 => f
                    .stats
                    .h2_peak_streams_per_connection
                    .load(std::sync::atomic::Ordering::Relaxed),
                Proto::H3 => f
                    .stats
                    .h3_peak_streams_per_connection
                    .load(std::sync::atomic::Ordering::Relaxed),
            })
            .max()
            .unwrap_or(0),
    );
    let published_bytes = workload.bytes * args.jobs as u64;
    if evidence.missing_protocol_evidence
        || evidence.payload_protocols != [args.protocol.protocol().as_str()]
        || evidence.response_validated_bytes != published_bytes
        || evidence.assignments_started != evidence.assignments_finished
    {
        return Err("missing protocol or lifecycle evidence".into());
    }
    for path in paths {
        verify_file(&path, workload.bytes)?;
    }
    let report = Report {
        schema_version: 1,
        validity_policy: "workstation_v1",
        source_id: env!("XDE_SOURCE_ID"),
        compiler: env!("XDE_COMPILER"),
        build_target: env!("XDE_BUILD_TARGET"),
        build_profile: env!("XDE_BUILD_PROFILE"),
        os: std::env::consts::OS,
        compio_revision: env!("XDE_COMPIO_REV"),
        lock_blake3: blake3::hash(include_bytes!("../../../../Cargo.lock"))
            .to_hex()
            .to_string(),
        mode: args.mode,
        jobs: args.jobs,
        job_ids,
        shared_origin: args.shared_origin,
        weighted: args.weighted,
        shared_egress_mib_s: args.shared_egress_mib_s,
        source_mib_s: args.source_mib_s,
        queue: args.queue.map(usize::from),
        global_connections: args.global_connections,
        global_assignments: args.assignments,
        quantum_bytes: args.quantum_mib * (1 << 20),
        workload,
        job_mib_s: completion_seconds
            .iter()
            .map(|seconds| workload.bytes as f64 / seconds / 1048576.0)
            .collect(),
        completion_seconds,
        completion_offsets_seconds,
        makespan_seconds,
        aggregate_mib_s: published_bytes as f64 / makespan_seconds / 1048576.0,
        published_bytes,
        controller_observations: snapshot.controller.observations,
        allocation_passes: snapshot.controller.allocation_passes,
        planner_evaluations: snapshot.controller.planner_evaluations,
        planner_global_limited: snapshot.controller.planner_global_limited,
        planner_local_limited: snapshot.controller.planner_local_limited,
        planner_source_limited: snapshot.controller.planner_source_limited,
        planner_unsettled: snapshot.controller.planner_unsettled,
        planner_topology_changes: snapshot.controller.planner_topology_changes,
        controller_seconds: snapshot.controller.active_time.as_secs_f64(),
        planner_bytes_per_job: snapshot.controller.planner_bytes_per_job,
        assignment_control_bytes: snapshot.controller.assignment_control_bytes,
        engine_memory_used_after_jobs: snapshot.memory_used,
        load: cpu,
        transport: evidence,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?
    );
    Ok(())
}
