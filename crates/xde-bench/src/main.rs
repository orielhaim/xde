use clap::Parser;
use serde::Serialize;
use xde_bench::{Pipeline, Proto, Trial, Validity, Workload, distribution, trial};

#[derive(Parser)]
struct Args {
    #[arg(long, value_enum, default_value_t = Proto::H1)]
    protocol: Proto,
    #[arg(long, default_value_t = 256)]
    size_mib: u64,
    #[arg(long, default_value_t = 1)]
    connections: u8,
    #[arg(long, default_value_t = 1)]
    streams: u16,
    #[arg(long, default_value_t = 0)]
    latency_ms: u64,
    #[arg(long)]
    per_connection_mib_s: Option<u64>,
    #[arg(long, default_value_t = 5)]
    trials: usize,
    #[arg(long, default_value_t = 1)]
    warmup: usize,
    #[arg(long)]
    paired_curl: bool,
    #[arg(long, value_enum, default_value_t = Pipeline::E)]
    pipeline: Pipeline,
}
#[derive(Serialize)]
struct Report {
    schema_version: u32,
    validity_policy: &'static str,
    source_id: &'static str,
    compio_revision: &'static str,
    compiler: &'static str,
    build_profile: &'static str,
    build_target: &'static str,
    os: String,
    processor_identifier: Option<String>,
    logical_processors: usize,
    lock_blake3: String,
    workload: Workload,
    trials: Vec<Trial>,
    valid_job_mib_s: Option<xde_bench::Distribution>,
    valid_paired_ratios: Option<xde_bench::Distribution>,
}
fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_env_filter("warn")
        .with_writer(std::io::stderr)
        .init();
    let args = Args::parse();
    if args.trials == 0 {
        return Err("trials must be positive".into());
    }
    if args.paired_curl
        && (args.pipeline != Pipeline::E
            || args.connections != 1
            || args.streams != 1
            || matches!(args.protocol, Proto::H3))
    {
        return Err("curl pairing requires H1/H2 at 1 connection and 1 stream".into());
    }
    let workload = Workload {
        protocol: args.protocol,
        bytes: args.size_mib.checked_mul(1048576).ok_or("size overflow")?,
        connections: args.connections,
        streams: args.streams,
        latency_ms: args.latency_ms,
        per_connection_bps: args
            .per_connection_mib_s
            .map(|n| n.checked_mul(1048576).ok_or("bandwidth overflow"))
            .transpose()?,
    }
    .validate()?;
    let mut trials = Vec::new();
    for id in 0..args
        .warmup
        .checked_add(args.trials)
        .ok_or("trial count overflow")?
    {
        let record = trial(
            id,
            id < args.warmup,
            workload,
            args.paired_curl,
            args.pipeline,
        );
        eprintln!(
            "trial {id}: {}",
            match &record.validity {
                Validity::Valid => "valid",
                Validity::Invalid(_) => "invalid",
            }
        );
        trials.push(record);
    }
    let valid_job_mib_s = distribution(
        trials
            .iter()
            .filter(|r| !r.warmup && matches!(r.validity, Validity::Valid))
            .filter_map(|r| r.xde.as_ref().ok().map(|a| a.job_mib_s))
            .collect(),
    );
    let valid_paired_ratios = distribution(trials.iter().filter_map(|r| r.paired_ratio).collect());
    let compiler = env!("XDE_COMPILER");
    let os = if cfg!(windows) {
        command("cmd", &["/c", "ver"])
    } else {
        command("uname", &["-a"])
    };
    let report = Report {
        schema_version: 4,
        validity_policy: "workstation_v1",
        source_id: env!("XDE_SOURCE_ID"),
        compio_revision: env!("XDE_COMPIO_REV"),
        compiler,
        build_profile: env!("XDE_BUILD_PROFILE"),
        build_target: env!("XDE_BUILD_TARGET"),
        os,
        processor_identifier: std::env::var("PROCESSOR_IDENTIFIER").ok(),
        logical_processors: std::thread::available_parallelism().map_or(1, usize::from),
        lock_blake3: blake3::hash(include_bytes!("../../../Cargo.lock"))
            .to_hex()
            .to_string(),
        workload,
        trials,
        valid_job_mib_s,
        valid_paired_ratios,
    };
    println!(
        "{}",
        serde_json::to_string_pretty(&report).map_err(|e| e.to_string())?
    );
    Ok(())
}
fn command(program: &str, args: &[&str]) -> String {
    std::process::Command::new(program)
        .args(args)
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
        .unwrap_or_else(|e| e.to_string())
}
