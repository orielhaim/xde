//! XDE system benchmark: fair curl pairing, scaling, adaptive Auto.

use clap::{Parser, ValueEnum};
use serde::Serialize;
use xde_bench::{
    Dist, Proto, format_phases, run_curl_clean, run_xde_timed, summarize, warmup_pause,
};
use xde_test::{FixtureSpec, spawn_h1, spawn_h2c};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value_t = 32)]
    size_mib: u64,
    #[arg(long, default_value_t = 5)]
    trials: usize,
    #[arg(long, default_value_t = 1)]
    warmup: usize,
    #[arg(long, value_enum, default_value_t = Suite::All)]
    suite: Suite,
    #[arg(long)]
    json: bool,
}

#[derive(Clone, Copy, ValueEnum)]
enum Suite {
    All,
    Single,
    Scaling,
    Adaptive,
}

#[derive(Serialize)]
struct Report {
    single: Vec<Paired>,
    scaling: Vec<ScaleRow>,
}

#[derive(Serialize)]
struct Paired {
    proto: String,
    xde: Dist,
    curl: Option<Dist>,
}

#[derive(Serialize)]
struct ScaleRow {
    connections: u8,
    xde: Dist,
    accepts: usize,
}

fn main() {
    tracing_subscriber::fmt().with_env_filter("warn").init();
    let args = Args::parse();
    let size = args.size_mib * 1024 * 1024;
    let mut report = Report {
        single: Vec::new(),
        scaling: Vec::new(),
    };

    if matches!(args.suite, Suite::All | Suite::Single) {
        report
            .single
            .push(run_paired(Proto::H1, false, size, &args));
        report.single.push(run_paired(Proto::H2, true, size, &args));
        if xde_bench::curl_supports(Proto::H3) {
            eprintln!(
                "H3 curl comparison requires an HTTP/3 curl build and the DualH3 fixture; skipped in the default loopback suite."
            );
        }
    }

    if matches!(args.suite, Suite::All | Suite::Scaling) {
        let spec = FixtureSpec {
            size,
            per_connection_bps: Some(100 * 1024 * 1024),
            ..FixtureSpec::default()
        };
        let server = spawn_h1(spec);
        let url = server.url();
        let mut baseline = 0.0;
        for conns in [1u8, 2, 3, 4] {
            let mut samples = Vec::new();
            for i in 0..args.warmup + args.trials {
                warmup_pause();
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("out.bin");
                match run_xde_timed(&url, &path, false, conns) {
                    Ok(r) => {
                        if i >= args.warmup {
                            samples.push(r.mib_s);
                        }
                        if i == args.warmup {
                            eprintln!("  {}c {}", conns, format_phases(&r.phases));
                        }
                    }
                    Err(e) => eprintln!("xde {conns}c failed: {e}"),
                }
            }
            let dist = summarize(&samples);
            if conns == 1 {
                baseline = dist.median;
            }
            eprintln!(
                "scale {conns}c median {:.1} MiB/s ({:.2}×) accepts={} workers_with_bytes={}",
                dist.median,
                if baseline > 0.0 {
                    dist.median / baseline
                } else {
                    0.0
                },
                server.stats.accepts(),
                server.stats.conn_bytes().iter().filter(|b| **b > 0).count()
            );
            report.scaling.push(ScaleRow {
                connections: conns,
                xde: dist,
                accepts: server.stats.accepts(),
            });
        }
        server.shutdown();
    }

    if matches!(args.suite, Suite::All | Suite::Adaptive) {
        let spec = FixtureSpec {
            size,
            per_connection_bps: Some(4 * 1024 * 1024),
            ..FixtureSpec::default()
        };
        let server = spawn_h1(spec);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auto.bin");
        let engine = xde::Engine::builder().shards(1).build().unwrap();
        let t0 = std::time::Instant::now();
        let job = engine.download(server.url()).to(&path).start().unwrap();
        match job.wait_blocking() {
            Ok(o) => {
                let wait = t0.elapsed();
                let mib = (o.bytes as f64 / wait.as_secs_f64().max(1e-9)) / (1024.0 * 1024.0);
                let shut0 = std::time::Instant::now();
                engine.shutdown().ok();
                eprintln!(
                    "adaptive auto {:.1} MiB/s wait={:.1}ms shutdown={:.1}ms accepts={}",
                    mib,
                    wait.as_secs_f64() * 1000.0,
                    shut0.elapsed().as_secs_f64() * 1000.0,
                    server.stats.accepts()
                );
            }
            Err(e) => {
                engine.shutdown().ok();
                eprintln!("adaptive failed: {e}");
            }
        }
        server.shutdown();
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&report).unwrap());
    } else {
        eprintln!("single-flow:");
        for p in &report.single {
            eprintln!(
                "  {} xde median {:.1} p10 {:.1} p90 {:.1} curl {:?}",
                p.proto,
                p.xde.median,
                p.xde.p10,
                p.xde.p90,
                p.curl.as_ref().map(|c| c.median)
            );
        }
    }
}

fn run_paired(proto: Proto, h2: bool, size: u64, args: &Args) -> Paired {
    let spec = FixtureSpec {
        size,
        ..FixtureSpec::default()
    };
    let (url, shutdown): (String, Box<dyn FnOnce()>) = if h2 {
        let s = spawn_h2c(spec);
        let url = s.url();
        (url, Box::new(move || s.shutdown()))
    } else {
        let s = spawn_h1(spec);
        let url = s.url();
        (url, Box::new(move || s.shutdown()))
    };

    let mut xde_s = Vec::new();
    let mut curl_s = Vec::new();
    for i in 0..args.warmup + args.trials {
        warmup_pause();
        let dir = tempfile::tempdir().unwrap();
        let xde_path = dir.path().join("xde.bin");
        let curl_path = dir.path().join("curl.bin");
        let xde_first = i % 2 == 0;
        if xde_first {
            if let Ok(r) = run_xde_timed(&url, &xde_path, h2, 1)
                && i >= args.warmup
            {
                xde_s.push(r.mib_s);
                eprintln!(
                    "  {} trial {} {:.1} MiB/s {}",
                    proto.name(),
                    i - args.warmup,
                    r.mib_s,
                    format_phases(&r.phases)
                );
            }
            if let Ok(v) = run_curl_clean(&url, &curl_path, proto)
                && i >= args.warmup
            {
                curl_s.push(v);
            }
        } else {
            if let Ok(v) = run_curl_clean(&url, &curl_path, proto)
                && i >= args.warmup
            {
                curl_s.push(v);
            }
            if let Ok(r) = run_xde_timed(&url, &xde_path, h2, 1)
                && i >= args.warmup
            {
                xde_s.push(r.mib_s);
                eprintln!(
                    "  {} trial {} {:.1} MiB/s {}",
                    proto.name(),
                    i - args.warmup,
                    r.mib_s,
                    format_phases(&r.phases)
                );
            }
        }
    }
    shutdown();
    Paired {
        proto: proto.name().into(),
        xde: summarize(&xde_s),
        curl: if curl_s.is_empty() {
            None
        } else {
            Some(summarize(&curl_s))
        },
    }
}
