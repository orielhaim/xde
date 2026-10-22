//! Adaptive Auto mode on the data plane.

use xde::{EngineLimits, Event, TransferPolicy, TransportLimits};
use xde_test::{
    DownloadEnv, FixtureSpec, assert_bytes_match, conservative_policy, spawn_h1, test_engine,
    test_engine_limited, wait_job,
};

#[test]
fn auto_does_not_explode_cold_start_requests() {
    let spec = FixtureSpec::small();
    let server = spawn_h1(spec.clone());
    let env = DownloadEnv::new();
    let engine = test_engine();
    let job = engine
        .download(server.url())
        .to(&env.path)
        .policy(conservative_policy())
        .start()
        .unwrap();
    let _ = wait_job(job).unwrap();
    let requests = server.stats.requests();
    assert!(
        requests <= 8,
        "cold start must not fan out, requests={requests}"
    );
    assert_eq!(server.stats.accepts(), 1);
    engine.shutdown().ok();
    server.shutdown();
}

#[rstest::rstest]
#[case(64, 2)]
#[case(128, 8)]
fn capped_origin_uses_more_than_one_connection_when_allowed(
    #[case] size_mib: u64,
    #[case] cap_mib_s: u64,
) {
    let spec = FixtureSpec {
        per_connection_bps: Some(cap_mib_s * 1024 * 1024),
        size: size_mib * 1024 * 1024,
        ..FixtureSpec::default()
    };
    let server = spawn_h1(spec.clone());
    let env = DownloadEnv::new();
    let engine = test_engine();
    let policy = TransferPolicy {
        transport: TransportLimits {
            max_physical_connections: 4,
            max_active_assignments: 8,
            ..Default::default()
        },
        ..Default::default()
    };
    let events = engine.events();
    let job = engine
        .download(server.url())
        .to(&env.path)
        .policy(policy)
        .start()
        .unwrap();
    let outcome = wait_job(job).unwrap();
    assert_eq!(outcome.bytes, spec.size);
    assert_bytes_match(&env.path, spec.size);
    let mut payload_connections = std::collections::HashSet::new();
    while let Ok(event) = events.try_recv_timeout(std::time::Duration::ZERO) {
        if let Event::WorkerFinished {
            connection, sample, ..
        } = event
            && sample.bytes > 0
        {
            payload_connections.insert(connection);
        }
    }
    assert!(
        payload_connections.len() >= 2,
        "bandwidth cap should induce useful scale-out, payload connections={payload_connections:?}"
    );
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn engine_assignment_ceiling_is_respected() {
    let spec = FixtureSpec::small();
    let server = spawn_h1(spec.clone());
    let env = DownloadEnv::new();
    let engine = test_engine_limited(EngineLimits {
        max_active_assignments: 1,
        max_physical_connections: 2,
        max_connections_per_origin: 2,
        max_jobs: 8,
        memory_bytes: 16 * 1024 * 1024,
        ..Default::default()
    });
    let job = engine.download(server.url()).to(&env.path).start().unwrap();
    let outcome = wait_job(job).unwrap();
    assert_eq!(outcome.bytes, spec.size);
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn two_jobs_share_one_engine() {
    let a_spec = FixtureSpec::small();
    let b_spec = FixtureSpec::small().with_size(128 * 1024);
    let sa = spawn_h1(a_spec.clone());
    let sb = spawn_h1(b_spec.clone());
    let ea = DownloadEnv::new();
    let eb = DownloadEnv::new();
    let engine = test_engine();
    let ja = engine.download(sa.url()).to(&ea.path).start().unwrap();
    let jb = engine.download(sb.url()).to(&eb.path).start().unwrap();
    assert_eq!(wait_job(ja).unwrap().bytes, a_spec.size);
    assert_eq!(wait_job(jb).unwrap().bytes, b_spec.size);
    engine.shutdown().ok();
    sa.shutdown();
    sb.shutdown();
}
