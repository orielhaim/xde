//! Protocol-path E2E against the shared fixture crate.

use std::time::Duration;

use rstest::rstest;
use xde::{Engine, Event, Protocol, TransferPolicy, TransportLimits};
use xde_test::{
    DownloadEnv, FixtureSpec, Truncate, assert_bytes_match, conservative_policy, spawn_h1,
    spawn_h1h3, spawn_h2c, test_engine, test_engine_h2, wait_job,
};

#[rstest]
#[case::h1(false)]
fn h1_keep_alive_reuses_one_socket(#[case] _unused: bool) {
    let spec = FixtureSpec::small().with_size(512 * 1024);
    let server = spawn_h1(spec.clone());
    let env = DownloadEnv::new();
    let engine = test_engine();
    let job = engine
        .download(server.url())
        .to(&env.path)
        .policy(conservative_policy())
        .start()
        .unwrap();
    let outcome = wait_job(job).unwrap();
    assert_eq!(outcome.bytes, spec.size);
    assert_bytes_match(&env.path, spec.size);
    assert_eq!(
        server.stats.accepts(),
        1,
        "keep-alive must reuse the socket"
    );
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn poisoned_h1_is_refilled() {
    let spec = FixtureSpec {
        size: 2 * 1024 * 1024,
        close_after_requests: Some(1),
        keep_alive: true,
        ..FixtureSpec::default()
    };
    let server = spawn_h1(spec.clone());
    let env = DownloadEnv::new();
    let engine = test_engine();
    let policy = TransferPolicy {
        initial_physical_connections: 1,
        transport: TransportLimits {
            max_physical_connections: 4,
            ..Default::default()
        },
        ..conservative_policy()
    };
    let job = engine
        .download(server.url())
        .to(&env.path)
        .policy(policy)
        .start()
        .unwrap();
    let outcome = wait_job(job).unwrap();
    assert_eq!(outcome.bytes, spec.size);
    assert_bytes_match(&env.path, spec.size);
    assert!(
        server.stats.accepts() >= 2,
        "poisoned H1 must be replaced, accepts={}",
        server.stats.accepts()
    );
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn h2_sibling_survives_truncated_stream() {
    let spec = FixtureSpec {
        size: 4 * 1024 * 1024,
        truncate: Some(Truncate {
            nth_request: 2,
            after_bytes: 1024,
        }),
        ..FixtureSpec::default()
    };
    let server = spawn_h2c(spec.clone());
    let env = DownloadEnv::new();
    let engine = test_engine_h2();
    let policy = TransferPolicy {
        initial_physical_connections: 1,
        initial_streams_per_connection: 2,
        transport: TransportLimits {
            max_physical_connections: 2,
            max_streams_per_connection: 8,
            max_active_assignments: 8,
        },
        ..Default::default()
    };
    let job = engine
        .download(server.url())
        .to(&env.path)
        .policy(policy)
        .start()
        .unwrap();
    let outcome = wait_job(job).expect("sibling streams must finish");
    assert_eq!(outcome.bytes, spec.size);
    assert_bytes_match(&env.path, spec.size);
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn h3_discovery_carries_bytes() {
    let spec = FixtureSpec::default().with_size(2 * 1024 * 1024);
    let server = spawn_h1h3(spec.clone());
    let env = DownloadEnv::new();
    let engine = Engine::builder()
        .shards(1)
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap();
    let events = engine.events();
    let job = engine
        .download(server.url())
        .to(&env.path)
        .policy(conservative_policy())
        .start()
        .unwrap();
    let handle = std::thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            match events.try_recv_timeout(Duration::from_millis(50)) {
                Ok(Event::ConnectionOpened {
                    protocol: Protocol::Http3,
                    ..
                }) => return true,
                Ok(_) => {}
                Err(_) => {}
            }
        }
        false
    });
    let outcome = wait_job(job).expect("h3 download");
    let saw_h3 = handle.join().unwrap();
    assert_eq!(outcome.bytes, spec.size);
    assert_bytes_match(&env.path, spec.size);
    assert!(saw_h3, "HTTP/3 connection must open via Alt-Svc");
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn redirect_is_followed_same_origin() {
    let target = spawn_h1(FixtureSpec::small());
    let bounce = spawn_h1(FixtureSpec {
        size: 0,
        redirect: Some(xde_test::Redirect {
            status: 302,
            location: target.url(),
        }),
        ..FixtureSpec::default()
    });
    let env = DownloadEnv::new();
    let engine = test_engine();
    let job = engine.download(bounce.url()).to(&env.path).start().unwrap();
    let outcome = wait_job(job).unwrap();
    assert_eq!(outcome.bytes, 256 * 1024);
    assert_bytes_match(&env.path, 256 * 1024);
    engine.shutdown().ok();
    bounce.shutdown();
    target.shutdown();
}

#[test]
fn retry_after_does_not_spin() {
    use std::sync::atomic::AtomicUsize;
    let spec = FixtureSpec {
        size: 64 * 1024,
        status: Some(429),
        retry_after: Some(Duration::from_secs(60)),
        ..FixtureSpec::default()
    };
    let server = spawn_h1(spec);
    let env = DownloadEnv::new();
    let engine = test_engine();
    let job = engine
        .download(server.url())
        .to(&env.path)
        .timeout(Duration::from_secs(2))
        .start()
        .unwrap();
    let _ = wait_job(job);
    let requests = server.stats.requests();
    engine.shutdown().ok();
    server.shutdown();
    assert!(
        requests < 8,
        "429 Retry-After must prevent early reclaim, requests={requests}"
    );
    let _ = AtomicUsize::new(0);
}
