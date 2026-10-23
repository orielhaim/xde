use std::time::{Duration, Instant};
use xde::{
    Engine, EngineLimits, Event, Job, SchedulingPolicy, Topology, TopologyPolicy, TransferPolicy,
};
use xde_test::{DownloadEnv, FixtureSpec, assert_bytes_match, spawn_h1, wait_job};

fn fixed() -> TransferPolicy {
    TransferPolicy {
        topology: TopologyPolicy::Fixed(Topology::new(1, 1)),
        ..Default::default()
    }
}

fn until(job: &Job, predicate: impl Fn(&xde::JobSnapshot) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let snapshot = job.snapshot().expect("live job");
        if predicate(&snapshot) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "state did not arrive: {snapshot:?}"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
fn one_connection_rotates_between_jobs_without_losing_range_coverage() {
    let spec = FixtureSpec {
        size: 4 << 20,
        per_connection_bps: Some(4 << 20),
        ..Default::default()
    };
    let server = spawn_h1(spec.clone());
    let a = DownloadEnv::new();
    let b = DownloadEnv::new();
    let engine = Engine::builder()
        .limits(EngineLimits {
            max_physical_connections: 1,
            max_active_assignments: 1,
            fair_quantum_bytes: 256 << 10,
            ..Default::default()
        })
        .build()
        .unwrap();
    let events = engine.events();
    let ja = engine
        .download(server.url())
        .policy(fixed())
        .to(&a.path)
        .start()
        .unwrap();
    let jb = engine
        .download(server.url())
        .policy(fixed())
        .to(&b.path)
        .start()
        .unwrap();
    let ids = [ja.id(), jb.id()];
    let monitor = std::thread::spawn(move || {
        let mut bytes = [0u64; 2];
        let deadline = Instant::now() + Duration::from_secs(15);
        while bytes.contains(&0) && Instant::now() < deadline {
            if let Ok(Event::WorkerFinished { job, sample, .. }) =
                events.try_recv_timeout(Duration::from_millis(100))
                && let Some(i) = ids.iter().position(|id| *id == job)
            {
                bytes[i] += sample.bytes;
            }
        }
        assert!(
            bytes.iter().all(|n| *n > 0),
            "both jobs need service: {bytes:?}"
        );
    });
    assert_eq!(wait_job(ja).unwrap().bytes, spec.size);
    assert_eq!(wait_job(jb).unwrap().bytes, spec.size);
    monitor.join().unwrap();
    assert_bytes_match(&a.path, spec.size);
    assert_bytes_match(&b.path, spec.size);
    engine.shutdown().unwrap();
    server.shutdown();
}

#[test]
fn queue_cancellation_activates_high_priority_then_fifo_peers() {
    let spec = FixtureSpec {
        size: 2 << 20,
        per_connection_bps: Some(2 << 20),
        ..Default::default()
    };
    let server = spawn_h1(spec.clone());
    let paths = [DownloadEnv::new(), DownloadEnv::new(), DownloadEnv::new()];
    let engine = Engine::builder()
        .limits(EngineLimits {
            scheduling: SchedulingPolicy::Queue {
                max_active_jobs: 1.try_into().unwrap(),
            },
            ..Default::default()
        })
        .build()
        .unwrap();
    let a = engine
        .download(server.url())
        .policy(fixed())
        .to(&paths[0].path)
        .start()
        .unwrap();
    until(&a, |s| s.phase == xde::JobPhase::Transferring);
    let low = engine
        .download(server.url())
        .policy(fixed())
        .priority(xde::Priority::Low)
        .to(&paths[1].path)
        .start()
        .unwrap();
    let high = engine
        .download(server.url())
        .policy(fixed())
        .priority(xde::Priority::High)
        .to(&paths[2].path)
        .start()
        .unwrap();
    for job in [&low, &high] {
        let s = job.snapshot().unwrap();
        assert!(s.queued);
        assert_eq!(s.granted.connections, 0);
        assert_eq!(s.active_streams, 0);
    }
    a.cancel().unwrap();
    assert!(matches!(wait_job(a), Err(xde::Error::Cancelled)));
    until(&high, |s| !s.queued);
    assert!(low.snapshot().unwrap().queued);
    assert_eq!(wait_job(high).unwrap().bytes, spec.size);
    assert_eq!(wait_job(low).unwrap().bytes, spec.size);
    assert_bytes_match(&paths[1].path, spec.size);
    assert_bytes_match(&paths[2].path, spec.size);
    engine.shutdown().unwrap();
    server.shutdown();
}

#[test]
fn pause_drains_requests_releases_grants_and_resumes_to_publication() {
    let spec = FixtureSpec {
        size: 8 << 20,
        per_connection_bps: Some(4 << 20),
        ..Default::default()
    };
    let server = spawn_h1(spec.clone());
    let path = DownloadEnv::new();
    let engine = Engine::builder().build().unwrap();
    let job = engine
        .download(server.url())
        .policy(fixed())
        .to(&path.path)
        .start()
        .unwrap();
    until(&job, |s| s.consumed_bytes >= 64 << 10);
    job.pause().unwrap();
    until(&job, |s| s.phase == xde::JobPhase::Paused);
    let paused = job.snapshot().unwrap();
    assert_eq!(paused.granted, xde::ResourceGrant::default());
    assert_eq!(paused.active_streams, 0);
    assert!(!path.path.exists());
    job.resume().unwrap();
    assert_eq!(wait_job(job).unwrap().bytes, spec.size);
    assert_bytes_match(&path.path, spec.size);
    engine.shutdown().unwrap();
    server.shutdown();
}
