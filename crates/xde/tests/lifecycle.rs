//! Job lifecycle: resume, cancel, integrity, custom destinations.

use std::sync::Arc;

use parking_lot::Mutex;
use xde::{
    ArtifactMode, BeginArtifact, ByteRange, CommitOutcome, DestinationCaps, DestinationHints,
    FlushLevel, IntegritySpec, RandomAccessDestination, TransferChunk, TransferPolicy,
    TransportLimits, WriteCompletion,
};
use xde_test::{
    DownloadEnv, FixtureSpec, assert_bytes_match, conservative_policy, payload, spawn_h1,
    test_engine, wait_job,
};

struct MemorySink {
    buf: Mutex<Vec<u8>>,
    write_gate: Option<Gate>,
    commit_gate: Option<Gate>,
    fail_commit: bool,
    panic_write: bool,
    fail_begin: bool,
    begin_gate: Option<Gate>,
    began: std::sync::atomic::AtomicBool,
}

struct Gate {
    entered: flume::Sender<()>,
    release: flume::Receiver<()>,
}

struct GateControl {
    entered: flume::Receiver<()>,
    release: flume::Sender<()>,
}

impl GateControl {
    fn wait(&self) {
        self.entered
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
    }
}

impl Drop for GateControl {
    fn drop(&mut self) {
        let _ = self.release.try_send(());
    }
}

fn gate() -> (Gate, GateControl) {
    let (entered, entered_rx) = flume::bounded(1);
    let (release_tx, release) = flume::bounded(1);
    (
        Gate { entered, release },
        GateControl {
            entered: entered_rx,
            release: release_tx,
        },
    )
}

impl Gate {
    async fn wait(&self) {
        self.entered.send(()).unwrap();
        self.release.recv_async().await.unwrap();
    }
}

impl MemorySink {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            buf: Mutex::new(Vec::new()),
            write_gate: None,
            commit_gate: None,
            fail_commit: false,
            panic_write: false,
            fail_begin: false,
            begin_gate: None,
            began: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

impl RandomAccessDestination for MemorySink {
    fn caps(&self) -> DestinationCaps {
        DestinationCaps::RANDOM_ACCESS
            | DestinationCaps::PARALLEL_WRITES
            | DestinationCaps::OUT_OF_ORDER
            | DestinationCaps::IDEMPOTENT_REWRITE
            | DestinationCaps::READ_BACK
    }

    fn hints(&self) -> DestinationHints {
        DestinationHints {
            max_parallel_writes: 4,
            max_inflight_bytes: 8 * 1024 * 1024,
            ..DestinationHints::default()
        }
    }

    async fn write_chunk(&self, chunk: TransferChunk) -> xde::Result<WriteCompletion> {
        assert!(self.began.load(std::sync::atomic::Ordering::Acquire));
        assert!(!self.panic_write, "injected destination panic");
        if let Some(gate) = &self.write_gate {
            gate.wait().await;
        }
        let offset = chunk.offset() as usize;
        let data = chunk.as_slice();
        {
            let mut buf = self.buf.lock();
            if buf.len() < offset + data.len() {
                buf.resize(offset + data.len(), 0);
            }
            buf[offset..offset + data.len()].copy_from_slice(data);
        }
        Ok(WriteCompletion {
            range: ByteRange::new(chunk.offset(), chunk.offset() + chunk.len() as u64),
            payload: chunk.into_payload(),
        })
    }

    async fn preallocate(&self, size: u64) -> xde::Result<()> {
        self.buf.lock().resize(size as usize, 0);
        Ok(())
    }

    async fn flush(&self, _level: FlushLevel) -> xde::Result<()> {
        Ok(())
    }

    async fn commit(&self, _outcome: CommitOutcome) -> xde::Result<()> {
        if let Some(gate) = &self.commit_gate {
            gate.wait().await;
        }
        if self.fail_commit {
            return Err(xde::Error::destination("injected commit failure"));
        }
        Ok(())
    }

    async fn read_back(&self, offset: u64, len: usize) -> xde::Result<Vec<u8>> {
        let buf = self.buf.lock();
        let start = offset as usize;
        let end = (start + len).min(buf.len());
        Ok(buf[start..end].to_vec())
    }

    async fn begin(&self, spec: BeginArtifact) -> xde::Result<()> {
        if let Some(gate) = &self.begin_gate {
            gate.wait().await;
        }
        if self.fail_begin {
            return Err(xde::Error::destination("injected begin failure"));
        }
        self.began.store(true, std::sync::atomic::Ordering::Release);
        if spec.mode == ArtifactMode::Fresh {
            self.buf.lock().clear();
        }
        if let Some(size) = spec.expected_length {
            self.preallocate(size).await?;
        }
        Ok(())
    }
}

#[test]
fn commit_failure_never_reports_success() {
    let server = spawn_h1(FixtureSpec::small());
    let mut sink = MemorySink::new();
    Arc::get_mut(&mut sink).unwrap().fail_commit = true;
    let engine = test_engine();
    let job = engine
        .download(server.url())
        .destination(sink)
        .start()
        .unwrap();
    assert!(wait_job(job).is_err());
    engine.shutdown().unwrap();
    server.shutdown();
}

#[test]
fn cancellation_waits_for_a_submitted_destination_write() {
    let server = spawn_h1(FixtureSpec::small());
    let mut sink = MemorySink::new();
    let (write_gate, control) = gate();
    Arc::get_mut(&mut sink).unwrap().write_gate = Some(write_gate);
    let engine = test_engine();
    let job = engine
        .download(server.url())
        .destination(sink)
        .policy(conservative_policy())
        .start()
        .unwrap();
    control.wait();
    job.cancel().unwrap();
    assert_eq!(job.snapshot().unwrap().phase, xde::JobPhase::Cancelling);
    let (done, result) = flume::bounded(1);
    let waiter = std::thread::spawn(move || {
        done.send(job.wait_blocking()).unwrap();
    });
    assert!(
        result.try_recv().is_err(),
        "write still owns the assignment"
    );
    drop(control);
    assert!(matches!(
        result
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap(),
        Err(xde::Error::Cancelled)
    ));
    waiter.join().unwrap();
    engine.shutdown().unwrap();
    server.shutdown();
}

#[test]
fn pending_custom_commit_does_not_block_another_job_or_lose_publication() {
    let server = spawn_h1(FixtureSpec::small());
    let mut sink = MemorySink::new();
    let (commit_gate, control) = gate();
    Arc::get_mut(&mut sink).unwrap().commit_gate = Some(commit_gate);
    let engine = test_engine();
    let job = engine
        .download(server.url())
        .destination(sink)
        .policy(conservative_policy())
        .start()
        .unwrap();
    control.wait();
    job.cancel().unwrap();
    assert_eq!(job.snapshot().unwrap().phase, xde::JobPhase::Committing);
    let env = DownloadEnv::new();
    let other = engine
        .download(server.url())
        .to(&env.path)
        .policy(conservative_policy())
        .start()
        .unwrap();
    assert_eq!(wait_job(other).unwrap().bytes, FixtureSpec::small().size);
    assert_bytes_match(&env.path, FixtureSpec::small().size);
    drop(control);
    assert_eq!(wait_job(job).unwrap().bytes, FixtureSpec::small().size);
    engine.shutdown().unwrap();
    server.shutdown();
}

#[test]
fn custom_memory_destination_receives_the_artifact() {
    let spec = FixtureSpec::small();
    let server = spawn_h1(spec.clone());
    let sink = MemorySink::new();
    let engine = test_engine();
    let job = engine
        .download(server.url())
        .destination(sink.clone())
        .policy(conservative_policy())
        .start()
        .unwrap();
    let outcome = wait_job(job).unwrap();
    assert_eq!(outcome.bytes, spec.size);
    assert_eq!(*sink.buf.lock(), payload::bytes(spec.size as usize));
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn progress_callback_is_aggregate_monotonic_and_completes() {
    let spec = FixtureSpec::default().with_size(4 * 1024 * 1024);
    let server = spawn_h1(spec.clone());
    let env = DownloadEnv::new();
    let engine = test_engine();
    let updates = Arc::new(Mutex::new(Vec::<xde::DownloadProgress>::new()));
    let captured = Arc::clone(&updates);
    let job = engine
        .download(server.url())
        .to(&env.path)
        .policy(conservative_policy())
        .on_progress(move |progress| captured.lock().push(progress))
        .start()
        .unwrap();

    assert_eq!(wait_job(job).unwrap().bytes, spec.size);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    while updates
        .lock()
        .last()
        .is_none_or(|progress| progress.fraction != Some(1.0))
        && std::time::Instant::now() < deadline
    {
        std::thread::yield_now();
    }

    let updates = updates.lock();
    assert!(!updates.is_empty());
    assert!(
        updates
            .windows(2)
            .all(|pair| pair[0].downloaded_bytes <= pair[1].downloaded_bytes)
    );
    let final_update = updates.last().unwrap();
    assert_eq!(final_update.downloaded_bytes, spec.size);
    assert_eq!(final_update.total_bytes, Some(spec.size));
    assert_eq!(final_update.fraction, Some(1.0));

    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn integrity_rejects_a_corrupt_payload() {
    let spec = FixtureSpec {
        size: 128 * 1024,
        corrupt_from: Some(0),
        ..FixtureSpec::default()
    };
    let honest = payload::bytes(spec.size as usize);
    let digest = blake3::hash(&honest);
    let expected = xde::ExpectedDigest::Blake3(*digest.as_bytes());
    let server = spawn_h1(spec);
    let env = DownloadEnv::new();
    let engine = test_engine();
    let job = engine
        .download(server.url())
        .to(&env.path)
        .integrity(IntegritySpec::strict(expected))
        .start()
        .unwrap();
    assert!(wait_job(job).is_err());
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn cancel_does_not_leave_the_engine_wedged() {
    let spec = FixtureSpec::default().with_size(32 * 1024 * 1024);
    let server = spawn_h1(FixtureSpec {
        per_connection_bps: Some(256 * 1024),
        ..spec
    });
    let env = DownloadEnv::new();
    let engine = test_engine();
    let job = engine.download(server.url()).to(&env.path).start().unwrap();
    job.cancel().unwrap();
    let err = wait_job(job);
    assert!(err.is_err(), "cancel must surface");
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn resume_after_cancel_completes_the_file() {
    let spec = FixtureSpec::default().with_size(4 * 1024 * 1024);
    let server = spawn_h1(FixtureSpec {
        per_connection_bps: Some(2 * 1024 * 1024),
        ..spec.clone()
    });
    let env = DownloadEnv::new();
    let engine = test_engine();
    let events = engine.events();
    let job = engine
        .download(server.url())
        .to(&env.path)
        .policy(conservative_policy())
        .start()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        match events.try_recv_timeout(std::time::Duration::from_millis(50)) {
            Ok(xde::Event::Progress { done, .. }) if done > 0 => break,
            Ok(xde::Event::Checkpointed { bytes_done, .. }) if bytes_done > 0 => break,
            _ => {}
        }
    }
    job.cancel().unwrap();
    let _ = wait_job(job);
    engine.shutdown().ok();

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
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn two_jobs_same_origin_complete() {
    let spec = FixtureSpec::small().with_size(64 * 1024);
    let server = spawn_h1(spec.clone());
    let a = DownloadEnv::new();
    let b = DownloadEnv::new();
    let engine = test_engine();
    let policy = TransferPolicy {
        topology: xde::TopologyPolicy::Fixed(xde::Topology::new(2, 1)),
        transport: TransportLimits {
            max_physical_connections: 4,
            max_active_assignments: 4,
            ..Default::default()
        },
        ..conservative_policy()
    };
    let ja = engine
        .download(server.url())
        .to(&a.path)
        .policy(policy.clone())
        .start()
        .unwrap();
    let jb = engine
        .download(server.url())
        .to(&b.path)
        .policy(policy)
        .start()
        .unwrap();
    assert_eq!(wait_job(ja).unwrap().bytes, spec.size);
    assert_eq!(wait_job(jb).unwrap().bytes, spec.size);
    assert_bytes_match(&a.path, spec.size);
    assert_bytes_match(&b.path, spec.size);
    engine.shutdown().ok();
    server.shutdown();
}

#[test]
fn multi_source_quarantines_corrupt_mirror() {
    let spec = FixtureSpec::small();
    let good = spawn_h1(spec.clone());
    let bad = spawn_h1(FixtureSpec {
        corrupt_from: Some(0),
        ..spec.clone()
    });
    let env = DownloadEnv::new();
    let engine = test_engine();
    let digest = blake3::hash(&payload::bytes(spec.size as usize));
    let job = engine
        .download(good.url())
        .mirror(bad.url())
        .to(&env.path)
        .integrity(IntegritySpec::strict(xde::ExpectedDigest::Blake3(
            *digest.as_bytes(),
        )))
        .start()
        .unwrap();
    let outcome = wait_job(job).expect("failover to honest mirror");
    assert_eq!(outcome.bytes, spec.size);
    assert_bytes_match(&env.path, spec.size);
    engine.shutdown().ok();
    good.shutdown();
    bad.shutdown();
}

#[test]
fn destination_panic_fails_its_job_and_the_engine_remains_usable() {
    let server = spawn_h1(FixtureSpec::small());
    let mut sink = MemorySink::new();
    Arc::get_mut(&mut sink).unwrap().panic_write = true;
    let engine = test_engine();
    let job = engine
        .download(server.url())
        .destination(sink)
        .start()
        .unwrap();
    assert!(wait_job(job).is_err());
    let env = DownloadEnv::new();
    let job = engine.download(server.url()).to(&env.path).start().unwrap();
    assert_eq!(wait_job(job).unwrap().bytes, FixtureSpec::small().size);
    assert_bytes_match(&env.path, FixtureSpec::small().size);
    engine.shutdown().unwrap();
    server.shutdown();
}

#[test]
fn failed_destination_preparation_never_starts_a_request() {
    let server = spawn_h1(FixtureSpec::small());
    let engine = test_engine();
    let mut sink = MemorySink::new();
    Arc::get_mut(&mut sink).unwrap().fail_begin = true;
    let job = engine
        .download(server.url())
        .destination(sink.clone())
        .start()
        .unwrap();
    assert!(wait_job(job).is_err());
    assert!(sink.buf.lock().is_empty());
    assert_eq!(
        server
            .stats
            .requests
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    engine.shutdown().unwrap();
}

#[test]
fn cancellation_drains_destination_preparation_without_starting_transfer() {
    let server = spawn_h1(FixtureSpec::small());
    let engine = test_engine();
    let (gate, control) = gate();
    let mut sink = MemorySink::new();
    Arc::get_mut(&mut sink).unwrap().begin_gate = Some(gate);
    let job = engine
        .download(server.url())
        .destination(sink.clone())
        .start()
        .unwrap();
    control.wait();
    job.cancel().unwrap();
    assert_eq!(job.snapshot().unwrap().phase, xde::JobPhase::Cancelling);
    drop(control);
    assert!(matches!(wait_job(job), Err(xde::Error::Cancelled)));
    assert_eq!(
        server
            .stats
            .requests
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    engine.shutdown().unwrap();
}

#[test]
fn one_custom_destination_has_one_active_lease() {
    let server = spawn_h1(FixtureSpec::small());
    let engine = test_engine();
    let (gate, control) = gate();
    let mut sink = MemorySink::new();
    Arc::get_mut(&mut sink).unwrap().begin_gate = Some(gate);
    let first = engine
        .download(server.url())
        .destination(sink.clone())
        .start()
        .unwrap();
    control.wait();
    let second = engine
        .download(server.url())
        .destination(sink.clone())
        .start();
    let rejected = second.is_err();
    first.cancel().unwrap();
    // Drain both callbacks even when the admission assertion regresses.
    if let Ok(second) = second {
        second.cancel().unwrap();
        control.release.send(()).unwrap();
        control.release.send(()).unwrap();
        assert!(wait_job(second).is_err());
    } else {
        control.release.send(()).unwrap();
    }
    assert!(wait_job(first).is_err());
    engine.shutdown().unwrap();
    assert!(
        rejected,
        "the same destination Arc cannot initialize two jobs concurrently"
    );
}
