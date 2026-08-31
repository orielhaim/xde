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
}

impl MemorySink {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            buf: Mutex::new(Vec::new()),
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
        Ok(())
    }

    async fn read_back(&self, offset: u64, len: usize) -> xde::Result<Vec<u8>> {
        let buf = self.buf.lock();
        let start = offset as usize;
        let end = (start + len).min(buf.len());
        Ok(buf[start..end].to_vec())
    }

    async fn begin(&self, spec: BeginArtifact) -> xde::Result<()> {
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
    job.cancel();
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
    job.cancel();
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
        initial_physical_connections: 2,
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
