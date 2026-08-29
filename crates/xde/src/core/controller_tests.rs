use super::*;
use crate::core::spec::JobSpec;
use url::Url;

fn spec(url: &str) -> JobSpec {
    JobSpec::new(Url::parse(url).unwrap())
}

fn admit(c: &mut Controller, s: JobSpec, key: &str, now: Instant) -> JobId {
    let (job, _) = c
        .world
        .admit_job(&s, key.to_string())
        .expect("admission succeeds");
    let actions = c.handle(
        Observation::JobAdmitted {
            job,
            spec: Box::new(s),
            resume: None,
        },
        now,
    );
    debug_assert!(actions.iter().any(|a| matches!(a, Action::Resolve { .. })));
    job
}

fn drive_to_transfer(
    c: &mut Controller,
    job: JobId,
    now: Instant,
    protocol: crate::core::events::Protocol,
    total: u64,
) -> (Vec<Action>, ConnectionId) {
    let origin = c.world.jobs.get(job).unwrap().origin;
    let mut all = Vec::new();
    let acts = c.handle(
        Observation::Resolved {
            origin,
            endpoints: vec!["127.0.0.1:8080".parse().unwrap()],
            failed: false,
            https_records: Vec::new(),
            from_cache: false,
        },
        now,
    );
    all.extend(acts);
    let conn = c
        .world
        .connections
        .iter()
        .next()
        .map(|(id, _)| id)
        .expect("connection allocated");
    let acts = c.handle(
        Observation::ConnectionReady {
            connection: conn,
            protocol,
            handshake: Duration::from_millis(5),
        },
        now,
    );
    all.extend(acts);
    let acts = c.handle(
        Observation::Probed {
            job,
            source: c.world.jobs.get(job).unwrap().primary_source(),
            connection: conn,
            supports_ranges: true,
            total_length: Some(total),
            etag: Some("\"v1\"".into()),
            last_modified: None,
            reusable: true,
            alt_svc_h3: None,
        },
        now,
    );
    all.extend(acts);
    (all, conn)
}

fn force_transferring(c: &mut Controller, job: JobId, total: u64) {
    let j = c.world.jobs.get_mut(job).unwrap();
    j.phase = JobPhase::Transferring;
    j.supports_ranges = true;
    j.total_length = Some(total);
    j.plan = Some(crate::core::segment::SegmentPlan::new(
        Some(total),
        crate::core::ranges::RangeSet::new(),
        Default::default(),
        Duration::from_secs(1),
        0,
    ));
}

fn sample(bytes: u64, wall_ms: u64, stall_ms: u64) -> crate::core::metrics::TransferSample {
    crate::core::metrics::TransferSample {
        bytes,
        ttfb: Duration::from_millis(1),
        response_wall: Duration::from_millis(wall_ms),
        receive_active: Duration::from_millis(wall_ms.saturating_sub(stall_ms).max(1)),
        memory_blocked: Duration::ZERO,
        destination_blocked: Duration::from_millis(stall_ms),
        next_pending: Duration::ZERO,
        max_frame_gap: Duration::ZERO,
        send_ready: Duration::ZERO,
        headers: Duration::ZERO,
        data_frames: 0,
        dest_accepts: 0,
        copy_count: 0,
        copied_bytes: 0,
        frame_p50: 0,
        frame_p90: 0,
        avg_frame: 0,
        io_reads_submitted: 0,
        io_reads_completed: 0,
        zero_read: Duration::ZERO,
        max_zero_read: Duration::ZERO,
    }
}

#[test]
fn auto_starts_with_one_physical_connection() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a.bin"), "k", now);
    let (actions, _) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http1_1,
        8 << 20,
    );
    let opens = actions
        .iter()
        .filter(|a| matches!(a, Action::OpenConnection { .. }))
        .count();
    assert_eq!(opens, 1, "cold start is one dial, got {actions:?}");
    let origin = c.world.jobs.get(job).unwrap().origin;
    assert_eq!(
        c.world.origins.get(origin).unwrap().adaptive_target_conns,
        1
    );
    assert_eq!(
        c.world.origins.get(origin).unwrap().adaptive_target_streams,
        1
    );
}

#[test]
fn h1_pump_claims_exactly_one_assignment() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a.bin"), "k", now);
    let (actions, conn) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http1_1,
        1000,
    );
    let starts = actions
        .iter()
        .filter(|a| matches!(a, Action::StartAssignment { .. }))
        .count();
    assert_eq!(starts, 1);
    assert_eq!(c.in_flight_on(conn), 1);
}

#[test]
fn h2_initial_stream_target_is_one() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://h2.test/a.bin"), "k", now);
    let (actions, conn) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http2,
        32 << 20,
    );
    let starts = actions
        .iter()
        .filter(|a| matches!(a, Action::StartAssignment { .. }))
        .count();
    assert_eq!(starts, 1, "one H2 stream at cold start: {actions:?}");
    assert_eq!(c.in_flight_on(conn), 1);
}

#[test]
fn poisoned_h1_closes_the_connection() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a.bin"), "k", now);
    let (actions, conn) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http1_1,
        1000,
    );
    let Action::StartAssignment { assignment, .. } = actions
        .iter()
        .find(|a| matches!(a, Action::StartAssignment { .. }))
        .cloned()
        .expect("assignment")
    else {
        unreachable!()
    };
    let actions = c.handle(
        Observation::AssignmentFailed {
            job,
            assignment,
            attempt: 1,
            disposition: Disposition::RetrySameRange {
                after: None,
                reason: "truncated",
            },
            connection: Some(conn),
            connection_state: ConnectionState::Poisoned,
            stream_health: StreamHealth::Failed,
        },
        now,
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::CloseConnection { .. }))
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::ScheduleTimer { .. }))
    );
}

#[test]
fn h2_stream_failure_does_not_close_the_physical_connection() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://h2.test/a.bin"), "k", now);
    let (actions, conn) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http2,
        8 << 20,
    );
    let Action::StartAssignment { assignment, .. } = actions
        .iter()
        .find(|a| matches!(a, Action::StartAssignment { .. }))
        .cloned()
        .expect("assignment")
    else {
        unreachable!()
    };
    let actions = c.handle(
        Observation::AssignmentFailed {
            job,
            assignment,
            attempt: 1,
            disposition: Disposition::RetrySameRange {
                after: None,
                reason: "stream reset",
            },
            connection: Some(conn),
            connection_state: ConnectionState::Reusable,
            stream_health: StreamHealth::Failed,
        },
        now,
    );
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::CloseConnection { connection } if *connection == conn)),
        "sibling H2 streams must keep the connection: {actions:?}"
    );
    assert!(c.world.connections.get(conn).is_some());
}

#[test]
fn dispatch_failure_does_not_leave_a_ghost_assignment() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a.bin"), "k", now);
    let (actions, conn) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http1_1,
        1000,
    );
    let Action::StartAssignment { assignment, .. } = actions
        .iter()
        .find(|a| matches!(a, Action::StartAssignment { .. }))
        .cloned()
        .expect("assignment")
    else {
        unreachable!()
    };
    let _ = c.handle(
        Observation::DispatchFailed {
            operation: DispatchOperation::StartAssignment,
            job: Some(job),
            assignment: Some(assignment),
            connection: Some(conn),
            origin: None,
        },
        now,
    );
    let plan = c.world.jobs.get(job).unwrap().plan.as_ref().unwrap();
    assert_eq!(plan.in_flight(), 0);
    assert_eq!(c.in_flight_on(conn), 0);
}

#[test]
fn deferred_retry_is_not_reclaimed_before_backoff() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a.bin"), "k", now);
    force_transferring(&mut c, job, 1000);
    let plan = c.world.jobs.get_mut(job).unwrap().plan.as_mut().unwrap();
    plan.defer(
        crate::core::ranges::ByteRange::new(0, 1000),
        now + Duration::from_secs(5),
        2,
    );
    let claim = plan.claim(crate::core::units::Rate::from_bps(1000.0), now);
    match claim {
        Claim::Saturated | Claim::Complete => {}
        other => panic!("deferred work must stay claimed, got {other:?}"),
    }
    plan.release_due(now + Duration::from_secs(5));
    assert!(matches!(
        plan.claim(
            crate::core::units::Rate::from_bps(1000.0),
            now + Duration::from_secs(5)
        ),
        Claim::Fresh(_)
    ));
}

#[test]
fn stall_dominated_window_does_not_open_a_topology_experiment() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://stall.test/a.bin"), "k", now);
    let (_, conn) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http1_1,
        64 << 20,
    );
    let origin = c.world.jobs.get(job).unwrap().origin;
    if let Some(o) = c.world.origins.get_mut(origin) {
        o.last_window_at = now - Duration::from_secs(2);
        o.last_window_bytes = 0;
        o.topology_experiment = None;
    }
    // High destination stall on the verified sample.
    let assignment = c
        .world
        .jobs
        .get(job)
        .unwrap()
        .plan
        .as_ref()
        .unwrap()
        .iter_assignments()
        .next()
        .map(|(id, _)| AssignmentRef::new(job, id));
    if let Some(assignment) = assignment {
        let _ = c.handle(
            Observation::AssignmentVerified {
                job,
                assignment,
                range: crate::core::ranges::ByteRange::new(0, 1024),
                sample: sample(1024, 1000, 800),
                connection: conn,
                connection_reusable: true,
            },
            now,
        );
    }
    if let Some(o) = c.world.origins.get_mut(origin) {
        o.last_window_at = now - Duration::from_secs(2);
    }
    let later = now + Duration::from_secs(2);
    let actions = c.handle(
        Observation::TimerExpired {
            event: TimerEvent::AdaptiveTick { origin },
        },
        later,
    );
    let origin_state = c.world.origins.get(origin).unwrap();
    assert!(
        origin_state.topology_experiment.is_none(),
        "stalled path must not add parallelism: {actions:?}"
    );
}

#[test]
fn origin_cooldown_blocks_further_dials() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://rl.test/a.bin"), "k", now);
    let origin = c.world.jobs.get(job).unwrap().origin;
    let _ = c.handle(
        Observation::RateLimited {
            origin,
            retry_after: Some(Duration::from_secs(10)),
        },
        now,
    );
    assert!(c.world.origins.get(origin).unwrap().is_cooled_down(now));
}

#[test]
fn cancellation_releases_the_destination_lease() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a.bin"), "k", now);
    let _ = c.handle(Observation::JobCancelled { job }, now);
    assert!(
        c.world
            .admit_job(&spec("https://example.test/a.bin"), "k".into())
            .is_ok()
    );
}

#[test]
fn deadline_fails_only_the_named_job() {
    let mut c = Controller::new();
    let now = Instant::now();
    let j1 = admit(&mut c, spec("https://a.test/1.bin"), "k1", now);
    let j2 = admit(&mut c, spec("https://b.test/2.bin"), "k2", now);
    let actions = c.handle(
        Observation::TimerExpired {
            event: TimerEvent::JobDeadline(j1),
        },
        now,
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::FailJob { job, .. } if *job == j1))
    );
    assert!(c.world.jobs.get(j2).is_some());
}

#[test]
fn second_job_on_resolved_origin_gets_a_probe() {
    let mut c = Controller::new();
    let now = Instant::now();
    let a = admit(&mut c, spec("https://shared.test/a.bin"), "ka", now);
    let _ = drive_to_transfer(
        &mut c,
        a,
        now,
        crate::core::events::Protocol::Http1_1,
        8 << 20,
    );
    let sb = spec("https://shared.test/b.bin");
    let (b, _) = c
        .world
        .admit_job(&sb, "kb".into())
        .expect("second job admits");
    let actions = c.handle(
        Observation::JobAdmitted {
            job: b,
            spec: Box::new(sb),
            resume: None,
        },
        now,
    );
    assert!(
        !actions
            .iter()
            .any(|act| matches!(act, Action::Resolve { .. })),
        "must not re-resolve a known origin: {actions:?}"
    );
    assert!(
        actions.iter().any(|act| match act {
            Action::OpenConnection { .. } => true,
            Action::Probe { job, .. } if *job == b => true,
            _ => false,
        }),
        "second job must probe without waiting for the first transfer: {actions:?}"
    );
    let phase = c.world.jobs.get(b).unwrap().phase;
    assert!(
        matches!(phase, JobPhase::Created | JobPhase::Probing),
        "unexpected phase {phase:?}"
    );
}

#[test]
fn destination_lease_conflict_is_rejected() {
    let mut c = Controller::new();
    let _ = c
        .world
        .admit_job(&spec("https://example.test/a.bin"), "same.bin".into())
        .unwrap();
    assert!(matches!(
        c.world
            .admit_job(&spec("https://other.test/b.bin"), "same.bin".into()),
        Err(crate::core::world::DestinationLeaseError::Busy(_))
    ));
}

#[test]
fn engine_connection_ceiling_caps_scale_out() {
    let mut c = Controller::new();
    c.set_engine_limits(&crate::core::policy::EngineLimits {
        max_physical_connections: 1,
        ..crate::core::policy::EngineLimits::default()
    });
    let now = Instant::now();
    let job = admit(&mut c, spec("https://cap.test/a.bin"), "k", now);
    force_transferring(&mut c, job, 100_000_000);
    let origin = c.world.jobs.get(job).unwrap().origin;
    if let Some(o) = c.world.origins.get_mut(origin) {
        o.adaptive_target_conns = 8;
    }
    c.world
        .note_endpoints(origin, &["127.0.0.1:9".parse().unwrap()]);
    let mut actions = Vec::new();
    c.scale_connections(&mut actions, Some(origin), now);
    let opens = actions
        .iter()
        .filter(|a| matches!(a, Action::OpenConnection { .. }))
        .count();
    assert!(
        c.world.connections.len() <= 1 || opens <= 1,
        "engine ceiling must bind: conns={} opens={opens} {actions:?}",
        c.world.connections.len()
    );
}
