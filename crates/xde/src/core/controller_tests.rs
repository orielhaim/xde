use super::*;
use crate::core::spec::JobSpec;
use url::Url;

/// Run alone in release mode; output is a diagnostic distribution, never a test threshold.
#[test]
#[ignore]
fn control_scaling_measurements() {
    use std::hint::black_box;
    for n in [1, 8, 32, 128, 512] {
        for shared in [false, true] {
            for connections in [0, 4] {
                let mut c = Controller::new();
                c.limits.max_physical_connections = (n * 4).min(u16::MAX as usize) as u16;
                c.limits.max_connections_per_origin = c.limits.max_physical_connections;
                c.limits.max_active_assignments = (n * 8) as u32;
                let now = Instant::now();
                let mut jobs = Vec::new();
                for i in 0..n {
                    let host = if shared { 0 } else { i };
                    let mut s = spec(&format!("http://host{host}.test/data"));
                    s.policy.topology = crate::TopologyPolicy::Fixed(crate::Topology::new(4, 2));
                    let job = c
                        .world
                        .admit_job(
                            &s,
                            crate::core::world::DestinationKey::File(format!("out{i}").into()),
                        )
                        .unwrap();
                    force_transferring(&mut c, job, 1 << 30);
                    let origin = c.world.jobs[job].origin;
                    c.world
                        .note_endpoints(origin, &["127.0.0.1:8080".parse().unwrap()]);
                    let endpoint = c.world.origins[origin].endpoints[0];
                    for _ in 0..connections {
                        let id = c.world.allocate_connection(origin, endpoint, 0);
                        let node = &mut c.world.connections[id];
                        node.owner = Some(job);
                        node.protocol = crate::Protocol::Http2;
                        node.status = ConnectionStatus::Ready;
                    }
                    jobs.push(job);
                }
                for trial in 0..5 {
                    let before = c.stats.allocation_passes;
                    let start = Instant::now();
                    for _ in 0..8 {
                        c.grant_demand = None;
                        c.refresh_grants(black_box(now));
                    }
                    println!(
                        "{{\"kind\":\"refresh\",\"jobs\":{n},\"shared\":{shared},\"connections_per_job\":{connections},\"trial\":{trial},\"ns_per_call\":{},\"passes\":{}}}",
                        start.elapsed().as_nanos() / 8,
                        c.stats.allocation_passes - before
                    );
                    let start = Instant::now();
                    for _ in 0..8 {
                        let connection = c.world.connections.keys().next().unwrap_or_default();
                        black_box(
                            c.world
                                .connections
                                .get(connection)
                                .and_then(|node| node.owner)
                                .is_some_and(|job| c.can_assign(job, connection, now)),
                        );
                    }
                    println!(
                        "{{\"kind\":\"selection\",\"jobs\":{n},\"shared\":{shared},\"connections_per_job\":{connections},\"trial\":{trial},\"ns_per_call\":{}}}",
                        start.elapsed().as_nanos() / 8
                    );
                }
                black_box(jobs);
            }
        }
    }
}

#[test]
fn grants_reuse_identical_inputs_and_invalidate_on_policy_changes() {
    let now = Instant::now();
    let mut c = Controller::new();
    let a = admit(&mut c, spec("http://a.test/data"), "a", now);
    let b = admit(&mut c, spec("http://b.test/data"), "b", now);
    c.refresh_grants(now);
    let before = c.stats.allocation_passes;
    let expected = c.world.jobs[a].grant;
    for _ in 0..8 {
        c.refresh_grants(now);
    }
    assert_eq!(c.stats.allocation_passes, before);
    assert_eq!(c.world.jobs[a].grant, expected);
    c.world.jobs[b].priority = crate::Priority::Critical;
    c.refresh_grants(now);
    assert_eq!(c.stats.allocation_passes, before + 1);
    let mut limits = c.limits;
    limits.max_physical_connections = 1;
    c.set_engine_limits(&limits);
    c.refresh_grants(now);
    assert_eq!(c.stats.allocation_passes, before + 2);
    assert_eq!(
        c.world
            .jobs
            .values()
            .map(|j| j.grant.connections as usize)
            .sum::<usize>(),
        1
    );
    c.world.jobs[a].phase = JobPhase::Paused;
    c.world.jobs[b].phase = JobPhase::Paused;
    c.refresh_grants(now);
    assert!(c.world.jobs.values().all(|j| j.grant == Default::default()));
}

#[test]
fn mixed_protocol_capacity_never_turns_serial_connections_into_multiplexed_slots() {
    let now = Instant::now();
    let mut c = Controller::new();
    let mut s = spec("http://mixed.test/data");
    s.policy.topology = crate::TopologyPolicy::Fixed(crate::Topology::new(4, 4));
    s.policy.transport.max_active_assignments = 32;
    let job = admit(&mut c, s, "mixed", now);
    drive_to_transfer(&mut c, job, now, crate::Protocol::Http2, 1 << 34);
    let opening: Vec<_> = c
        .world
        .connections
        .iter()
        .filter(|(_, c)| c.status == ConnectionStatus::Connecting)
        .map(|(id, _)| id)
        .collect();
    assert_eq!(opening.len(), 3);
    for connection in opening {
        c.handle(
            Observation::ConnectionReady {
                connection,
                protocol: crate::Protocol::Http1_1,
                handshake: Duration::from_millis(1),
            },
            now,
        );
    }
    assert_eq!(c.world.jobs[job].grant.assignments, 7);
    assert_eq!(c.ready_job_capacity(job).slots(4), 7);
    assert_eq!(c.world.jobs[job].plan.as_ref().unwrap().in_flight(), 7);
    assert!(!c.globally_constrained(job));
    let mut limits = c.limits;
    limits.max_active_assignments = 5;
    c.set_engine_limits(&limits);
    c.refresh_grants(now);
    assert_eq!(c.world.jobs[job].grant.assignments, 5);
    assert!(c.globally_constrained(job));
    for (id, connection) in &c.world.connections {
        assert!(
            c.in_flight_on(id)
                <= if connection.protocol == crate::Protocol::Http1_1 {
                    1
                } else {
                    4
                }
        );
    }
}

#[test]
fn distinct_active_connections_respect_a_reduced_grant_until_drain() {
    let now = Instant::now();
    let mut c = Controller::new();
    let mut s = spec("http://draining.test/data");
    s.policy.topology = crate::TopologyPolicy::Fixed(crate::Topology::new(2, 2));
    let job = admit(&mut c, s, "draining", now);
    let (_, old) = drive_to_transfer(&mut c, job, now, crate::Protocol::Http2, 1 << 34);
    let assignments: Vec<_> = c.world.jobs[job]
        .plan
        .as_ref()
        .unwrap()
        .iter_assignments()
        .map(|(id, _)| id)
        .collect();
    for id in &assignments[1..] {
        c.world.jobs[job].plan.as_mut().unwrap().abandon(*id);
        c.release_assignment_slot(Some(old));
    }
    let next = c
        .world
        .connections
        .iter()
        .find(|(_, c)| c.status == ConnectionStatus::Connecting)
        .unwrap()
        .0;
    c.world.connections[next].status = ConnectionStatus::Ready;
    c.world.connections[next].protocol = crate::Protocol::Http2;
    c.world.jobs[job].grant = crate::ResourceGrant {
        connections: 1,
        assignments: 2,
    };
    let mut actions = Vec::new();
    let origin = c.world.jobs[job].origin;
    c.pump(&mut actions, origin, now);
    assert!(
        !actions.iter().any(
            |a| matches!(a, Action::StartAssignment { connection, .. } if *connection == next)
        )
    );
    c.world.jobs[job]
        .plan
        .as_mut()
        .unwrap()
        .abandon(assignments[0]);
    c.release_assignment_slot(Some(old));
    c.world.connections[old].status = ConnectionStatus::Retired;
    c.pump(&mut actions, origin, now);
    assert!(
        actions.iter().any(
            |a| matches!(a, Action::StartAssignment { connection, .. } if *connection == next)
        )
    );
}

#[test]
fn excess_busy_multiplexed_connections_are_not_replenished_while_draining() {
    let mut c = Controller::new();
    let now = Instant::now();
    let mut s = spec("http://mux-drain.test/data");
    s.policy.topology = crate::TopologyPolicy::Fixed(crate::Topology::new(2, 4));
    let job = admit(&mut c, s, "mux-drain", now);
    let (_, retained) = drive_to_transfer(&mut c, job, now, crate::Protocol::Http2, 1 << 34);
    let retiring = c
        .world
        .connections
        .iter()
        .find(|(_, c)| c.status == ConnectionStatus::Connecting)
        .unwrap()
        .0;
    c.handle(
        Observation::ConnectionReady {
            connection: retiring,
            protocol: crate::Protocol::Http2,
            handshake: Duration::from_millis(1),
        },
        now,
    );
    assert_eq!(c.in_flight_on(retained), 4);
    assert_eq!(c.in_flight_on(retiring), 4);
    let requests = |connection| {
        c.world.jobs[job]
            .plan
            .as_ref()
            .unwrap()
            .iter_assignments()
            .filter(|(_, a)| a.connection == Some(connection))
            .map(|(id, a)| (c.assignment_ref(job, id), a.range))
            .collect::<Vec<_>>()
    };
    let kept = requests(retained);
    let drained = requests(retiring);
    let mut limits = c.limits;
    limits.max_physical_connections = 1;
    c.set_engine_limits(&limits);
    c.reconcile(&mut Vec::new(), now);
    assert_eq!(
        c.world.jobs[job].grant,
        crate::ResourceGrant {
            connections: 1,
            assignments: 4
        }
    );
    let mut actions = Vec::new();
    for (connection, requests) in [(retained, &kept[..2]), (retiring, &drained[..3])] {
        for &(assignment, range) in requests {
            actions.extend(c.handle(
                Observation::AssignmentVerified {
                    job,
                    assignment,
                    range,
                    connection,
                    sample: sample(range.len(), 1000, 0),
                    connection_reusable: true,
                },
                now + Duration::from_secs(1),
            ));
        }
    }
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::StartAssignment { .. }))
    );
    assert!(actions.iter().all(
        |a| !matches!(a, Action::StartAssignment { connection, .. } if *connection == retiring)
    ));
    assert_eq!(c.in_flight_on(retiring), 1);
    let (assignment, range) = drained[3];
    let actions = c.handle(
        Observation::AssignmentVerified {
            job,
            assignment,
            range,
            connection: retiring,
            sample: sample(range.len(), 1000, 0),
            connection_reusable: true,
        },
        now + Duration::from_secs(2),
    );
    assert!(
        actions.iter().any(
            |a| matches!(a, Action::CloseConnection { connection } if *connection == retiring)
        )
    );
    assert_eq!(c.reserved_connections(), 2);
    c.handle(
        Observation::ConnectionGone {
            connection: retiring,
        },
        now + Duration::from_secs(2),
    );
    assert_eq!(c.reserved_connections(), 1);
    assert!(
        c.world.jobs[job]
            .plan
            .as_ref()
            .unwrap()
            .iter_assignments()
            .all(|(_, a)| a.connection == Some(retained))
    );
}

#[test]
fn cooled_origin_handoff_refreshes_multiplexed_capacity_before_the_first_claim() {
    let mut c = Controller::new();
    c.limits.max_physical_connections = 1;
    let now = Instant::now();
    let mut s = spec("http://handoff.test/a");
    s.policy.topology = crate::TopologyPolicy::Fixed(crate::Topology::new(1, 4));
    let donor = admit(&mut c, s.clone(), "donor", now);
    let (_, connection) = drive_to_transfer(&mut c, donor, now, crate::Protocol::Http2, 16 << 10);
    let (assignment, range) = active_assignment(&c, donor);
    let recipient = admit(&mut c, s, "recipient", now);
    force_transferring(&mut c, recipient, 1 << 34);
    let origin = c.world.jobs[recipient].origin;
    c.world.origins[origin].cooldown_until = Some(now + Duration::from_secs(60));
    c.handle(
        Observation::AssignmentVerified {
            job: donor,
            assignment,
            range,
            connection,
            sample: sample(range.len(), 10, 0),
            connection_reusable: true,
        },
        now,
    );
    assert_eq!(c.world.connections[connection].owner, Some(recipient));
    assert_eq!(c.world.jobs[recipient].grant.assignments, 4);
    assert_eq!(
        c.world.jobs[recipient].plan.as_ref().unwrap().in_flight(),
        4
    );
}

fn spec(url: &str) -> JobSpec {
    JobSpec::new(Url::parse(url).unwrap())
}

fn admit(c: &mut Controller, s: JobSpec, key: &str, now: Instant) -> JobId {
    let job = c
        .world
        .admit_job(&s, crate::core::world::DestinationKey::File(key.into()))
        .expect("admission succeeds");
    let actions = c.handle(
        Observation::JobAdmitted {
            job,
            spec: Box::new(s),
            resume: None,
        },
        now,
    );
    let _ = actions;
    assert!(c.world.jobs.contains_key(job));
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

fn sample(bytes: u64, wall_ms: u64, stall_ms: u64) -> Box<crate::core::metrics::TransferSample> {
    Box::new(crate::core::metrics::TransferSample {
        bytes,
        ttfb: Duration::from_millis(1),
        response_wall: Duration::from_millis(wall_ms),
        receive_active: Duration::from_millis(wall_ms.saturating_sub(stall_ms).max(1)),
        memory_blocked: Duration::ZERO,
        memory_wait: Duration::ZERO,
        destination_blocked: Duration::from_millis(stall_ms),
        next_pending: Duration::ZERO,
        max_frame_gap: Duration::ZERO,
        send_ready: Duration::ZERO,
        headers: Duration::ZERO,
        data_frames: 0,
        dest_accepts: 0,
        destination: Default::default(),
        payload: Default::default(),
        copy_count: 0,
        copied_bytes: 0,
        frame_p50: 0,
        frame_p90: 0,
        avg_frame: 0,
        io_reads_submitted: 0,
        io_reads_completed: 0,
        zero_read: Duration::ZERO,
        max_zero_read: Duration::ZERO,
    })
}

fn active_assignment(c: &Controller, job: JobId) -> (AssignmentRef, ByteRange) {
    let j = &c.world.jobs[job];
    let (id, assignment) = j.plan.as_ref().unwrap().iter_assignments().next().unwrap();
    (AssignmentRef::new(job, j.plan_id, id), assignment.range)
}

#[test]
fn commit_acknowledgement_cannot_complete_a_transferring_job() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a"), "k", now);
    drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http1_1,
        1024,
    );
    let actions = c.handle(
        Observation::DestinationCommitted {
            job,
            final_length: 1024,
            digest: None,
        },
        now,
    );
    assert!(actions.is_empty());
    assert_eq!(c.world.jobs[job].phase, JobPhase::Transferring);
    assert_eq!(
        c.world
            .destination_leases
            .get(&c.world.jobs[job].destination),
        Some(&job)
    );
}

#[test]
fn cancelled_last_writer_cannot_start_a_commit() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a"), "k", now);
    let (_, connection) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http1_1,
        1024,
    );
    let (assignment, range) = active_assignment(&c, job);
    let destination = c.world.jobs[job].destination.clone();
    c.handle(Observation::JobCancelled { job }, now);
    assert_eq!(c.world.destination_leases.get(&destination), Some(&job));
    let actions = c.handle(
        Observation::AssignmentVerified {
            job,
            assignment,
            range,
            sample: sample(range.len(), 10, 0),
            connection,
            connection_reusable: true,
        },
        now,
    );
    assert!(actions.iter().any(|a| matches!(
        a,
        Action::FailJob {
            error: JobFailure::Cancelled,
            ..
        }
    )));
    assert!(!actions.iter().any(|a| matches!(
        a,
        Action::CommitDestination { .. } | Action::CompleteJob { .. }
    )));
    assert!(!c.world.destination_leases.contains_key(&destination));
    assert!(c.world.sources.is_empty());
    assert_eq!(c.active_assignment_count, 0);
}

#[test]
fn full_body_restart_drains_writers_and_rejects_retired_plan_messages() {
    let mut c = Controller::new();
    let now = Instant::now();
    let mut s = spec("https://example.test/a");
    s.policy.topology = crate::TopologyPolicy::Fixed(crate::Topology::new(1, 2));
    let job = admit(&mut c, s, "k", now);
    let (_, connection) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http2,
        8 << 20,
    );
    let old: Vec<_> = c.world.jobs[job]
        .plan
        .as_ref()
        .unwrap()
        .iter_assignments()
        .map(|(id, a)| (c.assignment_ref(job, id), a.range))
        .collect();
    assert!(old.len() >= 2);
    let actions = c.handle(
        Observation::AssignmentFailed {
            job,
            assignment: old[0].0,
            attempt: 0,
            disposition: Disposition::FullBodyForRangeRequest,
            connection: Some(connection),
            connection_state: ConnectionState::Reusable,
            stream_health: StreamHealth::Failed,
        },
        now,
    );
    assert_eq!(c.world.jobs[job].phase, JobPhase::Restarting);
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::StartFullBody { .. }))
    );
    for (assignment, range) in old.iter().skip(1).copied() {
        c.handle(
            Observation::AssignmentVerified {
                job,
                assignment,
                range,
                sample: sample(range.len(), 10, 0),
                connection,
                connection_reusable: true,
            },
            now,
        );
    }
    let j = &c.world.jobs[job];
    assert_eq!(j.phase, JobPhase::Transferring);
    assert_ne!(j.plan_id, old[0].0.plan);
    assert_eq!(j.plan.as_ref().unwrap().bytes_done(), 0);
    let count = c.active_assignment_count;
    let stale = c.handle(
        Observation::AssignmentFailed {
            job,
            assignment: old[0].0,
            attempt: 0,
            disposition: Disposition::Fatal {
                status: 0,
                reason: "old failure",
            },
            connection: Some(connection),
            connection_state: ConnectionState::Poisoned,
            stream_health: StreamHealth::Failed,
        },
        now,
    );
    assert!(stale.is_empty());
    assert_eq!(c.active_assignment_count, count);
    let stale = c.handle(
        Observation::TimerExpired {
            event: TimerEvent::RetryReady {
                assignment: old[0].0,
                range: old[0].1,
                attempt: 1,
            },
        },
        now,
    );
    assert!(stale.is_empty());
}

#[test]
fn publication_owns_cancellation_once_commit_begins() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a"), "k", now);
    let (_, connection) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http1_1,
        1024,
    );
    let (assignment, range) = active_assignment(&c, job);
    let actions = c.handle(
        Observation::AssignmentVerified {
            job,
            assignment,
            range,
            sample: sample(range.len(), 10, 0),
            connection,
            connection_reusable: true,
        },
        now,
    );
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::CommitDestination { .. }))
    );
    assert!(c.handle(Observation::JobCancelled { job }, now).is_empty());
    let actions = c.handle(
        Observation::DestinationCommitted {
            job,
            final_length: 1024,
            digest: None,
        },
        now,
    );
    assert_eq!(
        actions
            .iter()
            .filter(|a| matches!(a, Action::CompleteJob { .. }))
            .count(),
        1
    );
    assert!(c.world.sources.is_empty());
    assert!(c.world.destination_leases.is_empty());
    assert!(
        c.handle(
            Observation::DestinationCommitted {
                job,
                final_length: 1024,
                digest: None
            },
            now
        )
        .is_empty()
    );
}

#[test]
fn verified_work_on_a_retired_connection_cannot_update_a_reused_connection_slot() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a"), "k", now);
    let (_, connection) =
        drive_to_transfer(&mut c, job, now, crate::core::events::Protocol::Http2, 1024);
    let (assignment, range) = active_assignment(&c, job);
    let origin = c.world.jobs[job].origin;
    let endpoint = c.world.connections[connection].endpoint;
    c.drop_connection(connection);
    let replacement = c.world.allocate_connection(origin, endpoint, 0);
    c.connection_state.insert(replacement, Default::default());
    let actions = c.handle(
        Observation::AssignmentVerified {
            job,
            assignment,
            range,
            sample: sample(range.len(), 10, 0),
            connection,
            connection_reusable: true,
        },
        now,
    );
    assert!(c.connection_state[replacement].ewma.is_none());
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::CommitDestination { .. }))
    );
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
    assert_eq!(c.world.jobs[job].planner.desired().connections, 1);
    assert_eq!(
        c.world.jobs[job].planner.desired().streams_per_connection,
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
fn small_h2_download_keeps_one_stream() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://h2.test/a.bin"), "k", now);
    let (actions, conn) = drive_to_transfer(
        &mut c,
        job,
        now,
        crate::core::events::Protocol::Http2,
        1 << 20,
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
    let initial = c.world.jobs[job].planner.desired();
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
        .map(|(id, _)| AssignmentRef::new(job, c.world.jobs[job].plan_id, id));
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
    let later = now + Duration::from_secs(2);
    let actions = c.handle(
        Observation::TimerExpired {
            event: TimerEvent::AdaptiveTick { job },
        },
        later,
    );
    assert!(
        c.world.jobs[job].planner.desired() == initial,
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
            .admit_job(
                &spec("https://example.test/a.bin"),
                crate::core::world::DestinationKey::File("k".into())
            )
            .is_ok()
    );
}

#[test]
fn terminal_jobs_release_source_metadata() {
    let mut c = Controller::new();
    let now = Instant::now();
    for index in 0..32 {
        let job = admit(
            &mut c,
            spec(&format!("https://example.test/{index}")),
            &format!("out{index}"),
            now,
        );
        assert_eq!(c.world.sources.len(), 1);
        c.handle(Observation::JobCancelled { job }, now);
        assert!(c.world.jobs.is_empty());
        assert!(c.world.destination_leases.is_empty());
        assert!(
            c.world.sources.is_empty(),
            "terminal job retained its sources"
        );
    }
    assert_eq!(
        c.world.origins.len(),
        1,
        "origin knowledge remains reusable"
    );
}

#[test]
fn cancelling_a_primary_probe_closes_it_before_connection_handoff() {
    let mut c = Controller::new();
    c.limits.max_physical_connections = 1;
    c.limits.max_connections_per_origin = 1;
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a"), "out", now);
    let origin = c.world.jobs[job].origin;
    c.handle(
        Observation::Resolved {
            origin,
            endpoints: vec!["127.0.0.1:8080".parse().unwrap()],
            failed: false,
            https_records: Vec::new(),
            from_cache: false,
        },
        now,
    );
    let connection = c.world.connections.keys().next().unwrap();
    c.handle(
        Observation::ConnectionReady {
            connection,
            protocol: crate::Protocol::Http2,
            handshake: Duration::from_millis(5),
        },
        now,
    );
    assert_eq!(c.world.jobs[job].phase, JobPhase::Probing);
    let peer = admit(&mut c, spec("https://example.test/b"), "peer", now);
    let actions = c.handle(Observation::JobCancelled { job }, now);
    assert!(
        actions.iter().any(|a| matches!(a,
            Action::CloseConnection { connection: closed } if *closed == connection
        )),
        "live probe was handed off: {actions:?}"
    );
    assert!(!c.world.connections.contains_key(connection));
    assert_eq!(c.world.jobs[peer].total_length, None);
}

#[test]
fn retired_control_results_cannot_mutate_a_new_job_or_lease() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://example.test/a"), "out", now);
    let (_, connection) = drive_to_transfer(&mut c, job, now, crate::Protocol::Http2, 1024);
    let source = c.world.jobs[job].sources[0];
    let (assignment, range) = active_assignment(&c, job);
    c.handle(Observation::JobCancelled { job }, now);
    c.handle(
        Observation::AssignmentVerified {
            job,
            assignment,
            range,
            sample: sample(range.len(), 10, 0),
            connection,
            connection_reusable: true,
        },
        now,
    );
    let replacement = admit(&mut c, spec("https://example.test/b"), "out", now);
    let fresh_source = c.world.jobs[replacement].sources[0];
    assert_ne!(job, replacement);
    assert_ne!(source, fresh_source);
    for observation in [
        Observation::Probed {
            job,
            connection,
            source,
            supports_ranges: true,
            total_length: Some(2048),
            etag: None,
            last_modified: None,
            reusable: true,
            alt_svc_h3: None,
        },
        Observation::MirrorSampled {
            job,
            source,
            connection,
            matches: true,
            reusable: true,
        },
        Observation::JobCancelled { job },
    ] {
        c.handle(observation, now);
        assert_eq!(c.world.sources.len(), 1);
        assert_eq!(c.world.jobs[replacement].sources, vec![fresh_source]);
        assert_eq!(c.world.jobs[replacement].total_length, None);
        assert_eq!(
            c.world
                .destination_leases
                .get(&c.world.jobs[replacement].destination),
            Some(&replacement)
        );
    }
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
    let b = c
        .world
        .admit_job(&sb, crate::core::world::DestinationKey::File("kb".into()))
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
    let phase = c.world.jobs.get(b).unwrap().phase.clone();
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
        .admit_job(
            &spec("https://example.test/a.bin"),
            crate::core::world::DestinationKey::File("same.bin".into()),
        )
        .unwrap();
    assert!(matches!(
        c.world.admit_job(
            &spec("https://other.test/b.bin"),
            crate::core::world::DestinationKey::File("same.bin".into())
        ),
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

#[test]
fn closing_connection_keeps_its_physical_reservation_until_acknowledged() {
    let mut c = Controller::new();
    c.set_engine_limits(&crate::EngineLimits {
        max_physical_connections: 1,
        ..Default::default()
    });
    let now = Instant::now();
    let job = admit(&mut c, spec("https://first.test/a"), "a", now);
    let (_, connection) = drive_to_transfer(&mut c, job, now, crate::Protocol::Http1_1, 1024);
    let (assignment, range) = active_assignment(&c, job);
    c.handle(Observation::JobCancelled { job }, now);
    c.handle(
        Observation::AssignmentVerified {
            job,
            assignment,
            range,
            sample: sample(range.len(), 10, 0),
            connection,
            connection_reusable: true,
        },
        now,
    );
    assert!(!c.world.connections.contains_key(connection));
    assert_eq!(c.reserved_connections(), 1);
    let next = admit(&mut c, spec("https://second.test/a"), "b", now);
    let origin = c.world.jobs[next].origin;
    let actions = c.handle(
        Observation::Resolved {
            origin,
            endpoints: vec!["127.0.0.1:8081".parse().unwrap()],
            failed: false,
            https_records: Vec::new(),
            from_cache: false,
        },
        now,
    );
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::OpenConnection { .. }))
    );
    let actions = c.handle(Observation::ConnectionGone { connection }, now);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::OpenConnection { .. }))
    );
    assert_eq!(c.reserved_connections(), 1);
    c.handle(Observation::ConnectionGone { connection }, now);
    assert_eq!(
        c.reserved_connections(),
        1,
        "duplicate acknowledgements cannot return another lease"
    );
}

#[test]
fn a_reused_connection_slot_does_not_replace_a_still_closing_lease() {
    let mut c = Controller::new();
    c.set_engine_limits(&crate::EngineLimits {
        max_physical_connections: 2,
        ..Default::default()
    });
    let now = Instant::now();
    let job = admit(&mut c, spec("https://closing.test/a"), "a", now);
    let (_, old) = drive_to_transfer(&mut c, job, now, crate::Protocol::Http1_1, 1024);
    let (assignment, range) = active_assignment(&c, job);
    c.handle(Observation::JobCancelled { job }, now);
    c.handle(
        Observation::AssignmentVerified {
            job,
            assignment,
            range,
            sample: sample(range.len(), 10, 0),
            connection: old,
            connection_reusable: true,
        },
        now,
    );
    let second = admit(&mut c, spec("https://second.test/a"), "b", now);
    c.handle(
        Observation::Resolved {
            origin: c.world.jobs[second].origin,
            endpoints: vec!["127.0.0.1:8081".parse().unwrap()],
            failed: false,
            https_records: Vec::new(),
            from_cache: false,
        },
        now,
    );
    assert_eq!(
        c.reserved_connections(),
        2,
        "closing and live generations must both count"
    );
    let third = admit(&mut c, spec("https://third.test/a"), "c", now);
    let actions = c.handle(
        Observation::Resolved {
            origin: c.world.jobs[third].origin,
            endpoints: vec!["127.0.0.1:8082".parse().unwrap()],
            failed: false,
            https_records: Vec::new(),
            from_cache: false,
        },
        now,
    );
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::OpenConnection { .. }))
    );
    let actions = c.handle(Observation::ConnectionGone { connection: old }, now);
    assert!(
        actions
            .iter()
            .any(|a| matches!(a, Action::OpenConnection { .. }))
    );
    assert_eq!(c.reserved_connections(), 2);
    c.handle(Observation::ConnectionGone { connection: old }, now);
    assert_eq!(c.reserved_connections(), 2);
}

#[test]
fn queue_waiters_do_not_change_the_running_jobs_range_boundaries() {
    let mut c = Controller::new();
    c.set_engine_limits(&crate::EngineLimits {
        scheduling: crate::SchedulingPolicy::Queue {
            max_active_jobs: 1.try_into().unwrap(),
        },
        fair_quantum_bytes: 1024,
        ..Default::default()
    });
    let now = Instant::now();
    let job = admit(&mut c, spec("https://queue.test/a"), "a", now);
    drive_to_transfer(&mut c, job, now, crate::Protocol::Http1_1, 64 << 20);
    let (assignment, range) = active_assignment(&c, job);
    let waiter = admit(&mut c, spec("https://queue.test/b"), "b", now);
    assert_eq!(c.world.jobs[waiter].grant, crate::ResourceGrant::default());
    let a = c.world.jobs[job]
        .plan
        .as_ref()
        .unwrap()
        .assignment(assignment.assignment)
        .unwrap();
    assert_eq!(
        a.control.end.load(std::sync::atomic::Ordering::Relaxed),
        range.end
    );
    assert!(!a.truncated);
}

#[test]
fn a_waiting_probe_never_borrows_another_jobs_busy_connection_owner() {
    let mut c = Controller::new();
    let now = Instant::now();
    let first = admit(&mut c, spec("https://shared.test/a"), "a", now);
    drive_to_transfer(&mut c, first, now, crate::Protocol::Http2, 64 << 20);
    c.world.jobs[first].planner = crate::core::planner::JobPlanner::new(
        crate::TopologyPolicy::Fixed(crate::Topology::new(3, 1)),
        c.world.jobs[first].policy.transport,
    );
    let second = admit(&mut c, spec("https://shared.test/b"), "b", now);
    let connection = c
        .world
        .connections
        .iter()
        .find(|(_, c)| c.owner == Some(second))
        .map(|(id, _)| id)
        .expect("second job has its own granted dial");
    let actions = c.handle(
        Observation::ConnectionReady {
            connection,
            handshake: Duration::from_millis(1),
            protocol: crate::Protocol::Http2,
        },
        now,
    );
    assert!(actions.iter().any(|a| matches!(a, Action::Probe { job, connection: id, .. } if *job == second && *id == connection)));
    for action in actions {
        if let Action::Probe {
            job, connection, ..
        } = action
        {
            assert_eq!(c.world.connections[connection].owner, Some(job));
        }
    }
}

fn job_with_pending_mirror(
    c: &mut Controller,
    now: Instant,
) -> (JobId, ConnectionId, SourceId, OriginId) {
    let mut s = spec("https://primary.test/a");
    s.sources.push(crate::core::spec::SourceRequest::new(
        Url::parse("https://mirror.test/a").unwrap(),
    ));
    let job = admit(c, s, "mirror-job", now);
    let (_, primary) = drive_to_transfer(c, job, now, crate::Protocol::Http1_1, 4 << 20);
    let j = &mut c.world.jobs[job];
    j.planner = crate::core::planner::JobPlanner::new(
        crate::TopologyPolicy::Fixed(crate::Topology::new(2, 1)),
        j.policy.transport,
    );
    let source = j.sources[1];
    j.probing_sources.push(source);
    let origin = c.world.sources[source].origin;
    (job, primary, source, origin)
}

#[test]
fn pausing_retires_mirror_controls_and_late_results_cannot_reactivate_them() {
    let mut c = Controller::new();
    let now = Instant::now();
    let (job, primary, source, origin) = job_with_pending_mirror(&mut c, now);
    c.world
        .note_endpoints(origin, &["127.0.0.1:8082".parse().unwrap()]);
    let endpoint = c.world.origins[origin].endpoints[0];
    let mirror = c
        .dial_connection(
            Some(job),
            &mut Vec::new(),
            origin,
            endpoint,
            0,
            false,
            Some(source),
            now,
        )
        .unwrap();
    let (assignment, range) = active_assignment(&c, job);
    c.handle(Observation::JobPaused { job }, now);
    assert!(!c.world.connections.contains_key(mirror));
    assert!(c.world.jobs[job].probing_sources.is_empty());
    c.handle(
        Observation::AssignmentVerified {
            job,
            assignment,
            range,
            sample: sample(range.len(), 10, 0),
            connection: primary,
            connection_reusable: true,
        },
        now,
    );
    assert_eq!(c.world.jobs[job].phase, JobPhase::Paused);
    c.handle(Observation::JobResumed { job }, now);
    c.handle(
        Observation::MirrorSampled {
            job,
            source,
            connection: mirror,
            matches: true,
            reusable: true,
        },
        now,
    );
    assert!(!c.world.jobs[job].active_sources.contains(&source));
    assert!(c.world.jobs[job].probing_sources.is_empty());
}

#[test]
fn mirror_resolution_is_not_lost_when_another_job_waits_to_probe_that_origin() {
    let mut c = Controller::new();
    let now = Instant::now();
    let (job, _, source, origin) = job_with_pending_mirror(&mut c, now);
    let other = admit(&mut c, spec("https://mirror.test/b"), "other", now);
    c.handle(
        Observation::Resolved {
            origin,
            endpoints: vec!["127.0.0.1:8082".parse().unwrap()],
            failed: false,
            https_records: Vec::new(),
            from_cache: false,
        },
        now,
    );
    assert!(
        c.world
            .connections
            .values()
            .any(|c| c.owner == Some(job) && c.serving_source == Some(source))
    );
    assert!(
        c.world
            .connections
            .values()
            .any(|c| c.owner == Some(other) && c.origin == origin)
    );
}

#[test]
fn validated_nonreusable_mirror_reconnects_without_repeating_equivalence_probe() {
    let mut c = Controller::new();
    let now = Instant::now();
    let (job, _, source, origin) = job_with_pending_mirror(&mut c, now);
    c.world
        .note_endpoints(origin, &["127.0.0.1:8082".parse().unwrap()]);
    let endpoint = c.world.origins[origin].endpoints[0];
    let mirror = c
        .dial_connection(
            Some(job),
            &mut Vec::new(),
            origin,
            endpoint,
            0,
            false,
            Some(source),
            now,
        )
        .unwrap();
    c.handle(
        Observation::ConnectionReady {
            connection: mirror,
            handshake: Duration::from_millis(1),
            protocol: crate::Protocol::Http1_1,
        },
        now,
    );
    c.world.jobs[job].sampling_sources.push(source);
    c.handle(
        Observation::MirrorSampled {
            job,
            source,
            connection: mirror,
            matches: true,
            reusable: false,
        },
        now,
    );
    assert!(!c.world.connections.contains_key(mirror));
    assert!(c.world.jobs[job].active_sources.contains(&source));
    c.handle(Observation::ConnectionGone { connection: mirror }, now);
    let replacement = c
        .world
        .connections
        .iter()
        .find(|(_, c)| c.serving_source == Some(source))
        .map(|(id, _)| id)
        .unwrap();
    assert_ne!(replacement, mirror);
    let actions = c.handle(
        Observation::ConnectionReady {
            connection: replacement,
            handshake: Duration::from_millis(1),
            protocol: crate::Protocol::Http1_1,
        },
        now + Duration::from_secs(3),
    );
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::Probe { connection, .. } if *connection == replacement))
    );
}

#[test]
fn global_headroom_prevents_futile_exploration_without_rewriting_user_limits() {
    let mut c = Controller::new();
    c.set_engine_limits(&crate::EngineLimits {
        max_physical_connections: 1,
        max_active_assignments: 1,
        ..Default::default()
    });
    let now = Instant::now();
    let job = admit(&mut c, spec("https://limited.test/a"), "a", now);
    drive_to_transfer(&mut c, job, now, crate::Protocol::Http1_1, 1 << 30);
    let (assignment, _) = active_assignment(&c, job);
    for step in 1..30 {
        c.world.jobs[job]
            .plan
            .as_ref()
            .unwrap()
            .assignment(assignment.assignment)
            .unwrap()
            .control
            .publish(step * (5 << 20), Duration::ZERO);
        c.handle(
            Observation::TimerExpired {
                event: TimerEvent::AdaptiveTick { job },
            },
            now + Duration::from_millis(step * 750),
        );
        assert_eq!(
            c.world.jobs[job].planner.desired(),
            crate::Topology::new(1, 1)
        );
    }
    assert_eq!(
        c.world.jobs[job].policy.transport.max_physical_connections,
        8
    );
}

#[test]
fn independent_jobs_with_spare_capacity_do_not_force_byte_quanta() {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("https://one.test/a"), "a", now);
    drive_to_transfer(&mut c, job, now, crate::Protocol::Http1_1, 64 << 20);
    let (assignment, range) = active_assignment(&c, job);
    admit(&mut c, spec("https://two.test/a"), "b", now);
    assert!(!c.contended);
    let a = c.world.jobs[job]
        .plan
        .as_ref()
        .unwrap()
        .assignment(assignment.assignment)
        .unwrap();
    assert_eq!(
        a.control.end.load(std::sync::atomic::Ordering::Relaxed),
        range.end
    );
    assert!(!a.truncated);
}

#[rstest::rstest]
#[case(crate::Topology::new(1, 2))]
#[case(crate::Topology::new(2, 1))]
fn adaptive_windows_wait_for_retired_topology_to_drain(#[case] topology: crate::Topology) {
    let mut c = Controller::new();
    let now = Instant::now();
    let mut s = spec("https://settling.test/a");
    s.policy.topology = crate::TopologyPolicy::Fixed(topology);
    let job = admit(&mut c, s, "a", now);
    drive_to_transfer(&mut c, job, now, crate::Protocol::Http2, 1 << 30);
    let opening: Vec<_> = c
        .world
        .connections
        .iter()
        .filter(|(_, c)| c.status == ConnectionStatus::Connecting)
        .map(|(id, _)| id)
        .collect();
    for connection in opening {
        c.handle(
            Observation::ConnectionReady {
                connection,
                protocol: crate::Protocol::Http2,
                handshake: Duration::from_millis(1),
            },
            now,
        );
    }
    assert_eq!(c.world.jobs[job].plan.as_ref().unwrap().in_flight(), 2);
    let j = &mut c.world.jobs[job];
    j.planner =
        crate::core::planner::JobPlanner::new(crate::TopologyPolicy::Adaptive, j.policy.transport);
    j.grant.connections = 1;
    j.grant.assignments = 1;
    j.planner_due = None;
    c.reconcile(&mut Vec::new(), now);
    let plan = c.world.jobs[job].plan.as_ref().unwrap();
    assert_eq!(
        plan.in_flight(),
        2,
        "retirement must retain both claims until acknowledgement"
    );
    assert_eq!(
        plan.iter_assignments().filter(|(_, a)| a.truncated).count(),
        0
    );
    assert!(
        plan.iter_assignments().all(|(_, a)| a
            .control
            .end
            .load(std::sync::atomic::Ordering::Relaxed)
            == a.range.end)
    );
    for step in 1..20 {
        for (_, assignment) in c.world.jobs[job].plan.as_ref().unwrap().iter_assignments() {
            assignment.control.publish(step * (5 << 20), Duration::ZERO);
        }
        c.adaptive_tick(
            &mut Vec::new(),
            job,
            now + Duration::from_millis(step * 750),
        );
    }
    assert_eq!(
        c.world.jobs[job].planner.desired(),
        crate::Topology::new(1, 1)
    );
    let assignments: Vec<_> = c.world.jobs[job]
        .plan
        .as_ref()
        .unwrap()
        .iter_assignments()
        .map(|(id, a)| (c.assignment_ref(job, id), a.connection.unwrap(), a.range))
        .collect();
    for (index, (assignment, connection, range)) in assignments.into_iter().enumerate() {
        let actions = c.handle(
            Observation::AssignmentVerified {
                job,
                assignment,
                connection,
                range,
                sample: sample(range.len(), 1000, 0),
                connection_reusable: true,
            },
            now + Duration::from_secs(15),
        );
        assert!(
            !actions
                .iter()
                .any(|a| matches!(a, Action::TruncateAssignment { .. }))
        );
        if index == 0 {
            assert!(
                !actions
                    .iter()
                    .any(|a| matches!(a, Action::StartAssignment { .. }))
            );
        }
        assert_eq!(c.world.jobs[job].plan.as_ref().unwrap().in_flight(), 1);
    }
}

#[rstest::rstest]
#[case(crate::Protocol::Http1_1)]
#[case(crate::Protocol::Http2)]
fn cold_topology_is_granted_before_first_claim(#[case] protocol: crate::Protocol) {
    let mut c = Controller::new();
    let now = Instant::now();
    let job = admit(&mut c, spec("http://cold.test/a"), "cold", now);
    drive_to_transfer(&mut c, job, now, protocol, 128 << 20);
    if protocol == crate::Protocol::Http1_1 {
        let connection = c
            .world
            .connections
            .iter()
            .find(|(_, c)| c.status == ConnectionStatus::Connecting)
            .unwrap()
            .0;
        c.handle(
            Observation::ConnectionReady {
                connection,
                protocol,
                handshake: Duration::from_millis(1),
            },
            now,
        );
    }
    let plan = c.world.jobs[job].plan.as_ref().unwrap();
    assert_eq!(
        plan.in_flight(),
        if protocol == crate::Protocol::Http1_1 {
            2
        } else {
            1
        }
    );
    assert!(plan.iter_assignments().all(|(_, a)| !a.truncated));
    let mut ranges: Vec<_> = plan.iter_assignments().map(|(_, a)| a.range).collect();
    ranges.sort_by_key(|r| r.start);
    assert_eq!(
        ranges,
        if protocol == crate::Protocol::Http1_1 {
            vec![
                ByteRange::new(0, 8 << 20),
                ByteRange::new(8 << 20, 16 << 20),
            ]
        } else {
            vec![ByteRange::new(0, 32 << 20)]
        }
    );
}

#[test]
fn growing_a_solo_claim_uses_grants_and_acknowledged_range_yield() {
    let mut c = Controller::new();
    let now = Instant::now();
    let mut s = spec("http://capped.test/a");
    s.policy.http_version = crate::HttpVersionPolicy::Http1Only;
    s.policy.topology = crate::TopologyPolicy::Fixed(crate::Topology::new(1, 1));
    s.policy.transport = crate::TransportLimits {
        max_physical_connections: 4,
        max_streams_per_connection: 1,
        max_active_assignments: 4,
    };
    let job = admit(&mut c, s, "a", now);
    drive_to_transfer(&mut c, job, now, crate::Protocol::Http1_1, 128 << 20);
    c.world.jobs[job].planner = crate::core::planner::JobPlanner::new(
        crate::TopologyPolicy::Fixed(crate::Topology::new(2, 1)),
        c.world.jobs[job].policy.transport,
    );
    let mut effects = Vec::new();
    c.reconcile(&mut effects, now);
    let (assignment, _) = active_assignment(&c, job);
    for step in 1..=4 {
        c.world.jobs[job]
            .plan
            .as_ref()
            .unwrap()
            .assignment(assignment.assignment)
            .unwrap()
            .control
            .publish(step * (6 << 20), Duration::ZERO);
        effects.extend(c.handle(
            Observation::TimerExpired {
                event: TimerEvent::AdaptiveTick { job },
            },
            now + Duration::from_millis(step * 750),
        ));
    }
    assert_eq!(
        c.world.jobs[job].planner.desired(),
        crate::Topology::new(2, 1)
    );
    assert_eq!(c.world.jobs[job].grant.connections, 2);
    assert!(
        effects
            .iter()
            .any(|effect| matches!(effect, Action::OpenConnection { .. }))
    );
    let connection = c
        .world
        .connections
        .iter()
        .find(|(_, c)| c.status == ConnectionStatus::Connecting)
        .map(|(id, _)| id)
        .unwrap();
    let actions = c.handle(
        Observation::ConnectionReady {
            connection,
            protocol: crate::Protocol::Http1_1,
            handshake: Duration::from_millis(1),
        },
        now + Duration::from_secs(3),
    );
    let (victim_connection, new_end) = actions
        .iter()
        .find_map(|a| match a {
            Action::TruncateAssignment {
                connection,
                new_end,
                ..
            } => Some((*connection, *new_end)),
            _ => None,
        })
        .expect("incumbent must yield before its tail is reassigned");
    assert!(
        !actions
            .iter()
            .any(|a| matches!(a, Action::StartAssignment { .. }))
    );
    assert_eq!(c.world.jobs[job].plan.as_ref().unwrap().in_flight(), 1);
    assert_eq!(
        c.world.jobs[job]
            .plan
            .as_ref()
            .unwrap()
            .assignment(assignment.assignment)
            .unwrap()
            .range
            .end,
        128 << 20
    );
    // A pending destination operation may finish beyond the requested cut.
    let acknowledged_end = new_end + (64 << 10);
    let actions = c.handle(
        Observation::AssignmentVerified {
            job,
            assignment,
            range: ByteRange::new(0, acknowledged_end),
            sample: sample(acknowledged_end, 3100, 0),
            connection: victim_connection,
            connection_reusable: false,
        },
        now + Duration::from_millis(3100),
    );
    assert!(actions.iter().any(|a| matches!(a, Action::StartAssignment { connection: id, range, .. } if *id == connection && range.start == acknowledged_end)));
    c.handle(
        Observation::ConnectionGone {
            connection: victim_connection,
        },
        now + Duration::from_millis(3101),
    );
    let replacement = c
        .world
        .connections
        .iter()
        .find(|(_, c)| c.status == ConnectionStatus::Connecting)
        .map(|(id, _)| id)
        .unwrap();
    c.handle(
        Observation::ConnectionReady {
            connection: replacement,
            protocol: crate::Protocol::Http1_1,
            handshake: Duration::from_millis(1),
        },
        now + Duration::from_millis(3102),
    );
    assert_eq!(c.world.jobs[job].plan.as_ref().unwrap().in_flight(), 2);
}
