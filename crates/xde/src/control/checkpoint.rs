use super::*;
use crate::storage::journal::JournalCheckpoint;

#[derive(Default)]
pub(super) struct Checkpoints {
    revision: u64,
    saved: u64,
    sequence: u64,
    phase: Phase,
    waiting: VecDeque<Action>,
}

#[derive(Default)]
enum Phase {
    #[default]
    Idle,
    Scheduled(Instant),
    Writing {
        id: u64,
        revision: u64,
        purpose: Purpose,
    },
}

enum Purpose {
    Progress,
    Reset,
    Drain,
}

pub(crate) enum Work {
    File {
        part: PathBuf,
        snapshot: JournalCheckpoint,
    },
    Custom(Arc<dyn crate::storage::DynDestination + Send + Sync>),
}

pub(crate) struct Completed {
    pub job: JobId,
    pub id: u64,
    pub result: Result<Option<Box<JournalPayload>>>,
}

impl Work {
    pub(crate) async fn run(self: Arc<Self>) -> Result<Option<Box<JournalPayload>>> {
        match self.as_ref() {
            Self::File { part, snapshot } => {
                let part = part.clone();
                let snapshot = snapshot.clone();
                compio::runtime::spawn_blocking(move || {
                    snapshot.persist_after_sync(&part).map(Some)
                })
                .await
                .map_err(|_| Error::journal("checkpoint task panicked"))?
            }
            Self::Custom(dest) => {
                dest.flush_dyn(crate::FlushLevel::Data).await?;
                Ok(None)
            }
        }
    }
}

impl Checkpoints {
    fn changed(&mut self) {
        self.revision = self
            .revision
            .checked_add(1)
            .expect("checkpoint revision exhausted");
    }

    fn schedule(&mut self, at: Instant) -> bool {
        if matches!(self.phase, Phase::Idle) {
            self.phase = Phase::Scheduled(at);
            true
        } else {
            false
        }
    }
}

pub(super) fn record(lp: &mut Loop, job: JobId, range: crate::core::ByteRange) {
    let Some(state) = lp.jobs.get_mut(&job) else {
        return;
    };
    if let Some(journal) = state.destination.journal_mut() {
        journal.record_completed(range);
    }
    state.checkpoints.changed();
    match state.context.durability() {
        crate::Durability::CrashSafe => start(lp, job, Purpose::Progress),
        crate::Durability::Periodic(interval) => schedule(lp, job, interval),
        crate::Durability::Relaxed => {}
    }
}

fn schedule(lp: &mut Loop, job: JobId, interval: Duration) {
    let at = Instant::now() + interval;
    if lp
        .jobs
        .get_mut(&job)
        .is_some_and(|state| state.checkpoints.schedule(at))
    {
        lp.timers
            .schedule(at, crate::core::timers::TimerEvent::CheckpointDue(job));
    }
}

pub(super) fn due(lp: &mut Loop, job: JobId, now: Instant) {
    let Some(state) = lp.jobs.get_mut(&job) else {
        return;
    };
    if matches!(state.checkpoints.phase, Phase::Scheduled(at) if at <= now) {
        state.checkpoints.phase = Phase::Idle;
        start(lp, job, Purpose::Progress);
    }
}

// Only lifecycle fences wait. Ordinary transfers continue during a checkpoint.
pub(super) fn defer(lp: &mut Loop, action: &Action) -> bool {
    let job = match action {
        Action::CommitDestination { job, .. }
        | Action::DrainDestination { job }
        | Action::ResetResumeData { job } => *job,
        _ => return false,
    };
    let Some(state) = lp.jobs.get_mut(&job) else {
        return false;
    };
    if matches!(state.checkpoints.phase, Phase::Writing { .. }) {
        state.checkpoints.waiting.push_back(action.clone());
        return true;
    }
    state.checkpoints.phase = Phase::Idle;
    if matches!(action, Action::CommitDestination { .. })
        && state.context.durability() != crate::Durability::Relaxed
        && state.checkpoints.saved != state.checkpoints.revision
    {
        state.checkpoints.waiting.push_back(action.clone());
        start(lp, job, Purpose::Progress);
        return true;
    }
    false
}

pub(super) fn drain(lp: &mut Loop, job: JobId) {
    let Some(state) = lp.jobs.get(&job) else {
        return;
    };
    if state.checkpoints.saved != state.checkpoints.revision
        && (state.destination.journal().is_some()
            || state.context.durability() != crate::Durability::Relaxed)
    {
        start(lp, job, Purpose::Drain);
    } else {
        lp.pending
            .push_back(Observation::DestinationDrained { job });
    }
}

pub(super) fn reset(lp: &mut Loop, job: JobId) {
    let Some(state) = lp.jobs.get_mut(&job) else {
        return;
    };
    if let Some(journal) = state.destination.journal_mut() {
        journal.clear_ranges();
    }
    state.checkpoints.changed();
    start(lp, job, Purpose::Reset);
}

fn start(lp: &mut Loop, job: JobId, purpose: Purpose) {
    let Some(state) = lp.jobs.get_mut(&job) else {
        return;
    };
    if matches!(state.checkpoints.phase, Phase::Writing { .. }) {
        return;
    }
    if matches!(purpose, Purpose::Reset) && state.destination.journal().is_none() {
        state.checkpoints.saved = state.checkpoints.revision;
        lp.pending.push_back(Observation::ResumeDataReset {
            job,
            result: Ok(()),
        });
        return;
    }
    if let Some(journal) = state.destination.journal_mut() {
        sync_fingerprint(lp.controller.world.jobs.get(job), journal);
    }
    let work = match &state.destination {
        DestinationState::File(file) => Work::File {
            part: state.part_path.clone(),
            snapshot: file.journal.checkpoint(),
        },
        DestinationState::Custom(dest) => Work::Custom(dest.clone()),
        DestinationState::Committing => {
            lp.pending.push_back(Observation::DestinationFailed {
                job,
                failure: crate::core::controller::DestinationFailure {
                    kind: crate::core::controller::DestinationFailureKind::DestinationError,
                },
            });
            return;
        }
    };
    state.checkpoints.sequence = state
        .checkpoints
        .sequence
        .checked_add(1)
        .expect("checkpoint sequence exhausted");
    let id = state.checkpoints.sequence;
    state.checkpoints.phase = Phase::Writing {
        id,
        revision: state.checkpoints.revision,
        purpose,
    };
    let work = Arc::new(work);
    let command = DeferredCommand::Checkpoint {
        job,
        id,
        work,
        completed: lp.checkpoint_tx.clone(),
    };
    match lp.shard_txs[0].try_send(command.command()) {
        Ok(()) => {}
        Err(TrySendError::Full(_)) => lp
            .deferred_dispatch
            .push_back(DeferredDispatch { shard: 0, command }),
        Err(TrySendError::Disconnected(_)) => complete(
            lp,
            Completed {
                job,
                id,
                result: Err(Error::journal("checkpoint shard disconnected")),
            },
        ),
    }
}

pub(super) fn complete(lp: &mut Loop, completion: Completed) {
    let Completed { job, id, result } = completion;
    let Some(state) = lp.jobs.get_mut(&job) else {
        return;
    };
    if !matches!(state.checkpoints.phase, Phase::Writing { id: expected, .. } if expected == id) {
        return;
    }
    let Phase::Writing {
        revision, purpose, ..
    } = std::mem::take(&mut state.checkpoints.phase)
    else {
        unreachable!()
    };
    let succeeded = result.is_ok();
    match result {
        Ok(payload) => {
            state.checkpoints.saved = revision;
            if let Some(payload) = payload
                && let Some(journal) = state.destination.journal_mut()
            {
                journal.acknowledge(payload);
            }
            if !matches!(purpose, Purpose::Reset)
                && let Some(journal) = state.destination.journal()
            {
                let bytes_done = journal.payload().durable.covered_len();
                emit(
                    lp,
                    crate::Event::Checkpointed {
                        job,
                        bytes_done,
                        durable: true,
                    },
                );
            }
        }
        Err(error) => tracing::warn!(target: "xde::checkpoint", ?job, %error, "checkpoint failed"),
    }
    if matches!(purpose, Purpose::Drain) {
        lp.pending
            .push_back(Observation::DestinationDrained { job });
        return;
    }
    let waiting = lp
        .jobs
        .get_mut(&job)
        .map(|s| std::mem::take(&mut s.checkpoints.waiting))
        .unwrap_or_default();
    if !succeeded {
        // A requested cancellation/failure remains authoritative after best-effort recovery.
        if waiting
            .iter()
            .any(|action| matches!(action, Action::DrainDestination { .. }))
        {
            lp.pending
                .push_back(Observation::DestinationDrained { job });
        } else {
            let failure = crate::core::controller::DestinationFailure {
                kind: crate::core::controller::DestinationFailureKind::DestinationError,
            };
            let observation = if matches!(purpose, Purpose::Reset) {
                Observation::ResumeDataReset {
                    job,
                    result: Err(failure),
                }
            } else {
                Observation::DestinationFailed { job, failure }
            };
            let actions = lp.controller.handle(observation, Instant::now());
            route_actions(lp, &actions);
        }
        return;
    }
    if matches!(purpose, Purpose::Reset) {
        lp.pending.push_back(Observation::ResumeDataReset {
            job,
            result: Ok(()),
        });
    }
    for action in waiting {
        route_actions(lp, &[action]);
    }
    let active = lp.controller.world.jobs.get(job).is_some_and(|j| {
        matches!(
            j.phase,
            crate::core::world::JobPhase::Transferring | crate::core::world::JobPhase::Paused
        )
    });
    if succeeded
        && active
        && let Some(state) = lp.jobs.get(&job)
        && state.checkpoints.saved != state.checkpoints.revision
    {
        match state.context.durability() {
            crate::Durability::Periodic(interval) => schedule(lp, job, interval),
            crate::Durability::CrashSafe => start(lp, job, Purpose::Progress),
            crate::Durability::Relaxed => {}
        }
    }
}

pub(super) fn failure_error(error: &crate::core::world::JobFailure) -> Error {
    match error {
        crate::core::world::JobFailure::Cancelled => Error::Cancelled,
        crate::core::world::JobFailure::DeadlineExceeded => Error::DeadlineExceeded,
        crate::core::world::JobFailure::Failed(message) => Error::destination(message.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn many_completed_ranges_keep_one_checkpoint_deadline() {
        let mut state = Checkpoints::default();
        let start = Instant::now();
        let mut timers = TimerQueue::new();
        let mut jobs = slotmap::SlotMap::<JobId, ()>::with_key();
        let job = jobs.insert(());
        for n in 0..100 {
            state.changed();
            let at = start + Duration::from_millis(2000 + n);
            if state.schedule(at) {
                timers.schedule(at, crate::core::timers::TimerEvent::CheckpointDue(job));
            }
        }
        assert_eq!(timers.len(), 1);
        assert_eq!(timers.next_deadline(), Some(start + Duration::from_secs(2)));
        assert_eq!(state.revision, 100);
        assert_eq!(state.saved, 0);
    }
}
