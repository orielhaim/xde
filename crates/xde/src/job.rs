use std::time::{Duration, Instant};

use crate::core::{Error, Result, ids::JobId};

use crate::control::{ControlHandle, JobOutcome};
use crate::snapshot::{JobPhase, JobSnapshot};

/// A running transfer. Dropping the handle does not cancel the job; call
/// [`Job::cancel`] to cancel it.
#[derive(Debug)]
pub struct Job {
    id: JobId,
    result: flume::Receiver<Result<JobOutcome>>,
    control: ControlHandle,
    cancelled: std::sync::atomic::AtomicBool,
}

impl Job {
    pub(crate) fn new(
        id: JobId,
        result: flume::Receiver<Result<JobOutcome>>,
        control: ControlHandle,
    ) -> Self {
        Self {
            id,
            result,
            control,
            cancelled: std::sync::atomic::AtomicBool::new(false),
        }
    }

    pub fn id(&self) -> JobId {
        self.id
    }

    /// Compact current-state projection. `None` when the job has already
    /// finished and been removed from the engine.
    pub fn snapshot(&self) -> crate::core::Result<crate::snapshot::JobSnapshot> {
        self.control
            .job_snapshot(self.id)
            .ok_or_else(|| Error::Runtime(crate::core::RuntimeError::EngineGone))
    }

    /// Request cancellation. The outcome resolves with `Error::Cancelled`
    /// once in-flight writes have reached a terminal state.
    pub fn cancel(&self) {
        if self
            .cancelled
            .swap(true, std::sync::atomic::Ordering::AcqRel)
        {
            return;
        }
        let _ = self.control.cancel(self.id);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(std::sync::atomic::Ordering::Acquire)
    }

    pub async fn wait(self) -> Result<JobOutcome> {
        self.result
            .recv_async()
            .await
            .map_err(|_| Error::Runtime(crate::core::RuntimeError::EngineGone))?
    }

    pub fn wait_blocking(self) -> Result<JobOutcome> {
        self.result
            .recv()
            .map_err(|_| Error::Runtime(crate::core::RuntimeError::EngineGone))?
    }

    /// Wait until the job finishes, failing if verified progress (or phase /
    /// concurrency) is unchanged for `stall` or the wait exceeds `overall`.
    #[allow(clippy::single_match)]
    pub fn wait_blocking_progressing(
        self,
        stall: Duration,
        overall: Duration,
    ) -> Result<JobOutcome> {
        let started = Instant::now();
        let mut last_move = Instant::now();
        let mut sig = ProgressSig::default();
        loop {
            if started.elapsed() > overall {
                return Err(Error::Stalled {
                    reason: format!(
                        "overall timeout after {:?}; last={sig:?} snap={:?}",
                        started.elapsed(),
                        self.snapshot().ok()
                    ),
                });
            }
            match self.result.recv_timeout(Duration::from_millis(25)) {
                Ok(outcome) => return outcome,
                Err(flume::RecvTimeoutError::Disconnected) => {
                    return Err(Error::Runtime(crate::core::RuntimeError::EngineGone));
                }
                Err(flume::RecvTimeoutError::Timeout) => match self.snapshot() {
                    Ok(snap) => {
                        let next = ProgressSig::from(&snap);
                        if next != sig {
                            sig = next;
                            last_move = Instant::now();
                        } else if last_move.elapsed() > stall {
                            return Err(Error::Stalled {
                                reason: format!("no progress for {stall:?}: {snap:?}"),
                            });
                        }
                    }
                    Err(_) => {}
                },
            }
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct ProgressSig {
    phase: Option<JobPhase>,
    verified: u64,
    connections: usize,
    streams: usize,
    total: Option<u64>,
}

impl From<&JobSnapshot> for ProgressSig {
    fn from(s: &JobSnapshot) -> Self {
        Self {
            phase: Some(s.phase),
            verified: s.verified_bytes,
            connections: s.active_connections,
            streams: s.active_streams,
            total: s.total_length,
        }
    }
}
