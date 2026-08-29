use crate::core::{Error, Result};
use event_listener::Event;
use futures_util::{
    FutureExt,
    future::{Either, select},
};
use std::{
    future::Future,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Instant,
};

const ACTIVE: u8 = 0;
const CANCELLED: u8 = 1;
const EXPIRED: u8 = 2;
const FINISHED: u8 = 3;

#[derive(Debug, Clone)]
pub struct JobContext {
    inner: Arc<Inner>,
    deadline: Option<Instant>,
    durability: crate::core::spec::Durability,
}

#[derive(Debug)]
struct Inner {
    state: AtomicU8,
    wake: Event,
}

impl JobContext {
    pub fn new(deadline: Option<Instant>) -> Self {
        Self::with_durability(deadline, crate::core::spec::Durability::default())
    }

    pub fn with_durability(
        deadline: Option<Instant>,
        durability: crate::core::spec::Durability,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                state: AtomicU8::new(ACTIVE),
                wake: Event::new(),
            }),
            deadline,
            durability,
        }
    }
    /// The job's persistence requirement - fetch tasks use this to decide
    /// whether per-piece fsync is required.
    pub fn durability(&self) -> crate::core::spec::Durability {
        self.durability
    }
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
    pub fn cancel(&self) {
        self.set_state(CANCELLED);
    }
    pub fn expire(&self) {
        self.set_state(EXPIRED);
    }
    pub fn finish(&self) {
        self.set_state(FINISHED);
    }
    fn set_state(&self, state: u8) {
        if self
            .inner
            .state
            .compare_exchange(ACTIVE, state, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            self.inner.wake.notify(usize::MAX);
        }
    }
    pub fn is_cancelled(&self) -> bool {
        matches!(
            self.inner.state.load(Ordering::Acquire),
            CANCELLED | EXPIRED
        )
    }
    pub fn check(&self) -> Result<()> {
        match self.inner.state.load(Ordering::Acquire) {
            ACTIVE | FINISHED => Ok(()),
            EXPIRED => Err(Error::DeadlineExceeded),
            _ => Err(Error::Cancelled),
        }
    }
    pub async fn state_changed(&self) {
        loop {
            let listener = self.inner.wake.listen();
            if self.inner.state.load(Ordering::Acquire) != ACTIVE {
                return;
            }
            listener.await;
        }
    }
    pub async fn cancelled(&self) -> Error {
        loop {
            let listener = self.inner.wake.listen();
            if let Err(error) = self.check() {
                return error;
            }
            listener.await;
        }
    }
    pub async fn run<T>(&self, future: impl Future<Output = Result<T>>) -> Result<T> {
        self.check()?;
        let operation = future.fuse();
        let cancelled = self.cancelled().fuse();
        futures_util::pin_mut!(operation, cancelled);
        match select(operation, cancelled).await {
            Either::Left((result, _)) => result,
            Either::Right((error, _)) => Err(error),
        }
    }
}
