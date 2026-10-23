//! Atomic byte-and-operation admission shared by all lanes of a destination.

use crate::core::error::{Error, Result};
use event_listener::Event;
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

#[derive(Debug, Default)]
struct Inflight {
    bytes: u64,
    operations: usize,
}

#[derive(Debug)]
pub struct CapacityCredits {
    max_inflight_bytes: u64,
    max_inflight_ops: usize,
    inflight: Mutex<Inflight>,
    available: Event,
}

impl CapacityCredits {
    pub fn new(max_inflight_bytes: u64, max_inflight_ops: usize) -> Arc<Self> {
        Arc::new(Self {
            max_inflight_bytes: max_inflight_bytes.max(1),
            max_inflight_ops: max_inflight_ops.max(1),
            inflight: Mutex::new(Inflight::default()),
            available: Event::new(),
        })
    }
    pub fn max_inflight_bytes(&self) -> u64 {
        self.max_inflight_bytes
    }
}

pub struct CapacityGuard {
    credits: Arc<CapacityCredits>,
    bytes: u64,
    waited: std::time::Duration,
}

impl CapacityGuard {
    pub fn waited(&self) -> std::time::Duration {
        self.waited
    }
    pub async fn acquire(credits: &Arc<CapacityCredits>, len: u64) -> Result<Self> {
        if len > credits.max_inflight_bytes {
            return Err(Error::destination(
                "write exceeds destination byte capacity",
            ));
        }
        let mut waited = std::time::Duration::ZERO;
        loop {
            // Register before checking state so a concurrent release cannot be lost.
            let listener = credits.available.listen();
            {
                let mut state = credits.inflight.lock();
                if state.operations < credits.max_inflight_ops
                    && len <= credits.max_inflight_bytes - state.bytes
                {
                    state.bytes += len;
                    state.operations += 1;
                    return Ok(Self {
                        credits: credits.clone(),
                        bytes: len,
                        waited,
                    });
                }
            }
            let start = std::time::Instant::now();
            listener.await;
            waited += start.elapsed();
        }
    }
}

impl Drop for CapacityGuard {
    fn drop(&mut self) {
        let mut state = self.credits.inflight.lock();
        state.bytes -= self.bytes;
        state.operations -= 1;
        drop(state);
        self.credits.available.notify(usize::MAX);
    }
}

#[derive(Debug)]
pub struct DestinationCapacityRegistry {
    entries: Mutex<HashMap<String, Weak<CapacityCredits>>>,
    defaults: (u64, usize),
}

impl DestinationCapacityRegistry {
    pub fn new(max_inflight_bytes: u64, max_inflight_ops: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            defaults: (max_inflight_bytes, max_inflight_ops),
        }
    }
    pub fn entry(&self, key: &str) -> Arc<CapacityCredits> {
        self.entry_with_hints(
            key,
            crate::core::sink::DestinationHints {
                max_inflight_bytes: self.defaults.0,
                max_parallel_writes: self.defaults.1.min(u16::MAX as usize) as u16,
                ..Default::default()
            },
        )
    }
    pub fn entry_with_hints(
        &self,
        key: &str,
        hints: crate::core::sink::DestinationHints,
    ) -> Arc<CapacityCredits> {
        let mut entries = self.entries.lock();
        if let Some(existing) = entries.get(key).and_then(Weak::upgrade) {
            return existing;
        }
        entries.retain(|_, credits| credits.strong_count() != 0);
        let credits = CapacityCredits::new(
            hints.max_inflight_bytes.min(self.defaults.0),
            usize::from(hints.max_parallel_writes).min(self.defaults.1),
        );
        entries.insert(key.to_owned(), Arc::downgrade(&credits));
        credits
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{FutureExt, pin_mut};

    #[test]
    fn waiting_and_cancellation_never_reserve_partial_capacity() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let credits = CapacityCredits::new(100, 2);
            let first = CapacityGuard::acquire(&credits, 60).await.unwrap();
            assert_eq!(first.waited(), std::time::Duration::ZERO);
            let waiting = CapacityGuard::acquire(&credits, 60);
            pin_mut!(waiting);
            assert!(waiting.as_mut().now_or_never().is_none());
            // The blocked 60-byte request holds neither bytes nor an operation.
            let second = CapacityGuard::acquire(&credits, 40).await.unwrap();
            assert_eq!(credits.inflight.lock().bytes, 100);
            drop(second);
            drop(first);
            let next = waiting.await.unwrap();
            assert!(next.waited() > std::time::Duration::ZERO);
            assert_eq!(credits.inflight.lock().bytes, 60);
            drop(next);
            let held = CapacityGuard::acquire(&credits, 100).await.unwrap();
            assert!(CapacityGuard::acquire(&credits, 1).now_or_never().is_none());
            drop(held);
            let all = CapacityGuard::acquire(&credits, 100).await.unwrap();
            assert_eq!(credits.inflight.lock().operations, 1);
            drop(all);
        });
    }

    #[test]
    fn cancelling_a_credit_waiter_leaves_only_submitted_work_reserved() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let credits = CapacityCredits::new(100, 1);
            let submitted = CapacityGuard::acquire(&credits, 100).await.unwrap();
            let context = crate::core::context::JobContext::new(None);
            let waiter = context.run(CapacityGuard::acquire(&credits, 1));
            pin_mut!(waiter);
            assert!(waiter.as_mut().now_or_never().is_none());
            context.cancel();
            assert!(matches!(waiter.await, Err(Error::Cancelled)));
            assert_eq!(credits.inflight.lock().bytes, 100);
            drop(submitted);
            assert_eq!(credits.inflight.lock().bytes, 0);
        });
    }

    #[test]
    fn oversized_writes_fail_without_consuming_capacity() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let credits = CapacityCredits::new(100, 1);
            assert!(CapacityGuard::acquire(&credits, 101).await.is_err());
            assert_eq!(credits.inflight.lock().bytes, 0);
            assert_eq!(credits.inflight.lock().operations, 0);
        });
    }

    #[test]
    fn registry_shares_live_pools_and_reclaims_retired_destinations() {
        let registry = DestinationCapacityRegistry::new(100, 2);
        let a = registry.entry("a");
        assert!(Arc::ptr_eq(&a, &registry.entry("a")));
        drop(a);
        let _b = registry.entry("b");
        assert_eq!(registry.entries.lock().len(), 1);
    }
}
