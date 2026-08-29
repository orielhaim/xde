//! Global destination-capacity accounting.
//!
//! A destination advertises ceilings (max inflight bytes, max inflight
//! operations). With N shard lanes writing to the same `.part`, those
//! ceilings must hold *globally*, not per lane. Lanes acquire credits from a
//! shared pool keyed by destination; cancellation is immediate because a
//! permit is only consumed once `recv` returns, and dropping the wait future
//! abandons it cleanly.

use std::{collections::HashMap, sync::Arc};

use flume::{Receiver, Sender};
use parking_lot::Mutex;

/// Permit granularity. Byte-exact semaphores would need millions of permits;
/// 256 KiB units keep channel traffic negligible while bounding over-commit
/// to less than one unit per waiter.
const BYTE_UNIT: u64 = 256 * 1024;

fn units_for(bytes: u64, total_units: u64) -> u64 {
    if bytes == 0 {
        return 0;
    }
    // Never request more than exists: an oversized operation proceeds at full
    // capacity instead of deadlocking.
    let needed = bytes.div_ceil(BYTE_UNIT);
    needed.min(total_units).max(1)
}

/// Async counting semaphore built on flume channels: acquire = recv,
/// release = send. Cancellation-safe by construction.
#[derive(Debug)]
struct Permits {
    tx: Sender<()>,
    rx: Receiver<()>,
}

impl Permits {
    fn new(count: u64) -> Self {
        let (tx, rx) = flume::bounded(count as usize);
        for _ in 0..count {
            let _ = tx.send(());
        }
        Self { tx, rx }
    }

    async fn acquire(&self, n: u64) {
        for _ in 0..n {
            // Dropping this future mid-wait cancels without leaking a permit.
            let _ = self.rx.recv_async().await;
        }
    }

    fn release(&self, n: u64) {
        for _ in 0..n {
            let _ = self.tx.try_send(());
        }
    }

    #[cfg(test)]
    fn available(&self) -> usize {
        self.rx.len()
    }
}

/// The shared credit pool for one destination.
#[derive(Debug)]
pub struct CapacityCredits {
    max_inflight_bytes: u64,
    byte_units: u64,
    bytes: Permits,
    ops: Permits,
}

impl CapacityCredits {
    pub fn new(max_inflight_bytes: u64, max_inflight_ops: usize) -> Arc<Self> {
        let byte_units = (max_inflight_bytes / BYTE_UNIT).max(1);
        Arc::new(Self {
            max_inflight_bytes,
            byte_units,
            bytes: Permits::new(byte_units),
            ops: Permits::new(max_inflight_ops.max(1) as u64),
        })
    }

    pub fn max_inflight_bytes(&self) -> u64 {
        self.max_inflight_bytes
    }
}

/// One acquired slice of global capacity. Release happens on drop, so error
/// and cancel paths cannot strand credits.
pub struct CapacityGuard {
    credits: Arc<CapacityCredits>,
    byte_units_held: u64,
    ops_held: bool,
}

impl CapacityGuard {
    /// Acquire capacity for one operation of `len` bytes. An operation larger
    /// than the whole ceiling clamps to the ceiling rather than deadlocking.
    pub async fn acquire(credits: &Arc<CapacityCredits>, len: u64) -> CapacityGuard {
        let units = units_for(len, credits.byte_units);
        credits.ops.acquire(1).await;
        credits.bytes.acquire(units).await;
        CapacityGuard {
            credits: credits.clone(),
            byte_units_held: units,
            ops_held: true,
        }
    }
}

impl Drop for CapacityGuard {
    fn drop(&mut self) {
        self.credits.bytes.release(self.byte_units_held);
        if self.ops_held {
            self.credits.ops.release(1);
        }
    }
}

/// Registry mapping a destination key (the `.part` path) to its shared pool.
/// All shard services share one registry; lanes on different shards calling
/// with the same path get the same credits.
#[derive(Debug, Default)]
pub struct DestinationCapacityRegistry {
    entries: Mutex<HashMap<String, Arc<CapacityCredits>>>,
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
        let mut entries = self.entries.lock();
        entries
            .entry(key.to_owned())
            .or_insert_with(|| CapacityCredits::new(self.defaults.0.max(1), self.defaults.1))
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        sync::atomic::{AtomicBool, Ordering},
        time::Duration,
    };

    fn block_on<F: Future>(f: F) -> F::Output {
        compio::runtime::Runtime::new().unwrap().block_on(f)
    }

    #[test]
    fn two_lanes_share_one_byte_ceiling() {
        block_on(async {
            let credits = CapacityCredits::new(2 * BYTE_UNIT, 8); // 512 KiB ceiling
            let g1 = CapacityGuard::acquire(&credits, BYTE_UNIT).await;
            let g2 = CapacityGuard::acquire(&credits, BYTE_UNIT).await;
            assert_eq!(credits.bytes.available(), 0, "ceiling reached");
            drop(g1);
            drop(g2);
            assert_eq!(credits.bytes.available(), 2, "released");
        });
    }

    #[test]
    fn operation_count_is_globally_bounded() {
        block_on(async {
            let credits = CapacityCredits::new(64 * BYTE_UNIT, 2);
            let g1 = CapacityGuard::acquire(&credits, 1024).await;
            let g2 = CapacityGuard::acquire(&credits, 1024).await;
            assert_eq!(credits.ops.available(), 0);
            drop(g1);
            drop(g2);
            assert_eq!(credits.ops.available(), 2);
        });
    }

    #[test]
    fn oversized_operation_clamps_instead_of_deadlocking() {
        block_on(async {
            let credits = CapacityCredits::new(BYTE_UNIT, 4); // 256 KiB ceiling
            let guard = CapacityGuard::acquire(&credits, 16 * BYTE_UNIT).await;
            assert_eq!(guard.byte_units_held, 1, "clamped to ceiling");
            drop(guard);
            assert_eq!(credits.bytes.available(), 1);
        });
    }

    #[test]
    fn waiter_proceeds_when_capacity_is_released() {
        block_on(async {
            let credits = CapacityCredits::new(BYTE_UNIT, 1);
            let held = CapacityGuard::acquire(&credits, 1024).await;
            let done = Arc::new(AtomicBool::new(false));
            let flag = done.clone();
            compio::runtime::spawn(async move {
                let _g = CapacityGuard::acquire(&credits, 1024).await;
                flag.store(true, Ordering::Release);
            })
            .detach();
            compio::time::sleep(Duration::from_millis(50)).await;
            assert!(!done.load(Ordering::Acquire), "must wait while saturated");
            drop(held);
            compio::time::sleep(Duration::from_millis(50)).await;
            assert!(done.load(Ordering::Acquire), "release must wake the waiter");
        });
    }

    #[test]
    fn registry_returns_same_pool_for_same_destination() {
        let reg = DestinationCapacityRegistry::new(64 * 1024 * 1024, 8);
        let a = reg.entry("C:\\x\\a.part");
        let b = reg.entry("C:\\x\\a.part");
        let c = reg.entry("C:\\x\\b.part");
        assert!(Arc::ptr_eq(&a, &b));
        assert!(!Arc::ptr_eq(&a, &c));
    }
}
