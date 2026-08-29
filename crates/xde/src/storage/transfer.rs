use crate::core::{Error, Result, context::JobContext};
use bytes::Bytes;
use compio_buf::IoBuf;
use event_listener::Event;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug)]
struct BudgetInner {
    limit: u64,
    used: AtomicU64,
    available: Event,
    /// Bytes kept out of reach for speculative acquirers so the worker that
    /// advances the artifact frontier can always obtain memory.
    reserve: u64,
}

/// How this acquisition relates to forward progress.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressClass {
    /// Advances the contiguous verified frontier (ordered hashes, sequential
    /// destinations, reorder drains). May consume the reserve.
    Frontier,
    /// Fills gaps out of order. May not dip into the reserve while frontier
    /// workers exist.
    Speculative,
}

#[derive(Debug, Clone)]
pub struct MemoryBudget(Arc<BudgetInner>);

impl MemoryBudget {
    pub fn new(limit: u64) -> Self {
        Self::with_reserve(limit, 0)
    }

    /// Hard ceiling `limit`; `reserve` bytes of it are usable only by
    /// Frontier-class acquisitions.
    pub fn with_reserve(limit: u64, reserve: u64) -> Self {
        let reserve = reserve.min(limit);
        Self(Arc::new(BudgetInner {
            limit: limit.max(1),
            used: AtomicU64::new(0),
            available: Event::new(),
            reserve,
        }))
    }
    pub fn limit(&self) -> u64 {
        self.0.limit
    }
    pub fn used(&self) -> u64 {
        self.0.used.load(Ordering::Acquire)
    }
    pub fn available(&self) -> u64 {
        self.limit().saturating_sub(self.used())
    }
    /// Acquire memory with an explicit forward-progress class. `Frontier`
    /// acquisitions may consume the reserve; speculative ones may not.
    async fn acquire_classed(
        &self,
        bytes: u64,
        class: ProgressClass,
        context: &JobContext,
    ) -> Result<MemoryPermit> {
        if bytes > self.limit() {
            return Err(Error::Config(format!(
                "payload of {bytes} bytes exceeds the {} byte memory budget",
                self.limit()
            )));
        }
        // Speculative acquirers must leave the reserve free while any
        // frontier worker might still need it. A frontier acquirer may use
        // everything. A speculative request larger than the non-reserve area
        // would deadlock forever, so it is upgraded rather than rejected.
        let effective_limit = match class {
            ProgressClass::Frontier => self.limit(),
            ProgressClass::Speculative => {
                let usable = self.limit() - self.0.reserve;
                if bytes > usable {
                    bytes // upgrade oversized speculative requests
                } else {
                    usable
                }
            }
        };
        if bytes > self.limit() {
            return Err(Error::Config(format!(
                "payload of {bytes} bytes exceeds the {} byte memory budget",
                self.limit()
            )));
        }
        context
            .run(async {
                loop {
                    let used = self.used();
                    // Headroom this class may use from here.
                    let headroom = effective_limit.saturating_sub(used);
                    if bytes <= headroom
                        && self
                            .0
                            .used
                            .compare_exchange_weak(
                                used,
                                used + bytes,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_ok()
                    {
                        return Ok(MemoryPermit {
                            budget: self.clone(),
                            bytes,
                        });
                    }
                    let listener = self.0.available.listen();
                    // Re-check after registering, in case space appeared.
                    if effective_limit.saturating_sub(self.used()) < bytes {
                        listener.await;
                    }
                }
            })
            .await
    }
}

#[derive(Debug)]
struct MemoryPermit {
    budget: MemoryBudget,
    bytes: u64,
}
impl Drop for MemoryPermit {
    fn drop(&mut self) {
        self.budget.0.used.fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget.0.available.notify(usize::MAX);
    }
}

#[derive(Debug)]
pub struct BudgetedPayload {
    bytes: Bytes,
    _permit: Arc<MemoryPermit>,
}
impl Clone for BudgetedPayload {
    fn clone(&self) -> Self {
        Self {
            bytes: self.bytes.clone(),
            _permit: self._permit.clone(),
        }
    }
}
impl BudgetedPayload {
    pub fn len(&self) -> usize {
        self.bytes.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
    fn trimmed(mut self, range: std::ops::Range<usize>) -> Self {
        self.bytes = Bytes::slice(&self.bytes, range);
        self
    }
    pub fn trim_prefix(self, bytes: usize) -> Self {
        let len = self.len();
        self.trimmed(bytes..len)
    }

    fn split_at(self, offset: usize) -> (Self, Self) {
        let permit = self._permit;
        (
            Self {
                bytes: Bytes::slice(&self.bytes, ..offset),
                _permit: permit.clone(),
            },
            Self {
                bytes: Bytes::slice(&self.bytes, offset..),
                _permit: permit,
            },
        )
    }
}

impl IoBuf for BudgetedPayload {
    fn as_init(&self) -> &[u8] {
        self.as_slice()
    }
}

#[derive(Debug)]
pub struct TransferChunk {
    offset: u64,
    payload: BudgetedPayload,
}
impl TransferChunk {
    pub async fn bytes(
        offset: u64,
        payload: Bytes,
        budget: &MemoryBudget,
        context: &JobContext,
    ) -> Result<Self> {
        Self::bytes_classed(offset, payload, budget, ProgressClass::Speculative, context).await
    }

    /// Acquire memory with an explicit forward-progress class: `Frontier`
    /// acquisitions may consume the budget's reserve.
    pub async fn bytes_classed(
        offset: u64,
        payload: Bytes,
        budget: &MemoryBudget,
        class: ProgressClass,
        context: &JobContext,
    ) -> Result<Self> {
        let permit = budget
            .acquire_classed(payload.len() as u64, class, context)
            .await?;
        Ok(Self {
            offset,
            payload: BudgetedPayload {
                bytes: payload,
                _permit: Arc::new(permit),
            },
        })
    }
    pub fn offset(&self) -> u64 {
        self.offset
    }
    pub fn len(&self) -> usize {
        self.payload.len()
    }
    pub fn is_empty(&self) -> bool {
        self.payload.is_empty()
    }
    pub fn as_slice(&self) -> &[u8] {
        self.payload.as_slice()
    }
    pub fn into_payload(self) -> BudgetedPayload {
        self.payload
    }
    pub fn retained_payload(&self) -> BudgetedPayload {
        self.payload.clone()
    }
    pub fn into_parts(self) -> (u64, BudgetedPayload) {
        (self.offset, self.payload)
    }
    pub fn trim_prefix(mut self, bytes: usize) -> Self {
        let len = self.len();
        self.payload = self.payload.trimmed(bytes..len);
        self.offset += bytes as u64;
        self
    }

    pub fn split_at(self, bytes: usize) -> (Self, Self) {
        assert!(bytes <= self.len());
        let offset = self.offset;
        let (left, right) = self.payload.split_at(bytes);
        (
            Self {
                offset,
                payload: left,
            },
            Self {
                offset: offset + bytes as u64,
                payload: right,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn permit_follows_payload_after_chunk_is_destructured() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let budget = MemoryBudget::new(8);
            let payload = TransferChunk::bytes(
                0,
                Bytes::from_static(b"1234"),
                &budget,
                &JobContext::new(None),
            )
            .await
            .unwrap()
            .into_payload();
            assert_eq!(budget.used(), 4);
            drop(payload);
            assert_eq!(budget.used(), 0);
        });
    }
    #[test]
    fn oversize_payload_is_rejected_without_underaccounting() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let budget = MemoryBudget::new(4);
            assert!(
                TransferChunk::bytes(
                    0,
                    Bytes::from_static(b"12345"),
                    &budget,
                    &JobContext::new(None)
                )
                .await
                .is_err()
            );
            assert_eq!(budget.used(), 0);
        });
    }

    #[test]
    fn cancellation_wakes_memory_credit_waiter() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            let budget = MemoryBudget::new(4);
            let context = JobContext::new(None);
            let held = TransferChunk::bytes(0, Bytes::from_static(b"1234"), &budget, &context)
                .await
                .unwrap();
            let cancel = context.clone();
            compio::runtime::spawn(async move {
                compio::time::sleep(std::time::Duration::from_millis(1)).await;
                cancel.cancel();
            })
            .detach();
            let result = TransferChunk::bytes(4, Bytes::from_static(b"5"), &budget, &context).await;
            assert!(matches!(result, Err(Error::Cancelled)));
            drop(held);
        });
    }

    #[test]
    fn reserve_is_invisible_to_speculative_and_visible_to_frontier() {
        compio::runtime::Runtime::new().unwrap().block_on(async {
            // 8 bytes total, 2 reserved for the frontier worker.
            let budget = MemoryBudget::with_reserve(8, 2);
            let ctx = JobContext::new(None);

            // Speculative fills up to the non-reserve area (6).
            let s1 = TransferChunk::bytes_classed(
                0,
                Bytes::from_static(b"123456"),
                &budget,
                ProgressClass::Speculative,
                &ctx,
            )
            .await
            .unwrap();
            assert_eq!(budget.used(), 6);

            // Another speculative byte cannot proceed: it would have to dip
            // into the reserve.
            let blocked = TransferChunk::bytes_classed(
                6,
                Bytes::from_static(b"x"),
                &budget,
                ProgressClass::Speculative,
                &ctx,
            );
            let mut blocked = std::pin::pin!(blocked);
            let waker = noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            assert!(
                blocked.as_mut().poll(&mut cx).is_pending(),
                "speculative must not dip into the reserve"
            );

            // Frontier may consume exactly the reserve.
            let f1 = TransferChunk::bytes_classed(
                6,
                Bytes::from_static(b"78"),
                &budget,
                ProgressClass::Frontier,
                &ctx,
            )
            .await
            .unwrap();
            assert_eq!(budget.used(), 8);
            drop(s1);
            drop(f1);
            assert_eq!(budget.used(), 0);
        });
    }

    fn noop_waker() -> std::task::Waker {
        use std::task::{RawWaker, RawWakerVTable, Waker};
        fn noop(_: *const ()) {}
        fn clone(p: *const ()) -> RawWaker {
            RawWaker::new(p, &VTABLE)
        }
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);
        unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
    }
}
