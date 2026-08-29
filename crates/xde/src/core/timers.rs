use std::{cmp::Ordering, collections::BinaryHeap, time::Instant};

use crate::core::{
    ids::{AssignmentRef, JobId, OriginId},
    ranges::ByteRange,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimerEvent {
    JobDeadline(JobId),
    RetryReady {
        assignment: AssignmentRef,
        range: ByteRange,
        attempt: u32,
    },
    /// DNS resolution failed; try again after backoff.
    ResolveRetry {
        origin: OriginId,
        host: String,
        port: u16,
    },
    OriginCooldownExpired(OriginId),
    CheckpointDue(JobId),
    /// Happy-Eyeballs stagger: try the endpoint at `rank` (0 = best) for
    /// this origin if no connection has become ready yet.
    EndpointStagger {
        origin: OriginId,
        rank: usize,
    },
    /// Adaptive topology tick for one origin: measure aggregate rate since
    /// last tick and decide whether to open/close connections.
    AdaptiveTick {
        origin: OriginId,
    },
    /// A retired connection still has in-flight assignments; check again
    /// whether it has drained and can be closed without aborting work.
    DrainConnection {
        connection: crate::core::ids::ConnectionId,
    },
    /// Medium-timescale rebalance for one origin: detect stragglers, shrink
    /// them to a fair share, and let fast workers pick up the released
    /// tails.
    RebalanceTick {
        origin: OriginId,
    },
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct ScheduledItem {
    at: Instant,
    seq: u64,
    event: TimerEvent,
}

impl Ord for ScheduledItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Earliest instant has highest priority (reverse order for min-heap)
        other
            .at
            .cmp(&self.at)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for ScheduledItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Default)]
pub struct TimerQueue {
    heap: BinaryHeap<ScheduledItem>,
    next_seq: u64,
}

impl TimerQueue {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn schedule(&mut self, at: Instant, event: TimerEvent) {
        let seq = self.next_seq;
        self.next_seq += 1;
        self.heap.push(ScheduledItem { at, seq, event });
    }

    pub fn next_deadline(&self) -> Option<Instant> {
        self.heap.peek().map(|item| item.at)
    }

    pub fn pop_expired(&mut self, now: Instant) -> Option<TimerEvent> {
        if let Some(item) = self.heap.peek()
            && item.at <= now
        {
            return self.heap.pop().map(|i| i.event);
        }
        None
    }

    pub fn drain_expired(&mut self, now: Instant) -> Vec<TimerEvent> {
        let mut expired = Vec::new();
        while let Some(event) = self.pop_expired(now) {
            expired.push(event);
        }
        expired
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    pub fn len(&self) -> usize {
        self.heap.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slotmap::SlotMap;
    use std::time::Duration;

    #[test]
    fn timer_queue_pops_in_temporal_order() {
        let mut tq = TimerQueue::new();
        let now = Instant::now();
        let mut jobs = SlotMap::<JobId, ()>::with_key();
        let j1 = jobs.insert(());
        let j2 = jobs.insert(());
        let j3 = jobs.insert(());

        tq.schedule(
            now + Duration::from_millis(100),
            TimerEvent::JobDeadline(j2),
        );
        tq.schedule(now + Duration::from_millis(50), TimerEvent::JobDeadline(j1));
        tq.schedule(
            now + Duration::from_millis(200),
            TimerEvent::JobDeadline(j3),
        );

        assert_eq!(tq.pop_expired(now), None);
        assert_eq!(
            tq.pop_expired(now + Duration::from_millis(60)),
            Some(TimerEvent::JobDeadline(j1))
        );
        assert_eq!(
            tq.pop_expired(now + Duration::from_millis(150)),
            Some(TimerEvent::JobDeadline(j2))
        );
        assert_eq!(
            tq.pop_expired(now + Duration::from_millis(250)),
            Some(TimerEvent::JobDeadline(j3))
        );
    }
}
