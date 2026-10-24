//! Range ownership with acknowledged request yields.
//!
//! A running assignment retains its complete claim until its terminal observation.
//! Tail work becomes claimable only after pending destination operations drain.

use std::time::{Duration, Instant};

use slotmap::SlotMap;
use smallvec::SmallVec;

use crate::core::{
    ewma::EwmaWithVariance,
    ids::{AssignId, ConnectionId},
    policy::SegmentationPolicy,
    ranges::{ByteRange, RangeSet},
    units::Rate,
};

/// One writer publishes coarse consumed progress; the controller samples it.
/// The end boundary is independently updated by range retirement.
#[derive(Debug)]
pub struct AssignmentControl {
    pub end: std::sync::atomic::AtomicU64,
    pub consumed: std::sync::atomic::AtomicU64,
    pub local_wait_nanos: std::sync::atomic::AtomicU64,
}
impl AssignmentControl {
    fn new(end: u64) -> Self {
        Self {
            end: end.into(),
            consumed: 0.into(),
            local_wait_nanos: 0.into(),
        }
    }
    pub fn publish(&self, bytes: u64, wait: Duration) {
        use std::sync::atomic::Ordering::Relaxed;
        self.local_wait_nanos
            .store(wait.as_nanos().min(u128::from(u64::MAX)) as u64, Relaxed);
        self.consumed.store(bytes, Relaxed);
    }
    pub fn bytes(&self) -> u64 {
        self.consumed.load(std::sync::atomic::Ordering::Relaxed)
    }
    pub fn local_wait(&self) -> Duration {
        Duration::from_nanos(
            self.local_wait_nanos
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

#[derive(Debug, Clone)]
pub struct Assignment {
    /// Owned until the terminal observation, including pending destination writes.
    pub range: ByteRange,
    pub control: std::sync::Arc<AssignmentControl>,
    pub wire_received: u64,
    pub destination_submitted: u64,
    pub destination_committed: u64,
    pub committed_ranges: RangeSet,
    pub started: Instant,
    pub last_progress: Instant,
    pub rate: EwmaWithVariance,
    /// Bytes of overlap prefix requested before `range.start`, for boundary checks.
    pub overlap: u32,
    /// Set when the tail was stolen, so the worker stops early and cleanly.
    pub truncated: bool,
    /// The connection currently executing this assignment, if claimed.
    pub connection: Option<ConnectionId>,
    /// Retry attempt for this piece of work (1-based after the first try).
    pub attempt: u32,
}

/// Bytes that failed and must not be reclaimed until `until`.
#[derive(Debug, Clone)]
struct DeferredWork {
    range: ByteRange,
    until: Instant,
    attempt: u32,
}

impl Assignment {
    #[inline]
    pub fn cursor(&self) -> u64 {
        self.committed_ranges
            .first_gap(self.range.start, self.range.end)
            .map_or(self.range.end, |gap| gap.start)
    }
    #[inline]
    pub fn remaining(&self) -> u64 {
        self.range.end.saturating_sub(self.cursor())
    }
    #[inline]
    pub fn is_done(&self) -> bool {
        self.cursor() >= self.range.end
    }
    pub fn eta(&self, now: Instant) -> Duration {
        let _ = now;
        Rate::from_bps(self.rate.mean()).time_for(self.remaining())
    }
    /// The range to actually request, including the overlap prefix.
    pub fn wire_range(&self) -> ByteRange {
        let start = self.cursor();
        let ov = self.overlap as u64;
        ByteRange::new(start.saturating_sub(ov.min(start)), self.range.end)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Claim {
    /// A brand new gap in the file.
    Fresh(AssignId),
    /// Ask an incumbent to drain; its tail remains claimed until acknowledgement.
    Yield { assignment: AssignId, new_end: u64 },
    /// Nothing to do: everything is claimed and nothing is worth splitting.
    Saturated,
    /// The transfer is finished.
    Complete,
}

#[derive(Debug)]
pub struct SegmentPlan {
    /// `None` means the server did not give us a length; single-stream mode.
    total: Option<u64>,
    /// Durably written.
    completed: RangeSet,
    /// completed ∪ in-flight. What is *not* here is claimable.
    claimed: RangeSet,
    assignments: SlotMap<AssignId, Assignment>,
    cfg: SegmentationPolicy,
    piece_duration: Duration,
    overlap_bytes: u32,
    /// The source cannot serve byte ranges: exactly one worker consumes
    /// `[completed.contiguous_prefix(), total)` in order.
    single_stream: bool,
    deferred: Vec<DeferredWork>,
}

impl SegmentPlan {
    pub fn new(
        total: Option<u64>,
        completed: RangeSet,
        cfg: SegmentationPolicy,
        piece_duration: Duration,
        overlap_bytes: u32,
    ) -> Self {
        let claimed = completed.clone();
        Self {
            total,
            completed,
            claimed,
            assignments: SlotMap::with_key(),
            cfg,
            piece_duration,
            overlap_bytes,
            single_stream: false,
            deferred: Vec::new(),
        }
    }

    /// Declare that this source cannot serve byte ranges; the plan then
    /// behaves as a single sequential stream even with a known total.
    pub fn set_single_stream(&mut self, on: bool) {
        self.single_stream = on;
    }

    #[inline]
    pub fn total(&self) -> Option<u64> {
        self.total
    }

    /// Learned the real length after the fact (probe or first 206).
    pub fn set_total(&mut self, total: u64) {
        self.total = Some(total);
    }

    #[inline]
    pub fn completed(&self) -> &RangeSet {
        &self.completed
    }
    #[inline]
    pub fn bytes_done(&self) -> u64 {
        self.completed.covered_len()
    }
    #[inline]
    pub fn bytes_remaining(&self) -> Option<u64> {
        self.total
            .map(|t| t.saturating_sub(self.completed.covered_len()))
    }
    #[inline]
    pub fn active_workers(&self) -> usize {
        self.assignments.len()
    }

    /// Assignments currently in flight. The controller pumps a connection
    /// until this reaches its stream capacity.
    #[inline]
    pub fn in_flight(&self) -> usize {
        self.assignments.len()
    }

    /// Can another assignment start right now?
    pub fn admits_worker(&self, now: Instant) -> bool {
        match self.total {
            Some(total) if self.completed.is_complete(total) => false,
            Some(_) if self.single_stream => self.assignments.is_empty(),
            Some(total) => {
                self.next_claimable_gap(total, now).is_some()
                    || self.plan_yield(Rate::COLD_START, now).is_some()
            }
            None => self.assignments.is_empty(),
        }
    }

    fn gap_is_deferred(&self, gap: ByteRange, now: Instant) -> bool {
        self.deferred
            .iter()
            .any(|d| d.until > now && d.range.intersects(&gap))
    }

    /// Keep `range` claimed until `until` so the pump cannot reclaim it.
    pub fn defer(&mut self, range: ByteRange, until: Instant, attempt: u32) {
        if range.is_empty() {
            return;
        }
        if range.end != u64::MAX {
            self.claimed.insert(range);
        }
        self.deferred.push(DeferredWork {
            range,
            until,
            attempt,
        });
    }

    /// Release deferred work whose backoff has elapsed. Returns the highest
    /// attempt among released ranges so the next StartRange can carry it.
    pub fn release_due(&mut self, now: Instant) -> u32 {
        let mut max_attempt = 0u32;
        let mut keep = Vec::new();
        for d in self.deferred.drain(..) {
            if d.until <= now {
                if d.range.end != u64::MAX {
                    self.claimed.remove(d.range);
                }
                max_attempt = max_attempt.max(d.attempt);
            } else {
                keep.push(d);
            }
        }
        self.deferred = keep;
        max_attempt
    }

    pub fn pending_attempt_for(&self, range: ByteRange) -> u32 {
        self.deferred
            .iter()
            .filter(|d| d.range.intersects(&range))
            .map(|d| d.attempt)
            .max()
            .unwrap_or(0)
    }
    /// Clamp the maximum piece to `max_piece`. Only ever LOWERS the cap:
    /// callers pass destination limits (e.g. `hints.max_operation_bytes`),
    /// and a hint must never raise what policy deliberately set.
    pub fn clamp_max_piece(&mut self, max_piece: u64) {
        if max_piece > 0 {
            self.cfg.max_piece = self.cfg.max_piece.min(max_piece);
        }
    }
    #[inline]
    pub fn assignment(&self, id: AssignId) -> Option<&Assignment> {
        self.assignments.get(id)
    }
    #[inline]
    pub fn assignment_mut(&mut self, id: AssignId) -> Option<&mut Assignment> {
        self.assignments.get_mut(id)
    }
    pub fn iter_assignments(&self) -> impl Iterator<Item = (AssignId, &Assignment)> {
        self.assignments.iter()
    }
    pub fn iter_assignments_mut(&mut self) -> impl Iterator<Item = (AssignId, &mut Assignment)> {
        self.assignments.iter_mut()
    }

    pub fn is_complete(&self) -> bool {
        match self.total {
            Some(t) => self.completed.is_complete(t),
            None => false,
        }
    }

    /// Target piece size for a worker expected to run at `est`.
    ///
    /// A fixed `MIN_SEGMENT_SIZE = 1MiB` is a relic. The real target is
    /// *duration*: 2-5s. A 100MB/s connection gets 200-500MB pieces, a 2MB/s
    /// connection gets 4-10MB. Same relative scheduling cost, same stealability.
    fn next_claimable_gap(&self, total: u64, now: Instant) -> Option<ByteRange> {
        let mut pos = 0u64;
        while pos < total {
            let gap = self.claimed.first_gap(pos, total)?;

            if !self.gap_is_deferred(gap, now) {
                return Some(gap);
            }
            // Skip the deferred covering this gap.
            let skip = self
                .deferred
                .iter()
                .filter(|d| d.until > now && d.range.intersects(&gap))
                .map(|d| d.range.end)
                .max()
                .unwrap_or(gap.end);
            pos = skip.max(gap.start.saturating_add(1));
        }
        None
    }

    pub fn piece_len_for(&self, est: Rate, remaining_total: u64) -> u64 {
        let by_duration = est.bytes_in(self.piece_duration);
        let tail = remaining_total <= self.cfg.tail_threshold;
        let min = if tail {
            self.cfg.tail_min_piece
        } else {
            self.cfg.min_piece
        };
        let max = if tail && remaining_total < self.cfg.max_piece {
            (self.cfg.max_piece / 8).max(min)
        } else {
            self.cfg.max_piece
        };
        align_up(by_duration.clamp(min, max), self.cfg.alignment)
    }

    /// Give this worker something to do.
    pub fn claim(&mut self, est: Rate, now: Instant) -> Claim {
        self.claim_with(est, now, false, 2)
    }

    /// `solo` means only one logical slot is currently granted. A later
    /// grant can obtain tail work through an acknowledged yield.
    /// `peer_slots` is the number of logical transfer slots available to this job;
    /// first claims carve unclaimed bytes into that many pieces instead of
    /// parking half the file behind a 2s steal grace.
    pub fn claim_with(&mut self, est: Rate, now: Instant, solo: bool, peer_slots: usize) -> Claim {
        self.claim_bounded(est, now, solo, peer_slots, None)
    }

    pub fn claim_bounded(
        &mut self,
        est: Rate,
        now: Instant,
        solo: bool,
        peer_slots: usize,
        quantum: Option<u64>,
    ) -> Claim {
        let Some(total) = self.total else {
            // Unknown length: exactly one stream, from wherever we left off.
            if !self.assignments.is_empty() {
                return Claim::Saturated;
            }
            let start = self.completed.contiguous_prefix();
            let id = self.push_assignment(ByteRange::new(start, u64::MAX), now, 0);
            return Claim::Fresh(id);
        };

        if self.completed.is_complete(total) {
            return Claim::Complete;
        }

        if self.single_stream {
            // One ordered worker owns everything from the verified prefix to
            // EOF; no gaps and no stealing.
            if !self.assignments.is_empty() {
                return Claim::Saturated;
            }
            let start = self.completed.contiguous_prefix();
            if start >= total {
                return Claim::Complete;
            }
            let id = self.push_assignment(ByteRange::new(start, total), now, 0);
            return Claim::Fresh(id);
        }
        let remaining_total = total - self.completed.covered_len();
        let piece = self
            .piece_len_for(est, remaining_total)
            .min(quantum.unwrap_or(u64::MAX));

        // 1. Prefer a virgin gap that is not in retry backoff.
        if let Some(gap) = self.next_claimable_gap(total, now) {
            let mut take_len = piece.max(1);
            if !solo {
                let inflight = self.assignments.len();
                let slots_left = peer_slots.max(1).saturating_sub(inflight).max(1);
                if slots_left > 1 {
                    let unclaimed = total.saturating_sub(self.claimed.covered_len());
                    if unclaimed > self.cfg.min_piece.saturating_mul(2) {
                        take_len = take_len
                            .min(unclaimed / slots_left as u64)
                            .max(self.cfg.min_piece);
                    }
                }
            }
            let take = gap.truncated_to(take_len);
            let take = snap_end(take, self.cfg.alignment, total);
            // Consistency overlap: when the bytes immediately before
            // this piece are already claimed (being written by another
            // worker, possibly another MIRROR), re-fetch a short prefix
            // and compare. Two sources disagreeing about the same
            // bytes is how corrupt mirrors get caught.
            let desired = u64::from(self.overlap_bytes).min(take.start);
            let overlap = if desired > 0
                && self
                    .claimed
                    .contains_range(ByteRange::new(take.start - desired, take.start))
            {
                desired as u32
            } else {
                0
            };
            let attempt = self.pending_attempt_for(take);
            let id = self.push_assignment(take, now, overlap);
            if let Some(a) = self.assignments.get_mut(id) {
                a.attempt = attempt;
            }
            return Claim::Fresh(id);
        }

        // 2. No gaps: steal the tail of the fattest assignment.
        if solo {
            return Claim::Saturated;
        }
        match self.plan_yield(est, now) {
            Some((victim, cut)) => {
                self.assignments[victim].truncated = true;
                Claim::Yield {
                    assignment: victim,
                    new_end: cut,
                }
            }
            None => Claim::Saturated,
        }
    }

    /// Request an incumbent yield; its original claim remains reserved.
    fn plan_yield(&self, est: Rate, now: Instant) -> Option<(AssignId, u64)> {
        let (victim, a) = self
            .assignments
            .iter()
            .filter(|(_, a)| !a.truncated)
            .filter(|(_, a)| now.saturating_duration_since(a.started) >= self.cfg.straggler_grace)
            .max_by_key(|(_, a)| {
                let rate = if a.rate.is_warm() {
                    a.rate.mean().max(Rate::FLOOR.bps())
                } else {
                    est.nonzero()
                };
                // eta proxy: seconds to finish remaining bytes, scaled to
                // stay in integer Ord territory
                ((a.remaining() as f64 / rate.max(1.0)) * 1e6) as u64
            })?;

        let protected = a.range.start.saturating_add(
            a.wire_received
                .max(a.destination_submitted)
                .max(a.control.bytes()),
        );
        let rem = a.range.end.saturating_sub(protected);
        let tail = self
            .bytes_remaining()
            .is_some_and(|r| r <= self.cfg.tail_threshold);
        let min_piece = if tail {
            self.cfg.tail_min_piece
        } else {
            self.cfg.min_piece
        };

        // Splitting something that is nearly done just adds a request.
        if rem < min_piece.saturating_mul(2) {
            return None;
        }

        let cut = align_up(protected.saturating_add(min_piece), self.cfg.alignment)
            .min(a.range.end - min_piece);

        (cut > a.cursor() && cut < a.range.end).then_some((victim, cut))
    }

    fn push_assignment(&mut self, range: ByteRange, now: Instant, overlap: u32) -> AssignId {
        if range.end != u64::MAX {
            self.claimed.insert(range);
        }
        self.assignments.insert(Assignment {
            range,
            control: std::sync::Arc::new(AssignmentControl::new(range.end)),
            wire_received: 0,
            destination_submitted: 0,
            destination_committed: 0,
            committed_ranges: RangeSet::new(),
            started: now,
            last_progress: now,
            rate: EwmaWithVariance::new(Duration::from_secs(4)),
            overlap,
            truncated: false,
            connection: None,
            attempt: 0,
        })
    }

    /// Progress on wire bytes received.
    pub fn on_wire_received(&mut self, id: AssignId, n: u64) {
        if let Some(a) = self.assignments.get_mut(id) {
            a.wire_received = a.wire_received.saturating_add(n);
        }
    }

    pub fn on_submitted(&mut self, id: AssignId, n: u64) {
        if let Some(a) = self.assignments.get_mut(id) {
            a.destination_submitted = a.destination_submitted.saturating_add(n);
        }
    }

    /// Destination acknowledged write completion.
    /// This proves bytes reached destination, but does NOT yet verify HTTP response completion.
    pub fn on_destination_committed(&mut self, id: AssignId, range: ByteRange, now: Instant) {
        if range.is_empty() {
            return;
        }
        let Some(a) = self.assignments.get_mut(id) else {
            return;
        };
        let range = range
            .intersection(&a.range)
            .unwrap_or(ByteRange::new(a.range.start, a.range.start));
        let before = a.cursor();
        a.committed_ranges.insert(range);
        a.destination_committed = a.committed_ranges.covered_len();
        let after = a.cursor();
        let advanced = after.saturating_sub(before);
        if advanced == 0 {
            return;
        }
        let dt = now.saturating_duration_since(a.last_progress);
        if dt > Duration::from_millis(20) {
            a.rate.observe(advanced as f64 / dt.as_secs_f64(), dt);
        }
        a.last_progress = now;
    }

    /// Response verification completed successfully for a range.
    /// Only verified ranges contribute to job completion!
    pub fn on_response_verified(&mut self, id: AssignId, range: ByteRange, _now: Instant) {
        if range.is_empty() {
            return;
        }
        let Some(a) = self.assignments.get(id) else {
            return;
        };
        let range = range
            .intersection(&a.range)
            .unwrap_or(ByteRange::new(a.range.start, a.range.start));
        if !range.is_empty() {
            self.completed.insert(range);
            self.claimed.insert(range);
        }
    }

    /// Worker finished with successful verification. Promotes committed
    /// ranges to completed and releases only the *unverified* remainder for
    /// reclaim, preserving the `claimed ⊇ completed` invariant the claim
    /// path relies on.
    pub fn finish_verified(&mut self, id: AssignId) -> Option<Assignment> {
        let a = self.assignments.remove(id)?;
        for r in a.committed_ranges.iter() {
            self.completed.insert(r);
            self.claimed.insert(r);
        }
        if a.range.end != u64::MAX {
            for gap in self.completed.gaps(a.range.end) {
                if let Some(unverified) = gap.intersection(&a.range)
                    && !unverified.is_empty()
                {
                    self.claimed.remove(unverified);
                }
            }
        }
        Some(a)
    }

    /// Worker finished (or was truncated and reached its new end).
    pub fn finish(&mut self, id: AssignId) -> Option<Assignment> {
        self.finish_verified(id)
    }

    /// Worker died or failed. We drop any unverified tentative ranges so they can be re-fetched.
    pub fn abandon(&mut self, id: AssignId) -> Option<Assignment> {
        let a = self.assignments.remove(id)?;
        // Unverified ranges remain unclaimed
        if a.range.end != u64::MAX {
            // Remove the unverified portion of the assignment range from claimed
            for r in self.completed.gaps(a.range.end) {
                if let Some(overlap) = r.intersection(&a.range) {
                    self.claimed.remove(overlap);
                }
            }
        }
        Some(a)
    }

    /// Roll back a range we thought was done but failed verification.
    pub fn invalidate(&mut self, r: ByteRange) {
        self.completed.remove(r);
        self.claimed.remove(r);
        tracing::warn!(target: "xde::segment", ?r, "range invalidated, will re-fetch");
    }

    /// Fleet statistics used for straggler detection.
    pub fn rate_stats(&self) -> EwmaWithVariance {
        let mut agg = EwmaWithVariance::new(Duration::from_secs(4));
        for (_, a) in self.assignments.iter().filter(|(_, a)| a.rate.is_warm()) {
            agg.observe(a.rate.mean(), Duration::from_secs(1));
        }
        agg
    }

    /// Mean and stddev of warm worker rates. Unlike `rate_stats` this works
    /// from a single sample pair, so two-worker transfers get straggler
    /// protection too.
    fn fleet_stats(&self) -> Option<(f64, f64)> {
        let rates: Vec<f64> = self
            .assignments
            .values()
            .filter(|a| a.rate.is_warm())
            .map(|a| a.rate.mean())
            .collect();
        if rates.is_empty() {
            return None;
        }
        let n = rates.len() as f64;
        let mean = rates.iter().sum::<f64>() / n;
        let var = rates.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n;
        Some((mean, var.max(0.0).sqrt()))
    }

    /// A download at 99% stuck behind one 2MB/s straggler can hang for 90s.
    /// We detect it by deviation from the per-stream rate distribution, not by
    /// a magic threshold, and we detect it early.
    pub fn stragglers(&self, now: Instant) -> SmallVec<[AssignId; 4]> {
        let mut out = SmallVec::new();
        if self.assignments.len() < 2 {
            return out;
        }
        let Some((fleet_mean, fleet_sd)) = self.fleet_stats() else {
            return out;
        };
        for (id, a) in self.assignments.iter() {
            if now.saturating_duration_since(a.started) < self.cfg.straggler_grace {
                continue;
            }
            if !a.rate.is_warm() {
                continue;
            }
            if a.remaining() < self.cfg.tail_min_piece {
                continue;
            }
            let z = if fleet_sd < 1e-9 {
                if a.rate.mean() < fleet_mean * 0.5 {
                    4.0
                } else {
                    0.0
                }
            } else {
                (fleet_mean - a.rate.mean()) / fleet_sd
            };
            // Two independent signals: statistical outlier OR clear deficit
            // against the fair share. The second catches small fleets where
            // dispersion-based detection is blind.
            let below_share = a.rate.mean() < fleet_mean * self.cfg.straggler_share;
            if z >= self.cfg.straggler_z || below_share {
                out.push(id);
            }
        }
        out
    }

    /// Workers that have produced nothing for `limit`. Distinct from stragglers:
    /// these are stalled, not slow, and the response is to kill the request.
    pub fn stalled(&self, now: Instant, limit: Duration) -> SmallVec<[AssignId; 4]> {
        self.assignments
            .iter()
            .filter(|(_, a)| now.saturating_duration_since(a.last_progress) > limit)
            .map(|(id, _)| id)
            .collect()
    }

    /// Request a straggler yield without releasing its range ownership.
    pub fn rebalance_stragglers(&mut self, now: Instant) -> SmallVec<[(AssignId, u64); 4]> {
        let mut cuts = SmallVec::new();
        if self.assignments.len() < 2 {
            return cuts;
        }
        let stragglers = self.stragglers(now);
        if stragglers.is_empty() {
            return cuts;
        }
        for id in stragglers {
            let Some(a) = self.assignments.get(id) else {
                continue;
            };
            if !a.rate.is_warm() {
                continue;
            }
            // What the straggler keeps: ~one piece-duration of its own
            // throughput, never below the minimum piece.
            let rate = a.rate.mean().max(Rate::FLOOR.bps());
            let keep = ((rate * self.piece_duration.as_secs_f64()) as u64)
                .max(self.cfg.min_piece)
                .max(self.cfg.tail_min_piece);
            let cursor = a.cursor();
            let end = a.range.end;
            let remaining = end.saturating_sub(cursor);
            if remaining <= keep.saturating_mul(2) {
                continue; // not worth the churn
            }
            let new_end = align_up(cursor + keep, self.cfg.alignment).min(end);
            if new_end >= end || new_end <= cursor {
                continue;
            }
            if a.truncated {
                continue;
            }
            self.assignments
                .get_mut(id)
                .expect("checked above")
                .truncated = true;
            cuts.push((id, new_end));
        }
        cuts
    }

    /// Estimated completion for the whole job, bounded by the slowest worker.
    pub fn eta(&self, now: Instant, spare_capacity: Rate) -> Option<Duration> {
        let total = self.total?;
        let left = total.saturating_sub(self.completed.covered_len());
        if left == 0 {
            return Some(Duration::ZERO);
        }
        let worker_eta = self
            .assignments
            .values()
            .map(|a| a.eta(now))
            .max()
            .unwrap_or(Duration::ZERO);
        let unclaimed = self
            .claimed
            .gaps(total)
            .iter()
            .map(|g| g.len())
            .sum::<u64>();
        let unclaimed_eta = spare_capacity.time_for(unclaimed);
        Some(worker_eta.max(unclaimed_eta))
    }

    #[cfg(debug_assertions)]
    pub fn assert_invariants(&self) {
        self.completed.assert_normalized();
        self.claimed.assert_normalized();
        for (_, a) in self.assignments.iter() {
            assert!(a.destination_committed <= a.range.len() || a.range.end == u64::MAX);
            if a.range.end != u64::MAX {
                assert!(
                    self.claimed.contains_range(ByteRange::new(
                        a.range.start,
                        a.cursor().max(a.range.start)
                    )),
                    "assignment progress not reflected in claimed set"
                );
            }
        }
        // Everything completed must be claimed.
        for r in self.completed.iter() {
            assert!(
                self.claimed.contains_range(r),
                "completed range not claimed: {r:?}"
            );
        }
    }
}

#[inline]
pub fn align_up(v: u64, align: u64) -> u64 {
    if align <= 1 {
        return v;
    }
    v.div_ceil(align) * align
}

#[inline]
fn snap_end(r: ByteRange, align: u64, total: u64) -> ByteRange {
    let end = align_up(r.end, align)
        .min(total)
        .min(r.end.max(r.start + 1));
    ByteRange::new(r.start, end.max(r.start + 1).min(total))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn out_of_order_completion_does_not_advance_the_committed_frontier() {
        let now = Instant::now();
        let mut plan = SegmentPlan::new(
            Some(8),
            RangeSet::new(),
            SegmentationPolicy::default(),
            Duration::from_secs(1),
            0,
        );
        let Claim::Fresh(assignment) = plan.claim(Rate::from_bps(8.0), now) else {
            panic!("assignment")
        };
        plan.on_destination_committed(assignment, ByteRange::new(4, 8), now);
        assert_eq!(plan.assignment(assignment).unwrap().cursor(), 0);
        assert_eq!(plan.bytes_done(), 0);
        assert!(!plan.is_complete());

        plan.on_destination_committed(assignment, ByteRange::new(0, 4), now);
        assert_eq!(plan.assignment(assignment).unwrap().cursor(), 8);
        // Destination writes complete does NOT mean verified complete!
        assert_eq!(plan.bytes_done(), 0);
        assert!(!plan.is_complete());

        // Once verified, plan completes
        plan.on_response_verified(assignment, ByteRange::new(0, 8), now);
        assert_eq!(plan.bytes_done(), 8);
        assert!(plan.is_complete());
    }

    #[test]
    fn failed_request_leaves_bytes_unverified_and_reclaimable() {
        let now = Instant::now();
        let mut plan = SegmentPlan::new(
            Some(10),
            RangeSet::new(),
            SegmentationPolicy::default(),
            Duration::from_secs(1),
            0,
        );
        let Claim::Fresh(assignment) = plan.claim(Rate::from_bps(10.0), now) else {
            panic!("assignment")
        };
        plan.on_destination_committed(assignment, ByteRange::new(0, 5), now);
        assert_eq!(plan.bytes_done(), 0);
        assert!(!plan.is_complete());

        // Failed request: abandoned
        plan.abandon(assignment);
        assert_eq!(plan.bytes_done(), 0);
        assert!(!plan.is_complete());
        assert!(plan.admits_worker(now));
    }

    #[test]
    fn first_claims_split_across_peer_slots() {
        let now = Instant::now();
        let mut plan = SegmentPlan::new(
            Some(256 * 1024 * 1024),
            RangeSet::new(),
            SegmentationPolicy::default(),
            Duration::from_secs(2),
            0,
        );
        let mut lens = Vec::new();
        for _ in 0..4 {
            let Claim::Fresh(id) = plan.claim_with(Rate::COLD_START, now, false, 4) else {
                panic!("fresh")
            };
            lens.push(plan.assignment(id).unwrap().range.len());
        }
        assert_eq!(lens, vec![64 * 1024 * 1024; 4]);
    }

    #[test]
    fn yielding_keeps_ownership_until_pending_writes_are_acknowledged() {
        let now = Instant::now();
        let cfg = SegmentationPolicy {
            min_piece: 1,
            tail_min_piece: 1,
            alignment: 1,
            straggler_grace: Duration::ZERO,
            ..SegmentationPolicy::default()
        };
        let mut plan = SegmentPlan::new(Some(100), RangeSet::new(), cfg, Duration::from_secs(1), 0);
        let Claim::Fresh(assignment) = plan.claim_with(Rate::from_bps(100.0), now, true, 1) else {
            panic!("assignment")
        };
        plan.on_wire_received(assignment, 70);
        plan.on_submitted(assignment, 60);
        let claim = plan.claim(Rate::from_bps(100.0), now);
        let Claim::Yield {
            assignment: victim,
            new_end,
        } = claim
        else {
            panic!("yield")
        };
        assert_eq!(victim, assignment);
        assert!(new_end >= 70);
        assert_eq!(
            plan.assignment(victim).unwrap().range,
            ByteRange::new(0, 100)
        );
        assert!(matches!(
            plan.claim(Rate::from_bps(100.0), now),
            Claim::Saturated
        ));
        plan.on_destination_committed(victim, ByteRange::new(0, 80), now);
        plan.on_response_verified(victim, ByteRange::new(0, 80), now);
        plan.finish_verified(victim).unwrap();
        let Claim::Fresh(next) = plan.claim(Rate::from_bps(100.0), now) else {
            panic!("released tail")
        };
        assert_eq!(plan.assignment(next).unwrap().range.start, 80);
    }

    proptest::proptest! {
        #[test]
        fn claims_never_overlap_owned_ranges(
            piece in 8u64..64,
        ) {
            let now = Instant::now();
            let cfg = SegmentationPolicy {
                min_piece: piece,
                tail_min_piece: 1,
                max_piece: piece * 4,
                alignment: 1,
                straggler_grace: Duration::ZERO,
                ..SegmentationPolicy::default()
            };
            let total = piece * 16;
            let mut plan = SegmentPlan::new(Some(total), RangeSet::new(), cfg, Duration::from_secs(1), 0);
            let mut ids = Vec::new();
            for _ in 0..8 {
                match plan.claim(Rate::from_bps(piece as f64), now) {
                    Claim::Fresh(id) => ids.push(id),
                    Claim::Yield { .. } | Claim::Saturated | Claim::Complete => break,
                }
            }
            let mut seen = RangeSet::new();
            for id in ids {
                let r = plan.assignment(id).unwrap().range;
                prop_assert!(!seen.contains(r.start) || r.is_empty());
                seen.insert(r);
            }
            seen.assert_normalized();
        }
    }
}
