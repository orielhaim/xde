//! Half-open byte ranges `[start, end)` and a normalized set of them.
//!
//! Deliberately arbitrary-width ranges rather than fixed-size chunk bitmaps:
//! the segmentation strategy is speed-aware, so piece boundaries are not on a
//! fixed grid. Swapping this for `range-set-blaze` / `roaring` is a benchmark
//! question, and the surface here is small enough to keep that swap cheap.

use std::fmt;

use smallvec::SmallVec;

#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ByteRange {
    pub start: u64,
    pub end: u64,
}

impl ByteRange {
    #[inline]
    pub const fn new(start: u64, end: u64) -> Self {
        debug_assert!(start <= end);
        Self { start, end }
    }
    #[inline]
    pub const fn at(start: u64, len: u64) -> Self {
        Self {
            start,
            end: start + len,
        }
    }
    #[inline]
    pub const fn len(&self) -> u64 {
        self.end - self.start
    }
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.start >= self.end
    }
    #[inline]
    pub const fn contains(&self, off: u64) -> bool {
        off >= self.start && off < self.end
    }
    #[inline]
    pub fn intersects(&self, o: &ByteRange) -> bool {
        self.start < o.end && o.start < self.end
    }
    #[inline]
    pub fn intersection(&self, o: &ByteRange) -> Option<ByteRange> {
        let s = self.start.max(o.start);
        let e = self.end.min(o.end);
        (s < e).then(|| ByteRange::new(s, e))
    }
    #[inline]
    pub fn split_at(&self, at: u64) -> (ByteRange, ByteRange) {
        let at = at.clamp(self.start, self.end);
        (ByteRange::new(self.start, at), ByteRange::new(at, self.end))
    }
    #[inline]
    pub fn truncated_to(&self, len: u64) -> ByteRange {
        ByteRange::new(self.start, self.end.min(self.start + len))
    }

    /// `bytes=start-lastByte`, inclusive on both ends per RFC 9110 §14.1.
    pub fn to_http_range(&self) -> String {
        debug_assert!(!self.is_empty());
        format!("bytes={}-{}", self.start, self.end - 1)
    }
}

impl fmt::Debug for ByteRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}..{})", self.start, self.end)
    }
}

/// Sorted, disjoint, non-adjacent set of ranges.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct RangeSet {
    ranges: Vec<ByteRange>,
    covered: u64,
}

impl RangeSet {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_ranges(it: impl IntoIterator<Item = ByteRange>) -> Self {
        let mut s = Self::new();
        for r in it {
            s.insert(r);
        }
        s
    }

    #[inline]
    pub fn covered_len(&self) -> u64 {
        self.covered
    }
    #[inline]
    pub fn segment_count(&self) -> usize {
        self.ranges.len()
    }
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = ByteRange> + '_ {
        self.ranges.iter().copied()
    }
    #[inline]
    pub fn as_slice(&self) -> &[ByteRange] {
        &self.ranges
    }
    pub fn clear(&mut self) {
        self.ranges.clear();
        self.covered = 0;
    }

    pub fn insert(&mut self, r: ByteRange) {
        if r.is_empty() {
            return;
        }
        // First range that could touch or overlap `r` (adjacency counts: end == start merges).
        let lo = self.ranges.partition_point(|x| x.end < r.start);
        // First range strictly after `r` (adjacency counts: start == end merges).
        let hi = self.ranges.partition_point(|x| x.start <= r.end);

        if lo == hi {
            self.ranges.insert(lo, r);
            self.covered += r.len();
            return;
        }

        let removed: u64 = self.ranges[lo..hi].iter().map(|x| x.len()).sum();
        let merged = ByteRange::new(
            r.start.min(self.ranges[lo].start),
            r.end.max(self.ranges[hi - 1].end),
        );
        self.ranges.splice(lo..hi, [merged]);
        self.covered = self.covered - removed + merged.len();
    }

    pub fn remove(&mut self, r: ByteRange) {
        if r.is_empty() || self.ranges.is_empty() {
            return;
        }
        let lo = self.ranges.partition_point(|x| x.end <= r.start);
        let hi = self.ranges.partition_point(|x| x.start < r.end);
        if lo >= hi {
            return;
        }

        let removed: u64 = self.ranges[lo..hi].iter().map(|x| x.len()).sum();
        let mut replacement: SmallVec<[ByteRange; 2]> = SmallVec::new();
        let first = self.ranges[lo];
        if first.start < r.start {
            replacement.push(ByteRange::new(first.start, r.start));
        }
        let last = self.ranges[hi - 1];
        if last.end > r.end {
            replacement.push(ByteRange::new(r.end, last.end));
        }
        let added: u64 = replacement.iter().map(|x| x.len()).sum();
        self.ranges.splice(lo..hi, replacement);
        self.covered = self.covered - removed + added;
    }

    pub fn contains(&self, off: u64) -> bool {
        let i = self.ranges.partition_point(|x| x.end <= off);
        self.ranges.get(i).is_some_and(|x| x.contains(off))
    }

    pub fn contains_range(&self, r: ByteRange) -> bool {
        if r.is_empty() {
            return true;
        }
        let i = self.ranges.partition_point(|x| x.end <= r.start);
        self.ranges
            .get(i)
            .is_some_and(|x| x.start <= r.start && x.end >= r.end)
    }

    /// First uncovered span at or after `from`, bounded by `limit`.
    pub fn first_gap(&self, from: u64, limit: u64) -> Option<ByteRange> {
        if from >= limit {
            return None;
        }
        let i = self.ranges.partition_point(|x| x.end <= from);
        match self.ranges.get(i) {
            None => Some(ByteRange::new(from, limit)),
            Some(next) if next.start > from => Some(ByteRange::new(from, next.start.min(limit))),
            Some(cur) => {
                // `from` sits inside `cur`; the gap starts at cur.end.
                if cur.end >= limit {
                    None
                } else {
                    let nxt_start = self.ranges.get(i + 1).map_or(limit, |n| n.start.min(limit));
                    Some(ByteRange::new(cur.end, nxt_start))
                }
            }
        }
    }

    /// All uncovered spans within `[0, limit)`.
    pub fn gaps(&self, limit: u64) -> Vec<ByteRange> {
        let mut out = Vec::new();
        let mut cur = 0u64;
        for r in &self.ranges {
            if r.start >= limit {
                break;
            }
            if r.start > cur {
                out.push(ByteRange::new(cur, r.start.min(limit)));
            }
            cur = cur.max(r.end);
        }
        if cur < limit {
            out.push(ByteRange::new(cur, limit));
        }
        out
    }

    pub fn is_complete(&self, total: u64) -> bool {
        match self.ranges.as_slice() {
            [only] => only.start == 0 && only.end >= total,
            [] => total == 0,
            _ => false,
        }
    }

    /// Longest prefix that is contiguous from 0. Used by sequential sinks.
    pub fn contiguous_prefix(&self) -> u64 {
        match self.ranges.first() {
            Some(r) if r.start == 0 => r.end,
            _ => 0,
        }
    }

    pub fn union_with(&mut self, other: &RangeSet) {
        for r in other.iter() {
            self.insert(r);
        }
    }

    /// Invariant check; exercised by proptest and by debug builds of the plan.
    pub fn assert_normalized(&self) {
        let mut prev: Option<ByteRange> = None;
        let mut sum = 0;
        for r in &self.ranges {
            assert!(!r.is_empty(), "empty range in set: {r:?}");
            if let Some(p) = prev {
                assert!(p.end < r.start, "non-normalized: {p:?} then {r:?}");
            }
            sum += r.len();
            prev = Some(*r);
        }
        assert_eq!(sum, self.covered, "covered counter drifted");
    }
}

impl fmt::Debug for RangeSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "RangeSet({} bytes in {} segs) ",
            self.covered,
            self.ranges.len()
        )?;
        f.debug_list().entries(self.ranges.iter()).finish()
    }
}

impl serde::Serialize for RangeSet {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(self.ranges.len()))?;
        for r in &self.ranges {
            seq.serialize_element(&(r.start, r.end))?;
        }
        seq.end()
    }
}

impl<'de> serde::Deserialize<'de> for RangeSet {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let raw: Vec<(u64, u64)> = serde::Deserialize::deserialize(d)?;
        // Rebuild through `insert` so a tampered journal cannot inject a
        // non-normalized set into the scheduler.
        Ok(RangeSet::from_ranges(
            raw.into_iter()
                .filter(|(s, e)| s < e)
                .map(|(s, e)| ByteRange::new(s, e)),
        ))
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::{ByteRange, RangeSet};

    const DOMAIN: usize = 128;

    proptest! {
        #[test]
        fn arbitrary_insert_remove_sequences_match_a_byte_model(
            operations in prop::collection::vec((any::<bool>(), 0_u8..=127, 0_u8..=127), 0..512)
        ) {
            let mut ranges = RangeSet::new();
            let mut model = [false; DOMAIN];

            for (insert, a, b) in operations {
                let start = usize::from(a.min(b));
                let end = usize::from(a.max(b)) + 1;
                let range = ByteRange::new(start as u64, end as u64);
                if insert {
                    ranges.insert(range);
                    model[start..end].fill(true);
                } else {
                    ranges.remove(range);
                    model[start..end].fill(false);
                }

                ranges.assert_normalized();
                prop_assert_eq!(
                    ranges.covered_len(),
                    model.iter().filter(|covered| **covered).count() as u64
                );
                for (offset, expected) in model.iter().copied().enumerate() {
                    prop_assert_eq!(ranges.contains(offset as u64), expected);
                }

                let gaps = RangeSet::from_ranges(ranges.gaps(DOMAIN as u64));
                for (offset, expected) in model.iter().copied().enumerate() {
                    prop_assert_eq!(gaps.contains(offset as u64), !expected);
                }
            }
        }

        #[test]
        fn construction_order_and_duplicates_do_not_change_the_set(
            raw in prop::collection::vec((0_u8..=127, 0_u8..=127), 0..256)
        ) {
            let normalized = raw.iter().map(|(a, b)| {
                let start = u64::from((*a).min(*b));
                let end = u64::from((*a).max(*b)) + 1;
                ByteRange::new(start, end)
            }).collect::<Vec<_>>();
            let forward = RangeSet::from_ranges(normalized.iter().copied());
            let reverse = RangeSet::from_ranges(
                normalized.iter().rev().copied().chain(normalized.iter().copied())
            );
            prop_assert_eq!(forward, reverse);
        }
    }
}
