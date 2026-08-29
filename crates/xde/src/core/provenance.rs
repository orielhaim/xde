//! Interval-based first-writer provenance for an artifact.
//!
//! One map per destination, shared by every shard. Adjacent spans from the
//! same source collapse. Sequential writes from a single source are O(1)
//! per chunk (extend the last interval) so the hot path never sorts.

use crate::core::{ids::SourceId, ranges::ByteRange};

#[derive(Debug, Clone, Default)]
pub struct ArtifactProvenance {
    /// Sorted, disjoint `(range, first writer)` spans.
    spans: Vec<(ByteRange, SourceId)>,
    first_writer: Option<SourceId>,
    multi_writer: bool,
}

impl ArtifactProvenance {
    pub fn new() -> Self {
        Self::default()
    }

    /// True once two distinct sources have written any byte of this artifact.
    pub fn has_multiple_writers(&self) -> bool {
        self.multi_writer
    }

    /// Spans overlapping `range` whose first writer is not `source`.
    pub fn foreign_spans(&self, range: ByteRange, source: SourceId) -> Vec<ByteRange> {
        if !self.multi_writer {
            return Vec::new();
        }
        let mut out = Vec::new();
        for (r, owner) in &self.spans {
            if *owner == source {
                continue;
            }
            if let Some(inter) = r.intersection(&range) {
                out.push(inter);
            }
            if r.start >= range.end {
                break;
            }
        }
        out
    }

    /// Record first-writer ownership. Later writers of the same bytes do not
    /// overwrite the original attribution.
    pub fn record(&mut self, range: ByteRange, source: SourceId) {
        if range.is_empty() {
            return;
        }
        match self.first_writer {
            None => self.first_writer = Some(source),
            Some(existing) if existing != source => self.multi_writer = true,
            _ => {}
        }
        // Fast path: extend the last span in place for sequential same-source
        // writes. This is the single-origin download.
        if let Some((last, last_s)) = self.spans.last_mut()
            && *last_s == source
            && range.start >= last.start
            && range.start <= last.end
        {
            last.end = last.end.max(range.end);
            return;
        }
        if self.spans.is_empty() {
            self.spans.push((range, source));
            return;
        }
        let mut holes = vec![range];
        for (owned, _) in &self.spans {
            let mut next = Vec::new();
            for h in holes {
                if let Some(inter) = h.intersection(owned) {
                    if h.start < inter.start {
                        next.push(ByteRange::new(h.start, inter.start));
                    }
                    if inter.end < h.end {
                        next.push(ByteRange::new(inter.end, h.end));
                    }
                } else {
                    next.push(h);
                }
            }
            holes = next;
            if holes.is_empty() {
                break;
            }
        }
        for h in holes {
            self.insert_span(h, source);
        }
    }

    fn insert_span(&mut self, range: ByteRange, source: SourceId) {
        if range.is_empty() {
            return;
        }
        self.spans.push((range, source));
        self.spans.sort_by_key(|(r, _)| r.start);
        let mut merged: Vec<(ByteRange, SourceId)> = Vec::new();
        for (r, s) in self.spans.drain(..) {
            if let Some((last, last_s)) = merged.last_mut()
                && *last_s == s
                && last.end >= r.start
            {
                last.end = last.end.max(r.end);
                continue;
            }
            merged.push((r, s));
        }
        self.spans = merged;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use slotmap::SlotMap;

    #[test]
    fn first_writer_wins_and_foreign_spans_are_interval_based() {
        let mut ids = SlotMap::<SourceId, ()>::with_key();
        let a = ids.insert(());
        let b = ids.insert(());
        let mut p = ArtifactProvenance::new();
        p.record(ByteRange::new(0, 1000), a);
        p.record(ByteRange::new(500, 1500), b);
        assert_eq!(p.foreign_spans(ByteRange::new(400, 800), b).len(), 1);
        assert!(p.foreign_spans(ByteRange::new(1000, 1500), b).is_empty());
        assert_eq!(p.spans.len(), 2);
        assert!(p.has_multiple_writers());
    }

    #[test]
    fn sequential_same_source_writes_collapse_to_one_span() {
        let mut ids = SlotMap::<SourceId, ()>::with_key();
        let a = ids.insert(());
        let mut p = ArtifactProvenance::new();
        for i in 0..64 {
            let s = i * 1024;
            p.record(ByteRange::new(s, s + 1024), a);
        }
        assert_eq!(p.spans.len(), 1);
        assert_eq!(p.spans[0].0, ByteRange::new(0, 64 * 1024));
        assert!(!p.has_multiple_writers());
    }
}
