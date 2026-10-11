//! Per-key planning of a polled batch.
//!
//! An Iggy partition gives a total order, but the business only needs an
//! order per key. [`Plan::new`] turns one polled batch into the weaker order
//! the events actually require, so the consumer can batch and reduce them:
//!
//! - **Keys** are independent. They may be processed in any order or
//!   concurrently.
//! - The **segments** of one key must be processed one after another.
//! - Inside a segment, the [head](Plan::head) run goes first, the
//!   [body](Plan::body) runs follow in any order relative to each other, and
//!   the [tail](Plan::tail) run goes last.
//! - A **run** holds every event of one type in its segment, in log order.
//!   It is reduced with that type's [`Algebra`](crate::partition::algebra::Algebra)
//!   before it is applied.
//!
//! Segments come from the fences in [`EventAtomicOrdering`]:
//!
//! - `Acquire` and `AcqRel` start a new segment, and their type's run becomes
//!   its head.
//! - `Release` ends the current segment, and its type's run becomes its tail.
//!   If that type is already the head of a segment that holds other types, the
//!   `Release` event gets a segment of its own instead, because one run can't
//!   be both first and last.
//!
//! This is stronger than the fences require (`Acquire` doesn't actually forbid
//! earlier events from moving past it), but every segment is then totally
//! ordered against its neighbours, which keeps execution simple.
//!
//! The planner is pure: it only reads the key, type tag and ordering of each
//! record, taken from message headers, and returns indices into its input.

use crate::events::{EventAtomicOrdering, EventTypeTag};
use std::collections::HashMap;
use std::hash::Hash;
use std::ops::Range;

/// Header data of one polled message, as needed by the planner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlanRecord<K> {
    /// Ordering key. Records with different keys are never ordered.
    ///
    /// A hash of the real key is fine: a collision only adds ordering
    /// constraints between two keys, it never removes one.
    pub key: K,
    /// Type of the event.
    pub tag: EventTypeTag,
    /// Ordering of the event within its key.
    pub ordering: EventAtomicOrdering,
}

/// Execution plan for one polled batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan<K> {
    /// Input indices, grouped by key, then segment, then run.
    order: Vec<usize>,
    runs: Vec<Run>,
    segments: Vec<Segment>,
    keys: Vec<KeyPlan<K>>,
}

/// The segments of one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPlan<K> {
    key: K,
    segments: Range<usize>,
}

/// A group of runs of one key, totally ordered against the key's other
/// segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    head: Option<usize>,
    body: Range<usize>,
    tail: Option<usize>,
}

/// The events of one type inside one segment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Run {
    tag: EventTypeTag,
    records: Range<usize>,
}

impl<K> KeyPlan<K> {
    /// The key all of these segments belong to.
    pub fn key(&self) -> &K {
        &self.key
    }
}

impl Run {
    /// Type of every event in this run.
    pub fn tag(&self) -> EventTypeTag {
        self.tag
    }
}

impl<K> Plan<K> {
    /// Plan `records`, given in log order.
    pub fn new(records: &[PlanRecord<K>]) -> Self
    where
        K: Eq + Hash + Clone,
    {
        let (keys, metas, segment_of) = assign_segments(records);

        let mut order: Vec<usize> = (0..records.len()).collect();
        order.sort_unstable_by_key(|&i| {
            let segment = segment_of[i];
            let meta = &metas[segment];
            let tag = records[i].tag;
            (meta.key, segment, meta.rank(tag), tag.get(), i)
        });

        let mut builder = Builder {
            runs: Vec::new(),
            segments: Vec::new(),
            keys,
            open: None,
        };
        // (segment, rank, tag, first position in `order`) of the run being scanned.
        let mut run: Option<(usize, Rank, EventTypeTag, usize)> = None;
        for (position, &i) in order.iter().enumerate() {
            let segment = segment_of[i];
            let tag = records[i].tag;
            let rank = metas[segment].rank(tag);
            if let Some((open_segment, open_rank, open_tag, start)) = run {
                if (open_segment, open_rank, open_tag) == (segment, rank, tag) {
                    continue;
                }
                builder.push_run(&metas, open_segment, open_rank, open_tag, start..position);
            }
            run = Some((segment, rank, tag, position));
        }
        if let Some((segment, rank, tag, start)) = run {
            builder.push_run(&metas, segment, rank, tag, start..order.len());
        }
        builder.finish_segment(&metas);

        Plan {
            order,
            runs: builder.runs,
            segments: builder.segments,
            keys: builder.keys,
        }
    }

    /// Every key in the batch, in order of first appearance.
    pub fn keys(&self) -> &[KeyPlan<K>] {
        &self.keys
    }

    /// The segments of `key`, in the order they must be processed.
    pub fn segments(&self, key: &KeyPlan<K>) -> &[Segment] {
        &self.segments[key.segments.clone()]
    }

    /// Run that must finish before any other run of `segment` starts.
    pub fn head(&self, segment: &Segment) -> Option<&Run> {
        segment.head.map(|i| &self.runs[i])
    }

    /// Runs that may be processed in any order relative to each other, after
    /// the head and before the tail.
    pub fn body(&self, segment: &Segment) -> &[Run] {
        &self.runs[segment.body.clone()]
    }

    /// Run that may only start after every other run of `segment` finished.
    pub fn tail(&self, segment: &Segment) -> Option<&Run> {
        segment.tail.map(|i| &self.runs[i])
    }

    /// Indices into the planned records, in log order.
    pub fn records(&self, run: &Run) -> &[usize] {
        &self.order[run.records.clone()]
    }
}

/// Position of a run inside its segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Rank {
    Head,
    Body,
    Tail,
}

struct SegmentMeta {
    /// Index into the key list.
    key: usize,
    head: Option<EventTypeTag>,
    tail: Option<EventTypeTag>,
    /// Holds a type other than `head`.
    mixed: bool,
}

impl SegmentMeta {
    fn rank(&self, tag: EventTypeTag) -> Rank {
        if self.head == Some(tag) {
            Rank::Head
        } else if self.tail == Some(tag) {
            Rank::Tail
        } else {
            Rank::Body
        }
    }
}

/// Walk the log once and give every record a segment.
///
/// Segment indices grow in log order, so sorting by them keeps the segments of
/// one key in order.
fn assign_segments<K: Eq + Hash + Clone>(
    records: &[PlanRecord<K>],
) -> (Vec<KeyPlan<K>>, Vec<SegmentMeta>, Vec<usize>) {
    // key -> (index into `keys`, segment still accepting records)
    let mut slots: HashMap<&K, (usize, Option<usize>)> = HashMap::new();
    let mut keys = Vec::new();
    let mut metas: Vec<SegmentMeta> = Vec::new();
    let mut segment_of = Vec::with_capacity(records.len());

    for record in records {
        let (key, open) = slots.entry(&record.key).or_insert_with(|| {
            let index = keys.len();
            keys.push(KeyPlan {
                key: record.key.clone(),
                segments: 0..0,
            });
            (index, None)
        });

        if let Some(segment) = *open {
            let meta = &metas[segment];
            let cut = match record.ordering {
                EventAtomicOrdering::Acquire | EventAtomicOrdering::AcqRel => true,
                EventAtomicOrdering::Release => meta.mixed && meta.head == Some(record.tag),
                EventAtomicOrdering::Relaxed => false,
            };
            if cut {
                *open = None;
            }
        }
        let segment = *open.get_or_insert_with(|| {
            let segment = metas.len();
            metas.push(SegmentMeta {
                key: *key,
                head: None,
                tail: None,
                mixed: false,
            });
            segment
        });

        let meta = &mut metas[segment];
        if matches!(
            record.ordering,
            EventAtomicOrdering::Acquire | EventAtomicOrdering::AcqRel
        ) {
            meta.head = Some(record.tag);
        }
        if meta.head.is_some_and(|head| head != record.tag) {
            meta.mixed = true;
        }
        if record.ordering == EventAtomicOrdering::Release {
            meta.tail = Some(record.tag);
            *open = None;
        }
        segment_of.push(segment);
    }

    (keys, metas, segment_of)
}

struct Builder<K> {
    runs: Vec<Run>,
    segments: Vec<Segment>,
    keys: Vec<KeyPlan<K>>,
    /// Index into the segment metadata, and the segment built from it so far.
    open: Option<(usize, Segment)>,
}

impl<K> Builder<K> {
    /// Runs arrive sorted by segment and rank.
    fn push_run(
        &mut self,
        metas: &[SegmentMeta],
        segment: usize,
        rank: Rank,
        tag: EventTypeTag,
        records: Range<usize>,
    ) {
        if self.open.as_ref().is_some_and(|(open, _)| *open != segment) {
            self.finish_segment(metas);
        }
        let index = self.runs.len();
        self.runs.push(Run { tag, records });
        let (_, building) = self.open.get_or_insert((
            segment,
            Segment {
                head: None,
                body: index..index,
                tail: None,
            },
        ));
        match rank {
            Rank::Head => {
                building.head = Some(index);
                building.body = self.runs.len()..self.runs.len();
            }
            Rank::Body => building.body.end = self.runs.len(),
            Rank::Tail => building.tail = Some(index),
        }
    }

    fn finish_segment(&mut self, metas: &[SegmentMeta]) {
        let Some((meta, segment)) = self.open.take() else {
            return;
        };
        let index = self.segments.len();
        self.segments.push(segment);
        let range = &mut self.keys[metas[meta].key].segments;
        if range.start == range.end {
            *range = index..self.segments.len();
        } else {
            range.end = self.segments.len();
        }
    }
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
pub(crate) mod test_support {
    use super::*;
    use EventAtomicOrdering::{AcqRel, Acquire, Relaxed, Release};

    pub(crate) const ORDERINGS: [EventAtomicOrdering; 4] = [Relaxed, Acquire, Release, AcqRel];

    pub(crate) fn record(key: u8, tag: u32, ordering: EventAtomicOrdering) -> PlanRecord<u8> {
        PlanRecord {
            key,
            tag: EventTypeTag::new(tag),
            ordering,
        }
    }

    #[derive(Debug, Clone, Copy)]
    pub(crate) struct Place {
        segment: usize,
        rank: Rank,
        run: usize,
        position: usize,
    }

    /// Where every record ends up; asserts each record is planned exactly once
    /// and stays in its own key and type.
    pub(crate) fn places(records: &[PlanRecord<u8>], plan: &Plan<u8>) -> Vec<Place> {
        let mut places: Vec<Option<Place>> = vec![None; records.len()];
        let mut run_id = 0;
        for key in plan.keys() {
            for (segment_index, segment) in plan.segments(key).iter().enumerate() {
                let head = plan.head(segment).map(|run| (Rank::Head, run));
                let body = plan.body(segment).iter().map(|run| (Rank::Body, run));
                let tail = plan.tail(segment).map(|run| (Rank::Tail, run));
                for (rank, run) in head.into_iter().chain(body).chain(tail) {
                    run_id += 1;
                    for (position, &i) in plan.records(run).iter().enumerate() {
                        assert_eq!(records[i].key, *key.key());
                        assert_eq!(records[i].tag, run.tag());
                        assert!(places[i].is_none(), "record {i} planned twice");
                        places[i] = Some(Place {
                            segment: segment_index,
                            rank,
                            run: run_id,
                            position,
                        });
                    }
                }
            }
        }
        places
            .into_iter()
            .enumerate()
            .map(|(i, place)| place.unwrap_or_else(|| panic!("record {i} not planned")))
            .collect()
    }

    /// `x` is processed before `y` in every execution the plan allows.
    pub(crate) fn precedes(x: Place, y: Place) -> bool {
        x.segment < y.segment
            || (x.segment == y.segment
                && ((x.run == y.run && x.position < y.position) || x.rank < y.rank))
    }

    /// The log order of `x` before `y` must be kept: they share a key and
    /// either a type or a fence between them.
    pub(crate) fn required(x: &PlanRecord<u8>, y: &PlanRecord<u8>) -> bool {
        x.key == y.key
            && (x.tag == y.tag
                || matches!(x.ordering, Acquire | AcqRel)
                || matches!(y.ordering, Release | AcqRel))
    }

    /// Every sequence of up to `max_len` records drawn from `alphabet`.
    pub(crate) fn for_each_sequence(
        alphabet: &[PlanRecord<u8>],
        max_len: u32,
        mut f: impl FnMut(&[PlanRecord<u8>]),
    ) {
        let mut sequence = Vec::new();
        for len in 1..=max_len {
            for mut code in 0..alphabet.len().pow(len) {
                sequence.clear();
                for _ in 0..len {
                    sequence.push(alphabet[code % alphabet.len()]);
                    code /= alphabet.len();
                }
                f(&sequence);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::test_support::*;
    use super::*;
    use EventAtomicOrdering::{Acquire, Relaxed, Release};

    fn assert_respects_ordering(records: &[PlanRecord<u8>]) {
        let plan = Plan::new(records);
        let places = places(records, &plan);
        for (i, x) in records.iter().enumerate() {
            for (j, y) in records.iter().enumerate().skip(i + 1) {
                if required(x, y) {
                    assert!(
                        precedes(places[i], places[j]),
                        "{records:?}: record {i} must precede record {j}, plan {plan:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn respects_every_same_key_constraint() {
        let two_keys: Vec<_> = [0, 1]
            .into_iter()
            .flat_map(|key| [1, 2].into_iter().map(move |tag| (key, tag)))
            .flat_map(|(key, tag)| ORDERINGS.map(|ordering| record(key, tag, ordering)))
            .collect();
        for_each_sequence(&two_keys, 4, assert_respects_ordering);

        let one_key: Vec<_> = [1, 2, 3]
            .into_iter()
            .flat_map(|tag| ORDERINGS.map(|ordering| record(0, tag, ordering)))
            .collect();
        for_each_sequence(&one_key, 5, assert_respects_ordering);
    }

    #[test]
    fn relaxed_events_of_a_key_form_one_segment_with_one_run_per_type() {
        let records = [
            record(0, 1, Relaxed),
            record(1, 1, Relaxed),
            record(0, 2, Relaxed),
            record(0, 1, Relaxed),
            record(0, 2, Relaxed),
        ];
        let plan = Plan::new(&records);
        let [key0, key1] = plan.keys() else {
            panic!("expected two keys: {plan:?}")
        };
        let [segment] = plan.segments(key0) else {
            panic!("expected one segment: {plan:?}")
        };
        let runs: Vec<_> = plan
            .body(segment)
            .iter()
            .map(|run| plan.records(run))
            .collect();
        assert_eq!(runs, [&[0, 3][..], &[2, 4][..]]);
        assert_eq!(plan.segments(key1).len(), 1);
    }

    #[test]
    fn release_tail_collects_earlier_events_of_its_type() {
        let records = [
            record(0, 1, Relaxed),
            record(0, 2, Relaxed),
            record(0, 1, Release),
            record(0, 2, Relaxed),
        ];
        let plan = Plan::new(&records);
        let segments = plan.segments(&plan.keys()[0]);
        assert_eq!(segments.len(), 2);
        let tail = plan.tail(&segments[0]).map(|run| plan.records(run));
        assert_eq!(tail, Some(&[0, 2][..]));
    }

    #[test]
    fn acquire_head_collects_later_events_of_its_type() {
        let records = [
            record(0, 2, Relaxed),
            record(0, 1, Acquire),
            record(0, 2, Relaxed),
            record(0, 1, Relaxed),
        ];
        let plan = Plan::new(&records);
        let segments = plan.segments(&plan.keys()[0]);
        assert_eq!(segments.len(), 2);
        let head = plan.head(&segments[1]).map(|run| plan.records(run));
        assert_eq!(head, Some(&[1, 3][..]));
    }
}
