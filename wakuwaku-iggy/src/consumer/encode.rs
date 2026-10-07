//! Encoding the unfinished work of a key as retry records.
//!
//! Runs are written already reduced. The orderings of the written entries are
//! chosen so that planning them again keeps every ordering the original plan
//! enforced: a head run starts with `Acquire`, a tail run ends with `Release`,
//! and everything else is `Relaxed`.

use crate::consumer::execute::{KeyFailure, Stage};
use crate::consumer::plan::{KeyPlan, Plan, Run};
use crate::consumer::record::ParsedRecord;
use crate::events::{EventAlgebraicProperties, EventAtomicOrdering};
use crate::handler::{HandlerList, RunRecord};
use crate::headers::EventHeaders;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum StageKind {
    Head,
    Body,
    Tail,
}

impl From<Stage> for StageKind {
    fn from(stage: Stage) -> Self {
        match stage {
            Stage::Head => StageKind::Head,
            Stage::Body(_) => StageKind::Body,
            Stage::Tail => StageKind::Tail,
        }
    }
}

/// Orderings of the `len` entries written for one run in a `kind` stage.
pub(crate) fn stage_orderings(
    kind: StageKind,
    len: usize,
) -> impl Iterator<Item = EventAtomicOrdering> {
    (0..len).map(move |i| match kind {
        StageKind::Head if i == 0 => EventAtomicOrdering::Acquire,
        StageKind::Tail if i.checked_add(1) == Some(len) => EventAtomicOrdering::Release,
        _ => EventAtomicOrdering::Relaxed,
    })
}

#[derive(Debug)]
pub(crate) struct RetryEntry {
    pub headers: EventHeaders,
    pub payload: Box<[u8]>,
}

/// Encode the unfinished work of `key`: everything from the failed segment
/// on, or every segment when `failure` is `None` (a diverted key).
///
/// In the failed segment, stages before the failed one completed and are
/// skipped, failed runs contribute their remainder, completed runs of the
/// failed stage are skipped, and later stages are reduced from their records.
pub(crate) fn encode_key<L: HandlerList>(
    handlers: &L,
    plan: &Plan<u64>,
    key: &KeyPlan<u64>,
    records: &[ParsedRecord],
    failure: Option<KeyFailure>,
) -> Vec<RetryEntry> {
    let (start, failed) = match failure {
        Some(failure) => (failure.segment, Some(failure.failed_runs)),
        None => (0, None),
    };
    let failed_kind = failed
        .as_ref()
        .and_then(|runs| runs.first())
        .map(|run| StageKind::from(run.stage));
    let mut failed = failed.unwrap_or_default();

    let mut entries = Vec::new();
    for (index, segment) in plan.segments(key).iter().enumerate().skip(start) {
        let in_failed_segment = index == start && failed_kind.is_some();
        let head = plan.head(segment).map(|run| (Stage::Head, run));
        let body = plan
            .body(segment)
            .iter()
            .enumerate()
            .map(|(i, run)| (Stage::Body(i), run));
        let tail = plan.tail(segment).map(|run| (Stage::Tail, run));
        for (stage, run) in head.into_iter().chain(body).chain(tail) {
            let kind = StageKind::from(stage);
            let payloads = match failed_kind.filter(|_| in_failed_segment) {
                Some(failed_kind) if kind < failed_kind => continue,
                Some(failed_kind) if kind == failed_kind => {
                    match failed.iter().position(|run| run.stage == stage) {
                        Some(position) => failed.swap_remove(position).remainder,
                        None => continue,
                    }
                }
                _ => reduce(handlers, plan, run, records),
            };
            push_run(handlers, &mut entries, *key.key(), kind, run, payloads);
        }
    }
    entries
}

fn reduce<L: HandlerList>(
    handlers: &L,
    plan: &Plan<u64>,
    run: &Run,
    records: &[ParsedRecord],
) -> Vec<Box<[u8]>> {
    let run_records: Vec<RunRecord<'_>> = plan
        .records(run)
        .iter()
        .map(|&i| RunRecord {
            payload: &records[i].payload,
            associativity: records[i].headers.properties.associativity,
        })
        .collect();
    handlers.reduce_run(run.tag(), &run_records)
}

fn push_run<L: HandlerList>(
    handlers: &L,
    entries: &mut Vec<RetryEntry>,
    key: u64,
    kind: StageKind,
    run: &Run,
    payloads: Vec<Box<[u8]>>,
) {
    let tag = run.tag();
    let Some(associativity) = handlers.associativity(tag) else {
        tracing::error!(
            key,
            tag = tag.get(),
            "dropping events with unknown type tag"
        );
        return;
    };
    let orderings = stage_orderings(kind, payloads.len());
    entries.extend(
        payloads
            .into_iter()
            .zip(orderings)
            .map(|(payload, atomic_level)| RetryEntry {
                headers: EventHeaders {
                    tag,
                    key,
                    properties: EventAlgebraicProperties {
                        atomic_level,
                        associativity,
                    },
                },
                payload,
            }),
    );
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    use crate::consumer::plan::PlanRecord;
    use crate::consumer::plan::test_support::*;

    /// Plan `records`, then write every run back out the way [`encode_key`]
    /// does, without reduction. Returns the written records and, for each of
    /// them, the index of the original record.
    fn reencode(records: &[PlanRecord<u8>]) -> (Vec<PlanRecord<u8>>, Vec<usize>) {
        let plan = Plan::new(records);
        let mut written = Vec::new();
        let mut origin = Vec::new();
        for key in plan.keys() {
            for segment in plan.segments(key) {
                let head = plan.head(segment).map(|run| (StageKind::Head, run));
                let body = plan.body(segment).iter().map(|run| (StageKind::Body, run));
                let tail = plan.tail(segment).map(|run| (StageKind::Tail, run));
                for (kind, run) in head.into_iter().chain(body).chain(tail) {
                    let indices = plan.records(run);
                    for (&i, ordering) in indices.iter().zip(stage_orderings(kind, indices.len())) {
                        written.push(PlanRecord {
                            ordering,
                            ..records[i]
                        });
                        origin.push(i);
                    }
                }
            }
        }
        (written, origin)
    }

    fn assert_replan_preserves_order(records: &[PlanRecord<u8>]) {
        let (written, origin) = reencode(records);
        let replanned = places(&written, &Plan::new(&written));
        let mut new_index = vec![0; records.len()];
        for (new, &old) in origin.iter().enumerate() {
            new_index[old] = new;
        }
        // Only orders the original records require are checked. The original
        // plan can be stricter: a `Release` inside a head run is written as
        // `Relaxed`, so the replanned head may absorb later events of its type.
        for i in 0..records.len() {
            for j in i + 1..records.len() {
                if required(&records[i], &records[j]) {
                    assert!(
                        precedes(replanned[new_index[i]], replanned[new_index[j]]),
                        "{records:?}: record {i} must precede record {j}, but not after \
                         re-encoding as {written:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn retry_encoding_preserves_plan_order() {
        let two_keys: Vec<_> = [0, 1]
            .into_iter()
            .flat_map(|key| [1, 2].into_iter().map(move |tag| (key, tag)))
            .flat_map(|(key, tag)| ORDERINGS.map(|ordering| record(key, tag, ordering)))
            .collect();
        for_each_sequence(&two_keys, 4, assert_replan_preserves_order);

        let one_key: Vec<_> = [1, 2, 3]
            .into_iter()
            .flat_map(|tag| ORDERINGS.map(|ordering| record(0, tag, ordering)))
            .collect();
        for_each_sequence(&one_key, 5, assert_replan_preserves_order);
    }
}
