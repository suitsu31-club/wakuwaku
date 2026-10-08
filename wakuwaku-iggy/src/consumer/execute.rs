//! Executing the plan of one key.

use crate::consumer::handler::{HandlerList, RunRecord, RunResult};
use crate::consumer::record::ParsedRecord;
use crate::partition::plan::{KeyPlan, Plan, Run};
use futures_util::future::join_all;
use std::time::Duration;

/// Position of a run inside its segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Stage {
    Head,
    /// Index into [`Plan::body`] of the segment.
    Body(usize),
    Tail,
}

/// A run that failed with a retryable error.
#[derive(Debug)]
pub(crate) struct FailedRun {
    pub stage: Stage,
    /// Indices of the run's records.
    pub records: Vec<usize>,
    /// The failed chunk and every chunk after it, serialized.
    pub remainder: Vec<Box<[u8]>>,
}

/// Where and why a key stopped.
#[derive(Debug)]
pub(crate) struct KeyFailure {
    /// Index into the key's segments.
    pub segment: usize,
    /// Error of the first failed run.
    pub error: anyhow::Error,
    /// Every failed run, all in the same stage of `segment`.
    pub failed_runs: Vec<FailedRun>,
}

#[derive(Debug)]
pub(crate) struct KeyReport {
    /// Indices of the records of every completed run.
    pub completed: Vec<usize>,
    pub failure: Option<KeyFailure>,
}

async fn run_one<L: HandlerList>(
    handlers: &L,
    plan: &Plan<u64>,
    run: &Run,
    records: &[ParsedRecord],
    delays: &'static [Duration],
) -> RunResult {
    let run_records: Vec<RunRecord<'_>> = plan
        .records(run)
        .iter()
        .map(|&i| RunRecord {
            payload: &records[i].payload,
            associativity: records[i].headers.properties.associativity,
        })
        .collect();
    handlers.handle_run(run.tag(), &run_records, delays).await
}

/// Results of one stage of a segment.
struct StageOutcome {
    error: Option<anyhow::Error>,
    failed_runs: Vec<FailedRun>,
}

impl StageOutcome {
    fn new() -> Self {
        Self {
            error: None,
            failed_runs: Vec::new(),
        }
    }

    fn settle(
        &mut self,
        key: u64,
        plan: &Plan<u64>,
        stage: Stage,
        run: &Run,
        result: RunResult,
        completed: &mut Vec<usize>,
    ) {
        match result {
            RunResult::Done => completed.extend_from_slice(plan.records(run)),
            RunResult::Failed { error, remainder } => {
                if self.error.is_none() {
                    self.error = Some(error);
                } else {
                    tracing::warn!(key, tag = run.tag().get(), %error, "another run of the key failed");
                }
                self.failed_runs.push(FailedRun {
                    stage,
                    records: plan.records(run).to_vec(),
                    remainder,
                });
            }
        }
    }
}

/// Execute every segment of `key` in order, stopping after the first stage
/// with a failed run.
pub(crate) async fn execute_key<L: HandlerList>(
    handlers: &L,
    plan: &Plan<u64>,
    key: &KeyPlan<u64>,
    records: &[ParsedRecord],
    delays: &'static [Duration],
) -> KeyReport {
    let key_hash = *key.key();
    let mut completed = Vec::new();
    for (index, segment) in plan.segments(key).iter().enumerate() {
        let mut outcome = StageOutcome::new();
        if let Some(run) = plan.head(segment) {
            let result = run_one(handlers, plan, run, records, delays).await;
            outcome.settle(key_hash, plan, Stage::Head, run, result, &mut completed);
        }
        if outcome.error.is_none() {
            let body = plan.body(segment);
            let results = join_all(
                body.iter()
                    .map(|run| run_one(handlers, plan, run, records, delays)),
            )
            .await;
            for (i, (run, result)) in body.iter().zip(results).enumerate() {
                outcome.settle(key_hash, plan, Stage::Body(i), run, result, &mut completed);
            }
        }
        if outcome.error.is_none()
            && let Some(run) = plan.tail(segment)
        {
            let result = run_one(handlers, plan, run, records, delays).await;
            outcome.settle(key_hash, plan, Stage::Tail, run, result, &mut completed);
        }
        if let Some(error) = outcome.error {
            return KeyReport {
                completed,
                failure: Some(KeyFailure {
                    segment: index,
                    error,
                    failed_runs: outcome.failed_runs,
                }),
            };
        }
    }
    KeyReport {
        completed,
        failure: None,
    }
}
