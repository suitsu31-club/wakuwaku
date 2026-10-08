//! In-memory schedule of the records read from one retry partition.
//!
//! A key is quarantined while it has a pending record: its later events are
//! diverted to the retry partition so they stay behind the failed ones.

use crate::consumer::record::ParsedRecord;
use crate::utils::backoff::delay_for;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub(crate) struct PendingRecord {
    /// The record, with its offset in the retry partition.
    pub record: ParsedRecord,
    pub failed_at_ms: u64,
}

#[derive(Debug)]
struct KeyRetry {
    /// In offset order.
    records: VecDeque<PendingRecord>,
    /// Attempts that failed since the key appeared or last gave up.
    attempt: usize,
    due_at: Instant,
}

#[derive(Debug)]
pub(crate) struct RetryState {
    keys: HashMap<u64, KeyRetry>,
    /// Records over all keys.
    pending: usize,
    delays: &'static [Duration],
}

/// What happened to a key after a retry attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Finish {
    /// No records are left; the key is no longer quarantined.
    Drained,
    /// The key runs again after `delay`.
    Scheduled { attempt: usize, delay: Duration },
    /// The last attempt failed; the records of the failed runs were dropped.
    GaveUp { dropped: usize },
}

fn later(now: Instant, delay: Duration) -> Instant {
    now.checked_add(delay).unwrap_or(now)
}

impl RetryState {
    pub(crate) fn new(delays: &'static [Duration]) -> Self {
        Self {
            keys: HashMap::new(),
            pending: 0,
            delays,
        }
    }

    /// Add a record read from the retry partition, in offset order.
    ///
    /// A new key is due `delays[0]` after its record was written.
    pub(crate) fn enqueue(&mut self, record: PendingRecord, now: Instant, now_ms: u64) {
        let delays = self.delays;
        let key = self
            .keys
            .entry(record.record.headers.key)
            .or_insert_with(|| {
                let elapsed = Duration::from_millis(now_ms.saturating_sub(record.failed_at_ms));
                KeyRetry {
                    records: VecDeque::new(),
                    attempt: 0,
                    due_at: later(now, delay_for(delays, 0).saturating_sub(elapsed)),
                }
            });
        key.records.push_back(record);
        self.pending = self.pending.saturating_add(1);
    }

    pub(crate) fn is_quarantined(&self, key: u64) -> bool {
        self.keys.contains_key(&key)
    }

    pub(crate) fn pending_len(&self) -> usize {
        self.pending
    }

    pub(crate) fn due_keys(&self, now: Instant) -> Vec<u64> {
        self.keys
            .iter()
            .filter(|(_, retry)| retry.due_at <= now)
            .map(|(&key, _)| key)
            .collect()
    }

    pub(crate) fn next_due(&self) -> Option<Instant> {
        self.keys.values().map(|retry| retry.due_at).min()
    }

    /// Every pending record of `key`, in offset order.
    pub(crate) fn records(&self, key: u64) -> Vec<ParsedRecord> {
        self.keys
            .get(&key)
            .map(|retry| {
                retry
                    .records
                    .iter()
                    .map(|pending| pending.record.clone())
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Record the outcome of an attempt on `key`: the offsets of records in
    /// completed runs and of records in failed runs.
    pub(crate) fn finish(
        &mut self,
        key: u64,
        resolved: &[u64],
        failed: &[u64],
        now: Instant,
    ) -> Finish {
        let Some(retry) = self.keys.get_mut(&key) else {
            return Finish::Drained;
        };
        let removed = remove_offsets(&mut retry.records, resolved);
        self.pending = self.pending.saturating_sub(removed);

        let finish = if failed.is_empty() {
            retry.due_at = now;
            Finish::Scheduled {
                attempt: retry.attempt,
                delay: Duration::ZERO,
            }
        } else {
            let next = retry.attempt.saturating_add(1);
            match self.delays.get(next) {
                Some(&delay) => {
                    retry.attempt = next;
                    retry.due_at = later(now, delay);
                    Finish::Scheduled {
                        attempt: next,
                        delay,
                    }
                }
                None => {
                    let dropped = remove_offsets(&mut retry.records, failed);
                    self.pending = self.pending.saturating_sub(dropped);
                    retry.attempt = 0;
                    retry.due_at = now;
                    Finish::GaveUp { dropped }
                }
            }
        };

        if retry.records.is_empty() {
            self.keys.remove(&key);
            if failed.is_empty() {
                return Finish::Drained;
            }
        }
        finish
    }

    /// Offset to store for the retry partition: just before the first pending
    /// record, or the last read record when nothing is pending.
    pub(crate) fn committable(&self, next_read: u64) -> Option<u64> {
        let first_pending = self
            .keys
            .values()
            .filter_map(|retry| retry.records.front())
            .map(|pending| pending.record.offset)
            .min();
        first_pending.unwrap_or(next_read).checked_sub(1)
    }
}

fn remove_offsets(records: &mut VecDeque<PendingRecord>, offsets: &[u64]) -> usize {
    if offsets.is_empty() {
        return 0;
    }
    let offsets: HashSet<u64> = offsets.iter().copied().collect();
    let before = records.len();
    records.retain(|pending| !offsets.contains(&pending.record.offset));
    before.saturating_sub(records.len())
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    use crate::consumer::DEFAULT_RETRY_DELAYS;
    use crate::events::{
        EventAlgebraicProperties, EventAssociativity, EventAtomicOrdering, EventTypeTag,
    };
    use crate::headers::EventHeaders;
    use bytes::Bytes;

    const FAILED_AT_MS: u64 = 1_000_000;

    fn pending(key: u64, offset: u64) -> PendingRecord {
        PendingRecord {
            record: ParsedRecord {
                offset,
                headers: EventHeaders {
                    tag: EventTypeTag::new(1),
                    key,
                    properties: EventAlgebraicProperties {
                        atomic_level: EventAtomicOrdering::Relaxed,
                        associativity: EventAssociativity::NonAssociative,
                    },
                },
                payload: Bytes::new(),
            },
            failed_at_ms: FAILED_AT_MS,
        }
    }

    fn offsets(state: &RetryState, key: u64) -> Vec<u64> {
        state.records(key).iter().map(|r| r.offset).collect()
    }

    #[test]
    fn default_delays_double_and_give_up_after_the_fourth_retry() {
        let mut state = RetryState::new(DEFAULT_RETRY_DELAYS);
        let t0 = Instant::now();
        state.enqueue(pending(7, 0), t0, FAILED_AT_MS);

        let mut now = t0 + Duration::from_secs(2);
        assert!(state.due_keys(now - Duration::from_millis(1)).is_empty());
        for seconds in [4, 8, 16] {
            assert_eq!(state.due_keys(now), [7]);
            let finish = state.finish(7, &[], &[0], now);
            let delay = Duration::from_secs(seconds);
            assert!(matches!(finish, Finish::Scheduled { delay: d, .. } if d == delay));
            assert_eq!(state.next_due(), Some(now + delay));
            now += delay;
        }
        assert_eq!(state.due_keys(now), [7]);
        assert_eq!(
            state.finish(7, &[], &[0], now),
            Finish::GaveUp { dropped: 1 }
        );
        assert!(!state.is_quarantined(7));
        assert_eq!(state.pending_len(), 0);
    }

    #[test]
    fn giving_up_drops_only_failed_records_and_leaves_the_key_due() {
        let mut state = RetryState::new(DEFAULT_RETRY_DELAYS);
        let t0 = Instant::now();
        state.enqueue(pending(7, 1), t0, FAILED_AT_MS);
        state.enqueue(pending(7, 2), t0, FAILED_AT_MS);
        let mut now = t0;
        for _ in 1..DEFAULT_RETRY_DELAYS.len() {
            assert!(matches!(
                state.finish(7, &[], &[1], now),
                Finish::Scheduled { .. }
            ));
            now += Duration::from_secs(60);
        }
        assert_eq!(
            state.finish(7, &[], &[1], now),
            Finish::GaveUp { dropped: 1 }
        );
        assert_eq!(offsets(&state, 7), [2]);
        assert_eq!(state.pending_len(), 1);
        assert_eq!(state.due_keys(now), [7]);
        // The attempt counter restarts.
        let next = state.finish(7, &[], &[2], now);
        assert_eq!(
            next,
            Finish::Scheduled {
                attempt: 1,
                delay: DEFAULT_RETRY_DELAYS[1]
            }
        );
    }

    #[test]
    fn quarantine_is_lifted_only_when_drained() {
        let mut state = RetryState::new(DEFAULT_RETRY_DELAYS);
        let t0 = Instant::now();
        state.enqueue(pending(7, 1), t0, FAILED_AT_MS);
        state.enqueue(pending(7, 2), t0, FAILED_AT_MS);
        assert!(matches!(
            state.finish(7, &[1], &[], t0),
            Finish::Scheduled { attempt: 0, .. }
        ));
        assert!(state.is_quarantined(7));
        assert_eq!(state.due_keys(t0), [7]);
        assert_eq!(state.finish(7, &[2], &[], t0), Finish::Drained);
        assert!(!state.is_quarantined(7));
        assert_eq!(state.pending_len(), 0);
    }

    #[test]
    fn committable_stays_below_the_first_pending_offset() {
        let mut state = RetryState::new(DEFAULT_RETRY_DELAYS);
        let t0 = Instant::now();
        state.enqueue(pending(1, 5), t0, FAILED_AT_MS);
        state.enqueue(pending(2, 7), t0, FAILED_AT_MS);
        state.enqueue(pending(1, 9), t0, FAILED_AT_MS);
        assert_eq!(state.committable(20), Some(4));
        state.finish(1, &[5], &[], t0);
        assert_eq!(state.committable(20), Some(6));
        state.finish(2, &[7], &[], t0);
        assert_eq!(state.committable(20), Some(8));
        state.finish(1, &[9], &[], t0);
        assert_eq!(state.committable(20), Some(19));

        state.enqueue(pending(3, 0), t0, FAILED_AT_MS);
        assert_eq!(state.committable(20), None);
    }

    #[test]
    fn old_record_is_due_immediately_on_startup() {
        let mut state = RetryState::new(DEFAULT_RETRY_DELAYS);
        let t0 = Instant::now();
        state.enqueue(pending(7, 0), t0, FAILED_AT_MS + 60_000);
        assert_eq!(state.due_keys(t0), [7]);

        let mut state = RetryState::new(DEFAULT_RETRY_DELAYS);
        state.enqueue(pending(7, 0), t0, FAILED_AT_MS + 500);
        assert_eq!(state.next_due(), Some(t0 + Duration::from_millis(1500)));
    }
}
