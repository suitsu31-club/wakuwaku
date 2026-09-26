//! What a consumer does with a message its processor failed on.
//!
//! Every consumer queue `Q` gets two companion queues, declared by
//! [`setup_consumer`](super::setup_consumer):
//!
//! - `Q.retry` holds messages waiting for another attempt. It has no consumer. Each message
//!   carries its own delay as the per-message TTL (`expiration`), and when that runs out the
//!   broker dead-letters the message through the default exchange back into `Q`.
//! - `Q.dlq` holds messages that are parked for good: retries ran out, or the error was one
//!   that retrying cannot fix. Nothing consumes it; inspect or move the messages by hand.
//!
//! `Q` itself is declared exactly as before (durable, no arguments), so an existing queue is
//! reused as it is.
//!
//! A message is handed to `Q.retry` or `Q.dlq` by publishing a copy there (persistent, with
//! publisher confirms) and only then acknowledging the original, so it is never lost in
//! between. The copy carries the headers below.
//!
//! Messages in `Q.retry` expire in queue order: one with a short delay waits behind one with
//! a longer delay that is ahead of it. A retry can therefore come later than its own backoff,
//! but never later than [`RetryPolicy::max_delay`] after the failure. A processor that must
//! not wait long gives itself a short `max_delay`.

use crate::error::Error;
use amqprs::{
    BasicProperties, DELIVERY_MODE_PERSISTENT, FieldName, FieldTable, FieldValue, LongStr,
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Header counting how many attempts at the message have failed so far.
pub const FAILED_ATTEMPTS_HEADER: &str = "wakuwaku-failed-attempts";
/// Header with the error of the last failed attempt.
pub const LAST_ERROR_HEADER: &str = "wakuwaku-last-error";
/// Header with the consumer queue the message failed in.
pub const QUEUE_HEADER: &str = "wakuwaku-queue";
/// Header on parked messages: `retries-exhausted` or `permanent-error`.
pub const DEAD_LETTER_REASON_HEADER: &str = "wakuwaku-dead-letter-reason";
/// Header on parked messages: when the message was parked, in Unix seconds.
pub const DEAD_LETTERED_AT_HEADER: &str = "wakuwaku-dead-lettered-at";

/// Longest error text kept in [`LAST_ERROR_HEADER`], in bytes.
const MAX_ERROR_LEN: usize = 1024;

/// Name of the queue holding `queue`'s messages until their next attempt.
pub fn retry_queue_name(queue: &str) -> String {
    format!("{queue}.retry")
}

/// Name of the queue where `queue`'s messages are parked for good.
pub fn dead_letter_queue_name(queue: &str) -> String {
    format!("{queue}.dlq")
}

/// How often and how fast a consumer retries a message that failed with a retryable error
/// ([`FailureAction::Retry`]).
///
/// The delay after the n-th failed attempt is `initial_delay * multiplier^(n-1)`, capped at
/// `max_delay`. After `max_attempts` failed attempts the message is parked in the
/// dead-letter queue, or dropped if `park_when_exhausted` is `false`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Attempts in total, the first delivery included. 1 parks a message on its first failure.
    pub max_attempts: u32,
    /// Delay before the first retry.
    pub initial_delay: Duration,
    /// Factor the delay grows by after every failed attempt.
    pub multiplier: u32,
    /// Upper bound of a single delay.
    pub max_delay: Duration,
    /// Whether a message whose attempts ran out is parked in the dead-letter queue (`true`) or
    /// acknowledged and dropped (`false`), for messages that are worthless once they are late.
    pub park_when_exhausted: bool,
}

impl RetryPolicy {
    /// 10 attempts, waiting 1 s, 4 s, 16 s, 64 s, 256 s and then 10 min between them: a message
    /// is parked about 46 minutes after its first failure, later if other messages were waiting
    /// in the retry queue ahead of it.
    pub const DEFAULT: Self = Self {
        max_attempts: 10,
        initial_delay: Duration::from_secs(1),
        multiplier: 4,
        max_delay: Duration::from_secs(600),
        park_when_exhausted: true,
    };

    /// This policy with a different number of attempts.
    pub const fn with_max_attempts(self, max_attempts: u32) -> Self {
        Self {
            max_attempts,
            ..self
        }
    }

    /// This policy with a different backoff.
    pub const fn with_backoff(
        self,
        initial_delay: Duration,
        multiplier: u32,
        max_delay: Duration,
    ) -> Self {
        Self {
            initial_delay,
            multiplier,
            max_delay,
            ..self
        }
    }

    /// This policy, dropping a message whose attempts ran out instead of parking it.
    pub const fn discard_when_exhausted(self) -> Self {
        Self {
            park_when_exhausted: false,
            ..self
        }
    }

    /// How long to wait after the `failed_attempts`-th failed attempt, or `None` when the
    /// message has had all its attempts.
    pub fn next_delay(&self, failed_attempts: u32) -> Option<Duration> {
        if failed_attempts >= self.max_attempts {
            return None;
        }
        let exponent = failed_attempts.saturating_sub(1);
        let delay = self
            .multiplier
            .max(1)
            .checked_pow(exponent)
            .and_then(|factor| self.initial_delay.checked_mul(factor))
            .unwrap_or(self.max_delay);
        Some(delay.min(self.max_delay))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// What a consumer does with a message whose processing returned an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureAction {
    /// Try again later, following the processor's [`RetryPolicy`]; park the message in the
    /// dead-letter queue once the attempts run out (or drop it, see
    /// [`RetryPolicy::park_when_exhausted`]).
    Retry,
    /// Park the message in the dead-letter queue right away: retrying cannot help.
    DeadLetter,
    /// Acknowledge and drop the message. For errors that are the answer to the message
    /// itself, such as a request for something that does not exist.
    Discard,
}

impl FailureAction {
    /// The default decision for each [`Error`] variant:
    ///
    /// | error | action |
    /// |---|---|
    /// | `DatabaseError` with SQLSTATE class 22 (data exception) or 23 (integrity constraint violation), or a row/column decode error | `DeadLetter` |
    /// | any other `DatabaseError`, `RedisError`, `SurrealDbError`, `AmqpError`, `Io` | `Retry` |
    /// | `SerializeError`, `DeserializeError`, `BusinessPanic` | `DeadLetter` |
    /// | `InvalidInput`, `NotFound`, `PermissionsDenied` | `Discard` |
    pub fn for_error(error: &Error) -> Self {
        match error {
            #[cfg(feature = "sqlx")]
            Error::DatabaseError(e) => {
                if is_permanent_database_error(e) {
                    Self::DeadLetter
                } else {
                    Self::Retry
                }
            }
            #[cfg(feature = "surreal")]
            Error::SurrealDbError(_) => Self::Retry,
            #[cfg(feature = "redis")]
            Error::RedisError(_) => Self::Retry,
            Error::AmqpError(_) | Error::Io(_) => Self::Retry,
            Error::SerializeError(_) | Error::DeserializeError(_) | Error::BusinessPanic(_) => {
                Self::DeadLetter
            }
            Error::InvalidInput | Error::NotFound | Error::PermissionsDenied => Self::Discard,
        }
    }
}

/// Whether running the same statement again can never succeed: the data itself is rejected
/// (SQLSTATE class 22 or 23) or cannot be decoded. Connection, pool, lock and serialization
/// failures, and anything unknown, count as transient.
#[cfg(feature = "sqlx")]
pub fn is_permanent_database_error(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(e) => e.code().is_some_and(|code| is_permanent_sqlstate(&code)),
        sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::ColumnIndexOutOfBounds { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Encode(_)
        | sqlx::Error::Decode(_)
        | sqlx::Error::InvalidArgument(_) => true,
        _ => false,
    }
}

#[cfg(feature = "sqlx")]
fn is_permanent_sqlstate(code: &str) -> bool {
    code.starts_with("22") || code.starts_with("23")
}

/// Failed attempts recorded on a delivered message; 0 on its first delivery.
pub(crate) fn failed_attempts(properties: &BasicProperties) -> u32 {
    let Some(value) = properties
        .headers()
        .and_then(|headers| headers.get(&field_name(FAILED_ATTEMPTS_HEADER)))
    else {
        return 0;
    };
    let count: i64 = match value {
        FieldValue::b(v) => (*v).into(),
        FieldValue::B(v) => (*v).into(),
        FieldValue::s(v) => (*v).into(),
        FieldValue::u(v) => (*v).into(),
        FieldValue::I(v) => (*v).into(),
        FieldValue::i(v) => (*v).into(),
        FieldValue::l(v) => *v,
        _ => 0,
    };
    u32::try_from(count.max(0)).unwrap_or(u32::MAX)
}

/// Where a failed message goes.
pub(crate) enum Destination {
    /// Back into the queue after `delay`, through the retry queue.
    Retry { delay: Duration },
    /// Parked in the dead-letter queue.
    DeadLetter { reason: &'static str },
}

/// Properties for the copy of a failed message sent to its retry or dead-letter queue.
///
/// Keeps the application's properties and headers, drops what the broker added when the
/// message was dead-lettered before (`x-death` and friends) and `user-id` (the broker checks
/// it against the publishing user), makes the copy persistent and records the failure.
pub(crate) fn forwarded_properties(
    original: &BasicProperties,
    queue: &str,
    failed_attempts: u32,
    error: &str,
    destination: &Destination,
) -> BasicProperties {
    let mut properties = BasicProperties::default();
    if let Some(v) = original.content_type() {
        properties.with_content_type(v);
    }
    if let Some(v) = original.content_encoding() {
        properties.with_content_encoding(v);
    }
    if let Some(v) = original.priority() {
        properties.with_priority(v);
    }
    if let Some(v) = original.correlation_id() {
        properties.with_correlation_id(v);
    }
    if let Some(v) = original.reply_to() {
        properties.with_reply_to(v);
    }
    if let Some(v) = original.message_id() {
        properties.with_message_id(v);
    }
    if let Some(v) = original.timestamp() {
        properties.with_timestamp(v);
    }
    if let Some(v) = original.message_type() {
        properties.with_message_type(v);
    }
    if let Some(v) = original.app_id() {
        properties.with_app_id(v);
    }
    properties.with_delivery_mode(DELIVERY_MODE_PERSISTENT);

    let mut headers = FieldTable::new();
    if let Some(original_headers) = original.headers() {
        for (name, value) in original_headers.as_ref() {
            let key: &String = name.as_ref();
            let added_by_broker = key == "x-death"
                || key.starts_with("x-first-death-")
                || key.starts_with("x-last-death-");
            if !added_by_broker && !key.starts_with("wakuwaku-") {
                headers.insert(name.clone(), value.clone());
            }
        }
    }
    headers.insert(
        field_name(FAILED_ATTEMPTS_HEADER),
        FieldValue::l(failed_attempts.into()),
    );
    headers.insert(field_name(LAST_ERROR_HEADER), long_str(error));
    headers.insert(field_name(QUEUE_HEADER), long_str(queue));

    match destination {
        Destination::Retry { delay } => {
            properties.with_expiration(&delay.as_millis().to_string());
        }
        Destination::DeadLetter { reason } => {
            headers.insert(field_name(DEAD_LETTER_REASON_HEADER), long_str(reason));
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_default();
            headers.insert(field_name(DEAD_LETTERED_AT_HEADER), FieldValue::T(now));
        }
    }
    properties.with_headers(headers);
    properties.finish()
}

fn field_name(name: &str) -> FieldName {
    name.try_into().expect("header names are short")
}

fn long_str(text: &str) -> FieldValue {
    let mut end = text.len().min(MAX_ERROR_LEN);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    FieldValue::S(
        LongStr::try_from(&text[..end]).expect("a string of at most MAX_ERROR_LEN bytes fits"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    #[test]
    fn default_policy_backs_off_exponentially_up_to_the_cap_then_gives_up() {
        let policy = RetryPolicy::DEFAULT;
        let delays: Vec<_> = (1..=10).map(|n| policy.next_delay(n)).collect();
        assert_eq!(
            delays,
            [
                Some(secs(1)),
                Some(secs(4)),
                Some(secs(16)),
                Some(secs(64)),
                Some(secs(256)),
                Some(secs(600)),
                Some(secs(600)),
                Some(secs(600)),
                Some(secs(600)),
                None,
            ]
        );
    }

    #[test]
    fn huge_exponents_saturate_at_the_cap() {
        let policy = RetryPolicy::DEFAULT.with_max_attempts(u32::MAX);
        assert_eq!(policy.next_delay(1_000), Some(secs(600)));
    }

    #[test]
    fn one_attempt_parks_on_the_first_failure() {
        assert_eq!(
            RetryPolicy::DEFAULT.with_max_attempts(1).next_delay(1),
            None
        );
    }

    #[cfg(feature = "sqlx")]
    #[test]
    fn only_data_and_constraint_errors_are_permanent() {
        for (code, permanent) in [
            ("23503", true),  // foreign_key_violation
            ("23001", true),  // restrict_violation
            ("22003", true),  // numeric_value_out_of_range
            ("40001", false), // serialization_failure
            ("40P01", false), // deadlock_detected
            ("08006", false), // connection_failure
            ("57P01", false), // admin_shutdown
            ("53300", false), // too_many_connections
            ("42P01", false), // undefined_table: a migration may still be on its way
        ] {
            assert_eq!(is_permanent_sqlstate(code), permanent, "{code}");
        }
        assert!(!is_permanent_database_error(&sqlx::Error::PoolTimedOut));
        assert!(!is_permanent_database_error(&sqlx::Error::RowNotFound));
        assert!(is_permanent_database_error(&sqlx::Error::ColumnNotFound(
            "id".into()
        )));
    }

    fn with_headers(entries: &[(&str, FieldValue)]) -> BasicProperties {
        let mut headers = FieldTable::new();
        for (name, value) in entries {
            headers.insert(field_name(name), value.clone());
        }
        BasicProperties::default().with_headers(headers).finish()
    }

    #[test]
    fn failed_attempts_reads_any_integer_header() {
        assert_eq!(failed_attempts(&BasicProperties::default()), 0);
        for value in [
            FieldValue::l(3),
            FieldValue::I(3),
            FieldValue::u(3),
            FieldValue::B(3),
        ] {
            assert_eq!(
                failed_attempts(&with_headers(&[(FAILED_ATTEMPTS_HEADER, value)])),
                3
            );
        }
        assert_eq!(
            failed_attempts(&with_headers(&[(
                FAILED_ATTEMPTS_HEADER,
                FieldValue::l(-5)
            )])),
            0
        );
        assert_eq!(
            failed_attempts(&with_headers(&[(FAILED_ATTEMPTS_HEADER, "3".into())])),
            0
        );
    }

    #[test]
    fn forwarded_copy_is_persistent_and_drops_broker_headers() {
        let mut original = with_headers(&[
            ("x-death", FieldValue::V),
            ("x-first-death-queue", "q.retry".into()),
            ("trace-id", "abc".into()),
            (FAILED_ATTEMPTS_HEADER, FieldValue::l(1)),
        ]);
        original
            .with_user_id("guest")
            .with_expiration("1000")
            .with_message_id("m-1");

        let retry = forwarded_properties(
            &original,
            "q",
            2,
            "boom",
            &Destination::Retry { delay: secs(4) },
        );
        assert_eq!(retry.delivery_mode(), Some(DELIVERY_MODE_PERSISTENT));
        assert_eq!(retry.expiration().map(String::as_str), Some("4000"));
        assert_eq!(retry.message_id().map(String::as_str), Some("m-1"));
        assert_eq!(retry.user_id(), None);
        assert_eq!(failed_attempts(&retry), 2);
        let headers = retry.headers().unwrap();
        assert!(headers.get(&field_name("x-death")).is_none());
        assert!(headers.get(&field_name("x-first-death-queue")).is_none());
        assert!(headers.get(&field_name("trace-id")).is_some());

        let parked = forwarded_properties(
            &original,
            "q",
            10,
            &"e".repeat(5000),
            &Destination::DeadLetter {
                reason: "retries-exhausted",
            },
        );
        assert_eq!(parked.expiration(), None);
        let headers = parked.headers().unwrap();
        assert!(headers.get(&field_name(DEAD_LETTERED_AT_HEADER)).is_some());
        match headers.get(&field_name(LAST_ERROR_HEADER)) {
            Some(FieldValue::S(text)) => assert_eq!(AsRef::<String>::as_ref(text).len(), 1024),
            other => panic!("unexpected error header {other:?}"),
        }
    }
}
