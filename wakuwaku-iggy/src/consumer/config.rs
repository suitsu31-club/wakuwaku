//! Consumer configuration.

use std::time::Duration;

/// Delays between retry attempts of a failed key: 2, 4, 8 and 16 seconds.
pub const DEFAULT_RETRY_DELAYS: &[Duration] = &[
    Duration::from_secs(2),
    Duration::from_secs(4),
    Duration::from_secs(8),
    Duration::from_secs(16),
];

/// When the consumer stores its offsets, which decides what a crash costs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeliveryMode {
    /// At most once. Offsets are stored right after polling, before the
    /// events are processed.
    ///
    /// A crash loses every event polled but not yet processed. No event is
    /// processed twice.
    NoDoubleConsume,
    /// At least once. The main offset is stored only after every key of the
    /// batch is done, dropped or acknowledged by the retry topic, and the retry
    /// offset never passes a record that is still pending.
    ///
    /// A crash loses nothing, but events processed since the last stored
    /// offset are processed again. A retry attempt that fails after some
    /// chunks of a run succeeded also applies those chunks again.
    #[default]
    NoEventLose,
}

/// Configuration of [`IggyConsumerRegisterCenter::start`](crate::consumer::IggyConsumerRegisterCenter::start).
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Stream holding the main topic and its retry topic.
    pub stream: String,
    /// Name of the Iggy consumer that stores the offsets.
    pub consumer_name: String,
    /// Partitions of the main topic to consume. Each gets its own task.
    pub partitions: Vec<u32>,
    pub mode: DeliveryMode,
    /// Delays between retry attempts of a failed key, also used between
    /// attempts of transient failures. Must not be empty.
    pub retry_delays: &'static [Duration],
    /// Messages fetched per poll.
    pub poll_count: u32,
    /// Sleep after an empty poll of the main topic.
    pub poll_interval: Duration,
    /// Maximum number of records of one partition waiting in its retry
    /// partition. New failing keys beyond it are dropped.
    pub max_pending_retry: usize,
}

impl ConsumerConfig {
    pub fn new(
        stream: impl Into<String>,
        consumer_name: impl Into<String>,
        partitions: Vec<u32>,
    ) -> Self {
        Self {
            stream: stream.into(),
            consumer_name: consumer_name.into(),
            partitions,
            mode: DeliveryMode::default(),
            retry_delays: DEFAULT_RETRY_DELAYS,
            poll_count: 1000,
            poll_interval: Duration::from_millis(100),
            max_pending_retry: 100_000,
        }
    }
}
