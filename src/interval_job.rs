#[cfg(feature = "amqprs")]
/// Trait for time-driven jobs that emit AMQP execution signals.
pub trait IntervalJobExecutionSignal: crate::amqp::AmqpMessageSend {
    /// Build a signal for the current scheduling tick.
    fn tick(now: time::OffsetDateTime) -> Self;
    /// Decide whether to emit a signal for the interval between two timestamps.
    fn time_pool(
        now: time::OffsetDateTime,
        last_time: time::OffsetDateTime,
    ) -> std::task::Poll<Self>;
}
