pub use amqprs::channel::ExchangeType as AmqpExchangeType;

mod confirm;
mod retry;

pub use confirm::{CONFIRM_TIMEOUT, ConfirmChannel, PublishOutcome};
#[cfg(feature = "sqlx")]
pub use retry::is_permanent_database_error;
pub use retry::{
    DEAD_LETTER_REASON_HEADER, DEAD_LETTERED_AT_HEADER, FAILED_ATTEMPTS_HEADER, FailureAction,
    LAST_ERROR_HEADER, QUEUE_HEADER, RetryPolicy, dead_letter_queue_name, retry_queue_name,
};

#[cfg(feature = "tracing-otel")]
use tracing::info;

use crate::error::Error;
use crate::pool::Pooled;
use amqprs::channel::{
    BasicAckArguments, BasicConsumeArguments, BasicNackArguments, BasicPublishArguments,
    BasicQosArguments, Channel, ExchangeDeclareArguments, QueueBindArguments,
    QueueDeclareArguments,
};
use amqprs::consumer::AsyncConsumer;
use amqprs::{
    BasicProperties, DELIVERY_MODE_PERSISTENT, DELIVERY_MODE_TRANSIENT, Deliver, FieldTable,
    FieldValue,
};
use confirm::ConfirmState;
use kanau::message::{MessageDe, MessageSer};
use kanau::processor::Processor;
use retry::Destination;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;

/// Pool of AMQP channels opened from a shared connection.
///
/// Every channel is in publisher-confirm mode ([`ConfirmChannel`]). A channel that closed is
/// dropped instead of being handed out again.
pub type AmqpPool = crate::pool::Pool<ConfirmChannel, amqprs::error::Error>;

/// The most channels [`AmqpPool::connect`] keeps checked out at the same time.
pub const DEFAULT_POOL_CAPACITY: usize = 512;

/// Channel numbers [`AmqpPool`] leaves free for channels opened outside of it, such as the
/// consumer channels from [`AmqpPool::factory_create`](crate::pool::Pool::factory_create).
pub const RESERVED_CHANNELS: u16 = 128;

/// How long a consumer waits before it requeues a message it could not hand to its retry or
/// dead-letter queue.
const REQUEUE_DELAY: Duration = Duration::from_secs(5);

impl AmqpPool {
    /// Build a channel pool from an existing AMQP connection.
    ///
    /// The pool holds at most [`DEFAULT_POOL_CAPACITY`] channels, and never more than the
    /// connection's negotiated `channel_max` minus [`RESERVED_CHANNELS`]. When all of them are
    /// in use, [`get`](crate::pool::Pool::get) waits for one to come back.
    pub async fn connect(connection: amqprs::connection::Connection) -> Self {
        Self::connect_with_capacity(connection, DEFAULT_POOL_CAPACITY).await
    }

    /// Like [`connect`](Self::connect) with a different capacity. It is still capped below the
    /// connection's `channel_max`.
    pub async fn connect_with_capacity(
        connection: amqprs::connection::Connection,
        capacity: usize,
    ) -> Self {
        let capacity = capacity.min(max_pooled_channels(connection.channel_max()));
        let factory = move || {
            let connection = connection.clone();
            Box::pin(async move { ConfirmChannel::open(&connection).await })
                as Pin<
                    Box<dyn Future<Output = Result<ConfirmChannel, amqprs::error::Error>> + Send>,
                >
        };
        crate::pool::Pool::with_health_check(Box::pin(factory), capacity, ConfirmChannel::is_usable)
    }
}

/// How many pooled channels a connection with the given `channel_max` leaves room for.
fn max_pooled_channels(channel_max: u16) -> usize {
    // 0 means "no limit" in AMQP: the protocol maximum.
    let channel_max = usize::from(if channel_max == 0 {
        u16::MAX
    } else {
        channel_max
    });
    let reserved = usize::from(RESERVED_CHANNELS).min(channel_max / 2);
    (channel_max - reserved).max(1)
}

/// Trait for routing message to rabbitmq
pub trait AmqpRouting {
    /// Exchange name
    const EXCHANGE: &'static str;

    /// Exchange type
    const EXCHANGE_TYPE: AmqpExchangeType;

    /// Routing key
    const ROUTING_KEY: &'static str;

    // Allow async fn in trait because we don't want the user to override this function
    #[allow(async_fn_in_trait)]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all, err, ret))]
    /// Declare the exchange used by this routing definition.
    fn ensure_exchange(pool: &AmqpPool) -> impl Future<Output = Result<(), Error>> + Send {
        async {
            let channel: Result<Pooled<ConfirmChannel, _>, Error> = pool.get().await.into();
            let channel = channel?;
            let channel = channel
                .get_ref()
                .ok_or(Error::Io(anyhow::anyhow!("Channel is unexpectedly closed")))?;
            channel
                .exchange_declare(
                    ExchangeDeclareArguments::of_type(Self::EXCHANGE, Self::EXCHANGE_TYPE)
                        .durable(true)
                        .finish(),
                )
                .await?;
            Ok(())
        }
    }
}

/// Trait for sending message to rabbitmq
pub trait AmqpMessageSend: MessageSer + Send + Sized + AmqpRouting {
    /// Whether the message is published persistent (delivery mode 2), so that it survives a
    /// broker restart in the durable queues it lands in. Set it to `false` for high-volume
    /// messages that are worthless after a restart.
    const PERSISTENT: bool = true;

    // Allow async fn in trait because we don't want the user to override this function
    #[allow(async_fn_in_trait)]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all, err, ret))]
    /// Send message to rabbitmq
    ///
    /// Returns once the broker has confirmed the message. A message that no queue is bound to
    /// take is dropped by the broker; that is not an error.
    async fn send(self, pool: &AmqpPool) -> Result<(), crate::error::Error> {
        let bytes = self.to_bytes().map_err(|e| e.into())?;
        let channel: Result<Pooled<ConfirmChannel, _>, crate::error::Error> =
            pool.get().await.into();
        let channel = channel?;
        let channel = channel
            .get_ref()
            .ok_or(Error::Io(anyhow::anyhow!("Channel is unexpectedly closed")))?;
        let properties = BasicProperties::default()
            .with_delivery_mode(if Self::PERSISTENT {
                DELIVERY_MODE_PERSISTENT
            } else {
                DELIVERY_MODE_TRANSIENT
            })
            .with_timestamp(unix_now())
            .finish();
        let outcome = channel
            .publish(
                properties,
                bytes.into_vec(),
                BasicPublishArguments::new(Self::EXCHANGE, Self::ROUTING_KEY)
                    .mandatory(true)
                    .finish(),
            )
            .await?;
        if let PublishOutcome::Returned {
            reply_code,
            reply_text,
        } = outcome
        {
            #[cfg(feature = "tracing")]
            tracing::debug!(
                exchange = Self::EXCHANGE,
                routing_key = Self::ROUTING_KEY,
                reply_code,
                reply_text,
                "No queue is bound for the message; the broker dropped it"
            );
            #[cfg(not(feature = "tracing"))]
            let _ = (reply_code, reply_text);
        }

        #[cfg(feature = "tracing-otel")]
        info!(monotonic_counter.mq_event_push = 1);
        Ok(())
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

/// Trait for consuming message from rabbitmq
///
/// When [`process`](Processor::process) fails, [`failure_action`](Self::failure_action)
/// decides whether the message is retried later, parked in the dead-letter queue or dropped;
/// see [`FailureAction`]. Retries follow [`RETRY_POLICY`](Self::RETRY_POLICY). A retried
/// message is processed again from scratch, so a processor should only return a retryable
/// error when running it again is safe.
pub trait AmqpMessageProcessor<Message: AmqpMessageSend + MessageDe>:
    Processor<Message, Output = (), Error = crate::error::Error>
{
    /// Queue name used by this message processor.
    const QUEUE: &'static str;

    /// How often and how fast a message that failed with a retryable error is retried.
    const RETRY_POLICY: RetryPolicy = RetryPolicy::DEFAULT;

    /// How many unacknowledged messages the broker sends the consumer ahead of time
    /// (`basic.qos` prefetch count). 0 means no limit.
    ///
    /// Messages are processed one at a time, so a large prefetch only moves the backlog from
    /// the broker into the consumer. A message that stays unacknowledged longer than the
    /// broker's `consumer_timeout` (30 minutes by default) makes the broker close the channel.
    const PREFETCH: u16 = 32;

    /// What to do with a message whose processing returned `error`.
    fn failure_action(error: &Error) -> FailureAction {
        FailureAction::for_error(error)
    }

    // Allow async fn in trait because we don't want the user to override this function
    #[allow(async_fn_in_trait)]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all, err))]
    /// Ensure the topology of the queue and get the channel with the queue bound
    fn ensure_queue(
        pool: &AmqpPool,
    ) -> impl Future<Output = Result<Channel, crate::error::Error>> + Send {
        async {
            // ensure exchange first
            Message::ensure_exchange(pool).await?;

            // Declare a durable, client-named queue
            let channel = pool.factory_create().await?;
            let queue_arg = QueueDeclareArguments::durable_client_named(Self::QUEUE);
            channel.queue_declare(queue_arg).await?;

            // Bind queue -> exchange with routing key
            let queue_bind_arg =
                QueueBindArguments::new(Self::QUEUE, Message::EXCHANGE, Message::ROUTING_KEY);
            channel.queue_bind(queue_bind_arg).await?;
            Ok(channel.into_channel())
        }
    }
}

/// Consumer for rabbitmq
///
/// Acknowledges a message once its processor succeeded. A failed message is handled as
/// described in [`FailureAction`]: a copy goes to the queue's retry or dead-letter queue
/// ([`retry_queue_name`], [`dead_letter_queue_name`]) and the original is acknowledged once the
/// broker confirmed the copy. If the copy cannot be published, the original is requeued after
/// a few seconds instead, never right away.
pub struct AmqpMessageConsumer<
    Message: AmqpMessageSend + MessageDe,
    Inner: AmqpMessageProcessor<Message>,
> {
    inner: Arc<Inner>,
    /// Confirm tracking of the consumer's channel, used to publish retries and parked messages.
    confirms: OnceCell<Arc<ConfirmState>>,
    _marker: PhantomData<Message>,
}

impl<Message: AmqpMessageSend + MessageDe, Inner: AmqpMessageProcessor<Message>>
    AmqpMessageConsumer<Message, Inner>
{
    /// Create a new consumer
    ///
    /// The first time a message fails, the consumer puts the channel it consumes on into
    /// confirm mode and registers its own callback on it. [`setup_consumer`] does that up
    /// front.
    pub fn new(inner: Arc<Inner>) -> Self {
        Self {
            inner,
            confirms: OnceCell::new(),
            _marker: PhantomData,
        }
    }

    fn with_confirms(inner: Arc<Inner>, confirms: Arc<ConfirmState>) -> Self {
        Self {
            inner,
            confirms: OnceCell::new_with(Some(confirms)),
            _marker: PhantomData,
        }
    }

    /// Process message
    pub async fn on_message(&self, _prop: BasicProperties, content: Vec<u8>) -> Result<(), Error> {
        self.process_content(&content).await
    }

    async fn process_content(&self, content: &[u8]) -> Result<(), Error> {
        #[cfg(feature = "tracing-otel")]
        info!(monotonic_counter.mq_event_receive = 1);
        let decoded_message = Message::from_bytes(content).map_err(|e| e.into())?;
        self.inner.process(decoded_message).await
    }

    /// Decide where a failed message goes, and log why.
    fn destination(error: &Error, failed_attempts: u32) -> Option<Destination> {
        #[cfg(feature = "tracing")]
        let queue = Inner::QUEUE;
        match Inner::failure_action(error) {
            FailureAction::Discard => {
                #[cfg(feature = "tracing")]
                tracing::error!(queue, %error, "The processor rejected the message; dropping it");
                None
            }
            FailureAction::DeadLetter => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    queue,
                    %error,
                    "Processing failed with an error that retrying cannot fix; parking the message in {}",
                    dead_letter_queue_name(queue)
                );
                Some(Destination::DeadLetter {
                    reason: "permanent-error",
                })
            }
            FailureAction::Retry => match Inner::RETRY_POLICY.next_delay(failed_attempts) {
                Some(delay) => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        queue,
                        failed_attempts,
                        max_attempts = Inner::RETRY_POLICY.max_attempts,
                        retry_in_ms = delay.as_millis() as u64,
                        %error,
                        "Processing failed; retrying later"
                    );
                    Some(Destination::Retry { delay })
                }
                None => {
                    #[cfg(feature = "tracing")]
                    tracing::error!(
                        queue,
                        failed_attempts,
                        %error,
                        "Processing failed on every attempt; parking the message in {}",
                        dead_letter_queue_name(queue)
                    );
                    Some(Destination::DeadLetter {
                        reason: "retries-exhausted",
                    })
                }
            },
        }
    }

    async fn handle_failure(
        &self,
        channel: &Channel,
        deliver: &Deliver,
        properties: &BasicProperties,
        content: Vec<u8>,
        error: Error,
    ) {
        let queue = Inner::QUEUE;
        let failed_attempts = retry::failed_attempts(properties).saturating_add(1);
        let Some(destination) = Self::destination(&error, failed_attempts) else {
            ack(
                channel,
                BasicAckArguments::new(deliver.delivery_tag(), false),
                5,
            )
            .await;
            return;
        };

        let target = match destination {
            Destination::Retry { .. } => retry_queue_name(queue),
            Destination::DeadLetter { .. } => dead_letter_queue_name(queue),
        };
        let forwarded = retry::forwarded_properties(
            properties,
            queue,
            failed_attempts,
            &error.to_string(),
            &destination,
        );
        let published = match self
            .confirms
            .get_or_try_init(|| ConfirmState::attach(channel))
            .await
        {
            Ok(confirms) => {
                confirms
                    .publish(
                        channel,
                        forwarded,
                        content,
                        // The default exchange routes straight to the queue of that name.
                        BasicPublishArguments::new("", &target)
                            .mandatory(true)
                            .finish(),
                    )
                    .await
            }
            Err(e) => Err(e.into()),
        };

        match published {
            Ok(PublishOutcome::Routed) => {
                ack(
                    channel,
                    BasicAckArguments::new(deliver.delivery_tag(), false),
                    5,
                )
                .await;
            }
            outcome => {
                #[cfg(feature = "tracing")]
                match outcome {
                    Ok(_) => tracing::error!(
                        queue,
                        target,
                        "Queue {target} does not exist; requeueing the message in {REQUEUE_DELAY:?}"
                    ),
                    Err(e) => tracing::error!(
                        queue,
                        target,
                        error = %e,
                        "Could not move the message to {target}; requeueing it in {REQUEUE_DELAY:?}"
                    ),
                }
                #[cfg(not(feature = "tracing"))]
                let _ = outcome;
                tokio::time::sleep(REQUEUE_DELAY).await;
                nack(
                    channel,
                    BasicNackArguments::new(deliver.delivery_tag(), false, true),
                    5,
                )
                .await;
            }
        }
    }
}

impl<M, I> AsyncConsumer for AmqpMessageConsumer<M, I>
where
    M: AmqpMessageSend + MessageDe + Send + Sync,
    I: AmqpMessageProcessor<M> + Send + Sync,
    M::DeError: Send,
{
    fn consume<'life0, 'life1, 'async_trait>(
        &'life0 mut self,
        channel: &'life1 Channel,
        deliver: Deliver,
        basic_properties: BasicProperties,
        content: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = ()> + Send + 'async_trait>>
    where
        Self: 'async_trait,
        'life0: 'async_trait,
        'life1: 'async_trait,
    {
        Box::pin(async move {
            match self.process_content(&content).await {
                Ok(()) => {
                    ack(
                        channel,
                        BasicAckArguments::new(deliver.delivery_tag(), false),
                        5,
                    )
                    .await;
                }
                Err(error) => {
                    self.handle_failure(channel, &deliver, &basic_properties, content, error)
                        .await;
                }
            }
        })
    }
}

/// Ack message with retry
pub async fn ack(channel: &Channel, arg: BasicAckArguments, max_retries: u32) {
    let mut retries = 0;
    while retries < max_retries {
        match channel.basic_ack(arg.clone()).await {
            Ok(_) => return,
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::error!("Failed to ack message: {e}");
                #[cfg(not(feature = "tracing"))]
                let _ = e;
                retries += 1;
            }
        }
    }

    if retries == max_retries {
        #[cfg(feature = "tracing")]
        tracing::error!("Failed to ack message after {} retries", max_retries);
    }
}

/// Nack a message, retrying on transient failures up to `max_retries`.
pub async fn nack(channel: &Channel, arg: BasicNackArguments, max_retries: u32) {
    let mut retries = 0;
    while retries < max_retries {
        match channel.basic_nack(arg.clone()).await {
            Ok(_) => return,
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::error!("Failed to nack message: {e}");
                #[cfg(not(feature = "tracing"))]
                let _ = e;
                retries += 1;
            }
        }
    }
}

/// Declare `queue`'s retry and dead-letter queues ([`retry_queue_name`],
/// [`dead_letter_queue_name`]).
///
/// The retry queue dead-letters expired messages through the default exchange back into
/// `queue`. Both are new names next to `queue`, so declaring them never conflicts with how
/// `queue` itself was declared. Changing their arguments later does: RabbitMQ refuses to
/// redeclare a queue with different arguments (`PRECONDITION_FAILED`).
pub async fn declare_failure_queues(
    channel: &Channel,
    queue: &str,
) -> Result<(), amqprs::error::Error> {
    let mut retry_arguments = FieldTable::new();
    retry_arguments.insert(
        "x-dead-letter-exchange".try_into().expect("short name"),
        FieldValue::from(""),
    );
    retry_arguments.insert(
        "x-dead-letter-routing-key".try_into().expect("short name"),
        FieldValue::from(queue),
    );
    channel
        .queue_declare(
            QueueDeclareArguments::durable_client_named(&retry_queue_name(queue))
                .arguments(retry_arguments)
                .finish(),
        )
        .await?;
    channel
        .queue_declare(QueueDeclareArguments::durable_client_named(
            &dead_letter_queue_name(queue),
        ))
        .await?;
    Ok(())
}

/// bind consumer for a message type
///
/// Binds `H::QUEUE` to the message's exchange, declares its retry and dead-letter queues
/// ([`declare_failure_queues`]), sets the prefetch to `H::PREFETCH`, puts the channel into
/// confirm mode for publishing retries and starts consuming with manual acks. The queue itself
/// must already exist ([`AmqpMessageProcessor::ensure_queue`]).
///
/// The channel should be used for this one consumer only: its callback is replaced.
pub async fn setup_consumer<M, H>(
    channel: &Channel,
    hook: Arc<H>,
) -> Result<(), amqprs::error::Error>
where
    M: AmqpMessageSend + MessageDe + Send + Sync + 'static,
    M::DeError: Send,
    H: AmqpMessageProcessor<M> + Send + Sync + 'static,
{
    let queue = H::QUEUE;
    channel
        .queue_bind(QueueBindArguments::new(queue, M::EXCHANGE, M::ROUTING_KEY).finish())
        .await?;
    declare_failure_queues(channel, queue).await?;
    channel
        .basic_qos(BasicQosArguments::new(0, H::PREFETCH, false))
        .await?;
    let confirms = ConfirmState::attach(channel).await?;
    channel
        .basic_consume(
            AmqpMessageConsumer::<M, H>::with_confirms(hook, confirms),
            BasicConsumeArguments::new(queue, "")
                .manual_ack(true)
                .finish(),
        )
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pooled_channels_stay_below_channel_max() {
        // RabbitMQ's default channel_max.
        assert_eq!(max_pooled_channels(2047), 1919);
        // 0 is "no limit", i.e. the protocol maximum.
        assert_eq!(max_pooled_channels(0), 65535 - 128);
        // Small limits keep half of the channels for consumers.
        assert_eq!(max_pooled_channels(64), 32);
        assert_eq!(max_pooled_channels(1), 1);
    }
}
