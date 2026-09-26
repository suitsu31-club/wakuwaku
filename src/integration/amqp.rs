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
use confirm::{ChannelLeaks, ConfirmState};
use kanau::message::{MessageDe, MessageSer};
use kanau::processor::Processor;
use retry::Destination;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::OnceCell;
use tokio::time::Instant;

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
    ///
    /// amqprs never reuses the number of a channel the broker closed (for example after a
    /// publish to an exchange that does not exist). Once the broker has closed as many of the
    /// pool's channels as the connection has numbers to spare, the pool closes the connection
    /// instead of letting amqprs run out of channel numbers on a connection that still looks
    /// open. Watch the connection and reconnect, or exit and get restarted, when it closes.
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
        let leaks = Arc::new(ChannelLeaks::new(channel_leak_budget(
            connection.channel_max(),
            capacity,
        )));
        let factory = move || {
            let connection = connection.clone();
            let leaks = leaks.clone();
            Box::pin(async move {
                if let Some(closed) = leaks.exhausted() {
                    return Err(close_leaking_connection(connection, &leaks, closed));
                }
                // Opening a channel waits for the broker. Open it in a task of its own: a
                // caller that stops waiting would otherwise leave amqprs holding a channel
                // number that is never released. A channel nobody waits for any more is
                // dropped, which closes it and frees its number.
                tokio::spawn(async move { ConfirmChannel::open_counting(&connection, leaks).await })
                    .await
                    .map_err(|e| amqprs::error::Error::ChannelOpenError(e.to_string()))?
            })
                as Pin<
                    Box<dyn Future<Output = Result<ConfirmChannel, amqprs::error::Error>> + Send>,
                >
        };
        crate::pool::Pool::with_health_check(Box::pin(factory), capacity, ConfirmChannel::is_usable)
    }
}

/// Close a connection whose channel numbers are running out, once, and explain why.
fn close_leaking_connection(
    connection: amqprs::connection::Connection,
    leaks: &ChannelLeaks,
    closed: usize,
) -> amqprs::error::Error {
    if leaks.start_closing() {
        #[cfg(feature = "tracing")]
        tracing::error!(
            closed_by_broker = closed,
            budget = leaks.budget(),
            "RabbitMQ closed {closed} channels on this connection, and amqprs cannot reuse their \
             numbers; closing the connection before it runs out of them"
        );
        tokio::spawn(async move {
            let _ = connection.close().await;
        });
    }
    amqprs::error::Error::ChannelOpenError(format!(
        "RabbitMQ closed {closed} channels on this connection (budget {}); the connection is \
         closed so that a new one can be opened",
        leaks.budget()
    ))
}

/// Effective channel numbers of a connection: 0 means "no limit" in AMQP, the protocol maximum.
fn channel_numbers(channel_max: u16) -> usize {
    usize::from(if channel_max == 0 {
        u16::MAX
    } else {
        channel_max
    })
}

/// Channel numbers kept for channels opened outside of the pool.
fn reserved_channels(channel_max: u16) -> usize {
    usize::from(RESERVED_CHANNELS).min(channel_numbers(channel_max) / 2)
}

/// How many pooled channels a connection with the given `channel_max` leaves room for.
fn max_pooled_channels(channel_max: u16) -> usize {
    (channel_numbers(channel_max) - reserved_channels(channel_max)).max(1)
}

/// How many channels the broker may close before a pool of `capacity` channels closes the
/// connection: the numbers left over once the pool and the reserved channels are open.
fn channel_leak_budget(channel_max: u16, capacity: usize) -> usize {
    channel_numbers(channel_max)
        .saturating_sub(capacity + reserved_channels(channel_max))
        .max(1)
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

    /// Whether a message that no queue is bound to take is an error. By default the broker
    /// drops such a message and [`send`](Self::send) succeeds. Set it for messages that must
    /// not get lost when their consumer's queue does not exist yet, so that the caller can
    /// report the failure to whoever can try again.
    const REQUIRE_ROUTE: bool = false;

    // Allow async fn in trait because we don't want the user to override this function
    #[allow(async_fn_in_trait)]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all, err))]
    /// Publish the message and report what became of it.
    ///
    /// Waits for the broker's confirm, at most [`CONFIRM_TIMEOUT`] in all (getting a channel
    /// included). Returns an error only when the message did not reach a queue: it was not
    /// handed to the connection in time, the broker nacked it, the channel closed first, or
    /// no queue took it while [`REQUIRE_ROUTE`](Self::REQUIRE_ROUTE) is set. A message that
    /// got no confirm in time is [`PublishOutcome::Unconfirmed`]: it will most likely still
    /// arrive, so publishing it again can deliver it twice.
    async fn publish(self, pool: &AmqpPool) -> Result<PublishOutcome, crate::error::Error> {
        let deadline = Instant::now() + CONFIRM_TIMEOUT;
        let bytes = self.to_bytes().map_err(|e| e.into())?;
        let channel = tokio::time::timeout_at(deadline, pool.get())
            .await
            .map_err(|_| {
                Error::AmqpError(amqprs::error::Error::ChannelUseError(format!(
                    "no AMQP channel within {CONFIRM_TIMEOUT:?}; the message for exchange '{}' \
                     was not published",
                    Self::EXCHANGE
                )))
            })?;
        let channel: Result<Pooled<ConfirmChannel, _>, crate::error::Error> = channel.into();
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
            .publish_until(
                properties,
                bytes.into_vec(),
                BasicPublishArguments::new(Self::EXCHANGE, Self::ROUTING_KEY)
                    .mandatory(true)
                    .finish(),
                Some(deadline),
            )
            .await?;
        if let PublishOutcome::Returned {
            reply_code,
            reply_text,
        } = &outcome
            && Self::REQUIRE_ROUTE
        {
            return Err(Error::AmqpError(amqprs::error::Error::ChannelUseError(
                format!(
                    "no queue is bound to take the message for exchange '{}', routing key '{}'; \
                     the broker dropped it ({reply_code} {reply_text})",
                    Self::EXCHANGE,
                    Self::ROUTING_KEY
                ),
            )));
        }

        #[cfg(feature = "tracing-otel")]
        info!(monotonic_counter.mq_event_push = 1);
        Ok(outcome)
    }

    // Allow async fn in trait because we don't want the user to override this function
    #[allow(async_fn_in_trait)]
    #[cfg_attr(feature = "tracing", tracing::instrument(skip_all, err, ret))]
    /// Send message to rabbitmq
    ///
    /// [`publish`](Self::publish) without the outcome. A message that no queue is bound to
    /// take is dropped by the broker, which is not an error unless
    /// [`REQUIRE_ROUTE`](Self::REQUIRE_ROUTE) is set. A message the broker did not confirm in
    /// time is logged and counts as sent: it was handed to the connection, and retrying it
    /// could deliver it twice.
    async fn send(self, pool: &AmqpPool) -> Result<(), crate::error::Error> {
        match self.publish(pool).await? {
            PublishOutcome::Routed => {}
            PublishOutcome::Returned {
                reply_code,
                reply_text,
            } => {
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
            PublishOutcome::Unconfirmed => {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    exchange = Self::EXCHANGE,
                    routing_key = Self::ROUTING_KEY,
                    "No confirm from RabbitMQ within {CONFIRM_TIMEOUT:?} (memory or disk alarm?); \
                     the message is delivered once the broker reads it, unless the connection \
                     is lost first"
                );
            }
        }
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
/// broker confirmed the copy, however long that takes. If the broker refuses the copy, the
/// original is requeued after a few seconds instead, never right away. If the channel closes
/// before the copy is confirmed, the broker requeues the original.
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
                None if Inner::RETRY_POLICY.park_when_exhausted => {
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
                None => {
                    #[cfg(feature = "tracing")]
                    tracing::warn!(
                        queue,
                        failed_attempts,
                        %error,
                        "Processing failed on every attempt; dropping the message"
                    );
                    None
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
        let confirms = match self
            .confirms
            .get_or_try_init(|| ConfirmState::attach(channel))
            .await
        {
            Ok(confirms) => confirms.clone(),
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    queue,
                    error = %e,
                    "Could not put the channel into confirm mode; requeueing the message in {REQUEUE_DELAY:?}"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = e;
                requeue_later(channel, deliver).await;
                return;
            }
        };
        // The default exchange routes straight to the queue of that name.
        let publish_copy = |content| {
            confirms.publish(
                channel,
                forwarded.clone(),
                content,
                BasicPublishArguments::new("", &target)
                    .mandatory(true)
                    .finish(),
                // No deadline: the original stays unacknowledged until the broker confirmed
                // the copy. Requeueing the original while the broker may still take the copy
                // would deliver the message twice.
                None,
            )
        };
        let mut published = publish_copy(content.clone()).await;
        if let Ok(PublishOutcome::Returned { .. }) = &published {
            // Someone deleted the queue while the consumer was running. Declare it again.
            #[cfg(feature = "tracing")]
            tracing::warn!(
                queue,
                target,
                "Queue {target} does not exist; declaring it again"
            );
            published = match declare_failure_queues(channel, queue).await {
                Ok(()) => publish_copy(content).await,
                Err(e) => Err(e.into()),
            };
        }

        match published {
            // `Unconfirmed` only comes with a deadline, and there is none here.
            Ok(PublishOutcome::Routed | PublishOutcome::Unconfirmed) => {
                ack(
                    channel,
                    BasicAckArguments::new(deliver.delivery_tag(), false),
                    5,
                )
                .await;
            }
            Ok(PublishOutcome::Returned { .. }) => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    queue,
                    target,
                    "Queue {target} does not exist; requeueing the message in {REQUEUE_DELAY:?}"
                );
                requeue_later(channel, deliver).await;
            }
            Err(e) if !channel.is_open() || !confirms.is_usable() => {
                // The channel closed, or its confirms can no longer be matched to publishes, so
                // nothing more can be acknowledged on it. Make sure it is closed: the broker
                // requeues the original, and the consumer is set up again (see
                // `AmqpConsumersRuntime::closed`). If the broker took the copy before, the
                // message is processed twice.
                #[cfg(feature = "tracing")]
                tracing::error!(
                    queue,
                    target,
                    error = %e,
                    "The consumer channel closed before RabbitMQ confirmed the copy for {target}; the broker requeues the message"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = e;
                if channel.is_open() {
                    let channel = channel.clone();
                    tokio::spawn(async move {
                        let _ = channel.close().await;
                    });
                }
            }
            Err(e) => {
                #[cfg(feature = "tracing")]
                tracing::error!(
                    queue,
                    target,
                    error = %e,
                    "Could not move the message to {target}; requeueing it in {REQUEUE_DELAY:?}"
                );
                #[cfg(not(feature = "tracing"))]
                let _ = e;
                requeue_later(channel, deliver).await;
            }
        }
    }
}

/// Put a message back into its queue after [`REQUEUE_DELAY`], so that a failure that repeats
/// does not spin.
async fn requeue_later(channel: &Channel, deliver: &Deliver) {
    tokio::time::sleep(REQUEUE_DELAY).await;
    nack(
        channel,
        BasicNackArguments::new(deliver.delivery_tag(), false, true),
        5,
    )
    .await;
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

    #[test]
    fn channels_the_broker_may_close_are_the_numbers_left_over() {
        // 2047 numbers, 512 pooled, 128 reserved.
        assert_eq!(channel_leak_budget(2047, 512), 1407);
        // A pool as large as the connection allows leaves no spare number: close on the first.
        assert_eq!(channel_leak_budget(2047, max_pooled_channels(2047)), 1);
        assert_eq!(channel_leak_budget(64, 16), 16);
    }
}
