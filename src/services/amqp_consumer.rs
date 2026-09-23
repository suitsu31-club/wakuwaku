//! Registering and starting a group of RabbitMQ consumers together.
//!
//! [`AmqpConsumerRegisterCenter`] collects consumers in a type-level linked list,
//! the same way [`ServiceBuilder`](super::builder::ServiceBuilder) does.
//! Every node holds one [`AmqpMessageProcessor`] and the event type it
//! consumes, so a single chain can mix different consumers and events. There is
//! no boxing and no dynamic dispatch.
//!
//! Calling [`setup`](AmqpConsumerRegisterCenter::setup) starts the consumers
//! one at a time, in the order they were registered. For each one it:
//!
//! 1. declares the event's durable exchange
//!    ([`AmqpRouting::ensure_exchange`](crate::integration::amqp::AmqpRouting::ensure_exchange)),
//! 2. opens a **dedicated** channel from the pool's factory (not a pooled
//!    channel), declares the durable queue [`AmqpMessageProcessor::QUEUE`],
//!    and binds it to the exchange with the event's routing key,
//! 3. starts consuming with manual ack ([`setup_consumer`]).
//!
//! The returned [`AmqpConsumersRuntime`] owns those channels. amqprs closes a
//! channel when it is dropped, so **dropping the runtime stops every consumer**.
//! Keep it alive for as long as the service should consume.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use wakuwaku::integration::amqp::AmqpPool;
//! use wakuwaku::services::amqp_consumer::AmqpConsumerRegisterCenter;
//! # use kanau::message::{MessageDe, MessageSer};
//! # use kanau::processor::Processor;
//! # use wakuwaku::integration::amqp::{
//! #     AmqpExchangeType, AmqpMessageProcessor, AmqpMessageSend, AmqpRouting,
//! # };
//! # macro_rules! event {
//! #     ($name:ident, $key:literal) => {
//! #         struct $name;
//! #         impl MessageSer for $name {
//! #             type SerError = anyhow::Error;
//! #             fn to_bytes(self) -> Result<Box<[u8]>, anyhow::Error> { Ok(Box::new([])) }
//! #         }
//! #         impl MessageDe for $name {
//! #             type DeError = anyhow::Error;
//! #             fn from_bytes(_: &[u8]) -> Result<Self, anyhow::Error> { Ok($name) }
//! #         }
//! #         impl AmqpRouting for $name {
//! #             const EXCHANGE: &'static str = "orders";
//! #             const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Topic;
//! #             const ROUTING_KEY: &'static str = $key;
//! #         }
//! #         impl AmqpMessageSend for $name {}
//! #     };
//! # }
//! # macro_rules! consumer {
//! #     ($name:ident, $event:ident, $queue:literal) => {
//! #         struct $name;
//! #         impl Processor<$event> for $name {
//! #             type Output = ();
//! #             type Error = wakuwaku::Error;
//! #             async fn process(&self, _: $event) -> Result<(), wakuwaku::Error> { Ok(()) }
//! #         }
//! #         impl AmqpMessageProcessor<$event> for $name {
//! #             const QUEUE: &'static str = $queue;
//! #         }
//! #     };
//! # }
//! # event!(OrderPlaced, "order.placed");
//! # event!(OrderShipped, "order.shipped");
//! # consumer!(SendReceipt, OrderPlaced, "send-receipt");
//! # consumer!(ReserveStock, OrderPlaced, "reserve-stock");
//! # consumer!(NotifyCustomer, OrderShipped, "notify-customer");
//!
//! async fn start(pool: &AmqpPool) -> Result<(), wakuwaku::Error> {
//!     // Each event type is inferred from the consumer's single
//!     // `AmqpMessageProcessor` impl.
//!     let runtime = AmqpConsumerRegisterCenter::new(Arc::new(SendReceipt))
//!         .push(Arc::new(ReserveStock))
//!         .push(Arc::new(NotifyCustomer))
//!         .setup(pool)
//!         .await?;
//!
//!     // Consumers run until `runtime` is dropped.
//!     std::future::pending::<()>().await;
//!     drop(runtime);
//!     Ok(())
//! }
//! ```
//!
//! If a consumer type implements [`AmqpMessageProcessor`] for more than one
//! event, the event cannot be inferred and must be named:
//! `AmqpConsumerRegisterCenter::<_, MyEvent>::new(..)` or `.push::<_, MyEvent>(..)`.

use crate::integration::amqp::{AmqpMessageProcessor, AmqpMessageSend, AmqpPool, setup_consumer};
use std::collections::LinkedList;
use std::marker::PhantomData;
use std::sync::Arc;

/// A node in the type-level list of registered consumers.
///
/// `Consumer` is the processor stored at this node, `Event` is the message
/// type it consumes, and `Chain` is the rest of the list: either another
/// `AmqpConsumerRegisterCenter` or `()` for the first registered consumer.
/// Build one with [`new`](Self::new), extend it with [`push`](Self::push),
/// and start everything with [`setup`](Self::setup). See the
/// [module documentation](self) for an example.
pub struct AmqpConsumerRegisterCenter<
    Consumer: AmqpMessageProcessor<Event>,
    Event: AmqpMessageSend + kanau::message::MessageDe,
    Chain = (),
> {
    head: Arc<Consumer>,
    chain: Chain,
    event: PhantomData<fn(Event)>,
}

impl<Consumer, Event> AmqpConsumerRegisterCenter<Consumer, Event>
where
    Consumer: AmqpMessageProcessor<Event>,
    Event: kanau::message::MessageDe + AmqpMessageSend,
{
    /// Start a new list with `head` as its first consumer.
    pub fn new(head: Arc<Consumer>) -> Self {
        Self {
            head,
            chain: (),
            event: PhantomData,
        }
    }
}

impl<Consumer1, Event1, Chain> AmqpConsumerRegisterCenter<Consumer1, Event1, Chain>
where
    Consumer1: AmqpMessageProcessor<Event1>,
    Event1: kanau::message::MessageDe + AmqpMessageSend,
{
    /// Register another consumer, returning the extended list.
    ///
    /// `next` becomes the new head. Consumers are still set up in
    /// registration order, so `next` starts after every consumer already in
    /// the list.
    pub fn push<Consumer2, Event2>(
        self,
        next: Arc<Consumer2>,
    ) -> AmqpConsumerRegisterCenter<Consumer2, Event2, Self>
    where
        Event2: kanau::message::MessageDe + AmqpMessageSend,
        Consumer2: AmqpMessageProcessor<Event2>,
    {
        AmqpConsumerRegisterCenter {
            head: next,
            chain: self,
            event: PhantomData,
        }
    }
}

impl<Consumer, Event, Chain> AmqpConsumerRegisterCenter<Consumer, Event, Chain>
where
    Consumer: AmqpMessageProcessor<Event> + Send + Sync + 'static,
    Event: kanau::message::MessageDe + AmqpMessageSend + Send + Sync + 'static,
    <Event as kanau::message::MessageDe>::DeError: Send + Sync,
    Self: Send + Sync + SetupOrderedConsumers,
{
    /// Start every registered consumer, in registration order.
    ///
    /// See the [module documentation](self) for what is declared on the
    /// broker for each consumer.
    ///
    /// # Errors
    ///
    /// Returns the first error from declaring the exchange or queue, opening
    /// the channel, or starting the consumer. Setup stops at that consumer.
    /// Channels already opened for earlier consumers are dropped, which closes
    /// them. Queues and exchanges that were already declared remain on the
    /// broker.
    pub async fn setup(
        self,
        channel_pool: &AmqpPool,
    ) -> Result<AmqpConsumersRuntime<Self>, crate::error::Error> {
        let linked_list = self.setup_ordered(channel_pool).await?;
        Ok(AmqpConsumersRuntime {
            _channels: linked_list,
            _register: self,
        })
    }
}

/// Starts each consumer of an [`AmqpConsumerRegisterCenter`] chain in
/// registration order.
///
/// Implemented for every chain whose consumers and events meet the bounds of
/// [`setup_consumer`]. It is public only because it appears in the bounds of
/// [`AmqpConsumerRegisterCenter::setup`]. Call `setup` instead of using it
/// directly.
pub trait SetupOrderedConsumers {
    /// Start this node and every node before it, returning the consuming
    /// channels in registration order (first registered first).
    ///
    /// The consumers stop when the returned channels are dropped.
    fn setup_ordered(
        &self,
        channel_pool: &AmqpPool,
    ) -> impl Future<Output = Result<LinkedList<amqprs::channel::Channel>, crate::error::Error>> + Send;
}

impl<Consumer, Event> SetupOrderedConsumers for AmqpConsumerRegisterCenter<Consumer, Event, ()>
where
    Consumer: AmqpMessageProcessor<Event> + Send + Sync + 'static,
    Event: kanau::message::MessageDe + AmqpMessageSend + Send + Sync + 'static,
    <Event as kanau::message::MessageDe>::DeError: Send + Sync,
    Self: Send + Sync,
{
    async fn setup_ordered(
        &self,
        channel_pool: &AmqpPool,
    ) -> Result<LinkedList<amqprs::channel::Channel>, crate::error::Error> {
        let ensured_queue = Consumer::ensure_queue(channel_pool).await?;
        setup_consumer::<Event, Consumer>(&ensured_queue, self.head.clone()).await?;
        Ok(LinkedList::from([ensured_queue]))
    }
}

impl<Consumer, Event, Chain: SetupOrderedConsumers> SetupOrderedConsumers
    for AmqpConsumerRegisterCenter<Consumer, Event, Chain>
where
    Consumer: AmqpMessageProcessor<Event> + Send + Sync + 'static,
    Event: kanau::message::MessageDe + AmqpMessageSend + Send + Sync + 'static,
    <Event as kanau::message::MessageDe>::DeError: Send + Sync,
    Self: Send + Sync,
{
    async fn setup_ordered(
        &self,
        channel_pool: &AmqpPool,
    ) -> Result<LinkedList<amqprs::channel::Channel>, crate::error::Error> {
        let mut previous = self.chain.setup_ordered(channel_pool).await?;
        let here_ensured_queue = Consumer::ensure_queue(channel_pool).await?;
        setup_consumer::<Event, Consumer>(&here_ensured_queue, self.head.clone()).await?;
        previous.push_back(here_ensured_queue);
        Ok(previous)
    }
}

/// Running consumers started by [`AmqpConsumerRegisterCenter::setup`].
///
/// Owns the registered consumers and one open channel per consumer.
/// Dropping this value closes those channels and stops consumption.
/// Messages that arrive afterwards stay in their durable queues until a
/// consumer is set up again.
pub struct AmqpConsumersRuntime<Reg> {
    _register: Reg,
    _channels: LinkedList<amqprs::channel::Channel>,
}
