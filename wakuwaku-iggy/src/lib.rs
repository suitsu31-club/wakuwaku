//! Typed event publishing and consuming on [Apache Iggy](https://iggy.apache.org),
//! with per-key ordering and algebraic batching.
//!
//! An Iggy partition is totally ordered, but most business logic only needs
//! the events *of one key* (a user, an order, …) in order. This crate lets each
//! event type declare how much order it needs and how its events combine, and
//! the consumer uses that to process a polled batch concurrently and in fewer
//! handler calls, without breaking any order the events ask for.
//!
//! # Events and keys
//!
//! An event type implements [`Event`]:
//!
//! - [`TYPE_TAG`](Event::TYPE_TAG): an [`EventTypeTag`] unique within its topic.
//! - [`Key`](Event::Key): a [`PartitionKey`]. The key type names the Iggy topic
//!   (its *super partition*, [`IggySuperPartitionName`]) shared by every event
//!   keyed by it, and its [`key_hash`] picks the partition. Events with equal
//!   keys therefore always share a partition.
//! - [`Algebra`](Event::Algebra): one of the markers below.
//! - [`atomic_ordering`](Event::atomic_ordering): the [`EventAtomicOrdering`]
//!   of each message.
//!
//! The body stored in the log is an [`EventBody`], serialized with
//! [`kanau::message`]. The [`Publisher`] sends events of any type to their main
//! topic, with headers carrying the tag, key hash, ordering and algebra.
//!
//! # Ordering
//!
//! Events of different keys are never ordered against each other. Within one
//! key, events of the same type always keep their log order, and
//! [`EventAtomicOrdering`] decides what may move past what:
//!
//! | Ordering  | Effect within the key                                       |
//! |-----------|-------------------------------------------------------------|
//! | `Relaxed` | may be reordered with events of other types                 |
//! | `Acquire` | nothing after it is processed before it                     |
//! | `Release` | nothing before it is processed after it                     |
//! | `AcqRel`  | both                                                        |
//!
//! The [planner](partition::plan) splits each key's events into *segments*
//! at these fences and groups each segment's events by type into *runs*.
//!
//! # Algebra
//!
//! Before a run reaches its handler, it is reduced according to its type's
//! algebra marker:
//!
//! | Marker                    | Reduction of a run                      |
//! |---------------------------|-----------------------------------------|
//! | [`NonAssociative`]        | none                                    |
//! | [`Commutative`]           | none; may be applied in any order       |
//! | [`Idempotent`]            | adjacent equal events collapse          |
//! | [`IdempotentCommutative`] | all equal events collapse               |
//! | [`Associative`]           | folded into one event with [`EventSemigroup::combine`] |
//!
//! The marker is static, and each one requires the matching bound on the
//! body (`Eq`, `Ord` or [`EventSemigroup`]). See [`partition::algebra`] for
//! the laws each marker promises.
//!
//! # Consuming
//!
//! Implement [`EventHandler`] for each event type, register the handlers with
//! [`IggyConsumerRegisterCenter::new`] and
//! [`push`](IggyConsumerRegisterCenter::push), and call
//! [`start`](IggyConsumerRegisterCenter::start) with a [`ConsumerConfig`].
//! All registered events must share one main topic. Each configured
//! partition gets its own task, which repeatedly:
//!
//! 1. polls a batch and plans it by key;
//! 2. processes keys concurrently, and each key's segments in order: the head
//!    run, then the body runs concurrently, then the tail run;
//! 3. decodes and reduces each run, and passes it to the handler in chunks of
//!    at most [`EventHandler::MAX_BATCH`] events;
//! 4. stores the offset as the [`DeliveryMode`] requires.
//!
//! A failing handler returns a [`HandleError`] whose [`ErrorClass`] decides
//! what happens:
//!
//! - [`Transient`](ErrorClass::Transient): the chunk is retried in place until
//!   it succeeds, blocking the partition.
//! - [`Retryable`](ErrorClass::Retryable): the key's unfinished work is written
//!   to the retry topic `retry_<topic>`, and the key is quarantined: its later
//!   events follow it there, so they stay behind the failed ones. The key is
//!   retried once after each of the
//!   [`retry_delays`](ConsumerConfig::retry_delays); if the last attempt
//!   fails, its failed runs are dropped.
//! - [`Unrecoverable`](ErrorClass::Unrecoverable): the chunk is dropped and
//!   logged.
//!
//! Records with bad headers or bodies that can't be decoded are dropped and
//! logged with [`tracing`].
//!
//! # Example
//!
//! ```no_run
//! use iggy::prelude::{Client, IggyClient};
//! use kanau::message::{MessageDe, MessageSer};
//! use std::hash::DefaultHasher;
//! use std::num::NonZeroUsize;
//! use std::sync::Arc;
//! use wakuwaku_iggy::{
//!     Associative, ConsumerConfig, Event, EventAtomicOrdering, EventBody, EventHandler,
//!     EventSemigroup, EventTypeTag, HandleError, IggyConsumerRegisterCenter,
//!     IggySuperPartitionName, PartitionKey, Publisher,
//! };
//!
//! #[derive(Hash)]
//! struct UserId(u64);
//!
//! impl PartitionKey for UserId {
//!     type PartitionMerge = DefaultHasher;
//!     const SUPER_PARTITION_NAME: IggySuperPartitionName = IggySuperPartitionName("users");
//! }
//!
//! /// Points granted to a user. Consecutive grants add up.
//! struct PointsGranted {
//!     user: u64,
//!     points: i64,
//! }
//!
//! impl MessageSer for PointsGranted {
//!     type SerError = anyhow::Error;
//!     fn to_bytes(self) -> Result<Box<[u8]>, anyhow::Error> {
//!         let mut bytes = self.user.to_le_bytes().to_vec();
//!         bytes.extend_from_slice(&self.points.to_le_bytes());
//!         Ok(bytes.into_boxed_slice())
//!     }
//! }
//!
//! impl MessageDe for PointsGranted {
//!     type DeError = anyhow::Error;
//!     fn from_bytes(bytes: &[u8]) -> Result<Self, anyhow::Error> {
//!         let (user, points) = bytes.split_at_checked(8).ok_or(anyhow::anyhow!("too short"))?;
//!         Ok(Self {
//!             user: u64::from_le_bytes(user.try_into()?),
//!             points: i64::from_le_bytes(points.try_into()?),
//!         })
//!     }
//! }
//!
//! impl EventBody for PointsGranted {}
//!
//! impl EventSemigroup for PointsGranted {
//!     fn combine(self, other: Self) -> Self {
//!         Self { user: self.user, points: self.points + other.points }
//!     }
//! }
//!
//! impl Event for PointsGranted {
//!     const TYPE_TAG: EventTypeTag = EventTypeTag::new(1);
//!     type Key = UserId;
//!     type Algebra = Associative;
//!     fn key(&self) -> UserId {
//!         UserId(self.user)
//!     }
//!     fn atomic_ordering(&self) -> EventAtomicOrdering {
//!         EventAtomicOrdering::Relaxed
//!     }
//! }
//!
//! struct GrantPoints;
//!
//! impl EventHandler<PointsGranted> for GrantPoints {
//!     const MAX_BATCH: NonZeroUsize = NonZeroUsize::new(100).unwrap();
//!
//!     async fn handle(&self, run: &[PointsGranted]) -> Result<(), HandleError> {
//!         // One key's grants, already summed into a single event.
//!         for grant in run {
//!             println!("user {} +{}", grant.user, grant.points);
//!         }
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Arc::new(IggyClient::from_connection_string(
//!         "iggy://iggy:iggy@localhost:8090",
//!     )?);
//!     client.connect().await?;
//!
//!     let publisher = Publisher::new(client.clone(), "shop")?;
//!     publisher.publish(PointsGranted { user: 7, points: 10 }).await?;
//!
//!     let runtime = IggyConsumerRegisterCenter::new(Arc::new(GrantPoints))
//!         .start(client, ConsumerConfig::new("shop", "points", vec![1]))
//!         .await?;
//!     tokio::signal::ctrl_c().await?;
//!     runtime.shutdown().await;
//!     Ok(())
//! }
//! ```
//!
//! # Modules
//!
//! - [`events`]: event traits, wire headers and the [`Publisher`].
//! - [`partition`]: partition keys, the per-key planner and the algebra.
//! - [`consumer`]: handlers, configuration and the consumer runtime.
//! - [`error`]: handler and setup errors.

#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic_in_result_fn,
    clippy::arithmetic_side_effects
)]
#![warn(missing_docs)]

pub mod consumer;
pub mod error;
pub mod events;
pub mod partition;
pub(crate) mod utils;

pub use consumer::{
    ConsumerConfig, ConsumerRuntime, DeliveryMode, EventHandler, IggyConsumerRegisterCenter,
};
pub use events::{Event, EventAtomicOrdering, EventBody, EventTypeTag, IntoEventBody, Publisher};
pub use partition::algebra::{
    Associative, Commutative, EventSemigroup, Idempotent, IdempotentCommutative, NonAssociative,
};
pub use partition::{IggySuperPartitionName, PartitionKey, key_hash};
