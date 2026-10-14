//! Typed event publishing and consuming on [Apache Iggy](https://iggy.apache.org),
//! with per-key ordering and algebraic batching.
//!
//! - [`events`]: the [`Event`] trait, its [`EventAtomicOrdering`], the wire
//!   [headers](events::headers) and the [`Publisher`].
//! - [`partition`]: [`PartitionKey`] and [`key_hash`], which pick the topic and
//!   partition, the per-key [planner](partition::plan) and the
//!   [algebra](partition::algebra) markers ([`NonAssociative`], [`Commutative`],
//!   [`Idempotent`], [`IdempotentCommutative`], [`Associative`]).
//! - [`consumer`]: [`EventHandler`], [`IggyConsumerRegisterCenter`],
//!   [`ConsumerConfig`], [`DeliveryMode`] and the [`ConsumerRuntime`].
//! - [`error`]: [`HandleError`](error::HandleError) and its
//!   [`ErrorClass`](error::ErrorClass), and the setup and publishing
//!   [`Error`](error::Error).
//!
//! See the [events guide](https://www.wakuwaku.rs/en/docs/events) for the model
//! (keys, ordering fences, runs, retries) and worked examples.
//!
//! # Example
//!
//! ```no_run
//! use iggy::prelude::{Client, IggyClient};
//! use kanau::message::{MessageDe, MessageSer};
//! use std::hash::DefaultHasher;
//! use std::num::NonZeroUsize;
//! use std::sync::Arc;
//! use wakuwaku_iggy::error::HandleError;
//! use wakuwaku_iggy::{
//!     Associative, ConsumerConfig, Event, EventAtomicOrdering, EventBody, EventHandler,
//!     EventSemigroup, EventTypeTag, IggyConsumerRegisterCenter, IggySuperPartitionName,
//!     PartitionKey, Publisher,
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
//!         Ok([self.user.to_le_bytes(), self.points.to_le_bytes()].concat().into())
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
//!     // Iggy partition ids are zero-based: list every partition of the topic,
//!     // here the single partition `0` of a topic created with defaults.
//!     let runtime = IggyConsumerRegisterCenter::new(Arc::new(GrantPoints))
//!         .start(client, ConsumerConfig::new("shop", "points", vec![0]))
//!         .await?;
//!     tokio::signal::ctrl_c().await?;
//!     runtime.shutdown().await;
//!     Ok(())
//! }
//! ```

#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic_in_result_fn,
    clippy::arithmetic_side_effects
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/suitsu31-club/wakuwaku-docs/main/icon.svg"
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
