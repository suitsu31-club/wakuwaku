//! Consuming the events of one main topic.
//!
//! [`IggyConsumerRegisterCenter`] collects one [`EventHandler`] per event
//! type; [`start`](IggyConsumerRegisterCenter::start) validates the topics and
//! spawns one task per configured partition, returning a [`ConsumerRuntime`].
//! Stop it with [`ConsumerRuntime::shutdown`] or [`ConsumerRuntime::abort`];
//! [`ConsumerConfig`] and [`DeliveryMode`] decide what is polled and when
//! offsets are stored.
//!
//! See the [consuming guide](https://www.wakuwaku.rs/en/docs/events/consuming)
//! for the per-partition pipeline, and the
//! [error handling guide](https://www.wakuwaku.rs/en/docs/events/error-handling)
//! for retries and quarantine.

pub mod config;
mod encode;
mod execute;
pub mod handler;
pub(crate) mod record;
pub(crate) mod retry;
pub(crate) mod runtime;

pub use config::{ConsumerConfig, DEFAULT_RETRY_DELAYS, DeliveryMode};
pub use handler::{EventHandler, IggyConsumerRegisterCenter};
pub use runtime::ConsumerRuntime;
