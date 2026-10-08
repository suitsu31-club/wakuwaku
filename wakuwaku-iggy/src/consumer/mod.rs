//! Consuming the events of one main topic.
//!
//! [`IggyConsumerRegisterCenter`] collects one [`EventHandler`] per event
//! type; [`start`](IggyConsumerRegisterCenter::start) validates the topics and
//! spawns one task per configured partition, returning a [`ConsumerRuntime`].
//! See the [crate documentation](crate#consuming) for how a batch is
//! processed and how handler errors are treated.

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
