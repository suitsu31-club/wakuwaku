//! Consuming the events of one main topic.

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
