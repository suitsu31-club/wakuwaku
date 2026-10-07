#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic_in_result_fn,
    clippy::arithmetic_side_effects
)]

pub mod algebra;
pub mod consumer;
pub mod error;
pub mod events;
pub mod handler;
pub mod headers;
pub mod partition;
pub mod publisher;
