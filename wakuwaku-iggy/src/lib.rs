#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic_in_result_fn,
    clippy::arithmetic_side_effects
)]

pub mod consumer;
pub mod error;
pub mod events;
pub mod partition;
pub(crate) mod utils;
