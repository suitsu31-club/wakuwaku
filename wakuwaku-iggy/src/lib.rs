#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic_in_result_fn,
    clippy::arithmetic_side_effects
)]

pub mod events;
pub mod partition;
pub mod algebra;
pub mod consumer;