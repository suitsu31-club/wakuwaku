//! Capability-typed data access for the wakuwaku stack.
//!
//! A query declares the [entities](Entity) it reads and writes as its
//! [`Effect`]; a [`Db`] handle runs it, as a
//! [`kanau::processor::Processor`], only if the handle's capability
//! [covers](Covers) that effect. Sending a write query to a [`ReadOnly`]
//! handle doesn't compile.
//!
//! - [`effect`]: entities, capabilities, effects, and hooks around write
//!   queries.
//! - [`query`]: [`Query`], [`Execute`], [`DataSource`] and [`Db`].
//! - `sqlx` (features `sqlx-pg` (default), `sqlx-mysql`): sqlx data sources
//!   and transactions.
//! - `redis` (feature `redis`): a Redis data source and typed collections.
//! - `meilisearch` (feature `meilisearch`): Meilisearch indexes as entities,
//!   typed filters, tenant tokens, and index sync.
//!
//! See the [data access guide](https://www.wakuwaku.rs/en/docs/data).

#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic_in_result_fn,
    clippy::arithmetic_side_effects
)]
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/suitsu31-club/wakuwaku-docs/main/icon.svg"
)]

pub mod effect;
#[cfg(feature = "meilisearch")]
pub mod meilisearch;
pub mod query;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(any(feature = "sqlx-pg", feature = "sqlx-mysql"))]
pub mod sqlx;

pub use effect::hook::{AfterWrite, BeforeWrite, WriteQuery};
pub use effect::io::{Covers, Effect, Kind, Read, ReadKind, Write, WriteKind};
pub use effect::markers::{CanRead, CanWrite, Entity, ReadOnly, ReadWrite};
pub use query::{DataSource, Db, Execute, Query};
