//! Capability-typed data access for the wakuwaku stack.
//!
//! Every query is a value, and every handle that runs queries is a
//! [`kanau::processor::Processor`]. A query declares which [entities](Entity)
//! it reads and writes as its [`Effect`], and a [`Db`] handle carries a
//! capability type that must [cover](Covers) that effect. A handle built
//! with [`ReadOnly`] therefore can't run a write query: the program doesn't
//! compile.
//!
//! # Layers
//!
//! - [`effect`]: entities, the capabilities to access them, effects, and
//!   hooks around write queries.
//! - [`query`]: [`Query`], [`Execute`], [`DataSource`] and the [`Db`] handle.
//! - `sqlx` (features `sqlx-pg`, `sqlx-mysql`): sqlx data sources and
//!   transactions written as pure state machines.
//! - `redis` (feature `redis`): a Redis data source, typed collections
//!   (caches, locks, a rate limiter), and structures that can be followed as
//!   they change (pipes, live views, stream buffers).
//!
//! # Features
//!
//! | Feature      | Default | Enables                                   |
//! |--------------|---------|-------------------------------------------|
//! | `sqlx-pg`    | yes     | PostgreSQL through sqlx                   |
//! | `sqlx-mysql` | no      | MySQL through sqlx                        |
//! | `redis`      | no      | Redis through `redis`                     |
//!
//! # What the types prove
//!
//! The capability check proves *routing*: code wired to a `Db<_, ReadOnly>`
//! never issues a query declared as a write. It can't prove that a query
//! declared as a read doesn't write, so keep the database's own guard on
//! replicas (a hot standby rejects writes, and `default_transaction_read_only`
//! does the same on a primary).

#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic_in_result_fn,
    clippy::arithmetic_side_effects
)]
#![warn(missing_docs)]

pub mod effect;
pub mod query;
#[cfg(feature = "redis")]
pub mod redis;
#[cfg(any(feature = "sqlx-pg", feature = "sqlx-mysql"))]
pub mod sqlx;

pub use effect::hook::{AfterWrite, BeforeWrite, WriteQuery};
pub use effect::io::{Covers, Effect, Kind, Read, ReadKind, Write, WriteKind};
pub use effect::markers::{CanRead, CanWrite, Entity, ReadOnly, ReadWrite};
pub use query::{DataSource, Db, Execute, Query};
