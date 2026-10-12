//! A proof-driven microservice framework.
//!
//! `wakuwaku` wires a service together: it builds every component from a
//! type-level registry of shared dependencies, serves the gRPC servers,
//! starts the event consumers and runs the cron jobs. The pieces it wires
//! live in their own crates, re-exported here behind feature flags, so a
//! service needs only this one dependency.
//!
//! # Proof-driven programming
//!
//! Correctness is split in two:
//!
//! - **Macro correctness**, how components fit together, is proven by
//!   traits and checked by the compiler. A component whose dependency was
//!   never registered, a query run through a handle that lacks the
//!   capability, a cron job with no interval: none of these compile.
//! - **Micro correctness**, what one component does, is proven by unit
//!   tests. Every component is a plain value implementing
//!   [`kanau::processor::Processor`] (or a trait built on it), so a test
//!   calls it directly, without a running service.
//!
//! # What this crate provides
//!
//! - [`services::builder`]: [`ServiceBuilder`], the type-indexed registry of
//!   shared dependencies, and [`ServiceCreation`], how a component is built
//!   from one of them.
//! - [`services::cron_job`]: [`CronJob`]s, run locally at a fixed interval.
//! - `services::grpc_service` (feature `grpc`): registering tonic servers
//!   built from the service builder.
//! - `services::iggy_consumer` (feature `iggy`): registering Iggy event
//!   handlers built from the service builder.
//!
//! # Re-exported crates
//!
//! | Path             | Crate             | Feature                                              |
//! |------------------|-------------------|------------------------------------------------------|
//! | `proof`          | `wakuwaku-proof`  | `proof`                                              |
//! | `iggy`           | `wakuwaku-iggy`   | `iggy`                                               |
//! | `entity`         | `wakuwaku-entity` | `entity`, `sqlx-pg`, `sqlx-mysql`, `redis`, `meilisearch` |
//! | [`deps`]         | third-party crates in the public API of the above | matching feature |
//!
//! # Features
//!
//! | Feature       | Default | Enables                                                     |
//! |---------------|---------|-------------------------------------------------------------|
//! | `grpc`        | yes     | gRPC server registration, `deps::tonic`                     |
//! | `proof`       | yes     | `proof`                                                     |
//! | `iggy`        | no      | `iggy`, Iggy consumer registration, `deps::iggy`            |
//! | `entity`      | no      | `entity` without a data source                              |
//! | `sqlx-pg`     | no      | `entity` with PostgreSQL through sqlx, `deps::sqlx`         |
//! | `sqlx-mysql`  | no      | `entity` with MySQL through sqlx, `deps::sqlx`              |
//! | `redis`       | no      | `entity` with Redis, `deps::redis`                          |
//! | `meilisearch` | no      | `entity` with Meilisearch, `deps::meilisearch_sdk`          |

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

pub mod services;

pub use services::ServiceCreation;
pub use services::builder::ServiceBuilder;
pub use services::cron_job::{CronJob, CronTick};

/// Capability-typed data access: the [`wakuwaku-entity`](wakuwaku_entity)
/// crate.
#[cfg(feature = "entity")]
pub use wakuwaku_entity as entity;

/// Typed events on Apache Iggy: the [`wakuwaku-iggy`](wakuwaku_iggy) crate.
#[cfg(feature = "iggy")]
pub use wakuwaku_iggy as iggy;

/// Proof-carrying validation: the [`wakuwaku-proof`](wakuwaku_proof) crate.
#[cfg(feature = "proof")]
pub use wakuwaku_proof as proof;

/// Third-party crates whose types appear in the public API of `wakuwaku`
/// and the crates it re-exports.
///
/// Use these paths instead of depending on the crates directly, so the
/// versions always match the ones `wakuwaku` was built against.
pub mod deps {
    pub use kanau;

    #[cfg(feature = "grpc")]
    pub use tonic;

    #[cfg(feature = "iggy")]
    pub use iggy;

    #[cfg(any(feature = "sqlx-pg", feature = "sqlx-mysql"))]
    pub use sqlx;

    #[cfg(feature = "redis")]
    pub use redis;

    #[cfg(feature = "meilisearch")]
    pub use meilisearch_sdk;
}
