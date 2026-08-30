//! Async backend utilities for RabbitMQ, Redis, SQLx, and lightweight pooling.
//!
//! This crate exposes feature-gated modules for messaging and storage helpers,
//! plus common error and pooling primitives.
#![warn(missing_docs)]

#[cfg(feature = "amqprs")]
/// RabbitMQ publishing/consuming abstractions.
pub mod amqp;

/// Shared crate error type and conversions.
pub mod error;
/// Traits for interval-based signal generation.
pub mod interval_job;
/// A lightweight bounded async resource pool.
pub mod pool;

#[cfg(feature = "sqlx")]
/// SQLx PostgreSQL wrapper utilities.
pub mod sqlx;

#[cfg(feature = "redis")]
/// Redis key-value helper traits and types.
pub mod redis;

#[cfg(feature = "surreal")]
/// SurrealDB wrapper utilities.
pub mod surreal;

/// Re-exported crate error type.
pub use error::Error;
