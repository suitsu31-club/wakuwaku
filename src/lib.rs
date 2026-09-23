//! Async backend utilities for RabbitMQ, Redis, SQLx, and lightweight pooling.
//!
//! This crate exposes feature-gated modules for messaging and storage helpers,
//! plus common error and pooling primitives.
#![warn(missing_docs)]

/// Shared crate error type and conversions.
pub mod error;
/// Traits for interval-based signal generation.
pub mod interval_job;
/// A lightweight bounded async resource pool.
pub mod pool;

pub mod integration;
pub mod services;

/// Re-exported crate error type.
pub use error::Error;
