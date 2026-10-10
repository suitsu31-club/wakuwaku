//! sqlx data sources and transactions.
//!
//! - [`backend`]: [`SqlxSource`](backend::SqlxSource), a [`DataSource`]
//!   over an sqlx pool, and the per-database [`SqlxBackend`](backend::SqlxBackend)
//!   implementations for PostgreSQL (`sqlx-pg`) and MySQL (`sqlx-mysql`).
//! - [`transaction`]: transactions written as pure state machines, the
//!   driver that runs them, and the hook on their final state.
//!
//! Queries implement [`Execute<SqlxSource<DB>>`](crate::query::Execute)
//! with `conn: &mut DB::Connection`, and run through a
//! [`Db`](crate::query::Db) on their own or as steps of a transaction.
//!
//! [`DataSource`]: crate::query::DataSource

pub mod backend;
pub mod transaction;
