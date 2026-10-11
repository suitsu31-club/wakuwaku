//! PostgreSQL.
//!
//! Any statement error aborts a PostgreSQL transaction: until it is rolled
//! back, every later statement fails and `COMMIT` silently rolls back. Only
//! errors sqlx raises on the client (decoding, a missing row, …) leave it
//! usable.

use crate::sqlx::backend::{SqlxBackend, SqlxSource, is_client_side};
use crate::sqlx::transaction::flow::{IsolationLevel, TxMode};
use ::sqlx::{Connection, PgConnection, Postgres, Transaction};

/// A PostgreSQL pool as a data source.
pub type PgSource = SqlxSource<Postgres>;

impl SqlxBackend for Postgres {
    fn begin(
        conn: &mut PgConnection,
        mode: TxMode,
    ) -> impl Future<Output = Result<Transaction<'_, Self>, ::sqlx::Error>> + Send {
        conn.begin_with(begin_statement(mode))
    }

    fn aborts_transaction(error: &::sqlx::Error) -> bool {
        !is_client_side(error)
    }
}

const fn begin_statement(mode: TxMode) -> &'static str {
    use IsolationLevel::*;
    match (mode.isolation, mode.read_only) {
        (None, false) => "BEGIN READ WRITE",
        (None, true) => "BEGIN READ ONLY",
        (Some(ReadUncommitted), false) => "BEGIN ISOLATION LEVEL READ UNCOMMITTED READ WRITE",
        (Some(ReadUncommitted), true) => "BEGIN ISOLATION LEVEL READ UNCOMMITTED READ ONLY",
        (Some(ReadCommitted), false) => "BEGIN ISOLATION LEVEL READ COMMITTED READ WRITE",
        (Some(ReadCommitted), true) => "BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY",
        (Some(RepeatableRead), false) => "BEGIN ISOLATION LEVEL REPEATABLE READ READ WRITE",
        (Some(RepeatableRead), true) => "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
        (Some(Serializable), false) => "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
        (Some(Serializable), true) => "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY",
    }
}
