//! MySQL (InnoDB).
//!
//! A failed statement normally rolls back only itself, and the transaction
//! goes on. A deadlock (error 1213) rolls back the whole transaction, after
//! which later statements would run in autocommit mode, outside any
//! transaction; the driver treats it as aborting.
//!
//! A lock wait timeout (error 1205) is treated as rolling back only the
//! statement, which is the server default. With
//! `innodb_rollback_on_timeout = ON` it rolls back the whole transaction too,
//! and flows must not continue after it.

use crate::sqlx::backend::{SqlxBackend, SqlxSource, is_client_side};
use crate::sqlx::transaction::flow::{IsolationLevel, TxMode};
use ::sqlx::mysql::MySqlDatabaseError;
use ::sqlx::{Connection, Executor, MySql, MySqlConnection, Transaction};

/// A MySQL pool as a data source.
pub type MySqlSource = SqlxSource<MySql>;

/// `ER_LOCK_DEADLOCK`.
const LOCK_DEADLOCK: u16 = 1213;

impl SqlxBackend for MySql {
    async fn begin(
        conn: &mut MySqlConnection,
        mode: TxMode,
    ) -> Result<Transaction<'_, Self>, ::sqlx::Error> {
        // `START TRANSACTION` can't name an isolation level. Without
        // `SESSION`, `SET TRANSACTION` applies to the next transaction only.
        if let Some(level) = mode.isolation {
            conn.execute(set_isolation_statement(level)).await?;
        }
        conn.begin_with(if mode.read_only {
            "START TRANSACTION READ ONLY"
        } else {
            "START TRANSACTION READ WRITE"
        })
        .await
    }

    fn aborts_transaction(error: &::sqlx::Error) -> bool {
        match error {
            ::sqlx::Error::Database(error) => error
                .try_downcast_ref::<MySqlDatabaseError>()
                .is_some_and(|error| error.number() == LOCK_DEADLOCK),
            error => !is_client_side(error),
        }
    }
}

const fn set_isolation_statement(level: IsolationLevel) -> &'static str {
    match level {
        IsolationLevel::ReadUncommitted => "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED",
        IsolationLevel::ReadCommitted => "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
        IsolationLevel::RepeatableRead => "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
        IsolationLevel::Serializable => "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE",
    }
}
