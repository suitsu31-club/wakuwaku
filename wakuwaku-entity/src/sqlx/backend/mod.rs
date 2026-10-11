//! sqlx pools as data sources, and what each database does differently.

#[cfg(feature = "sqlx-mysql")]
pub mod mysql;
#[cfg(feature = "sqlx-pg")]
pub mod pgsql;

#[cfg(feature = "sqlx-mysql")]
pub use mysql::MySqlSource;
#[cfg(feature = "sqlx-pg")]
pub use pgsql::PgSource;

use crate::query::{DataSource, Execute};
use crate::sqlx::transaction::flow::TxMode;
use ::sqlx::{Database, Pool, Transaction};

/// A database the transaction driver knows how to handle.
pub trait SqlxBackend: Database {
    /// Start a transaction in `mode` on `conn`.
    fn begin(
        conn: &mut Self::Connection,
        mode: TxMode,
    ) -> impl Future<Output = Result<Transaction<'_, Self>, ::sqlx::Error>> + Send;

    /// Whether `error`, returned by a statement inside a transaction, ended
    /// that transaction on the server.
    ///
    /// After such an error the transaction can only be rolled back: further
    /// statements fail, or, worse, run outside the transaction.
    fn aborts_transaction(error: &::sqlx::Error) -> bool;
}

/// Whether `error` was raised by sqlx on the client, after or instead of
/// sending the statement, so it can't have changed the server's state.
#[allow(unused)]
pub(crate) fn is_client_side(error: &::sqlx::Error) -> bool {
    use ::sqlx::Error;
    matches!(
        error,
        Error::RowNotFound
            | Error::TypeNotFound { .. }
            | Error::ColumnIndexOutOfBounds { .. }
            | Error::ColumnNotFound(_)
            | Error::ColumnDecode { .. }
            | Error::Encode(_)
            | Error::Decode(_)
            | Error::InvalidArgument(_)
    )
}

/// An sqlx connection pool as a [`DataSource`].
///
/// Each query on its own acquires a connection from the pool and runs in
/// autocommit mode. The pool itself stays private to keep
/// [`Db`](crate::query::Db) capabilities sound; run migrations and other
/// setup on the pool before wrapping it.
#[derive(Debug)]
pub struct SqlxSource<DB: Database> {
    pool: Pool<DB>,
}

impl<DB: Database> SqlxSource<DB> {
    /// Run queries on connections from `pool`.
    pub const fn new(pool: Pool<DB>) -> Self {
        Self { pool }
    }

    pub(crate) const fn pool(&self) -> &Pool<DB> {
        &self.pool
    }
}

impl<DB: Database> Clone for SqlxSource<DB> {
    fn clone(&self) -> Self {
        Self::new(self.pool.clone())
    }
}

impl<DB: SqlxBackend> DataSource for SqlxSource<DB> {
    type Connection = DB::Connection;
    type Error = ::sqlx::Error;

    async fn run<Q: Execute<Self>>(&self, query: Q) -> Result<Q::Output, ::sqlx::Error> {
        let mut conn = self.pool.acquire().await?;
        query.execute(&mut *conn).await
    }
}
