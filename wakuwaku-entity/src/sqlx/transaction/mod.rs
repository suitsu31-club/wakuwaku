//! Transactions on sqlx sources.
//!
//! Write the transaction as a [`TxFlow`] (see [`flow`]), wrap it in a
//! [`Transaction`], and process it with a [`Db`] whose capability covers the
//! flow's effect:
//!
//! ```no_run
//! use kanau::processor::Processor;
//! use sqlx::{PgConnection, PgPool, Postgres};
//! use wakuwaku_entity::sqlx::backend::{PgSource, SqlxSource};
//! use wakuwaku_entity::sqlx::transaction::{Step, Transaction, TxFlow, TxOutput};
//! use wakuwaku_entity::{Db, Entity, Execute, Query, Read, ReadWrite, Write};
//!
//! struct Account;
//! impl Entity for Account {}
//!
//! /// The queries of the transfer, and what they return.
//! enum TransferQuery {
//!     Debit { from: i64, amount: i64 },
//!     Credit { to: i64, amount: i64 },
//! }
//!
//! impl Query for TransferQuery {
//!     type Effect = (Read<Account>, Write<Account>);
//! }
//!
//! impl Execute<SqlxSource<Postgres>> for TransferQuery {
//!     /// Rows affected.
//!     type Output = u64;
//!     async fn execute(self, conn: &mut PgConnection) -> Result<u64, sqlx::Error> {
//!         let done = match self {
//!             TransferQuery::Debit { from, amount } => {
//!                 sqlx::query("UPDATE account SET balance = balance - $2 WHERE id = $1 AND balance >= $2")
//!                     .bind(from)
//!                     .bind(amount)
//!                     .execute(conn)
//!                     .await?
//!             }
//!             TransferQuery::Credit { to, amount } => {
//!                 sqlx::query("UPDATE account SET balance = balance + $2 WHERE id = $1")
//!                     .bind(to)
//!                     .bind(amount)
//!                     .execute(conn)
//!                     .await?
//!             }
//!         };
//!         Ok(done.rows_affected())
//!     }
//! }
//!
//! /// Debit, then credit. Each state is what the flow waits for.
//! enum Transfer {
//!     Start { from: i64, to: i64, amount: i64 },
//!     Debited { to: i64, amount: i64 },
//!     Credited { amount: i64 },
//! }
//!
//! impl TxFlow for Transfer {
//!     type Database = Postgres;
//!     type Query = TransferQuery;
//!     type Commit = ();
//!     type Abort = &'static str;
//!     /// Event to publish once the transaction ended.
//!     type Report = Option<i64>;
//!
//!     fn start(self) -> Step<Self> {
//!         let Transfer::Start { from, to, amount } = self else { unreachable!() };
//!         Step::Run {
//!             next: Transfer::Debited { to, amount },
//!             query: TransferQuery::Debit { from, amount },
//!         }
//!     }
//!
//!     fn resume(self, rows: Result<u64, sqlx::Error>) -> Step<Self> {
//!         match (self, rows) {
//!             (Transfer::Debited { to, amount }, Ok(1)) => Step::Run {
//!                 next: Transfer::Credited { amount },
//!                 query: TransferQuery::Credit { to, amount },
//!             },
//!             (Transfer::Credited { amount }, Ok(1)) => {
//!                 Step::Commit { output: (), report: Some(amount) }
//!             }
//!             _ => Step::Rollback { output: "insufficient funds or no account", report: None },
//!         }
//!     }
//!
//!     fn aborted(self, _: sqlx::Error) -> (&'static str, Option<i64>) {
//!         ("aborted", None)
//!     }
//! }
//!
//! # async fn run(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! let db: Db<PgSource, ReadWrite> = Db::new(SqlxSource::new(pool));
//! let outcome = db.process(Transaction(Transfer::Start { from: 1, to: 2, amount: 10 })).await?;
//! assert_eq!(outcome, TxOutput::Committed(()));
//! # Ok(())
//! # }
//! ```
//!
//! To act on how the transaction ended (publish its events, invalidate a
//! cache, …), wrap the handle in an [`AfterTransaction`](hook::AfterTransaction).

pub mod flow;
pub mod hook;

pub use flow::{
    FlowEffect, FlowOutput, IsolationLevel, Step, TxError, TxFinalState, TxFlow, TxMode, TxOutput,
};

use crate::effect::io::Covers;
use crate::query::{Db, Execute};
use crate::sqlx::backend::{SqlxBackend, SqlxSource};
use flow::{Begin, Done, TxMachine};
use kanau::processor::Processor;

/// Run the flow `F` as one transaction.
#[derive(Debug, Clone)]
pub struct Transaction<F>(pub F);

impl<DB, C, F> Processor<Transaction<F>> for Db<SqlxSource<DB>, C>
where
    DB: SqlxBackend,
    F: TxFlow<Database = DB>,
    C: Covers<FlowEffect<F>>,
{
    type Output = TxOutput<F::Commit, F::Abort>;
    type Error = TxError;

    async fn process(&self, Transaction(flow): Transaction<F>) -> Result<Self::Output, TxError> {
        drive(self.source(), flow).await.output
    }
}

/// Run `flow` on a connection from `source`, executing what its
/// [`TxMachine`] asks for.
pub(crate) async fn drive<F: TxFlow>(source: &SqlxSource<F::Database>, flow: F) -> Done<F> {
    let begin = Begin::new(flow);
    let mut conn = match source.pool().acquire().await {
        Ok(conn) => conn,
        Err(error) => return begin.failed(error),
    };
    let mut tx = match F::Database::begin(&mut conn, begin.mode()).await {
        Ok(tx) => tx,
        Err(error) => return begin.failed(error),
    };
    let mut machine = begin.begun();
    loop {
        machine = match machine {
            TxMachine::Run(running, query) => running.ran(query.execute(&mut tx).await),
            TxMachine::Commit(committing) => return committing.committed(tx.commit().await),
            TxMachine::Rollback(rolling_back) => {
                // The outcome is the same either way; see `RollingBack::rolled_back`.
                let _ = tx.rollback().await;
                return rolling_back.rolled_back();
            }
        };
    }
}
