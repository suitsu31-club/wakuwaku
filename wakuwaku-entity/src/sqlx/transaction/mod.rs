//! Transactions on sqlx sources.
//!
//! Write the transaction as a [`TxFlow`] (see [`flow`]), wrap its first state
//! in a [`Transaction`], and process it with a [`Db`] whose capability covers
//! the flow's effect. Each step runs its queries and returns the next state;
//! the queries are the same [`Execute`](crate::Execute) values that run on
//! their own.
//!
//! ```no_run
//! use kanau::processor::Processor;
//! use sqlx::{PgConnection, PgPool, Postgres};
//! use wakuwaku_entity::sqlx::backend::{PgSource, SqlxSource};
//! use wakuwaku_entity::sqlx::transaction::{
//!     Either, Transaction, Tx, TxEnd, TxFlow, TxOutput, TxState,
//! };
//! use wakuwaku_entity::{Db, Entity, Execute, Query, Read, ReadWrite, Write};
//!
//! struct Account;
//! impl Entity for Account {}
//!
//! /// Takes `amount` from `from` if the balance allows it. Rows affected.
//! struct Debit { from: i64, amount: i64 }
//!
//! impl Query for Debit {
//!     type Effect = (Read<Account>, Write<Account>);
//! }
//!
//! impl Execute<SqlxSource<Postgres>> for Debit {
//!     type Output = u64;
//!     type Error = sqlx::Error;
//!     async fn execute(self, conn: &mut PgConnection) -> Result<u64, sqlx::Error> {
//!         let done = sqlx::query("UPDATE account SET balance = balance - $2 WHERE id = $1 AND balance >= $2")
//!             .bind(self.from)
//!             .bind(self.amount)
//!             .execute(conn)
//!             .await?;
//!         Ok(done.rows_affected())
//!     }
//! }
//!
//! /// Adds `amount` to `to`, and returns the new balance.
//! struct Credit { to: i64, amount: i64 }
//!
//! impl Query for Credit {
//!     type Effect = Write<Account>;
//! }
//!
//! impl Execute<SqlxSource<Postgres>> for Credit {
//!     type Output = Option<i64>;
//!     type Error = sqlx::Error;
//!     async fn execute(self, conn: &mut PgConnection) -> Result<Option<i64>, sqlx::Error> {
//!         sqlx::query_scalar("UPDATE account SET balance = balance + $2 WHERE id = $1 RETURNING balance")
//!             .bind(self.to)
//!             .bind(self.amount)
//!             .fetch_optional(conn)
//!             .await
//!     }
//! }
//!
//! /// Debit, then credit.
//! enum Transfer {
//!     Start { from: i64, to: i64, amount: i64 },
//!     Debited { to: i64, amount: i64 },
//!     End(TxEnd<Transfer>),
//! }
//!
//! impl Transfer {
//!     fn fail(why: &'static str) -> Self {
//!         Transfer::End(TxEnd::Rollback { output: why, report: None })
//!     }
//! }
//!
//! impl Query for Transfer {
//!     type Effect = (Read<Account>, Write<Account>);
//! }
//!
//! impl From<TxEnd<Transfer>> for Transfer {
//!     fn from(end: TxEnd<Transfer>) -> Self {
//!         Transfer::End(end)
//!     }
//! }
//!
//! impl TxFlow for Transfer {
//!     type Database = Postgres;
//!     /// The new balance of the credited account.
//!     type Commit = i64;
//!     type Abort = &'static str;
//!     /// Event to publish once the transaction ended.
//!     type Report = Option<i64>;
//!
//!     async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
//!         match state {
//!             Transfer::Start { from, to, amount } => {
//!                 let (tx, debited) = tx.run(Debit { from, amount }).await;
//!                 tx.with(match debited {
//!                     Ok(1) => Transfer::Debited { to, amount },
//!                     Ok(_) => Transfer::fail("insufficient funds"),
//!                     Err(_) => Transfer::fail("debit failed"),
//!                 })
//!             }
//!             Transfer::Debited { to, amount } => {
//!                 let (tx, credited) = tx.run(Credit { to, amount }).await;
//!                 tx.with(match credited {
//!                     Ok(Some(balance)) => {
//!                         Transfer::End(TxEnd::Commit { output: balance, report: Some(amount) })
//!                     }
//!                     Ok(None) => Transfer::fail("no such account"),
//!                     Err(_) => Transfer::fail("credit failed"),
//!                 })
//!             }
//!             end @ Transfer::End(_) => tx.end(end).await,
//!         }
//!     }
//!
//!     fn finished(self) -> Either<Self, TxEnd<Self>> {
//!         match self {
//!             Transfer::End(end) => Either::Right(end),
//!             state => Either::Left(state),
//!         }
//!     }
//! }
//!
//! # async fn run(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! let db: Db<PgSource, ReadWrite> = Db::new(SqlxSource::new(pool));
//! let outcome = db.process(Transaction(Transfer::Start { from: 1, to: 2, amount: 10 })).await?;
//! assert!(matches!(outcome, TxOutput::Committed(_)));
//! # Ok(())
//! # }
//! ```
//!
//! To act on how the transaction ended (publish its events, invalidate a
//! cache, …), wrap the handle in an [`AfterTransaction`](hook::AfterTransaction).

pub mod flow;
pub mod hook;

pub use either::Either;
pub use flow::{
    AsSqlxError, IsolationLevel, Tx, TxEnd, TxError, TxFinalState, TxFlow, TxMode, TxOutput,
    TxState,
};

use crate::effect::io::Covers;
use crate::query::Db;
use crate::sqlx::backend::{SqlxBackend, SqlxSource};
use flow::Done;
use kanau::processor::Processor;

/// Run the flow `F`, starting from the state it holds, as one transaction.
#[derive(Debug, Clone)]
pub struct Transaction<F>(pub F);

impl<DB, C, F> Processor<Transaction<F>> for Db<SqlxSource<DB>, C>
where
    DB: SqlxBackend,
    F: TxFlow<Database = DB>,
    C: Covers<F::Effect>,
{
    type Output = TxOutput<F::Commit, F::Abort>;
    type Error = TxError;

    async fn process(&self, Transaction(flow): Transaction<F>) -> Result<Self::Output, TxError> {
        drive(self.source(), flow).await.output
    }
}

/// Run `flow` on a connection from `source`: end it if its state is
/// terminal, else step it, until it ends.
pub(crate) async fn drive<F: TxFlow>(source: &SqlxSource<F::Database>, flow: F) -> Done<F> {
    let mut conn = match source.pool().acquire().await {
        Ok(conn) => conn,
        Err(error) => return Done::not_begun(error),
    };
    let tx = match F::Database::begin(&mut conn, flow::mode::<F>()).await {
        Ok(tx) => tx,
        Err(error) => return Done::not_begun(error),
    };
    let (mut tx, mut state) = (Tx::new(tx), flow);
    loop {
        (tx, state) = match tx.end(state).await.into_open() {
            Either::Left(open) => open,
            Either::Right(done) => return done,
        };
        (tx, state) = match F::step(tx, state).await.into_open() {
            Either::Left(open) => open,
            Either::Right(done) => return done,
        };
    }
}
