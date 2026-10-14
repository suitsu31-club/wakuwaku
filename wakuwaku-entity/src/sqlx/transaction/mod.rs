//! Transactions on sqlx sources.
//!
//! Write the transaction as a [`TxFlow`] that declares what it touches with
//! [`tx_effect!`](crate::tx_effect), wrap its first state in a
//! [`Transaction`], and process it with a [`Db`] whose capability covers
//! that declaration. The result is a [`TxOutput`] or a [`TxError`]; to act
//! on how the transaction ended, wrap the handle in an
//! [`AfterTransaction`](hook::AfterTransaction).
//!
//! ```no_run
//! use kanau::processor::Processor;
//! use sqlx::{PgConnection, PgPool, Postgres};
//! use wakuwaku_entity::sqlx::backend::{PgSource, SqlxSource};
//! use wakuwaku_entity::sqlx::transaction::{Either, Transaction, Tx, TxEnd, TxFlow, TxState};
//! use wakuwaku_entity::{Db, Entity, Execute, Query, ReadWrite, Write, tx_effect};
//!
//! struct Account;
//! impl Entity for Account {}
//!
//! /// Adds `amount` to `to`. Rows affected.
//! struct Credit { to: i64, amount: i64 }
//! impl Query for Credit {
//!     type Effect = Write<Account>;
//! }
//! impl Execute<SqlxSource<Postgres>> for Credit {
//!     type Output = u64;
//!     type Error = sqlx::Error;
//!     async fn execute(self, conn: &mut PgConnection) -> Result<u64, sqlx::Error> {
//!         let done = sqlx::query("UPDATE account SET balance = balance + $2 WHERE id = $1")
//!             .bind(self.to)
//!             .bind(self.amount)
//!             .execute(conn)
//!             .await?;
//!         Ok(done.rows_affected())
//!     }
//! }
//!
//! enum Bonus {
//!     Start { to: i64 },
//!     End(TxEnd<Bonus>),
//! }
//! tx_effect! {
//!     enum BonusEffect { write Account }
//! }
//!
//! impl From<TxEnd<Bonus>> for Bonus {
//!     fn from(end: TxEnd<Bonus>) -> Self {
//!         Bonus::End(end)
//!     }
//! }
//! impl TxFlow for Bonus {
//!     type Capability = BonusEffect;
//!     type Database = Postgres;
//!     type Commit = ();
//!     type Abort = ();
//!     type Report = ();
//!
//!     async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
//!         let Bonus::Start { to } = state else { unreachable!("never stepped when terminal") };
//!         let (tx, credited) = tx.run(Credit { to, amount: 10 }).await;
//!         tx.with(Bonus::End(match credited {
//!             Ok(1) => TxEnd::Commit { output: (), report: () },
//!             _ => TxEnd::Rollback { output: (), report: () },
//!         }))
//!     }
//!
//!     fn finished(self) -> Either<Self, TxEnd<Self>> {
//!         match self {
//!             Bonus::End(end) => Either::Right(end),
//!             state => Either::Left(state),
//!         }
//!     }
//! }
//!
//! # async fn run(pool: PgPool) -> Result<(), Box<dyn std::error::Error>> {
//! let db: Db<PgSource, ReadWrite> = Db::new(SqlxSource::new(pool));
//! let outcome = db.process(Transaction(Bonus::Start { to: 2 })).await?;
//! # Ok(())
//! # }
//! ```
//!
//! See the [transactions guide](https://www.wakuwaku.rs/en/docs/data/transactions).

pub mod flow;
pub mod hook;

pub use either::Either;
pub use flow::{
    AsSqlxError, IsolationLevel, Tx, TxEffect, TxEnd, TxError, TxFinalState, TxFlow, TxMode,
    TxOutput, TxState,
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
    C: Covers<<F::Capability as TxEffect>::Effect>,
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
