//! A processor that runs after each transaction, on its final state.
//!
//! [`AfterTransaction`] wraps a sqlx [`Db`] handle. After every
//! [`Transaction`] whose flow ran, it hands the flow's report and what
//! became of the transaction ([`TxFinalState`]) to its hook. Whether to act
//! on a commit, a rollback, or an unknown commit is the hook's choice.
//!
//! As with [write hooks](crate::effect::hook), a hook error goes to an error
//! handler whose own error is [`Infallible`], and never changes what the
//! caller gets.
//!
//! Queries run on their own are forwarded to the wrapped handle, so write
//! hooks can wrap this one: `AfterWrite<AfterTransaction<Db<_, _>, _, _>, _, _>`.
//! Reach the transaction hook again through
//! [`AfterWrite::inner`](crate::effect::hook::AfterWrite::inner).

use crate::effect::hook::run_hook;
use crate::effect::io::Covers;
use crate::query::{Db, Execute};
use crate::sqlx::backend::{SqlxBackend, SqlxSource};
use crate::sqlx::transaction::{
    FlowEffect, Transaction, TxError, TxFinalState, TxFlow, TxOutput, drive,
};
use kanau::processor::{Processor, ProcessorReturn};
use std::convert::Infallible;

/// Runs `hook` on the final state of each transaction.
#[derive(Debug, Clone)]
pub struct AfterTransaction<D, H, EH> {
    inner: D,
    hook: H,
    on_error: EH,
}

impl<D, H, EH> AfterTransaction<D, H, EH> {
    /// Wrap `inner`, sending hook errors to `on_error`.
    pub const fn new(inner: D, hook: H, on_error: EH) -> Self {
        Self { inner, hook, on_error }
    }

    /// The wrapped handle.
    pub const fn inner(&self) -> &D {
        &self.inner
    }
}

impl<DB, C, F, H, EH> Processor<Transaction<F>> for AfterTransaction<Db<SqlxSource<DB>, C>, H, EH>
where
    DB: SqlxBackend,
    F: TxFlow<Database = DB>,
    C: Covers<FlowEffect<F>>,
    H: Processor<TxFinalState<F::Report>> + Sync,
    H::Error: Send,
    EH: Processor<H::Error, Error = Infallible> + Sync,
{
    type Output = TxOutput<F::Commit, F::Abort>;
    type Error = TxError;

    async fn process(&self, Transaction(flow): Transaction<F>) -> Result<Self::Output, TxError> {
        let done = drive(self.inner.source(), flow).await;
        if let Some(state) = done.report {
            run_hook(&self.hook, &self.on_error, state).await;
        }
        done.output
    }
}

impl<DB, C, Q, H, EH> Processor<Q> for AfterTransaction<Db<SqlxSource<DB>, C>, H, EH>
where
    DB: SqlxBackend,
    Q: Execute<SqlxSource<DB>>,
    C: Covers<Q::Effect>,
    H: Sync,
    EH: Sync,
{
    type Output = Q::Output;
    type Error = ::sqlx::Error;

    fn process(
        &self,
        query: Q,
    ) -> impl Future<Output = ProcessorReturn<Db<SqlxSource<DB>, C>, Q>> + Send {
        self.inner.process(query)
    }
}
