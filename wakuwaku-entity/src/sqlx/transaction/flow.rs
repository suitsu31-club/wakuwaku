//! Transactions as state machines whose steps run queries.
//!
//! A [`TxFlow`] value is the current state. [`TxFlow::step`] gets it with
//! the open transaction, a [`Tx`], runs queries with [`Tx::run`], and
//! returns the next state as a [`TxState`] ([`Tx::with`] or [`Tx::end`]).
//! A state is terminal when [`TxFlow::finished`] projects it to a
//! [`TxEnd`]; the driver ends such a state instead of stepping it.
//!
//! See the [transactions guide](https://www.wakuwaku.rs/en/docs/data/transactions).

use crate::effect::io::{Effect, Kind};
use crate::query::{Execute, Query};
use crate::sqlx::backend::{SqlxBackend, SqlxSource};
use ::sqlx::Transaction;
use either::Either;
use std::marker::PhantomData;

/// SQL transaction isolation level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// `READ UNCOMMITTED`. PostgreSQL runs it as `READ COMMITTED`.
    ReadUncommitted,
    /// `READ COMMITTED`.
    ReadCommitted,
    /// `REPEATABLE READ`.
    RepeatableRead,
    /// `SERIALIZABLE`.
    Serializable,
}

/// How a transaction starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxMode {
    /// Isolation level, or `None` for the server's default.
    pub isolation: Option<IsolationLevel>,
    /// Start the transaction `READ ONLY`.
    pub read_only: bool,
}

/// The error of a query that runs in a transaction.
///
/// Whether a failed query aborted the transaction depends on the sqlx error
/// behind it (see [`SqlxBackend::aborts_transaction`]). An error of the
/// query's own, such as a lost optimistic-lock fence, never aborts it.
pub trait AsSqlxError {
    /// The sqlx error this is or wraps, or `None` for an error of the
    /// query's own.
    fn as_sqlx_error(&self) -> Option<&::sqlx::Error>;
}

impl AsSqlxError for ::sqlx::Error {
    fn as_sqlx_error(&self) -> Option<&::sqlx::Error> {
        Some(self)
    }
}

/// Whether the effect of the query `Q` writes.
type EffectKind<Q> = <<Q as Query>::Effect as Effect>::Kind;

/// The business logic of a transaction, as a state machine.
///
/// The implementing type is the state. Its [`Effect`](Query::Effect) is
/// everything the transaction may touch: a [`Db`](crate::query::Db) must
/// cover it to run the flow, and a flow whose effect only reads starts its
/// transaction `READ ONLY` and can't run a query that writes.
///
/// The terminal states are the [`TxEnd`]s, turned into states by
/// `From<TxEnd<Self>>`. [`finished`](Self::finished) must give each back:
/// `finished(end.into())` is `Right(end)`.
pub trait TxFlow: Query + From<TxEnd<Self>> {
    /// The database the flow runs on.
    type Database: SqlxBackend;
    /// Returned to the caller after a commit.
    type Commit: Send;
    /// Returned to the caller after a rollback.
    type Abort: Send;
    /// Handed to the transaction hook in the [final state](super::TxFinalState).
    type Report: Send;

    /// Isolation level of the transaction, or `None` for the server's default.
    const ISOLATION: Option<IsolationLevel> = None;

    /// Run the queries of `state` on `tx`, and return the next state.
    ///
    /// Never called with a terminal state. A query error reaches the step
    /// as the query's result; if it ended the transaction on the server
    /// ([`Tx::is_aborted`]), later queries fail without being sent and the
    /// flow can only roll back, so it should go to a terminal state.
    fn step(tx: Tx<'_, Self>, state: Self) -> impl Future<Output = TxState<'_, Self>> + Send;

    /// `Right` with how the transaction ends if the state is terminal, the
    /// state itself otherwise.
    fn finished(self) -> Either<Self, TxEnd<Self>>;
}

/// How a flow ends: its terminal states.
///
/// Not `Clone`: a terminal state is consumed once, when the transaction
/// ends.
#[derive(Debug)]
pub enum TxEnd<F: TxFlow> {
    /// Commit the transaction.
    Commit {
        /// For the caller.
        output: F::Commit,
        /// For the hook.
        report: F::Report,
    },
    /// Roll the transaction back.
    Rollback {
        /// For the caller.
        output: F::Abort,
        /// For the hook.
        report: F::Report,
    },
}

/// What the caller of a transaction gets when it ended as planned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxOutput<C, A> {
    /// The flow committed.
    Committed(C),
    /// The flow rolled back.
    RolledBack(A),
}

/// What the transaction hook gets: the flow's report, and what became of
/// the transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxFinalState<R> {
    /// The transaction committed.
    Committed(R),
    /// The transaction was rolled back, whether the flow asked for it, a
    /// statement aborted it, or the server rejected the `COMMIT`.
    RolledBack(R),
    /// `COMMIT` was sent but its result was lost: the transaction may or may
    /// not have committed.
    CommitUnknown(R),
}

/// Why a transaction didn't end as its flow planned.
#[derive(Debug, thiserror::Error)]
pub enum TxError {
    /// No connection, or `BEGIN` failed. The flow never ran.
    #[error("failed to begin transaction: {0}")]
    Begin(#[source] ::sqlx::Error),
    /// The server rejected `COMMIT`; the transaction is rolled back.
    #[error("commit rejected, transaction rolled back: {0}")]
    CommitRejected(#[source] ::sqlx::Error),
    /// `COMMIT` failed in transit; it may or may not have been applied.
    #[error("commit outcome unknown: {0}")]
    CommitUnknown(#[source] ::sqlx::Error),
    /// The flow ended with a commit after a statement had aborted the
    /// transaction; it was rolled back.
    #[error("transaction aborted by a failed statement, rolled back")]
    Aborted,
    /// [`TxFlow::finished`] didn't give back the end the transaction was
    /// closed with. If it still gave an end, the hook gets what became of
    /// the transaction.
    #[error("flow state after its end is not that end")]
    EndMismatch,
}

/// An open transaction, handed to [`TxFlow::step`].
pub struct Tx<'c, F: TxFlow> {
    tx: Transaction<'c, F::Database>,
    aborted: bool,
    flow: PhantomData<fn() -> F>,
}

impl<'c, F: TxFlow> Tx<'c, F> {
    pub(crate) const fn new(tx: Transaction<'c, F::Database>) -> Self {
        Self {
            tx,
            aborted: false,
            flow: PhantomData,
        }
    }

    /// Run `query` in the transaction.
    ///
    /// If its error ended the transaction on the server, the transaction is
    /// [aborted](Self::is_aborted): later queries fail without being sent,
    /// and it can only roll back.
    ///
    /// A query that writes can't run in a flow whose effect only reads.
    pub async fn run<Q>(mut self, query: Q) -> (Self, Result<Q::Output, Q::Error>)
    where
        Q: Execute<SqlxSource<F::Database>, Error: AsSqlxError>,
        EffectKind<Q>: Kind<Join<EffectKind<F>> = EffectKind<F>>,
    {
        if self.aborted {
            return (self, Err(already_aborted().into()));
        }
        let result = query.execute(&mut self.tx).await;
        if let Err(error) = &result
            && error
                .as_sqlx_error()
                .is_some_and(F::Database::aborts_transaction)
        {
            self.aborted = true;
        }
        (self, result)
    }

    /// Whether a query error ended the transaction on the server.
    pub const fn is_aborted(&self) -> bool {
        self.aborted
    }

    /// Go on with `state`.
    pub fn with(self, state: F) -> TxState<'c, F> {
        TxState {
            tx: Some(self.tx),
            state,
            aborted: self.aborted,
            commit: None,
        }
    }

    /// End the transaction if `state` is terminal, else go on with it.
    ///
    /// A [commit](TxEnd::Commit) sends `COMMIT`, unless the transaction is
    /// aborted: then it is rolled back, and the caller gets
    /// [`TxError::Aborted`]. A [rollback](TxEnd::Rollback) sends `ROLLBACK`;
    /// its result doesn't matter, as a transaction that was never committed
    /// can't commit any more.
    pub async fn end(self, state: F) -> TxState<'c, F> {
        let end = match state.finished() {
            Either::Left(state) => return self.with(state),
            Either::Right(end) => end,
        };
        let commit = if matches!(end, TxEnd::Commit { .. }) && !self.aborted {
            Some(self.tx.commit().await)
        } else {
            let _ = self.tx.rollback().await;
            None
        };
        TxState {
            tx: None,
            state: end.into(),
            aborted: self.aborted,
            commit,
        }
    }
}

/// The error of a query sent after the transaction was aborted. It counts
/// as aborting on every backend.
fn already_aborted() -> ::sqlx::Error {
    ::sqlx::Error::Protocol("transaction already aborted by an earlier statement".into())
}

/// A state of the flow, and the transaction it runs in: open, or ended if
/// the state is terminal. Built by [`Tx::with`] and [`Tx::end`].
pub struct TxState<'c, F: TxFlow> {
    /// `None` once [`Tx::end`] closed it, and only then.
    tx: Option<Transaction<'c, F::Database>>,
    state: F,
    aborted: bool,
    /// The result of `COMMIT`, if it was sent.
    commit: Option<Result<(), ::sqlx::Error>>,
}

impl<'c, F: TxFlow> TxState<'c, F> {
    /// The open transaction and the state to step, or how the transaction
    /// ended.
    pub(crate) fn into_open(self) -> Either<(Tx<'c, F>, F), Done<F>> {
        let Self {
            tx,
            state,
            aborted,
            commit,
        } = self;
        match tx {
            Some(tx) => Either::Left((
                Tx {
                    tx,
                    aborted,
                    flow: PhantomData,
                },
                state,
            )),
            None => Either::Right(close(state, aborted, commit)),
        }
    }
}

/// A finished transaction.
pub(crate) struct Done<F: TxFlow> {
    /// For the caller.
    pub(crate) output: Result<TxOutput<F::Commit, F::Abort>, TxError>,
    /// For the hook. `None` if the flow never ran.
    pub(crate) report: Option<TxFinalState<F::Report>>,
}

impl<F: TxFlow> Done<F> {
    /// No connection, or `BEGIN` failed.
    pub(crate) const fn not_begun(error: ::sqlx::Error) -> Self {
        Self {
            output: Err(TxError::Begin(error)),
            report: None,
        }
    }
}

/// How to begin a transaction for `F`: its isolation level, and `READ ONLY`
/// if its effect doesn't write.
pub(crate) const fn mode<F: TxFlow>() -> TxMode {
    TxMode {
        isolation: F::ISOLATION,
        read_only: !<EffectKind<F> as Kind>::WRITES,
    }
}

/// What became of a transaction that [`Tx::end`] closed with the terminal
/// `state`.
fn close<F: TxFlow>(state: F, aborted: bool, commit: Option<Result<(), ::sqlx::Error>>) -> Done<F> {
    let (output, report) = match state.finished() {
        Either::Right(TxEnd::Commit { output, report }) => (Either::Left(output), report),
        Either::Right(TxEnd::Rollback { output, report }) => (Either::Right(output), report),
        Either::Left(_) => {
            return Done {
                output: Err(TxError::EndMismatch),
                report: None,
            };
        }
    };
    let (output, state) = match (output, commit) {
        (Either::Left(output), Some(Ok(()))) => (
            Ok(TxOutput::Committed(output)),
            TxFinalState::Committed(report),
        ),
        (Either::Left(_), Some(Err(error @ ::sqlx::Error::Database(_)))) => (
            Err(TxError::CommitRejected(error)),
            TxFinalState::RolledBack(report),
        ),
        (Either::Left(_), Some(Err(error))) => (
            Err(TxError::CommitUnknown(error)),
            TxFinalState::CommitUnknown(report),
        ),
        (Either::Left(_), None) if aborted => {
            (Err(TxError::Aborted), TxFinalState::RolledBack(report))
        }
        (Either::Right(output), None) => (
            Ok(TxOutput::RolledBack(output)),
            TxFinalState::RolledBack(report),
        ),
        // `finished` turned the end the transaction was closed with into
        // the other one.
        (Either::Left(_), None) => (Err(TxError::EndMismatch), TxFinalState::RolledBack(report)),
        (Either::Right(_), Some(Ok(()))) => {
            (Err(TxError::EndMismatch), TxFinalState::Committed(report))
        }
        (Either::Right(_), Some(Err(::sqlx::Error::Database(_)))) => {
            (Err(TxError::EndMismatch), TxFinalState::RolledBack(report))
        }
        (Either::Right(_), Some(Err(_))) => (
            Err(TxError::EndMismatch),
            TxFinalState::CommitUnknown(report),
        ),
    };
    Done {
        output,
        report: Some(state),
    }
}

#[cfg(all(test, feature = "sqlx-pg"))]
mod tests {
    use super::*;
    use crate::effect::io::{Read, Write};
    use crate::effect::markers::Entity;
    use ::sqlx::Postgres;
    use ::sqlx::error::{DatabaseError, ErrorKind};
    use std::borrow::Cow;
    use std::error::Error as StdError;
    use std::fmt;

    struct Account;
    impl Entity for Account {}

    /// A flow that only ends: its states are a start and its ends.
    enum Check<X: Effect> {
        Start(PhantomData<fn() -> X>),
        End(TxEnd<Check<X>>),
    }

    impl<X: Effect> Query for Check<X> {
        type Effect = X;
    }

    impl<X: Effect> From<TxEnd<Check<X>>> for Check<X> {
        fn from(end: TxEnd<Check<X>>) -> Self {
            Check::End(end)
        }
    }

    impl<X: Effect> TxFlow for Check<X> {
        type Database = Postgres;
        type Commit = i64;
        type Abort = String;
        type Report = Vec<String>;

        const ISOLATION: Option<IsolationLevel> = Some(IsolationLevel::Serializable);

        async fn step(tx: Tx<'_, Self>, state: Self) -> TxState<'_, Self> {
            tx.end(state).await
        }

        fn finished(self) -> Either<Self, TxEnd<Self>> {
            match self {
                Check::End(end) => Either::Right(end),
                state => Either::Left(state),
            }
        }
    }

    type ReadCheck = Check<Read<Account>>;
    type WriteCheck = Check<(Read<Account>, Write<Account>)>;

    fn commit(output: i64) -> WriteCheck {
        Check::End(TxEnd::Commit {
            output,
            report: vec!["commit".into()],
        })
    }

    fn rollback(output: &str) -> WriteCheck {
        Check::End(TxEnd::Rollback {
            output: output.into(),
            report: vec!["rollback".into()],
        })
    }

    #[derive(Debug)]
    struct ServerError;

    impl fmt::Display for ServerError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("server error")
        }
    }

    impl StdError for ServerError {}

    impl DatabaseError for ServerError {
        fn message(&self) -> &str {
            "server error"
        }
        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed("40001"))
        }
        fn as_error(&self) -> &(dyn StdError + Send + Sync + 'static) {
            self
        }
        fn as_error_mut(&mut self) -> &mut (dyn StdError + Send + Sync + 'static) {
            self
        }
        fn into_error(self: Box<Self>) -> Box<dyn StdError + Send + Sync + 'static> {
            self
        }
        fn kind(&self) -> ErrorKind {
            ErrorKind::Other
        }
    }

    fn io_error() -> ::sqlx::Error {
        ::sqlx::Error::Io(std::io::Error::other("connection reset"))
    }

    #[test]
    fn mode_is_read_only_iff_effect_only_reads() {
        assert_eq!(
            mode::<ReadCheck>(),
            TxMode {
                isolation: Some(IsolationLevel::Serializable),
                read_only: true
            }
        );
        assert!(!mode::<WriteCheck>().read_only);
    }

    #[test]
    fn already_aborted_counts_as_aborting() {
        assert!(Postgres::aborts_transaction(&already_aborted()));
    }

    #[test]
    fn commit_path() {
        let done = close(commit(5), false, Some(Ok(())));
        assert!(matches!(done.output, Ok(TxOutput::Committed(5))));
        assert_eq!(
            done.report,
            Some(TxFinalState::Committed(vec!["commit".into()]))
        );
    }

    #[test]
    fn rollback_path_aborted_or_not() {
        for aborted in [false, true] {
            let done = close(rollback("no"), aborted, None);
            assert!(matches!(&done.output, Ok(TxOutput::RolledBack(why)) if why == "no"));
            assert_eq!(
                done.report,
                Some(TxFinalState::RolledBack(vec!["rollback".into()]))
            );
        }
    }

    #[test]
    fn commit_after_abort_is_rolled_back() {
        let done = close(commit(5), true, None);
        assert!(matches!(done.output, Err(TxError::Aborted)));
        assert_eq!(
            done.report,
            Some(TxFinalState::RolledBack(vec!["commit".into()]))
        );
    }

    #[test]
    fn rejected_commit_is_rolled_back_and_lost_commit_is_unknown() {
        let rejected = close(
            commit(5),
            false,
            Some(Err(::sqlx::Error::Database(Box::new(ServerError)))),
        );
        assert!(matches!(rejected.output, Err(TxError::CommitRejected(_))));
        assert!(matches!(rejected.report, Some(TxFinalState::RolledBack(_))));

        let lost = close(commit(5), false, Some(Err(io_error())));
        assert!(matches!(lost.output, Err(TxError::CommitUnknown(_))));
        assert!(matches!(lost.report, Some(TxFinalState::CommitUnknown(_))));
    }

    #[test]
    fn end_mismatch_reports_what_became_of_the_transaction() {
        let not_terminal = close(WriteCheck::Start(PhantomData), false, Some(Ok(())));
        assert!(matches!(not_terminal.output, Err(TxError::EndMismatch)));
        assert!(not_terminal.report.is_none());

        let committed_as_rollback = close(rollback("no"), false, Some(Ok(())));
        assert!(matches!(
            committed_as_rollback.output,
            Err(TxError::EndMismatch)
        ));
        assert!(matches!(
            committed_as_rollback.report,
            Some(TxFinalState::Committed(_))
        ));
    }
}
