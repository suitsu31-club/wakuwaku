//! Transactions as pure state machines.
//!
//! Nothing in this module does I/O. A [`TxFlow`] is the business logic of a
//! transaction: given the result of its last query, it decides the next
//! step. A [`TxMachine`] wraps a flow with the protocol around it (`BEGIN`,
//! `COMMIT`, `ROLLBACK`, and what each failure means), and tells the driver
//! which statement to send next. The driver in
//! [`transaction`](crate::sqlx::transaction) only executes what the machine
//! asks for and feeds the results back.
//!
//! Both are plain values and plain functions, so a flow can be tested by
//! feeding it results by hand, without a database.
//!
//! ```text
//! Begin ──begun──▶ Run ⇄ ran ──▶ Commit ──committed──▶ Done
//!   │                     └────▶ Rollback ─rolled_back─▶ Done
//!   └──failed──────────────────────────────────────────▶ Done
//! ```

use crate::effect::io::{Effect, Kind};
use crate::query::{Execute, Query};
use crate::sqlx::backend::{SqlxBackend, SqlxSource};

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

/// What the queries of the flow `F` return.
pub type FlowOutput<F> =
    <<F as TxFlow>::Query as Execute<SqlxSource<<F as TxFlow>::Database>>>::Output;

/// Everything the flow `F` may touch: the effect of its query type.
pub type FlowEffect<F> = <<F as TxFlow>::Query as Query>::Effect;

/// The business logic of a transaction, as a state machine.
///
/// The implementing type is the state. Each transition consumes it and
/// returns the next [`Step`]. The transitions are synchronous, so a flow
/// can't do I/O; it can only ask for queries.
///
/// A flow that issues several kinds of query uses an enum of them as
/// [`Query`](Self::Query), with an enum of their outputs. The effect of that
/// enum is the effect of the whole transaction: a [`Db`](crate::query::Db)
/// must cover it to run the flow, and a flow whose effect only reads starts
/// its transaction `READ ONLY`.
pub trait TxFlow: Sized + Send {
    /// The database the flow runs on.
    type Database: SqlxBackend;
    /// The queries the flow issues.
    type Query: Execute<SqlxSource<Self::Database>>;
    /// Returned to the caller after a commit.
    type Commit: Send;
    /// Returned to the caller after a rollback.
    type Abort: Send;
    /// Handed to the transaction hook in the [final state](super::TxFinalState).
    type Report: Send;

    /// Isolation level of the transaction, or `None` for the server's default.
    const ISOLATION: Option<IsolationLevel> = None;

    /// First step, right after `BEGIN`.
    fn start(self) -> Step<Self>;

    /// Next step, given the result of the last query.
    ///
    /// `output` is an error only if the transaction is still usable after
    /// it (see [`SqlxBackend::aborts_transaction`]). The flow may go on,
    /// commit, or roll back.
    fn resume(self, output: Result<FlowOutput<Self>, ::sqlx::Error>) -> Step<Self>;

    /// The last query failed and ended the transaction on the server, so it
    /// will be rolled back. Describe the outcome.
    fn aborted(self, error: ::sqlx::Error) -> (Self::Abort, Self::Report);
}

/// What a flow does next.
pub enum Step<F: TxFlow> {
    /// Run `query`, then resume `next` with its result.
    Run {
        /// State to resume.
        next: F,
        /// Query to run.
        query: F::Query,
    },
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
    /// The flow rolled back, or the transaction was aborted.
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
}

/// A finished transaction.
pub struct Done<F: TxFlow> {
    /// For the caller.
    pub output: Result<TxOutput<F::Commit, F::Abort>, TxError>,
    /// For the hook. `None` if the flow never ran.
    pub report: Option<TxFinalState<F::Report>>,
}

/// A transaction about to begin.
pub struct Begin<F> {
    flow: F,
}

impl<F: TxFlow> Begin<F> {
    /// Prepare to run `flow`.
    pub const fn new(flow: F) -> Self {
        Self { flow }
    }

    /// How to begin: the flow's isolation level, and `READ ONLY` if its
    /// effect doesn't write.
    pub const fn mode(&self) -> TxMode {
        TxMode {
            isolation: F::ISOLATION,
            read_only: !<<FlowEffect<F> as Effect>::Kind as Kind>::WRITES,
        }
    }

    /// `BEGIN` succeeded.
    pub fn begun(self) -> TxMachine<F> {
        TxMachine::step(self.flow.start())
    }

    /// No connection, or `BEGIN` failed.
    pub fn failed(self, error: ::sqlx::Error) -> Done<F> {
        Done { output: Err(TxError::Begin(error)), report: None }
    }
}

/// An open transaction, and the statement to send next.
pub enum TxMachine<F: TxFlow> {
    /// Run the query, then call [`Running::ran`].
    Run(Running<F>, F::Query),
    /// Send `COMMIT`, then call [`Committing::committed`].
    Commit(Committing<F>),
    /// Send `ROLLBACK`, then call [`RollingBack::rolled_back`].
    Rollback(RollingBack<F>),
}

impl<F: TxFlow> TxMachine<F> {
    fn step(step: Step<F>) -> Self {
        match step {
            Step::Run { next, query } => Self::Run(Running { flow: next }, query),
            Step::Commit { output, report } => Self::Commit(Committing { output, report }),
            Step::Rollback { output, report } => Self::Rollback(RollingBack { output, report }),
        }
    }
}

/// A flow waiting for the result of its query.
pub struct Running<F> {
    flow: F,
}

impl<F: TxFlow> Running<F> {
    /// The query finished with `result`.
    ///
    /// If the error aborted the transaction, the flow is asked for its
    /// outcome and the transaction rolls back; otherwise the flow resumes.
    pub fn ran(self, result: Result<FlowOutput<F>, ::sqlx::Error>) -> TxMachine<F> {
        match result {
            Err(error) if F::Database::aborts_transaction(&error) => {
                let (output, report) = self.flow.aborted(error);
                TxMachine::Rollback(RollingBack { output, report })
            }
            result => TxMachine::step(self.flow.resume(result)),
        }
    }
}

/// A transaction waiting for its `COMMIT`.
pub struct Committing<F: TxFlow> {
    output: F::Commit,
    report: F::Report,
}

impl<F: TxFlow> Committing<F> {
    /// `COMMIT` finished with `result`.
    ///
    /// An error from the server means it refused to commit, which leaves
    /// the transaction rolled back. Any other error leaves the outcome
    /// unknown.
    pub fn committed(self, result: Result<(), ::sqlx::Error>) -> Done<F> {
        let Self { output, report } = self;
        match result {
            Ok(()) => Done {
                output: Ok(TxOutput::Committed(output)),
                report: Some(TxFinalState::Committed(report)),
            },
            Err(error @ ::sqlx::Error::Database(_)) => Done {
                output: Err(TxError::CommitRejected(error)),
                report: Some(TxFinalState::RolledBack(report)),
            },
            Err(error) => Done {
                output: Err(TxError::CommitUnknown(error)),
                report: Some(TxFinalState::CommitUnknown(report)),
            },
        }
    }
}

/// A transaction waiting for its `ROLLBACK`.
pub struct RollingBack<F: TxFlow> {
    output: F::Abort,
    report: F::Report,
}

impl<F: TxFlow> RollingBack<F> {
    /// `ROLLBACK` was sent.
    ///
    /// Its result doesn't matter: a transaction that was never committed
    /// can't commit any more, whether `ROLLBACK` succeeded or the connection
    /// died.
    pub fn rolled_back(self) -> Done<F> {
        Done {
            output: Ok(TxOutput::RolledBack(self.output)),
            report: Some(TxFinalState::RolledBack(self.report)),
        }
    }
}

#[cfg(all(test, feature = "sqlx-pg"))]
mod tests {
    use super::*;
    use crate::effect::io::{Read, Write};
    use crate::effect::markers::Entity;
    use ::sqlx::error::{DatabaseError, ErrorKind};
    use ::sqlx::{PgConnection, Postgres};
    use std::borrow::Cow;
    use std::error::Error as StdError;
    use std::fmt;

    struct Account;
    impl Entity for Account {}

    /// A query that returns a canned result and never touches the connection.
    struct Canned<X>(i64, std::marker::PhantomData<fn() -> X>);

    impl<X: Effect> Query for Canned<X> {
        type Effect = X;
    }

    impl<X: Effect> Execute<SqlxSource<Postgres>> for Canned<X> {
        type Output = i64;
        async fn execute(self, _: &mut PgConnection) -> Result<i64, ::sqlx::Error> {
            Ok(self.0)
        }
    }

    /// Reads a balance, then commits it if it is positive, else rolls back.
    /// The report records every result the flow saw.
    enum Check<X> {
        Init(std::marker::PhantomData<fn() -> X>),
        Reading(Vec<String>),
    }

    impl<X> Check<X> {
        const fn init() -> Self {
            Check::Init(std::marker::PhantomData)
        }
    }

    impl<X: Effect + 'static> TxFlow for Check<X> {
        type Database = Postgres;
        type Query = Canned<X>;
        type Commit = i64;
        type Abort = String;
        type Report = Vec<String>;

        const ISOLATION: Option<IsolationLevel> = Some(IsolationLevel::Serializable);

        fn start(self) -> Step<Self> {
            Step::Run { next: Check::Reading(Vec::new()), query: Canned(0, Default::default()) }
        }

        fn resume(self, output: Result<i64, ::sqlx::Error>) -> Step<Self> {
            let Check::Reading(mut seen) = self else {
                return Step::Rollback { output: "bad state".into(), report: Vec::new() };
            };
            match output {
                Ok(balance) if balance > 0 => {
                    seen.push(format!("ok {balance}"));
                    Step::Commit { output: balance, report: seen }
                }
                Ok(balance) => {
                    seen.push(format!("ok {balance}"));
                    Step::Rollback { output: "not positive".into(), report: seen }
                }
                Err(error) => {
                    seen.push(format!("err {error}"));
                    Step::Rollback { output: "client error".into(), report: seen }
                }
            }
        }

        fn aborted(self, error: ::sqlx::Error) -> (String, Vec<String>) {
            (format!("aborted: {error}"), vec!["aborted".into()])
        }
    }

    type ReadCheck = Check<Read<Account>>;
    type WriteCheck = Check<(Read<Account>, Write<Account>)>;

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

    fn running<F: TxFlow>(flow: F) -> Running<F> {
        match Begin::new(flow).begun() {
            TxMachine::Run(running, _) => running,
            _ => unreachable!("flow starts with a query"),
        }
    }

    #[test]
    fn mode_is_read_only_iff_effect_only_reads() {
        let read = Begin::new(ReadCheck::init()).mode();
        let write = Begin::new(WriteCheck::init()).mode();
        assert_eq!(read, TxMode { isolation: Some(IsolationLevel::Serializable), read_only: true });
        assert!(!write.read_only);
    }

    #[test]
    fn failed_begin_never_runs_the_flow() {
        let done = Begin::new(ReadCheck::init()).failed(io_error());
        assert!(matches!(done.output, Err(TxError::Begin(_))));
        assert!(done.report.is_none());
    }

    #[test]
    fn commit_path() {
        let TxMachine::Commit(committing) = running(WriteCheck::init()).ran(Ok(5)) else {
            unreachable!("positive balance commits")
        };
        let done = committing.committed(Ok(()));
        assert!(matches!(done.output, Ok(TxOutput::Committed(5))));
        assert_eq!(done.report, Some(TxFinalState::Committed(vec!["ok 5".into()])));
    }

    #[test]
    fn flow_rollback_path() {
        let TxMachine::Rollback(rolling_back) = running(WriteCheck::init()).ran(Ok(0)) else {
            unreachable!("zero balance rolls back")
        };
        let done = rolling_back.rolled_back();
        assert!(matches!(&done.output, Ok(TxOutput::RolledBack(why)) if why == "not positive"));
        assert_eq!(done.report, Some(TxFinalState::RolledBack(vec!["ok 0".into()])));
    }

    #[test]
    fn client_side_error_resumes_the_flow() {
        let TxMachine::Rollback(rolling_back) =
            running(WriteCheck::init()).ran(Err(::sqlx::Error::RowNotFound))
        else {
            unreachable!("the flow rolls back on errors it sees")
        };
        let done = rolling_back.rolled_back();
        assert!(matches!(&done.output, Ok(TxOutput::RolledBack(why)) if why == "client error"));
    }

    #[test]
    fn aborting_error_skips_resume_and_rolls_back() {
        for error in [::sqlx::Error::Database(Box::new(ServerError)), io_error()] {
            let TxMachine::Rollback(rolling_back) = running(WriteCheck::init()).ran(Err(error))
            else {
                unreachable!("an aborted transaction must roll back")
            };
            let done = rolling_back.rolled_back();
            assert!(
                matches!(&done.output, Ok(TxOutput::RolledBack(why)) if why.starts_with("aborted"))
            );
            assert_eq!(done.report, Some(TxFinalState::RolledBack(vec!["aborted".into()])));
        }
    }

    #[test]
    fn rejected_commit_is_rolled_back_and_lost_commit_is_unknown() {
        let commit = || match running(WriteCheck::init()).ran(Ok(5)) {
            TxMachine::Commit(committing) => committing,
            _ => unreachable!("positive balance commits"),
        };

        let rejected = commit().committed(Err(::sqlx::Error::Database(Box::new(ServerError))));
        assert!(matches!(rejected.output, Err(TxError::CommitRejected(_))));
        assert!(matches!(rejected.report, Some(TxFinalState::RolledBack(_))));

        let lost = commit().committed(Err(io_error()));
        assert!(matches!(lost.output, Err(TxError::CommitUnknown(_))));
        assert!(matches!(lost.report, Some(TxFinalState::CommitUnknown(_))));
    }
}
