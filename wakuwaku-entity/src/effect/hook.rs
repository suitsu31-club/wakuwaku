//! Processors that run before or after write queries.
//!
//! A write query describes itself as a [`WriteQuery::Change`], built from the
//! query alone. Many queries may share one change type (`UserChanged`), so
//! one hook covers all of them. A hook is any [`Processor`] of that change:
//!
//! - [`BeforeWrite`] runs the hook, then the query.
//! - [`AfterWrite`] runs the query, then the hook if the query succeeded.
//!
//! Both wrap any processor of the query (a [`Db`](crate::query::Db) or
//! another hook) and forward queries of [`ReadKind`] untouched. They nest:
//! `AfterWrite<BeforeWrite<Db<_, _>, _, _>, _, _>`.
//!
//! # Hook errors
//!
//! A hook never changes the result of the query. Its error goes to an error
//! handler, a processor of the hook's error whose own error is
//! [`Infallible`]: it must deal with the failure itself (log it, retry, park
//! it in an outbox, …). The wrapped handle therefore stays a drop-in
//! replacement: same output, same error as the processor it wraps.
//!
//! These hooks are for queries run on their own. Queries inside a
//! transaction run on the transaction's connection and don't pass through
//! them; hook the transaction's final state instead.

use crate::effect::io::{Effect, Kind, ReadKind, WriteKind};
use crate::query::Query;
use kanau::processor::{Processor, ProcessorReturn};
use std::convert::Infallible;

/// A query whose effect writes, described for hooks.
pub trait WriteQuery: Query {
    /// What the query changes.
    type Change: Send;

    /// Describe the change, before the query runs.
    fn change(&self) -> Self::Change;
}

/// Runs `hook` before each write query, then the query.
///
/// The query runs even if the hook fails; the failure goes to `on_error`.
#[derive(Debug, Clone)]
pub struct BeforeWrite<D, H, EH> {
    inner: D,
    hook: H,
    on_error: EH,
}

/// Runs each write query, then `hook` if the query succeeded.
///
/// A hook failure goes to `on_error`; the caller still gets the query's
/// output.
#[derive(Debug, Clone)]
pub struct AfterWrite<D, H, EH> {
    inner: D,
    hook: H,
    on_error: EH,
}

macro_rules! hook_wrapper {
    ($name:ident, $dispatch:ident) => {
        impl<D, H, EH> $name<D, H, EH> {
            /// Wrap `inner`, sending hook errors to `on_error`.
            pub const fn new(inner: D, hook: H, on_error: EH) -> Self {
                Self { inner, hook, on_error }
            }

            /// The wrapped processor.
            pub const fn inner(&self) -> &D {
                &self.inner
            }
        }

        impl<Q, D, H, EH> Processor<Q> for $name<D, H, EH>
        where
            Q: Query,
            D: Processor<Q>,
            <Q::Effect as Effect>::Kind: HookDispatch<Q, D, H, EH>,
        {
            type Output = D::Output;
            type Error = D::Error;

            fn process(
                &self,
                query: Q,
            ) -> impl Future<Output = ProcessorReturn<D, Q>> + Send {
                <<Q::Effect as Effect>::Kind as HookDispatch<Q, D, H, EH>>::$dispatch(
                    &self.inner,
                    &self.hook,
                    &self.on_error,
                    query,
                )
            }
        }
    };
}

hook_wrapper!(BeforeWrite, before);
hook_wrapper!(AfterWrite, after);

/// Chooses what [`BeforeWrite`] and [`AfterWrite`] do with a query, by the
/// [`Kind`] of its effect.
///
/// Implemented for [`ReadKind`] (forward the query) and [`WriteKind`] (run
/// the hook). It is public only because it appears in their bounds.
pub trait HookDispatch<Q: Send, D: Processor<Q>, H, EH>: Kind {
    /// Run `hook` on the change of `query`, then `query`.
    fn before(
        inner: &D,
        hook: &H,
        on_error: &EH,
        query: Q,
    ) -> impl Future<Output = ProcessorReturn<D, Q>> + Send;

    /// Run `query`, then `hook` on its change if it succeeded.
    fn after(
        inner: &D,
        hook: &H,
        on_error: &EH,
        query: Q,
    ) -> impl Future<Output = ProcessorReturn<D, Q>> + Send;
}

impl<Q, D, H, EH> HookDispatch<Q, D, H, EH> for ReadKind
where
    Q: Send,
    D: Processor<Q> + Sync,
{
    fn before(
        inner: &D,
        _: &H,
        _: &EH,
        query: Q,
    ) -> impl Future<Output = ProcessorReturn<D, Q>> + Send {
        inner.process(query)
    }

    fn after(
        inner: &D,
        _: &H,
        _: &EH,
        query: Q,
    ) -> impl Future<Output = ProcessorReturn<D, Q>> + Send {
        inner.process(query)
    }
}

impl<Q, D, H, EH> HookDispatch<Q, D, H, EH> for WriteKind
where
    Q: WriteQuery,
    D: Processor<Q> + Sync,
    D::Output: Send,
    H: Processor<Q::Change> + Sync,
    H::Error: Send,
    EH: Processor<H::Error, Error = Infallible> + Sync,
{
    async fn before(inner: &D, hook: &H, on_error: &EH, query: Q) -> ProcessorReturn<D, Q> {
        run_hook(hook, on_error, query.change()).await;
        inner.process(query).await
    }

    async fn after(inner: &D, hook: &H, on_error: &EH, query: Q) -> ProcessorReturn<D, Q> {
        let change = query.change();
        let output = inner.process(query).await?;
        run_hook(hook, on_error, change).await;
        Ok(output)
    }
}

/// Run `hook` on `input`, and `on_error` on its error.
pub(crate) async fn run_hook<I, H, EH>(hook: &H, on_error: &EH, input: I)
where
    I: Send,
    H: Processor<I>,
    H::Error: Send,
    EH: Processor<H::Error, Error = Infallible>,
{
    if let Some(error) = hook.process(input).await.err() {
        let Ok(_) = on_error.process(error).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effect::io::{Read, Write};
    use crate::effect::markers::Entity;
    use std::sync::Mutex;

    struct User;
    impl Entity for User {}

    struct FindUser;
    impl Query for FindUser {
        type Effect = Read<User>;
    }

    struct RenameUser {
        fail: bool,
    }
    impl Query for RenameUser {
        type Effect = (Read<User>, Write<User>);
    }
    impl WriteQuery for RenameUser {
        type Change = &'static str;
        fn change(&self) -> &'static str {
            "renamed"
        }
    }

    /// Records every call in order, so tests can check what ran when.
    #[derive(Default)]
    struct Log(Mutex<Vec<&'static str>>);

    impl Log {
        fn push(&self, entry: &'static str) {
            if let Ok(mut log) = self.0.lock() {
                log.push(entry);
            }
        }
        fn take(&self) -> Vec<&'static str> {
            self.0.lock().map(|mut log| std::mem::take(&mut *log)).unwrap_or_default()
        }
    }

    struct Store<'a>(&'a Log);

    impl Processor<FindUser> for Store<'_> {
        type Output = ();
        type Error = &'static str;
        async fn process(&self, _: FindUser) -> Result<(), &'static str> {
            self.0.push("find");
            Ok(())
        }
    }

    impl Processor<RenameUser> for Store<'_> {
        type Output = ();
        type Error = &'static str;
        async fn process(&self, query: RenameUser) -> Result<(), &'static str> {
            self.0.push("rename");
            if query.fail { Err("query failed") } else { Ok(()) }
        }
    }

    struct Hook<'a> {
        log: &'a Log,
        fail: bool,
    }

    impl Processor<&'static str> for Hook<'_> {
        type Output = ();
        type Error = &'static str;
        async fn process(&self, change: &'static str) -> Result<(), &'static str> {
            self.log.push(change);
            if self.fail { Err("hook failed") } else { Ok(()) }
        }
    }

    struct OnError<'a>(&'a Log);

    impl Processor<&'static str> for OnError<'_> {
        type Output = ();
        type Error = Infallible;
        async fn process(&self, error: &'static str) -> Result<(), Infallible> {
            self.0.push(error);
            Ok(())
        }
    }

    #[tokio::test]
    async fn before_runs_hook_first_and_query_despite_hook_error() {
        let log = Log::default();
        let db = BeforeWrite::new(Store(&log), Hook { log: &log, fail: true }, OnError(&log));

        assert_eq!(db.process(RenameUser { fail: false }).await, Ok(()));
        assert_eq!(log.take(), ["renamed", "hook failed", "rename"]);
    }

    #[tokio::test]
    async fn after_runs_hook_only_on_success_and_keeps_query_result() {
        let log = Log::default();
        let db = AfterWrite::new(Store(&log), Hook { log: &log, fail: true }, OnError(&log));

        assert_eq!(db.process(RenameUser { fail: false }).await, Ok(()));
        assert_eq!(log.take(), ["rename", "renamed", "hook failed"]);

        assert_eq!(db.process(RenameUser { fail: true }).await, Err("query failed"));
        assert_eq!(log.take(), ["rename"]);
    }

    #[tokio::test]
    async fn reads_skip_hooks() {
        let log = Log::default();
        let hook = Hook { log: &log, fail: false };
        let db = AfterWrite::new(BeforeWrite::new(Store(&log), hook, OnError(&log)), Hook {
            log: &log,
            fail: false,
        }, OnError(&log));

        assert_eq!(db.process(FindUser).await, Ok(()));
        assert_eq!(log.take(), ["find"]);
    }
}
