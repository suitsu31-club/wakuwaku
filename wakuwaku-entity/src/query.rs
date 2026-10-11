//! Queries, data sources, and the [`Db`] handle that runs one on the other.
//!
//! A query is split in two traits:
//!
//! - [`Query`] says what it touches. It doesn't depend on where it runs, so
//!   hooks and capability checks work the same for every data source.
//! - [`Execute<S>`] says how it runs on the data source `S`. A query written
//!   in portable SQL may implement it for several sources.
//!
//! A [`Db<S, C>`] owns a data source `S` and a capability `C`. It implements
//! [`Processor<Q>`] for every query `Q` that runs on `S` and whose effect `C`
//! [covers](Covers):
//!
//! ```compile_fail
//! # use wakuwaku_entity::{DataSource, Db, Entity, Execute, Query, ReadOnly, Write};
//! # use kanau::processor::Processor;
//! # struct Post;
//! # impl Entity for Post {}
//! # struct Memory;
//! # impl DataSource for Memory {
//! #     type Connection = ();
//! #     type Error = std::convert::Infallible;
//! #     async fn run<Q: Execute<Self>>(&self, query: Q) -> Result<Q::Output, Self::Error> {
//! #         query.execute(&mut ()).await
//! #     }
//! # }
//! struct DeletePost(u64);
//!
//! impl Query for DeletePost {
//!     type Effect = Write<Post>;
//! }
//! # impl Execute<Memory> for DeletePost {
//! #     type Output = ();
//! #     async fn execute(self, _: &mut ()) -> Result<(), std::convert::Infallible> { Ok(()) }
//! # }
//!
//! async fn on_replica(db: &Db<Memory, ReadOnly>) {
//!     // error: `ReadOnly: CanWrite<Post>` is not satisfied
//!     db.process(DeletePost(1)).await;
//! }
//! ```
//!
//! Handlers should stay generic over the handle, listing the queries they
//! need (`D: Processor<FindUser> + Processor<DeletePost>`). The capability is
//! then checked once, where the handle is wired in.

use crate::effect::io::{Covers, Effect};
use kanau::processor::Processor;
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;

/// What a query touches.
pub trait Query: Send + Sized {
    /// Entities the query reads and writes. See [`effect::io`](crate::effect::io).
    type Effect: Effect;
}

/// Something that can run queries: a connection pool, a multiplexed client, ….
pub trait DataSource: Send + Sync + Sized {
    /// The connection a query runs on.
    type Connection: Send;
    /// Error of the data source and of the queries that run on it.
    type Error: Send;

    /// Get a connection and run `query` on it.
    fn run<Q: Execute<Self>>(
        &self,
        query: Q,
    ) -> impl Future<Output = Result<Q::Output, Self::Error>> + Send;
}

/// How a query runs on the data source `S`.
pub trait Execute<S: DataSource>: Query {
    /// What the query returns.
    type Output: Send;

    /// Run the query on `conn`.
    ///
    /// For sqlx sources `conn` is also the connection of an open
    /// transaction, so the same query works inside and outside one.
    fn execute(
        self,
        conn: &mut S::Connection,
    ) -> impl Future<Output = Result<Self::Output, S::Error>> + Send;
}

/// A data source `S` usable with the capability `C`.
///
/// Implements [`Processor<Q>`] for every `Q: Execute<S>` with
/// `C: Covers<Q::Effect>`. The data source itself stays private, so the
/// capability can't be bypassed through it.
pub struct Db<S, C> {
    source: S,
    capability: PhantomData<fn() -> C>,
}

impl<S, C> Db<S, C> {
    /// Use `source` with the capability `C`.
    ///
    /// Only build a capability that writes from a source connected to a
    /// writable database.
    pub const fn new(source: S) -> Self {
        Self { source, capability: PhantomData }
    }

    #[allow(unused)]
    pub(crate) const fn source(&self) -> &S {
        &self.source
    }
}

impl<S: Clone, C> Clone for Db<S, C> {
    fn clone(&self) -> Self {
        Self::new(self.source.clone())
    }
}

impl<S: Debug, C> Debug for Db<S, C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db")
            .field("source", &self.source)
            .field("capability", &std::any::type_name::<C>())
            .finish()
    }
}

impl<S, C, Q> Processor<Q> for Db<S, C>
where
    S: DataSource,
    Q: Execute<S>,
    C: Covers<Q::Effect>,
{
    type Output = Q::Output;
    type Error = S::Error;

    fn process(&self, query: Q) -> impl Future<Output = Result<Q::Output, S::Error>> + Send {
        self.source.run(query)
    }
}
