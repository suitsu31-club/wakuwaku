//! Queries, data sources, and the [`Db`] handle that runs one on the other.
//!
//! - [`Query`]: what a query touches, its [`Effect`].
//! - [`Execute<S>`]: how it runs on the data source `S`, and how it fails.
//! - [`DataSource`]: something that runs queries.
//! - [`Db<S, C>`]: a data source with a capability `C`. It implements
//!   [`Processor<Q>`] for every query `Q` that runs on `S` and whose effect
//!   `C` [covers](Covers):
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
//! #     async fn run<Q: Execute<Self>>(&self, query: Q) -> Result<Q::Output, Q::Error> {
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
//! #     type Error = std::convert::Infallible;
//! #     async fn execute(self, _: &mut ()) -> Result<(), std::convert::Infallible> { Ok(()) }
//! # }
//!
//! async fn on_replica(db: &Db<Memory, ReadOnly>) {
//!     // error: `ReadOnly: CanWrite<Post>` is not satisfied
//!     db.process(DeletePost(1)).await;
//! }
//! ```
//!
//! See the [queries guide](https://www.wakuwaku.rs/en/docs/data/queries).

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
    /// Error of the data source itself. Queries that run on it fail with
    /// this error or [one of their own](Execute::Error) that wraps it.
    type Error: Send;

    /// Get a connection and run `query` on it.
    ///
    /// Failing to get the connection is an error of the query, through
    /// `From<Self::Error>`.
    fn run<Q: Execute<Self>>(
        &self,
        query: Q,
    ) -> impl Future<Output = Result<Q::Output, Q::Error>> + Send;
}

/// How a query runs on the data source `S`.
///
/// A query that can refuse for reasons of its own (a lost optimistic-lock
/// fence, a violated business rule, …) declares its own
/// [`Error`](Execute::Error) wrapping the source's:
///
/// ```
/// # use wakuwaku_entity::{DataSource, Entity, Execute, Query, Write};
/// # struct Post;
/// # impl Entity for Post {}
/// # struct Memory;
/// # #[derive(Debug)]
/// # struct MemoryError;
/// # impl DataSource for Memory {
/// #     type Connection = u64;
/// #     type Error = MemoryError;
/// #     async fn run<Q: Execute<Self>>(&self, query: Q) -> Result<Q::Output, Q::Error> {
/// #         query.execute(&mut 1).await
/// #     }
/// # }
/// #[derive(Debug)]
/// enum BumpError {
///     /// Someone else bumped the post first.
///     Conflict,
///     Source(MemoryError),
/// }
///
/// impl From<MemoryError> for BumpError {
///     fn from(error: MemoryError) -> Self {
///         Self::Source(error)
///     }
/// }
///
/// struct BumpPost { generation: u64 }
///
/// impl Query for BumpPost {
///     type Effect = Write<Post>;
/// }
///
/// impl Execute<Memory> for BumpPost {
///     type Output = u64;
///     type Error = BumpError;
///     async fn execute(self, current: &mut u64) -> Result<u64, BumpError> {
///         if *current != self.generation {
///             return Err(BumpError::Conflict);
///         }
///         *current += 1;
///         Ok(*current)
///     }
/// }
/// ```
pub trait Execute<S: DataSource>: Query {
    /// What the query returns.
    type Output: Send;
    /// How the query fails: `S::Error`, or an error of the query's own that
    /// wraps it.
    type Error: From<S::Error> + Send;

    /// Run the query on `conn`.
    ///
    /// For sqlx sources `conn` is also the connection of an open
    /// transaction, so the same query works inside and outside one.
    fn execute(
        self,
        conn: &mut S::Connection,
    ) -> impl Future<Output = Result<Self::Output, Self::Error>> + Send;
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
        Self {
            source,
            capability: PhantomData,
        }
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
    type Error = Q::Error;

    fn process(&self, query: Q) -> impl Future<Output = Result<Q::Output, Q::Error>> + Send {
        self.source.run(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effect::io::Write;
    use crate::effect::markers::{Entity, ReadWrite};

    struct Post;
    impl Entity for Post {}

    /// Holds one post generation; `down` fails every connection.
    struct Memory {
        generation: u64,
        down: bool,
    }

    #[derive(Debug, PartialEq)]
    struct Down;

    impl DataSource for Memory {
        type Connection = u64;
        type Error = Down;

        async fn run<Q: Execute<Self>>(&self, query: Q) -> Result<Q::Output, Q::Error> {
            if self.down {
                return Err(Down.into());
            }
            query.execute(&mut self.generation.clone()).await
        }
    }

    #[derive(Debug, PartialEq)]
    enum BumpError {
        Conflict,
        Source(Down),
    }

    impl From<Down> for BumpError {
        fn from(error: Down) -> Self {
            Self::Source(error)
        }
    }

    struct BumpPost {
        generation: u64,
    }

    impl Query for BumpPost {
        type Effect = Write<Post>;
    }

    impl Execute<Memory> for BumpPost {
        type Output = u64;
        type Error = BumpError;
        async fn execute(self, current: &mut u64) -> Result<u64, BumpError> {
            if *current != self.generation {
                return Err(BumpError::Conflict);
            }
            Ok(current.wrapping_add(1))
        }
    }

    #[tokio::test]
    async fn process_returns_the_query_error() {
        let up: Db<Memory, ReadWrite> = Db::new(Memory {
            generation: 3,
            down: false,
        });
        assert_eq!(up.process(BumpPost { generation: 3 }).await, Ok(4));
        assert_eq!(
            up.process(BumpPost { generation: 2 }).await,
            Err(BumpError::Conflict)
        );

        let down: Db<Memory, ReadWrite> = Db::new(Memory {
            generation: 3,
            down: true,
        });
        assert_eq!(
            down.process(BumpPost { generation: 3 }).await,
            Err(BumpError::Source(Down))
        );
    }
}
