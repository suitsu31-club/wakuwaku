//! Redis as a data source.
//!
//! [`RedisSource`] runs each query on a clone of an async connection. With
//! the default [`MultiplexedConnection`] (or a cluster or managed
//! connection), clones share one socket, so this is cheap.
//!
//! ```no_run
//! use kanau::processor::Processor;
//! use redis::AsyncCommands;
//! use redis::aio::ConnectionLike;
//! use wakuwaku_entity::redis::RedisSource;
//! use wakuwaku_entity::{Db, Entity, Execute, Query, ReadWrite, Write};
//!
//! struct Session;
//! impl Entity for Session {}
//!
//! struct StoreSession {
//!     token: String,
//!     user: u64,
//! }
//!
//! impl Query for StoreSession {
//!     type Effect = Write<Session>;
//! }
//!
//! impl<C: ConnectionLike + Clone + Send + Sync> Execute<RedisSource<C>> for StoreSession {
//!     type Output = ();
//!     async fn execute(self, conn: &mut C) -> redis::RedisResult<()> {
//!         conn.set_ex(format!("session:{}", self.token), self.user, 3600).await
//!     }
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let client = redis::Client::open("redis://127.0.0.1/")?;
//! let db: Db<RedisSource, ReadWrite> =
//!     Db::new(RedisSource::new(client.get_multiplexed_async_connection().await?));
//! db.process(StoreSession { token: "abc".into(), user: 7 }).await?;
//! # Ok(())
//! # }
//! ```

pub mod collection;
pub mod dynamic;

use crate::query::{DataSource, Execute};
use redis::RedisError;
use redis::aio::{ConnectionLike, MultiplexedConnection};

/// A cloneable async Redis connection as a [`DataSource`].
#[derive(Clone)]
pub struct RedisSource<C = MultiplexedConnection> {
    connection: C,
}

impl<C> RedisSource<C> {
    /// Run queries on clones of `connection`.
    pub const fn new(connection: C) -> Self {
        Self { connection }
    }
}

impl<C> DataSource for RedisSource<C>
where
    C: ConnectionLike + Clone + Send + Sync,
{
    type Connection = C;
    type Error = RedisError;

    async fn run<Q: Execute<Self>>(&self, query: Q) -> Result<Q::Output, RedisError> {
        let mut connection = self.connection.clone();
        query.execute(&mut connection).await
    }
}
