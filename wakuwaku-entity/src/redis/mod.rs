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
//!
//! On top of it:
//!
//! - [`collection`]: typed data structures queried on demand (caches,
//!   locks, a rate limiter).
//! - [`dynamic`]: typed data structures that can also be followed as they
//!   change (a pipe, live views, stream buffers).
//!
//! # Keys
//!
//! An item lives under `@@wakuwaku-entity:<kind>:<namespace>:<name>`, where
//! `<kind>` is the type of the structure. An item made of several keys
//! wraps `<namespace>:<name>` in braces and appends a suffix to each key, so
//! all of its keys share one cluster hash slot. The namespace separates
//! structures of the same type: two structures of the same kind with the
//! same namespace share their keys, so give each one its own namespace.

pub mod collection;
pub mod dynamic;

use crate::query::{DataSource, Execute};
use kanau::message::{DeserializeError, SerializeError};
use redis::aio::{ConnectionLike, MultiplexedConnection};
use redis::{ErrorKind, RedisError};
use std::io::Write as _;
use std::time::Duration;

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

/// Prefix of every key written by a [`collection`] or a [`dynamic`]
/// structure.
pub const KEY_PREFIX: &str = "@@wakuwaku-entity:";

/// The name of an item in a structure: the last segment of its key.
///
/// Implemented for strings, byte strings and integers. Implement it for
/// identifier newtypes by writing their canonical representation.
pub trait Name {
    /// Append the name to `key`.
    fn write_name(&self, key: &mut Vec<u8>);
}

impl<T: Name + ?Sized> Name for &T {
    fn write_name(&self, key: &mut Vec<u8>) {
        (**self).write_name(key);
    }
}

impl Name for str {
    fn write_name(&self, key: &mut Vec<u8>) {
        key.extend_from_slice(self.as_bytes());
    }
}

impl Name for String {
    fn write_name(&self, key: &mut Vec<u8>) {
        key.extend_from_slice(self.as_bytes());
    }
}

impl Name for [u8] {
    fn write_name(&self, key: &mut Vec<u8>) {
        key.extend_from_slice(self);
    }
}

impl Name for Vec<u8> {
    fn write_name(&self, key: &mut Vec<u8>) {
        key.extend_from_slice(self);
    }
}

macro_rules! integer_names {
    ($($t:ty),*) => {$(
        impl Name for $t {
            fn write_name(&self, key: &mut Vec<u8>) {
                // Writing to a `Vec` never fails.
                let _ = write!(key, "{self}");
            }
        }
    )*};
}

integer_names!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize
);

/// Key of `name` in the structure `kind` / `namespace`.
fn item_key(kind: &str, namespace: &str, name: &(impl Name + ?Sized)) -> Vec<u8> {
    let mut key = Vec::with_capacity(64);
    key.extend_from_slice(KEY_PREFIX.as_bytes());
    key.extend_from_slice(kind.as_bytes());
    key.push(b':');
    key.extend_from_slice(namespace.as_bytes());
    key.push(b':');
    name.write_name(&mut key);
    key
}

/// Common prefix of the keys of `name` in the structure `kind` /
/// `namespace`, when the item spans several keys. Append a distinct suffix
/// for each key.
///
/// The braces make `namespace:name` the hash tag of every key. The keys
/// share everything up to the end of the name, so the first `}` after the
/// `{` falls at the same place in all of them, and the tags are equal.
fn tagged_key(kind: &str, namespace: &str, name: &(impl Name + ?Sized)) -> Vec<u8> {
    let mut key = Vec::with_capacity(64);
    key.extend_from_slice(KEY_PREFIX.as_bytes());
    key.extend_from_slice(kind.as_bytes());
    key.extend_from_slice(b":{");
    key.extend_from_slice(namespace.as_bytes());
    key.push(b':');
    name.write_name(&mut key);
    key.push(b'}');
    key
}

/// Two keys of `name` in the structure `kind` / `namespace`, in one cluster
/// hash slot: the [tagged key](tagged_key) followed by `first` and `second`.
fn tagged_pair(
    kind: &str,
    namespace: &str,
    name: &(impl Name + ?Sized),
    first: &[u8],
    second: &[u8],
) -> (Vec<u8>, Vec<u8>) {
    let mut one = tagged_key(kind, namespace, name);
    let mut two = one.clone();
    one.extend_from_slice(first);
    two.extend_from_slice(second);
    (one, two)
}

/// `duration` in whole milliseconds, at least 1 so Redis accepts it as an
/// expiry.
fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}

fn serialize_error(error: impl Into<SerializeError>) -> RedisError {
    RedisError::from((
        ErrorKind::Client,
        "failed to serialize a stored body",
        error.into().to_string(),
    ))
}

fn deserialize_error(error: impl Into<DeserializeError>) -> RedisError {
    RedisError::from((
        ErrorKind::Parse,
        "failed to deserialize a stored body",
        error.into().to_string(),
    ))
}

/// An error for a reply that doesn't match what the structure wrote.
fn corrupted(detail: &'static str) -> RedisError {
    RedisError::from((
        ErrorKind::UnexpectedReturnType,
        "corrupted item",
        detail.to_owned(),
    ))
}
