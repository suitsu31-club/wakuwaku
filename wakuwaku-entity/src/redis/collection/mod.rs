//! Typed data structures stored in Redis.
//!
//! Each collection is a small `Copy` value, usually a `const`, generic over
//! the [`Name`] of its items and, for caches, the body type. Its methods
//! build [queries](crate::Query) to run through a [`Db`](crate::Db):
//!
//! - [`Cache`](cache::Cache): bodies with a TTL.
//! - [`HashedCache`](cache::HashedCache): bodies with a TTL and their
//!   BLAKE3 hash, which can be compared without downloading the body.
//! - [`Lock`](lock::Lock): a mutual exclusion lock.
//! - [`RwLock`](lock::RwLock): a reader-writer lock.
//! - [`TokenBucket`](token_bucket_limiter::TokenBucket): a token bucket rate
//!   limiter.
//!
//! A collection type is also the [`Entity`](crate::Entity) its queries read
//! and write, so a capability may grant access to one collection only:
//!
//! ```
//! use wakuwaku_entity::redis::collection::cache::Cache;
//! use wakuwaku_entity::{CanRead, CanWrite};
//!
//! struct SessionToken(String);
//! struct Session {
//!     user: u64,
//! }
//!
//! /// Reads every cache, writes only the session cache.
//! enum Gateway {}
//! impl<N: ?Sized, B> CanRead<Cache<N, B>> for Gateway {}
//! impl CanWrite<Cache<SessionToken, Session>> for Gateway {}
//! ```
//!
//! # Keys
//!
//! An item lives under `@@wakuwaku-entity:<collection>:<namespace>:<name>`.
//! The namespace separates collections of the same type. Two collections of
//! the same kind with the same namespace share their keys, so give each one
//! its own namespace.
//!
//! Locks and the rate limiter read the server clock with `TIME` inside Lua
//! scripts, which needs effects replication of scripts (the default since
//! Redis 5).

/// Lua that sets `now` to the server time in milliseconds.
macro_rules! lua_now {
    () => {
        "local time = redis.call('TIME')\n\
         local now = tonumber(time[1]) * 1000 + math.floor(tonumber(time[2]) / 1000)\n"
    };
}

pub mod cache;
pub mod lock;
pub mod token_bucket_limiter;

use kanau::message::{DeserializeError, SerializeError};
use redis::{ErrorKind, RedisError};
use std::io::Write as _;
use std::time::Duration;

/// Prefix of every key written by a collection.
pub const COLLECTION_PREFIX: &str = "@@wakuwaku-entity:";

/// The name of an item in a collection: the last segment of its key.
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

/// Key of `name` in the collection `kind` / `namespace`.
fn item_key(kind: &str, namespace: &str, name: &(impl Name + ?Sized)) -> Vec<u8> {
    let mut key = Vec::with_capacity(64);
    key.extend_from_slice(COLLECTION_PREFIX.as_bytes());
    key.extend_from_slice(kind.as_bytes());
    key.push(b':');
    key.extend_from_slice(namespace.as_bytes());
    key.push(b':');
    name.write_name(&mut key);
    key
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
        "failed to serialize a collection body",
        error.into().to_string(),
    ))
}

fn deserialize_error(error: impl Into<DeserializeError>) -> RedisError {
    RedisError::from((
        ErrorKind::Parse,
        "failed to deserialize a collection body",
        error.into().to_string(),
    ))
}

/// An error for a reply that doesn't match what the collection wrote.
fn corrupted(detail: &'static str) -> RedisError {
    RedisError::from((
        ErrorKind::UnexpectedReturnType,
        "corrupted collection item",
        detail.to_owned(),
    ))
}

/// A random token identifying the holder of a lock.
fn lock_token() -> Result<[u8; 16], RedisError> {
    let mut token = [0; 16];
    getrandom::fill(&mut token).map_err(|error| {
        RedisError::from((
            ErrorKind::Client,
            "failed to generate a lock token",
            error.to_string(),
        ))
    })?;
    Ok(token)
}
