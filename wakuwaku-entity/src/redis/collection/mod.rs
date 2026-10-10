//! Typed data structures stored in Redis.
//!
//! Each collection is a small `Copy` value, usually a `const`, generic over
//! the [`Name`](super::Name) of its items and, for caches, the body type. Its
//! methods build [queries](crate::Query) to run through a [`Db`](crate::Db):
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
//! Items live under the [keys](super#keys) shared by every Redis structure.
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

use redis::{ErrorKind, RedisError};

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
