//! Caches with a TTL.
//!
//! - [`Cache`] stores each body under one string key.
//! - [`HashedCache`] also stores the BLAKE3 hash of each body, so a reader
//!   that already holds a copy can check it without downloading the body.
//!
//! Bodies are any [`MessageSer`] / [`MessageDe`] type.
//!
//! ```no_run
//! use std::time::Duration;
//! use kanau::message::{DeserializeError, MessageDe, MessageSer, SerializeError};
//! use kanau::processor::Processor;
//! use wakuwaku_entity::redis::RedisSource;
//! use wakuwaku_entity::redis::collection::cache::Cache;
//! use wakuwaku_entity::{Db, ReadWrite};
//!
//! struct Profile(String);
//! # impl MessageSer for Profile {
//! #     type SerError = SerializeError;
//! #     fn to_bytes(self) -> Result<Box<[u8]>, SerializeError> { Ok(self.0.into_bytes().into()) }
//! # }
//! # impl MessageDe for Profile {
//! #     type DeError = DeserializeError;
//! #     fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError> {
//! #         Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
//! #     }
//! # }
//!
//! const PROFILES: Cache<u64, Profile> = Cache::new("profile", Duration::from_secs(300));
//!
//! # async fn run(db: Db<RedisSource, ReadWrite>) -> redis::RedisResult<()> {
//! db.process(PROFILES.put(&7, Profile("Haruki".into()))).await?;
//! let profile: Option<Profile> = db.process(PROFILES.get(&7)).await?;
//! # Ok(())
//! # }
//! ```

use super::{Name, corrupted, deserialize_error, item_key, millis, serialize_error};
use crate::effect::io::{Read, Write};
use crate::effect::markers::Entity;
use crate::query::{Execute, Query};
use crate::redis::RedisSource;
pub use blake3::Hash;
use kanau::message::{MessageDe, MessageSer};
use redis::aio::ConnectionLike;
use redis::{RedisResult, Script};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::time::Duration;

/// Names `N` and bodies `B` of a cache, without owning either.
type Items<N, B> = PhantomData<fn(&N) -> B>;

macro_rules! cache_collection {
    ($cache:ident, $kind:literal) => {
        impl<N: ?Sized, B> $cache<N, B> {
            /// The cache in `namespace`, whose bodies expire `ttl` after
            /// they are put. A TTL under a millisecond counts as one.
            pub const fn new(namespace: &'static str, ttl: Duration) -> Self {
                Self {
                    namespace,
                    ttl,
                    items: PhantomData,
                }
            }

            /// Namespace of the cache.
            pub const fn namespace(&self) -> &'static str {
                self.namespace
            }

            /// How long a body lives after it is put.
            pub const fn ttl(&self) -> Duration {
                self.ttl
            }

            fn key(&self, name: &N) -> Vec<u8>
            where
                N: Name,
            {
                item_key($kind, self.namespace, name)
            }
        }

        impl<N: ?Sized, B> Clone for $cache<N, B> {
            fn clone(&self) -> Self {
                *self
            }
        }

        impl<N: ?Sized, B> Copy for $cache<N, B> {}

        impl<N: ?Sized, B> Debug for $cache<N, B> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($cache))
                    .field("namespace", &self.namespace)
                    .field("ttl", &self.ttl)
                    .finish()
            }
        }

        impl<N: ?Sized, B> Entity for $cache<N, B> {}
    };
}

/// Bodies `B` named by `N`, each expiring a fixed TTL after it is put.
pub struct Cache<N: ?Sized, B> {
    namespace: &'static str,
    ttl: Duration,
    items: Items<N, B>,
}

cache_collection!(Cache, "Cache");

impl<N: Name + ?Sized, B> Cache<N, B> {
    /// Read the body of `name`, `None` if it is absent or expired.
    pub fn get(&self, name: &N) -> CacheGet<N, B> {
        CacheGet {
            key: self.key(name),
            items: PhantomData,
        }
    }

    /// Store `body` as `name`, replacing any previous body and resetting
    /// the TTL.
    pub fn put(&self, name: &N, body: B) -> CachePut<N, B> {
        CachePut {
            key: self.key(name),
            ttl: self.ttl,
            body,
            items: PhantomData,
        }
    }

    /// Delete `name`. Outputs whether it was present.
    pub fn remove(&self, name: &N) -> CacheRemove<N, B> {
        CacheRemove {
            key: self.key(name),
            items: PhantomData,
        }
    }
}

/// Query from [`Cache::get`].
pub struct CacheGet<N: ?Sized, B> {
    key: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for CacheGet<N, B> {
    type Effect = Read<Cache<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for CacheGet<N, B>
where
    N: ?Sized,
    B: MessageDe + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<B>;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<B>> {
        let bytes: Option<Vec<u8>> = redis::cmd("GET").arg(&self.key).query_async(conn).await?;
        bytes
            .map(|bytes| B::from_bytes(&bytes).map_err(deserialize_error))
            .transpose()
    }
}

/// Query from [`Cache::put`].
pub struct CachePut<N: ?Sized, B> {
    key: Vec<u8>,
    ttl: Duration,
    body: B,
    items: Items<N, B>,
}

impl<N: ?Sized, B: Send> Query for CachePut<N, B> {
    type Effect = Write<Cache<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for CachePut<N, B>
where
    N: ?Sized,
    B: MessageSer + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = ();

    async fn execute(self, conn: &mut C) -> RedisResult<()> {
        let body = self.body.to_bytes().map_err(serialize_error)?;
        redis::cmd("SET")
            .arg(&self.key)
            .arg(&*body)
            .arg("PX")
            .arg(millis(self.ttl))
            .exec_async(conn)
            .await
    }
}

/// Query from [`Cache::remove`].
pub struct CacheRemove<N: ?Sized, B> {
    key: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for CacheRemove<N, B> {
    type Effect = Write<Cache<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for CacheRemove<N, B>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        unlink(&self.key, conn).await
    }
}

/// Like [`Cache`], but each item also holds the BLAKE3 hash of its
/// serialized body.
///
/// An item is a Redis hash with the fields `body` and `hash`, written
/// together. [`hash`](Self::hash) and [`get_if_changed`](Self::get_if_changed)
/// compare a copy against the stored body by transferring the 32-byte hash
/// only.
pub struct HashedCache<N: ?Sized, B> {
    namespace: &'static str,
    ttl: Duration,
    items: Items<N, B>,
}

cache_collection!(HashedCache, "HashedCache");

const BODY_FIELD: &str = "body";
const HASH_FIELD: &str = "hash";

/// Returns `{0}` if the item is absent, `{1}` if its hash is `ARGV[1]`,
/// `{2, hash, body}` otherwise.
static GET_IF_CHANGED: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local hash = redis.call('HGET', KEYS[1], 'hash')
if not hash then return {0} end
if hash == ARGV[1] then return {1} end
return {2, hash, redis.call('HGET', KEYS[1], 'body')}
",
    )
});

impl<N: Name + ?Sized, B> HashedCache<N, B> {
    /// Read the body of `name` and its hash, `None` if it is absent or
    /// expired.
    pub fn get(&self, name: &N) -> HashedCacheGet<N, B> {
        HashedCacheGet {
            key: self.key(name),
            items: PhantomData,
        }
    }

    /// Read only the hash of `name`'s body.
    pub fn hash(&self, name: &N) -> HashedCacheHash<N, B> {
        HashedCacheHash {
            key: self.key(name),
            items: PhantomData,
        }
    }

    /// Read the body of `name` unless its hash is `known`.
    pub fn get_if_changed(&self, name: &N, known: Hash) -> HashedCacheGetIfChanged<N, B> {
        HashedCacheGetIfChanged {
            key: self.key(name),
            known,
            items: PhantomData,
        }
    }

    /// Store `body` and its hash as `name`, replacing any previous body and
    /// resetting the TTL. Outputs the hash.
    pub fn put(&self, name: &N, body: B) -> HashedCachePut<N, B> {
        HashedCachePut {
            key: self.key(name),
            ttl: self.ttl,
            body,
            items: PhantomData,
        }
    }

    /// Delete `name`. Outputs whether it was present.
    pub fn remove(&self, name: &N) -> HashedCacheRemove<N, B> {
        HashedCacheRemove {
            key: self.key(name),
            items: PhantomData,
        }
    }
}

/// A body read from a [`HashedCache`] with its hash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hashed<B> {
    /// The body.
    pub body: B,
    /// BLAKE3 hash of the serialized body.
    pub hash: Hash,
}

/// Output of [`HashedCache::get_if_changed`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Freshness<B> {
    /// No body is stored.
    Missing,
    /// The stored body has the known hash.
    Unchanged,
    /// The stored body has another hash.
    Changed(Hashed<B>),
}

fn parse_hash(bytes: &[u8]) -> RedisResult<Hash> {
    <[u8; 32]>::try_from(bytes)
        .map(Hash::from_bytes)
        .map_err(|_| corrupted("a BLAKE3 hash is not 32 bytes long"))
}

fn parse_hashed<B: MessageDe>(body: &[u8], hash: &[u8]) -> RedisResult<Hashed<B>> {
    Ok(Hashed {
        body: B::from_bytes(body).map_err(deserialize_error)?,
        hash: parse_hash(hash)?,
    })
}

/// Query from [`HashedCache::get`].
pub struct HashedCacheGet<N: ?Sized, B> {
    key: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for HashedCacheGet<N, B> {
    type Effect = Read<HashedCache<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for HashedCacheGet<N, B>
where
    N: ?Sized,
    B: MessageDe + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<Hashed<B>>;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<Hashed<B>>> {
        let fields: (Option<Vec<u8>>, Option<Vec<u8>>) = redis::cmd("HMGET")
            .arg(&self.key)
            .arg(BODY_FIELD)
            .arg(HASH_FIELD)
            .query_async(conn)
            .await?;
        match fields {
            (Some(body), Some(hash)) => parse_hashed(&body, &hash).map(Some),
            (None, None) => Ok(None),
            _ => Err(corrupted("a hashed cache item lacks its body or its hash")),
        }
    }
}

/// Query from [`HashedCache::hash`].
pub struct HashedCacheHash<N: ?Sized, B> {
    key: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for HashedCacheHash<N, B> {
    type Effect = Read<HashedCache<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for HashedCacheHash<N, B>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<Hash>;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<Hash>> {
        let hash: Option<Vec<u8>> = redis::cmd("HGET")
            .arg(&self.key)
            .arg(HASH_FIELD)
            .query_async(conn)
            .await?;
        hash.as_deref().map(parse_hash).transpose()
    }
}

/// Query from [`HashedCache::get_if_changed`].
pub struct HashedCacheGetIfChanged<N: ?Sized, B> {
    key: Vec<u8>,
    known: Hash,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for HashedCacheGetIfChanged<N, B> {
    type Effect = Read<HashedCache<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for HashedCacheGetIfChanged<N, B>
where
    N: ?Sized,
    B: MessageDe + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Freshness<B>;

    async fn execute(self, conn: &mut C) -> RedisResult<Freshness<B>> {
        let reply: Vec<redis::Value> = GET_IF_CHANGED
            .key(&self.key)
            .arg(self.known.as_bytes())
            .invoke_async(conn)
            .await?;
        match reply.as_slice() {
            [redis::Value::Int(0)] => Ok(Freshness::Missing),
            [redis::Value::Int(1)] => Ok(Freshness::Unchanged),
            [
                redis::Value::Int(2),
                redis::Value::BulkString(hash),
                redis::Value::BulkString(body),
            ] => parse_hashed(body, hash).map(Freshness::Changed),
            _ => Err(corrupted("a hashed cache item lacks its body or its hash")),
        }
    }
}

/// Query from [`HashedCache::put`].
pub struct HashedCachePut<N: ?Sized, B> {
    key: Vec<u8>,
    ttl: Duration,
    body: B,
    items: Items<N, B>,
}

impl<N: ?Sized, B: Send> Query for HashedCachePut<N, B> {
    type Effect = Write<HashedCache<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for HashedCachePut<N, B>
where
    N: ?Sized,
    B: MessageSer + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Hash;

    async fn execute(self, conn: &mut C) -> RedisResult<Hash> {
        let body = self.body.to_bytes().map_err(serialize_error)?;
        let hash = blake3::hash(&body);
        redis::pipe()
            .atomic()
            .cmd("HSET")
            .arg(&self.key)
            .arg(BODY_FIELD)
            .arg(&*body)
            .arg(HASH_FIELD)
            .arg(hash.as_bytes())
            .ignore()
            .cmd("PEXPIRE")
            .arg(&self.key)
            .arg(millis(self.ttl))
            .ignore()
            .exec_async(conn)
            .await?;
        Ok(hash)
    }
}

/// Query from [`HashedCache::remove`].
pub struct HashedCacheRemove<N: ?Sized, B> {
    key: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for HashedCacheRemove<N, B> {
    type Effect = Write<HashedCache<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for HashedCacheRemove<N, B>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        unlink(&self.key, conn).await
    }
}

async fn unlink(key: &[u8], conn: &mut (impl ConnectionLike + Send)) -> RedisResult<bool> {
    let removed: u64 = redis::cmd("UNLINK").arg(key).query_async(conn).await?;
    Ok(removed > 0)
}
