//! Locks with a lease.
//!
//! - [`Lock`]: at most one holder per name.
//! - [`RwLock`]: either one writer or any number of readers per name.
//!
//! Acquiring never waits: the query outputs a token if the lock was free
//! and `None` otherwise, and the caller decides how to retry. A holder
//! keeps the lock until it releases it or the lease (the lock's TTL)
//! runs out; extend the lease to hold it longer. A holder whose lease ran
//! out may have been overtaken, which [`LockToken::release`] and
//! [`LockToken::extend`] report by outputting `false`.
//!
//! The lease is what keeps a crashed holder from blocking everyone, so it
//! also bounds what the lock protects: work that may outlive the lease must
//! extend it, and a stalled process may resume after losing the lock.
//! Pass a fencing value to the protected resource when that matters.
//!
//! ```no_run
//! use std::time::Duration;
//! use kanau::processor::Processor;
//! use wakuwaku_entity::redis::RedisSource;
//! use wakuwaku_entity::redis::collection::lock::Lock;
//! use wakuwaku_entity::{Db, ReadWrite};
//!
//! const JOBS: Lock<str> = Lock::new("job", Duration::from_secs(30));
//!
//! # async fn run(db: Db<RedisSource, ReadWrite>) -> redis::RedisResult<()> {
//! if let Some(token) = db.process(JOBS.try_lock("nightly-report")).await? {
//!     // … run the job, extending the lease every few seconds …
//!     db.process(token.extend()).await?;
//!     db.process(token.release()).await?;
//! }
//! # Ok(())
//! # }
//! ```

use super::lock_token;
use crate::effect::io::Write;
use crate::effect::markers::Entity;
use crate::query::{Execute, Query};
use crate::redis::RedisSource;
use crate::redis::{Name, item_key, millis, tagged_pair};
use redis::aio::ConnectionLike;
use redis::{RedisResult, Script};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::time::Duration;

/// Deletes `KEYS[1]` if it holds the token `ARGV[1]`.
static RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
",
    )
});

/// Expires `KEYS[1]` in `ARGV[2]` ms if it holds the token `ARGV[1]`.
static EXTEND: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
",
    )
});

/// Adds the reader `ARGV[1]` for `ARGV[2]` ms to the sorted set of reader
/// expiries `KEYS[2]` unless the writer key `KEYS[1]` exists.
static READ_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(concat!(
        lua_now!(),
        r"
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
local ttl = tonumber(ARGV[2])
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
redis.call('ZADD', KEYS[2], now + ttl, ARGV[1])
if redis.call('PTTL', KEYS[2]) < ttl then redis.call('PEXPIRE', KEYS[2], ttl) end
return 1
",
    ))
});

/// Removes the reader `ARGV[1]` from `KEYS[1]`; returns whether its lease
/// was still running.
static READ_RELEASE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(concat!(
        lua_now!(),
        r"
local expiry = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not expiry then return 0 end
redis.call('ZREM', KEYS[1], ARGV[1])
if tonumber(expiry) > now then return 1 end
return 0
",
    ))
});

/// Renews the lease of the reader `ARGV[1]` in `KEYS[1]` for `ARGV[2]` ms if
/// it is still running.
static READ_EXTEND: LazyLock<Script> = LazyLock::new(|| {
    Script::new(concat!(
        lua_now!(),
        r"
local expiry = redis.call('ZSCORE', KEYS[1], ARGV[1])
if not expiry or tonumber(expiry) <= now then return 0 end
local ttl = tonumber(ARGV[2])
redis.call('ZADD', KEYS[1], 'XX', now + ttl, ARGV[1])
if redis.call('PTTL', KEYS[1]) < ttl then redis.call('PEXPIRE', KEYS[1], ttl) end
return 1
",
    ))
});

/// Sets the writer key `KEYS[1]` to `ARGV[1]` for `ARGV[2]` ms if it doesn't
/// exist and the sorted set of readers `KEYS[2]` has no running lease.
static WRITE_ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(concat!(
        lua_now!(),
        r"
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)
if redis.call('ZCARD', KEYS[2]) > 0 then return 0 end
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then return 1 end
return 0
",
    ))
});

macro_rules! lock_collection {
    ($lock:ident) => {
        impl<N: ?Sized> $lock<N> {
            /// The lock in `namespace`, whose leases last `ttl`. A TTL under
            /// a millisecond counts as one.
            pub const fn new(namespace: &'static str, ttl: Duration) -> Self {
                Self {
                    namespace,
                    ttl,
                    names: PhantomData,
                }
            }

            /// Namespace of the lock.
            pub const fn namespace(&self) -> &'static str {
                self.namespace
            }

            /// How long a lease lasts.
            pub const fn ttl(&self) -> Duration {
                self.ttl
            }
        }

        impl<N: ?Sized> Clone for $lock<N> {
            fn clone(&self) -> Self {
                *self
            }
        }

        impl<N: ?Sized> Copy for $lock<N> {}

        impl<N: ?Sized> Debug for $lock<N> {
            fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($lock))
                    .field("namespace", &self.namespace)
                    .field("ttl", &self.ttl)
                    .finish()
            }
        }

        impl<N: ?Sized> Entity for $lock<N> {}
    };
}

/// A mutual exclusion lock per name `N`.
///
/// Each name is one string key holding the random token of its holder.
pub struct Lock<N: ?Sized> {
    namespace: &'static str,
    ttl: Duration,
    names: PhantomData<fn(&N)>,
}

lock_collection!(Lock);

impl<N: Name + ?Sized> Lock<N> {
    /// Take the lock of `name` if it is free.
    pub fn try_lock(&self, name: &N) -> LockAcquire<N> {
        LockAcquire {
            key: item_key("Lock", self.namespace, name),
            ttl: self.ttl,
            names: PhantomData,
        }
    }
}

/// Query from [`Lock::try_lock`].
pub struct LockAcquire<N: ?Sized> {
    key: Vec<u8>,
    ttl: Duration,
    names: PhantomData<fn(&N)>,
}

impl<N: ?Sized> Query for LockAcquire<N> {
    type Effect = Write<Lock<N>>;
}

impl<N, C> Execute<RedisSource<C>> for LockAcquire<N>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<LockToken<Lock<N>>>;

    async fn execute(self, conn: &mut C) -> RedisResult<Self::Output> {
        let token = lock_token()?;
        let acquired: bool = redis::cmd("SET")
            .arg(&self.key)
            .arg(&token)
            .arg("NX")
            .arg("PX")
            .arg(millis(self.ttl))
            .query_async(conn)
            .await?;
        Ok(acquired.then(|| LockToken::new(self.key, token, self.ttl)))
    }
}

/// Proof of holding the lock `L` exclusively: a [`Lock`] or the write side
/// of a [`RwLock`].
///
/// Dropping the token doesn't release the lock; the lease runs out instead.
pub struct LockToken<L> {
    key: Vec<u8>,
    token: [u8; 16],
    ttl: Duration,
    lock: PhantomData<fn() -> L>,
}

impl<L> LockToken<L> {
    fn new(key: Vec<u8>, token: [u8; 16], ttl: Duration) -> Self {
        Self {
            key,
            token,
            ttl,
            lock: PhantomData,
        }
    }

    /// Release the lock. Outputs `false` if the lease had run out and the
    /// lock was no longer held by this token.
    pub fn release(self) -> LockRelease<L> {
        LockRelease { token: self }
    }

    /// Restart the lease. Outputs `false` if the lease had run out and the
    /// lock was no longer held by this token.
    pub fn extend(&self) -> LockExtend<'_, L> {
        LockExtend { token: self }
    }
}

impl<L> Debug for LockToken<L> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LockToken")
            .field("key", &String::from_utf8_lossy(&self.key))
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

/// Query from [`LockToken::release`].
pub struct LockRelease<L> {
    token: LockToken<L>,
}

impl<L: Entity> Query for LockRelease<L> {
    type Effect = Write<L>;
}

impl<L, C> Execute<RedisSource<C>> for LockRelease<L>
where
    L: Entity,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        RELEASE
            .key(&self.token.key)
            .arg(&self.token.token)
            .invoke_async(conn)
            .await
    }
}

/// Query from [`LockToken::extend`].
pub struct LockExtend<'t, L> {
    token: &'t LockToken<L>,
}

impl<L: Entity> Query for LockExtend<'_, L> {
    type Effect = Write<L>;
}

impl<L, C> Execute<RedisSource<C>> for LockExtend<'_, L>
where
    L: Entity,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        EXTEND
            .key(&self.token.key)
            .arg(&self.token.token)
            .arg(millis(self.token.ttl))
            .invoke_async(conn)
            .await
    }
}

/// A reader-writer lock per name `N`.
///
/// Each name is two keys in one cluster hash slot: a string key holding the
/// token of the writer, and a sorted set of reader tokens scored by the end
/// of their lease. Readers don't wait for a writer that is trying to
/// acquire, so a steady stream of readers can starve writers.
pub struct RwLock<N: ?Sized> {
    namespace: &'static str,
    ttl: Duration,
    names: PhantomData<fn(&N)>,
}

lock_collection!(RwLock);

impl<N: Name + ?Sized> RwLock<N> {
    /// Take a read lease on `name` if no writer holds it.
    pub fn try_read(&self, name: &N) -> RwLockRead<N> {
        let (writer, readers) = self.keys(name);
        RwLockRead {
            writer,
            readers,
            ttl: self.ttl,
            names: PhantomData,
        }
    }

    /// Take the write lock of `name` if neither a writer nor a reader holds
    /// it.
    pub fn try_write(&self, name: &N) -> RwLockWrite<N> {
        let (writer, readers) = self.keys(name);
        RwLockWrite {
            writer,
            readers,
            ttl: self.ttl,
            names: PhantomData,
        }
    }

    /// The writer and readers keys of `name`, in one cluster hash slot.
    fn keys(&self, name: &N) -> (Vec<u8>, Vec<u8>) {
        tagged_pair("RwLock", self.namespace, name, b":w", b":r")
    }
}

/// Query from [`RwLock::try_read`].
pub struct RwLockRead<N: ?Sized> {
    writer: Vec<u8>,
    readers: Vec<u8>,
    ttl: Duration,
    names: PhantomData<fn(&N)>,
}

impl<N: ?Sized> Query for RwLockRead<N> {
    type Effect = Write<RwLock<N>>;
}

impl<N, C> Execute<RedisSource<C>> for RwLockRead<N>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<ReadToken<N>>;

    async fn execute(self, conn: &mut C) -> RedisResult<Self::Output> {
        let token = lock_token()?;
        let acquired: bool = READ_ACQUIRE
            .key(&self.writer)
            .key(&self.readers)
            .arg(&token)
            .arg(millis(self.ttl))
            .invoke_async(conn)
            .await?;
        Ok(acquired.then_some(ReadToken {
            key: self.readers,
            token,
            ttl: self.ttl,
            names: PhantomData,
        }))
    }
}

/// Query from [`RwLock::try_write`].
pub struct RwLockWrite<N: ?Sized> {
    writer: Vec<u8>,
    readers: Vec<u8>,
    ttl: Duration,
    names: PhantomData<fn(&N)>,
}

impl<N: ?Sized> Query for RwLockWrite<N> {
    type Effect = Write<RwLock<N>>;
}

impl<N, C> Execute<RedisSource<C>> for RwLockWrite<N>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<LockToken<RwLock<N>>>;

    async fn execute(self, conn: &mut C) -> RedisResult<Self::Output> {
        let token = lock_token()?;
        let acquired: bool = WRITE_ACQUIRE
            .key(&self.writer)
            .key(&self.readers)
            .arg(&token)
            .arg(millis(self.ttl))
            .invoke_async(conn)
            .await?;
        Ok(acquired.then(|| LockToken::new(self.writer, token, self.ttl)))
    }
}

/// Proof of holding a read lease on a [`RwLock`].
///
/// Dropping the token doesn't release the lease; it runs out instead.
pub struct ReadToken<N: ?Sized> {
    key: Vec<u8>,
    token: [u8; 16],
    ttl: Duration,
    names: PhantomData<fn(&N)>,
}

impl<N: ?Sized> ReadToken<N> {
    /// Release the read lease. Outputs `false` if it had already run out.
    pub fn release(self) -> ReadRelease<N> {
        ReadRelease { token: self }
    }

    /// Restart the read lease. Outputs `false` if it had already run out.
    pub fn extend(&self) -> ReadExtend<'_, N> {
        ReadExtend { token: self }
    }
}

impl<N: ?Sized> Debug for ReadToken<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadToken")
            .field("key", &String::from_utf8_lossy(&self.key))
            .field("ttl", &self.ttl)
            .finish_non_exhaustive()
    }
}

/// Query from [`ReadToken::release`].
pub struct ReadRelease<N: ?Sized> {
    token: ReadToken<N>,
}

impl<N: ?Sized> Query for ReadRelease<N> {
    type Effect = Write<RwLock<N>>;
}

impl<N, C> Execute<RedisSource<C>> for ReadRelease<N>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        READ_RELEASE
            .key(&self.token.key)
            .arg(&self.token.token)
            .invoke_async(conn)
            .await
    }
}

/// Query from [`ReadToken::extend`].
pub struct ReadExtend<'t, N: ?Sized> {
    token: &'t ReadToken<N>,
}

impl<N: ?Sized> Query for ReadExtend<'_, N> {
    type Effect = Write<RwLock<N>>;
}

impl<N, C> Execute<RedisSource<C>> for ReadExtend<'_, N>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        READ_EXTEND
            .key(&self.token.key)
            .arg(&self.token.token)
            .arg(millis(self.token.ttl))
            .invoke_async(conn)
            .await
    }
}
