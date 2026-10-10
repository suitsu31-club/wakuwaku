//! A rate limiter based on token buckets.
//!
//! Each name has a bucket holding up to `capacity` tokens, refilled
//! continuously at a fixed [`Rate`]. A request takes some tokens and is
//! denied if the bucket holds fewer. A bucket starts full, and its key
//! expires once it would be full again, so idle names cost nothing.
//!
//! ```no_run
//! use std::time::Duration;
//! use kanau::processor::Processor;
//! use wakuwaku_entity::redis::RedisSource;
//! use wakuwaku_entity::redis::collection::token_bucket_limiter::{Admission, Rate, TokenBucket};
//! use wakuwaku_entity::{Db, ReadWrite};
//!
//! /// Bursts of 20 logins per address, 1 more every 3 seconds.
//! const LOGINS: TokenBucket<str> =
//!     TokenBucket::new("login", 20, Rate::new(1, Duration::from_secs(3)));
//!
//! # async fn run(db: Db<RedisSource, ReadWrite>) -> redis::RedisResult<()> {
//! match db.process(LOGINS.acquire("203.0.113.7", 1)).await? {
//!     Admission::Allowed { remaining } => { /* … */ }
//!     Admission::Denied { retry_after } => { /* 429 */ }
//! }
//! # Ok(())
//! # }
//! ```

use crate::effect::io::Write;
use crate::effect::markers::Entity;
use crate::query::{Execute, Query};
use crate::redis::RedisSource;
use crate::redis::{Name, item_key};
use redis::aio::ConnectionLike;
use redis::{RedisResult, Script};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::LazyLock;
use std::time::Duration;

/// Takes `ARGV[4]` tokens from the bucket `KEYS[1]` of capacity `ARGV[1]`,
/// refilled with `ARGV[2]` tokens every `ARGV[3]` µs.
///
/// Returns `{allowed, retry_after_ms, remaining}`; `retry_after_ms` is -1
/// when the request is larger than the bucket.
static ACQUIRE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(concat!(
        lua_now!(),
        r"
local capacity = tonumber(ARGV[1])
local refill = tonumber(ARGV[2])
local period_us = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local state = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(state[1])
local ts = tonumber(state[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end
local elapsed_us = math.max(now - ts, 0) * 1000
tokens = math.min(capacity, tokens + elapsed_us * refill / period_us)
if cost > capacity then return {0, -1, math.floor(tokens)} end
local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after = math.ceil((cost - tokens) * period_us / refill / 1000)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
local until_full = math.ceil((capacity - tokens) * period_us / refill / 1000)
redis.call('PEXPIRE', KEYS[1], math.max(until_full, 1))
return {allowed, retry_after, math.floor(tokens)}
",
    ))
});

/// How fast a bucket refills: `tokens` every `period`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rate {
    tokens: NonZeroU64,
    period: Duration,
}

impl Rate {
    /// `tokens` every `period`.
    ///
    /// The bucket refills continuously; `period` is measured in whole
    /// microseconds, at least one.
    ///
    /// # Panics
    ///
    /// If `tokens` is zero.
    pub const fn new(tokens: u64, period: Duration) -> Self {
        match NonZeroU64::new(tokens) {
            Some(tokens) => Self { tokens, period },
            None => panic!("a rate must refill at least one token"),
        }
    }

    /// `tokens` every second.
    ///
    /// # Panics
    ///
    /// If `tokens` is zero.
    pub const fn per_second(tokens: u64) -> Self {
        Self::new(tokens, Duration::from_secs(1))
    }

    /// Tokens added every [`period`](Self::period).
    pub const fn tokens(&self) -> NonZeroU64 {
        self.tokens
    }

    /// Time to add [`tokens`](Self::tokens).
    pub const fn period(&self) -> Duration {
        self.period
    }

    fn period_micros(&self) -> u64 {
        u64::try_from(self.period.as_micros())
            .unwrap_or(u64::MAX)
            .max(1)
    }
}

/// A token bucket per name `N`.
///
/// Each name is a Redis hash with the fields `tokens` and `ts` (the server
/// time of the last update), updated by a Lua script.
pub struct TokenBucket<N: ?Sized> {
    namespace: &'static str,
    capacity: u64,
    rate: Rate,
    names: PhantomData<fn(&N)>,
}

impl<N: ?Sized> TokenBucket<N> {
    /// Buckets in `namespace` holding up to `capacity` tokens and refilled
    /// at `rate`.
    pub const fn new(namespace: &'static str, capacity: u64, rate: Rate) -> Self {
        Self {
            namespace,
            capacity,
            rate,
            names: PhantomData,
        }
    }

    /// Namespace of the buckets.
    pub const fn namespace(&self) -> &'static str {
        self.namespace
    }

    /// Most tokens a bucket holds.
    pub const fn capacity(&self) -> u64 {
        self.capacity
    }

    /// How fast a bucket refills.
    pub const fn rate(&self) -> Rate {
        self.rate
    }
}

impl<N: Name + ?Sized> TokenBucket<N> {
    /// Take `cost` tokens from the bucket of `name`, if it holds that many.
    pub fn acquire(&self, name: &N, cost: u64) -> TokenBucketAcquire<N> {
        TokenBucketAcquire {
            key: self.key(name),
            capacity: self.capacity,
            rate: self.rate,
            cost,
            names: PhantomData,
        }
    }

    /// Refill the bucket of `name`. Outputs whether it had been used.
    pub fn reset(&self, name: &N) -> TokenBucketReset<N> {
        TokenBucketReset {
            key: self.key(name),
            names: PhantomData,
        }
    }

    fn key(&self, name: &N) -> Vec<u8> {
        item_key("TokenBucket", self.namespace, name)
    }
}

impl<N: ?Sized> Clone for TokenBucket<N> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<N: ?Sized> Copy for TokenBucket<N> {}

impl<N: ?Sized> Debug for TokenBucket<N> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenBucket")
            .field("namespace", &self.namespace)
            .field("capacity", &self.capacity)
            .field("rate", &self.rate)
            .finish()
    }
}

impl<N: ?Sized> Entity for TokenBucket<N> {}

/// Output of [`TokenBucket::acquire`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// The tokens were taken.
    Allowed {
        /// Whole tokens left in the bucket.
        remaining: u64,
    },
    /// The bucket holds too few tokens; none were taken.
    Denied {
        /// When the bucket will hold enough tokens if nothing else takes
        /// any, `None` if the request is larger than the bucket.
        retry_after: Option<Duration>,
    },
}

/// Query from [`TokenBucket::acquire`].
pub struct TokenBucketAcquire<N: ?Sized> {
    key: Vec<u8>,
    capacity: u64,
    rate: Rate,
    cost: u64,
    names: PhantomData<fn(&N)>,
}

impl<N: ?Sized> Query for TokenBucketAcquire<N> {
    type Effect = Write<TokenBucket<N>>;
}

impl<N, C> Execute<RedisSource<C>> for TokenBucketAcquire<N>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Admission;

    async fn execute(self, conn: &mut C) -> RedisResult<Admission> {
        let (allowed, retry_after, remaining): (i64, i64, i64) = ACQUIRE
            .key(&self.key)
            .arg(self.capacity)
            .arg(self.rate.tokens.get())
            .arg(self.rate.period_micros())
            .arg(self.cost)
            .invoke_async(conn)
            .await?;
        Ok(if allowed == 1 {
            Admission::Allowed {
                remaining: u64::try_from(remaining).unwrap_or(0),
            }
        } else {
            Admission::Denied {
                retry_after: u64::try_from(retry_after).ok().map(Duration::from_millis),
            }
        })
    }
}

/// Query from [`TokenBucket::reset`].
pub struct TokenBucketReset<N: ?Sized> {
    key: Vec<u8>,
    names: PhantomData<fn(&N)>,
}

impl<N: ?Sized> Query for TokenBucketReset<N> {
    type Effect = Write<TokenBucket<N>>;
}

impl<N, C> Execute<RedisSource<C>> for TokenBucketReset<N>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        let removed: u64 = redis::cmd("DEL").arg(&self.key).query_async(conn).await?;
        Ok(removed > 0)
    }
}
