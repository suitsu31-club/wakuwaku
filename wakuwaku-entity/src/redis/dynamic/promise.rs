//! Values completed once and waited on by anyone: distributed oneshots.
//!
//! A [`Promise`] is how a request handler waits for the outcome of work
//! that runs elsewhere, such as an event consumer: the handler names the
//! promise, starts the work, and [waits](Promise::wait) on the name; the
//! worker [completes](Promise::complete) it once its effect is durable.
//! The handler may start waiting before or after the completion, on any
//! replica. See the
//! [promises guide](https://www.wakuwaku.rs/en/docs/data/redis/promise).
//!
//! ```no_run
//! use std::time::Duration;
//! use kanau::message::{DeserializeError, MessageDe, MessageSer, SerializeError};
//! use kanau::processor::Processor;
//! use wakuwaku_entity::redis::RedisSource;
//! use wakuwaku_entity::redis::dynamic::promise::Promise;
//! use wakuwaku_entity::redis::watch_hub::WatchHub;
//! use wakuwaku_entity::{Db, ReadWrite};
//!
//! enum Registration {
//!     Created,
//!     NameTaken,
//! }
//! # impl MessageSer for Registration {
//! #     type SerError = SerializeError;
//! #     fn to_bytes(self) -> Result<Box<[u8]>, SerializeError> {
//! #         Ok(Box::new([matches!(self, Self::Created).into()]))
//! #     }
//! # }
//! # impl MessageDe for Registration {
//! #     type DeError = DeserializeError;
//! #     fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError> {
//! #         Ok(if bytes == [1] { Self::Created } else { Self::NameTaken })
//! #     }
//! # }
//!
//! const REGISTRATIONS: Promise<u64, Registration> =
//!     Promise::new("registration", Duration::from_secs(600));
//!
//! # async fn run(db: Db<RedisSource, ReadWrite>, hub: WatchHub) -> redis::RedisResult<()> {
//! // Worker, once the account is stored
//! db.process(REGISTRATIONS.complete(&7, Registration::Created)).await?;
//!
//! // Request handler, on any replica
//! match db.process(REGISTRATIONS.wait(&7, &hub, Duration::from_secs(5))).await? {
//!     Some(Registration::Created) => println!("welcome"),
//!     Some(Registration::NameTaken) => println!("pick another name"),
//!     None => println!("still working on it"),
//! }
//! # Ok(())
//! # }
//! ```

use crate::effect::io::{Read, Write};
use crate::effect::markers::Entity;
use crate::query::{Execute, Query};
use crate::redis::watch_hub::WatchHub;
use crate::redis::{Name, RedisSource, deserialize_error, item_key, millis, serialize_error};
use kanau::message::{MessageDe, MessageSer};
use redis::aio::ConnectionLike;
use redis::{RedisError, RedisResult, Script};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::Instant;

/// Names `N` and values `B` of a promise, without owning either.
type Items<N, B> = PhantomData<fn(&N) -> B>;

/// Sets `KEYS[1]` to `ARGV[1]` with a TTL of `ARGV[2]` ms unless it exists,
/// and wakes its waits. Returns whether it set it.
static COMPLETE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
if not redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then return 0 end
redis.call('PUBLISH', KEYS[1], '')
return 1
",
    )
});

/// Values `B` completed once, named by `N`.
///
/// A completed promise keeps its value for its TTL, then disappears, so a
/// wait that starts later sees it as not completed. Pick a TTL longer than
/// the time between the completion and the last wait you expect, such as
/// a client polling again after a timed-out request.
///
/// Redis is not the record of the outcome: store it with the work's effect
/// in the primary database, and fall back to that when a wait times out
/// after the work should have finished.
pub struct Promise<N: ?Sized, B> {
    namespace: &'static str,
    ttl: Duration,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Promise<N, B> {
    /// The promises in `namespace`, kept for `ttl` once completed. A TTL
    /// under a millisecond counts as one.
    pub const fn new(namespace: &'static str, ttl: Duration) -> Self {
        Self {
            namespace,
            ttl,
            items: PhantomData,
        }
    }

    /// Namespace of the promises.
    pub const fn namespace(&self) -> &'static str {
        self.namespace
    }

    /// How long a completed promise keeps its value.
    pub const fn ttl(&self) -> Duration {
        self.ttl
    }
}

impl<N: Name + ?Sized, B> Promise<N, B> {
    /// Complete `name` with `value` and wake its waits. Outputs `false`,
    /// changing nothing, if it was already completed, so redelivered work
    /// can complete it again safely.
    pub fn complete(&self, name: &N, value: B) -> PromiseComplete<N, B> {
        PromiseComplete {
            key: self.key(name),
            ttl: self.ttl,
            value,
            items: PhantomData,
        }
    }

    /// Read the value of `name` without waiting, `None` if it is not
    /// completed.
    pub fn get(&self, name: &N) -> PromiseGet<N, B> {
        PromiseGet {
            key: self.key(name),
            items: PhantomData,
        }
    }

    /// Wait up to `timeout` for `name` to be completed, woken by `hub`.
    /// Outputs its value, or `None` if it is still not completed at the
    /// end.
    pub fn wait(&self, name: &N, hub: &WatchHub, timeout: Duration) -> PromiseWait<N, B> {
        PromiseWait {
            key: self.key(name),
            hub: hub.clone(),
            timeout,
            items: PhantomData,
        }
    }

    /// Delete `name`. Outputs whether it was completed.
    pub fn remove(&self, name: &N) -> PromiseRemove<N, B> {
        PromiseRemove {
            key: self.key(name),
            items: PhantomData,
        }
    }

    fn key(&self, name: &N) -> Vec<u8> {
        item_key("Promise", self.namespace, name)
    }
}

impl<N: ?Sized, B> Clone for Promise<N, B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<N: ?Sized, B> Copy for Promise<N, B> {}

impl<N: ?Sized, B> Debug for Promise<N, B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Promise")
            .field("namespace", &self.namespace)
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl<N: ?Sized, B> Entity for Promise<N, B> {}

/// Read the value at `key`, `None` if absent.
async fn read<B: MessageDe>(
    key: &[u8],
    conn: &mut (impl ConnectionLike + Send),
) -> RedisResult<Option<B>> {
    let value: Option<Vec<u8>> = redis::cmd("GET").arg(key).query_async(conn).await?;
    value
        .map(|value| B::from_bytes(&value).map_err(deserialize_error))
        .transpose()
}

/// Query from [`Promise::complete`].
pub struct PromiseComplete<N: ?Sized, B> {
    key: Vec<u8>,
    ttl: Duration,
    value: B,
    items: Items<N, B>,
}

impl<N: ?Sized, B: Send> Query for PromiseComplete<N, B> {
    type Effect = Write<Promise<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for PromiseComplete<N, B>
where
    N: ?Sized,
    B: MessageSer + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;
    type Error = RedisError;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        let value = self.value.to_bytes().map_err(serialize_error)?;
        COMPLETE
            .key(&self.key)
            .arg(&*value)
            .arg(millis(self.ttl))
            .invoke_async(conn)
            .await
    }
}

/// Query from [`Promise::get`].
pub struct PromiseGet<N: ?Sized, B> {
    key: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for PromiseGet<N, B> {
    type Effect = Read<Promise<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for PromiseGet<N, B>
where
    N: ?Sized,
    B: MessageDe + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<B>;
    type Error = RedisError;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<B>> {
        read(&self.key, conn).await
    }
}

/// Query from [`Promise::wait`].
pub struct PromiseWait<N: ?Sized, B> {
    key: Vec<u8>,
    hub: WatchHub,
    timeout: Duration,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for PromiseWait<N, B> {
    type Effect = Read<Promise<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for PromiseWait<N, B>
where
    N: ?Sized,
    B: MessageDe + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<B>;
    type Error = RedisError;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<B>> {
        let deadline = Instant::now().checked_add(self.timeout);
        // A completed promise needs no subscription.
        if let Some(value) = read(&self.key, conn).await? {
            return Ok(Some(value));
        }
        let mut waiter = self.hub.waiter(self.key.clone());
        let mut turns = waiter.turns(deadline);
        while turns.next().await {
            if let Some(value) = read(&self.key, conn).await? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }
}

/// Query from [`Promise::remove`].
pub struct PromiseRemove<N: ?Sized, B> {
    key: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for PromiseRemove<N, B> {
    type Effect = Write<Promise<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for PromiseRemove<N, B>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;
    type Error = RedisError;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        redis::cmd("UNLINK").arg(&self.key).query_async(conn).await
    }
}
