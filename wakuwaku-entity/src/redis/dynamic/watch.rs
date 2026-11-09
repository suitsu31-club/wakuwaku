//! The latest version of a value, waited on for newer ones: distributed
//! watch channels.
//!
//! A [`Watch`] projects state owned elsewhere, such as a payment row in the
//! primary database, so that request handlers on any replica can wait for
//! it to move on. The owner [publishes](Watch::publish) each version it
//! commits, with the version it stored; waiters ask for anything
//! [newer](Watch::changed) than the version they last saw. See the
//! [watches guide](https://www.wakuwaku.rs/en/docs/data/redis/watch).
//!
//! ```no_run
//! use std::time::Duration;
//! use kanau::message::{DeserializeError, MessageDe, MessageSer, SerializeError};
//! use kanau::processor::Processor;
//! use wakuwaku_entity::redis::RedisSource;
//! use wakuwaku_entity::redis::dynamic::watch::Watch;
//! use wakuwaku_entity::redis::watch_hub::WatchHub;
//! use wakuwaku_entity::{Db, ReadWrite};
//!
//! struct PaymentStatus(String);
//! # impl MessageSer for PaymentStatus {
//! #     type SerError = SerializeError;
//! #     fn to_bytes(self) -> Result<Box<[u8]>, SerializeError> { Ok(self.0.into_bytes().into()) }
//! # }
//! # impl MessageDe for PaymentStatus {
//! #     type DeError = DeserializeError;
//! #     fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError> {
//! #         Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
//! #     }
//! # }
//!
//! const PAYMENTS: Watch<u64, PaymentStatus> = Watch::new("payment", Duration::from_secs(3600));
//!
//! # async fn run(db: Db<RedisSource, ReadWrite>, hub: WatchHub) -> redis::RedisResult<()> {
//! // Owner, after committing version 3 of payment 42
//! db.process(PAYMENTS.publish(&42, 3, PaymentStatus("requires_action".into()))).await?;
//!
//! // Request handler, on any replica, having seen version 3
//! let next = PAYMENTS.changed(&42, 3, &hub, Duration::from_secs(10));
//! match db.process(next).await? {
//!     Some(status) => println!("now {} at version {}", status.body.0, status.version),
//!     None => println!("no news yet"),
//! }
//! # Ok(())
//! # }
//! ```

use super::{Versioned, read_versioned};
use crate::effect::io::{Read, Write};
use crate::effect::markers::Entity;
use crate::query::{Execute, Query};
use crate::redis::watch_hub::WatchHub;
use crate::redis::{Name, RedisSource, item_key, millis, serialize_error};
use kanau::message::{MessageDe, MessageSer};
use redis::aio::ConnectionLike;
use redis::{RedisError, RedisResult, Script};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::Instant;

/// Names `N` and bodies `B` of a watch, without owning either.
type Items<N, B> = PhantomData<fn(&N) -> B>;

/// If the version `ARGV[1]` is newer than the one of `KEYS[1]` (0 when
/// absent), stores it with the body `ARGV[2]`, restarts the TTL of
/// `ARGV[3]` ms and wakes the waits. Returns whether it stored it.
///
/// Versions are compared as canonical decimal strings, longer is larger,
/// so every `u64` compares exactly.
static PUBLISH: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local current = redis.call('HGET', KEYS[1], 'v') or '0'
local version = ARGV[1]
if #version < #current or (#version == #current and version <= current) then return 0 end
redis.call('HSET', KEYS[1], 'v', version, 'b', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
redis.call('PUBLISH', KEYS[1], '')
return 1
",
    )
});

/// The latest version of bodies `B`, named by `N`.
///
/// Versions come from the publisher, usually the version of the state it
/// projects, and only move forward: publishing a version that is not newer
/// than the stored one changes nothing. Redelivered or reordered
/// publications are therefore harmless. An absent watch is at version 0,
/// so versions start at 1.
///
/// Every publication restarts the TTL, so a watch lives one TTL after its
/// last change. Redis is not the record of the state: read the owner of
/// the state when a wait times out after it should have changed.
pub struct Watch<N: ?Sized, B> {
    namespace: &'static str,
    ttl: Duration,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Watch<N, B> {
    /// The watches in `namespace`, removed `ttl` after their last
    /// publication. A TTL under a millisecond counts as one.
    pub const fn new(namespace: &'static str, ttl: Duration) -> Self {
        Self {
            namespace,
            ttl,
            items: PhantomData,
        }
    }

    /// Namespace of the watches.
    pub const fn namespace(&self) -> &'static str {
        self.namespace
    }

    /// How long a watch lives after its last publication.
    pub const fn ttl(&self) -> Duration {
        self.ttl
    }
}

impl<N: Name + ?Sized, B> Watch<N, B> {
    /// Store `body` as `version` of `name` and wake its waits, if `version`
    /// is newer than the stored one. Outputs `false`, changing nothing,
    /// otherwise.
    pub fn publish(&self, name: &N, version: u64, body: B) -> WatchPublish<N, B> {
        WatchPublish {
            key: self.key(name),
            ttl: self.ttl,
            version,
            body,
            items: PhantomData,
        }
    }

    /// Read the latest version of `name` and its body, `None` if it is
    /// absent.
    pub fn get(&self, name: &N) -> WatchGet<N, B> {
        WatchGet {
            key: self.key(name),
            items: PhantomData,
        }
    }

    /// Wait up to `timeout` for a version of `name` newer than `after`,
    /// woken by `hub`. Outputs the latest version if it is newer, at once
    /// if it already is, or `None` if none is at the end. `after` 0 waits
    /// for any version.
    pub fn changed(
        &self,
        name: &N,
        after: u64,
        hub: &WatchHub,
        timeout: Duration,
    ) -> WatchChanged<N, B> {
        WatchChanged {
            key: self.key(name),
            after,
            hub: hub.clone(),
            timeout,
            items: PhantomData,
        }
    }

    /// Delete `name`. Outputs whether it was present.
    pub fn remove(&self, name: &N) -> WatchRemove<N, B> {
        WatchRemove {
            key: self.key(name),
            items: PhantomData,
        }
    }

    fn key(&self, name: &N) -> Vec<u8> {
        item_key("Watch", self.namespace, name)
    }
}

impl<N: ?Sized, B> Clone for Watch<N, B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<N: ?Sized, B> Copy for Watch<N, B> {}

impl<N: ?Sized, B> Debug for Watch<N, B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Watch")
            .field("namespace", &self.namespace)
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl<N: ?Sized, B> Entity for Watch<N, B> {}

/// Query from [`Watch::publish`].
pub struct WatchPublish<N: ?Sized, B> {
    key: Vec<u8>,
    ttl: Duration,
    version: u64,
    body: B,
    items: Items<N, B>,
}

impl<N: ?Sized, B: Send> Query for WatchPublish<N, B> {
    type Effect = Write<Watch<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for WatchPublish<N, B>
where
    N: ?Sized,
    B: MessageSer + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;
    type Error = RedisError;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        let body = self.body.to_bytes().map_err(serialize_error)?;
        PUBLISH
            .key(&self.key)
            .arg(self.version)
            .arg(&*body)
            .arg(millis(self.ttl))
            .invoke_async(conn)
            .await
    }
}

/// Query from [`Watch::get`].
pub struct WatchGet<N: ?Sized, B> {
    key: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for WatchGet<N, B> {
    type Effect = Read<Watch<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for WatchGet<N, B>
where
    N: ?Sized,
    B: MessageDe + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<Versioned<B>>;
    type Error = RedisError;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<Versioned<B>>> {
        read_versioned(&self.key, conn).await
    }
}

/// Query from [`Watch::changed`].
pub struct WatchChanged<N: ?Sized, B> {
    key: Vec<u8>,
    after: u64,
    hub: WatchHub,
    timeout: Duration,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for WatchChanged<N, B> {
    type Effect = Read<Watch<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for WatchChanged<N, B>
where
    N: ?Sized,
    B: MessageDe + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<Versioned<B>>;
    type Error = RedisError;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<Versioned<B>>> {
        let deadline = Instant::now().checked_add(self.timeout);
        let newer = |latest: &Versioned<B>| latest.version > self.after;
        // A version that is already newer needs no subscription.
        if let Some(latest) = read_versioned::<B>(&self.key, conn).await?
            && newer(&latest)
        {
            return Ok(Some(latest));
        }
        let mut waiter = self.hub.waiter(self.key.clone());
        let mut turns = waiter.turns(deadline);
        while turns.next().await {
            if let Some(latest) = read_versioned::<B>(&self.key, conn).await?
                && newer(&latest)
            {
                return Ok(Some(latest));
            }
        }
        Ok(None)
    }
}

/// Query from [`Watch::remove`].
pub struct WatchRemove<N: ?Sized, B> {
    key: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for WatchRemove<N, B> {
    type Effect = Write<Watch<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for WatchRemove<N, B>
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
