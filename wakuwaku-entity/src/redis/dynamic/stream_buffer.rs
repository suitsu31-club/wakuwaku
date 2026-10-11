//! Append-only arrays of bodies, followed as they grow.
//!
//! A stream buffer holds the bodies appended to it, in order, until its
//! producer finishes it. Any number of subscribers can read it from any
//! index and follow the new bodies, so a client that reconnects resumes
//! where it stopped. It suits streamed output such as the chunks of an LLM
//! response.
//!
//! ```no_run
//! use std::time::Duration;
//! use futures_util::StreamExt;
//! use kanau::message::{DeserializeError, MessageDe, MessageSer, SerializeError};
//! use kanau::processor::Processor;
//! use redis::aio::MultiplexedConnection;
//! use wakuwaku_entity::redis::RedisSource;
//! use wakuwaku_entity::redis::dynamic::stream_buffer::StreamBuffer;
//! use wakuwaku_entity::{Db, ReadWrite};
//!
//! struct Chunk(String);
//! # impl MessageSer for Chunk {
//! #     type SerError = SerializeError;
//! #     fn to_bytes(self) -> Result<Box<[u8]>, SerializeError> { Ok(self.0.into_bytes().into()) }
//! # }
//! # impl MessageDe for Chunk {
//! #     type DeError = DeserializeError;
//! #     fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError> {
//! #         Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
//! #     }
//! # }
//!
//! const COMPLETIONS: StreamBuffer<u64, Chunk> =
//!     StreamBuffer::new("completion", Duration::from_secs(120));
//!
//! # async fn run(
//! #     db: Db<RedisSource, ReadWrite>,
//! #     dedicated: MultiplexedConnection,
//! # ) -> redis::RedisResult<()> {
//! // Producer
//! db.process(COMPLETIONS.append(&7, Chunk("Hello".into()))).await?;
//! db.process(COMPLETIONS.append(&7, Chunk(", world".into()))).await?;
//! db.process(COMPLETIONS.finish(&7)).await?;
//!
//! // Subscriber, from the start, on a connection of its own
//! let subscription = db.process(COMPLETIONS.subscribe(&7, 0, dedicated)).await?;
//! let mut chunks = std::pin::pin!(subscription.into_stream());
//! while let Some(chunk) = chunks.next().await {
//!     print!("{}", chunk?.0);
//! }
//! # Ok(())
//! # }
//! ```

use super::{Entry, EntryId, StreamCursor, gone, parse_range};
use crate::effect::io::{Read, Write};
use crate::effect::markers::Entity;
use crate::query::{Execute, Query};
use crate::redis::{
    Name, RedisSource, corrupted, deserialize_error, item_key, millis, serialize_error,
};
use futures_util::Stream;
use kanau::message::{MessageDe, MessageSer};
use redis::aio::ConnectionLike;
use redis::{RedisResult, Script};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::time::Duration;

/// Names `N` and bodies `B` of a stream buffer, without owning either.
type Items<N, B> = PhantomData<fn(&N) -> B>;

/// Field of an entry holding a body.
const BODY_FIELD: &[u8] = b"b";
/// Field of the entry that ends a finished buffer.
const END_FIELD: &[u8] = b"end";

/// Appends `ARGV[1]` to `KEYS[1]` and restarts its TTL of `ARGV[2]` ms.
/// Returns the index of the body, or -1 if the buffer is finished.
static APPEND: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)[1]
if last and last[2][1] == 'end' then return -1 end
local len = redis.call('XLEN', KEYS[1])
redis.call('XADD', KEYS[1], string.format('0-%d', len + 1), 'b', ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return len
",
    )
});

/// Ends `KEYS[1]` and restarts its TTL of `ARGV[1]` ms. Returns 0 if it was
/// already finished.
static FINISH: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local last = redis.call('XREVRANGE', KEYS[1], '+', '-', 'COUNT', 1)[1]
if last and last[2][1] == 'end' then return 0 end
local len = redis.call('XLEN', KEYS[1])
redis.call('XADD', KEYS[1], string.format('0-%d', len + 1), 'end', '')
redis.call('PEXPIRE', KEYS[1], ARGV[1])
return 1
",
    )
});

/// Append-only arrays of bodies `B`, named by `N`.
///
/// Each name is one Redis stream. The body at index `i` is the entry
/// `0-(i + 1)`, and a finished buffer ends with one more entry marking the
/// end.
///
/// Every append and the finish restart the TTL, so a buffer lives one TTL
/// after its last change, finished or not. A subscriber that gets nothing
/// for a whole TTL checks whether the buffer still exists, and fails if
/// not, so it doesn't wait forever on a producer that stopped without
/// finishing.
pub struct StreamBuffer<N: ?Sized, B> {
    namespace: &'static str,
    ttl: Duration,
    items: Items<N, B>,
}

impl<N: ?Sized, B> StreamBuffer<N, B> {
    /// The stream buffers in `namespace`, removed `ttl` after their last
    /// change. A TTL under a millisecond counts as one.
    pub const fn new(namespace: &'static str, ttl: Duration) -> Self {
        Self {
            namespace,
            ttl,
            items: PhantomData,
        }
    }

    /// Namespace of the buffers.
    pub const fn namespace(&self) -> &'static str {
        self.namespace
    }

    /// How long a buffer lives after its last change.
    pub const fn ttl(&self) -> Duration {
        self.ttl
    }
}

impl<N: Name + ?Sized, B> StreamBuffer<N, B> {
    /// Append `body` to `name`, creating the buffer if needed. Outputs the
    /// index of the body, `None` if the buffer is finished.
    pub fn append(&self, name: &N, body: B) -> StreamBufferAppend<N, B> {
        StreamBufferAppend {
            key: self.key(name),
            ttl: self.ttl,
            body,
            items: PhantomData,
        }
    }

    /// Finish `name`, creating it empty if needed: subscribers end after
    /// the last body, and appends fail. Outputs `false` if it was already
    /// finished.
    pub fn finish(&self, name: &N) -> StreamBufferFinish<N, B> {
        StreamBufferFinish {
            key: self.key(name),
            ttl: self.ttl,
            items: PhantomData,
        }
    }

    /// Read the bodies of `name` from index `from` on. An absent buffer
    /// reads as empty and unfinished.
    pub fn read(&self, name: &N, from: u64) -> StreamBufferRead<N, B> {
        StreamBufferRead {
            key: self.key(name),
            from,
            items: PhantomData,
        }
    }

    /// Follow `name` from index `from` on, using `conn`, a connection used
    /// for nothing else (see [following changes](super#following-changes)).
    ///
    /// The subscription first reads the bodies already appended, then waits
    /// for new ones, and ends after the last body of a finished buffer. It
    /// may start before the buffer exists.
    pub fn subscribe<D>(&self, name: &N, from: u64, conn: D) -> StreamBufferSubscribe<N, B, D> {
        StreamBufferSubscribe {
            key: self.key(name),
            from,
            ttl: self.ttl,
            conn,
            items: PhantomData,
        }
    }

    /// Delete `name`. Outputs whether it was present.
    ///
    /// Its subscribers fail one TTL later, like for an expired buffer.
    pub fn remove(&self, name: &N) -> StreamBufferRemove<N, B> {
        StreamBufferRemove {
            key: self.key(name),
            items: PhantomData,
        }
    }

    fn key(&self, name: &N) -> Vec<u8> {
        item_key("StreamBuffer", self.namespace, name)
    }
}

impl<N: ?Sized, B> Clone for StreamBuffer<N, B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<N: ?Sized, B> Copy for StreamBuffer<N, B> {}

impl<N: ?Sized, B> Debug for StreamBuffer<N, B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamBuffer")
            .field("namespace", &self.namespace)
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl<N: ?Sized, B> Entity for StreamBuffer<N, B> {}

/// What a stream buffer entry holds.
enum Item<B> {
    Body(B),
    End,
}

impl<B: MessageDe> Item<B> {
    fn parse(entry: &Entry) -> RedisResult<Self> {
        if let Some(body) = entry.get(BODY_FIELD) {
            B::from_bytes(body)
                .map(Self::Body)
                .map_err(deserialize_error)
        } else if entry.get(END_FIELD).is_some() {
            Ok(Self::End)
        } else {
            Err(corrupted("unknown stream buffer entry"))
        }
    }
}

/// ID of the entry after which the body at index `from` comes.
const fn cursor(from: u64) -> EntryId {
    EntryId { ms: 0, seq: from }
}

/// Query from [`StreamBuffer::append`].
pub struct StreamBufferAppend<N: ?Sized, B> {
    key: Vec<u8>,
    ttl: Duration,
    body: B,
    items: Items<N, B>,
}

impl<N: ?Sized, B: Send> Query for StreamBufferAppend<N, B> {
    type Effect = Write<StreamBuffer<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for StreamBufferAppend<N, B>
where
    N: ?Sized,
    B: MessageSer + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<u64>;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<u64>> {
        let body = self.body.to_bytes().map_err(serialize_error)?;
        let index: i64 = APPEND
            .key(&self.key)
            .arg(&*body)
            .arg(millis(self.ttl))
            .invoke_async(conn)
            .await?;
        Ok(u64::try_from(index).ok())
    }
}

/// Query from [`StreamBuffer::finish`].
pub struct StreamBufferFinish<N: ?Sized, B> {
    key: Vec<u8>,
    ttl: Duration,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for StreamBufferFinish<N, B> {
    type Effect = Write<StreamBuffer<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for StreamBufferFinish<N, B>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        FINISH
            .key(&self.key)
            .arg(millis(self.ttl))
            .invoke_async(conn)
            .await
    }
}

/// Bodies read from a [`StreamBuffer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Contents<B> {
    /// The bodies from the requested index on, in order.
    pub bodies: Vec<B>,
    /// Whether the buffer is finished, so no body follows these.
    pub finished: bool,
}

/// Query from [`StreamBuffer::read`].
pub struct StreamBufferRead<N: ?Sized, B> {
    key: Vec<u8>,
    from: u64,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for StreamBufferRead<N, B> {
    type Effect = Read<StreamBuffer<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for StreamBufferRead<N, B>
where
    N: ?Sized,
    B: MessageDe + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Contents<B>;

    async fn execute(self, conn: &mut C) -> RedisResult<Contents<B>> {
        let reply = redis::cmd("XRANGE")
            .arg(&self.key)
            .arg(
                EntryId {
                    ms: 0,
                    seq: self.from.saturating_add(1),
                }
                .to_string(),
            )
            .arg("+")
            .query_async(conn)
            .await?;
        let entries = parse_range(reply)?;
        let mut contents = Contents {
            bodies: Vec::with_capacity(entries.len()),
            finished: false,
        };
        for entry in &entries {
            match Item::parse(entry)? {
                Item::Body(body) => contents.bodies.push(body),
                Item::End => contents.finished = true,
            }
        }
        Ok(contents)
    }
}

/// Query from [`StreamBuffer::subscribe`].
pub struct StreamBufferSubscribe<N: ?Sized, B, D> {
    key: Vec<u8>,
    from: u64,
    ttl: Duration,
    conn: D,
    items: Items<N, B>,
}

impl<N: ?Sized, B, D: Send> Query for StreamBufferSubscribe<N, B, D> {
    type Effect = Read<StreamBuffer<N, B>>;
}

impl<N, B, C, D> Execute<RedisSource<C>> for StreamBufferSubscribe<N, B, D>
where
    N: ?Sized,
    B: MessageDe,
    C: ConnectionLike + Clone + Send + Sync,
    D: ConnectionLike + Send,
{
    type Output = StreamBufferSubscription<B, D>;

    async fn execute(self, _: &mut C) -> RedisResult<StreamBufferSubscription<B, D>> {
        Ok(StreamBufferSubscription {
            cursor: StreamCursor::new(self.conn, self.key, cursor(self.from)),
            ttl: self.ttl,
            ended: false,
            bodies: PhantomData,
        })
    }
}

/// A subscription to a [`StreamBuffer`], from [`StreamBuffer::subscribe`].
pub struct StreamBufferSubscription<B, D> {
    cursor: StreamCursor<D>,
    ttl: Duration,
    ended: bool,
    bodies: PhantomData<fn() -> B>,
}

impl<B: MessageDe, D: ConnectionLike + Send> StreamBufferSubscription<B, D> {
    /// The next body, waiting for it to be appended. `None` after the last
    /// body of a finished buffer.
    ///
    /// Fails if nothing is appended for a whole TTL and the buffer no
    /// longer exists, after which it outputs `None`. Other failures may be
    /// retried.
    pub async fn recv(&mut self) -> RedisResult<Option<B>> {
        let block = millis(self.ttl);
        while !self.ended {
            let Some(entry) = self.cursor.next(block).await? else {
                let exists: bool = redis::cmd("EXISTS")
                    .arg(&self.cursor.key)
                    .query_async(&mut self.cursor.conn)
                    .await?;
                if !exists {
                    self.ended = true;
                    return Err(gone(
                        "the stream buffer expired or was removed before it finished",
                    ));
                }
                continue;
            };
            match Item::parse(&entry)? {
                Item::Body(body) => return Ok(Some(body)),
                Item::End => self.ended = true,
            }
        }
        Ok(None)
    }

    /// The bodies as a stream, ending after the last body of a finished
    /// buffer.
    pub fn into_stream(self) -> impl Stream<Item = RedisResult<B>> {
        futures_util::stream::unfold(self, |mut subscription| async move {
            subscription
                .recv()
                .await
                .transpose()
                .map(|item| (item, subscription))
        })
    }
}

impl<B, D> Debug for StreamBufferSubscription<B, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamBufferSubscription")
            .field("key", &String::from_utf8_lossy(&self.cursor.key))
            .field("after", &self.cursor.after)
            .field("ended", &self.ended)
            .finish_non_exhaustive()
    }
}

/// Query from [`StreamBuffer::remove`].
pub struct StreamBufferRemove<N: ?Sized, B> {
    key: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for StreamBufferRemove<N, B> {
    type Effect = Write<StreamBuffer<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for StreamBufferRemove<N, B>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        redis::cmd("UNLINK").arg(&self.key).query_async(conn).await
    }
}
