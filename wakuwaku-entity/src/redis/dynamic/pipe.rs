//! One-way message pipes from one sender to one receiver.
//!
//! The receiver gets each message once, in the order they were sent, until
//! the sender closes the pipe. Taking a message removes it, so a receiver
//! that fails after taking one loses it. See the
//! [pipes guide](https://www.wakuwaku.rs/en/docs/data/redis/pipe).
//!
//! ```no_run
//! use std::time::Duration;
//! use futures_util::StreamExt;
//! use kanau::message::{DeserializeError, MessageDe, MessageSer, SerializeError};
//! use kanau::processor::Processor;
//! use wakuwaku_entity::redis::RedisSource;
//! use wakuwaku_entity::redis::dynamic::pipe::Pipe;
//! use wakuwaku_entity::redis::watch_hub::WatchHub;
//! use wakuwaku_entity::{Db, ReadWrite};
//!
//! struct Frame(String);
//! # impl MessageSer for Frame {
//! #     type SerError = SerializeError;
//! #     fn to_bytes(self) -> Result<Box<[u8]>, SerializeError> { Ok(self.0.into_bytes().into()) }
//! # }
//! # impl MessageDe for Frame {
//! #     type DeError = DeserializeError;
//! #     fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError> {
//! #         Ok(Self(String::from_utf8_lossy(bytes).into_owned()))
//! #     }
//! # }
//!
//! const TO_CLIENT: Pipe<str, Frame> = Pipe::new("to-client", Duration::from_secs(60));
//!
//! # async fn run(db: Db<RedisSource, ReadWrite>, hub: WatchHub) -> redis::RedisResult<()> {
//! // Sender
//! db.process(TO_CLIENT.send("session-1", Frame("hello".into()))).await?;
//! db.process(TO_CLIENT.close("session-1")).await?;
//!
//! // Receiver, woken by the process's hub
//! let receiver = db.process(TO_CLIENT.receive("session-1", &hub)).await?;
//! let mut frames = std::pin::pin!(receiver.into_stream());
//! while let Some(frame) = frames.next().await {
//!     let Frame(text) = frame?;
//!     println!("{text}");
//! }
//! # Ok(())
//! # }
//! ```

use super::gone;
use crate::effect::io::Write;
use crate::effect::markers::Entity;
use crate::query::{Execute, Query};
use crate::redis::watch_hub::{Waiter, WatchHub};
use crate::redis::{
    Name, RedisSource, corrupted, deserialize_error, millis, serialize_error, tagged_pair,
};
use futures_util::Stream;
use kanau::message::{MessageDe, MessageSer};
use redis::aio::ConnectionLike;
use redis::{RedisError, RedisResult, Script, Value};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::LazyLock;
use std::time::Duration;
use tokio::time::Instant;

/// Names `N` and messages `B` of a pipe, without owning either.
type Items<N, B> = PhantomData<fn(&N) -> B>;

/// Pushes `ARGV[1]` to the list `KEYS[1]`, restarts its TTL of `ARGV[2]`
/// ms, and wakes the receiver on the channel `ARGV[3]`.
static PUSH: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
redis.call('RPUSH', KEYS[1], ARGV[1])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
redis.call('PUBLISH', ARGV[3], '')
",
    )
});

/// Pops the first message of `KEYS[1]` and returns it. Without one, pops
/// the close marker of `KEYS[2]` and returns 0, or returns nil if the pipe
/// is still open.
static TAKE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local message = redis.call('LPOP', KEYS[1])
if message then return message end
if redis.call('LPOP', KEYS[2]) then return 0 end
return false
",
    )
});

/// Pipes of messages `B`, named by `N`.
///
/// The receiver sees the close after every message sent before it. Every
/// send and the close restart the TTL of what they write, so a pipe whose
/// sender stopped without closing it disappears one TTL later, and a
/// receiver that gets nothing for a whole TTL fails instead of waiting
/// forever.
pub struct Pipe<N: ?Sized, B> {
    namespace: &'static str,
    ttl: Duration,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Pipe<N, B> {
    /// The pipes in `namespace`, abandoned after `ttl` without a message. A
    /// TTL under a millisecond counts as one.
    pub const fn new(namespace: &'static str, ttl: Duration) -> Self {
        Self {
            namespace,
            ttl,
            items: PhantomData,
        }
    }

    /// Namespace of the pipes.
    pub const fn namespace(&self) -> &'static str {
        self.namespace
    }

    /// How long a pipe lives without a message.
    pub const fn ttl(&self) -> Duration {
        self.ttl
    }
}

impl<N: Name + ?Sized, B> Pipe<N, B> {
    /// Send `body` through `name`.
    ///
    /// Sending after [`close`](Self::close) is allowed but the receiver
    /// never reads it.
    pub fn send(&self, name: &N, body: B) -> PipeSend<N, B> {
        let (messages, _) = self.keys(name);
        PipeSend {
            channel: messages.clone(),
            messages,
            ttl: self.ttl,
            body,
            items: PhantomData,
        }
    }

    /// Close `name`: its receiver ends after taking the messages sent
    /// before.
    pub fn close(&self, name: &N) -> PipeClose<N, B> {
        let (channel, close) = self.keys(name);
        PipeClose {
            close,
            channel,
            ttl: self.ttl,
            items: PhantomData,
        }
    }

    /// Receive the messages of `name`, woken by `hub`.
    ///
    /// Taking a message removes it, so a pipe must have one receiver at a
    /// time.
    pub fn receive(&self, name: &N, hub: &WatchHub) -> PipeReceive<N, B> {
        let (messages, close) = self.keys(name);
        PipeReceive {
            messages,
            close,
            ttl: self.ttl,
            hub: hub.clone(),
            items: PhantomData,
        }
    }

    /// The message and close keys of `name`. The message key is also the
    /// channel that wakes the receiver.
    fn keys(&self, name: &N) -> (Vec<u8>, Vec<u8>) {
        tagged_pair("Pipe", self.namespace, name, b":m", b":c")
    }
}

impl<N: ?Sized, B> Clone for Pipe<N, B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<N: ?Sized, B> Copy for Pipe<N, B> {}

impl<N: ?Sized, B> Debug for Pipe<N, B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Pipe")
            .field("namespace", &self.namespace)
            .field("ttl", &self.ttl)
            .finish()
    }
}

impl<N: ?Sized, B> Entity for Pipe<N, B> {}

/// Push `value` to the list `key`, restart its TTL, and wake the receiver
/// on `channel`.
async fn push(
    key: &[u8],
    value: &[u8],
    channel: &[u8],
    ttl: Duration,
    conn: &mut (impl ConnectionLike + Send),
) -> RedisResult<()> {
    PUSH.key(key)
        .arg(value)
        .arg(millis(ttl))
        .arg(channel)
        .invoke_async(conn)
        .await
}

/// Query from [`Pipe::send`].
pub struct PipeSend<N: ?Sized, B> {
    messages: Vec<u8>,
    channel: Vec<u8>,
    ttl: Duration,
    body: B,
    items: Items<N, B>,
}

impl<N: ?Sized, B: Send> Query for PipeSend<N, B> {
    type Effect = Write<Pipe<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for PipeSend<N, B>
where
    N: ?Sized,
    B: MessageSer + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = ();
    type Error = RedisError;

    async fn execute(self, conn: &mut C) -> RedisResult<()> {
        let body = self.body.to_bytes().map_err(serialize_error)?;
        push(&self.messages, &body, &self.channel, self.ttl, conn).await
    }
}

/// Query from [`Pipe::close`].
pub struct PipeClose<N: ?Sized, B> {
    close: Vec<u8>,
    channel: Vec<u8>,
    ttl: Duration,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for PipeClose<N, B> {
    type Effect = Write<Pipe<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for PipeClose<N, B>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = ();
    type Error = RedisError;

    async fn execute(self, conn: &mut C) -> RedisResult<()> {
        push(&self.close, b"", &self.channel, self.ttl, conn).await
    }
}

/// Query from [`Pipe::receive`]. Taking messages removes them, so it is a
/// write.
pub struct PipeReceive<N: ?Sized, B> {
    messages: Vec<u8>,
    close: Vec<u8>,
    ttl: Duration,
    hub: WatchHub,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for PipeReceive<N, B> {
    type Effect = Write<Pipe<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for PipeReceive<N, B>
where
    N: ?Sized,
    B: MessageDe,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = PipeReceiver<B, C>;
    type Error = RedisError;

    async fn execute(self, conn: &mut C) -> RedisResult<PipeReceiver<B, C>> {
        Ok(PipeReceiver {
            conn: conn.clone(),
            waiter: self.hub.waiter(self.messages.clone()),
            messages: self.messages,
            close: self.close,
            ttl: self.ttl,
            closed: false,
            bodies: PhantomData,
        })
    }
}

/// What one take found.
enum Taken {
    Message(Vec<u8>),
    Closed,
    Empty,
}

/// Take the next message of `messages`, or the close marker of `close`.
async fn take(
    messages: &[u8],
    close: &[u8],
    conn: &mut (impl ConnectionLike + Send),
) -> RedisResult<Taken> {
    let reply: Value = TAKE.key(messages).key(close).invoke_async(conn).await?;
    match reply {
        Value::BulkString(message) => Ok(Taken::Message(message)),
        Value::Int(0) => Ok(Taken::Closed),
        Value::Nil => Ok(Taken::Empty),
        _ => Err(corrupted("unexpected pipe take reply")),
    }
}

/// The receiving end of a [`Pipe`], from [`Pipe::receive`], reading on a
/// clone of the [`RedisSource`] connection `C`.
pub struct PipeReceiver<B, C> {
    conn: C,
    waiter: Waiter,
    messages: Vec<u8>,
    close: Vec<u8>,
    ttl: Duration,
    closed: bool,
    bodies: PhantomData<fn() -> B>,
}

impl<B: MessageDe, C: ConnectionLike + Send> PipeReceiver<B, C> {
    /// Take the next message, waiting for it. `None` once the sender closed
    /// the pipe.
    ///
    /// Fails if nothing arrives for a whole TTL: the pipe is then
    /// abandoned. A failed read may be retried.
    pub async fn recv(&mut self) -> RedisResult<Option<B>> {
        if self.closed {
            return Ok(None);
        }
        let mut turns = self.waiter.turns(Instant::now().checked_add(self.ttl));
        while turns.next().await {
            match take(&self.messages, &self.close, &mut self.conn).await? {
                Taken::Message(body) => {
                    return B::from_bytes(&body).map(Some).map_err(deserialize_error);
                }
                Taken::Closed => {
                    self.closed = true;
                    return Ok(None);
                }
                Taken::Empty => {}
            }
        }
        Err(gone("the pipe received nothing for its whole TTL"))
    }

    /// The messages as a stream, ending when the sender closes the pipe.
    pub fn into_stream(self) -> impl Stream<Item = RedisResult<B>> {
        futures_util::stream::unfold(self, |mut receiver| async move {
            receiver
                .recv()
                .await
                .transpose()
                .map(|item| (item, receiver))
        })
    }
}

impl<B, C> Debug for PipeReceiver<B, C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipeReceiver")
            .field("messages", &String::from_utf8_lossy(&self.messages))
            .field("ttl", &self.ttl)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}
