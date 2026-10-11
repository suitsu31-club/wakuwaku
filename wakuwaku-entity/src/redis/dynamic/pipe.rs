//! One-way message pipes from one sender to one receiver.
//!
//! A pipe is a queue of messages, like a websocket in one direction: the
//! receiver gets each message once, in the order they were sent, until the
//! sender closes the pipe. Messages wait in Redis while no receiver is
//! listening, so the receiver may connect before or after the sender. Use a
//! second pipe for the other direction.
//!
//! A message is removed from Redis when the receiver takes it, so a
//! receiver that fails after taking a message loses it.
//!
//! ```no_run
//! use std::time::Duration;
//! use futures_util::StreamExt;
//! use kanau::message::{DeserializeError, MessageDe, MessageSer, SerializeError};
//! use kanau::processor::Processor;
//! use redis::aio::MultiplexedConnection;
//! use wakuwaku_entity::redis::RedisSource;
//! use wakuwaku_entity::redis::dynamic::pipe::Pipe;
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
//! # async fn run(
//! #     db: Db<RedisSource, ReadWrite>,
//! #     dedicated: MultiplexedConnection,
//! # ) -> redis::RedisResult<()> {
//! // Sender
//! db.process(TO_CLIENT.send("session-1", Frame("hello".into()))).await?;
//! db.process(TO_CLIENT.close("session-1")).await?;
//!
//! // Receiver, on a connection of its own
//! let receiver = db.process(TO_CLIENT.receive("session-1", dedicated)).await?;
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
use crate::redis::{Name, RedisSource, deserialize_error, millis, serialize_error, tagged_pair};
use futures_util::Stream;
use kanau::message::{MessageDe, MessageSer};
use redis::RedisResult;
use redis::aio::ConnectionLike;
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::time::Duration;

/// Names `N` and messages `B` of a pipe, without owning either.
type Items<N, B> = PhantomData<fn(&N) -> B>;

/// Pipes of messages `B`, named by `N`.
///
/// Each name is two lists in one cluster hash slot: the messages, and a
/// list holding a marker once the sender closed the pipe. The receiver
/// blocks on both, taking messages first, so it sees the close after every
/// message sent before it.
///
/// Every send and the close restart the TTL of what they write, so a pipe
/// whose sender stopped without closing it disappears one TTL later, and a
/// receiver that gets nothing for a whole TTL fails instead of waiting
/// forever. A sender that may stay quiet for longer must send a heartbeat.
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
        PipeSend {
            messages: self.keys(name).0,
            ttl: self.ttl,
            body,
            items: PhantomData,
        }
    }

    /// Close `name`: its receiver ends after taking the messages sent
    /// before.
    pub fn close(&self, name: &N) -> PipeClose<N, B> {
        PipeClose {
            close: self.keys(name).1,
            ttl: self.ttl,
            items: PhantomData,
        }
    }

    /// Receive the messages of `name` on `conn`, a connection used for
    /// nothing else (see [following changes](super#following-changes)).
    ///
    /// Taking a message removes it, so a pipe must have one receiver at a
    /// time.
    pub fn receive<D>(&self, name: &N, conn: D) -> PipeReceive<N, B, D> {
        let (messages, close) = self.keys(name);
        PipeReceive {
            messages,
            close,
            ttl: self.ttl,
            conn,
            items: PhantomData,
        }
    }

    /// The message and close keys of `name`.
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

/// Push `value` to the list `key` and restart its TTL, atomically.
async fn push(
    key: &[u8],
    value: &[u8],
    ttl: Duration,
    conn: &mut (impl ConnectionLike + Send),
) -> RedisResult<()> {
    redis::pipe()
        .atomic()
        .cmd("RPUSH")
        .arg(key)
        .arg(value)
        .ignore()
        .cmd("PEXPIRE")
        .arg(key)
        .arg(millis(ttl))
        .ignore()
        .exec_async(conn)
        .await
}

/// Query from [`Pipe::send`].
pub struct PipeSend<N: ?Sized, B> {
    messages: Vec<u8>,
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

    async fn execute(self, conn: &mut C) -> RedisResult<()> {
        let body = self.body.to_bytes().map_err(serialize_error)?;
        push(&self.messages, &body, self.ttl, conn).await
    }
}

/// Query from [`Pipe::close`].
pub struct PipeClose<N: ?Sized, B> {
    close: Vec<u8>,
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

    async fn execute(self, conn: &mut C) -> RedisResult<()> {
        push(&self.close, b"", self.ttl, conn).await
    }
}

/// Query from [`Pipe::receive`]. Taking messages removes them, so it is a
/// write.
pub struct PipeReceive<N: ?Sized, B, D> {
    messages: Vec<u8>,
    close: Vec<u8>,
    ttl: Duration,
    conn: D,
    items: Items<N, B>,
}

impl<N: ?Sized, B, D: Send> Query for PipeReceive<N, B, D> {
    type Effect = Write<Pipe<N, B>>;
}

impl<N, B, C, D> Execute<RedisSource<C>> for PipeReceive<N, B, D>
where
    N: ?Sized,
    B: MessageDe,
    C: ConnectionLike + Clone + Send + Sync,
    D: ConnectionLike + Send,
{
    type Output = PipeReceiver<B, D>;

    async fn execute(self, _: &mut C) -> RedisResult<PipeReceiver<B, D>> {
        Ok(PipeReceiver {
            conn: self.conn,
            messages: self.messages,
            close: self.close,
            ttl: self.ttl,
            closed: false,
            bodies: PhantomData,
        })
    }
}

/// The receiving end of a [`Pipe`], from [`Pipe::receive`].
pub struct PipeReceiver<B, D> {
    conn: D,
    messages: Vec<u8>,
    close: Vec<u8>,
    ttl: Duration,
    closed: bool,
    bodies: PhantomData<fn() -> B>,
}

impl<B: MessageDe, D: ConnectionLike + Send> PipeReceiver<B, D> {
    /// Take the next message, waiting for it. `None` once the sender closed
    /// the pipe.
    ///
    /// Fails if nothing arrives for a whole TTL: the pipe is then
    /// abandoned. A failed read may be retried.
    pub async fn recv(&mut self) -> RedisResult<Option<B>> {
        if self.closed {
            return Ok(None);
        }
        // Fractional timeouts need Redis 6.
        let timeout = Duration::from_millis(millis(self.ttl)).as_secs_f64();
        let popped: Option<(Vec<u8>, Vec<u8>)> = redis::cmd("BLPOP")
            .arg(&self.messages)
            .arg(&self.close)
            .arg(timeout)
            .query_async(&mut self.conn)
            .await?;
        let Some((key, body)) = popped else {
            return Err(gone("the pipe received nothing for its whole TTL"));
        };
        if key == self.close {
            self.closed = true;
            return Ok(None);
        }
        B::from_bytes(&body).map(Some).map_err(deserialize_error)
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

impl<B, D> Debug for PipeReceiver<B, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PipeReceiver")
            .field("messages", &String::from_utf8_lossy(&self.messages))
            .field("ttl", &self.ttl)
            .field("closed", &self.closed)
            .finish_non_exhaustive()
    }
}
