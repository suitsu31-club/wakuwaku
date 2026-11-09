//! Typed data structures in Redis that can be waited on as they change.
//!
//! - [`Promise`](promise::Promise): a value completed once, waited on by
//!   anyone; a distributed oneshot.
//! - [`Watch`](watch::Watch): the latest version of a value, waited on for
//!   newer ones; a distributed watch channel.
//! - [`Pipe`](pipe::Pipe): messages from one sender to one receiver, each
//!   taken by the receiver once.
//! - [`LiveView`](live_view::LiveView): a durable body with a linear
//!   history of diffs.
//! - [`StreamBuffer`](stream_buffer::StreamBuffer): an append-only array of
//!   bodies with an end.
//!
//! Like the [collections](super::collection), each structure is a `Copy`
//! value whose methods build [queries](crate::Query), and is the
//! [`Entity`](crate::Entity) they read and write.
//!
//! # Waiting on changes
//!
//! Waiting on a promise or a watch, receiving from a pipe and subscribing
//! to a live view or a stream buffer take the process's
//! [`WatchHub`](super::watch_hub::WatchHub). The wait reads the item with
//! plain commands on the [`RedisSource`](super::RedisSource) connection, and the
//! hub wakes it whenever a writer changes the item, so any number of waits
//! share the hub's single connection:
//!
//! ```no_run
//! use wakuwaku_entity::redis::watch_hub::WatchHub;
//!
//! # async fn run(client: redis::Client) -> redis::RedisResult<()> {
//! let hub = WatchHub::connect(client).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Receivers and subscriptions have an `async fn recv` and an
//! `into_stream` that turns them into a [`Stream`](futures_util::Stream).
//! Waits read what they missed before they sleep, so they never skip an
//! item.
//!
//! See the [Redis guide](https://www.wakuwaku.rs/en/docs/data/redis#following-changes-the-watch-hub).

pub mod live_view;
pub mod pipe;
pub mod promise;
pub mod stream_buffer;
pub mod watch;

use super::{corrupted, deserialize_error};
use kanau::message::MessageDe;
use redis::aio::ConnectionLike;
use redis::{ErrorKind, RedisError, RedisResult, Value};
use std::collections::VecDeque;
use std::fmt::{self, Display, Formatter};

/// An error for a receiver or subscription whose structure stopped existing
/// before its end.
fn gone(detail: &'static str) -> RedisError {
    RedisError::from((ErrorKind::Client, "structure gone", detail.to_owned()))
}

/// Entries read by one `XRANGE` of a [`StreamCursor`].
const READ_BATCH: usize = 128;

/// ID of a stream entry. Both structures write explicit IDs, so each part
/// carries meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct EntryId {
    ms: u64,
    seq: u64,
}

impl EntryId {
    fn parse(bytes: &[u8]) -> RedisResult<Self> {
        let parsed = std::str::from_utf8(bytes)
            .ok()
            .and_then(|id| id.split_once('-'))
            .and_then(|(ms, seq)| {
                Some(Self {
                    ms: ms.parse().ok()?,
                    seq: seq.parse().ok()?,
                })
            });
        parsed.ok_or_else(|| corrupted("malformed stream entry ID"))
    }

    /// The smallest ID after this one, `None` after the largest.
    fn successor(self) -> Option<Self> {
        match self.seq.checked_add(1) {
            Some(seq) => Some(Self { ms: self.ms, seq }),
            None => Some(Self {
                ms: self.ms.checked_add(1)?,
                seq: 0,
            }),
        }
    }
}

impl Display for EntryId {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// A stream entry: its ID and its fields as the reply holds them, field
/// names and values alternating.
#[derive(Debug, PartialEq)]
struct Entry {
    id: EntryId,
    fields: Vec<Value>,
}

impl Entry {
    /// Value of the field `name`, `None` if the entry lacks it.
    fn get(&self, name: &[u8]) -> Option<&[u8]> {
        let (pairs, _) = self.fields.as_chunks::<2>();
        pairs.iter().find_map(|pair| match pair {
            [Value::BulkString(field), Value::BulkString(value)] if field == name => {
                Some(value.as_slice())
            }
            _ => None,
        })
    }
}

/// `[id, [field, value, …]]`.
fn parse_entry(value: Value) -> RedisResult<Entry> {
    let Value::Array(parts) = value else {
        return Err(corrupted("stream entry is not an array"));
    };
    let [id, fields]: [Value; 2] = parts
        .try_into()
        .map_err(|_| corrupted("stream entry is not an ID and its fields"))?;
    let id = match &id {
        Value::BulkString(id) => EntryId::parse(id)?,
        Value::SimpleString(id) => EntryId::parse(id.as_bytes())?,
        _ => return Err(corrupted("stream entry ID is not a string")),
    };
    match fields {
        Value::Array(fields) if fields.len() % 2 == 0 => Ok(Entry { id, fields }),
        _ => Err(corrupted("stream entry fields are not name-value pairs")),
    }
}

/// Reply of `XRANGE`.
fn parse_range(value: Value) -> RedisResult<Vec<Entry>> {
    match value {
        Value::Array(entries) => entries.into_iter().map(parse_entry).collect(),
        Value::Nil => Ok(Vec::new()),
        _ => Err(corrupted("XRANGE reply is not an array")),
    }
}

/// Reads the entries of one stream after a cursor, in batches.
struct StreamCursor {
    key: Vec<u8>,
    after: EntryId,
    pending: VecDeque<Entry>,
}

impl StreamCursor {
    fn new(key: Vec<u8>, after: EntryId) -> Self {
        Self {
            key,
            after,
            pending: VecDeque::new(),
        }
    }

    /// The next entry, `None` if the stream has none after the cursor yet.
    async fn next(
        &mut self,
        conn: &mut (impl ConnectionLike + Send),
    ) -> RedisResult<Option<Entry>> {
        if let Some(entry) = self.pending.pop_front() {
            return Ok(Some(entry));
        }
        let Some(start) = self.after.successor() else {
            return Ok(None);
        };
        let reply = redis::cmd("XRANGE")
            .arg(&self.key)
            .arg(start.to_string())
            .arg("+")
            .arg("COUNT")
            .arg(READ_BATCH)
            .query_async(conn)
            .await?;
        let entries = parse_range(reply)?;
        if let Some(last) = entries.last() {
            self.after = last.id;
        }
        self.pending = entries.into();
        Ok(self.pending.pop_front())
    }
}

/// Field of a hash holding the version of a [`Versioned`] body.
const VERSION_FIELD: &str = "v";
/// Field of a hash holding the body of a [`Versioned`] body.
const BODY_FIELD: &str = "b";

/// A body at a version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<B> {
    /// The version, from 1.
    pub version: u64,
    /// The body at that version.
    pub body: B,
}

/// Read the version and body stored in the hash `key`, `None` if it is
/// absent.
async fn read_versioned<B: MessageDe>(
    key: &[u8],
    conn: &mut (impl ConnectionLike + Send),
) -> RedisResult<Option<Versioned<B>>> {
    let (version, body): (Option<u64>, Option<Vec<u8>>) = redis::cmd("HMGET")
        .arg(key)
        .arg(VERSION_FIELD)
        .arg(BODY_FIELD)
        .query_async(conn)
        .await?;
    match (version, body) {
        (Some(version), Some(body)) => Ok(Some(Versioned {
            version,
            body: B::from_bytes(&body).map_err(deserialize_error)?,
        })),
        (None, None) => Ok(None),
        _ => Err(corrupted("versioned body lacks its version or body")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(text: &str) -> Value {
        Value::BulkString(text.as_bytes().to_vec())
    }

    fn entry(id: &str, fields: &[&str]) -> Value {
        Value::Array(vec![
            bytes(id),
            Value::Array(fields.iter().map(|f| bytes(f)).collect()),
        ])
    }

    #[test]
    fn xrange_replies_parse() {
        let reply = Value::Array(vec![
            entry("3-0", &["d", "x"]),
            entry("0-12", &["b", "", "e", "y"]),
        ]);
        let parsed = parse_range(reply).unwrap_or_default();
        let ids: Vec<_> = parsed.iter().map(|entry| entry.id).collect();
        assert_eq!(ids, [EntryId { ms: 3, seq: 0 }, EntryId { ms: 0, seq: 12 }]);
        assert_eq!(parsed[0].get(b"d"), Some(&b"x"[..]));
        assert_eq!(parsed[0].get(b"x"), None);
        assert_eq!(parsed[1].get(b"b"), Some(&b""[..]));
        assert_eq!(parsed[1].get(b"e"), Some(&b"y"[..]));
        assert_eq!(parsed[1].get(b""), None);
        assert!(parse_range(Value::Nil).is_ok_and(|entries| entries.is_empty()));
    }

    #[test]
    fn entries_with_other_shapes_are_rejected() {
        assert!(parse_entry(entry("1-0", &["a", "1", "b"])).is_err());
        assert!(parse_entry(entry("1", &["a", "1"])).is_err());
        assert!(parse_entry(entry("1-x", &["a", "1"])).is_err());
    }

    #[test]
    fn successor_carries_into_the_milliseconds() {
        let id = |ms, seq| EntryId { ms, seq };
        assert_eq!(id(4, 0).successor(), Some(id(4, 1)));
        assert_eq!(id(4, u64::MAX).successor(), Some(id(5, 0)));
        assert_eq!(id(u64::MAX, u64::MAX).successor(), None);
    }
}
