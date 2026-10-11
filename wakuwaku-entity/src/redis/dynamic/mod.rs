//! Typed data structures in Redis that can be followed as they change.
//!
//! - [`Pipe`](pipe::Pipe): messages from one sender to one receiver, each
//!   taken by the receiver once. Two pipes make a duplex channel.
//! - [`LiveView`](live_view::LiveView): a durable body with a linear
//!   history of diffs. The latest version reads in O(1), and the history is
//!   squashed into a new base when it grows past a bound.
//! - [`StreamBuffer`](stream_buffer::StreamBuffer): an append-only array of
//!   bodies with an end, such as the chunks of an LLM response.
//!
//! Like the [collections](super::collection), each structure is a `Copy`
//! value whose methods build [queries](crate::Query), and is the
//! [`Entity`](crate::Entity) they read and write.
//!
//! # Following changes
//!
//! Receiving from a pipe and subscribing to a live view or a stream buffer
//! are queries too, so the capability of the [`Db`](crate::Db) is checked
//! as usual. They output a receiver or subscription that waits with blocking
//! Redis commands (`BLPOP`, `XREAD BLOCK`) on a connection of its own, passed
//! to the query. A blocking command stalls every other command sent on the
//! same socket, so don't share that connection, nor clone it from the
//! multiplexed connection of the [`RedisSource`](super::RedisSource). It must
//! also not time out responses before the block ends; for a
//! [`MultiplexedConnection`](redis::aio::MultiplexedConnection), turn the
//! response timeout off:
//!
//! ```no_run
//! use redis::AsyncConnectionConfig;
//!
//! # async fn run(client: redis::Client) -> redis::RedisResult<()> {
//! let config = AsyncConnectionConfig::new().set_response_timeout(None);
//! let dedicated = client.get_multiplexed_async_connection_with_config(&config).await?;
//! # Ok(())
//! # }
//! ```
//!
//! Each receiver and subscription has an `async fn recv` and an
//! `into_stream` that turns it into a [`Stream`](futures_util::Stream).
//!
//! Live views and stream buffers are Redis streams, read with `XREAD`; a
//! subscription catches up on what it missed before it waits, so it never
//! skips an item.

pub mod live_view;
pub mod pipe;
pub mod stream_buffer;

use super::corrupted;
use redis::aio::ConnectionLike;
use redis::{ErrorKind, RedisError, RedisResult, Value};
use std::collections::VecDeque;
use std::fmt::{self, Display, Formatter};

/// An error for a receiver or subscription whose structure stopped existing
/// before its end.
fn gone(detail: &'static str) -> RedisError {
    RedisError::from((ErrorKind::Client, "structure gone", detail.to_owned()))
}

/// Entries read by one `XREAD`.
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

/// Reply of `XREAD` on one stream, `None` if it timed out.
///
/// RESP2 replies `[[key, entries]]`, RESP3 `{key: entries}`.
fn parse_read(value: Value) -> RedisResult<Option<Vec<Entry>>> {
    let entries = match value {
        Value::Nil => return Ok(None),
        Value::Array(streams) => match streams.into_iter().next() {
            Some(Value::Array(stream)) => stream.into_iter().nth(1),
            Some(_) => return Err(corrupted("XREAD stream is not a key and its entries")),
            None => None,
        },
        Value::Map(streams) => streams.into_iter().next().map(|(_, entries)| entries),
        _ => return Err(corrupted("XREAD reply is neither nil, an array nor a map")),
    };
    entries.map_or(Ok(Some(Vec::new())), |entries| {
        parse_range(entries).map(Some)
    })
}

/// Reads the entries of one stream after a cursor, in batches, on a
/// dedicated connection.
struct StreamCursor<D> {
    conn: D,
    key: Vec<u8>,
    after: EntryId,
    pending: VecDeque<Entry>,
}

impl<D> StreamCursor<D> {
    fn new(conn: D, key: Vec<u8>, after: EntryId) -> Self {
        Self {
            conn,
            key,
            after,
            pending: VecDeque::new(),
        }
    }
}

impl<D: ConnectionLike + Send> StreamCursor<D> {
    /// The next entry, waiting up to `block_ms` (0: forever) for one to be
    /// added. `None` if the wait timed out.
    async fn next(&mut self, block_ms: u64) -> RedisResult<Option<Entry>> {
        if let Some(entry) = self.pending.pop_front() {
            return Ok(Some(entry));
        }
        let reply: Value = redis::cmd("XREAD")
            .arg("COUNT")
            .arg(READ_BATCH)
            .arg("BLOCK")
            .arg(block_ms)
            .arg("STREAMS")
            .arg(&self.key)
            .arg(self.after.to_string())
            .query_async(&mut self.conn)
            .await?;
        let Some(entries) = parse_read(reply)? else {
            return Ok(None);
        };
        if let Some(last) = entries.last() {
            self.after = last.id;
        }
        self.pending = entries.into();
        Ok(self.pending.pop_front())
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
    fn xread_replies_parse_in_both_protocols() {
        let entries = || {
            Value::Array(vec![
                entry("3-0", &["d", "x"]),
                entry("0-12", &["b", "", "e", "y"]),
            ])
        };
        let resp2 = Value::Array(vec![Value::Array(vec![bytes("key"), entries()])]);
        let resp3 = Value::Map(vec![(bytes("key"), entries())]);
        for reply in [resp2, resp3] {
            let parsed = parse_read(reply).ok().flatten().unwrap_or_default();
            let ids: Vec<_> = parsed.iter().map(|entry| entry.id).collect();
            assert_eq!(ids, [EntryId { ms: 3, seq: 0 }, EntryId { ms: 0, seq: 12 }]);
            assert_eq!(parsed[0].get(b"d"), Some(&b"x"[..]));
            assert_eq!(parsed[0].get(b"x"), None);
            assert_eq!(parsed[1].get(b"b"), Some(&b""[..]));
            assert_eq!(parsed[1].get(b"e"), Some(&b"y"[..]));
            assert_eq!(parsed[1].get(b""), None);
        }
        assert_eq!(parse_read(Value::Nil).ok(), Some(None));
    }

    #[test]
    fn entries_with_other_shapes_are_rejected() {
        assert!(parse_entry(entry("1-0", &["a", "1", "b"])).is_err());
        assert!(parse_entry(entry("1", &["a", "1"])).is_err());
        assert!(parse_entry(entry("1-x", &["a", "1"])).is_err());
    }
}
