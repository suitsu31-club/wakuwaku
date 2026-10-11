//! Durable bodies with a linear history of diffs, followed as they change.
//!
//! A live view holds a body `B` at a version, starting at 1. Each
//! [commit](LiveView::commit) names the version it builds on and moves the
//! view to the next one, so writers that race see a conflict instead of
//! overwriting each other. The view keeps the latest body, read in O(1),
//! and its history: a base body followed by the diffs that lead to the
//! latest one. Once the history holds more diffs than the view's bound, the
//! next commit makes the latest body the new base and drops the diffs.
//!
//! Subscribers follow the view as [events](LiveViewEvent): a diff per
//! commit, or the whole body when the diffs they missed were squashed into
//! a new base.
//!
//! ```no_run
//! use futures_util::StreamExt;
//! use kanau::message::{DeserializeError, MessageDe, MessageSer, SerializeError};
//! use kanau::processor::Processor;
//! use redis::aio::MultiplexedConnection;
//! use wakuwaku_entity::redis::RedisSource;
//! use wakuwaku_entity::redis::dynamic::live_view::{Commit, LiveView, LiveViewDiff};
//! use wakuwaku_entity::{Db, ReadWrite};
//!
//! /// A document as a list of lines.
//! #[derive(Clone)]
//! struct Lines(Vec<String>);
//! /// Lines appended to a document.
//! struct Append(Vec<String>);
//!
//! impl LiveViewDiff for Lines {
//!     type Diff = Append;
//!     fn apply(&mut self, diff: Append) {
//!         self.0.extend(diff.0);
//!     }
//! }
//! # impl MessageSer for Lines {
//! #     type SerError = SerializeError;
//! #     fn to_bytes(self) -> Result<Box<[u8]>, SerializeError> { Ok(self.0.join("\n").into_bytes().into()) }
//! # }
//! # impl MessageDe for Lines {
//! #     type DeError = DeserializeError;
//! #     fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError> {
//! #         Ok(Self(String::from_utf8_lossy(bytes).lines().map(str::to_owned).collect()))
//! #     }
//! # }
//! # impl MessageSer for Append {
//! #     type SerError = SerializeError;
//! #     fn to_bytes(self) -> Result<Box<[u8]>, SerializeError> { Lines(self.0).to_bytes() }
//! # }
//! # impl MessageDe for Append {
//! #     type DeError = DeserializeError;
//! #     fn from_bytes(bytes: &[u8]) -> Result<Self, DeserializeError> { Lines::from_bytes(bytes).map(|l| Self(l.0)) }
//! # }
//!
//! const DOCUMENTS: LiveView<u64, Lines> = LiveView::new("document", 100);
//!
//! # async fn run(
//! #     db: Db<RedisSource, ReadWrite>,
//! #     dedicated: MultiplexedConnection,
//! # ) -> redis::RedisResult<()> {
//! // Writer
//! db.process(DOCUMENTS.create(&7, Lines(vec!["title".into()]))).await?;
//! let mut latest = db.process(DOCUMENTS.get(&7)).await?.expect("just created");
//! let diff = vec!["first line".to_owned()];
//! let mut next = latest.body.clone();
//! next.apply(Append(diff.clone()));
//! match db.process(DOCUMENTS.commit(&7, latest.version, Append(diff), next)).await? {
//!     Commit::Committed { version } => println!("now at {version}"),
//!     Commit::Conflict { head } => println!("someone else committed {head} first"),
//!     Commit::Absent => println!("removed meanwhile"),
//! }
//!
//! // Subscriber, on a connection of its own
//! if let Some((mut view, subscription)) = db.process(DOCUMENTS.subscribe(&7, dedicated)).await? {
//!     let mut events = std::pin::pin!(subscription.into_stream());
//!     while let Some(event) = events.next().await {
//!         view.update(event?);
//!         println!("{} lines at version {}", view.body.0.len(), view.version);
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use super::{Entry, EntryId, StreamCursor, parse_range};
use crate::effect::io::{Read, Write};
use crate::effect::markers::Entity;
use crate::query::{Execute, Query};
use crate::redis::{Name, RedisSource, corrupted, deserialize_error, serialize_error, tagged_pair};
use futures_util::Stream;
use kanau::message::{MessageDe, MessageSer};
use redis::aio::ConnectionLike;
use redis::{RedisResult, Script};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::LazyLock;

/// A body that changes by diffs, so it can be a [`LiveView`].
pub trait LiveViewDiff: MessageSer + MessageDe {
    /// The change from one version of the body to the next.
    type Diff: MessageSer + MessageDe;

    /// Turn this version of the body into the next one.
    fn apply(&mut self, diff: Self::Diff);
}

/// Names `N` and bodies `B` of a live view, without owning either.
type Items<N, B> = PhantomData<fn(&N) -> B>;

/// Field of the head holding its version.
const VERSION_FIELD: &str = "v";
/// Field of the head holding its body.
const BODY_FIELD: &str = "b";
/// Field of a history entry holding the body at its version, in the base.
const BASE_ENTRY: &[u8] = b"base";
/// Field of a history entry holding the diff to its version from the
/// previous one; every entry but the first base has one.
const DIFF_ENTRY: &[u8] = b"diff";

/// Creates the view with head `KEYS[1]` and history `KEYS[2]` at version 1
/// with the body `ARGV[1]`, unless it exists. Returns whether it created it.
static CREATE: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
if redis.call('EXISTS', KEYS[1]) == 1 then return 0 end
redis.call('HSET', KEYS[1], 'v', '1', 'b', ARGV[1])
redis.call('DEL', KEYS[2])
redis.call('XADD', KEYS[2], '1-0', 'base', ARGV[1])
return 1
",
    )
});

/// If the head `KEYS[1]` is at version `ARGV[1]`, moves it to the next
/// version with the body `ARGV[3]`, and adds the diff `ARGV[2]` to the
/// history `KEYS[2]`. If the history already holds `ARGV[4]` diffs, it is
/// replaced by a base entry holding both the body and the diff, so a
/// subscriber at the previous version still reads a diff. Returns `{0}` if
/// the view is absent, `{1, head}` on a conflict, `{2, version}` once
/// committed.
static COMMIT: LazyLock<Script> = LazyLock::new(|| {
    Script::new(
        r"
local head = redis.call('HGET', KEYS[1], 'v')
if not head then return {0} end
if head ~= ARGV[1] then return {1, tonumber(head)} end
local version = string.format('%d', tonumber(head) + 1)
redis.call('HSET', KEYS[1], 'v', version, 'b', ARGV[3])
if redis.call('XLEN', KEYS[2]) > tonumber(ARGV[4]) then
  redis.call('XTRIM', KEYS[2], 'MAXLEN', 0)
  redis.call('XADD', KEYS[2], version .. '-0', 'base', ARGV[3], 'diff', ARGV[2])
else
  redis.call('XADD', KEYS[2], version .. '-0', 'diff', ARGV[2])
end
return {2, tonumber(version)}
",
    )
});

/// Durable bodies `B` with a linear history of diffs, named by `N`.
///
/// Each name is two keys in one cluster hash slot: a hash holding the
/// latest version and body (the head), and a Redis stream holding the
/// history. The history starts with a base entry, the body at its
/// version, followed by one entry per later version holding its diff. Each
/// entry has the ID `<version>-0`. A base made by squashing also holds the
/// diff from the previous version, so subscribers that are up to date keep
/// receiving diffs.
///
/// Live views don't expire. Removing one doesn't notify its subscribers,
/// which keep waiting; recreating it restarts the versions at 1, which
/// subscribers past version 1 don't see. Use a new name instead of
/// recreating a view.
pub struct LiveView<N: ?Sized, B> {
    namespace: &'static str,
    max_diffs: usize,
    items: Items<N, B>,
}

impl<N: ?Sized, B> LiveView<N, B> {
    /// The live views in `namespace`, whose history holds at most
    /// `max_diffs` diffs after its base.
    pub const fn new(namespace: &'static str, max_diffs: usize) -> Self {
        Self {
            namespace,
            max_diffs,
            items: PhantomData,
        }
    }

    /// Namespace of the views.
    pub const fn namespace(&self) -> &'static str {
        self.namespace
    }

    /// How many diffs the history of a view holds at most.
    pub const fn max_diffs(&self) -> usize {
        self.max_diffs
    }
}

impl<N: Name + ?Sized, B: LiveViewDiff> LiveView<N, B> {
    /// Create `name` at version 1 with `body`. Outputs `false`, changing
    /// nothing, if it exists.
    pub fn create(&self, name: &N, body: B) -> LiveViewCreate<N, B> {
        let (head, log) = self.keys(name);
        LiveViewCreate {
            head,
            log,
            body,
            items: PhantomData,
        }
    }

    /// Read the latest version of `name` and its body, `None` if it is
    /// absent.
    pub fn get(&self, name: &N) -> LiveViewGet<N, B> {
        LiveViewGet {
            head: self.keys(name).0,
            items: PhantomData,
        }
    }

    /// Read only the latest version of `name`.
    pub fn version(&self, name: &N) -> LiveViewVersion<N, B> {
        LiveViewVersion {
            head: self.keys(name).0,
            items: PhantomData,
        }
    }

    /// Read the history of `name`, `None` if it is absent.
    pub fn history(&self, name: &N) -> LiveViewHistory<N, B> {
        LiveViewHistory {
            log: self.keys(name).1,
            items: PhantomData,
        }
    }

    /// Move `name` from version `parent` to the next one, whose body is
    /// `body`, through `diff`.
    ///
    /// `body` must be the body at `parent` with `diff` applied. Nothing
    /// checks it: subscribers apply `diff`, readers of the latest version
    /// get `body`, and they disagree if the two don't match.
    pub fn commit(&self, name: &N, parent: u64, diff: B::Diff, body: B) -> LiveViewCommit<N, B> {
        let (head, log) = self.keys(name);
        LiveViewCommit {
            head,
            log,
            parent,
            max_diffs: self.max_diffs,
            diff,
            body,
            names: PhantomData,
        }
    }

    /// Follow `name` using `conn`, a connection used for nothing else (see
    /// [following changes](super#following-changes)). Outputs the latest
    /// version and a subscription to the versions after it, `None` if the
    /// view is absent.
    pub fn subscribe<D>(&self, name: &N, conn: D) -> LiveViewSubscribe<N, B, D> {
        let (head, log) = self.keys(name);
        LiveViewSubscribe {
            head,
            log,
            conn,
            items: PhantomData,
        }
    }

    /// Follow `name` from `version` on, using `conn`, a connection used for
    /// nothing else. The subscription outputs the versions after `version`,
    /// starting with a [snapshot](LiveViewEvent::Snapshot) if the diffs
    /// right after `version` were squashed into a new base.
    pub fn resume<D>(&self, name: &N, version: u64, conn: D) -> LiveViewResume<N, B, D> {
        LiveViewResume {
            log: self.keys(name).1,
            version,
            conn,
            items: PhantomData,
        }
    }

    /// Delete `name` and its history. Outputs whether it was present.
    pub fn remove(&self, name: &N) -> LiveViewRemove<N, B> {
        let (head, log) = self.keys(name);
        LiveViewRemove {
            head,
            log,
            items: PhantomData,
        }
    }

    /// The head and history keys of `name`.
    fn keys(&self, name: &N) -> (Vec<u8>, Vec<u8>) {
        tagged_pair("LiveView", self.namespace, name, b":h", b":l")
    }
}

impl<N: ?Sized, B> Clone for LiveView<N, B> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<N: ?Sized, B> Copy for LiveView<N, B> {}

impl<N: ?Sized, B> Debug for LiveView<N, B> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveView")
            .field("namespace", &self.namespace)
            .field("max_diffs", &self.max_diffs)
            .finish()
    }
}

impl<N: ?Sized, B> Entity for LiveView<N, B> {}

/// A body at a version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<B> {
    /// The version, from 1.
    pub version: u64,
    /// The body at that version.
    pub body: B,
}

impl<B: LiveViewDiff> Versioned<B> {
    /// Move to the version of `event`, which must come right after this
    /// one, as the events of a subscription do.
    pub fn update(&mut self, event: LiveViewEvent<B>) {
        match event {
            LiveViewEvent::Diff { version, diff } => {
                self.body.apply(diff);
                self.version = version;
            }
            LiveViewEvent::Snapshot(snapshot) => *self = snapshot,
        }
    }
}

/// A change of a [`LiveView`] seen by a subscription.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LiveViewEvent<B: LiveViewDiff> {
    /// The previous version became `version` through `diff`.
    Diff {
        /// The new version.
        version: u64,
        /// The change from the previous version.
        diff: B::Diff,
    },
    /// The whole body at a version, sent instead of the diffs leading to it
    /// when they were squashed into a new base before the subscription read
    /// them.
    Snapshot(Versioned<B>),
}

/// The history of a [`LiveView`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct History<B: LiveViewDiff> {
    /// The oldest version kept.
    pub base: Versioned<B>,
    /// The diffs leading from the base to the latest version: the diff at
    /// index `i` makes version `base.version + i + 1`.
    pub diffs: Vec<B::Diff>,
}

impl<B: LiveViewDiff> History<B> {
    /// Apply the diffs to the base.
    pub fn into_latest(self) -> Versioned<B> {
        let mut latest = self.base;
        for diff in self.diffs {
            latest.body.apply(diff);
            latest.version = latest.version.saturating_add(1);
        }
        latest
    }
}

/// Output of [`LiveView::commit`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Commit {
    /// The view moved to `version`.
    Committed {
        /// The new version.
        version: u64,
    },
    /// The view was not at the parent version; nothing changed.
    Conflict {
        /// The latest version.
        head: u64,
    },
    /// The view doesn't exist; nothing changed.
    Absent,
}

fn parse_body<B: MessageDe>(bytes: &[u8]) -> RedisResult<B> {
    B::from_bytes(bytes).map_err(deserialize_error)
}

/// The version of a history entry, from its ID `<version>-0`.
fn entry_version(entry: &Entry) -> RedisResult<u64> {
    if entry.id.seq != 0 {
        return Err(corrupted("live view history entry ID is not <version>-0"));
    }
    Ok(entry.id.ms)
}

/// The event a history entry stands for, seen from version `current`: its
/// diff if it follows `current`, its base body otherwise.
fn parse_event<B: LiveViewDiff>(entry: &Entry, current: u64) -> RedisResult<LiveViewEvent<B>> {
    let version = entry_version(entry)?;
    if Some(version) == current.checked_add(1)
        && let Some(diff) = entry.get(DIFF_ENTRY)
    {
        let diff = B::Diff::from_bytes(diff).map_err(deserialize_error)?;
        return Ok(LiveViewEvent::Diff { version, diff });
    }
    let Some(body) = entry.get(BASE_ENTRY) else {
        return Err(corrupted(
            "live view diff doesn't follow the previous version",
        ));
    };
    Ok(LiveViewEvent::Snapshot(Versioned {
        version,
        body: parse_body(body)?,
    }))
}

/// Query from [`LiveView::create`].
pub struct LiveViewCreate<N: ?Sized, B> {
    head: Vec<u8>,
    log: Vec<u8>,
    body: B,
    items: Items<N, B>,
}

impl<N: ?Sized, B: Send> Query for LiveViewCreate<N, B> {
    type Effect = Write<LiveView<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for LiveViewCreate<N, B>
where
    N: ?Sized,
    B: LiveViewDiff + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        let body = self.body.to_bytes().map_err(serialize_error)?;
        CREATE
            .key(&self.head)
            .key(&self.log)
            .arg(&*body)
            .invoke_async(conn)
            .await
    }
}

/// Read the head `key`.
async fn read_head<B: MessageDe>(
    head: &[u8],
    conn: &mut (impl ConnectionLike + Send),
) -> RedisResult<Option<Versioned<B>>> {
    let (version, body): (Option<u64>, Option<Vec<u8>>) = redis::cmd("HMGET")
        .arg(head)
        .arg(VERSION_FIELD)
        .arg(BODY_FIELD)
        .query_async(conn)
        .await?;
    match (version, body) {
        (Some(version), Some(body)) => Ok(Some(Versioned {
            version,
            body: parse_body(&body)?,
        })),
        (None, None) => Ok(None),
        _ => Err(corrupted("live view head lacks its version or body")),
    }
}

/// Query from [`LiveView::get`].
pub struct LiveViewGet<N: ?Sized, B> {
    head: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for LiveViewGet<N, B> {
    type Effect = Read<LiveView<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for LiveViewGet<N, B>
where
    N: ?Sized,
    B: LiveViewDiff + Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<Versioned<B>>;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<Versioned<B>>> {
        read_head(&self.head, conn).await
    }
}

/// Query from [`LiveView::version`].
pub struct LiveViewVersion<N: ?Sized, B> {
    head: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for LiveViewVersion<N, B> {
    type Effect = Read<LiveView<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for LiveViewVersion<N, B>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<u64>;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<u64>> {
        redis::cmd("HGET")
            .arg(&self.head)
            .arg(VERSION_FIELD)
            .query_async(conn)
            .await
    }
}

/// Query from [`LiveView::history`].
pub struct LiveViewHistory<N: ?Sized, B> {
    log: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for LiveViewHistory<N, B> {
    type Effect = Read<LiveView<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for LiveViewHistory<N, B>
where
    N: ?Sized,
    B: LiveViewDiff + Send,
    B::Diff: Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Option<History<B>>;

    async fn execute(self, conn: &mut C) -> RedisResult<Option<History<B>>> {
        let reply = redis::cmd("XRANGE")
            .arg(&self.log)
            .arg("-")
            .arg("+")
            .query_async(conn)
            .await?;
        let mut entries = parse_range(reply)?.into_iter();
        let Some(first) = entries.next() else {
            return Ok(None);
        };
        let Some(base) = first.get(BASE_ENTRY) else {
            return Err(corrupted("live view history doesn't start with a base"));
        };
        let mut history = History {
            base: Versioned {
                version: entry_version(&first)?,
                body: parse_body(base)?,
            },
            diffs: Vec::with_capacity(entries.len()),
        };
        let mut version = history.base.version;
        for entry in entries {
            let LiveViewEvent::Diff {
                version: next,
                diff,
            } = parse_event::<B>(&entry, version)?
            else {
                return Err(corrupted("live view history has a base after its start"));
            };
            version = next;
            history.diffs.push(diff);
        }
        Ok(Some(history))
    }
}

/// Query from [`LiveView::commit`].
pub struct LiveViewCommit<N: ?Sized, B: LiveViewDiff> {
    head: Vec<u8>,
    log: Vec<u8>,
    parent: u64,
    max_diffs: usize,
    diff: B::Diff,
    body: B,
    names: PhantomData<fn(&N)>,
}

impl<N: ?Sized, B> Query for LiveViewCommit<N, B>
where
    B: LiveViewDiff + Send,
    B::Diff: Send,
{
    type Effect = Write<LiveView<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for LiveViewCommit<N, B>
where
    N: ?Sized,
    B: LiveViewDiff + Send,
    B::Diff: Send,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = Commit;

    async fn execute(self, conn: &mut C) -> RedisResult<Commit> {
        let diff = self.diff.to_bytes().map_err(serialize_error)?;
        let body = self.body.to_bytes().map_err(serialize_error)?;
        let reply: Vec<u64> = COMMIT
            .key(&self.head)
            .key(&self.log)
            .arg(self.parent)
            .arg(&*diff)
            .arg(&*body)
            .arg(self.max_diffs)
            .invoke_async(conn)
            .await?;
        match reply.as_slice() {
            [0] => Ok(Commit::Absent),
            [1, head] => Ok(Commit::Conflict { head: *head }),
            [2, version] => Ok(Commit::Committed { version: *version }),
            _ => Err(corrupted("unexpected live view commit reply")),
        }
    }
}

/// Query from [`LiveView::subscribe`].
pub struct LiveViewSubscribe<N: ?Sized, B, D> {
    head: Vec<u8>,
    log: Vec<u8>,
    conn: D,
    items: Items<N, B>,
}

impl<N: ?Sized, B, D: Send> Query for LiveViewSubscribe<N, B, D> {
    type Effect = Read<LiveView<N, B>>;
}

impl<N, B, C, D> Execute<RedisSource<C>> for LiveViewSubscribe<N, B, D>
where
    N: ?Sized,
    B: LiveViewDiff + Send,
    C: ConnectionLike + Clone + Send + Sync,
    D: ConnectionLike + Send,
{
    type Output = Option<(Versioned<B>, LiveViewSubscription<B, D>)>;

    async fn execute(self, conn: &mut C) -> RedisResult<Self::Output> {
        let Some(latest) = read_head::<B>(&self.head, conn).await? else {
            return Ok(None);
        };
        let subscription = LiveViewSubscription::new(self.conn, self.log, latest.version);
        Ok(Some((latest, subscription)))
    }
}

/// Query from [`LiveView::resume`].
pub struct LiveViewResume<N: ?Sized, B, D> {
    log: Vec<u8>,
    version: u64,
    conn: D,
    items: Items<N, B>,
}

impl<N: ?Sized, B, D: Send> Query for LiveViewResume<N, B, D> {
    type Effect = Read<LiveView<N, B>>;
}

impl<N, B, C, D> Execute<RedisSource<C>> for LiveViewResume<N, B, D>
where
    N: ?Sized,
    B: LiveViewDiff,
    C: ConnectionLike + Clone + Send + Sync,
    D: ConnectionLike + Send,
{
    type Output = LiveViewSubscription<B, D>;

    async fn execute(self, _: &mut C) -> RedisResult<LiveViewSubscription<B, D>> {
        Ok(LiveViewSubscription::new(self.conn, self.log, self.version))
    }
}

/// A subscription to a [`LiveView`], from [`LiveView::subscribe`] or
/// [`LiveView::resume`].
pub struct LiveViewSubscription<B, D> {
    cursor: StreamCursor<D>,
    version: u64,
    bodies: PhantomData<fn() -> B>,
}

impl<B, D> LiveViewSubscription<B, D> {
    fn new(conn: D, log: Vec<u8>, version: u64) -> Self {
        Self {
            cursor: StreamCursor::new(
                conn,
                log,
                EntryId {
                    ms: version,
                    seq: 0,
                },
            ),
            version,
            bodies: PhantomData,
        }
    }

    /// The version of the last event, or the one the subscription started
    /// at. Resume from it after a failure.
    pub const fn version(&self) -> u64 {
        self.version
    }
}

impl<B: LiveViewDiff, D: ConnectionLike + Send> LiveViewSubscription<B, D> {
    /// The next change, waiting for it. On failure, resume from
    /// [`version`](Self::version).
    pub async fn recv(&mut self) -> RedisResult<LiveViewEvent<B>> {
        let entry = loop {
            if let Some(entry) = self.cursor.next(0).await? {
                break entry;
            }
        };
        let event = parse_event::<B>(&entry, self.version)?;
        self.version = match &event {
            LiveViewEvent::Diff { version, .. } => *version,
            LiveViewEvent::Snapshot(snapshot) => snapshot.version,
        };
        Ok(event)
    }

    /// The changes as a stream. It never ends; drop it to unsubscribe.
    pub fn into_stream(self) -> impl Stream<Item = RedisResult<LiveViewEvent<B>>> {
        futures_util::stream::unfold(self, |mut subscription| async move {
            let event = subscription.recv().await;
            Some((event, subscription))
        })
    }
}

impl<B, D> Debug for LiveViewSubscription<B, D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("LiveViewSubscription")
            .field("log", &String::from_utf8_lossy(&self.cursor.key))
            .field("version", &self.version)
            .finish_non_exhaustive()
    }
}

/// Query from [`LiveView::remove`].
pub struct LiveViewRemove<N: ?Sized, B> {
    head: Vec<u8>,
    log: Vec<u8>,
    items: Items<N, B>,
}

impl<N: ?Sized, B> Query for LiveViewRemove<N, B> {
    type Effect = Write<LiveView<N, B>>;
}

impl<N, B, C> Execute<RedisSource<C>> for LiveViewRemove<N, B>
where
    N: ?Sized,
    C: ConnectionLike + Clone + Send + Sync,
{
    type Output = bool;

    async fn execute(self, conn: &mut C) -> RedisResult<bool> {
        let removed: u64 = redis::cmd("UNLINK")
            .arg(&self.head)
            .arg(&self.log)
            .query_async(conn)
            .await?;
        Ok(removed > 0)
    }
}
