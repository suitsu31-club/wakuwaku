//! Keeping an index in sync with its source of truth.
//!
//! An index holds a projection of data that lives in another data source.
//! Keeping the two in sync goes wrong in four ways:
//!
//! | Problem | Cause | Remedy | Here |
//! |---|---|---|---|
//! | Silent failure | Writes only enqueue a task; it fails later | Wait for the task | [`Indexer`] waits by default |
//! | Stale overwrite | Two updates of one document race; the older lands last | Reload the latest state by key, one worker per key | [`Indexer`] reloads; partition your delivery by key |
//! | Lost update | A crash or an outage between the commit and the index write | A durable log of changes (outbox, CDC) | Your delivery |
//! | Drift | Bugs, missed changes, changed settings | Rebuild the index from scratch | [`rebuild()`] |
//!
//! This module provides the pieces that are the same everywhere. How
//! changes travel from the source to the [`Indexer`] depends on your
//! infrastructure and stays yours.
//!
//! # Pieces
//!
//! - A [`Projection`] says how to load rows of the source by key and turn
//!   each into a document, or into nothing if the row shouldn't be
//!   searchable.
//! - The [`Indexer`] is a [`Processor`](kanau::processor::Processor) of
//!   anything that converts into [`Reindex`]: it reloads the given keys
//!   from the source, writes their documents, and deletes the documents of
//!   keys that are gone or project to nothing. It never trusts the content
//!   of a change, only its keys, so a change delivered twice or late does
//!   no harm.
//! - [`rebuild()`] fills a fresh copy of the index from a [`Scan`] of the
//!   whole source, then swaps it with the live index.
//!
//! # Delivering changes
//!
//! Anything that can call a processor works:
//!
//! - **A write hook.** Wrap the source's handle in
//!   [`AfterWrite`](crate::AfterWrite) with the indexer as its hook, and
//!   convert the query's change into a [`Reindex`]. For sqlx transactions,
//!   hook `sqlx::transaction::hook::AfterTransaction` instead. Simple, but
//!   a crash after the commit loses the update. Use
//!   [`Indexer::without_waiting`] so requests don't wait for Meilisearch.
//! - **A durable log.** Write an outbox row in the same transaction as the
//!   change, relay it through a queue, and consume it with the indexer.
//!   Partition the queue by key so that one key is reindexed by one worker
//!   at a time, and acknowledge a message only once the indexer
//!   succeeded.
//!
//! Either way, rebuild periodically: it repairs whatever the delivery
//! missed. Starting with a hook and a nightly rebuild is reasonable; the
//! index is then at most a day off after a failure.

pub mod hook;
pub mod rebuild;

pub use hook::{Indexer, Reindexed};
pub use rebuild::{RebuildOptions, Rebuilt, rebuild};

use crate::meilisearch::SearchIndex;
use crate::query::Query;
use std::fmt::{self, Debug, Formatter};

/// The key of the documents of the index `I`.
pub type Key<I> = <I as SearchIndex>::Key;

/// How the rows of a source become the documents of an index.
pub trait Projection: Send + Sync {
    /// The index the documents go to.
    type Index: SearchIndex;

    /// A row of the source, as [`Load`](Self::Load) outputs it.
    type Row: Send;

    /// The query loading rows by key.
    type Load: Query;

    /// A query loading the rows of `keys`, in any order. Keys without a
    /// row are left out of its output.
    fn load(&self, keys: &[Key<Self::Index>]) -> Self::Load;

    /// The key of the document `row` projects to.
    fn key(&self, row: &Self::Row) -> Key<Self::Index>;

    /// The document of `row`, or `None` if it shouldn't be searchable (a
    /// draft, a soft-deleted row, …). A `None` deletes the document.
    fn project(&self, row: Self::Row) -> Option<<Self::Index as SearchIndex>::Document>;
}

/// A [`Projection`] whose source can be read in full, page by page.
pub trait Scan: Projection {
    /// The query loading one page.
    type Page: Query;

    /// A query loading the rows whose key comes after `after` (every row if
    /// `None`), in ascending key order, at most `limit` of them.
    ///
    /// The order must be strict and stable: [`rebuild()`] continues after
    /// the key of the last row of each page, and stops at the first page
    /// with fewer than `limit` rows.
    fn page(&self, after: Option<&Key<Self::Index>>, limit: usize) -> Self::Page;
}

/// The documents of these keys may have changed: reload them.
pub struct Reindex<I: SearchIndex> {
    keys: Vec<I::Key>,
}

impl<I: SearchIndex> Reindex<I> {
    /// Reindex `keys`. Duplicates are reindexed once.
    pub const fn new(keys: Vec<I::Key>) -> Self {
        Self { keys }
    }

    /// Reindex `key`.
    pub fn one(key: I::Key) -> Self {
        Self { keys: vec![key] }
    }

    /// The keys to reindex.
    pub fn keys(&self) -> &[I::Key] {
        &self.keys
    }

    /// Take the keys back.
    pub fn into_keys(self) -> Vec<I::Key> {
        self.keys
    }
}

impl<I: SearchIndex> Clone for Reindex<I> {
    fn clone(&self) -> Self {
        Self {
            keys: self.keys.clone(),
        }
    }
}

impl<I: SearchIndex> Debug for Reindex<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reindex")
            .field("index", &I::UID)
            .field("keys", &self.keys)
            .finish()
    }
}

/// Why syncing an index failed. `E` is the error of the source.
#[derive(Debug, thiserror::Error)]
pub enum SyncError<E> {
    /// Loading rows from the source failed.
    #[error("loading from the source failed: {0}")]
    Source(E),
    /// Meilisearch rejected a request, or a task failed or timed out.
    #[error(transparent)]
    Meilisearch(#[from] meilisearch_sdk::errors::Error),
    /// A page of a [`Scan`] ended at the key the previous page ended at,
    /// so the scan would never finish.
    #[error("the scan returned the same page twice, after key {0}")]
    StuckScan(String),
}
