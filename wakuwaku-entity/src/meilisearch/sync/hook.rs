//! The [`Indexer`]: reindexing documents by key.
//!
//! ```no_run
//! # use std::convert::Infallible;
//! # use kanau::processor::Processor;
//! # use wakuwaku_entity::meilisearch::sync::{Indexer, Projection, Reindex, SyncError};
//! # use wakuwaku_entity::meilisearch::{MeiliSource, NoField, SearchIndex};
//! # use wakuwaku_entity::{AfterWrite, Db, Entity, ReadOnly, ReadWrite};
//! # struct PostIndex;
//! # impl Entity for PostIndex {}
//! # impl SearchIndex for PostIndex {
//! #     const UID: &'static str = "posts";
//! #     const PRIMARY_KEY: &'static str = "id";
//! #     type Key = u64;
//! #     type Document = serde_json::Value;
//! #     type Filterable = NoField;
//! #     type Sortable = NoField;
//! # }
//! # struct PostProjection;
//! # struct LoadPosts(Vec<u64>);
//! # impl wakuwaku_entity::Query for LoadPosts { type Effect = (); }
//! # impl Projection for PostProjection {
//! #     type Index = PostIndex;
//! #     type Row = (u64, String);
//! #     type Load = LoadPosts;
//! #     fn load(&self, keys: &[u64]) -> LoadPosts { LoadPosts(keys.to_vec()) }
//! #     fn key(&self, row: &(u64, String)) -> u64 { row.0 }
//! #     fn project(&self, row: (u64, String)) -> Option<serde_json::Value> { None }
//! # }
//! # #[derive(Clone)] struct Postgres;
//! # impl Processor<LoadPosts> for Postgres {
//! #     type Output = Vec<(u64, String)>;
//! #     type Error = Infallible;
//! #     async fn process(&self, _: LoadPosts) -> Result<Vec<(u64, String)>, Infallible> { Ok(vec![]) }
//! # }
//! /// The change a write query on posts describes (`WriteQuery::Change`).
//! struct PostChanged {
//!     id: u64,
//! }
//!
//! impl From<PostChanged> for Reindex<PostIndex> {
//!     fn from(change: PostChanged) -> Self {
//!         Reindex::one(change.id)
//!     }
//! }
//!
//! struct LogError;
//! impl Processor<SyncError<Infallible>> for LogError {
//!     type Output = ();
//!     type Error = Infallible;
//!     async fn process(&self, error: SyncError<Infallible>) -> Result<(), Infallible> {
//!         eprintln!("reindexing failed: {error}");
//!         Ok(())
//!     }
//! }
//!
//! # fn wire(postgres: Postgres, meili: Db<MeiliSource, ReadWrite>) {
//! // `postgres` reads posts, `meili` writes the post index.
//! let indexer = Indexer::new(postgres.clone(), meili, PostProjection).without_waiting();
//! let posts = AfterWrite::new(postgres, indexer, LogError);
//! # }
//! ```
//!
//! See the [sync guide](https://www.wakuwaku.rs/en/docs/data/meilisearch/sync#the-indexer).

use crate::meilisearch::sync::{Key, Projection, Reindex, SyncError};
use crate::meilisearch::write::{Delete, Upsert, WaitTask};
use kanau::processor::Processor;
use meilisearch_sdk::errors::Error;
use meilisearch_sdk::task_info::TaskInfo;
use std::collections::HashSet;
use std::time::Duration;

/// How long the [`Indexer`] waits for each task unless told otherwise.
pub const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(30);

/// Reindexes documents by key: loads their rows from the source, writes
/// the documents they project to, and deletes the rest.
///
/// - `Src` reads the source: a [`Processor`] of the projection's
///   [`Load`](Projection::Load) query, such as a read-only handle on the
///   database.
/// - `M` writes the index: a processor of [`Upsert`], [`Delete`] and
///   [`WaitTask`], such as a [`Db<MeiliSource, C>`](crate::Db) whose
///   capability writes the index.
/// - `P` is the [`Projection`].
///
/// It processes anything that converts into a [`Reindex`] of the index,
/// so it can be the hook of an [`AfterWrite`](crate::AfterWrite) directly.
/// Only the keys of a change are used and documents always come from a
/// fresh load, so a change delivered twice or late does no harm. Load from
/// the primary: a lagging replica hands it the row as it was before.
///
/// By default it waits until Meilisearch has applied its writes, so a
/// success means the index is up to date with what it loaded. Without
/// waiting, a task that fails later goes unnoticed until the next
/// [`rebuild`](super::rebuild()).
///
/// # Errors
///
/// [`SyncError::Source`] if loading failed, before anything is written;
/// [`SyncError::Meilisearch`] if a write or a wait failed. Writes enqueued
/// before the failure stay enqueued.
#[derive(Debug, Clone)]
pub struct Indexer<Src, M, P> {
    source: Src,
    meili: M,
    projection: P,
    wait: Option<Duration>,
}

impl<Src, M, P> Indexer<Src, M, P> {
    /// Load rows with `source`, write documents with `meili`, and wait up
    /// to [`DEFAULT_TASK_TIMEOUT`] for each write.
    pub const fn new(source: Src, meili: M, projection: P) -> Self {
        Self {
            source,
            meili,
            projection,
            wait: Some(DEFAULT_TASK_TIMEOUT),
        }
    }

    /// Wait up to `timeout` for each write.
    #[must_use]
    pub const fn wait_timeout(mut self, timeout: Duration) -> Self {
        self.wait = Some(timeout);
        self
    }

    /// Don't wait for writes: succeed once they are enqueued.
    ///
    /// For hooks that run inline with a request. Tasks that fail are not
    /// reported.
    #[must_use]
    pub const fn without_waiting(mut self) -> Self {
        self.wait = None;
        self
    }
}

/// What one reindexing did.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Reindexed {
    /// Documents written.
    pub upserted: usize,
    /// Documents deleted, because their row is gone or projects to
    /// nothing. Includes keys that had no document.
    pub deleted: usize,
}

impl<Src, M, P, Ch> Processor<Ch> for Indexer<Src, M, P>
where
    Ch: Into<Reindex<P::Index>> + Send,
    P: Projection,
    Src: Processor<P::Load, Output = Vec<P::Row>> + Sync,
    Src::Error: Send,
    M: Processor<Upsert<P::Index>, Output = TaskInfo, Error = Error>
        + Processor<Delete<P::Index>, Output = TaskInfo, Error = Error>
        + Processor<WaitTask, Output = (), Error = Error>
        + Sync,
{
    type Output = Reindexed;
    type Error = SyncError<Src::Error>;

    async fn process(&self, change: Ch) -> Result<Reindexed, SyncError<Src::Error>> {
        let keys = change.into().into_keys();
        if keys.is_empty() {
            return Ok(Reindexed::default());
        }
        let rows = self
            .source
            .process(self.projection.load(&keys))
            .await
            .map_err(SyncError::Source)?;

        let mut missing: HashSet<Key<P::Index>> = keys.into_iter().collect();
        let mut documents = Vec::with_capacity(rows.len());
        let mut deleted = Vec::new();
        for row in rows {
            let key = self.projection.key(&row);
            missing.remove(&key);
            match self.projection.project(row) {
                Some(document) => documents.push(document),
                None => deleted.push(key),
            }
        }
        deleted.extend(missing);

        let report = Reindexed {
            upserted: documents.len(),
            deleted: deleted.len(),
        };
        let mut tasks = Vec::with_capacity(2);
        if !documents.is_empty() {
            tasks.push(self.meili.process(Upsert::new(documents)).await?);
        }
        if !deleted.is_empty() {
            tasks.push(self.meili.process(Delete::new(deleted)).await?);
        }
        if let Some(timeout) = self.wait {
            for task in tasks {
                self.meili.process(WaitTask::new(task, timeout)).await?;
            }
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Entity;
    use crate::meilisearch::{NoField, SearchIndex};
    use crate::query::Query;
    use serde::{Deserialize, Serialize};
    use std::sync::Mutex;

    struct PostIndex;
    impl Entity for PostIndex {}

    impl SearchIndex for PostIndex {
        const UID: &'static str = "posts";
        const PRIMARY_KEY: &'static str = "id";
        type Key = u64;
        type Document = PostDocument;
        type Filterable = NoField;
        type Sortable = NoField;
    }

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct PostDocument {
        id: u64,
        title: String,
    }

    struct Row {
        id: u64,
        title: String,
        published: bool,
    }

    struct Posts;

    struct LoadPosts(Vec<u64>);

    impl Query for LoadPosts {
        type Effect = ();
    }

    impl Projection for Posts {
        type Index = PostIndex;
        type Row = Row;
        type Load = LoadPosts;

        fn load(&self, keys: &[u64]) -> LoadPosts {
            LoadPosts(keys.to_vec())
        }

        fn key(&self, row: &Row) -> u64 {
            row.id
        }

        fn project(&self, row: Row) -> Option<PostDocument> {
            row.published.then_some(PostDocument {
                id: row.id,
                title: row.title,
            })
        }
    }

    /// Posts 1 (published) and 2 (draft).
    struct Source {
        fail: bool,
    }

    impl Processor<LoadPosts> for Source {
        type Output = Vec<Row>;
        type Error = &'static str;

        async fn process(&self, query: LoadPosts) -> Result<Vec<Row>, &'static str> {
            if self.fail {
                return Err("source down");
            }
            let rows = [(1, true), (2, false)]
                .into_iter()
                .filter(|(id, _)| query.0.contains(id))
                .map(|(id, published)| Row {
                    id,
                    title: format!("post {id}"),
                    published,
                })
                .collect();
            Ok(rows)
        }
    }

    #[derive(Debug, PartialEq)]
    enum Call {
        Upsert(Vec<PostDocument>),
        Delete(Vec<u64>),
        Wait(u32),
    }

    #[derive(Default)]
    struct Meili {
        calls: Mutex<Vec<Call>>,
        fail_tasks: bool,
    }

    impl Meili {
        fn record(&self, call: Call) -> Result<TaskInfo, Error> {
            let mut calls = self.calls.lock().map_err(|_| Error::Timeout)?;
            calls.push(call);
            let uid = calls.len();
            Ok(serde_json::from_value(serde_json::json!({
                "enqueuedAt": "2026-01-01T00:00:00Z",
                "indexUid": "posts",
                "status": "enqueued",
                "type": "documentAdditionOrUpdate",
                "taskUid": uid,
            }))?)
        }

        fn calls(&self) -> Vec<Call> {
            self.calls
                .lock()
                .map(|mut calls| std::mem::take(&mut *calls))
                .unwrap_or_default()
        }
    }

    impl Processor<Upsert<PostIndex>> for Meili {
        type Output = TaskInfo;
        type Error = Error;

        async fn process(&self, query: Upsert<PostIndex>) -> Result<TaskInfo, Error> {
            self.record(Call::Upsert(query.documents().to_vec()))
        }
    }

    impl Processor<Delete<PostIndex>> for Meili {
        type Output = TaskInfo;
        type Error = Error;

        async fn process(&self, query: Delete<PostIndex>) -> Result<TaskInfo, Error> {
            let mut keys = query.keys().to_vec();
            keys.sort_unstable();
            self.record(Call::Delete(keys))
        }
    }

    impl Processor<WaitTask> for Meili {
        type Output = ();
        type Error = Error;

        async fn process(&self, query: WaitTask) -> Result<(), Error> {
            self.record(Call::Wait(query.task().get_task_uid()))?;
            if self.fail_tasks {
                return Err(Error::Timeout);
            }
            Ok(())
        }
    }

    fn published(id: u64) -> PostDocument {
        PostDocument {
            id,
            title: format!("post {id}"),
        }
    }

    #[tokio::test]
    async fn writes_projected_rows_deletes_the_rest_and_waits() {
        let meili = Meili::default();
        let indexer = Indexer::new(Source { fail: false }, &meili, Posts);

        let report = indexer
            .process(Reindex::<PostIndex>::new(vec![1, 2, 3, 3]))
            .await;

        // 1 is published, 2 is a draft, 3 has no row.
        assert!(matches!(
            report,
            Ok(Reindexed {
                upserted: 1,
                deleted: 2
            })
        ));
        assert_eq!(
            meili.calls(),
            [
                Call::Upsert(vec![published(1)]),
                Call::Delete(vec![2, 3]),
                Call::Wait(1),
                Call::Wait(2),
            ]
        );
    }

    #[tokio::test]
    async fn without_waiting_only_enqueues() {
        let meili = Meili::default();
        let indexer = Indexer::new(Source { fail: false }, &meili, Posts).without_waiting();

        let report = indexer.process(Reindex::<PostIndex>::one(1)).await;

        assert!(matches!(
            report,
            Ok(Reindexed {
                upserted: 1,
                deleted: 0
            })
        ));
        assert_eq!(meili.calls(), [Call::Upsert(vec![published(1)])]);
    }

    #[tokio::test]
    async fn failures_are_reported_and_a_failed_load_writes_nothing() {
        let meili = Meili::default();
        let indexer = Indexer::new(Source { fail: true }, &meili, Posts);
        let report = indexer.process(Reindex::<PostIndex>::one(1)).await;
        assert!(matches!(report, Err(SyncError::Source("source down"))));
        assert_eq!(meili.calls(), []);

        let meili = Meili {
            fail_tasks: true,
            ..Meili::default()
        };
        let indexer = Indexer::new(Source { fail: false }, &meili, Posts);
        let report = indexer.process(Reindex::<PostIndex>::one(1)).await;
        assert!(matches!(
            report,
            Err(SyncError::Meilisearch(Error::Timeout))
        ));
    }
}
