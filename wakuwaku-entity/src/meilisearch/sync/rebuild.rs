//! Rebuilding an index from its whole source.
//!
//! [`rebuild()`] fills a fresh copy of the index from a [`Scan`] of the
//! source, then swaps it with the live index in one step.
//!
//! See the [sync guide](https://www.wakuwaku.rs/en/docs/data/meilisearch/sync#rebuilding-an-index).

use crate::effect::markers::CanWrite;
use crate::meilisearch::sync::{Key, Scan, SyncError};
use crate::meilisearch::write::{apply_settings, upsert, wait};
use crate::meilisearch::{MeiliSource, SearchIndex};
use crate::query::Db;
use kanau::processor::Processor;
use meilisearch_sdk::client::{Client, SwapIndexes};
use meilisearch_sdk::errors::{Error, ErrorCode};
use meilisearch_sdk::task_info::TaskInfo;
use std::num::NonZeroUsize;
use std::time::{Duration, SystemTime};

/// Suffix of the uid of the copy an index is rebuilt in.
///
/// The copy of `posts` is `posts__wakuwaku_rebuild`. Only one rebuild of an
/// index may run at a time: a rebuild starts by deleting a leftover copy.
pub const COPY_SUFFIX: &str = "__wakuwaku_rebuild";

/// How [`rebuild()`] reads and writes.
#[derive(Debug, Clone, Copy)]
pub struct RebuildOptions {
    /// Rows per page read from the source, and documents per write.
    pub batch_size: NonZeroUsize,
    /// How long to wait for each task.
    pub task_timeout: Duration,
    /// How often to check whether a task has run.
    pub poll_interval: Duration,
}

impl Default for RebuildOptions {
    /// Pages of 1000 rows, ten minutes per task, checked every 200 ms.
    fn default() -> Self {
        Self {
            batch_size: NonZeroUsize::new(1000).unwrap_or(NonZeroUsize::MIN),
            task_timeout: Duration::from_secs(600),
            poll_interval: Duration::from_millis(200),
        }
    }
}

/// What a [`rebuild()`] did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rebuilt {
    /// When the rebuild started reading the source. Changes made since may
    /// be missing from the index: see [`rebuild()`].
    pub started_at: SystemTime,
    /// Documents written to the index, one per row that projects to a
    /// document.
    pub documents: usize,
}

/// Rebuild the index of `projection` from every row of `source`, and swap
/// it with the live index.
///
/// Fills a copy named with [`COPY_SUFFIX`] with the index's settings, the
/// documents of every row of the [`Scan`], then swaps it with the live
/// index (creating the live index if needed) and deletes the old
/// documents. Searches see either the old documents or the new ones, never
/// a half-filled index. On failure the live index is left as it was, and
/// the copy is deleted by the next rebuild.
///
/// The copy holds each row as its page was read: a change to a row after
/// that, reindexed into the live index meanwhile, is lost at the swap.
/// Reindex the keys changed since [`Rebuilt::started_at`] to catch up.
///
/// The handle `meili` must be able to write the index; its API key needs
/// the `indexes.create`, `indexes.delete`, `indexes.swap`,
/// `settings.update`, `documents.add` and `tasks.get` actions on both the
/// index and its copy.
///
/// # Errors
///
/// [`SyncError::Source`] if reading a page failed,
/// [`SyncError::Meilisearch`] if a request or task failed or timed out, and
/// [`SyncError::StuckScan`] if a page ended at the key the previous page
/// ended at.
pub async fn rebuild<P, Src, C>(
    projection: &P,
    source: &Src,
    meili: &Db<MeiliSource, C>,
    options: &RebuildOptions,
) -> Result<Rebuilt, SyncError<Src::Error>>
where
    P: Scan,
    Src: Processor<P::Page, Output = Vec<P::Row>>,
    C: CanWrite<P::Index>,
{
    let client: &Client = &meili.source().client;
    let live = <P::Index as SearchIndex>::UID;
    let primary_key = <P::Index as SearchIndex>::PRIMARY_KEY;
    let copy = format!("{live}{COPY_SUFFIX}");
    let settle = |task: TaskInfo| async move {
        wait(
            client,
            &task,
            options.task_timeout,
            Some(options.poll_interval),
        )
        .await
    };

    let started_at = SystemTime::now();
    let deleted = settle(client.delete_index(&copy).await?).await;
    tolerate(deleted, ErrorCode::IndexNotFound)?;
    settle(client.create_index(&copy, Some(primary_key)).await?).await?;
    settle(apply_settings::<P::Index>(client, &copy).await?).await?;

    let batch_size = options.batch_size.get();
    let mut after: Option<Key<P::Index>> = None;
    let mut documents = 0_usize;
    let mut pending: Option<TaskInfo> = None;
    loop {
        let rows = source
            .process(projection.page(after.as_ref(), batch_size))
            .await
            .map_err(SyncError::Source)?;
        let last_page = rows.len() < batch_size;
        let Some(last) = rows.last() else { break };
        let last_key = projection.key(last);
        if after.as_ref() == Some(&last_key) {
            return Err(SyncError::StuckScan(last_key.to_string()));
        }

        let batch: Vec<_> = rows
            .into_iter()
            .filter_map(|row| projection.project(row))
            .collect();
        if !batch.is_empty() {
            documents = documents.saturating_add(batch.len());
            let task = upsert::<P::Index>(client, &copy, &batch).await?;
            // Keep one write in flight while reading the next page.
            if let Some(previous) = pending.replace(task) {
                settle(previous).await?;
            }
        }
        if last_page {
            break;
        }
        after = Some(last_key);
    }
    if let Some(task) = pending {
        settle(task).await?;
    }

    let created = settle(client.create_index(live, Some(primary_key)).await?).await;
    tolerate(created, ErrorCode::IndexAlreadyExists)?;
    let swap = SwapIndexes {
        indexes: (live.to_owned(), copy.clone()),
        rename: None,
    };
    settle(client.swap_indexes([&swap]).await?).await?;
    // The copy now holds the old documents.
    settle(client.delete_index(&copy).await?).await?;

    Ok(Rebuilt {
        started_at,
        documents,
    })
}

/// `result`, with a failed task of error code `code` counted as a success.
fn tolerate(result: Result<(), Error>, code: ErrorCode) -> Result<(), Error> {
    match result {
        Err(Error::Meilisearch(error)) if error.error_code == code => Ok(()),
        result => result,
    }
}
