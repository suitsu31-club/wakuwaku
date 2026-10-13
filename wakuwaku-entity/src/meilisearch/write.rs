//! Writing documents and settings, and waiting for the tasks they enqueue.
//!
//! Meilisearch applies writes asynchronously: [`Upsert`], [`Delete`] and
//! [`ApplySettings`] output the [`TaskInfo`] of their enqueued task, and
//! [`WaitTask`] waits until it ran. Tasks of one index run in the order
//! they were enqueued.
//!
//! See the [writes guide](https://www.wakuwaku.rs/en/docs/data/meilisearch/writes).

use crate::effect::io::Write;
use crate::meilisearch::{MeiliSource, SearchIndex, effective_settings};
use crate::query::{Execute, Query};
use meilisearch_sdk::client::Client;
use meilisearch_sdk::errors::Error;
use meilisearch_sdk::task_info::TaskInfo;
use meilisearch_sdk::tasks::Task;
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

/// Add documents to the index `I`, replacing documents with the same
/// primary key.
pub struct Upsert<I: SearchIndex> {
    documents: Vec<I::Document>,
}

impl<I: SearchIndex> Upsert<I> {
    /// Add or replace `documents`.
    pub const fn new(documents: Vec<I::Document>) -> Self {
        Self { documents }
    }

    /// The documents to write.
    pub fn documents(&self) -> &[I::Document] {
        &self.documents
    }
}

impl<I: SearchIndex> Debug for Upsert<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Upsert")
            .field("index", &I::UID)
            .field("documents", &self.documents.len())
            .finish()
    }
}

impl<I: SearchIndex> Query for Upsert<I> {
    type Effect = Write<I>;
}

impl<I: SearchIndex> Execute<MeiliSource> for Upsert<I> {
    type Output = TaskInfo;
    type Error = Error;

    async fn execute(self, client: &mut Arc<Client>) -> Result<TaskInfo, Error> {
        upsert::<I>(client, I::UID, &self.documents).await
    }
}

/// Add or replace `documents` in the index `uid`, which holds documents of
/// `I`.
pub(crate) async fn upsert<I: SearchIndex>(
    client: &Client,
    uid: &str,
    documents: &[I::Document],
) -> Result<TaskInfo, Error> {
    client
        .index(uid)
        .add_or_replace(documents, Some(I::PRIMARY_KEY))
        .await
}

/// Delete documents from the index `I` by primary key. Keys without a
/// document are ignored.
pub struct Delete<I: SearchIndex> {
    keys: Vec<I::Key>,
}

impl<I: SearchIndex> Delete<I> {
    /// Delete the documents of `keys`.
    pub const fn new(keys: Vec<I::Key>) -> Self {
        Self { keys }
    }

    /// The keys of the documents to delete.
    pub fn keys(&self) -> &[I::Key] {
        &self.keys
    }
}

impl<I: SearchIndex> Debug for Delete<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Delete")
            .field("index", &I::UID)
            .field("keys", &self.keys)
            .finish()
    }
}

impl<I: SearchIndex> Query for Delete<I> {
    type Effect = Write<I>;
}

impl<I: SearchIndex> Execute<MeiliSource> for Delete<I> {
    type Output = TaskInfo;
    type Error = Error;

    async fn execute(self, client: &mut Arc<Client>) -> Result<TaskInfo, Error> {
        client.index(I::UID).delete_documents(&self.keys).await
    }
}

/// Write the settings of the index `I`: [`SearchIndex::settings`], with the
/// filterable and sortable attributes of its field sets. Creates the index
/// if it doesn't exist.
///
/// Changing the filterable, sortable or searchable attributes makes
/// Meilisearch reindex every document. Run it when deploying, not per
/// request.
pub struct ApplySettings<I: SearchIndex> {
    index: PhantomData<fn() -> I>,
}

impl<I: SearchIndex> ApplySettings<I> {
    /// Apply the settings of `I`.
    pub const fn new() -> Self {
        Self { index: PhantomData }
    }
}

impl<I: SearchIndex> Default for ApplySettings<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: SearchIndex> Debug for ApplySettings<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("ApplySettings")
            .field("index", &I::UID)
            .finish()
    }
}

impl<I: SearchIndex> Query for ApplySettings<I> {
    type Effect = Write<I>;
}

impl<I: SearchIndex> Execute<MeiliSource> for ApplySettings<I> {
    type Output = TaskInfo;
    type Error = Error;

    async fn execute(self, client: &mut Arc<Client>) -> Result<TaskInfo, Error> {
        apply_settings::<I>(client, I::UID).await
    }
}

/// Write the settings of `I` to the index `uid`.
pub(crate) async fn apply_settings<I: SearchIndex>(
    client: &Client,
    uid: &str,
) -> Result<TaskInfo, Error> {
    client
        .index(uid)
        .set_settings(&effective_settings::<I>())
        .await
}

/// Wait until a task has run.
///
/// Outputs `()` if the task succeeded. Fails with
/// [`Error::Meilisearch`] carrying the task's error if it failed, and with
/// [`Error::Timeout`] if it is still pending after the timeout. A timed
/// out task keeps running.
///
/// It touches no entity, so any handle may run it.
#[derive(Debug)]
pub struct WaitTask {
    task: TaskInfo,
    timeout: Duration,
}

impl WaitTask {
    /// Wait for `task`, at most `timeout`.
    pub const fn new(task: TaskInfo, timeout: Duration) -> Self {
        Self { task, timeout }
    }

    /// The task to wait for.
    pub const fn task(&self) -> &TaskInfo {
        &self.task
    }
}

impl Query for WaitTask {
    type Effect = ();
}

impl Execute<MeiliSource> for WaitTask {
    type Output = ();
    type Error = Error;

    async fn execute(self, client: &mut Arc<Client>) -> Result<(), Error> {
        wait(client, &self.task, self.timeout, None).await
    }
}

/// Wait for `task`, polling every `interval` (50 ms if `None`).
pub(crate) async fn wait(
    client: &Client,
    task: &TaskInfo,
    timeout: Duration,
    interval: Option<Duration>,
) -> Result<(), Error> {
    match client.wait_for_task(task, interval, Some(timeout)).await? {
        Task::Succeeded { .. } => Ok(()),
        Task::Failed { content } => Err(Error::Meilisearch(content.error)),
        // `wait_for_task` returns only once the task has run.
        Task::Enqueued { .. } | Task::Processing { .. } => Err(Error::Timeout),
    }
}
