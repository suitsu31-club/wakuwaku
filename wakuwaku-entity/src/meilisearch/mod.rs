//! Meilisearch as a data source.
//!
//! Meilisearch is a search engine, not a database. Three things set it apart
//! from the other data sources of this crate:
//!
//! - Queries may come straight from end users, even from a browser that
//!   talks to Meilisearch directly.
//! - An index is never the source of truth. It holds a projection of data
//!   that lives elsewhere, so writes to it rarely need hooks.
//! - An index has to be kept in sync with that source.
//!
//! # Indexes are entities
//!
//! An index is described once by a type implementing [`SearchIndex`]. The
//! type is also an [`Entity`], so capabilities apply as usual:
//! [`ReadOnly`](crate::ReadOnly) may search every index, and an indexer
//! service gets a capability that writes its indexes and nothing
//! else. A query that searches `PostIndex` has the effect `Read<PostIndex>`,
//! not `Read<Post>`: reading the projection doesn't need access to the
//! source, and writing it doesn't grant write access to the source.
//!
//! Map capabilities to Meilisearch API keys when wiring: a
//! [`MeiliSource`] built from a search key belongs in a read-only handle,
//! and one built from a key with the `documents.*` and `settings.*` actions
//! on some indexes belongs in a handle that writes only those indexes.
//!
//! ```no_run
//! use kanau::processor::Processor;
//! use meilisearch_sdk::client::Client;
//! use meilisearch_sdk::settings::Settings;
//! use serde::{Deserialize, Serialize};
//! use wakuwaku_entity::meilisearch::filter::{Field, Filter};
//! use wakuwaku_entity::meilisearch::search::{Search, SearchRequest};
//! use wakuwaku_entity::meilisearch::write::Upsert;
//! use wakuwaku_entity::meilisearch::{MeiliSource, NoField, SearchIndex};
//! use wakuwaku_entity::{Db, Entity, ReadOnly, ReadWrite};
//!
//! /// What a search may return. Only public data goes here.
//! #[derive(Serialize, Deserialize)]
//! struct PostDocument {
//!     id: u64,
//!     author: u64,
//!     title: String,
//! }
//!
//! #[derive(Debug, Clone, Copy, Serialize, Deserialize)]
//! #[serde(rename_all = "snake_case")]
//! enum PostFilter {
//!     Author,
//! }
//!
//! impl Field for PostFilter {
//!     const ALL: &'static [Self] = &[Self::Author];
//!     fn name(self) -> &'static str {
//!         match self {
//!             Self::Author => "author",
//!         }
//!     }
//! }
//!
//! struct PostIndex;
//! impl Entity for PostIndex {}
//!
//! impl SearchIndex for PostIndex {
//!     const UID: &'static str = "posts";
//!     const PRIMARY_KEY: &'static str = "id";
//!     type Key = u64;
//!     type Document = PostDocument;
//!     type Filterable = PostFilter;
//!     type Sortable = NoField;
//!
//!     fn settings() -> Settings {
//!         Settings::new()
//!             .with_searchable_attributes(["title"])
//!             .with_displayed_attributes(["id", "author", "title"])
//!     }
//! }
//!
//! # async fn run() -> Result<(), meilisearch_sdk::errors::Error> {
//! let writer: Db<MeiliSource, ReadWrite> =
//!     Db::new(MeiliSource::new(Client::new("http://127.0.0.1:7700", Some("documents-key"))?));
//! let task = writer
//!     .process(Upsert::<PostIndex>::new(vec![PostDocument {
//!         id: 1,
//!         author: 7,
//!         title: "Hello".into(),
//!     }]))
//!     .await?;
//!
//! let reader: Db<MeiliSource, ReadOnly> =
//!     Db::new(MeiliSource::new(Client::new("http://127.0.0.1:7700", Some("search-key"))?));
//! let request: SearchRequest<PostIndex> = serde_json::from_str(r#"{"q": "hello"}"#)?;
//! let results = reader
//!     .process(Search::scoped(request, Filter::Eq(PostFilter::Author, 7.into())))
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Queries from end users
//!
//! - [`search`]: a [`SearchRequest`](search::SearchRequest) deserialized
//!   from the client, run by the server under a scope the client can't
//!   remove.
//! - [`token`]: tenant tokens, for frontends that query Meilisearch
//!   directly. Meilisearch enforces the token's scope on every search.
//! - [`filter`]: the typed filters both of them use, and the escape hatch
//!   for raw filter expressions.
//!
//! A tenant token restricts which indexes the client may search and which
//! documents it may see. Everything else in a search request is up to the
//! client: the attributes to retrieve, facets, sort, page size, hybrid
//! search. The index settings are therefore part of the security
//! configuration once a frontend talks to Meilisearch:
//!
//! - `displayedAttributes` defaults to every attribute. Keep secrets out of
//!   [`SearchIndex::Document`] and list the displayed attributes anyway.
//! - Every filterable attribute can also be used as a facet, which reveals
//!   value counts among the documents the client may see.
//! - `pagination.maxTotalHits` bounds how deep a client can page.
//! - Any embedder configured on the index can be triggered by any client
//!   allowed to search it.
//!
//! [`ApplySettings`](write::ApplySettings) writes [`SearchIndex::settings`]
//! to the index; run it when deploying.
//!
//! # Writing and syncing
//!
//! - [`write`](self::write): upserting and deleting documents, waiting for
//!   tasks, applying settings.
//! - [`sync`]: keeping an index in sync with its source of truth.
//!
//! Meilisearch processes writes asynchronously: a write query outputs the
//! [`TaskInfo`](meilisearch_sdk::task_info::TaskInfo) of an enqueued task,
//! and errors such as a missing primary key only show up once the task
//! runs. Wait for the task with [`WaitTask`](write::WaitTask) when the
//! result matters.

pub mod filter;
pub mod search;
pub mod sync;
pub mod token;
pub mod write;

use crate::effect::markers::Entity;
use crate::meilisearch::filter::Field;
use crate::query::{DataSource, Execute};
use meilisearch_sdk::client::Client;
use meilisearch_sdk::errors::Error;
use meilisearch_sdk::settings::Settings;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::sync::Arc;

/// A Meilisearch client as a [`DataSource`].
///
/// Queries run on a shared [`Client`]; cloning the source is cheap.
#[derive(Debug, Clone)]
pub struct MeiliSource {
    client: Arc<Client>,
}

impl MeiliSource {
    /// Run queries with `client`, and with its API key.
    pub fn new(client: Client) -> Self {
        Self {
            client: Arc::new(client),
        }
    }
}

impl DataSource for MeiliSource {
    type Connection = Arc<Client>;
    type Error = Error;

    async fn run<Q: Execute<Self>>(&self, query: Q) -> Result<Q::Output, Q::Error> {
        let mut client = Arc::clone(&self.client);
        query.execute(&mut client).await
    }
}

/// A Meilisearch index, described for queries and for capabilities.
///
/// Implement it on a marker type that is also an [`Entity`]. Each index
/// gets its own type, so capabilities can tell indexes apart.
pub trait SearchIndex: Entity + Send + Sync + 'static {
    /// Uid of the index.
    const UID: &'static str;

    /// Attribute holding the primary key of each document.
    const PRIMARY_KEY: &'static str;

    /// Value of the primary key.
    type Key: Display + Debug + Serialize + Eq + Hash + Clone + Send + Sync + 'static;

    /// A document of the index: what a search may return.
    ///
    /// Keep it separate from the row it is built from, and put only data
    /// every searcher may see in it. A frontend holding a tenant token can
    /// read every displayed attribute of every document its scope allows.
    type Document: Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Attributes searches may filter and facet on.
    ///
    /// [`ApplySettings`](write::ApplySettings) makes exactly these
    /// attributes filterable. Use [`NoField`] for none.
    type Filterable: Field;

    /// Attributes searches may sort on.
    ///
    /// [`ApplySettings`](write::ApplySettings) makes exactly these
    /// attributes sortable. Use [`NoField`] for none.
    type Sortable: Field;

    /// Settings of the index, other than the filterable and sortable
    /// attributes, which come from [`Filterable`](Self::Filterable) and
    /// [`Sortable`](Self::Sortable).
    ///
    /// Settings left unset keep their current value on the index. Set
    /// `displayedAttributes`: it defaults to every attribute.
    fn settings() -> Settings {
        Settings::new()
    }
}

/// The settings [`ApplySettings`](write::ApplySettings) writes for `I`.
fn effective_settings<I: SearchIndex>() -> Settings {
    I::settings()
        .with_filterable_attributes(I::Filterable::ALL.iter().map(|field| field.name()))
        .with_sortable_attributes(I::Sortable::ALL.iter().map(|field| field.name()))
}

/// A [`Field`] set with no field: for indexes without filterable or
/// sortable attributes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum NoField {}

impl Field for NoField {
    const ALL: &'static [Self] = &[];

    fn name(self) -> &'static str {
        match self {}
    }
}
