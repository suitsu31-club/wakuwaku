//! Meilisearch as a data source.
//!
//! [`MeiliSource`] runs queries on a Meilisearch [`Client`]. Each index is a
//! marker type implementing [`SearchIndex`]; the type is also an [`Entity`],
//! so a search of `PostIndex` has the effect `Read<PostIndex>` and a write
//! to it `Write<PostIndex>`, independent of the entity it projects.
//!
//! - [`search`]: a client's [`SearchRequest`](search::SearchRequest), run
//!   under a scope set by the server.
//! - [`filter`]: typed filters and sorts.
//! - [`token`]: tenant tokens, for frontends that search Meilisearch
//!   directly.
//! - [`write`](mod@write): upserts, deletes, settings, and waiting for
//!   the tasks they enqueue.
//! - [`sync`]: keeping an index in sync with its source of truth.
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
//! See the [Meilisearch guide](https://www.wakuwaku.rs/en/docs/data/meilisearch).

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
