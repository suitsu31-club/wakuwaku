//! Searches the server runs for a client.
//!
//! A [`SearchRequest`] is what a client may ask, deserialized from its JSON;
//! a [`Search`] runs it under a scope set by the server.
//!
//! See the [search guide](https://www.wakuwaku.rs/en/docs/data/meilisearch/search#running-a-search-under-a-scope).

use crate::effect::io::Read;
use crate::meilisearch::filter::{Field, Filter, FilterError, Sort};
use crate::meilisearch::{MeiliSource, SearchIndex};
use crate::query::{Execute, Query};
use meilisearch_sdk::client::Client;
use meilisearch_sdk::errors::Error;
use meilisearch_sdk::search::{SearchResults, Selectors};
use serde::{Deserialize, Serialize};
use std::fmt::{self, Debug, Formatter};
use std::sync::Arc;

/// Page size used when a request doesn't set one.
pub const DEFAULT_HITS_PER_PAGE: usize = 20;

/// Largest page size a [`Search`] allows unless told otherwise.
pub const DEFAULT_MAX_HITS_PER_PAGE: usize = 100;

/// What a client may ask of a search on the index `I`.
///
/// Every field is optional; unknown fields are rejected, so a client can't
/// expect an option the server ignores:
///
/// ```json
/// {
///   "q": "rust",
///   "filter": {"gte": ["published_at", 1700000000]},
///   "sort": [{"field": "published_at", "order": "desc"}],
///   "facets": ["tags"],
///   "page": 2,
///   "hitsPerPage": 20
/// }
/// ```
#[derive(Serialize, Deserialize)]
#[serde(
    rename_all = "camelCase",
    deny_unknown_fields,
    bound(
        serialize = "I::Filterable: Serialize, I::Sortable: Serialize",
        deserialize = "I::Filterable: Deserialize<'de>, I::Sortable: Deserialize<'de>"
    )
)]
pub struct SearchRequest<I: SearchIndex> {
    /// Text to search for. Without it, every document in scope matches.
    #[serde(default)]
    pub q: Option<String>,
    /// Condition on the documents, on top of the server's scope.
    #[serde(default)]
    pub filter: Option<Filter<I::Filterable>>,
    /// Sorts, applied in order before relevancy.
    #[serde(default)]
    pub sort: Vec<Sort<I::Sortable>>,
    /// Fields to count the values of among the matching documents.
    #[serde(default)]
    pub facets: Vec<I::Filterable>,
    /// Page to return, starting at 1.
    #[serde(default)]
    pub page: Option<usize>,
    /// Documents per page, bounded by the search.
    #[serde(default)]
    pub hits_per_page: Option<usize>,
}

impl<I: SearchIndex> SearchRequest<I> {
    /// A request for every document in scope, first page.
    pub const fn new() -> Self {
        Self {
            q: None,
            filter: None,
            sort: Vec::new(),
            facets: Vec::new(),
            page: None,
            hits_per_page: None,
        }
    }
}

impl<I: SearchIndex> Default for SearchRequest<I> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I: SearchIndex> Clone for SearchRequest<I> {
    fn clone(&self) -> Self {
        Self {
            q: self.q.clone(),
            filter: self.filter.clone(),
            sort: self.sort.clone(),
            facets: self.facets.clone(),
            page: self.page,
            hits_per_page: self.hits_per_page,
        }
    }
}

impl<I: SearchIndex> Debug for SearchRequest<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SearchRequest")
            .field("index", &I::UID)
            .field("q", &self.q)
            .field("filter", &self.filter)
            .field("sort", &self.sort)
            .field("facets", &self.facets)
            .field("page", &self.page)
            .field("hits_per_page", &self.hits_per_page)
            .finish()
    }
}

/// A search on the index `I`: a client's [`SearchRequest`] under a scope
/// set by the server.
///
/// Outputs the results with their documents deserialized as
/// [`SearchIndex::Document`]. A filter that can't be rendered fails the
/// query with [`Error::Other`] wrapping a [`FilterError`]; call
/// [`Filter::render`] first to reject such a request upfront.
pub struct Search<I: SearchIndex> {
    request: SearchRequest<I>,
    scope: Option<Filter<I::Filterable>>,
    max_hits_per_page: usize,
}

impl<I: SearchIndex> Search<I> {
    /// Run `request` on the documents matching `scope` only.
    ///
    /// The scope and the request's filter are sent as separate elements of
    /// Meilisearch's filter array, parsed one by one and combined with
    /// `AND`, so nothing in the request's filter can lift the scope.
    pub const fn scoped(request: SearchRequest<I>, scope: Filter<I::Filterable>) -> Self {
        Self {
            request,
            scope: Some(scope),
            max_hits_per_page: DEFAULT_MAX_HITS_PER_PAGE,
        }
    }

    /// Run `request` on every document of the index.
    pub const fn unscoped(request: SearchRequest<I>) -> Self {
        Self {
            request,
            scope: None,
            max_hits_per_page: DEFAULT_MAX_HITS_PER_PAGE,
        }
    }

    /// Bound the page size to `max` documents instead of
    /// [`DEFAULT_MAX_HITS_PER_PAGE`]. Larger requests get `max`.
    #[must_use]
    pub const fn max_hits_per_page(mut self, max: usize) -> Self {
        self.max_hits_per_page = max;
        self
    }
}

impl<I: SearchIndex> Debug for Search<I> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("Search")
            .field("request", &self.request)
            .field("scope", &self.scope)
            .field("max_hits_per_page", &self.max_hits_per_page)
            .finish()
    }
}

impl<I: SearchIndex> Query for Search<I> {
    type Effect = Read<I>;
}

impl<I: SearchIndex> Execute<MeiliSource> for Search<I> {
    type Output = SearchResults<I::Document>;
    type Error = Error;

    async fn execute(self, client: &mut Arc<Client>) -> Result<Self::Output, Error> {
        let request = self.request;
        let filters = [self.scope.as_ref(), request.filter.as_ref()]
            .into_iter()
            .flatten()
            .map(Filter::render)
            .collect::<Result<Vec<_>, _>>()
            .map_err(filter_error)?;
        let filters: Vec<&str> = filters.iter().map(String::as_str).collect();
        let sort: Vec<String> = request.sort.iter().map(Sort::render).collect();
        let sort: Vec<&str> = sort.iter().map(String::as_str).collect();
        let facets: Vec<&str> = request.facets.iter().map(|field| field.name()).collect();

        let index = client.index(I::UID);
        let mut query = index.search();
        query
            .with_page(request.page.unwrap_or(1))
            .with_hits_per_page(
                request
                    .hits_per_page
                    .unwrap_or(DEFAULT_HITS_PER_PAGE)
                    .min(self.max_hits_per_page),
            );
        if let Some(q) = &request.q {
            query.with_query(q);
        }
        if !filters.is_empty() {
            query.with_array_filter(filters);
        }
        if !sort.is_empty() {
            query.with_sort(&sort);
        }
        if !facets.is_empty() {
            query.with_facets(Selectors::Some(&facets));
        }
        query.execute().await
    }
}

pub(crate) fn filter_error(error: FilterError) -> Error {
    Error::Other(Box::new(error))
}
