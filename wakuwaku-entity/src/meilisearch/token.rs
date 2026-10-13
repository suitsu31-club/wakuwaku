//! Tenant tokens, for frontends that search Meilisearch directly.
//!
//! A [`TokenSigner`] signs JWTs that a client sends to Meilisearch as its
//! API key; Meilisearch then lets it search only the indexes listed in the
//! token, each under the token's filter.
//!
//! ```no_run
//! # use wakuwaku_entity::meilisearch::filter::{Field, Filter};
//! # use wakuwaku_entity::meilisearch::token::TokenSigner;
//! # use wakuwaku_entity::meilisearch::{NoField, SearchIndex};
//! # use wakuwaku_entity::{Entity, ReadOnly};
//! # use std::time::{Duration, SystemTime};
//! # #[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
//! # enum PostFilter { Author }
//! # impl Field for PostFilter {
//! #     const ALL: &'static [Self] = &[Self::Author];
//! #     fn name(self) -> &'static str { "author" }
//! # }
//! # struct PostIndex;
//! # impl Entity for PostIndex {}
//! # impl SearchIndex for PostIndex {
//! #     const UID: &'static str = "posts";
//! #     const PRIMARY_KEY: &'static str = "id";
//! #     type Key = u64;
//! #     type Document = serde_json::Value;
//! #     type Filterable = PostFilter;
//! #     type Sortable = NoField;
//! # }
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let search_client = meilisearch_sdk::client::Client::new(
//!     "http://127.0.0.1:7700",
//!     Some("the search key"),
//! )?;
//! let signer: TokenSigner<ReadOnly> =
//!     TokenSigner::new("uid-of-the-search-key", search_client);
//!
//! // Valid for an hour, searches the posts of user 7 only.
//! let token = signer
//!     .token(SystemTime::now() + Duration::from_secs(3600))
//!     .scoped::<PostIndex>(&Filter::Eq(PostFilter::Author, 7.into()))?
//!     .sign()?;
//! # Ok(())
//! # }
//! ```
//!
//! See the [tenant tokens guide](https://www.wakuwaku.rs/en/docs/data/meilisearch/tenant-tokens).

use crate::effect::io::{Covers, Read};
use crate::meilisearch::SearchIndex;
use crate::meilisearch::filter::{Filter, FilterError};
use meilisearch_sdk::client::Client;
use meilisearch_sdk::errors::Error;
use serde_json::{Map, Value, json};
use std::fmt::{self, Debug, Formatter};
use std::marker::PhantomData;
use std::time::SystemTime;
use time::OffsetDateTime;

/// Signs tenant tokens that grant searches the capability `C` may run.
///
/// A token may only list indexes `I` with `C: Covers<Read<I>>`, so a
/// service handing out tokens is limited like a handle of the same
/// capability.
pub struct TokenSigner<C> {
    key_uid: String,
    client: Client,
    capability: PhantomData<fn() -> C>,
}

impl<C> TokenSigner<C> {
    /// Sign with the API key of `client`, whose uid is `key_uid`.
    ///
    /// The key must have the `search` action, and should have no other: a
    /// token can't do more than its key. Meilisearch rejects tokens signed
    /// with the master key. Tokens can't be revoked one by one: deleting
    /// the key revokes every token it signed.
    pub fn new(key_uid: impl Into<String>, client: Client) -> Self {
        Self {
            key_uid: key_uid.into(),
            client,
            capability: PhantomData,
        }
    }

    /// Start a token that expires at `expires_at` and allows no index yet.
    pub fn token(&self, expires_at: SystemTime) -> TenantToken<'_, C> {
        TenantToken {
            signer: self,
            rules: Map::new(),
            expires_at,
        }
    }
}

impl<C> Debug for TokenSigner<C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TokenSigner")
            .field("key_uid", &self.key_uid)
            .field("capability", &std::any::type_name::<C>())
            .finish_non_exhaustive()
    }
}

/// A tenant token being built: the indexes it may search, with their
/// scopes.
pub struct TenantToken<'s, C> {
    signer: &'s TokenSigner<C>,
    rules: Map<String, Value>,
    expires_at: SystemTime,
}

impl<C> TenantToken<'_, C> {
    /// Allow searching the documents of `I` that match `scope`.
    ///
    /// Meilisearch applies the scope as a separate element of each search's
    /// filter array, so nothing the client sends can lift it. Replaces any
    /// earlier rule for `I`.
    ///
    /// # Errors
    ///
    /// [`FilterError`] if `scope` can't be [rendered](Filter::render).
    pub fn scoped<I: SearchIndex>(
        mut self,
        scope: &Filter<I::Filterable>,
    ) -> Result<Self, FilterError>
    where
        C: Covers<Read<I>>,
    {
        let filter = scope.render()?;
        self.rules
            .insert(I::UID.to_owned(), json!({ "filter": filter }));
        Ok(self)
    }

    /// Allow searching every document of `I`.
    ///
    /// Replaces any earlier rule for `I`.
    #[must_use]
    pub fn unscoped<I: SearchIndex>(mut self) -> Self
    where
        C: Covers<Read<I>>,
    {
        self.rules.insert(I::UID.to_owned(), json!({}));
        self
    }

    /// Sign the token. Signing is local: it sends no request.
    ///
    /// # Errors
    ///
    /// Fails if the token has already expired, or if the key uid isn't a
    /// UUID v4.
    pub fn sign(self) -> Result<String, Error> {
        self.signer.client.generate_tenant_token(
            self.signer.key_uid.clone(),
            Value::Object(self.rules),
            None,
            Some(OffsetDateTime::from(self.expires_at)),
        )
    }
}

impl<C> Debug for TenantToken<'_, C> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("TenantToken")
            .field("signer", &self.signer)
            .field("rules", &self.rules)
            .field("expires_at", &self.expires_at)
            .finish()
    }
}
