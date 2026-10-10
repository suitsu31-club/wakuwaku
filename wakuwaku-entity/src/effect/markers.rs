//! Entities and the capabilities to read and write them.
//!
//! A capability is a type that is never constructed. It says what a [`Db`]
//! handle may do through the marker traits it implements:
//!
//! - [`CanRead<E>`]: may run queries that read `E`.
//! - [`CanWrite<E>`]: may run queries that write `E`. Writing implies reading.
//!
//! [`ReadOnly`] and [`ReadWrite`] cover every entity. A service that may only
//! write some entities defines its own capability:
//!
//! ```
//! use wakuwaku_entity::{CanRead, CanWrite, Entity};
//!
//! struct User;
//! impl Entity for User {}
//!
//! struct Session;
//! impl Entity for Session {}
//!
//! /// Reads everything from the replica, writes only sessions.
//! enum ApiReplica {}
//! impl<E: Entity> CanRead<E> for ApiReplica {}
//! impl CanWrite<Session> for ApiReplica {}
//! ```
//!
//! [`Db`]: crate::query::Db

/// Something a query can read or write: a table, a key space, a document
/// collection, ….
///
/// An entity is a name at the type level. It isn't tied to a data source, so
/// the same entity may live in PostgreSQL and be cached in Redis.
pub trait Entity {}

/// The capability may run queries that read `E`.
pub trait CanRead<E: Entity> {}

/// The capability may run queries that write `E`.
pub trait CanWrite<E: Entity>: CanRead<E> {}

/// May read every entity and write none.
///
/// The capability of handles connected to a read-only replica.
#[derive(Debug)]
pub enum ReadOnly {}

impl<E: Entity> CanRead<E> for ReadOnly {}

/// May read and write every entity.
#[derive(Debug)]
pub enum ReadWrite {}

impl<E: Entity> CanRead<E> for ReadWrite {}
impl<E: Entity> CanWrite<E> for ReadWrite {}
