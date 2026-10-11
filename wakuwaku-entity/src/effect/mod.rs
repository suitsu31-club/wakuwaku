//! What queries touch, and who may touch it.
//!
//! - [`markers`]: [`Entity`](markers::Entity) and the capability traits
//!   [`CanRead`](markers::CanRead) / [`CanWrite`](markers::CanWrite), with the
//!   two stock capabilities [`ReadOnly`](markers::ReadOnly) and
//!   [`ReadWrite`](markers::ReadWrite).
//! - [`io`]: the effect wrappers [`Read`](io::Read) / [`Write`](io::Write),
//!   effect sets, and [`Covers`](io::Covers), which checks a capability
//!   against an effect.
//! - [`hook`]: processors that run before or after write queries.

pub mod hook;
pub mod io;
pub mod markers;
