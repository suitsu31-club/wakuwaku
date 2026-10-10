//! Effects: what a query reads and writes.
//!
//! An [`Effect`] is a type-level set of accesses:
//!
//! - [`Read<E>`] and [`Write<E>`] are single accesses to the entity `E`.
//! - `()` touches nothing (`SELECT 1`, `PING`).
//! - A tuple of effects is their union, up to 12 elements. Tuples nest.
//!
//! Each effect has a [`Kind`]: [`WriteKind`] if it writes anything,
//! [`ReadKind`] otherwise. The kind decides whether write hooks run and
//! whether a transaction starts as read-only.
//!
//! A capability `C` may run a query with effect `X` when `C: Covers<X>`,
//! which holds when `C` has the [`CanRead`] / [`CanWrite`] marker for every
//! access in `X`.

use crate::effect::markers::{CanRead, CanWrite, Entity};
use std::marker::PhantomData;

/// Reads the entity `E`. Only used as a type.
pub struct Read<E>(PhantomData<fn() -> E>);

/// Writes the entity `E`. Only used as a type.
pub struct Write<E>(PhantomData<fn() -> E>);

mod sealed {
    pub trait Sealed {}
    impl Sealed for super::ReadKind {}
    impl Sealed for super::WriteKind {}
}

/// Whether an effect writes. Implemented only by [`ReadKind`] and
/// [`WriteKind`].
pub trait Kind: sealed::Sealed {
    /// `true` for [`WriteKind`].
    const WRITES: bool;

    /// The kind of the union of an effect of this kind and one of kind `K`.
    type Join<K: Kind>: Kind;
}

/// Kind of effects that only read.
#[derive(Debug)]
pub enum ReadKind {}

/// Kind of effects that write something.
#[derive(Debug)]
pub enum WriteKind {}

impl Kind for ReadKind {
    const WRITES: bool = false;
    type Join<K: Kind> = K;
}

impl Kind for WriteKind {
    const WRITES: bool = true;
    type Join<K: Kind> = WriteKind;
}

/// A type-level set of entity accesses.
pub trait Effect {
    /// Whether the set contains a write.
    type Kind: Kind;
}

impl<E: Entity> Effect for Read<E> {
    type Kind = ReadKind;
}

impl<E: Entity> Effect for Write<E> {
    type Kind = WriteKind;
}

impl Effect for () {
    type Kind = ReadKind;
}

/// The capability `Self` may perform every access in the effect `X`.
///
/// Implemented automatically from the [`CanRead`] / [`CanWrite`] markers;
/// don't implement it by hand.
pub trait Covers<X> {}

impl<C: CanRead<E>, E: Entity> Covers<Read<E>> for C {}

impl<C: CanWrite<E>, E: Entity> Covers<Write<E>> for C {}

impl<C> Covers<()> for C {}

macro_rules! tuple_effects {
    () => {};
    ($head:ident $(, $tail:ident)*) => {
        impl<$head: Effect $(, $tail: Effect)*> Effect for ($head, $($tail,)*) {
            type Kind = <$head::Kind as Kind>::Join<<($($tail,)*) as Effect>::Kind>;
        }

        impl<Cap, $head $(, $tail)*> Covers<($head, $($tail,)*)> for Cap
        where
            Cap: Covers<$head> + Covers<($($tail,)*)>,
        {
        }

        tuple_effects!($($tail),*);
    };
}

tuple_effects!(A, B, C, D, E, F, G, H, I, J, K, L);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effect::markers::{ReadOnly, ReadWrite};

    struct User;
    impl Entity for User {}
    struct Post;
    impl Entity for Post {}

    fn writes<X: Effect>() -> bool {
        <X::Kind as Kind>::WRITES
    }

    #[test]
    fn kind_is_write_iff_any_access_writes() {
        assert!(!writes::<()>());
        assert!(!writes::<(Read<User>, Read<Post>)>());
        assert!(writes::<(Read<User>, Write<Post>)>());
        assert!(writes::<(Write<User>, Read<Post>)>());
        assert!(writes::<((Read<User>,), (Read<Post>, Write<User>))>());
    }

    fn covers<C: Covers<X>, X>() {}

    #[test]
    fn stock_capabilities_cover_their_accesses() {
        covers::<ReadOnly, (Read<User>, (Read<Post>, ()))>();
        covers::<ReadWrite, (Read<User>, Write<Post>)>();
    }
}
