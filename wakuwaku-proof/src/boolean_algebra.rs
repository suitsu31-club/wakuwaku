//! Laws of Boolean algebra over [`And`] and [`Or`], as [`TermDerive`] rules.
//!
//! Each rule type names one law and is used as the `R` parameter of [`TermDerive`] /
//! [`Proven::derive`]. See [`TermDerive`] for the full table and an example.

use crate::{And, Or, Predicate, Proven, TermDerive};

/// Commutativity: `A ∧ B ⇒ B ∧ A` and `A ∨ B ⇒ B ∨ A`. See [`TermDerive`].
pub enum Commute {}

impl<T, A, B> TermDerive<And<B, A>, T, Commute> for And<A, B>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<B, A>> {
        proof.relabel()
    }
}

impl<T, A, B> TermDerive<Or<B, A>, T, Commute> for Or<A, B>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<B, A>> {
        proof.relabel()
    }
}

/// Associativity: `(A ∧ B) ∧ C ⇔ A ∧ (B ∧ C)` and `(A ∨ B) ∨ C ⇔ A ∨ (B ∨ C)`. See
/// [`TermDerive`].
pub enum Associate {}

impl<T, A, B, C> TermDerive<And<A, And<B, C>>, T, Associate> for And<And<A, B>, C>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<A, And<B, C>>> {
        proof.relabel()
    }
}

impl<T, A, B, C> TermDerive<And<And<A, B>, C>, T, Associate> for And<A, And<B, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<And<A, B>, C>> {
        proof.relabel()
    }
}

impl<T, A, B, C> TermDerive<Or<A, Or<B, C>>, T, Associate> for Or<Or<A, B>, C>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<A, Or<B, C>>> {
        proof.relabel()
    }
}

impl<T, A, B, C> TermDerive<Or<Or<A, B>, C>, T, Associate> for Or<A, Or<B, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<Or<A, B>, C>> {
        proof.relabel()
    }
}

/// Idempotence: `A ∧ A ⇔ A` and `A ∨ A ⇔ A`. See [`TermDerive`].
pub enum Idempotence {}

impl<T, A> TermDerive<A, T, Idempotence> for And<A, A>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, A> {
        proof.relabel()
    }
}

impl<T, A: Predicate<T>> TermDerive<And<A, A>, T, Idempotence> for A {
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<A, A>> {
        proof.relabel()
    }
}

impl<T, A> TermDerive<A, T, Idempotence> for Or<A, A>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, A> {
        proof.relabel()
    }
}

impl<T, A: Predicate<T>> TermDerive<Or<A, A>, T, Idempotence> for A {
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<A, A>> {
        proof.relabel()
    }
}

/// Absorption: `A ∧ (A ∨ B) ⇔ A` and `A ∨ (A ∧ B) ⇔ A`. See [`TermDerive`].
///
/// In the `⇐` direction `B` is arbitrary and chosen by the target type.
pub enum Absorb {}

impl<T, A, B> TermDerive<A, T, Absorb> for And<A, Or<A, B>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, A> {
        proof.relabel()
    }
}

impl<T, A: Predicate<T>, B> TermDerive<And<A, Or<A, B>>, T, Absorb> for A {
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<A, Or<A, B>>> {
        proof.relabel()
    }
}

impl<T, A, B> TermDerive<A, T, Absorb> for Or<A, And<A, B>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, A> {
        proof.relabel()
    }
}

impl<T, A: Predicate<T>, B> TermDerive<Or<A, And<A, B>>, T, Absorb> for A {
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<A, And<A, B>>> {
        proof.relabel()
    }
}

/// Distributivity: `A ∧ (B ∨ C) ⇔ (A ∧ B) ∨ (A ∧ C)` and
/// `A ∨ (B ∧ C) ⇔ (A ∨ B) ∧ (A ∨ C)`. See [`TermDerive`].
pub enum Distribute {}

impl<T, A, B, C> TermDerive<Or<And<A, B>, And<A, C>>, T, Distribute> for And<A, Or<B, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<And<A, B>, And<A, C>>> {
        proof.relabel()
    }
}

impl<T, A, B, C> TermDerive<And<A, Or<B, C>>, T, Distribute> for Or<And<A, B>, And<A, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<A, Or<B, C>>> {
        proof.relabel()
    }
}

impl<T, A, B, C> TermDerive<And<Or<A, B>, Or<A, C>>, T, Distribute> for Or<A, And<B, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<Or<A, B>, Or<A, C>>> {
        proof.relabel()
    }
}

impl<T, A, B, C> TermDerive<Or<A, And<B, C>>, T, Distribute> for And<Or<A, B>, Or<A, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<A, And<B, C>>> {
        proof.relabel()
    }
}