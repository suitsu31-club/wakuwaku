//! Laws of Boolean algebra over [`And`] and [`Or`], as [`TermDerive`] rules.
//!
//! Each rule type names one law and is used as the `R` parameter of [`TermDerive`] /
//! [`Proven::derive`]. See [`TermDerive`] for the full table, how evidence is carried
//! over, and an example.

use crate::{And, Evidence, Or, Predicate, Proven, TermDerive};

/// Commutativity: `A ∧ B ⇒ B ∧ A` and `A ∨ B ⇒ B ∨ A`. See [`TermDerive`].
pub enum Commute {}

impl<T, A, B> TermDerive<And<B, A>, T, Commute> for And<A, B>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<B, A>> {
        proof.map_evidence(|And(a, b)| And(b, a))
    }
}

impl<T, A, B> TermDerive<Or<B, A>, T, Commute> for Or<A, B>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<B, A>> {
        proof.map_evidence(|ab| match ab {
            Or::Left(a) => Or::Right(a),
            Or::Right(b) => Or::Left(b),
            Or::Both(a, b) => Or::Both(b, a),
        })
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
        proof.map_evidence(|And(Evidence(And(a, b)), c)| And(a, Evidence(And(b, c))))
    }
}

impl<T, A, B, C> TermDerive<And<And<A, B>, C>, T, Associate> for And<A, And<B, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<And<A, B>, C>> {
        proof.map_evidence(|And(a, Evidence(And(b, c)))| And(Evidence(And(a, b)), c))
    }
}

impl<T, A, B, C> TermDerive<Or<A, Or<B, C>>, T, Associate> for Or<Or<A, B>, C>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<A, Or<B, C>>> {
        proof.map_evidence(|abc| match abc {
            Or::Left(Evidence(Or::Left(a))) => Or::Left(a),
            Or::Left(Evidence(Or::Right(b))) => Or::Right(Evidence(Or::Left(b))),
            Or::Left(Evidence(Or::Both(a, b))) => Or::Both(a, Evidence(Or::Left(b))),
            Or::Right(c) => Or::Right(Evidence(Or::Right(c))),
            Or::Both(Evidence(Or::Left(a)), c) => Or::Both(a, Evidence(Or::Right(c))),
            Or::Both(Evidence(Or::Right(b)), c) => Or::Right(Evidence(Or::Both(b, c))),
            Or::Both(Evidence(Or::Both(a, b)), c) => Or::Both(a, Evidence(Or::Both(b, c))),
        })
    }
}

impl<T, A, B, C> TermDerive<Or<Or<A, B>, C>, T, Associate> for Or<A, Or<B, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<Or<A, B>, C>> {
        proof.map_evidence(|abc| match abc {
            Or::Left(a) => Or::Left(Evidence(Or::Left(a))),
            Or::Right(Evidence(Or::Left(b))) => Or::Left(Evidence(Or::Right(b))),
            Or::Right(Evidence(Or::Right(c))) => Or::Right(c),
            Or::Right(Evidence(Or::Both(b, c))) => Or::Both(Evidence(Or::Right(b)), c),
            Or::Both(a, Evidence(Or::Left(b))) => Or::Left(Evidence(Or::Both(a, b))),
            Or::Both(a, Evidence(Or::Right(c))) => Or::Both(Evidence(Or::Left(a)), c),
            Or::Both(a, Evidence(Or::Both(b, c))) => Or::Both(Evidence(Or::Both(a, b)), c),
        })
    }
}

/// Idempotence: `A ∧ A ⇔ A` and `A ∨ A ⇔ A`. See [`TermDerive`].
///
/// `A ⇒ A ∧ A` duplicates the evidence and requires `A: Clone`.
pub enum Idempotence {}

impl<T, A> TermDerive<A, T, Idempotence> for And<A, A>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, A> {
        proof.map_evidence(|And(Evidence(a), _)| a)
    }
}

impl<T, A: Predicate<T> + Clone> TermDerive<And<A, A>, T, Idempotence> for A {
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<A, A>> {
        proof.map_evidence(|a| {
            let a = Evidence(a);
            And(a.duplicate(), a)
        })
    }
}

impl<T, A> TermDerive<A, T, Idempotence> for Or<A, A>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, A> {
        proof.map_evidence(|aa| match aa {
            Or::Left(Evidence(a)) | Or::Right(Evidence(a)) | Or::Both(Evidence(a), _) => a,
        })
    }
}

impl<T, A: Predicate<T>> TermDerive<Or<A, A>, T, Idempotence> for A {
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<A, A>> {
        proof.map_evidence(|a| Or::Left(Evidence(a)))
    }
}

/// Absorption: `A ∧ (A ∨ B) ⇔ A` and `A ∨ (A ∧ B) ⇔ A`. See [`TermDerive`].
///
/// In the `⇐` direction `B` is arbitrary and chosen by the target type.
/// `A ⇒ A ∧ (A ∨ B)` duplicates the evidence and requires `A: Clone`.
pub enum Absorb {}

impl<T, A, B> TermDerive<A, T, Absorb> for And<A, Or<A, B>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, A> {
        proof.map_evidence(|And(Evidence(a), _)| a)
    }
}

impl<T, A: Predicate<T> + Clone, B> TermDerive<And<A, Or<A, B>>, T, Absorb> for A {
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<A, Or<A, B>>> {
        proof.map_evidence(|a| {
            let a = Evidence(a);
            And(a.duplicate(), Evidence(Or::Left(a)))
        })
    }
}

impl<T, A, B> TermDerive<A, T, Absorb> for Or<A, And<A, B>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, A> {
        proof.map_evidence(|aab| match aab {
            Or::Left(Evidence(a))
            | Or::Right(Evidence(And(Evidence(a), _)))
            | Or::Both(Evidence(a), _) => a,
        })
    }
}

impl<T, A: Predicate<T>, B> TermDerive<Or<A, And<A, B>>, T, Absorb> for A {
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<A, And<A, B>>> {
        proof.map_evidence(|a| Or::Left(Evidence(a)))
    }
}

/// Distributivity: `A ∧ (B ∨ C) ⇔ (A ∧ B) ∨ (A ∧ C)` and
/// `A ∨ (B ∧ C) ⇔ (A ∨ B) ∧ (A ∨ C)`. See [`TermDerive`].
///
/// `A ∨ (B ∧ C) ⇒ (A ∨ B) ∧ (A ∨ C)` duplicates `A`'s evidence and requires
/// `A: Clone`. `A ∧ (B ∨ C) ⇒ (A ∧ B) ∨ (A ∧ C)` keeps only `A ∧ B` when both `B` and
/// `C` held, so it needs no `Clone`.
pub enum Distribute {}

impl<T, A, B, C> TermDerive<Or<And<A, B>, And<A, C>>, T, Distribute> for And<A, Or<B, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<And<A, B>, And<A, C>>> {
        proof.map_evidence(|And(a, Evidence(bc))| match bc {
            Or::Left(b) | Or::Both(b, _) => Or::Left(Evidence(And(a, b))),
            Or::Right(c) => Or::Right(Evidence(And(a, c))),
        })
    }
}

impl<T, A, B, C> TermDerive<And<A, Or<B, C>>, T, Distribute> for Or<And<A, B>, And<A, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<A, Or<B, C>>> {
        proof.map_evidence(|abac| match abac {
            Or::Left(Evidence(And(a, b))) => And(a, Evidence(Or::Left(b))),
            Or::Right(Evidence(And(a, c))) => And(a, Evidence(Or::Right(c))),
            Or::Both(Evidence(And(a, b)), Evidence(And(_, c))) => {
                And(a, Evidence(Or::Both(b, c)))
            }
        })
    }
}

impl<T, A, B, C> TermDerive<And<Or<A, B>, Or<A, C>>, T, Distribute> for Or<A, And<B, C>>
where
    Self: Predicate<T>,
    A: Clone,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, And<Or<A, B>, Or<A, C>>> {
        proof.map_evidence(|abc| match abc {
            Or::Left(a) => And(Evidence(Or::Left(a.duplicate())), Evidence(Or::Left(a))),
            Or::Right(Evidence(And(b, c))) => {
                And(Evidence(Or::Right(b)), Evidence(Or::Right(c)))
            }
            Or::Both(a, Evidence(And(b, c))) => And(
                Evidence(Or::Both(a.duplicate(), b)),
                Evidence(Or::Both(a, c)),
            ),
        })
    }
}

impl<T, A, B, C> TermDerive<Or<A, And<B, C>>, T, Distribute> for And<Or<A, B>, Or<A, C>>
where
    Self: Predicate<T>,
{
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, Or<A, And<B, C>>> {
        proof.map_evidence(|And(Evidence(ab), Evidence(ac))| match (ab, ac) {
            (Or::Both(a, b), Or::Right(c) | Or::Both(_, c)) | (Or::Right(b), Or::Both(a, c)) => {
                Or::Both(a, Evidence(And(b, c)))
            }
            (Or::Right(b), Or::Right(c)) => Or::Right(Evidence(And(b, c))),
            (Or::Left(a) | Or::Both(a, _), _) | (Or::Right(_), Or::Left(a)) => Or::Left(a),
        })
    }
}
