//! Static algebra of event types.
//!
//! Every [`Event`](crate::events::Event) picks exactly one marker type from
//! this module as its [`Algebra`](crate::events::Event::Algebra). The marker
//! decides how the consumer may optimize a *run*: the events of one type, for
//! one key, inside one ordering segment (see
//! [`consumer::plan`](crate::consumer::plan)). Runs never span keys, so every
//! law below only has to hold for events of the same key.
//!
//! The marker is a property of the type, not of a single message: the
//! consumer can only call [`EventSemigroup::combine`] or compare events if the
//! decoded type statically provides it. The bounds on each marker make an
//! unsupported choice fail to compile.
//!
//! Writing `s · e` for "apply event `e` to state `s`", the laws are:
//!
//! | Marker                    | Law                                   | Reduction of a run                     |
//! |---------------------------|---------------------------------------|----------------------------------------|
//! | [`NonAssociative`]        | none                                  | none                                   |
//! | [`Commutative`]           | `s · a · b = s · b · a`               | none; the run may be applied in any order |
//! | [`Idempotent`]            | `s · a · a = s · a`                   | adjacent equal events are collapsed    |
//! | [`IdempotentCommutative`] | both of the above                     | all equal events are collapsed         |
//! | [`Associative`]           | `s · a · b = s · combine(a, b)`       | the run is folded into one event       |
//!
//! Once a run folds into a single event, idempotency and commutativity cannot
//! shrink it further, so [`Associative`] has no idempotent or commutative
//! variant.

use crate::events::EventAssociativity;

mod sealed {
    pub trait Sealed {}
}

/// Optimization applied by the consumer to a run of decoded events of type `E`.
///
/// Sealed: [`EventAssociativity`] is written into every message header, so
/// each marker's reduction must stay in one-to-one correspondence with its
/// [`ASSOCIATIVITY`](Algebra::ASSOCIATIVITY).
pub trait Algebra<E>: sealed::Sealed {
    /// Value written into the message header for events using this algebra.
    const ASSOCIATIVITY: EventAssociativity;

    /// Reduce `run`, given in log order, in place.
    ///
    /// Applying the reduced run in order must have the same effect as applying
    /// the original run in order.
    fn reduce(run: &mut Vec<E>);
}

/// Event types whose same-key runs can be folded.
pub trait EventSemigroup: Sized {
    /// Combine `self` with the event that follows it.
    ///
    /// Must be associative, and applying the result must equal applying
    /// `self` and then `other`.
    fn combine(self, other: Self) -> Self;
}

/// No optimization: every event is applied, in log order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NonAssociative;

/// Events of the run may be applied in any order. Nothing is removed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commutative;

/// Consecutive equal events are collapsed into one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Idempotent;

/// Equal events are collapsed into one wherever they appear in the run, and
/// the run may be applied in any order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdempotentCommutative;

/// The run is folded into a single event with [`EventSemigroup::combine`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Associative;

impl sealed::Sealed for NonAssociative {}
impl sealed::Sealed for Commutative {}
impl sealed::Sealed for Idempotent {}
impl sealed::Sealed for IdempotentCommutative {}
impl sealed::Sealed for Associative {}

impl<E> Algebra<E> for NonAssociative {
    const ASSOCIATIVITY: EventAssociativity = EventAssociativity::NonAssociative;
    fn reduce(_run: &mut Vec<E>) {}
}

impl<E> Algebra<E> for Commutative {
    const ASSOCIATIVITY: EventAssociativity = EventAssociativity::Commutative;
    fn reduce(_run: &mut Vec<E>) {}
}

impl<E: Eq> Algebra<E> for Idempotent {
    const ASSOCIATIVITY: EventAssociativity = EventAssociativity::Idempotent;
    fn reduce(run: &mut Vec<E>) {
        run.dedup();
    }
}

impl<E: Ord> Algebra<E> for IdempotentCommutative {
    const ASSOCIATIVITY: EventAssociativity = EventAssociativity::IdempotentCommutative;
    fn reduce(run: &mut Vec<E>) {
        // Commutativity permits reordering, which brings equal events together.
        run.sort_unstable();
        run.dedup();
    }
}

impl<E: EventSemigroup> Algebra<E> for Associative {
    const ASSOCIATIVITY: EventAssociativity = EventAssociativity::Associative;
    fn reduce(run: &mut Vec<E>) {
        let folded = run.drain(..).reduce(E::combine);
        run.extend(folded);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Eq)]
    struct Append(String);

    impl EventSemigroup for Append {
        fn combine(mut self, other: Self) -> Self {
            self.0.push_str(&other.0);
            self
        }
    }

    #[test]
    fn associative_folds_in_log_order() {
        let mut run = vec![Append("a".into()), Append("b".into()), Append("c".into())];
        <Associative as Algebra<Append>>::reduce(&mut run);
        assert_eq!(run, vec![Append("abc".into())]);
    }

    #[test]
    fn idempotent_only_collapses_adjacent_duplicates() {
        // `a, b, a` must not become `a, b`: the second `a` overrides `b`.
        let mut run = vec![1, 1, 2, 1, 1];
        <Idempotent as Algebra<i32>>::reduce(&mut run);
        assert_eq!(run, vec![1, 2, 1]);
    }

    #[test]
    fn idempotent_commutative_collapses_all_duplicates() {
        let mut run = vec![1, 1, 2, 1, 1];
        <IdempotentCommutative as Algebra<i32>>::reduce(&mut run);
        assert_eq!(run, vec![1, 2]);
    }
}
