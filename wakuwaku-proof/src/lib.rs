#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic_in_result_fn,
    clippy::arithmetic_side_effects
)]

//! Proof-carrying validation.
//!
//! A [`Predicate`] is a proposition about a subject of type `T` that is decided at
//! runtime, possibly with I/O (a database lookup, a remote call, …). Running it
//! through [`prove`] either fails with a [`ValidateError`] or hands back a
//! [`Proven<T, P>`]: the subject together with type-level evidence that `P` held.
//!
//! Downstream code that requires a validated value takes `Proven<T, P>` instead of
//! `T`, so "forgot to validate" becomes a type error rather than a runtime bug.
//!
//! Predicates compose with [`And`] and [`Or`]. Both evaluate their operands in the
//! [`CheckOrder`] passed to [`prove`], and forward that order to nested predicates.
//!
//! A proof converts into a proof of an equivalent or weaker proposition without
//! rechecking through [`TermDerive`] / [`Proven::derive`], which implement the laws
//! of Boolean algebra over [`And`] and [`Or`] (commutativity, associativity, …).
//!
//! # Contexts and errors
//!
//! Every predicate names a [`ValidateContext`] (`Predicate::Ctx`) holding whatever
//! it needs to decide (configuration, connection handles, …), plus the I/O error
//! type that deciding may produce. A check has three outcomes:
//!
//! - `Ok(())`: the proposition holds.
//! - [`ValidateError::Deny`]: the proposition was decided and is false.
//! - [`ValidateError::IoError`]: the proposition could not be decided.
//!
//! # Example
//!
//! ```
//! use std::convert::Infallible;
//! use wakuwaku_proof::{
//!     And, CheckOrder, DeniedReason, Predicate, Proven, ValidateContext, ValidateError, prove,
//! };
//!
//! #[derive(Clone)]
//! struct Limits {
//!     max_len: usize,
//! }
//!
//! impl ValidateContext for Limits {
//!     type IoError = Infallible;
//! }
//!
//! static EMPTY: DeniedReason = DeniedReason("username is empty");
//! static TOO_LONG: DeniedReason = DeniedReason("username is too long");
//!
//! struct NonEmpty;
//!
//! impl Predicate<String> for NonEmpty {
//!     type Ctx = Limits;
//!
//!     async fn check(
//!         name: &String,
//!         _: &Limits,
//!         _: CheckOrder,
//!     ) -> Result<(), ValidateError<Infallible>> {
//!         if name.is_empty() { Err(ValidateError::Deny(EMPTY)) } else { Ok(()) }
//!     }
//! }
//!
//! struct ShortEnough;
//!
//! impl Predicate<String> for ShortEnough {
//!     type Ctx = Limits;
//!
//!     async fn check(
//!         name: &String,
//!         limits: &Limits,
//!         _: CheckOrder,
//!     ) -> Result<(), ValidateError<Infallible>> {
//!         if name.len() > limits.max_len { Err(ValidateError::Deny(TOO_LONG)) } else { Ok(()) }
//!     }
//! }
//!
//! type ValidUsername = And<NonEmpty, ShortEnough>;
//!
//! // Only callable with a name that has been through `prove`.
//! fn register(name: Proven<String, ValidUsername>) -> usize {
//!     name.subject().len()
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let limits = Limits { max_len: 8 };
//!
//! let Ok(name) = prove::<_, ValidUsername>("haruki".to_owned(), &limits, CheckOrder::Sequential).await
//! else {
//!     unreachable!()
//! };
//! assert_eq!(register(name), 6);
//!
//! let denied = prove::<_, ValidUsername>(String::new(), &limits, CheckOrder::Parallel).await;
//! assert!(matches!(denied, Err(ValidateError::Deny(reason)) if reason == EMPTY));
//! # }
//! ```

pub mod boolean_algebra;

use std::marker::PhantomData;

#[cfg(doc)]
use boolean_algebra::{Absorb, Associate, Commute, Distribute, Idempotence};

/// Human-readable explanation of why a [`Predicate`] rejected its subject.
///
/// Equality is **pointer identity** of the `&'static str`, not string content: two
/// `DeniedReason`s are equal only when they point at the same string data. Separate
/// literals with identical text may or may not be merged by the compiler, and
/// references to a `const` are not guaranteed to share an address either. Declare
/// each reason once as a `static` and compare against that `static`.
///
/// ```
/// use wakuwaku_proof::DeniedReason;
///
/// static BANNED: DeniedReason = DeniedReason("user is banned");
///
/// let reason = BANNED;
/// assert_eq!(reason, BANNED);
/// ```
#[derive(Debug, Copy, Clone)]
pub struct DeniedReason(pub &'static str);

impl PartialEq<Self> for DeniedReason {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.0, other.0)
    }
}

impl Eq for DeniedReason {}

/// Failure of a [`Predicate`] check.
#[derive(Debug)]
pub enum ValidateError<E> {
    /// The proposition could not be decided, e.g. a backing service was unreachable.
    /// Says nothing about whether the subject is valid.
    IoError(E),
    /// The proposition was decided and does not hold for the subject.
    Deny(DeniedReason),
}

/// Environment a [`Predicate`] reads while deciding: configuration, connection
/// handles, caches, and so on.
pub trait ValidateContext {
    /// Error produced when deciding fails for reasons unrelated to the subject.
    /// Surfaces as [`ValidateError::IoError`].
    type IoError;
}

/// How combinators ([`And`], [`Or`]) evaluate their operands.
///
/// The order is forwarded unchanged to nested predicates. Leaf predicates receive it
/// too and may ignore it.
#[derive(Debug, Clone, Copy)]
pub enum CheckOrder {
    /// Poll both operands concurrently on the current task with [`tokio::join!`].
    /// Both run to completion even when one fails early. Nothing is spawned.
    Parallel,
    /// Await the operands one after another, left to right. Whether the right operand
    /// is skipped depends on the combinator; see [`And`] and [`Or`].
    Sequential,
}

/// A proposition about `T`, decided at runtime.
///
/// Implementors are usually zero-sized marker types: `check` takes no `self`, so all
/// state the decision needs lives in [`Predicate::Ctx`]. Use [`prove`] to run a
/// predicate and obtain a [`Proven`].
pub trait Predicate<T> {
    /// Context the check reads, which also fixes the I/O error type.
    type Ctx: ValidateContext;

    /// Decides whether the proposition holds for `subject`.
    ///
    /// Returns `Ok(())` if it holds, [`ValidateError::Deny`] if it does not, and
    /// [`ValidateError::IoError`] if it could not be decided. `order` is only
    /// meaningful to predicates that compose others; see [`CheckOrder`].
    ///
    /// The returned future must be `Send`.
    fn check(
        subject: &T,
        ctx: &Self::Ctx,
        order: CheckOrder,
    ) -> impl Future<Output = Result<(), PredicateError<Self, T>>> + Send;
}

/// The [`ValidateError`] produced by predicate `P` checking a `T`.
type PredicateError<P, T> = ValidateError<<<P as Predicate<T>>::Ctx as ValidateContext>::IoError>;

/// Evidence that predicate `P` held for the wrapped subject.
///
/// The field is private. Outside this crate a `Proven` can only come from [`prove`],
/// or from an existing proof via [`Proven::from_a`] / [`Proven::from_b`] (for [`Or`]),
/// [`Proven::from_both`] / [`Proven::into_a`] / [`Proven::into_b`] (for [`And`]), or
/// [`Proven::derive`] (see [`TermDerive`]).
///
/// The subject is owned and only exposed through `&T`, so it cannot be mutated after
/// the check (interior mutability excepted). The evidence reflects the moment of the
/// check: if `P` depends on external state, such as a database row, that state may
/// have changed since.
pub struct Proven<T, P> {
    subject: T,
    // `fn() -> P` keeps `P` out of auto-trait and drop-check reasoning: `P` is a
    // marker, never stored.
    _p: PhantomData<fn() -> P>,
}

impl<T, P> Proven<T, P> {
    /// The validated subject.
    pub fn subject(&self) -> &T {
        &self.subject
    }

    /// Converts this into a proof of `Q` along the law `R`, without rechecking.
    ///
    /// `R` is normally inferred: write `proof.derive::<Q, _>()`, or just
    /// `proof.derive()` where the target type is known. See [`TermDerive`].
    pub fn derive<Q, R>(self) -> Proven<T, Q>
    where
        P: TermDerive<Q, T, R>,
    {
        P::term_derive(self)
    }

    /// Relabels the proof as `Q` without checking anything.
    ///
    /// Private on purpose: every caller must justify that `P` implies `Q`.
    fn relabel<Q>(self) -> Proven<T, Q> {
        Proven {
            subject: self.subject,
            _p: PhantomData,
        }
    }
}

/// Checks `P` against `subject` and, if it holds, wraps the subject in [`Proven`].
///
/// `check_order` is passed to [`Predicate::check`] and controls how combinators
/// evaluate their operands.
///
/// # Errors
///
/// Returns the error from [`Predicate::check`]. The subject is dropped in that case.
pub async fn prove<T, P: Predicate<T>>(
    subject: T,
    ctx: &P::Ctx,
    check_order: CheckOrder,
) -> Result<Proven<T, P>, PredicateError<P, T>> {
    P::check(&subject, ctx, check_order).await?;
    Ok(Proven {
        subject,
        _p: PhantomData,
    })
}

/// Conjunction: holds when both `A` and `B` hold for the subject.
///
/// The context is `A::Ctx`. `B`'s context is built once per check by cloning it and
/// converting with [`From`], even when `B` ends up not running. `B`'s I/O errors are
/// converted into `A`'s.
///
/// - [`CheckOrder::Sequential`]: `A` first; if it fails, its error is returned and
///   `B` is not run.
/// - [`CheckOrder::Parallel`]: both run to completion. If both fail, `A`'s error is
///   returned and `B`'s is discarded.
///
/// Proofs of both operands about equal subjects combine into a proof of the
/// conjunction with [`Proven::from_both`]. A proof of the conjunction can be weakened
/// to a proof of either operand without rechecking: see [`Proven::into_a`] and
/// [`Proven::into_b`].
pub struct And<A, B>(PhantomData<(A, B)>);

impl<T: Eq, A, B> Proven<T, And<A, B>> {
    /// `A` and `B` held for equal subjects, so `A ∧ B` holds.
    ///
    /// The subjects are compared with [`Eq`]; the result keeps `a`'s subject and drops
    /// `b`'s.
    ///
    /// This is only as sound as `T`'s [`Eq`]: if two values compare equal yet a
    /// predicate distinguishes them (a custom `Eq` that ignores a field `B` inspects,
    /// for instance), the resulting proof may not hold for the kept subject.
    ///
    /// # Errors
    ///
    /// Returns both proofs unchanged if the subjects differ.
    pub fn from_both(
        a: Proven<T, A>,
        b: Proven<T, B>,
    ) -> Result<Self, Mismatched<T, A, B>> {
        if a.subject != b.subject {
            return Err((a, b));
        }
        Ok(Proven {
            subject: a.subject,
            _p: PhantomData,
        })
    }
}

/// Both proofs handed back by [`Proven::from_both`] when their subjects differ.
type Mismatched<T, A, B> = (Proven<T, A>, Proven<T, B>);

impl<T, A, B> Proven<T, And<A, B>> {
    /// `A ∧ B` held for the subject, so `A` holds.
    pub fn into_a(self) -> Proven<T, A> {
        Proven {
            subject: self.subject,
            _p: PhantomData,
        }
    }

    /// `A ∧ B` held for the subject, so `B` holds.
    pub fn into_b(self) -> Proven<T, B> {
        Proven {
            subject: self.subject,
            _p: PhantomData,
        }
    }
}

impl<T, A: Predicate<T>, B: Predicate<T>> Predicate<T> for And<A, B>
where
    A::Ctx: Send + Sync + Clone,
    B::Ctx: From<A::Ctx> + Send + Sync,
    <B::Ctx as ValidateContext>::IoError: Into<<A::Ctx as ValidateContext>::IoError> + Send + Sync,
    <A::Ctx as ValidateContext>::IoError: Send,
    A: Sync,
    T: Sync,
{
    type Ctx = A::Ctx;

    async fn check(
        s: &T,
        ctx: &Self::Ctx,
        check_order: CheckOrder,
    ) -> Result<(), PredicateError<A, T>> {
        let ctx_b: B::Ctx = ctx.clone().into();
        match check_order {
            CheckOrder::Parallel => {
                let (a, b) = tokio::join!(
                    A::check(s, ctx, CheckOrder::Parallel),
                    B::check(s, &ctx_b, CheckOrder::Parallel),
                );
                a?;
                match b {
                    Ok(()) => Ok(()),
                    Err(ValidateError::Deny(s)) => Err(ValidateError::Deny(s)),
                    Err(ValidateError::IoError(e)) => Err(ValidateError::IoError(e.into())),
                }
            }
            CheckOrder::Sequential => {
                A::check(s, ctx, CheckOrder::Sequential).await?;
                B::check(s, &ctx_b, CheckOrder::Sequential)
                    .await
                    .map_err(|e| match e {
                        ValidateError::Deny(s) => ValidateError::Deny(s),
                        ValidateError::IoError(e) => ValidateError::IoError(e.into()),
                    })?;
                Ok(())
            }
        }
    }
}

/// Disjunction: holds when `A` or `B` (or both) hold for the subject.
///
/// The context is `A::Ctx`. `B`'s context is built once per check by cloning it and
/// converting with [`From`].
///
/// In both [`CheckOrder`]s, both operands run to completion; `Sequential` does not
/// skip `B` when `A` holds. If either holds, the check succeeds and any error from the
/// other operand, including an I/O error, is discarded. If both fail, `A`'s error is
/// returned and `B`'s is discarded, which is why `B`'s I/O error needs no conversion.
///
/// A proof of either operand can be weakened to a proof of the disjunction without
/// rechecking: see [`Proven::from_a`] and [`Proven::from_b`].
pub struct Or<A, B>(PhantomData<(A, B)>);

impl<T, A, B> Proven<T, Or<A, B>> {
    /// `A` held for the subject, so `A ∨ B` holds.
    pub fn from_a(a: Proven<T, A>) -> Self {
        Proven {
            subject: a.subject,
            _p: PhantomData,
        }
    }

    /// `B` held for the subject, so `A ∨ B` holds.
    pub fn from_b(b: Proven<T, B>) -> Self {
        Proven {
            subject: b.subject,
            _p: PhantomData,
        }
    }
}

impl<T, A: Predicate<T>, B: Predicate<T>> Predicate<T> for Or<A, B>
where
    A::Ctx: Send + Sync + Clone,
    B::Ctx: From<A::Ctx> + Send + Sync,
    <B::Ctx as ValidateContext>::IoError: Send + Sync,
    <A::Ctx as ValidateContext>::IoError: Send,
    A: Sync,
    T: Sync,
{
    type Ctx = A::Ctx;

    async fn check(
        s: &T,
        ctx: &Self::Ctx,
        check_order: CheckOrder,
    ) -> Result<(), PredicateError<A, T>> {
        let ctx_b: B::Ctx = ctx.clone().into();
        match check_order {
            CheckOrder::Parallel => {
                let (a, b) = tokio::join!(
                    A::check(s, ctx, CheckOrder::Parallel),
                    B::check(s, &ctx_b, CheckOrder::Parallel),
                );
                match (a, b) {
                    (_, Ok(())) | (Ok(()), _) => Ok(()),
                    (Err(e), Err(_)) => Err(e),
                }
            }
            CheckOrder::Sequential => {
                let a = A::check(s, ctx, CheckOrder::Sequential).await;
                let b = B::check(s, &ctx_b, CheckOrder::Sequential).await;
                match (a, b) {
                    (_, Ok(())) | (Ok(()), _) => Ok(()),
                    (Err(e), Err(_)) => Err(e),
                }
            }
        }
    }
}

/// A law of inference: whenever `Self` holds for a subject, `P` holds for it too.
///
/// [`term_derive`](Self::term_derive) turns a proof of `Self` into a proof of `P`
/// without rechecking, so it performs no I/O and cannot fail. [`Proven::derive`] is
/// the method-call form. The derived proof is about the same subject at the same
/// moment as the original, with the same caveats (see [`Proven`]).
///
/// `R` names the law that justifies the step. Without it, laws would be overlapping
/// impls of one trait: [`Commute`] and [`Associate`] both rewrite `(A ∧ A) ∧ A` into
/// `A ∧ (A ∧ A)`, which Rust rejects. `R` is normally inferred from the source and
/// target types; when more than one law fits, as in that example, name it:
/// `proof.derive::<_, Associate>()`.
///
/// # Provided laws
///
/// The rule types live in [`boolean_algebra`].
///
/// `∧` is [`And`] and `∨` is [`Or`]. Every `⇔` is two impls, one per direction.
///
/// | `R`             | Laws                                                                 |
/// |-----------------|----------------------------------------------------------------------|
/// | [`Commute`]     | `A ∧ B ⇒ B ∧ A`, `A ∨ B ⇒ B ∨ A`                                     |
/// | [`Associate`]   | `(A ∧ B) ∧ C ⇔ A ∧ (B ∧ C)`, `(A ∨ B) ∨ C ⇔ A ∨ (B ∨ C)`             |
/// | [`Idempotence`] | `A ∧ A ⇔ A`, `A ∨ A ⇔ A`                                             |
/// | [`Absorb`]      | `A ∧ (A ∨ B) ⇔ A`, `A ∨ (A ∧ B) ⇔ A`                                 |
/// | [`Distribute`]  | `A ∧ (B ∨ C) ⇔ (A ∧ B) ∨ (A ∧ C)`, `A ∨ (B ∧ C) ⇔ (A ∨ B) ∧ (A ∨ C)` |
///
/// There is no negation predicate, so laws involving complement are absent.
/// Elimination (`A ∧ B ⇒ A`) and introduction (`A ⇒ A ∨ B`; `A`, `B` ⇒ `A ∧ B`) are
/// the inherent methods [`Proven::into_a`], [`Proven::into_b`], [`Proven::from_a`],
/// [`Proven::from_b`] and [`Proven::from_both`]. Laws rewrite the whole proposition
/// only; there is no rule for rewriting inside an operand.
///
/// `Self` must itself be a [`Predicate<T>`], so a law applies only where the source
/// proposition implements [`Predicate<T>`], i.e. where its operands' contexts are
/// compatible (see [`And`] and [`Or`]). The target need not be checkable.
///
/// # Implementing
///
/// `term_derive` must return a [`Proven<T, P>`], which outside this crate can only be
/// obtained from [`prove`] or by transforming existing proofs. An implementation
/// therefore cannot fabricate evidence, and adding laws for your own predicates is
/// sound. Give each new law its own rule type for `R`.
///
/// # Example
///
/// ```
/// use std::convert::Infallible;
/// use wakuwaku_proof::{
///     And, CheckOrder, DeniedReason, Or, Predicate, Proven, ValidateContext, ValidateError,
///     prove,
/// };
///
/// #[derive(Clone)]
/// struct NoCtx;
///
/// impl ValidateContext for NoCtx {
///     type IoError = Infallible;
/// }
///
/// static ODD: DeniedReason = DeniedReason("odd");
/// static LARGE: DeniedReason = DeniedReason("large");
/// static ZERO: DeniedReason = DeniedReason("zero");
///
/// fn decide(holds: bool, reason: DeniedReason) -> Result<(), ValidateError<Infallible>> {
///     if holds { Ok(()) } else { Err(ValidateError::Deny(reason)) }
/// }
///
/// struct Even;
/// struct Small;
/// struct Positive;
///
/// impl Predicate<u32> for Even {
///     type Ctx = NoCtx;
///     async fn check(n: &u32, _: &NoCtx, _: CheckOrder) -> Result<(), ValidateError<Infallible>> {
///         decide(n % 2 == 0, ODD)
///     }
/// }
///
/// impl Predicate<u32> for Small {
///     type Ctx = NoCtx;
///     async fn check(n: &u32, _: &NoCtx, _: CheckOrder) -> Result<(), ValidateError<Infallible>> {
///         decide(*n < 100, LARGE)
///     }
/// }
///
/// impl Predicate<u32> for Positive {
///     type Ctx = NoCtx;
///     async fn check(n: &u32, _: &NoCtx, _: CheckOrder) -> Result<(), ValidateError<Infallible>> {
///         decide(*n > 0, ZERO)
///     }
/// }
///
/// // Downstream code states its requirement in its own shape.
/// fn accept(n: Proven<u32, Or<And<Even, Positive>, And<Even, Small>>>) -> u32 {
///     *n.subject()
/// }
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let Ok(n) = prove::<_, And<Even, Or<Small, Positive>>>(42, &NoCtx, CheckOrder::Sequential).await
/// else {
///     unreachable!()
/// };
///
/// // Even ∧ (Small ∨ Positive) ⇒ (Even ∧ Small) ∨ (Even ∧ Positive), by `Distribute`.
/// let n = n.derive::<Or<And<Even, Small>, And<Even, Positive>>, _>();
/// // ⇒ (Even ∧ Positive) ∨ (Even ∧ Small), by `Commute`; the target is inferred.
/// assert_eq!(accept(n.derive()), 42);
/// # }
/// ```
pub trait TermDerive<P, T, R>: Predicate<T> + Sized {
    /// `Self` held for the subject, so `P` holds for it.
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, P>;
}

