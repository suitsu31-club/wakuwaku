#![deny(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::panic_in_result_fn,
    clippy::arithmetic_side_effects
)]
#![warn(missing_docs)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/suitsu31-club/wakuwaku-docs/main/icon.svg"
)]
#![doc = include_str!("../README.md")]

pub mod boolean_algebra;

use std::borrow::Borrow;
use std::convert::Infallible;
use std::fmt::{self, Display, Formatter};
use std::marker::PhantomData;
use std::ops::Deref;

#[cfg(doc)]
use boolean_algebra::{Absorb, Associate, Commute, Distribute, Idempotence};

/// Human-readable explanation of why a [`Predicate`] rejected its subject. Its
/// [`Display`] is the text.
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

impl Display for DeniedReason {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// Failure of a [`Predicate`] check.
///
/// Implements [`std::error::Error`] when `E` is [`Debug`](fmt::Debug) and [`Display`],
/// which includes [`Infallible`], so it propagates with `?` into `Box<dyn Error>` or
/// `anyhow::Error`.
///
/// ```
/// use std::convert::Infallible;
/// use wakuwaku_proof::{DeniedReason, ValidateError};
///
/// static EMPTY: DeniedReason = DeniedReason("username is empty");
///
/// let denied: ValidateError<Infallible> = ValidateError::Deny(EMPTY);
/// assert_eq!(denied.to_string(), "username is empty");
///
/// let io: ValidateError<std::io::Error> =
///     ValidateError::IoError(std::io::Error::other("connection refused"));
/// assert_eq!(io.to_string(), "could not decide the proposition: connection refused");
/// ```
#[derive(Debug, thiserror::Error)]
pub enum ValidateError<E> {
    /// The proposition could not be decided, e.g. a backing service was unreachable.
    /// Says nothing about whether the subject is valid.
    #[error("could not decide the proposition: {0}")]
    IoError(E),
    /// The proposition was decided and does not hold for the subject. Displays as the
    /// [`DeniedReason`]'s text.
    #[error("{0}")]
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
/// The implementing type is also the evidence: when the proposition holds,
/// [`check`](Self::check) returns a value of `Self`. A predicate with nothing to report
/// is a unit struct that returns itself; one that learns something while deciding,
/// such as the row it looked up, carries it in its fields. Use [`prove`] to run a
/// predicate and obtain a [`Proven`], which exposes the evidence by reference through
/// [`Proven::evidence`].
///
/// `check` takes no `self`, so all state the decision needs lives in
/// [`Predicate::Ctx`].
///
/// A value of `Self` on its own proves nothing: anyone can construct a unit-struct
/// predicate, and calling `check` directly returns evidence detached from any subject.
/// Code that requires a validated value must take [`Proven<T, Self>`], not `Self`.
pub trait Predicate<T>: Sized {
    /// Context the check reads, which also fixes the I/O error type.
    type Ctx: ValidateContext;

    /// Decides whether the proposition holds for `subject`.
    ///
    /// Returns the evidence if it holds, [`ValidateError::Deny`] if it does not, and
    /// [`ValidateError::IoError`] if it could not be decided. `order` is only
    /// meaningful to predicates that compose others; see [`CheckOrder`].
    ///
    /// The returned future must be `Send`.
    fn check(
        subject: &T,
        ctx: &Self::Ctx,
        order: CheckOrder,
    ) -> impl Future<Output = Result<Self, PredicateError<Self, T>>> + Send;
}

/// The [`ValidateError`] produced by predicate `P` checking a `T`.
type PredicateError<P, T> = ValidateError<<<P as Predicate<T>>::Ctx as ValidateContext>::IoError>;

/// Evidence that predicate `P` held, as stored in [`Proven`], [`And`] and [`Or`].
///
/// Read it through [`Deref`]. It cannot be taken out by value, and it is not [`Clone`]
/// even when `P` is: only this crate's laws duplicate evidence, and only those whose
/// target needs the same proof twice, which then require `P: Clone` (see
/// [`TermDerive`]).
#[derive(Debug)]
pub struct Evidence<P>(P);

impl<P: Clone> Evidence<P> {
    /// A second copy, for laws whose target contains the same proof twice.
    fn duplicate(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<P> Deref for Evidence<P> {
    type Target = P;

    fn deref(&self) -> &P {
        &self.0
    }
}

/// Proof that predicate `P` held for the wrapped subject, carrying the evidence the
/// check produced.
///
/// The fields are private. Outside this crate a `Proven` can only come from [`prove`],
/// from [`PureVerifier::prove`], or from an existing proof via [`Proven::from_a`] /
/// [`Proven::from_b`] (for [`Or`]), [`Proven::from_both`] / [`Proven::into_a`] /
/// [`Proven::into_b`] (for [`And`]), or [`Proven::derive`] (see [`TermDerive`]).
///
/// The subject is owned and only exposed through `&T`, so it cannot be mutated after
/// the check (interior mutability excepted). The evidence is only exposed through
/// `&P`. Both reflect the moment of the check: if `P` depends on external state, such
/// as a database row, that state may have changed since.
///
/// `Proven<T, P>` is [`Send`] / [`Sync`] when both `T` and `P` are, and
/// [`Debug`](fmt::Debug) when both are.
#[derive(Debug)]
pub struct Proven<T, P> {
    subject: T,
    evidence: Evidence<P>,
}

impl<T, P> Proven<T, P> {
    /// The validated subject.
    pub fn subject(&self) -> &T {
        &self.subject
    }

    /// The evidence that `P` held: what `P`'s check returned, or what a law built from
    /// the evidence of the proof it was derived from.
    pub fn evidence(&self) -> &P {
        &self.evidence
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

    /// Converts the proof into a proof of `Q` about the same subject, building `Q`'s
    /// evidence from `P`'s with `f`. Nothing is checked.
    ///
    /// Private on purpose: every caller must justify that `P` implies `Q` and that `f`
    /// turns evidence of `P` into evidence of `Q`.
    fn map_evidence<Q>(self, f: impl FnOnce(P) -> Q) -> Proven<T, Q> {
        Proven {
            subject: self.subject,
            evidence: Evidence(f(self.evidence.0)),
        }
    }
}

/// Checks `P` against `subject` and, if it holds, wraps the subject and the evidence
/// in [`Proven`].
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
    let evidence = P::check(&subject, ctx, check_order).await?;
    Ok(Proven {
        subject,
        evidence: Evidence(evidence),
    })
}

/// A pure check that `P` holds for a `T`, given as a function pointer. Together with
/// [`prove`], one of the two ways to obtain a [`Proven`].
///
/// The function sees only the subject: no context, no I/O, no [`CheckOrder`]. It returns
/// the evidence, or [`ValidateError::Deny`]; the [`Infallible`] I/O error rules out
/// [`ValidateError::IoError`]. `P` is only the evidence type and need not implement
/// [`Predicate`].
///
/// The function takes the subject as `&B`, a borrowed form of `T` obtained through
/// [`Borrow`]. `B` defaults to `T`; set it to check a `String` through `&str`, a
/// `Vec<u8>` through `&[u8]`, and so on. The proof is only as sound as `T`'s
/// [`Borrow<B>`]: the function decides `P` for the borrowed view, and [`Borrow`]'s
/// contract is what makes that view stand for the owned subject.
///
/// [`new`](Self::new) is a `const fn` and a function pointer is always [`Send`] and
/// [`Sync`], whatever `T` and `P` are, so a verifier can be declared as a `static` or a
/// `const`.
///
/// Whoever writes the function decides what it proves, exactly as with a [`Predicate`]
/// implementation. The function must return a `P`, so only code that can construct a `P`
/// can write a verifier for it: give the evidence type a private field to keep its
/// proofs in its defining module.
///
/// ```
/// use std::convert::Infallible;
/// use wakuwaku_proof::{DeniedReason, PureVerifier, ValidateError};
///
/// static EMPTY: DeniedReason = DeniedReason("username is empty");
///
/// pub struct NonEmpty(());
///
/// fn non_empty(name: &str) -> Result<NonEmpty, ValidateError<Infallible>> {
///     if name.is_empty() { Err(ValidateError::Deny(EMPTY)) } else { Ok(NonEmpty(())) }
/// }
///
/// // Owns a `String`, checks it as `&str`.
/// pub static NON_EMPTY: PureVerifier<String, NonEmpty, str> = PureVerifier::new(non_empty);
/// // Same check, same evidence, for `Box<str>` subjects.
/// pub const NON_EMPTY_BOXED: PureVerifier<Box<str>, NonEmpty, str> =
///     PureVerifier::new(non_empty);
///
/// let name = NON_EMPTY.prove("haruki".to_owned());
/// assert!(matches!(name, Ok(proof) if proof.subject() == "haruki"));
///
/// // `Deny` is the only possible error.
/// let Err(ValidateError::Deny(reason)) = NON_EMPTY_BOXED.prove(Box::from("")) else {
///     unreachable!()
/// };
/// assert_eq!(reason, EMPTY);
/// ```
pub struct PureVerifier<T, P, B: ?Sized = T> {
    check: fn(&B) -> Result<P, ValidateError<Infallible>>,
    subject: PhantomData<fn() -> T>,
}

impl<T, P, B: ?Sized> PureVerifier<T, P, B> {
    /// A verifier that runs `check`.
    pub const fn new(check: fn(&B) -> Result<P, ValidateError<Infallible>>) -> Self {
        Self {
            check,
            subject: PhantomData,
        }
    }
}

impl<T: Borrow<B>, P, B: ?Sized> PureVerifier<T, P, B> {
    /// Runs the check against `subject` and, if it holds, wraps the subject and the
    /// evidence in [`Proven`].
    ///
    /// # Errors
    ///
    /// Returns the [`ValidateError::Deny`] from the check. The subject is dropped in that
    /// case.
    pub fn prove(&self, subject: T) -> Result<Proven<T, P>, ValidateError<Infallible>> {
        let evidence = (self.check)(subject.borrow())?;
        Ok(Proven {
            subject,
            evidence: Evidence(evidence),
        })
    }
}

/// Conjunction: holds when both `A` and `B` hold for the subject. The evidence is the
/// evidence of both operands.
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
#[derive(Debug)]
pub struct And<A, B>(pub Evidence<A>, pub Evidence<B>);

impl<T: Eq, A, B> Proven<T, And<A, B>> {
    /// `A` and `B` held for equal subjects, so `A ∧ B` holds.
    ///
    /// The subjects are compared with [`Eq`]; the result keeps `a`'s subject and drops
    /// `b`'s. Both evidences are kept.
    ///
    /// This is only as sound as `T`'s [`Eq`]: if two values compare equal yet a
    /// predicate distinguishes them (a custom `Eq` that ignores a field `B` inspects,
    /// for instance), the resulting proof may not hold for the kept subject.
    ///
    /// # Errors
    ///
    /// Returns both proofs unchanged if the subjects differ.
    pub fn from_both(a: Proven<T, A>, b: Proven<T, B>) -> Result<Self, Mismatched<T, A, B>> {
        if a.subject != b.subject {
            return Err((a, b));
        }
        Ok(Proven {
            subject: a.subject,
            evidence: Evidence(And(a.evidence, b.evidence)),
        })
    }
}

/// Both proofs handed back by [`Proven::from_both`] when their subjects differ.
type Mismatched<T, A, B> = (Proven<T, A>, Proven<T, B>);

impl<T, A, B> Proven<T, And<A, B>> {
    /// `A ∧ B` held for the subject, so `A` holds. `B`'s evidence is dropped.
    pub fn into_a(self) -> Proven<T, A> {
        self.map_evidence(|And(Evidence(a), _)| a)
    }

    /// `A ∧ B` held for the subject, so `B` holds. `A`'s evidence is dropped.
    pub fn into_b(self) -> Proven<T, B> {
        self.map_evidence(|And(_, Evidence(b))| b)
    }
}

impl<T, A: Predicate<T>, B: Predicate<T>> Predicate<T> for And<A, B>
where
    A::Ctx: Send + Sync + Clone,
    B::Ctx: From<A::Ctx> + Send + Sync,
    <B::Ctx as ValidateContext>::IoError: Into<<A::Ctx as ValidateContext>::IoError> + Send + Sync,
    <A::Ctx as ValidateContext>::IoError: Send,
    A: Send + Sync,
    B: Send,
    T: Sync,
{
    type Ctx = A::Ctx;

    async fn check(
        s: &T,
        ctx: &Self::Ctx,
        check_order: CheckOrder,
    ) -> Result<Self, PredicateError<A, T>> {
        let ctx_b: B::Ctx = ctx.clone().into();
        let (a, b) = match check_order {
            CheckOrder::Parallel => {
                let (a, b) = tokio::join!(
                    A::check(s, ctx, CheckOrder::Parallel),
                    B::check(s, &ctx_b, CheckOrder::Parallel),
                );
                (a?, b)
            }
            CheckOrder::Sequential => {
                let a = A::check(s, ctx, CheckOrder::Sequential).await?;
                (a, B::check(s, &ctx_b, CheckOrder::Sequential).await)
            }
        };
        let b = b.map_err(|e| match e {
            ValidateError::Deny(r) => ValidateError::Deny(r),
            ValidateError::IoError(e) => ValidateError::IoError(e.into()),
        })?;
        Ok(And(Evidence(a), Evidence(b)))
    }
}

/// Disjunction: holds when `A` or `B` (or both) hold for the subject. The evidence is
/// that of whichever operands are known to hold.
///
/// The context is `A::Ctx`. `B`'s context is built once per check by cloning it and
/// converting with [`From`].
///
/// In both [`CheckOrder`]s, both operands run to completion; `Sequential` does not
/// skip `B` when `A` holds. If both hold, the evidence is [`Or::Both`]. If exactly one
/// holds, the evidence is that side's, and the other operand's error, including an
/// I/O error, is discarded. If both fail, `A`'s error is returned and `B`'s is
/// discarded, which is why `B`'s I/O error needs no conversion.
///
/// A proof of either operand can be weakened to a proof of the disjunction without
/// rechecking: see [`Proven::from_a`] and [`Proven::from_b`].
#[derive(Debug)]
pub enum Or<A, B> {
    /// Evidence for `A` only. Says nothing about `B`: it failed or could not be
    /// decided when checked, or it was never checked (see [`Proven::from_a`]).
    Left(Evidence<A>),
    /// Evidence for `B` only. Says nothing about `A`.
    Right(Evidence<B>),
    /// Evidence for both operands.
    Both(Evidence<A>, Evidence<B>),
}

impl<T, A, B> Proven<T, Or<A, B>> {
    /// `A` held for the subject, so `A ∨ B` holds. The evidence is [`Or::Left`].
    pub fn from_a(a: Proven<T, A>) -> Self {
        a.map_evidence(|a| Or::Left(Evidence(a)))
    }

    /// `B` held for the subject, so `A ∨ B` holds. The evidence is [`Or::Right`].
    pub fn from_b(b: Proven<T, B>) -> Self {
        b.map_evidence(|b| Or::Right(Evidence(b)))
    }
}

impl<T, A: Predicate<T>, B: Predicate<T>> Predicate<T> for Or<A, B>
where
    A::Ctx: Send + Sync + Clone,
    B::Ctx: From<A::Ctx> + Send + Sync,
    <B::Ctx as ValidateContext>::IoError: Send + Sync,
    <A::Ctx as ValidateContext>::IoError: Send,
    A: Send + Sync,
    B: Send,
    T: Sync,
{
    type Ctx = A::Ctx;

    async fn check(
        s: &T,
        ctx: &Self::Ctx,
        check_order: CheckOrder,
    ) -> Result<Self, PredicateError<A, T>> {
        let ctx_b: B::Ctx = ctx.clone().into();
        let (a, b) = match check_order {
            CheckOrder::Parallel => tokio::join!(
                A::check(s, ctx, CheckOrder::Parallel),
                B::check(s, &ctx_b, CheckOrder::Parallel),
            ),
            CheckOrder::Sequential => (
                A::check(s, ctx, CheckOrder::Sequential).await,
                B::check(s, &ctx_b, CheckOrder::Sequential).await,
            ),
        };
        match (a, b) {
            (Ok(a), Ok(b)) => Ok(Or::Both(Evidence(a), Evidence(b))),
            (Ok(a), Err(_)) => Ok(Or::Left(Evidence(a))),
            (Err(_), Ok(b)) => Ok(Or::Right(Evidence(b))),
            (Err(e), Err(_)) => Err(e),
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
/// `R` names the law that justifies the step, so laws do not overlap as impls. It is
/// normally inferred from the source and target types; when more than one law fits
/// (both [`Commute`] and [`Associate`] rewrite `(A ∧ A) ∧ A` into `A ∧ (A ∧ A)`), name
/// it: `proof.derive::<_, Associate>()`.
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
/// # Evidence
///
/// Laws carry the evidence over to the target (each predicate is its own evidence; see
/// [`Predicate`]). Evidence the target has no room for is dropped: `A ∧ A ⇒ A` keeps
/// the left proof, as does `A ∨ A ⇒ A` when both are present, and
/// `A ∧ (B ∨ C) ⇒ (A ∧ B) ∨ (A ∧ C)` keeps only `A ∧ B` when both `B` and `C` held.
/// Laws that build a disjunction from a single proof (`A ⇒ A ∨ A`,
/// `A ⇒ A ∨ (A ∧ B)`) produce [`Or::Left`].
///
/// Laws whose target holds the same proof twice duplicate it and therefore require
/// `A: Clone`: `A ⇒ A ∧ A`, `A ⇒ A ∧ (A ∨ B)` and `A ∨ (B ∧ C) ⇒ (A ∨ B) ∧ (A ∨ C)`.
///
/// # Implementing
///
/// `term_derive` must return a [`Proven<T, P>`], which outside this crate can only be
/// obtained from [`prove`], from [`PureVerifier::prove`], or by transforming existing
/// proofs. An implementation therefore cannot fabricate evidence, and adding laws for
/// your own predicates is sound. Give each new law its own rule type for `R`.
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
/// fn decide<P>(holds: bool, proof: P, reason: DeniedReason) -> Result<P, ValidateError<Infallible>> {
///     if holds { Ok(proof) } else { Err(ValidateError::Deny(reason)) }
/// }
///
/// struct Even;
/// struct Small;
/// struct Positive;
///
/// impl Predicate<u32> for Even {
///     type Ctx = NoCtx;
///     async fn check(n: &u32, _: &NoCtx, _: CheckOrder) -> Result<Self, ValidateError<Infallible>> {
///         decide(n % 2 == 0, Even, ODD)
///     }
/// }
///
/// impl Predicate<u32> for Small {
///     type Ctx = NoCtx;
///     async fn check(n: &u32, _: &NoCtx, _: CheckOrder) -> Result<Self, ValidateError<Infallible>> {
///         decide(*n < 100, Small, LARGE)
///     }
/// }
///
/// impl Predicate<u32> for Positive {
///     type Ctx = NoCtx;
///     async fn check(n: &u32, _: &NoCtx, _: CheckOrder) -> Result<Self, ValidateError<Infallible>> {
///         decide(*n > 0, Positive, ZERO)
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
/// // 42 is both small and positive, but the law keeps only the `Even ∧ Small` side.
/// assert!(matches!(n.evidence(), Or::Left(_)));
/// // ⇒ (Even ∧ Positive) ∨ (Even ∧ Small), by `Commute`; the target is inferred.
/// assert_eq!(accept(n.derive()), 42);
/// # }
/// ```
pub trait TermDerive<P, T, R>: Predicate<T> {
    /// `Self` held for the subject, so `P` holds for it.
    fn term_derive(proof: Proven<T, Self>) -> Proven<T, P>;
}
