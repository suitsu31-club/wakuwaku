# Wakuwaku Proof

Proof-carrying validation.

A [`Predicate`] is a proposition about a subject of type `T` that is decided at
runtime, possibly with I/O (a database lookup, a remote call, …). Running it
through [`prove`] either fails with a [`ValidateError`] or hands back a
[`Proven<T, P>`]: the subject together with the evidence that `P` held.

Downstream code that requires a validated value takes `Proven<T, P>` instead of
`T`, so "forgot to validate" becomes a type error rather than a runtime bug.

Predicates compose with [`And`] and [`Or`]. Both evaluate their operands in the
[`CheckOrder`] passed to [`prove`], and forward that order to nested predicates.

A proof converts into a proof of an equivalent or weaker proposition without
rechecking through [`TermDerive`] / [`Proven::derive`], which implement the laws
of Boolean algebra over [`And`] and [`Or`] (commutativity, associativity, …).

# Evidence

The predicate type is its own evidence: a successful check returns a value of
the predicate type. A predicate with nothing to report is a unit struct; one that
learns something while deciding (the row it looked up, a resolved id, …)
carries it in its fields. [`And`] holds the evidence of both operands, and [`Or`]
holds whichever operands are known to hold (`Left`, `Right` or `Both`).

[`Proven::evidence`] gives the evidence by reference only. Each piece is wrapped
in [`Evidence`], which is not `Clone`; only this crate's laws duplicate evidence,
and only where the target needs the same proof twice, which then requires the
evidence to be `Clone`.

A predicate value on its own proves nothing: anyone can construct one, and
calling [`Predicate::check`] directly returns evidence detached from any subject.
Take `Proven<T, P>` in signatures, never a bare `P`.

# Contexts and errors

Every predicate names a [`ValidateContext`] (`Predicate::Ctx`) holding whatever
it needs to decide (configuration, connection handles, …), plus the I/O error
type that deciding may produce. A check has three outcomes:

- `Ok(evidence)`: the proposition holds.
- [`ValidateError::Deny`]: the proposition was decided and is false.
- [`ValidateError::IoError`]: the proposition could not be decided.

# Example

```rust
use std::convert::Infallible;
use wakuwaku_proof::{
    And, CheckOrder, DeniedReason, Predicate, Proven, ValidateContext, ValidateError, prove,
};

#[derive(Clone)]
struct Limits {
    max_len: usize,
}

impl ValidateContext for Limits {
    type IoError = Infallible;
}

static EMPTY: DeniedReason = DeniedReason("username is empty");
static TOO_LONG: DeniedReason = DeniedReason("username is too long");

struct NonEmpty;

impl Predicate<String> for NonEmpty {
    type Ctx = Limits;

    async fn check(
        name: &String,
        _: &Limits,
        _: CheckOrder,
    ) -> Result<Self, ValidateError<Infallible>> {
        if name.is_empty() { Err(ValidateError::Deny(EMPTY)) } else { Ok(NonEmpty) }
    }
}

// Records how many more bytes the name could have taken.
struct ShortEnough {
    spare: usize,
}

impl Predicate<String> for ShortEnough {
    type Ctx = Limits;

    async fn check(
        name: &String,
        limits: &Limits,
        _: CheckOrder,
    ) -> Result<Self, ValidateError<Infallible>> {
        match limits.max_len.checked_sub(name.len()) {
            Some(spare) => Ok(ShortEnough { spare }),
            None => Err(ValidateError::Deny(TOO_LONG)),
        }
    }
}

type ValidUsername = And<NonEmpty, ShortEnough>;

// Only callable with a name that has been through `prove`.
fn register(name: Proven<String, ValidUsername>) -> usize {
    let And(_, short_enough) = name.evidence();
    short_enough.spare
}

#[tokio::main]
async fn main() {
    let limits = Limits { max_len: 8 };
    
    let Ok(name) = prove::<_, ValidUsername>("haruki".to_owned(), &limits, CheckOrder::Sequential).await
    else {
        unreachable!()
    };
    assert_eq!(register(name), 2);
    
    let denied = prove::<_, ValidUsername>(String::new(), &limits, CheckOrder::Parallel).await;
    assert!(matches!(denied, Err(ValidateError::Deny(reason)) if reason == EMPTY));
}
```
