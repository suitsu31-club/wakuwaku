# Wakuwaku Proof

Proof-carrying validation.

A [`Predicate`] is a proposition about a subject of type `T` that is decided at
runtime, possibly with I/O (a database lookup, a remote call, …). Running it
through [`prove`] either fails with a [`ValidateError`] or hands back a
[`Proven<T, P>`]: the subject together with type-level evidence that `P` held.

Downstream code that requires a validated value takes `Proven<T, P>` instead of
`T`, so "forgot to validate" becomes a type error rather than a runtime bug.

Predicates compose with [`And`] and [`Or`]. Both evaluate their operands in the
[`CheckOrder`] passed to [`prove`], and forward that order to nested predicates.

A proof converts into a proof of an equivalent or weaker proposition without
rechecking through [`TermDerive`] / [`Proven::derive`], which implement the laws
of Boolean algebra over [`And`] and [`Or`] (commutativity, associativity, …).

# Contexts and errors

Every predicate names a [`ValidateContext`] (`Predicate::Ctx`) holding whatever
it needs to decide (configuration, connection handles, …), plus the I/O error
type that deciding may produce. A check has three outcomes:

- `Ok(())`: the proposition holds.
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
    ) -> Result<(), ValidateError<Infallible>> {
        if name.is_empty() { Err(ValidateError::Deny(EMPTY)) } else { Ok(()) }
    }
}

struct ShortEnough;

impl Predicate<String> for ShortEnough {
    type Ctx = Limits;

    async fn check(
        name: &String,
        limits: &Limits,
        _: CheckOrder,
    ) -> Result<(), ValidateError<Infallible>> {
        if name.len() > limits.max_len { Err(ValidateError::Deny(TOO_LONG)) } else { Ok(()) }
    }
}

type ValidUsername = And<NonEmpty, ShortEnough>;

// Only callable with a name that has been through `prove`.
fn register(name: Proven<String, ValidUsername>) -> usize {
    name.subject().len()
}

#[tokio::main]
async fn main() {
    let limits = Limits { max_len: 8 };
    
    let Ok(name) = prove::<_, ValidUsername>("haruki".to_owned(), &limits, CheckOrder::Sequential).await
    else {
        unreachable!()
    };
    assert_eq!(register(name), 6);
    
    let denied = prove::<_, ValidUsername>(String::new(), &limits, CheckOrder::Parallel).await;
    assert!(matches!(denied, Err(ValidateError::Deny(reason)) if reason == EMPTY));
}
```
