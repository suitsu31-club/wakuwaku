# Wakuwaku Proof

Proof-carrying validation: code that requires a validated value takes a
[`Proven<T, P>`] (the subject plus evidence that `P` held) instead of a bare `T`.

- [`Predicate`]: a proposition about a `T`, decided at runtime against a
  [`ValidateContext`], possibly with I/O. [`prove`] runs it.
- [`PureVerifier`]: a pure check as a function pointer, declarable as a
  `static`; [`PureVerifier::prove`] runs it.
- [`ValidateError`]: [`Deny`](ValidateError::Deny) with a [`DeniedReason`] when
  the proposition is false, [`IoError`](ValidateError::IoError) when it could
  not be decided.
- [`And`] / [`Or`]: composition, evaluated in a [`CheckOrder`].
- [`TermDerive`] / [`Proven::derive`]: Boolean-algebra laws that turn a proof
  into a proof of an equivalent or weaker proposition without rechecking (rules
  in [`boolean_algebra`]).

See the [validation guide](https://www.wakuwaku.rs/en/docs/validation).

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
