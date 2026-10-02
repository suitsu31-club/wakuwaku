use std::marker::PhantomData;

#[derive(Debug, Copy, Clone)]
pub struct DeniedReason(pub &'static str);

impl PartialEq<Self> for DeniedReason {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self.0, other.0)
    }
}

impl Eq for DeniedReason {}

#[derive(Debug)]
pub enum ValidateError<E> {
    IoError(E),
    Deny(DeniedReason),
}

pub trait ValidateContext {
    type IoError;
}

#[derive(Debug, Clone, Copy)]
pub enum CheckOrder {
    Parallel,
    Sequential,
}

/// A proposition about T, decided at runtime.
pub trait Predicate<T> {
    type Ctx: ValidateContext;
    fn check(
        subject: &T,
        ctx: &Self::Ctx,
        order: CheckOrder,
    ) -> impl Future<Output = Result<(), PredicateError<Self, T>>> + Send;
}

type PredicateError<P, T> = ValidateError<<<P as Predicate<T>>::Ctx as ValidateContext>::IoError>;

/// Evidence that P held for `subject`. The field is private, so there is no way to build this outside `prove`.
pub struct Proven<T, P> {
    subject: T,
    _p: PhantomData<fn() -> P>,
}

impl<T, P> Proven<T, P> {
    pub fn subject(&self) -> &T {
        &self.subject
    }
}

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

pub struct And<A, B>(PhantomData<(A, B)>);

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
