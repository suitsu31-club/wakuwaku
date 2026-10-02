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

/// A proposition about T, decided at runtime.
pub trait Predicate<T> {
    type Ctx: ValidateContext;
    fn check(
        subject: &T,
        ctx: &Self::Ctx,
    ) -> impl Future<Output = Result<(), ValidateError<<Self::Ctx as ValidateContext>::IoError>>> + Send;
}

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
) -> Result<Proven<T, P>, ValidateError<<P::Ctx as ValidateContext>::IoError>> {
    P::check(&subject, ctx).await?;
    Ok(Proven {
        subject,
        _p: PhantomData,
    })
}
