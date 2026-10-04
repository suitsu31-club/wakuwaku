use crate::events::{
    EventAlgebraicProperties, EventAssociativity, EventAtomicOrdering, EventOptimize, IntoEventBody,
};

pub struct IdempotentEvent<T: Eq> {
    event_atomic_ordering: EventAtomicOrdering,
    event: T,
}

impl<T: Eq> IdempotentEvent<T> {
    pub fn new(event_atomic_ordering: EventAtomicOrdering, event: T) -> Self {
        Self {
            event_atomic_ordering,
            event,
        }
    }
}

impl<T: Eq> EventOptimize for IdempotentEvent<T> {
    type EventInner = T;
    fn get_algebraic_properties(&self) -> EventAlgebraicProperties {
        EventAlgebraicProperties {
            associativity: EventAssociativity::Idempotent,
            atomic_level: self.event_atomic_ordering,
        }
    }
    fn into_inner(self) -> Self::EventInner {
        self.event
    }
}

impl<T: Eq + IntoEventBody> IntoEventBody for IdempotentEvent<T> {
    type Target = T::Target;
    fn into_event_body(self) -> Self::Target {
        self.event.into_event_body()
    }
}
