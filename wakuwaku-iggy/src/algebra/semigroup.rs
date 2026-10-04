use crate::events::{
    EventAlgebraicProperties, EventAssociativity, EventAtomicOrdering, EventOptimize, IntoEventBody,
};

pub trait EventSemigroup {
    fn combine(&self, other: Self) -> Self;
}

pub struct SemigroupOptimize<T: EventSemigroup> {
    event_atomic_ordering: EventAtomicOrdering,
    event: T,
}

impl<T: EventSemigroup> SemigroupOptimize<T> {
    pub fn new(event: T, ordering: EventAtomicOrdering) -> Self {
        Self {
            event_atomic_ordering: ordering,
            event,
        }
    }
}

impl<T: EventSemigroup> EventOptimize for SemigroupOptimize<T> {
    type EventInner = T;
    fn get_algebraic_properties(&self) -> EventAlgebraicProperties {
        EventAlgebraicProperties {
            associativity: EventAssociativity::Associative,
            atomic_level: self.event_atomic_ordering,
        }
    }
    fn into_inner(self) -> Self::EventInner {
        self.event
    }
}

impl<T: EventSemigroup + IntoEventBody> IntoEventBody for SemigroupOptimize<T> {
    type Target = T::Target;
    fn into_event_body(self) -> Self::Target {
        self.event.into_event_body()
    }
}
