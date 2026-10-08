//! Event traits, their wire envelope and the publisher.

pub mod headers;
pub mod publisher;

pub use publisher::Publisher;

use crate::partition::PartitionKey;
use crate::partition::algebra::Algebra;
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventTypeTag(u32);

impl EventTypeTag {
    pub const fn new(tag: u32) -> Self {
        Self(tag)
    }
    pub const fn get(self) -> u32 {
        self.0
    }
}

pub trait EventBody: kanau::message::MessageDe + kanau::message::MessageSer {}

pub trait IntoEventBody {
    type Target: EventBody;
    fn into_event_body(self) -> Self::Target;
}

impl<T: EventBody> IntoEventBody for T {
    type Target = T;
    fn into_event_body(self) -> Self::Target {
        self
    }
}

pub trait Event: IntoEventBody {
    const TYPE_TAG: EventTypeTag;
    type Key: PartitionKey;
    /// How the consumer may optimize a run of this event's decoded bodies.
    ///
    /// One of the markers in [`algebra`](crate::partition::algebra). The choice is static:
    /// the consumer reduces runs with it, and [`algebraic_properties`]
    /// only copies its [`ASSOCIATIVITY`](Algebra::ASSOCIATIVITY) into the
    /// message header.
    ///
    /// [`algebraic_properties`]: Event::algebraic_properties
    type Algebra: Algebra<Self::Target>;
    fn key(&self) -> Self::Key;
    /// Ordering of this message relative to other messages of the same key.
    fn atomic_ordering(&self) -> EventAtomicOrdering;
    /// Properties written into the message header.
    fn algebraic_properties(&self) -> EventAlgebraicProperties {
        EventAlgebraicProperties {
            atomic_level: self.atomic_ordering(),
            associativity: <Self::Algebra as Algebra<Self::Target>>::ASSOCIATIVITY,
        }
    }
}

#[derive(Debug)]
pub enum EventParseError {
    UnknownEventType,
    UnknownPropertiesVersion,
    BadProperties,
    /// The named header is absent.
    MissingHeader(&'static str),
    /// The named header has the wrong length or content.
    BadHeader(&'static str),
    UnknownRetryVersion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventAlgebraicProperties {
    pub atomic_level: EventAtomicOrdering,
    pub associativity: EventAssociativity,
}

impl EventAlgebraicProperties {
    pub const VERSION: u8 = 1;
    pub const LENGTH: usize = 3;
    pub fn into_bytes(self) -> [u8; Self::LENGTH] {
        [1u8, self.atomic_level as u8, self.associativity as u8]
    }
    pub fn parse(bytes: &[u8]) -> Result<Self, EventParseError> {
        let [version, atomic_level, associativity] = bytes
            .try_into()
            .map_err(|_| EventParseError::BadProperties)?;
        if version != Self::VERSION {
            return Err(EventParseError::UnknownPropertiesVersion);
        }
        Ok(EventAlgebraicProperties {
            atomic_level: atomic_level
                .try_into()
                .map_err(|_| EventParseError::BadProperties)?,
            associativity: associativity
                .try_into()
                .map_err(|_| EventParseError::BadProperties)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
/// Ordering of an event relative to the other events **of the same key**.
///
/// Events of different keys are never ordered against each other, even when
/// they share an Iggy partition. Whatever the ordering, events of the same key
/// and the same type keep their log order.
///
/// The consumer's [planner](crate::partition::plan) enforces fences
/// conservatively: `Acquire` and `AcqRel` also keep earlier events of the key
/// before the fence, and `Release` also keeps later events after it.
pub enum EventAtomicOrdering {
    /// The event may move past any event of the same key with a different type.
    Relaxed = 1,
    /// No later event of the same key may be processed before this event.
    Acquire = 2,
    /// No earlier event of the same key may be processed after this event.
    Release = 4,
    /// `Acquire` + `Release`.
    AcqRel = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
/// Wire value of an event type's [`Algebra`]. Each variant names the marker in
/// [`algebra`](crate::partition::algebra) that selects it, and that marker's reduction is
/// what the consumer applies.
pub enum EventAssociativity {
    /// [`algebra::NonAssociative`](crate::partition::algebra::NonAssociative).
    NonAssociative = 0,
    /// [`algebra::Associative`](crate::partition::algebra::Associative).
    Associative = 1,
    /// [`algebra::Idempotent`](crate::partition::algebra::Idempotent).
    Idempotent = 2,
    /// [`algebra::Commutative`](crate::partition::algebra::Commutative).
    Commutative = 3,
    /// [`algebra::IdempotentCommutative`](crate::partition::algebra::IdempotentCommutative).
    IdempotentCommutative = 4,
}
