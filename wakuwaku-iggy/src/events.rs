use crate::partition::PartitionKey;
use num_enum::{IntoPrimitive, TryFromPrimitive};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventTypeTag(u32);

pub trait EventOptimize {
    type EventInner;
    fn get_algebraic_properties(&self) -> EventAlgebraicProperties;
    fn into_inner(self) -> Self::EventInner;
}

pub trait Event: EventOptimize {
    const TYPE_TAG: EventTypeTag;
    type Key: PartitionKey;
    fn key(&self) -> Self::Key;
}

#[derive(Debug)]
pub enum EventParseError {
    UnknownEventType,
    UnknownPropertiesGen,
    BadProperties,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventAlgebraicProperties {
    pub atomic_level: EventAtomicOrdering,
    pub associativity: EventAssociativity,
}

impl EventAlgebraicProperties {
    pub const GEN: u8 = 1;
    pub const LENGTH: usize = 2;
    pub fn into_bytes(self) -> [u8; Self::LENGTH] {
        [self.atomic_level as u8, self.associativity as u8]
    }
    pub fn parse(bytes: &[u8]) -> Result<Self, EventParseError> {
        let [atomic_level, associativity] = bytes
            .try_into()
            .map_err(|_| EventParseError::BadProperties)?;
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
/// The atomic ordering of the event. This can help to batch the same types of events together.
/// No matter what the atomic ordering is, the order of the same type of events is preserved.
pub enum EventAtomicOrdering {
    /// The event is free to move, as long as the order of the same type of events is preserved
    Relaxed = 1,
    /// No events after this event can be processed before this event
    Acquire = 2,
    /// No events before this event can be processed after this event
    Release = 4,
    /// `Acquire` + `Release`.
    AcqRel = 6,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, TryFromPrimitive, IntoPrimitive)]
#[repr(u8)]
/// The associativity of the same type of events. This can help to optimize the processing of batched
/// events of the same type.
pub enum EventAssociativity {
    /// Require [EventSemigroup](crate::algebra::semigroup) trait.
    Associative = 1,
    /// Require `Eq` trait. The same events will be reduced to one event.
    Idempotent = 2,
    /// The event will be processed in any order.
    Commutative = 3,
    /// No algebraic optimization is possible.
    NonAssociative = 0,
}
