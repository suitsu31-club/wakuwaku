//! Event traits, their wire envelope and the publisher.
//!
//! An event type implements [`Event`]: it names its [type tag](Event::TYPE_TAG),
//! its [partition key](Event::Key), its [algebra](Event::Algebra) and the
//! [ordering](EventAtomicOrdering) of each message. Its body, the bytes stored
//! in the log, is an [`EventBody`]: either the event itself or a separate type
//! it converts into with [`IntoEventBody`].
//!
//! The [`Publisher`] writes events with the [`headers`] the consumer reads
//! back.

pub mod headers;
pub mod publisher;

pub use publisher::Publisher;

use crate::partition::PartitionKey;
use crate::partition::algebra::Algebra;
use num_enum::{IntoPrimitive, TryFromPrimitive};

/// Identifies an event type on the wire.
///
/// Every event type sharing a main topic needs its own tag. The consumer
/// dispatches records to handlers by tag, and rejects a handler list that
/// registers a tag twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct EventTypeTag(u32);

impl EventTypeTag {
    /// Wrap a raw tag.
    pub const fn new(tag: u32) -> Self {
        Self(tag)
    }
    /// The raw tag.
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Payload of an event as stored in the log, serialized with
/// [`kanau::message`].
///
/// Every `EventBody` is also an [`IntoEventBody`] targeting itself.
pub trait EventBody: kanau::message::MessageDe + kanau::message::MessageSer {}

/// Conversion of an event into the body that is published.
///
/// Implement it directly when the event type is not itself the body, for
/// example a domain type that converts into a wire DTO. Otherwise implement
/// [`EventBody`] and get this trait for free.
pub trait IntoEventBody {
    /// The published body. The consumer decodes records into this type and
    /// passes it to the [handler](crate::consumer::EventHandler).
    type Target: EventBody;
    /// Convert the event into its body.
    fn into_event_body(self) -> Self::Target;
}

impl<T: EventBody> IntoEventBody for T {
    type Target = T;
    fn into_event_body(self) -> Self::Target {
        self
    }
}

/// An event type that can be published and consumed.
pub trait Event: IntoEventBody {
    /// Tag written into every message of this type.
    const TYPE_TAG: EventTypeTag;
    /// Partition key. Its [super partition](PartitionKey::SUPER_PARTITION_NAME)
    /// is the main topic of this event, and its [hash](crate::partition::key_hash)
    /// picks the partition and scopes the ordering.
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
    /// Key of this message.
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

/// Malformed message headers.
#[derive(Debug)]
pub enum EventParseError {
    /// The type tag is not a known event type.
    UnknownEventType,
    /// The properties header has a version this crate does not know.
    UnknownPropertiesVersion,
    /// The properties header has the wrong length or an unknown value.
    BadProperties,
    /// The named header is absent.
    MissingHeader(&'static str),
    /// The named header has the wrong length or content.
    BadHeader(&'static str),
    /// The retry header has a version this crate does not know.
    UnknownRetryVersion,
}

/// Per-message properties carried in the [`PROPS_HEADER`](headers::PROPS_HEADER).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventAlgebraicProperties {
    /// Ordering of the message within its key.
    pub atomic_level: EventAtomicOrdering,
    /// Wire value of the event type's [`Algebra`].
    pub associativity: EventAssociativity,
}

impl EventAlgebraicProperties {
    /// Version byte of the encoding.
    pub const VERSION: u8 = 1;
    /// Length of the encoding in bytes.
    pub const LENGTH: usize = 3;
    /// Encode as `[version, ordering, associativity]`.
    pub fn into_bytes(self) -> [u8; Self::LENGTH] {
        [1u8, self.atomic_level as u8, self.associativity as u8]
    }
    /// Decode the output of [`into_bytes`](Self::into_bytes).
    ///
    /// # Errors
    ///
    /// [`EventParseError::UnknownPropertiesVersion`] for another version, and
    /// [`EventParseError::BadProperties`] for a wrong length or an unknown
    /// ordering or associativity.
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
