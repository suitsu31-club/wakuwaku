//! Errors of handlers, setup and publishing.

use crate::events::EventTypeTag;
use std::fmt::{Display, Formatter};

/// How the consumer reacts to a failed [handler](crate::handler::EventHandler)
/// call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Infrastructure is down. The same chunk is retried in place, with the
    /// configured delays, until it succeeds. The partition makes no progress
    /// meanwhile.
    Transient,
    /// The key fails. Its unfinished work goes to the retry topic, and the
    /// key's later events wait behind it.
    Retryable,
    /// The chunk can never succeed. It is dropped and logged, and the rest of
    /// the run continues.
    Unrecoverable,
}

/// Error returned by a [handler](crate::handler::EventHandler).
#[derive(Debug)]
pub struct HandleError {
    class: ErrorClass,
    source: anyhow::Error,
}

impl HandleError {
    pub fn new(class: ErrorClass, source: impl Into<anyhow::Error>) -> Self {
        Self {
            class,
            source: source.into(),
        }
    }

    /// See [`ErrorClass::Transient`].
    pub fn transient(source: impl Into<anyhow::Error>) -> Self {
        Self::new(ErrorClass::Transient, source)
    }

    /// See [`ErrorClass::Retryable`].
    pub fn retryable(source: impl Into<anyhow::Error>) -> Self {
        Self::new(ErrorClass::Retryable, source)
    }

    /// See [`ErrorClass::Unrecoverable`].
    pub fn unrecoverable(source: impl Into<anyhow::Error>) -> Self {
        Self::new(ErrorClass::Unrecoverable, source)
    }

    pub fn class(&self) -> ErrorClass {
        self.class
    }

    pub fn source(&self) -> &anyhow::Error {
        &self.source
    }

    pub(crate) fn into_source(self) -> anyhow::Error {
        self.source
    }
}

impl Display for HandleError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.source, f)
    }
}

/// Error of setting up a consumer or publishing an event.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{0}")]
    /// Iggy client or server error.
    Iggy(#[from] iggy::prelude::IggyError),

    #[error("{0}")]
    /// Serialize Error by kanau
    Serialize(#[from] kanau::message::SerializeError),

    #[error("Topic {topic} does not exist in stream {stream}")]
    /// The main topic of the registered events does not exist.
    TopicNotFound { stream: String, topic: String },

    #[error("Partition {partition} does not exist in topic {topic}")]
    /// A configured partition does not exist in the main topic.
    PartitionNotFound { topic: String, partition: u32 },

    #[error(
        "Topic {topic} has {topic_count} partitions but its retry topic {retry_topic} has {retry_count}"
    )]
    /// The retry topic exists with a different partition count.
    PartitionCountMismatch {
        topic: String,
        retry_topic: String,
        topic_count: u32,
        retry_count: u32,
    },

    #[error("Registered events use different topics: {first} and {second}")]
    /// One consumer only reads one main topic.
    MixedTopics {
        first: &'static str,
        second: &'static str,
    },

    #[error("Event type {} is registered twice", .0.get())]
    /// Two handlers are registered for the same event type tag.
    DuplicateEventType(EventTypeTag),

    #[error("Invalid consumer configuration: {0}")]
    /// The consumer configuration is unusable.
    InvalidConfig(&'static str),
}
