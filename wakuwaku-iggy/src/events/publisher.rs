//! Publishing events to their main topic.
//!
//! See the [publishing guide](https://www.wakuwaku.rs/en/docs/events/publishing).

use crate::error::Error;
use crate::events::Event;
use crate::events::headers::{EventHeaders, HeaderKeys};
use crate::partition::{PartitionKey, key_hash};
use bytes::Bytes;
use iggy::prelude::{Identifier, IggyClient, IggyMessage, MessageClient, Partitioning};
use kanau::message::{MessageSer, SerializeError};
use std::collections::HashMap;
use std::sync::Arc;

/// Publishes events of any type to their main topic in one stream.
///
/// Events are partitioned by the hash of their key, so every event of a key
/// lands in the same partition, in publish order. Cloning is cheap: clones
/// share the same client.
#[derive(Debug, Clone)]
pub struct Publisher {
    client: Arc<IggyClient>,
    stream: Identifier,
    keys: HeaderKeys,
}

impl Publisher {
    /// Create a publisher writing to `stream`.
    ///
    /// # Errors
    ///
    /// [`Error::Iggy`] if `stream` is not a valid identifier.
    pub fn new(client: Arc<IggyClient>, stream: &str) -> Result<Self, Error> {
        Ok(Self {
            client,
            stream: Identifier::try_from(stream)?,
            keys: HeaderKeys::new()?,
        })
    }

    /// Serialize `event` and send it to the main topic of its key type.
    ///
    /// # Errors
    ///
    /// [`Error::Serialize`] if the body can't be serialized, and
    /// [`Error::Iggy`] if the message can't be built or sent. The send is not
    /// retried.
    pub async fn publish<E: Event>(&self, event: E) -> Result<(), Error> {
        let topic = Self::topic::<E>()?;
        let (key, message) = self.to_message(event)?;
        self.client
            .send_messages(
                &self.stream,
                &topic,
                &Partitioning::messages_key_u64(key),
                &mut [message],
            )
            .await?;
        Ok(())
    }

    /// Serialize `events` and send them to the main topic of their key type.
    ///
    /// Events are grouped by key hash and each group is sent in one request,
    /// so events of one key keep their relative order. Groups are sent in the
    /// order their key first appears in `events`.
    ///
    /// # Errors
    ///
    /// [`Error::Serialize`] if a body can't be serialized, and
    /// [`Error::Iggy`] if a message can't be built or a group can't be sent.
    /// Every event is serialized before anything is sent, so a serialization
    /// error sends nothing. A send error stops at the failing group: the
    /// groups before it are already sent, the ones after are not. Sends are
    /// not retried.
    pub async fn publish_batch<E: Event>(
        &self,
        events: impl IntoIterator<Item = E>,
    ) -> Result<(), Error> {
        let topic = Self::topic::<E>()?;
        let mut slots = HashMap::<u64, usize>::new();
        let mut groups = Vec::<(u64, Vec<IggyMessage>)>::new();
        for event in events {
            let (key, message) = self.to_message(event)?;
            match slots.get(&key).and_then(|&slot| groups.get_mut(slot)) {
                Some((_, messages)) => messages.push(message),
                None => {
                    slots.insert(key, groups.len());
                    groups.push((key, vec![message]));
                }
            }
        }
        for (key, mut messages) in groups {
            self.client
                .send_messages(
                    &self.stream,
                    &topic,
                    &Partitioning::messages_key_u64(key),
                    &mut messages,
                )
                .await?;
        }
        Ok(())
    }

    /// Main topic of `E`.
    fn topic<E: Event>() -> Result<Identifier, Error> {
        Ok(Identifier::try_from(
            <E::Key as PartitionKey>::SUPER_PARTITION_NAME.0,
        )?)
    }

    /// Serialize `event` into a message, returning it with its key hash.
    fn to_message<E: Event>(&self, event: E) -> Result<(u64, IggyMessage), Error> {
        let key = key_hash(&event.key());
        let properties = event.algebraic_properties();
        let body = event
            .into_event_body()
            .to_bytes()
            .map_err(Into::<SerializeError>::into)?;
        let headers = EventHeaders {
            tag: E::TYPE_TAG,
            key,
            properties,
        }
        .to_map(&self.keys, None)?;
        let message = IggyMessage::builder()
            .payload(Bytes::from(body))
            .user_headers(headers)
            .build()?;
        Ok((key, message))
    }
}
