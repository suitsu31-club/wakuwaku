//! Publishing events to their main topic.

use crate::error::Error;
use crate::events::Event;
use crate::headers::{EventHeaders, HeaderKeys};
use crate::partition::{PartitionKey, key_hash};
use bytes::Bytes;
use iggy::prelude::{Identifier, IggyClient, IggyMessage, MessageClient, Partitioning};
use kanau::message::{MessageSer, SerializeError};
use std::sync::Arc;

/// Publishes events of any type to their main topic in one stream.
///
/// Events are partitioned by the hash of their key, so every event of a key
/// lands in the same partition, in publish order.
pub struct Publisher {
    client: Arc<IggyClient>,
    stream: Identifier,
    keys: HeaderKeys,
}

impl Publisher {
    pub fn new(client: Arc<IggyClient>, stream: &str) -> Result<Self, Error> {
        Ok(Self {
            client,
            stream: Identifier::try_from(stream)?,
            keys: HeaderKeys::new()?,
        })
    }

    /// Serialize `event` and send it to the main topic of its key type.
    pub async fn publish<E: Event>(&self, event: E) -> Result<(), Error> {
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
        let topic = Identifier::try_from(<E::Key as PartitionKey>::SUPER_PARTITION_NAME.0)?;
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
}
