//! Message headers written by the publisher and read by the consumer.
//!
//! Every value is a raw byte header.

use crate::events::{EventAlgebraicProperties, EventParseError, EventTypeTag};
use iggy::prelude::{HeaderKey, HeaderValue, IggyError, IggyMessage};
use std::collections::BTreeMap;
use std::str::FromStr;

/// Event type tag, a little-endian `u32`.
pub const TAG_HEADER: &str = "wakuwaku-tag";
/// [Key hash](crate::partition::key_hash), a little-endian `u64`.
pub const KEY_HEADER: &str = "wakuwaku-key";
/// [`EventAlgebraicProperties::into_bytes`].
pub const PROPS_HEADER: &str = "wakuwaku-props";
/// [`RetryProperties::into_bytes`], only on records of a retry topic.
pub const RETRY_HEADER: &str = "wakuwaku-retry";

/// Parsed header keys, built once per publisher or consumer.
#[derive(Debug, Clone)]
pub(crate) struct HeaderKeys {
    tag: HeaderKey,
    key: HeaderKey,
    props: HeaderKey,
    retry: HeaderKey,
}

impl HeaderKeys {
    pub(crate) fn new() -> Result<Self, IggyError> {
        Ok(Self {
            tag: HeaderKey::from_str(TAG_HEADER)?,
            key: HeaderKey::from_str(KEY_HEADER)?,
            props: HeaderKey::from_str(PROPS_HEADER)?,
            retry: HeaderKey::from_str(RETRY_HEADER)?,
        })
    }
}

/// Headers every event message carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventHeaders {
    pub tag: EventTypeTag,
    pub key: u64,
    pub properties: EventAlgebraicProperties,
}

impl EventHeaders {
    pub(crate) fn to_map(
        self,
        keys: &HeaderKeys,
        retry: Option<RetryProperties>,
    ) -> Result<BTreeMap<HeaderKey, HeaderValue>, IggyError> {
        let mut map = BTreeMap::new();
        map.insert(
            keys.tag.clone(),
            HeaderValue::try_from(self.tag.get().to_le_bytes().to_vec())?,
        );
        map.insert(
            keys.key.clone(),
            HeaderValue::try_from(self.key.to_le_bytes().to_vec())?,
        );
        map.insert(
            keys.props.clone(),
            HeaderValue::try_from(self.properties.into_bytes().to_vec())?,
        );
        if let Some(retry) = retry {
            map.insert(
                keys.retry.clone(),
                HeaderValue::try_from(retry.into_bytes().to_vec())?,
            );
        }
        Ok(map)
    }

    pub(crate) fn parse(
        message: &IggyMessage,
        keys: &HeaderKeys,
    ) -> Result<(Self, Option<RetryProperties>), EventParseError> {
        let map = message
            .user_headers_map()
            .map_err(|_| EventParseError::MissingHeader(TAG_HEADER))?
            .ok_or(EventParseError::MissingHeader(TAG_HEADER))?;
        let get = |key: &HeaderKey, name: &'static str| {
            map.get(key)
                .map(HeaderValue::as_bytes)
                .ok_or(EventParseError::MissingHeader(name))
        };
        let tag = get(&keys.tag, TAG_HEADER)?
            .try_into()
            .map_err(|_| EventParseError::BadHeader(TAG_HEADER))?;
        let key = get(&keys.key, KEY_HEADER)?
            .try_into()
            .map_err(|_| EventParseError::BadHeader(KEY_HEADER))?;
        let properties = EventAlgebraicProperties::parse(get(&keys.props, PROPS_HEADER)?)?;
        let retry = map
            .get(&keys.retry)
            .map(|value| RetryProperties::parse(value.as_bytes()))
            .transpose()?;
        let headers = EventHeaders {
            tag: EventTypeTag::new(u32::from_le_bytes(tag)),
            key: u64::from_le_bytes(key),
            properties,
        };
        Ok((headers, retry))
    }
}

/// Header of a record in a retry topic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryProperties {
    /// Wall clock, in milliseconds since the Unix epoch, when the record was
    /// written to the retry topic.
    pub failed_at_ms: u64,
}

impl RetryProperties {
    pub const VERSION: u8 = 1;
    pub const LENGTH: usize = 9;

    pub fn into_bytes(self) -> [u8; Self::LENGTH] {
        let [a, b, c, d, e, f, g, h] = self.failed_at_ms.to_le_bytes();
        [Self::VERSION, a, b, c, d, e, f, g, h]
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, EventParseError> {
        let [version, a, b, c, d, e, f, g, h] = bytes
            .try_into()
            .map_err(|_| EventParseError::BadHeader(RETRY_HEADER))?;
        if version != Self::VERSION {
            return Err(EventParseError::UnknownRetryVersion);
        }
        Ok(RetryProperties {
            failed_at_ms: u64::from_le_bytes([a, b, c, d, e, f, g, h]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::{EventAssociativity, EventAtomicOrdering};
    use bytes::Bytes;

    #[test]
    fn retry_properties_round_trip() {
        let properties = RetryProperties {
            failed_at_ms: 0x0102_0304_0506_0708,
        };
        let parsed = RetryProperties::parse(&properties.into_bytes());
        assert_eq!(parsed.ok(), Some(properties));
    }

    #[test]
    fn retry_properties_reject_wrong_length() {
        let bytes = RetryProperties { failed_at_ms: 1 }.into_bytes();
        assert!(matches!(
            RetryProperties::parse(&bytes[..8]),
            Err(EventParseError::BadHeader(RETRY_HEADER))
        ));
        assert!(matches!(
            RetryProperties::parse(&[bytes.as_slice(), &[0]].concat()),
            Err(EventParseError::BadHeader(RETRY_HEADER))
        ));
    }

    #[test]
    fn retry_properties_reject_wrong_version() {
        let mut bytes = RetryProperties { failed_at_ms: 1 }.into_bytes();
        bytes[0] = 2;
        assert!(matches!(
            RetryProperties::parse(&bytes),
            Err(EventParseError::UnknownRetryVersion)
        ));
    }

    #[test]
    fn event_headers_round_trip_through_a_message() {
        let keys = HeaderKeys::new().unwrap_or_else(|e| panic!("{e}"));
        let headers = EventHeaders {
            tag: EventTypeTag::new(7),
            key: u64::MAX - 3,
            properties: EventAlgebraicProperties {
                atomic_level: EventAtomicOrdering::Release,
                associativity: EventAssociativity::Idempotent,
            },
        };
        let retry = RetryProperties { failed_at_ms: 42 };
        let message = headers
            .to_map(&keys, Some(retry))
            .and_then(|map| {
                IggyMessage::builder()
                    .payload(Bytes::from_static(b"x"))
                    .user_headers(map)
                    .build()
            })
            .unwrap_or_else(|e| panic!("{e}"));
        let parsed = EventHeaders::parse(&message, &keys);
        assert!(matches!(parsed, Ok((h, Some(r))) if h == headers && r == retry));
    }
}
