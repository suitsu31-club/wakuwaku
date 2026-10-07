use crate::consumer::plan::PlanRecord;
use crate::events::EventParseError;
use crate::headers::{EventHeaders, HeaderKeys, RetryProperties};
use bytes::Bytes;
use iggy::prelude::IggyMessage;

/// A polled message whose headers parsed.
#[derive(Debug, Clone)]
pub(crate) struct ParsedRecord {
    /// Offset in the partition it was polled from.
    pub offset: u64,
    pub headers: EventHeaders,
    pub payload: Bytes,
}

pub(crate) fn parse(
    message: &IggyMessage,
    keys: &HeaderKeys,
) -> Result<(ParsedRecord, Option<RetryProperties>), EventParseError> {
    let (headers, retry) = EventHeaders::parse(message, keys)?;
    let record = ParsedRecord {
        offset: message.header.offset,
        headers,
        payload: message.payload.clone(),
    };
    Ok((record, retry))
}

pub(crate) fn plan_records(records: &[ParsedRecord]) -> Vec<PlanRecord<u64>> {
    records
        .iter()
        .map(|record| PlanRecord {
            key: record.headers.key,
            tag: record.headers.tag,
            ordering: record.headers.properties.atomic_level,
        })
        .collect()
}
