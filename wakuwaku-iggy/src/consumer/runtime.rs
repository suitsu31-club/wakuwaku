//! Setup and the per-partition consumer loop.
//!
//! Each partition task reads its retry partition to the end, runs the retry
//! keys that are due, then polls one batch of the main partition. Keys of the
//! batch that are quarantined (have records in retry) are diverted to retry
//! unexecuted, and keys that fail are written to retry from where they
//! failed. See [`DeliveryMode`] for when offsets are stored.

use crate::consumer::backoff::until_ok;
use crate::consumer::config::{ConsumerConfig, DeliveryMode};
use crate::consumer::encode::{RetryEntry, encode_key};
use crate::consumer::execute::{KeyReport, execute_key};
use crate::consumer::plan::Plan;
use crate::consumer::record::{self, ParsedRecord, plan_records};
use crate::consumer::retry::{Finish, PendingRecord, RetryState};
use crate::error::Error;
use crate::handler::HandlerList;
use crate::headers::{HeaderKeys, RetryProperties};
use bytes::Bytes;
use futures_util::future::join_all;
use iggy::prelude::{
    Consumer, ConsumerOffsetClient, Identifier, IggyClient, IggyMessage, IggyMessageHeader,
    MessageClient, Partitioning, PolledMessages, PollingStrategy, TopicClient, TopicCreateOptions,
};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::task::JoinSet;

/// Running partition tasks started by
/// [`IggyConsumerRegisterCenter::start`](crate::handler::IggyConsumerRegisterCenter::start).
///
/// Dropping this value aborts every partition task. Work in flight is lost
/// as far as the [`DeliveryMode`] allows: in
/// [`NoEventLose`](DeliveryMode::NoEventLose), everything not yet committed is
/// read again by the next consumer with the same name.
pub struct ConsumerRuntime {
    tasks: JoinSet<()>,
}

impl ConsumerRuntime {
    /// Abort every partition task and wait for them to stop.
    pub async fn shutdown(mut self) {
        self.tasks.shutdown().await;
    }
}

pub(crate) async fn start<L: HandlerList>(
    handlers: Arc<L>,
    client: Arc<IggyClient>,
    config: ConsumerConfig,
) -> Result<ConsumerRuntime, Error> {
    if config.retry_delays.is_empty() {
        return Err(Error::InvalidConfig("retry_delays must not be empty"));
    }
    if config.partitions.is_empty() {
        return Err(Error::InvalidConfig("partitions must not be empty"));
    }
    if config.poll_count == 0 {
        return Err(Error::InvalidConfig("poll_count must not be zero"));
    }

    let topic = handlers.topic()?;
    let mut tags = Vec::new();
    handlers.collect_tags(&mut tags);
    let mut seen = HashSet::with_capacity(tags.len());
    if let Some(&duplicate) = tags.iter().find(|&&tag| !seen.insert(tag)) {
        return Err(Error::DuplicateEventType(duplicate));
    }

    let stream = Identifier::try_from(config.stream.as_str())?;
    let main_topic = Identifier::try_from(topic)?;
    let main = client
        .get_topic(&stream, &main_topic)
        .await?
        .ok_or_else(|| Error::TopicNotFound {
            stream: config.stream.clone(),
            topic: topic.to_owned(),
        })?;
    let retry_name = format!("retry_{topic}");
    let retry_topic = Identifier::try_from(retry_name.as_str())?;
    match client.get_topic(&stream, &retry_topic).await? {
        None => {
            let options = TopicCreateOptions {
                partitions_count: Some(main.partitions_count),
                ..Default::default()
            };
            client.create_topic(&stream, &retry_name, &options).await?;
        }
        Some(retry) if retry.partitions_count != main.partitions_count => {
            return Err(Error::PartitionCountMismatch {
                topic: topic.to_owned(),
                retry_topic: retry_name,
                topic_count: main.partitions_count,
                retry_count: retry.partitions_count,
            });
        }
        Some(_) => {}
    }
    if let Some(&partition) = config
        .partitions
        .iter()
        .find(|&&id| !main.partitions.iter().any(|p| p.id == id))
    {
        return Err(Error::PartitionNotFound {
            topic: topic.to_owned(),
            partition,
        });
    }

    let keys = Arc::new(HeaderKeys::new()?);
    let consumer = Consumer::new(Identifier::try_from(config.consumer_name.as_str())?);
    let mut tasks = JoinSet::new();
    for &partition in &config.partitions {
        let task = PartitionTask {
            handlers: handlers.clone(),
            client: client.clone(),
            keys: keys.clone(),
            config: config.clone(),
            stream: stream.clone(),
            main_topic: main_topic.clone(),
            retry_topic: retry_topic.clone(),
            consumer: consumer.clone(),
            partition,
        };
        tasks.spawn(task.run());
    }
    Ok(ConsumerRuntime { tasks })
}

struct PartitionTask<L> {
    handlers: Arc<L>,
    client: Arc<IggyClient>,
    keys: Arc<HeaderKeys>,
    config: ConsumerConfig,
    stream: Identifier,
    main_topic: Identifier,
    retry_topic: Identifier,
    consumer: Consumer,
    partition: u32,
}

/// Read position in one partition.
#[derive(Debug, Clone, Copy)]
struct Cursor {
    /// Offset of the next record to read, or `None` to start at the first
    /// retained record.
    next: Option<u64>,
}

impl Cursor {
    fn strategy(self) -> PollingStrategy {
        self.next
            .map_or_else(PollingStrategy::first, PollingStrategy::offset)
    }

    fn next_or_zero(self) -> u64 {
        self.next.unwrap_or(0)
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// A fresh copy of `message`, which is still unsent.
///
/// `send_messages` may rewrite messages in place (encryption), so every
/// attempt sends copies of the original messages.
fn copy_message(message: &IggyMessage) -> IggyMessage {
    let header = &message.header;
    IggyMessage {
        header: IggyMessageHeader {
            checksum: header.checksum,
            id: header.id,
            offset: header.offset,
            timestamp: header.timestamp,
            origin_timestamp: header.origin_timestamp,
            user_headers_length: header.user_headers_length,
            payload_length: header.payload_length,
            reserved: header.reserved,
        },
        payload: message.payload.clone(),
        user_headers: message.user_headers.clone(),
    }
}

impl<L: HandlerList> PartitionTask<L> {
    fn delays(&self) -> &'static [Duration] {
        self.config.retry_delays
    }

    async fn run(self) {
        let mut main = self.stored_cursor(&self.main_topic).await;
        let mut retry = self.stored_cursor(&self.retry_topic).await;
        let mut state = RetryState::new(self.delays());
        let mut retry_stored = None;
        loop {
            self.drain_retry(&mut state, &mut retry).await;
            self.run_due(&mut state).await;
            if self.config.mode == DeliveryMode::NoEventLose
                && let Some(offset) = state.committable(retry.next_or_zero())
                && retry_stored.is_none_or(|stored| offset > stored)
            {
                self.store(&self.retry_topic, offset).await;
                retry_stored = Some(offset);
            }

            let polled = self.poll(&self.main_topic, main).await;
            let Some(last) = polled.messages.last().map(|m| m.header.offset) else {
                let idle = state
                    .next_due()
                    .map(|due| due.saturating_duration_since(Instant::now()))
                    .map_or(self.config.poll_interval, |until| {
                        until.min(self.config.poll_interval)
                    });
                tokio::time::sleep(idle).await;
                continue;
            };
            if self.config.mode == DeliveryMode::NoDoubleConsume {
                self.store(&self.main_topic, last).await;
            }
            self.process_main(&state, &polled).await;
            if self.config.mode == DeliveryMode::NoEventLose {
                self.store(&self.main_topic, last).await;
            }
            main.next = Some(last.saturating_add(1));
        }
    }

    async fn stored_cursor(&self, topic: &Identifier) -> Cursor {
        let (client, consumer, stream, partition) =
            (&*self.client, &self.consumer, &self.stream, self.partition);
        let stored = until_ok("get consumer offset", self.delays(), move || {
            client.get_consumer_offset(consumer, stream, topic, Some(partition))
        })
        .await;
        Cursor {
            next: stored.map(|info| info.stored_offset.saturating_add(1)),
        }
    }

    async fn store(&self, topic: &Identifier, offset: u64) {
        let (client, consumer, stream, partition) =
            (&*self.client, &self.consumer, &self.stream, self.partition);
        until_ok("store consumer offset", self.delays(), move || {
            client.store_consumer_offset(consumer, stream, topic, Some(partition), offset)
        })
        .await;
    }

    async fn poll(&self, topic: &Identifier, cursor: Cursor) -> PolledMessages {
        let (client, consumer, stream, partition) =
            (&*self.client, &self.consumer, &self.stream, self.partition);
        let strategy = cursor.strategy();
        let strategy = &strategy;
        let count = self.config.poll_count;
        until_ok("poll messages", self.delays(), move || {
            client.poll_messages(
                stream,
                topic,
                Some(partition),
                consumer,
                strategy,
                count,
                false,
            )
        })
        .await
    }

    /// Read the retry partition to its end into `state`.
    async fn drain_retry(&self, state: &mut RetryState, cursor: &mut Cursor) {
        loop {
            let polled = self.poll(&self.retry_topic, *cursor).await;
            let Some(last) = polled.messages.last().map(|m| m.header.offset) else {
                return;
            };
            let now = Instant::now();
            let now_ms = now_ms();
            for message in &polled.messages {
                let offset = message.header.offset;
                match record::parse(message, &self.keys) {
                    Ok((record, Some(retry))) => state.enqueue(
                        PendingRecord {
                            record,
                            failed_at_ms: retry.failed_at_ms,
                        },
                        now,
                        now_ms,
                    ),
                    Ok((_, None)) => tracing::error!(
                        partition = self.partition,
                        offset,
                        "dropping retry record without retry header"
                    ),
                    Err(error) => tracing::error!(
                        partition = self.partition,
                        offset,
                        ?error,
                        "dropping retry record with bad headers"
                    ),
                }
            }
            cursor.next = Some(last.saturating_add(1));
            if self.config.mode == DeliveryMode::NoDoubleConsume {
                self.store(&self.retry_topic, last).await;
            }
        }
    }

    /// Run one attempt for every due retry key.
    async fn run_due(&self, state: &mut RetryState) {
        let due = state.due_keys(Instant::now());
        if due.is_empty() {
            return;
        }
        let work: Vec<(u64, Vec<ParsedRecord>)> = due
            .into_iter()
            .map(|key| (key, state.records(key)))
            .collect();
        let reports = join_all(work.iter().map(|(_, records)| self.attempt(records))).await;
        let now = Instant::now();
        for ((key, records), report) in work.iter().zip(reports) {
            let offset_of = |&i: &usize| records[i].offset;
            let resolved: Vec<u64> = report.completed.iter().map(offset_of).collect();
            let (error, failed) = match &report.failure {
                Some(failure) => (
                    Some(&failure.error),
                    failure
                        .failed_runs
                        .iter()
                        .flat_map(|run| &run.records)
                        .map(offset_of)
                        .collect(),
                ),
                None => (None, Vec::new()),
            };
            match (state.finish(*key, &resolved, &failed, now), error) {
                (Finish::GaveUp { dropped }, error) => tracing::error!(
                    partition = self.partition,
                    key,
                    dropped,
                    error = error.map(tracing::field::display),
                    "giving up retry"
                ),
                (Finish::Scheduled { attempt, delay }, Some(error)) => tracing::warn!(
                    partition = self.partition,
                    key,
                    attempt,
                    ?delay,
                    %error,
                    "retry attempt failed"
                ),
                _ => {}
            }
        }
    }

    /// Plan and execute the pending records of one retry key.
    async fn attempt(&self, records: &[ParsedRecord]) -> KeyReport {
        let plan = Plan::new(&plan_records(records));
        match plan.keys().first() {
            Some(key) => execute_key(&*self.handlers, &plan, key, records, self.delays()).await,
            None => KeyReport {
                completed: Vec::new(),
                failure: None,
            },
        }
    }

    /// Execute or divert every key of a main batch, and write the failed and
    /// diverted work to the retry partition.
    async fn process_main(&self, state: &RetryState, polled: &PolledMessages) {
        let mut records = Vec::with_capacity(polled.messages.len());
        for message in &polled.messages {
            match record::parse(message, &self.keys) {
                Ok((record, _)) => records.push(record),
                Err(error) => tracing::error!(
                    partition = self.partition,
                    offset = message.header.offset,
                    ?error,
                    "dropping record with bad headers"
                ),
            }
        }
        let plan = Plan::new(&plan_records(&records));
        let quarantined: Vec<bool> = plan
            .keys()
            .iter()
            .map(|key| state.is_quarantined(*key.key()))
            .collect();
        let reports = join_all(
            plan.keys()
                .iter()
                .zip(&quarantined)
                .filter(|(_, quarantined)| !**quarantined)
                .map(|(key, _)| execute_key(&*self.handlers, &plan, key, &records, self.delays())),
        )
        .await;

        let mut entries: Vec<RetryEntry> = Vec::new();
        let mut reports = reports.into_iter();
        for (key, &quarantined) in plan.keys().iter().zip(&quarantined) {
            if quarantined {
                entries.extend(encode_key(&*self.handlers, &plan, key, &records, None));
                continue;
            }
            let Some(failure) = reports.next().and_then(|report| report.failure) else {
                continue;
            };
            let key_hash = *key.key();
            let error = failure.error.to_string();
            let failed = encode_key(&*self.handlers, &plan, key, &records, Some(failure));
            let backlog = state
                .pending_len()
                .saturating_add(entries.len())
                .saturating_add(failed.len());
            if backlog > self.config.max_pending_retry {
                tracing::error!(
                    partition = self.partition,
                    key = key_hash,
                    dropped = failed.len(),
                    %error,
                    "retry backlog full, dropping failed events"
                );
            } else {
                tracing::warn!(
                    partition = self.partition,
                    key = key_hash,
                    %error,
                    "key failed, sending to retry"
                );
                entries.extend(failed);
            }
        }
        self.send_retry(entries).await;
    }

    /// Write `entries` to the retry partition in one batch.
    async fn send_retry(&self, entries: Vec<RetryEntry>) {
        let retry = Some(RetryProperties {
            failed_at_ms: now_ms(),
        });
        let mut messages = Vec::with_capacity(entries.len());
        for entry in entries {
            let message = entry.headers.to_map(&self.keys, retry).and_then(|headers| {
                IggyMessage::builder()
                    .payload(Bytes::from(entry.payload))
                    .user_headers(headers)
                    .build()
            });
            match message {
                Ok(message) => messages.push(message),
                Err(error) => tracing::error!(
                    partition = self.partition,
                    key = entry.headers.key,
                    %error,
                    "dropping retry record that can't be built"
                ),
            }
        }
        if messages.is_empty() {
            return;
        }
        let (client, stream, topic) = (&*self.client, &self.stream, &self.retry_topic);
        let partitioning = Partitioning::partition_id(self.partition);
        let (partitioning, messages) = (&partitioning, &messages);
        until_ok("send retry records", self.delays(), move || {
            let mut batch: Vec<IggyMessage> = messages.iter().map(copy_message).collect();
            async move {
                client
                    .send_messages(stream, topic, partitioning, &mut batch)
                    .await
                    .map(drop)
            }
        })
        .await;
    }
}
