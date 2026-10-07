//! Event handlers and the type-level list that dispatches runs to them.
//!
//! Register one [`EventHandler`] per event type with
//! [`IggyConsumerRegisterCenter::new`] and
//! [`push`](IggyConsumerRegisterCenter::push). Every registered event must
//! live in the same main topic.

use crate::algebra::Algebra;
use crate::consumer::backoff::delay_for;
use crate::consumer::{ConsumerConfig, ConsumerRuntime};
use crate::error::{ErrorClass, HandleError};
use crate::events::{Event, EventAssociativity, EventTypeTag};
use crate::partition::PartitionKey;
use iggy::prelude::IggyClient;
use kanau::message::{DeserializeError, MessageDe, MessageSer, SerializeError};
use std::marker::PhantomData;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

/// Applies the events of type `E`.
pub trait EventHandler<E: Event>: Send + Sync + 'static {
    /// Largest number of events passed to one [`handle`](Self::handle) call.
    const MAX_BATCH: NonZeroUsize;

    /// Apply `run`, events of one key in the order they must be applied,
    /// already reduced with `E`'s [`Algebra`].
    ///
    /// The [`ErrorClass`] of a returned error decides what happens next.
    fn handle(&self, run: &[E::Target]) -> impl Future<Output = Result<(), HandleError>> + Send;
}

/// A node in the type-level list of registered handlers.
///
/// `H` handles events of type `E`, and `Chain` is the rest of the list:
/// either another `IggyConsumerRegisterCenter` or `()` for the first
/// registered handler. Build one with [`new`](Self::new), extend it with
/// [`push`](Self::push), and start consuming with [`start`](Self::start).
pub struct IggyConsumerRegisterCenter<H: EventHandler<E>, E: Event, Chain = ()> {
    head: Arc<H>,
    chain: Chain,
    event: PhantomData<fn(E)>,
}

impl<H: EventHandler<E>, E: Event> IggyConsumerRegisterCenter<H, E> {
    /// Start a new list with `head` as its first handler.
    pub fn new(head: Arc<H>) -> Self {
        Self {
            head,
            chain: (),
            event: PhantomData,
        }
    }
}

impl<H1: EventHandler<E1>, E1: Event, Chain> IggyConsumerRegisterCenter<H1, E1, Chain> {
    /// Register another handler, returning the extended list.
    pub fn push<H2: EventHandler<E2>, E2: Event>(
        self,
        next: Arc<H2>,
    ) -> IggyConsumerRegisterCenter<H2, E2, Self> {
        IggyConsumerRegisterCenter {
            head: next,
            chain: self,
            event: PhantomData,
        }
    }
}

impl<H: EventHandler<E>, E: Event, Chain> IggyConsumerRegisterCenter<H, E, Chain>
where
    Self: HandlerList,
{
    /// Validate the topics and spawn one task per configured partition.
    ///
    /// Creates the retry topic `retry_<topic>` with the main topic's partition
    /// count if it does not exist.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`](crate::error::Error::InvalidConfig)
    /// for an unusable `config`, and an error if the registered events don't
    /// share one topic, a tag is registered twice, the main topic or a
    /// configured partition doesn't exist, the retry topic has a different
    /// partition count, or an Iggy call fails.
    pub async fn start(
        self,
        client: Arc<IggyClient>,
        config: ConsumerConfig,
    ) -> Result<ConsumerRuntime, crate::error::Error> {
        crate::consumer::runtime::start(Arc::new(self), client, config).await
    }
}

/// One record of a run, as stored in the log.
pub struct RunRecord<'a> {
    pub payload: &'a [u8],
    /// Associativity from the record's header.
    pub associativity: EventAssociativity,
}

/// Outcome of [`HandlerList::handle_run`].
pub enum RunResult {
    /// Every chunk succeeded or was dropped as unrecoverable.
    Done,
    /// A chunk failed with [`ErrorClass::Retryable`].
    Failed {
        error: anyhow::Error,
        /// The failed chunk and every chunk after it, serialized.
        remainder: Vec<Box<[u8]>>,
    },
}

/// Dispatches runs to the handler registered for their type tag.
///
/// Implemented for every [`IggyConsumerRegisterCenter`] list. It is public
/// only because it appears in the bounds of
/// [`IggyConsumerRegisterCenter::start`]. Call `start` instead of using it
/// directly.
pub trait HandlerList: Send + Sync + 'static {
    /// The main topic shared by every registered event.
    fn topic(&self) -> Result<&'static str, crate::error::Error>;
    /// Push the tag of every registered event.
    fn collect_tags(&self, out: &mut Vec<EventTypeTag>);
    /// Associativity of the event registered for `tag`.
    fn associativity(&self, tag: EventTypeTag) -> Option<EventAssociativity>;
    /// Decode, reduce and handle `run`, the records of one run with type
    /// `tag`.
    fn handle_run<'a>(
        &'a self,
        tag: EventTypeTag,
        run: &'a [RunRecord<'a>],
        delays: &'static [Duration],
    ) -> impl Future<Output = RunResult> + Send + 'a;
    /// Decode and reduce `run`, then serialize the reduced events.
    fn reduce_run(&self, tag: EventTypeTag, run: &[RunRecord<'_>]) -> Vec<Box<[u8]>>;
}

/// Decode `run`, dropping records that can't be decoded, and reduce it.
fn decode<E: Event>(run: &[RunRecord<'_>]) -> Vec<E::Target> {
    let tag = E::TYPE_TAG.get();
    let expected = <E::Algebra as Algebra<E::Target>>::ASSOCIATIVITY;
    let mut events = Vec::with_capacity(run.len());
    for record in run {
        if record.associativity != expected {
            tracing::error!(
                tag,
                header = ?record.associativity,
                ?expected,
                "dropping event whose header associativity does not match its type"
            );
            continue;
        }
        match E::Target::from_bytes(record.payload) {
            Ok(event) => events.push(event),
            Err(error) => {
                let error: DeserializeError = error.into();
                tracing::error!(tag, %error, "dropping event that can't be decoded");
            }
        }
    }
    <E::Algebra as Algebra<E::Target>>::reduce(&mut events);
    events
}

/// Serialize `events`, dropping those that can't be serialized.
fn encode<E: Event>(events: impl Iterator<Item = E::Target>) -> Vec<Box<[u8]>> {
    events
        .filter_map(|event| match event.to_bytes() {
            Ok(bytes) => Some(bytes),
            Err(error) => {
                let error: SerializeError = error.into();
                tracing::error!(
                    tag = E::TYPE_TAG.get(),
                    %error,
                    "dropping event that can't be serialized"
                );
                None
            }
        })
        .collect()
}

async fn handle_events<E, H>(
    handler: &H,
    run: &[RunRecord<'_>],
    delays: &'static [Duration],
) -> RunResult
where
    E: Event,
    E::Target: Send + Sync,
    H: EventHandler<E>,
{
    let tag = E::TYPE_TAG.get();
    let mut events = decode::<E>(run);
    let mut start = 0usize;
    while start < events.len() {
        let end = start.saturating_add(H::MAX_BATCH.get()).min(events.len());
        let mut attempt = 0usize;
        loop {
            let Err(error) = handler.handle(&events[start..end]).await else {
                break;
            };
            match error.class() {
                ErrorClass::Transient => {
                    let delay = delay_for(delays, attempt);
                    tracing::warn!(tag, attempt, %error, ?delay, "transient handler failure, retrying chunk");
                    tokio::time::sleep(delay).await;
                    attempt = attempt.saturating_add(1);
                }
                ErrorClass::Unrecoverable => {
                    tracing::error!(tag, %error, "dropping chunk after unrecoverable handler failure");
                    break;
                }
                ErrorClass::Retryable => {
                    return RunResult::Failed {
                        error: error.into_source(),
                        remainder: encode::<E>(events.drain(start..)),
                    };
                }
            }
        }
        start = end;
    }
    RunResult::Done
}

impl<H, E> HandlerList for IggyConsumerRegisterCenter<H, E, ()>
where
    H: EventHandler<E>,
    E: Event + Send + Sync + 'static,
    E::Target: Send + Sync,
{
    fn topic(&self) -> Result<&'static str, crate::error::Error> {
        Ok(<E::Key as PartitionKey>::SUPER_PARTITION_NAME.0)
    }

    fn collect_tags(&self, out: &mut Vec<EventTypeTag>) {
        out.push(E::TYPE_TAG);
    }

    fn associativity(&self, tag: EventTypeTag) -> Option<EventAssociativity> {
        (tag == E::TYPE_TAG).then_some(<E::Algebra as Algebra<E::Target>>::ASSOCIATIVITY)
    }

    async fn handle_run<'a>(
        &'a self,
        tag: EventTypeTag,
        run: &'a [RunRecord<'a>],
        delays: &'static [Duration],
    ) -> RunResult {
        if tag != E::TYPE_TAG {
            tracing::error!(
                tag = tag.get(),
                count = run.len(),
                "dropping events with unknown type tag"
            );
            return RunResult::Done;
        }
        handle_events::<E, H>(&self.head, run, delays).await
    }

    fn reduce_run(&self, tag: EventTypeTag, run: &[RunRecord<'_>]) -> Vec<Box<[u8]>> {
        if tag != E::TYPE_TAG {
            return Vec::new();
        }
        encode::<E>(decode::<E>(run).into_iter())
    }
}

impl<H, E, Chain> HandlerList for IggyConsumerRegisterCenter<H, E, Chain>
where
    H: EventHandler<E>,
    E: Event + Send + Sync + 'static,
    E::Target: Send + Sync,
    Chain: HandlerList,
{
    fn topic(&self) -> Result<&'static str, crate::error::Error> {
        let first = self.chain.topic()?;
        let second = <E::Key as PartitionKey>::SUPER_PARTITION_NAME.0;
        if first != second {
            return Err(crate::error::Error::MixedTopics { first, second });
        }
        Ok(first)
    }

    fn collect_tags(&self, out: &mut Vec<EventTypeTag>) {
        out.push(E::TYPE_TAG);
        self.chain.collect_tags(out);
    }

    fn associativity(&self, tag: EventTypeTag) -> Option<EventAssociativity> {
        if tag == E::TYPE_TAG {
            Some(<E::Algebra as Algebra<E::Target>>::ASSOCIATIVITY)
        } else {
            self.chain.associativity(tag)
        }
    }

    async fn handle_run<'a>(
        &'a self,
        tag: EventTypeTag,
        run: &'a [RunRecord<'a>],
        delays: &'static [Duration],
    ) -> RunResult {
        if tag != E::TYPE_TAG {
            return self.chain.handle_run(tag, run, delays).await;
        }
        handle_events::<E, H>(&self.head, run, delays).await
    }

    fn reduce_run(&self, tag: EventTypeTag, run: &[RunRecord<'_>]) -> Vec<Box<[u8]>> {
        if tag != E::TYPE_TAG {
            return self.chain.reduce_run(tag, run);
        }
        encode::<E>(decode::<E>(run).into_iter())
    }
}
