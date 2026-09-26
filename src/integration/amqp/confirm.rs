//! Publisher confirms: publish on a channel and wait until the broker has taken the message.

use crate::error::Error;
use amqprs::callbacks::ChannelCallback;
use amqprs::channel::{BasicPublishArguments, Channel, ConfirmSelectArguments};
use amqprs::connection::Connection;
use amqprs::{Ack, BasicProperties, Cancel, CloseChannel, Nack, Return};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::time::Instant;

/// How long [`AmqpMessageSend::send`](super::AmqpMessageSend::send) and
/// [`ConfirmChannel::publish`] wait for the broker. Past it, a message that was already handed
/// to the connection is reported as [`PublishOutcome::Unconfirmed`], and one that was not is an
/// error.
pub const CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);

/// What became of a published message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    /// The broker confirmed it and at least one queue took it. For a persistent message on a
    /// durable queue the broker confirms only once the message is on disk.
    Routed,
    /// The message was published as `mandatory` but no queue was bound to take it. The broker
    /// returned it and it is gone.
    Returned {
        /// AMQP reply code, usually 312 (`NO_ROUTE`).
        reply_code: u16,
        /// The broker's explanation.
        reply_text: String,
    },
    /// The message was handed to the connection, but the broker did not confirm it in time,
    /// typically because a memory or disk alarm made it stop reading from publishing
    /// connections.
    ///
    /// The outcome is unknown, not failed: the broker takes the message once it reads it
    /// again, unless the connection is lost first. Publishing it again can deliver it twice.
    Unconfirmed,
}

enum Confirmation {
    Ack { returned: Option<(u16, String)> },
    Nack,
    Closed(String),
}

#[derive(Default)]
struct Tracker {
    /// Sequence number of the last publish, counted the way the broker counts them: from 1,
    /// once the channel is in confirm mode.
    last_seq: u64,
    pending: BTreeMap<u64, oneshot::Sender<Confirmation>>,
    /// A `basic.return` waiting for the confirm that follows it. The broker sends a return
    /// right before the confirm of the same message.
    returned: Option<(u16, String)>,
    /// Set once the channel closed, or once its callback was replaced or dropped.
    closed: Option<String>,
}

/// Counts the channels the broker closed on one connection.
///
/// amqprs frees a channel number only when the client closes the channel. A channel the broker
/// closed (for example after a publish to an exchange that does not exist) keeps its number for
/// as long as the connection lives, and once every number is taken amqprs cannot open channels
/// any more while the connection still reports itself open. [`AmqpPool`](super::AmqpPool)
/// closes the connection before that happens, so the application sees a lost connection.
pub(crate) struct ChannelLeaks {
    closed_by_broker: AtomicUsize,
    /// How many channels the broker may close before the connection has to go.
    budget: usize,
    closing: AtomicBool,
}

impl ChannelLeaks {
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            closed_by_broker: AtomicUsize::new(0),
            budget,
            closing: AtomicBool::new(false),
        }
    }

    fn record(&self) {
        self.closed_by_broker.fetch_add(1, Ordering::Relaxed);
    }

    /// `Some(count)` once the broker closed `budget` channels.
    pub(crate) fn exhausted(&self) -> Option<usize> {
        let count = self.closed_by_broker.load(Ordering::Relaxed);
        (count >= self.budget).then_some(count)
    }

    /// `true` for the first caller only.
    pub(crate) fn start_closing(&self) -> bool {
        !self.closing.swap(true, Ordering::Relaxed)
    }

    pub(crate) fn budget(&self) -> usize {
        self.budget
    }
}

/// Confirm bookkeeping for one channel, shared with the callback registered on it.
pub(crate) struct ConfirmState {
    tracker: Mutex<Tracker>,
    /// Keeps one publish in flight at a time, so that a `basic.return` can only belong to it.
    publish_lock: tokio::sync::Mutex<()>,
}

impl ConfirmState {
    /// Put `channel` into confirm mode and register the callback that tracks its confirms.
    ///
    /// This replaces any callback registered on the channel before. Publishing on the channel
    /// must go through [`publish`](Self::publish) from then on: a publish that bypasses it
    /// shifts the broker's numbering, and the channel is marked unusable once that shows.
    pub(crate) async fn attach(channel: &Channel) -> Result<Arc<Self>, amqprs::error::Error> {
        Self::attach_counting(channel, None).await
    }

    /// Like [`attach`](Self::attach), and counts the channel in `leaks` if the broker closes it.
    pub(crate) async fn attach_counting(
        channel: &Channel,
        leaks: Option<Arc<ChannelLeaks>>,
    ) -> Result<Arc<Self>, amqprs::error::Error> {
        let state = Arc::new(Self {
            tracker: Mutex::default(),
            publish_lock: tokio::sync::Mutex::new(()),
        });
        channel
            .register_callback(ConfirmCallback {
                state: state.clone(),
                leaks,
            })
            .await?;
        channel
            .confirm_select(ConfirmSelectArguments::new(false))
            .await?;
        Ok(state)
    }

    fn tracker(&self) -> MutexGuard<'_, Tracker> {
        // Nothing panics while holding the lock; recover the data if something ever does.
        self.tracker.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `false` once the channel closed or its confirms can no longer be matched to publishes.
    pub(crate) fn is_usable(&self) -> bool {
        self.tracker().closed.is_none()
    }

    /// Publish on `channel` and wait for the broker's confirm.
    ///
    /// With a `deadline`, a message that is not handed to the connection by then is an error
    /// (it was not published), and one that is but gets no confirm by then is
    /// [`PublishOutcome::Unconfirmed`]. Without one, it waits until the broker confirms the
    /// message or the channel closes, and never returns `Unconfirmed`.
    pub(crate) async fn publish(
        &self,
        channel: &Channel,
        properties: BasicProperties,
        content: Vec<u8>,
        args: BasicPublishArguments,
        deadline: Option<Instant>,
    ) -> Result<PublishOutcome, Error> {
        let target = format!(
            "exchange '{}', routing key '{}'",
            args.exchange, args.routing_key
        );

        let hand_over = async {
            let in_flight = self.publish_lock.lock().await;
            let (seq, rx) = {
                let mut tracker = self.tracker();
                if let Some(reason) = &tracker.closed {
                    return Err(amqp_error(format!("channel is unusable: {reason}")));
                }
                tracker.last_seq += 1;
                let seq = tracker.last_seq;
                let (tx, rx) = oneshot::channel();
                tracker.pending.insert(seq, tx);
                (seq, rx)
            };
            // Until `basic_publish` succeeds, the frame has not reached the connection's
            // outgoing queue and the broker will not count it. When it fails, or when this
            // future is dropped meanwhile (a caller's timeout, a client that went away), the
            // tracker must forget the publish too, or every later confirm on this channel
            // would be matched to the publish before it.
            let unsent = Unsent { state: self, seq };
            channel.basic_publish(properties, content, args).await?;
            unsent.disarm();
            Ok((in_flight, seq, rx))
        };
        let (_in_flight, seq, rx) = match deadline {
            Some(deadline) => tokio::time::timeout_at(deadline, hand_over)
                .await
                .map_err(|_| {
                    amqp_error(format!(
                        "the message for {target} was not handed to the connection in time, so it was not published"
                    ))
                })??,
            None => hand_over.await?,
        };

        let confirmation = match deadline {
            Some(deadline) => match tokio::time::timeout_at(deadline, rx).await {
                Ok(confirmation) => confirmation,
                Err(_) => {
                    // A late confirm for this sequence number finds no waiter and is ignored.
                    self.tracker().pending.remove(&seq);
                    return Ok(PublishOutcome::Unconfirmed);
                }
            },
            None => rx.await,
        };
        match confirmation {
            Ok(Confirmation::Ack { returned: None }) => Ok(PublishOutcome::Routed),
            Ok(Confirmation::Ack {
                returned: Some((reply_code, reply_text)),
            }) => Ok(PublishOutcome::Returned {
                reply_code,
                reply_text,
            }),
            Ok(Confirmation::Nack) => Err(amqp_error(format!(
                "the broker refused (nacked) the message for {target}"
            ))),
            Ok(Confirmation::Closed(reason)) => Err(amqp_error(format!(
                "channel closed before the broker confirmed the message for {target}: {reason}"
            ))),
            Err(_) => Err(amqp_error(format!(
                "channel closed before the broker confirmed the message for {target}"
            ))),
        }
    }

    fn confirm(&self, delivery_tag: u64, multiple: bool, positive: bool) {
        let mut tracker = self.tracker();
        if delivery_tag > tracker.last_seq {
            // Something published on this channel without going through `publish`.
            tracker.close("publisher confirms are out of step with the broker".to_string());
            return;
        }
        let last_seq = tracker.last_seq;
        // The return, if any, belongs to the message confirmed now. That is the publish in
        // flight when this confirms the last sequence number; otherwise it is an earlier
        // publish that stopped waiting (`Unconfirmed`), and the return must not be pinned on
        // the one after it.
        let returned = tracker.returned.take().filter(|_| delivery_tag == last_seq);
        let confirmed = if multiple {
            let rest = tracker.pending.split_off(&(delivery_tag + 1));
            std::mem::replace(&mut tracker.pending, rest)
        } else {
            tracker
                .pending
                .remove_entry(&delivery_tag)
                .into_iter()
                .collect()
        };
        for (seq, waiter) in confirmed {
            let confirmation = if positive {
                Confirmation::Ack {
                    returned: if seq == last_seq {
                        returned.clone()
                    } else {
                        None
                    },
                }
            } else {
                Confirmation::Nack
            };
            let _ = waiter.send(confirmation);
        }
    }
}

/// Takes a publish's sequence number back unless the publish reached the connection.
struct Unsent<'a> {
    state: &'a ConfirmState,
    seq: u64,
}

impl Unsent<'_> {
    /// The frame is in the connection's outgoing queue: keep the sequence number.
    fn disarm(self) {
        std::mem::forget(self);
    }
}

impl Drop for Unsent<'_> {
    fn drop(&mut self) {
        // amqprs sends the whole publish with a single `send` on a tokio mpsc, which enqueues
        // nothing when it fails or is dropped. `publish_lock` is still held, so no other publish
        // took a sequence number after this one.
        let mut tracker = self.state.tracker();
        tracker.pending.remove(&self.seq);
        if tracker.last_seq == self.seq {
            tracker.last_seq -= 1;
        } else {
            tracker.close("a cancelled publish left the confirm numbering unknown".to_string());
        }
    }
}

impl Tracker {
    fn close(&mut self, reason: String) {
        for (_, waiter) in std::mem::take(&mut self.pending) {
            let _ = waiter.send(Confirmation::Closed(reason.clone()));
        }
        self.closed.get_or_insert(reason);
    }
}

fn amqp_error(message: String) -> Error {
    Error::AmqpError(amqprs::error::Error::ChannelUseError(message))
}

/// The callback that feeds a channel's confirms, returns and close into its [`ConfirmState`].
struct ConfirmCallback {
    state: Arc<ConfirmState>,
    leaks: Option<Arc<ChannelLeaks>>,
}

impl Drop for ConfirmCallback {
    /// The channel's dispatcher drops its callback when the channel or connection closes, or
    /// when another callback replaces it. Either way no more confirms arrive for this state.
    fn drop(&mut self) {
        self.state
            .tracker()
            .close("channel closed or its callback was replaced".to_string());
    }
}

#[async_trait]
impl ChannelCallback for ConfirmCallback {
    async fn close(
        &mut self,
        channel: &Channel,
        close: CloseChannel,
    ) -> Result<(), amqprs::error::Error> {
        #[cfg(feature = "tracing")]
        tracing::error!(
            channel = %channel,
            reply_code = close.reply_code(),
            reply_text = %close.reply_text(),
            "RabbitMQ closed the channel"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = channel;
        if let Some(leaks) = &self.leaks {
            leaks.record();
        }
        self.state.tracker().close(format!(
            "closed by the broker: {} {}",
            close.reply_code(),
            close.reply_text()
        ));
        Ok(())
    }

    /// The broker cancelled a consumer on this channel, usually because its queue was deleted.
    /// Nothing is delivered to it any more, but the channel stays open. Close the channel, so
    /// that [`AmqpConsumersRuntime::closed`](crate::services::amqp_consumer::AmqpConsumersRuntime::closed)
    /// reports the consumer as gone and the service can set it up again.
    async fn cancel(
        &mut self,
        channel: &Channel,
        cancel: Cancel,
    ) -> Result<(), amqprs::error::Error> {
        #[cfg(feature = "tracing")]
        tracing::error!(
            channel = %channel,
            consumer_tag = %cancel.consumer_tag(),
            "RabbitMQ cancelled the consumer (was its queue deleted?); closing its channel"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = cancel;
        // The close handshake goes through this channel's dispatcher, which is busy running
        // this callback: close from another task.
        let channel = channel.clone();
        tokio::spawn(async move {
            let _ = channel.close().await;
        });
        Ok(())
    }

    async fn flow(
        &mut self,
        _channel: &Channel,
        active: bool,
    ) -> Result<bool, amqprs::error::Error> {
        Ok(active)
    }

    async fn publish_ack(&mut self, _channel: &Channel, ack: Ack) {
        self.state.confirm(ack.delivery_tag(), ack.mutiple(), true);
    }

    async fn publish_nack(&mut self, _channel: &Channel, nack: Nack) {
        self.state
            .confirm(nack.delivery_tag(), nack.multiple(), false);
    }

    async fn publish_return(
        &mut self,
        _channel: &Channel,
        ret: Return,
        _basic_properties: BasicProperties,
        _content: Vec<u8>,
    ) {
        self.state.tracker().returned = Some((ret.reply_code(), ret.reply_text().clone()));
    }
}

/// A channel in publisher-confirm mode. [`AmqpPool`](super::AmqpPool) hands these out.
///
/// It dereferences to the [`Channel`] for declaring exchanges and queues. Publish only through
/// [`publish`](Self::publish): a `basic_publish` on the bare channel is confirmed by the broker
/// as well, which throws off the confirm numbering and makes the channel unusable.
pub struct ConfirmChannel {
    channel: Channel,
    state: Arc<ConfirmState>,
}

impl ConfirmChannel {
    /// Open a new channel on `connection` and put it into confirm mode.
    pub async fn open(connection: &Connection) -> Result<Self, amqprs::error::Error> {
        let channel = connection.open_channel(None).await?;
        Self::attach(channel).await
    }

    /// Like [`open`](Self::open), and counts the channel in `leaks` if the broker closes it.
    pub(crate) async fn open_counting(
        connection: &Connection,
        leaks: Arc<ChannelLeaks>,
    ) -> Result<Self, amqprs::error::Error> {
        let channel = connection.open_channel(None).await?;
        let state = ConfirmState::attach_counting(&channel, Some(leaks)).await?;
        Ok(Self { channel, state })
    }

    /// Put an open channel that nothing has published on into confirm mode.
    ///
    /// Replaces any callback registered on the channel.
    pub async fn attach(channel: Channel) -> Result<Self, amqprs::error::Error> {
        let state = ConfirmState::attach(&channel).await?;
        Ok(Self { channel, state })
    }

    /// Whether the channel is open and its confirms can still be matched to publishes.
    pub fn is_usable(&self) -> bool {
        self.channel.is_open() && self.state.is_usable()
    }

    /// Publish a message and wait until the broker confirms it, at most [`CONFIRM_TIMEOUT`].
    ///
    /// Returns an error when the message did not reach the broker: it was not handed to the
    /// connection within [`CONFIRM_TIMEOUT`], the broker nacked it, or the channel closed
    /// first (for example because the exchange does not exist). A `mandatory` message that no
    /// queue takes is [`PublishOutcome::Returned`], and one the broker did not confirm in time
    /// is [`PublishOutcome::Unconfirmed`].
    pub async fn publish(
        &self,
        properties: BasicProperties,
        content: Vec<u8>,
        args: BasicPublishArguments,
    ) -> Result<PublishOutcome, Error> {
        self.publish_until(
            properties,
            content,
            args,
            Some(Instant::now() + CONFIRM_TIMEOUT),
        )
        .await
    }

    /// [`publish`](Self::publish) with the given deadline, or without one.
    pub(crate) async fn publish_until(
        &self,
        properties: BasicProperties,
        content: Vec<u8>,
        args: BasicPublishArguments,
        deadline: Option<Instant>,
    ) -> Result<PublishOutcome, Error> {
        self.state
            .publish(&self.channel, properties, content, args, deadline)
            .await
    }

    /// The underlying channel.
    pub fn channel(&self) -> &Channel {
        &self.channel
    }

    /// Give up confirm tracking and return the underlying channel.
    pub fn into_channel(self) -> Channel {
        self.channel
    }
}

impl Deref for ConfirmChannel {
    type Target = Channel;

    fn deref(&self) -> &Channel {
        &self.channel
    }
}
