//! Publisher confirms: publish on a channel and wait until the broker has taken the message.

use crate::error::Error;
use amqprs::callbacks::ChannelCallback;
use amqprs::channel::{BasicPublishArguments, Channel, ConfirmSelectArguments};
use amqprs::connection::Connection;
use amqprs::{Ack, BasicProperties, Cancel, CloseChannel, Nack, Return};
use async_trait::async_trait;
use std::collections::BTreeMap;
use std::ops::Deref;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::oneshot;

/// How long a publish waits for the broker's confirm before it counts as failed.
pub const CONFIRM_TIMEOUT: Duration = Duration::from_secs(30);

/// What the broker did with a published message it confirmed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishOutcome {
    /// At least one queue took the message. For a persistent message on a durable queue the
    /// broker confirms only once the message is on disk.
    Routed,
    /// The message was published as `mandatory` but no queue was bound to take it. The broker
    /// returned it and it is gone.
    Returned {
        /// AMQP reply code, usually 312 (`NO_ROUTE`).
        reply_code: u16,
        /// The broker's explanation.
        reply_text: String,
    },
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
    /// A `basic.return` for the publish in flight. The broker sends it before that publish's
    /// `basic.ack`.
    returned: Option<(u16, String)>,
    /// Set once the channel closed, or once its callback was replaced or dropped.
    closed: Option<String>,
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
        let state = Arc::new(Self {
            tracker: Mutex::default(),
            publish_lock: tokio::sync::Mutex::new(()),
        });
        channel
            .register_callback(ConfirmCallback {
                state: state.clone(),
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
    pub(crate) async fn publish(
        &self,
        channel: &Channel,
        properties: BasicProperties,
        content: Vec<u8>,
        args: BasicPublishArguments,
    ) -> Result<PublishOutcome, Error> {
        let _in_flight = self.publish_lock.lock().await;
        let (tx, rx) = oneshot::channel();
        let seq = {
            let mut tracker = self.tracker();
            if let Some(reason) = &tracker.closed {
                return Err(amqp_error(format!("channel is unusable: {reason}")));
            }
            tracker.last_seq += 1;
            tracker.returned = None;
            let seq = tracker.last_seq;
            tracker.pending.insert(seq, tx);
            seq
        };

        let exchange = args.exchange.clone();
        let routing_key = args.routing_key.clone();
        if let Err(e) = channel.basic_publish(properties, content, args).await {
            // The frame never left, so the broker did not count it either.
            let mut tracker = self.tracker();
            tracker.pending.remove(&seq);
            tracker.last_seq -= 1;
            return Err(e.into());
        }

        match tokio::time::timeout(CONFIRM_TIMEOUT, rx).await {
            Ok(Ok(Confirmation::Ack { returned: None })) => Ok(PublishOutcome::Routed),
            Ok(Ok(Confirmation::Ack {
                returned: Some((reply_code, reply_text)),
            })) => Ok(PublishOutcome::Returned {
                reply_code,
                reply_text,
            }),
            Ok(Ok(Confirmation::Nack)) => Err(amqp_error(format!(
                "the broker refused (nacked) the message for exchange '{exchange}', routing key '{routing_key}'"
            ))),
            Ok(Ok(Confirmation::Closed(reason))) => Err(amqp_error(format!(
                "channel closed before the broker confirmed the message for exchange '{exchange}', routing key '{routing_key}': {reason}"
            ))),
            Ok(Err(_)) => Err(amqp_error(
                "channel closed before the broker confirmed the message".to_string(),
            )),
            Err(_) => {
                // A late confirm for this sequence number is ignored.
                self.tracker().pending.remove(&seq);
                Err(amqp_error(format!(
                    "no confirm from the broker within {CONFIRM_TIMEOUT:?} for exchange '{exchange}', routing key '{routing_key}'"
                )))
            }
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
        // A return belongs to the publish in flight; keep it until that one is confirmed.
        let returned = if delivery_tag == last_seq {
            tracker.returned.take()
        } else {
            None
        };
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
        self.state.tracker().close(format!(
            "closed by the broker: {} {}",
            close.reply_code(),
            close.reply_text()
        ));
        Ok(())
    }

    async fn cancel(
        &mut self,
        channel: &Channel,
        cancel: Cancel,
    ) -> Result<(), amqprs::error::Error> {
        #[cfg(feature = "tracing")]
        tracing::error!(
            channel = %channel,
            consumer_tag = %cancel.consumer_tag(),
            "RabbitMQ cancelled the consumer (was its queue deleted?)"
        );
        #[cfg(not(feature = "tracing"))]
        let _ = (channel, cancel);
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

    /// Publish a message and wait until the broker confirms it (at most [`CONFIRM_TIMEOUT`]).
    ///
    /// Returns an error when the broker nacks the message, when the channel closes first
    /// (for example because the exchange does not exist) or on timeout. A `mandatory` message
    /// that no queue takes is confirmed as [`PublishOutcome::Returned`].
    pub async fn publish(
        &self,
        properties: BasicProperties,
        content: Vec<u8>,
        args: BasicPublishArguments,
    ) -> Result<PublishOutcome, Error> {
        self.state
            .publish(&self.channel, properties, content, args)
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
