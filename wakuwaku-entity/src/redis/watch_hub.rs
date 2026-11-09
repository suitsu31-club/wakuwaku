//! The reactor that wakes every wait on a dynamic structure.
//!
//! A wait on a [dynamic](super::dynamic) structure reads the item with a plain,
//! non-blocking command on the [`RedisSource`](crate::redis::RedisSource)
//! connection, like a [`Future`] polls. If there is nothing to take yet, it
//! sleeps until the item may have changed. Writers publish a wake-up on the
//! item's channel in the same Lua script as their write, and the
//! [`WatchHub`] of each process listens to the channels its waits need on
//! one Pub/Sub connection, the way a reactor watches sockets for a runtime.
//!
//! A wait subscribes before it reads, and reads again after each wake-up,
//! so it can't miss a change: one published before the subscription is
//! already visible to the read. A wake-up carries nothing; extra ones only
//! cost a read, and after a reconnect every wait reads again.
//!
//! See the [Redis guide](https://www.wakuwaku.rs/en/docs/data/redis#following-changes-the-watch-hub).

use futures_util::StreamExt;
use redis::aio::{PubSub, PubSubSink};
use redis::{Client, ErrorKind, RedisError, RedisResult, Value};
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug, Formatter};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep, timeout, timeout_at};

/// Time between two pings of an idle connection.
const PING_INTERVAL: Duration = Duration::from_secs(15);
/// Time a connection or a ping has to answer before the connection counts
/// as lost.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(5);
/// First delay before reconnecting.
const MIN_BACKOFF: Duration = Duration::from_millis(100);
/// Longest delay between two reconnection attempts.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// What the waits on one channel observe.
#[derive(Debug, Clone, Copy)]
struct SlotState {
    /// Bumped by every wake-up and every (re)subscription.
    epoch: u64,
    /// Whether the channel was subscribed once, so a read made now is
    /// followed by a wake-up if the item changes.
    live: bool,
}

/// The waits on one channel.
struct Slot {
    waits: usize,
    state: watch::Sender<SlotState>,
}

type Table = Mutex<HashMap<Vec<u8>, Slot>>;

fn lock(table: &Table) -> MutexGuard<'_, HashMap<Vec<u8>, Slot>> {
    // A panic while the table is locked leaves it consistent: every update
    // is a single field assignment.
    table.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Wake the waits on `channel`.
fn wake(table: &Table, channel: &[u8]) {
    if let Some(slot) = lock(table).get(channel) {
        slot.state
            .send_modify(|state| state.epoch = state.epoch.wrapping_add(1));
    }
}

/// Mark `channel` subscribed and wake its waits, which read again.
fn mark_live(table: &Table, channel: &[u8]) {
    if let Some(slot) = lock(table).get(channel) {
        slot.state.send_modify(|state| {
            state.live = true;
            state.epoch = state.epoch.wrapping_add(1);
        });
    }
}

/// What the waits share with the reactor.
struct Shared {
    table: Arc<Table>,
    /// Channels whose subscription may need to change. Dropping the last
    /// sender stops the reactor.
    changes: mpsc::UnboundedSender<Vec<u8>>,
}

/// The reactor waking the waits on the [dynamic](super::dynamic) structures of one
/// process.
///
/// Connect one per process and pass it to every wait: `receive`,
/// `subscribe`, `resume`, `wait` and `changed`. It holds the only
/// connection that waits need besides the shared
/// [`RedisSource`](crate::redis::RedisSource) one, whatever their number;
/// waits on the same item share one subscription.
///
/// Cloning is cheap: clones share the reactor. The reactor reconnects with
/// a backoff when the connection is lost, and stops once every clone and
/// every wait using it is dropped.
///
/// On Redis Cluster, connect it to any node: writers publish with
/// `PUBLISH`, which the cluster forwards to every node.
#[derive(Clone)]
pub struct WatchHub {
    shared: Arc<Shared>,
}

impl WatchHub {
    /// Open a Pub/Sub connection with `client` and start the reactor on the
    /// current Tokio runtime. `client` is kept to reconnect.
    ///
    /// # Errors
    ///
    /// Fails if the first connection fails. Later ones are retried.
    pub async fn connect(client: Client) -> RedisResult<Self> {
        let pubsub = client.get_async_pubsub().await?;
        let table = Arc::new(Table::default());
        let (changes, requests) = mpsc::unbounded_channel();
        let reactor = Reactor {
            client,
            table: Arc::clone(&table),
            requests,
        };
        tokio::spawn(reactor.run(pubsub));
        Ok(Self {
            shared: Arc::new(Shared { table, changes }),
        })
    }

    /// Start waiting on `channel`.
    pub(crate) fn waiter(&self, channel: Vec<u8>) -> Waiter {
        let mut table = lock(&self.shared.table);
        let slot = table.entry(channel.clone()).or_insert_with(|| Slot {
            waits: 0,
            state: watch::Sender::new(SlotState {
                epoch: 0,
                live: false,
            }),
        });
        slot.waits = slot.waits.saturating_add(1);
        let state = slot.state.subscribe();
        if slot.waits == 1 {
            // Fails only once the reactor stopped, and then nothing wakes the
            // wait but its deadline.
            let _ = self.shared.changes.send(channel.clone());
        }
        drop(table);
        Waiter {
            shared: Arc::clone(&self.shared),
            channel,
            state,
            live: false,
            seen: 0,
        }
    }
}

impl Debug for WatchHub {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("WatchHub")
            .field("channels", &lock(&self.shared.table).len())
            .finish()
    }
}

/// `future`, unless `deadline` passes first.
async fn until<F: Future>(deadline: Option<Instant>, future: F) -> Option<F::Output> {
    match deadline {
        Some(deadline) => timeout_at(deadline, future).await.ok(),
        None => Some(future.await),
    }
}

/// A wait on one channel, keeping it subscribed while it lives.
pub(crate) struct Waiter {
    shared: Arc<Shared>,
    channel: Vec<u8>,
    state: watch::Receiver<SlotState>,
    /// Whether the channel was seen subscribed.
    live: bool,
    /// Epoch at the last read.
    seen: u64,
}

impl Waiter {
    /// The reads of one wait, until `deadline`, `None` for no deadline.
    pub(crate) fn turns(&mut self, deadline: Option<Instant>) -> Turns<'_> {
        Turns {
            waiter: self,
            deadline,
            first: true,
            last: false,
        }
    }

    /// Wait for the channel to be subscribed. `false` if `deadline` passed
    /// first.
    async fn subscribed(&mut self, deadline: Option<Instant>) -> bool {
        if !self.live {
            let subscribed = until(deadline, self.state.wait_for(|state| state.live)).await;
            self.live = subscribed.is_some_and(|result| result.is_ok());
        }
        self.live
    }

    /// Wait for a wake-up after the last read. `false` if `deadline` passed
    /// first.
    async fn changed(&mut self, deadline: Option<Instant>) -> bool {
        let seen = self.seen;
        let changed = until(deadline, self.state.wait_for(|state| state.epoch != seen))
            .await
            .map(|result| result.is_ok());
        match changed {
            Some(true) => true,
            // The slot outlives its waits, so its sender can't be gone.
            Some(false) => {
                until(deadline, std::future::pending::<()>()).await;
                false
            }
            None => false,
        }
    }
}

impl Drop for Waiter {
    fn drop(&mut self) {
        let mut table = lock(&self.shared.table);
        if let Some(slot) = table.get_mut(&self.channel) {
            slot.waits = slot.waits.saturating_sub(1);
            if slot.waits == 0 {
                let _ = self.shared.changes.send(std::mem::take(&mut self.channel));
            }
        }
    }
}

/// The reads of one wait on a [`Waiter`]: read the item each time
/// [`next`](Self::next) outputs `true`, and stop once the read finds what
/// the wait is for.
pub(crate) struct Turns<'a> {
    waiter: &'a mut Waiter,
    deadline: Option<Instant>,
    first: bool,
    last: bool,
}

impl Turns<'_> {
    /// Whether to read the item (again). The first read comes once the
    /// channel is subscribed, each next one after a wake-up. Once the
    /// deadline passes, one last read comes, then `false`.
    pub(crate) async fn next(&mut self) -> bool {
        if self.last {
            return false;
        }
        let woken = if self.first {
            self.first = false;
            self.waiter.subscribed(self.deadline).await
        } else {
            self.waiter.changed(self.deadline).await
        };
        self.last = !woken;
        self.waiter.seen = self.waiter.state.borrow().epoch;
        true
    }
}

/// Why a connection stopped serving.
enum Ending {
    /// Every hub and wait is gone.
    Stopped,
    /// The connection failed.
    Lost(RedisError),
}

/// The task subscribing to channels and dispatching wake-ups.
struct Reactor {
    client: Client,
    table: Arc<Table>,
    requests: mpsc::UnboundedReceiver<Vec<u8>>,
}

impl Reactor {
    async fn run(mut self, mut pubsub: PubSub) {
        loop {
            match serve(&self.table, &mut self.requests, pubsub).await {
                Ending::Stopped => return,
                Ending::Lost(error) => {
                    tracing::warn!(%error, "watch hub lost its connection, reconnecting");
                }
            }
            match self.reconnect().await {
                Some(next) => pubsub = next,
                None => return,
            }
        }
    }

    /// A new connection, `None` if every hub and wait is gone meanwhile.
    async fn reconnect(&mut self) -> Option<PubSub> {
        let mut backoff = MIN_BACKOFF;
        loop {
            let delay = sleep(backoff);
            tokio::pin!(delay);
            // Changes requested meanwhile are applied by the resubscription;
            // only the end of the requests matters.
            loop {
                tokio::select! {
                    () = &mut delay => break,
                    request = self.requests.recv() => {
                        request?;
                    }
                }
            }
            match timeout(ANSWER_TIMEOUT, self.client.get_async_pubsub()).await {
                Ok(Ok(pubsub)) => return Some(pubsub),
                Ok(Err(error)) => tracing::warn!(%error, "watch hub failed to reconnect"),
                Err(_) => tracing::warn!("watch hub timed out reconnecting"),
            }
            backoff = backoff.saturating_mul(2).min(MAX_BACKOFF);
        }
    }
}

/// Serve the waits on `pubsub` until it fails or every hub and wait is
/// gone.
async fn serve(
    table: &Table,
    requests: &mut mpsc::UnboundedReceiver<Vec<u8>>,
    pubsub: PubSub,
) -> Ending {
    let (mut sink, mut messages) = pubsub.split();
    let mut pinger = sink.clone();
    let wakes = async {
        while let Some(message) = messages.next().await {
            if let Ok(channel) = message.get_channel::<Vec<u8>>() {
                wake(table, &channel);
            }
        }
    };
    let pings = async move {
        loop {
            sleep(PING_INTERVAL).await;
            match timeout(ANSWER_TIMEOUT, pinger.ping::<Value>()).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => return error,
                Err(_) => return lost("the watch hub connection stopped answering pings"),
            }
        }
    };
    let subscriptions = async {
        let mut subscribed = HashSet::new();
        // Resubscribe the channels that still have waits, and wake those so
        // they read what they missed while disconnected.
        let channels: Vec<Vec<u8>> = {
            let mut table = lock(table);
            table.retain(|_, slot| slot.waits > 0);
            table.keys().cloned().collect()
        };
        for channel in channels {
            sink.subscribe(&channel).await?;
            mark_live(table, &channel);
            subscribed.insert(channel);
        }
        while let Some(channel) = requests.recv().await {
            update(table, &mut sink, &mut subscribed, channel).await?;
        }
        Ok::<_, RedisError>(())
    };
    tokio::select! {
        () = wakes => Ending::Lost(lost("the watch hub connection closed")),
        error = pings => Ending::Lost(error),
        result = subscriptions => match result {
            Ok(()) => Ending::Stopped,
            Err(error) => Ending::Lost(error),
        },
    }
}

/// Subscribe to `channel` if it has waits, unsubscribe and forget it
/// otherwise.
async fn update(
    table: &Table,
    sink: &mut PubSubSink,
    subscribed: &mut HashSet<Vec<u8>>,
    channel: Vec<u8>,
) -> RedisResult<()> {
    let wanted = {
        let mut table = lock(table);
        match table.get(&channel) {
            Some(slot) if slot.waits > 0 => true,
            Some(_) => {
                table.remove(&channel);
                false
            }
            None => false,
        }
    };
    match (wanted, subscribed.contains(&channel)) {
        (true, false) => {
            sink.subscribe(&channel).await?;
            mark_live(table, &channel);
            subscribed.insert(channel);
        }
        (false, true) => {
            subscribed.remove(&channel);
            sink.unsubscribe(&channel).await?;
        }
        _ => {}
    }
    Ok(())
}

fn lost(detail: &'static str) -> RedisError {
    RedisError::from((
        ErrorKind::Io,
        "watch hub connection lost",
        detail.to_owned(),
    ))
}
