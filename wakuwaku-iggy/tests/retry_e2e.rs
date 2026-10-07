//! End-to-end tests against a running Iggy server.
//!
//! ```text
//! WAKUWAKU_IGGY_URL=iggy://iggy:iggy@127.0.0.1:8090 \
//!   cargo test -p wakuwaku-iggy --test retry_e2e -- --ignored --test-threads=1
//! ```

use iggy::prelude::{
    Client, Consumer, HeaderKey, Identifier, IggyClient, MessageClient, PollingStrategy,
    StreamClient, TopicClient, TopicCreateOptions,
};
use kanau::message::{MessageDe, MessageSer};
use std::collections::hash_map::DefaultHasher;
use std::num::NonZeroUsize;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use wakuwaku_iggy::algebra::{Associative, EventSemigroup};
use wakuwaku_iggy::consumer::{ConsumerConfig, ConsumerRuntime};
use wakuwaku_iggy::error::{ErrorClass, HandleError};
use wakuwaku_iggy::events::{Event, EventAtomicOrdering, EventBody, EventTypeTag};
use wakuwaku_iggy::handler::{EventHandler, IggyConsumerRegisterCenter};
use wakuwaku_iggy::headers::RETRY_HEADER;
use wakuwaku_iggy::partition::{IggySuperPartitionName, PartitionKey};
use wakuwaku_iggy::publisher::Publisher;

const DELAYS: &[Duration] = &[
    Duration::from_millis(200),
    Duration::from_millis(400),
    Duration::from_millis(800),
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Add {
    user: u64,
    amount: u64,
}

#[derive(Hash)]
struct User(u64);

impl PartitionKey for User {
    type PartitionMerge = DefaultHasher;
    const SUPER_PARTITION_NAME: IggySuperPartitionName = IggySuperPartitionName("add");
}

impl MessageSer for Add {
    type SerError = anyhow::Error;
    fn to_bytes(self) -> Result<Box<[u8]>, Self::SerError> {
        let mut bytes = Vec::with_capacity(16);
        bytes.extend_from_slice(&self.user.to_le_bytes());
        bytes.extend_from_slice(&self.amount.to_le_bytes());
        Ok(bytes.into_boxed_slice())
    }
}

impl MessageDe for Add {
    type DeError = anyhow::Error;
    fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeError> {
        let (user, amount) = bytes
            .split_first_chunk::<8>()
            .and_then(|(user, rest)| Some((user, rest.first_chunk::<8>()?)))
            .ok_or_else(|| anyhow::anyhow!("expected 16 bytes, got {}", bytes.len()))?;
        Ok(Add {
            user: u64::from_le_bytes(*user),
            amount: u64::from_le_bytes(*amount),
        })
    }
}

impl EventBody for Add {}

impl EventSemigroup for Add {
    fn combine(self, other: Self) -> Self {
        Add {
            user: self.user,
            amount: self.amount + other.amount,
        }
    }
}

impl Event for Add {
    const TYPE_TAG: EventTypeTag = EventTypeTag::new(1);
    type Key = User;
    type Algebra = Associative;
    fn key(&self) -> User {
        User(self.user)
    }
    fn atomic_ordering(&self) -> EventAtomicOrdering {
        EventAtomicOrdering::Relaxed
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Call {
    user: u64,
    amounts: Vec<u64>,
    ok: bool,
}

/// Decides the outcome of a call from the user, the amounts and the number
/// of earlier calls for that user.
type FailWhen = dyn Fn(u64, &[u64], usize) -> Option<ErrorClass> + Send + Sync;

struct Recorder {
    calls: Arc<Mutex<Vec<Call>>>,
    fail: Box<FailWhen>,
}

impl EventHandler<Add> for Recorder {
    const MAX_BATCH: NonZeroUsize = NonZeroUsize::new(100).expect("non-zero");

    async fn handle(&self, run: &[Add]) -> Result<(), HandleError> {
        let user = run[0].user;
        let amounts: Vec<u64> = run.iter().map(|add| add.amount).collect();
        let mut calls = self.calls.lock().expect("calls lock");
        let earlier = calls.iter().filter(|call| call.user == user).count();
        let outcome = (self.fail)(user, &amounts, earlier);
        calls.push(Call {
            user,
            amounts,
            ok: outcome.is_none(),
        });
        match outcome {
            None => Ok(()),
            Some(class) => Err(HandleError::new(class, anyhow::anyhow!("injected failure"))),
        }
    }
}

struct Fixture {
    client: Arc<IggyClient>,
    stream: String,
    partitions: Vec<u32>,
    calls: Arc<Mutex<Vec<Call>>>,
    publisher: Publisher,
}

impl Fixture {
    async fn new(test: &str) -> Self {
        let url = std::env::var("WAKUWAKU_IGGY_URL")
            .unwrap_or_else(|_| "iggy://iggy:iggy@127.0.0.1:8090".to_owned());
        let client = IggyClient::from_connection_string(&url).expect("connection string");
        client.connect().await.expect("connect");
        let client = Arc::new(client);

        let stream = format!("wakuwaku_e2e_{test}");
        let stream_id = Identifier::named(&stream).expect("stream id");
        if client
            .get_stream(&stream_id)
            .await
            .expect("get stream")
            .is_some()
        {
            client
                .delete_stream(&stream_id)
                .await
                .expect("delete stream");
        }
        client.create_stream(&stream).await.expect("create stream");
        let options = TopicCreateOptions {
            partitions_count: Some(2),
            ..Default::default()
        };
        let topic = client
            .create_topic(&stream_id, "add", &options)
            .await
            .expect("create topic");
        let partitions = topic.partitions.iter().map(|p| p.id).collect();
        let publisher = Publisher::new(client.clone(), &stream).expect("publisher");
        Fixture {
            client,
            stream,
            partitions,
            calls: Arc::new(Mutex::new(Vec::new())),
            publisher,
        }
    }

    async fn start(
        &self,
        fail: impl Fn(u64, &[u64], usize) -> Option<ErrorClass> + Send + Sync + 'static,
    ) -> ConsumerRuntime {
        let handler = Recorder {
            calls: self.calls.clone(),
            fail: Box::new(fail),
        };
        let mut config = ConsumerConfig::new(&self.stream, "e2e", self.partitions.clone());
        config.retry_delays = DELAYS;
        config.poll_interval = Duration::from_millis(20);
        IggyConsumerRegisterCenter::<_, Add>::new(Arc::new(handler))
            .start(self.client.clone(), config)
            .await
            .expect("start consumer")
    }

    async fn publish(&self, user: u64, amount: u64) {
        self.publisher
            .publish(Add { user, amount })
            .await
            .expect("publish");
    }

    fn calls_of(&self, user: u64) -> Vec<Call> {
        let calls = self.calls.lock().expect("calls lock");
        calls
            .iter()
            .filter(|call| call.user == user)
            .cloned()
            .collect()
    }

    async fn wait_for(&self, what: &str, condition: impl Fn(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !condition(self) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}; calls: {:?}",
                self.calls.lock().expect("calls lock")
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Every record in the retry topic, decoded, with whether it carries the
    /// retry header.
    async fn retry_records(&self) -> Vec<(Add, bool)> {
        let stream_id = Identifier::named(&self.stream).expect("stream id");
        let topic_id = Identifier::named("retry_add").expect("topic id");
        let consumer = Consumer::new(Identifier::named("e2e-inspect").expect("consumer id"));
        let retry_key = HeaderKey::from_str(RETRY_HEADER).expect("header key");
        let mut records = Vec::new();
        for &partition in &self.partitions {
            let polled = self
                .client
                .poll_messages(
                    &stream_id,
                    &topic_id,
                    Some(partition),
                    &consumer,
                    &PollingStrategy::first(),
                    1000,
                    false,
                )
                .await
                .expect("poll retry topic");
            for message in &polled.messages {
                let add = Add::from_bytes(&message.payload).expect("decode retry record");
                let headers = message.user_headers_map().expect("headers");
                let has_retry = headers.is_some_and(|map| map.contains_key(&retry_key));
                records.push((add, has_retry));
            }
        }
        records
    }

    async fn finish(self, runtime: ConsumerRuntime) {
        runtime.shutdown().await;
        let stream_id = Identifier::named(&self.stream).expect("stream id");
        self.client
            .delete_stream(&stream_id)
            .await
            .expect("delete stream");
    }
}

#[tokio::test]
#[ignore = "needs a running Iggy server"]
async fn retryable_failure_is_retried_in_key_order() {
    let fixture = Fixture::new("retry_order").await;
    fixture.publish(1, 1).await;
    fixture.publish(1, 2).await;
    fixture.publish(2, 5).await;
    let runtime = fixture
        .start(|user, _, earlier| (user == 1 && earlier == 0).then_some(ErrorClass::Retryable))
        .await;

    fixture
        .wait_for("user 1 to fail", |f| f.calls_of(1).iter().any(|c| !c.ok))
        .await;
    fixture.publish(1, 10).await;
    let successful = |f: &Fixture| -> Vec<u64> {
        f.calls_of(1)
            .into_iter()
            .filter(|call| call.ok)
            .flat_map(|call| call.amounts)
            .collect()
    };
    fixture
        .wait_for("user 1 to apply 13", |f| {
            successful(f).iter().sum::<u64>() == 13
        })
        .await;
    fixture
        .wait_for("user 2 to apply 5", |f| !f.calls_of(2).is_empty())
        .await;

    assert_eq!(
        fixture.calls_of(2),
        [Call {
            user: 2,
            amounts: vec![5],
            ok: true
        }]
    );
    let user1 = fixture.calls_of(1);
    assert_eq!(
        user1[0],
        Call {
            user: 1,
            amounts: vec![3],
            ok: false
        }
    );
    let applied = successful(&fixture);
    assert!(
        applied == [13] || applied == [3, 10],
        "user 1 applied {applied:?}; calls {user1:?}"
    );
    let retry = fixture.retry_records().await;
    let first_user1 = retry.iter().find(|(add, _)| add.user == 1);
    assert_eq!(
        first_user1,
        Some(&(Add { user: 1, amount: 3 }, true)),
        "retry records: {retry:?}"
    );
    fixture.finish(runtime).await;
}

#[tokio::test]
#[ignore = "needs a running Iggy server"]
async fn exhausted_retries_drop_only_the_failing_run() {
    let fixture = Fixture::new("exhausted").await;
    let runtime = fixture
        .start(|_, amounts, _| (amounts == [1]).then_some(ErrorClass::Retryable))
        .await;
    fixture.publish(3, 1).await;
    fixture
        .wait_for("four failed calls", |f| {
            f.calls_of(3).iter().filter(|c| !c.ok).count() == 4
        })
        .await;
    fixture.publish(3, 2).await;
    fixture
        .wait_for("user 3 to apply 2", |f| f.calls_of(3).iter().any(|c| c.ok))
        .await;
    tokio::time::sleep(Duration::from_secs(2)).await;

    let failed = |amounts: Vec<u64>| Call {
        user: 3,
        amounts,
        ok: false,
    };
    let expected = [
        failed(vec![1]),
        failed(vec![1]),
        failed(vec![1]),
        failed(vec![1]),
        Call {
            user: 3,
            amounts: vec![2],
            ok: true,
        },
    ];
    assert_eq!(fixture.calls_of(3), expected);
    fixture.finish(runtime).await;
}

#[tokio::test]
#[ignore = "needs a running Iggy server"]
async fn transient_error_retries_in_place() {
    let fixture = Fixture::new("transient").await;
    let runtime = fixture
        .start(|user, _, earlier| (user == 4 && earlier == 0).then_some(ErrorClass::Transient))
        .await;
    fixture.publish(4, 7).await;
    fixture
        .wait_for("user 4 to apply 7", |f| f.calls_of(4).iter().any(|c| c.ok))
        .await;

    let expected = [
        Call {
            user: 4,
            amounts: vec![7],
            ok: false,
        },
        Call {
            user: 4,
            amounts: vec![7],
            ok: true,
        },
    ];
    assert_eq!(fixture.calls_of(4), expected);
    let retry = fixture.retry_records().await;
    assert!(
        retry.iter().all(|(add, _)| add.user != 4),
        "retry records: {retry:?}"
    );
    fixture.finish(runtime).await;
}
