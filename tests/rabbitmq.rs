//! Consumer failure handling and publishing against a real RabbitMQ.
//!
//! Set `WAKUWAKU_TEST_AMQP_URL` to run them, for example `amqp://guest:guest@localhost:5672`
//! (without a path, the vhost is `/`). Without it every test returns right away.
#![cfg(feature = "amqprs")]

use amqprs::channel::{
    BasicGetArguments, BasicPublishArguments, Channel, QueueDeclareArguments, QueueDeleteArguments,
};
use amqprs::connection::{Connection, OpenConnectionArguments};
use amqprs::{BasicProperties, FieldValue};
use kanau::message::{MessageDe, MessageSer};
use kanau::processor::Processor;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use wakuwaku::Error;
use wakuwaku::integration::amqp::{
    AmqpExchangeType, AmqpMessageProcessor, AmqpMessageSend, AmqpPool, AmqpRouting,
    DEAD_LETTER_REASON_HEADER, FAILED_ATTEMPTS_HEADER, LAST_ERROR_HEADER, QUEUE_HEADER,
    RetryPolicy, dead_letter_queue_name, retry_queue_name,
};
use wakuwaku::services::amqp_consumer::AmqpConsumerRegisterCenter;

async fn connect() -> Option<Connection> {
    let url = std::env::var("WAKUWAKU_TEST_AMQP_URL").ok()?;
    let args = OpenConnectionArguments::try_from(url.as_str()).expect("valid AMQP URL");
    Some(
        Connection::open(&args)
            .await
            .expect("RabbitMQ is reachable"),
    )
}

/// Remove what an earlier run of a test left behind.
async fn delete_queues(channel: &Channel, queue: &str) {
    for name in [
        queue.to_string(),
        retry_queue_name(queue),
        dead_letter_queue_name(queue),
    ] {
        channel
            .queue_delete(QueueDeleteArguments::new(&name))
            .await
            .unwrap();
    }
}

async fn ready_messages(channel: &Channel, queue: &str) -> u32 {
    let (_, count, _) = channel
        .queue_declare(QueueDeclareArguments::new(queue).passive(true).finish())
        .await
        .unwrap()
        .unwrap();
    count
}

async fn wait_for(what: &str, mut done: impl AsyncFnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while !done().await {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn header<'a>(properties: &'a BasicProperties, name: &str) -> Option<&'a FieldValue> {
    properties.headers()?.get(&name.try_into().unwrap())
}

fn header_text(properties: &BasicProperties, name: &str) -> String {
    match header(properties, name) {
        Some(FieldValue::S(text)) => AsRef::<String>::as_ref(text).clone(),
        other => panic!("header {name} is {other:?}"),
    }
}

/// An event whose body is a big-endian `u32`; any other body fails to decode.
macro_rules! event {
    ($name:ident, $exchange:literal) => {
        struct $name(u32);

        impl MessageSer for $name {
            type SerError = anyhow::Error;
            fn to_bytes(self) -> Result<Box<[u8]>, anyhow::Error> {
                Ok(Box::new(self.0.to_be_bytes()))
            }
        }

        impl MessageDe for $name {
            type DeError = anyhow::Error;
            fn from_bytes(bytes: &[u8]) -> Result<Self, anyhow::Error> {
                let bytes: [u8; 4] = bytes.try_into()?;
                Ok(Self(u32::from_be_bytes(bytes)))
            }
        }

        impl AmqpRouting for $name {
            const EXCHANGE: &'static str = $exchange;
            const EXCHANGE_TYPE: AmqpExchangeType = AmqpExchangeType::Direct;
            const ROUTING_KEY: &'static str = "test";
        }

        impl AmqpMessageSend for $name {}
    };
}

/// A processor that fails its first `failures` attempts with `error()`, then succeeds, and
/// records when each attempt happened.
macro_rules! processor {
    ($name:ident, $event:ident, $queue:literal, $max_attempts:expr) => {
        struct $name {
            failures: usize,
            error: fn() -> Error,
            attempts: Mutex<Vec<Instant>>,
        }

        #[allow(dead_code, reason = "not every test looks at the attempts")]
        impl $name {
            fn new(failures: usize, error: fn() -> Error) -> Arc<Self> {
                Arc::new(Self {
                    failures,
                    error,
                    attempts: Mutex::default(),
                })
            }

            fn attempts(&self) -> Vec<Instant> {
                self.attempts.lock().unwrap().clone()
            }
        }

        impl Processor<$event> for $name {
            type Output = ();
            type Error = Error;
            async fn process(&self, _: $event) -> Result<(), Error> {
                let mut attempts = self.attempts.lock().unwrap();
                attempts.push(Instant::now());
                if attempts.len() <= self.failures {
                    Err((self.error)())
                } else {
                    Ok(())
                }
            }
        }

        impl AmqpMessageProcessor<$event> for $name {
            const QUEUE: &'static str = $queue;
            const RETRY_POLICY: RetryPolicy = RetryPolicy::DEFAULT
                .with_max_attempts($max_attempts)
                .with_backoff(Duration::from_millis(200), 2, Duration::from_secs(1));
        }
    };
}

fn io_error() -> Error {
    Error::Io(anyhow::anyhow!("database is down"))
}

event!(RetriedEvent, "wakuwaku-test-retried");
processor!(RetriedProcessor, RetriedEvent, "wakuwaku-test-retried", 5);

#[tokio::test]
async fn retryable_failures_are_retried_after_a_growing_delay() {
    let Some(connection) = connect().await else {
        return;
    };
    let admin = connection.open_channel(None).await.unwrap();
    let queue = "wakuwaku-test-retried";
    delete_queues(&admin, queue).await;
    let pool = AmqpPool::connect(connection.clone()).await;
    let processor = RetriedProcessor::new(2, io_error);
    let _consumers = AmqpConsumerRegisterCenter::new(processor.clone())
        .setup(&pool)
        .await
        .unwrap();

    RetriedEvent(1).send(&pool).await.unwrap();

    wait_for("the third attempt", async || {
        processor.attempts().len() == 3
    })
    .await;
    let attempts = processor.attempts();
    // 200 ms after the first failure, 400 ms after the second.
    assert!(attempts[1] - attempts[0] >= Duration::from_millis(200));
    assert!(attempts[2] - attempts[1] >= Duration::from_millis(400));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        processor.attempts().len(),
        3,
        "processed again after success"
    );
    assert_eq!(ready_messages(&admin, &retry_queue_name(queue)).await, 0);
    assert_eq!(
        ready_messages(&admin, &dead_letter_queue_name(queue)).await,
        0
    );
}

event!(ExhaustedEvent, "wakuwaku-test-exhausted");
processor!(
    ExhaustedProcessor,
    ExhaustedEvent,
    "wakuwaku-test-exhausted",
    3
);

#[tokio::test]
async fn messages_are_parked_once_their_attempts_run_out() {
    let Some(connection) = connect().await else {
        return;
    };
    let admin = connection.open_channel(None).await.unwrap();
    let queue = "wakuwaku-test-exhausted";
    delete_queues(&admin, queue).await;
    let pool = AmqpPool::connect(connection.clone()).await;
    let processor = ExhaustedProcessor::new(usize::MAX, io_error);
    let _consumers = AmqpConsumerRegisterCenter::new(processor.clone())
        .setup(&pool)
        .await
        .unwrap();

    ExhaustedEvent(7).send(&pool).await.unwrap();

    let dlq = dead_letter_queue_name(queue);
    wait_for("the parked message", async || {
        ready_messages(&admin, &dlq).await == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(processor.attempts().len(), 3);
    assert_eq!(ready_messages(&admin, queue).await, 0);
    assert_eq!(ready_messages(&admin, &retry_queue_name(queue)).await, 0);

    let (_, properties, body) = admin
        .basic_get(BasicGetArguments::new(&dlq).no_ack(true).finish())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(body, 7u32.to_be_bytes());
    assert_eq!(properties.delivery_mode(), Some(2));
    assert_eq!(
        header(&properties, FAILED_ATTEMPTS_HEADER),
        Some(&FieldValue::l(3))
    );
    assert_eq!(header_text(&properties, QUEUE_HEADER), queue);
    assert_eq!(
        header_text(&properties, DEAD_LETTER_REASON_HEADER),
        "retries-exhausted"
    );
    assert!(header_text(&properties, LAST_ERROR_HEADER).contains("database is down"));
}

event!(PermanentEvent, "wakuwaku-test-permanent");
processor!(
    PermanentProcessor,
    PermanentEvent,
    "wakuwaku-test-permanent",
    5
);

#[tokio::test]
async fn permanent_errors_are_parked_without_retrying() {
    let Some(connection) = connect().await else {
        return;
    };
    let admin = connection.open_channel(None).await.unwrap();
    let queue = "wakuwaku-test-permanent";
    delete_queues(&admin, queue).await;
    let pool = AmqpPool::connect(connection.clone()).await;
    let processor = PermanentProcessor::new(usize::MAX, || {
        Error::BusinessPanic(anyhow::anyhow!("inconsistent order"))
    });
    let _consumers = AmqpConsumerRegisterCenter::new(processor.clone())
        .setup(&pool)
        .await
        .unwrap();

    PermanentEvent(1).send(&pool).await.unwrap();
    // A body the event cannot be decoded from.
    let raw = pool.factory_create().await.unwrap();
    raw.publish(
        BasicProperties::default(),
        b"garbage".to_vec(),
        BasicPublishArguments::new("wakuwaku-test-permanent", "test"),
    )
    .await
    .unwrap();

    let dlq = dead_letter_queue_name(queue);
    wait_for("both parked messages", async || {
        ready_messages(&admin, &dlq).await == 2
    })
    .await;
    assert_eq!(processor.attempts().len(), 1);
    for _ in 0..2 {
        let (_, properties, _) = admin
            .basic_get(BasicGetArguments::new(&dlq).no_ack(true).finish())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            header_text(&properties, DEAD_LETTER_REASON_HEADER),
            "permanent-error"
        );
        assert_eq!(
            header(&properties, FAILED_ATTEMPTS_HEADER),
            Some(&FieldValue::l(1))
        );
    }
}

event!(RejectedEvent, "wakuwaku-test-rejected");
processor!(
    RejectedProcessor,
    RejectedEvent,
    "wakuwaku-test-rejected",
    5
);

#[tokio::test]
async fn rejected_messages_are_acknowledged_and_dropped() {
    let Some(connection) = connect().await else {
        return;
    };
    let admin = connection.open_channel(None).await.unwrap();
    let queue = "wakuwaku-test-rejected";
    delete_queues(&admin, queue).await;
    let pool = AmqpPool::connect(connection.clone()).await;
    let processor = RejectedProcessor::new(usize::MAX, || Error::NotFound);
    let consumers = AmqpConsumerRegisterCenter::new(processor.clone())
        .setup(&pool)
        .await
        .unwrap();

    RejectedEvent(1).send(&pool).await.unwrap();
    wait_for("the attempt", async || processor.attempts().len() == 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Stopping the consumer would requeue a message it had not acknowledged.
    drop(consumers);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(processor.attempts().len(), 1);
    assert_eq!(ready_messages(&admin, queue).await, 0);
    assert_eq!(
        ready_messages(&admin, &dead_letter_queue_name(queue)).await,
        0
    );
}

event!(PersistentEvent, "wakuwaku-test-persistent");
processor!(
    PersistentProcessor,
    PersistentEvent,
    "wakuwaku-test-persistent",
    5
);

#[tokio::test]
async fn bursts_beyond_channel_max_are_published_persistent() {
    let Some(connection) = connect().await else {
        return;
    };
    let admin = connection.open_channel(None).await.unwrap();
    let queue = "wakuwaku-test-persistent";
    delete_queues(&admin, queue).await;
    let pool = AmqpPool::connect(connection.clone()).await;
    assert!(pool.capacity() < usize::from(connection.channel_max()));
    // Declare and bind the queue without consuming from it.
    drop(
        <PersistentProcessor as AmqpMessageProcessor<PersistentEvent>>::ensure_queue(&pool)
            .await
            .unwrap(),
    );

    // More concurrent sends than the connection has channels.
    let burst = usize::from(connection.channel_max()) * 2;
    let mut sends = tokio::task::JoinSet::new();
    for i in 0..burst {
        let pool = pool.clone();
        sends.spawn(async move { PersistentEvent(i as u32).send(&pool).await });
    }
    while let Some(sent) = sends.join_next().await {
        sent.unwrap().unwrap();
    }

    assert_eq!(ready_messages(&admin, queue).await as usize, burst);
    let (_, properties, _) = admin
        .basic_get(BasicGetArguments::new(queue).no_ack(true).finish())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(properties.delivery_mode(), Some(2));
    assert!(properties.timestamp().is_some());
    delete_queues(&admin, queue).await;
}

event!(MissingExchangeEvent, "wakuwaku-test-missing-exchange");
event!(RecoveredEvent, "wakuwaku-test-recovered");

#[tokio::test]
async fn a_channel_closed_by_the_broker_is_not_reused() {
    let Some(connection) = connect().await else {
        return;
    };
    // One channel, so the second send would get the first one back if it were reused.
    let pool = AmqpPool::connect_with_capacity(connection.clone(), 1).await;
    RecoveredEvent::ensure_exchange(&pool).await.unwrap();

    // The broker closes the channel: the exchange does not exist.
    let error = MissingExchangeEvent(1).send(&pool).await.unwrap_err();
    assert!(error.to_string().contains("404"), "{error}");

    tokio::time::timeout(Duration::from_secs(5), RecoveredEvent(1).send(&pool))
        .await
        .expect("the send does not hang")
        .unwrap();
}

#[tokio::test]
async fn the_consumer_runtime_reports_a_lost_connection() {
    let Some(connection) = connect().await else {
        return;
    };
    let admin = connection.open_channel(None).await.unwrap();
    delete_queues(&admin, "wakuwaku-test-retried-lost").await;
    let pool = AmqpPool::connect(connection.clone()).await;
    let consumers = AmqpConsumerRegisterCenter::new(LostProcessor::new(0, io_error))
        .setup(&pool)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(1500), consumers.closed())
            .await
            .is_err()
    );

    connection.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), consumers.closed())
        .await
        .expect("closed() resolves once the connection is gone");
}

processor!(LostProcessor, RetriedEvent, "wakuwaku-test-retried-lost", 5);
