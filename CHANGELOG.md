# Changelog

## 0.3.1

### Fixed

- A consumer no longer puts a failed message straight back into its queue, over and over. Before, a `DatabaseError`, `RedisError`, `SurrealDbError` or `Io` from the processor made the consumer `basic.nack` the message with `requeue = true`, so the broker delivered it again right away, forever, as fast as the consumer could fail. Now:
  - retryable errors are retried after a growing delay, following the processor's `RetryPolicy` (default: 10 attempts, 1 s, 4 s, 16 s, 64 s, 256 s, then 10 min apart). The consumer publishes a copy of the message to `<queue>.retry` with the delay as its TTL and acknowledges the original once the broker confirmed the copy; when the TTL runs out, the broker dead-letters the copy back into `<queue>`.
  - once the attempts run out, and right away for errors that retrying cannot fix, the message is parked in `<queue>.dlq` and an `ERROR` is logged. The copy carries `wakuwaku-failed-attempts`, `wakuwaku-last-error`, `wakuwaku-queue`, `wakuwaku-dead-letter-reason` and `wakuwaku-dead-lettered-at` headers.
  - the original stays unacknowledged until the broker confirmed the copy, however long that takes (for example during a memory alarm), so the message never exists twice. If the broker refuses the copy, the original is requeued after 5 seconds, never immediately; if the channel closes first, the broker requeues it. A retry or dead-letter queue deleted while the consumer runs is declared again.
  - a policy can drop messages whose attempts ran out instead of parking them (`RetryPolicy::discard_when_exhausted`), for messages that are worthless once they are late.
- An `AmqpError` from the processor (for example a failed publish inside it) left the delivery neither acknowledged nor rejected, until the broker's consumer timeout closed the channel and the consumer with it. It is now retried like the other transient errors.
- `SerializeError`, `DeserializeError` and `BusinessPanic` park the message in `<queue>.dlq` instead of dropping it (the (de)serialization errors were dropped without a log line).
- `AmqpMessageSend::send` published non-persistent messages and did not wait for the publisher confirm it had turned on, so a broker restart or an internal broker error lost messages without an error. Messages are now persistent (delivery mode 2, see `AmqpMessageSend::PERSISTENT`) and `send` waits for the broker's confirm, at most 30 s (`CONFIRM_TIMEOUT`) in all. It returns an error when the message did not reach a queue: not handed to the connection in time, nacked, or the channel closed first (for example publishing to an exchange that does not exist). A message that was handed to the connection but got no confirm in time (the broker stops reading from publishing connections during a memory or disk alarm) is logged as a warning and `send` succeeds: the broker takes it once the alarm clears, and treating it as failed would make callers publish it twice.
- A channel number is released only when the client closes the channel, so every channel the broker closed used one up for good, and after about 2000 amqprs panicked and could not open channels any more on a connection that still reported itself open. `AmqpPool` now closes the connection before its channel numbers run out, so the application sees a lost connection. Channels are also opened in a task of their own, so that a caller that stops waiting does not leak a channel number.
- A publish cancelled before its frame reached the connection left the confirm tracking one step off, so every later publish on that channel waited for the wrong confirm. The sequence number is now taken back.
- The broker cancelling a consumer (its queue was deleted) left the channel open with no consumer on it, and `AmqpConsumersRuntime::closed` never resolved. The channel is now closed, so `closed` resolves.
- `AmqpPool` allowed 4096 channels on one connection, twice RabbitMQ's default `channel_max` of 2047, so a burst of concurrent sends failed to open channels. The pool is now capped at 512 and below the negotiated `channel_max` minus 128 channels left for consumers; further sends wait for a free channel.
- A pooled channel that the broker had closed went back into the pool and was handed out again. `Pool` can now check resources (`Pool::with_health_check`) and `AmqpPool` drops channels that are closed.
- Consumers limit the prefetch to `AmqpMessageProcessor::PREFETCH` (default 32). Without a limit the broker pushed the whole backlog into the consumer, and a message that waited there longer than the broker's `consumer_timeout` made the broker close the channel.

### Added

- `RetryPolicy`, `FailureAction` and, on `AmqpMessageProcessor`, `RETRY_POLICY`, `PREFETCH` and `failure_action` to change how a consumer handles failures. `is_permanent_database_error` (feature `sqlx`) is the default split of database errors: SQLSTATE classes 22 and 23 and decode errors are permanent, everything else is retried.
- `ConfirmChannel` and `PublishOutcome`: a channel in publisher-confirm mode whose `publish` waits for the broker's confirm. `PublishOutcome::Unconfirmed` is a message handed to the connection without a confirm in time.
- `AmqpMessageSend::publish`, which is `send` returning the `PublishOutcome`, and `AmqpMessageSend::REQUIRE_ROUTE`, which makes a message that no queue takes an error instead of being dropped.
- `RetryPolicy::park_when_exhausted` and `RetryPolicy::discard_when_exhausted`.
- `AmqpPool::connect_with_capacity`, `Pool::capacity`, `Pool::with_health_check`.
- `declare_failure_queues`, `retry_queue_name`, `dead_letter_queue_name` and the header name constants.
- `AmqpConsumersRuntime::closed`, which resolves once a consumer channel has closed (lost connection, channel closed by the broker), so a service can exit and be restarted instead of running without consumers.

### Changed

- `AmqpPool` is now `Pool<ConfirmChannel, amqprs::error::Error>`. `ConfirmChannel` dereferences to `amqprs::channel::Channel`, so calling channel methods on a pooled channel still compiles; code that names the type `Pooled<Channel, _>` has to say `Pooled<ConfirmChannel, _>`. Publish on pooled channels only through `ConfirmChannel::publish` or `AmqpMessageSend::send`: a bare `basic_publish` throws off the confirm numbering and the channel is dropped.
- `setup_consumer` also declares `<queue>.retry` and `<queue>.dlq`, sets the prefetch, and puts the consumer's channel into confirm mode, replacing the channel's callback.

### Upgrading a running broker

- The consumer queues themselves are declared exactly as before (durable, no arguments), so existing queues are reused as they are. `<queue>.retry` (with `x-dead-letter-exchange = ""` and `x-dead-letter-routing-key = <queue>`) and `<queue>.dlq` are new names, so declaring them cannot fail with `PRECONDITION_FAILED`. Nothing has to be deleted or changed on the broker, and no policy is needed.
- Messages published by 0.3.0 and still queued are non-persistent; let the queues drain before restarting the broker.
- Going back to 0.3.0 is safe: messages still waiting in `<queue>.retry` are dead-lettered back into `<queue>` by the broker and processed by the old consumers. Messages in `<queue>.dlq` stay there.
