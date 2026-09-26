# wakuwaku

`wakuwaku` is a small async utility crate for backend services that use:

- RabbitMQ (`amqprs`)
- PostgreSQL (`sqlx`)
- Redis (`redis`)
- shared connection pooling helpers

It provides reusable building blocks for message publishing/consuming, Redis key-value helpers, SQLx pool wrapping, and common error handling.

## Features

Default features:

- `amqprs`
- `sqlx`
- `redis`
- `uuid`
- `tracing`

Optional feature:

- `tracing-otel` (enables OpenTelemetry-style metric hooks through `tracing`)

## Installation

```toml
[dependencies]
wakuwaku = "0.1"
```

Disable default features if you only need specific modules:

```toml
[dependencies]
wakuwaku = { version = "0.1", default-features = false, features = ["redis"] }
```

## Module overview

- `wakuwaku::integration::amqp`  
  Traits and utilities for exchange/queue setup, message sending, and consumer wiring. Messages are published persistent and `send` waits for the broker's publisher confirm; the channel pool stays below the connection's `channel_max`. A consumer retries a failed message later with a growing delay (through a `<queue>.retry` queue) and parks it in `<queue>.dlq` once its attempts run out or the error cannot be fixed by retrying; see `FailureAction` and `RetryPolicy`.
- `wakuwaku::redis`  
  `KeyValue`/`KeyValueRead`/`KeyValueWrite` traits for typed Redis get/set/delete with binary payloads.
- `wakuwaku::sqlx`  
  `DatabaseProcessor` wrapper around `sqlx::PgPool`.
- `wakuwaku::pool`  
  Generic bounded async resource pool.
- `wakuwaku::services`  
  `ServiceBuilder` (compile-time, type-indexed service registry), `ServiceCreation` (build a service from a registered dependency), and, with `amqprs`, `AmqpConsumerRegisterCenter` for starting a group of consumers together. `ServiceBuilder::amqp_consumer::<Consumer, _, _>()` builds each consumer from its registered dependency and ends with `.setup(&pool)`. With `tonic`, `GrpcServiceRegisterCenter` collects gRPC servers; `ServiceBuilder::grpc_service::<XxxServer<_>, ServiceImpl, _>(XxxServer::new)` builds each server from its implementation's registered dependency and ends with `.into_router_fn()`, an `impl FnOnce(Router<L>) -> Router<L>` that adds every server to a tonic router.
- `wakuwaku::error`  
  Unified error type used across features.

## Example

```rust
use wakuwaku::redis::{KeyValue, KeyValueRead, KeyValueWrite, RedisConnection, RedisKey};

#[derive(Clone)]
struct Session {
    key: RedisKey,
    value: Vec<u8>,
}

impl KeyValue for Session {
    type Key = RedisKey;
    type Value = Vec<u8>;

    fn key(&self) -> Self::Key { self.key.clone() }
    fn value(&self) -> Self::Value { self.value.clone() }
    fn into_value(self) -> Self::Value { self.value }
    fn new(key: Self::Key, value: Self::Value) -> Self { Self { key, value } }
}
```

## License

MIT
