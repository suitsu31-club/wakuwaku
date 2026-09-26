//! Feature-gated integrations with external messaging and storage backends.
//!
//! Each submodule is compiled only when its Cargo feature is enabled:
//!
//! | Module     | Feature   | Default | Provides                                                  |
//! |------------|-----------|---------|-----------------------------------------------------------|
//! | `amqp`     | `amqprs`  | yes     | RabbitMQ channel pool, message routing/sending, consumers |
//! | `redis`    | `redis`   | yes     | Typed key-value read/write traits over Redis              |
//! | `sqlx`     | `sqlx`    | no      | `DatabaseProcessor` wrapper around a PostgreSQL `PgPool`  |
//! | `surreal`  | `surreal` | no      | `SurrealProcessor` wrapper around a SurrealDB client      |
//!
//! With the `tracing-otel` feature, `amqp`, `sqlx`, and `surreal` also emit
//! `monotonic_counter.*` events through `tracing` for OpenTelemetry metrics
//! (messages published/received, database handle accesses).

#[cfg(feature = "amqprs")]
/// RabbitMQ publishing/consuming abstractions.
pub mod amqp;
#[cfg(feature = "redis")]
/// Redis key-value helper traits and types.
pub mod redis;
#[cfg(feature = "sqlx")]
/// SQLx PostgreSQL wrapper utilities.
pub mod sqlx;
#[cfg(feature = "surreal")]
/// SurrealDB wrapper utilities.
pub mod surreal;
