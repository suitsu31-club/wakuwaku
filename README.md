# wakuwaku

`wakuwaku` is a proof-driven microservice framework for Rust.

It wires a service together: every component (gRPC server, event handler, cron job) is built from a type-level registry of shared dependencies, and the crates that do the actual work are re-exported behind feature flags, so a service depends on `wakuwaku` alone.

## Proof-driven programming

- **Macro correctness** (how components fit together) is proven by traits and checked by the compiler. A component whose dependency was never registered, a write query run through a read-only handle, or a cron job with a zero interval does not build.
- **Micro correctness** (what one component does) is proven by unit tests. Every component is a plain value implementing a `kanau::processor::Processor`-based trait, so a test calls it directly, without starting the service.

## Crates

| Crate             | Re-exported as      | Provides                                                                 |
|-------------------|---------------------|--------------------------------------------------------------------------|
| `wakuwaku`        |                     | `ServiceBuilder`, gRPC registration, cron jobs, Iggy consumer registration |
| `wakuwaku-entity` | `wakuwaku::entity`  | Capability-typed queries and transactions over sqlx, Redis, Meilisearch  |
| `wakuwaku-iggy`   | `wakuwaku::iggy`    | Typed events on Apache Iggy with per-key ordering and algebraic batching |
| `wakuwaku-proof`  | `wakuwaku::proof`   | Proof-carrying validation                                                |

Third-party crates that appear in these APIs (`kanau`, `tonic`, `iggy`, `sqlx`, `redis`, `meilisearch_sdk`) are re-exported under `wakuwaku::deps`.

## Features

| Feature       | Default | Enables                                              |
|---------------|---------|------------------------------------------------------|
| `grpc`        | yes     | gRPC server registration, `deps::tonic`              |
| `proof`       | yes     | `wakuwaku::proof`                                    |
| `iggy`        | no      | `wakuwaku::iggy`, Iggy consumer registration, `deps::iggy` |
| `entity`      | no      | `wakuwaku::entity` without a data source             |
| `sqlx-pg`     | no      | `entity` with PostgreSQL, `deps::sqlx`               |
| `sqlx-mysql`  | no      | `entity` with MySQL, `deps::sqlx`                    |
| `redis`       | no      | `entity` with Redis, `deps::redis`                   |
| `meilisearch` | no      | `entity` with Meilisearch, `deps::meilisearch_sdk`   |

```toml
[dependencies]
wakuwaku = { version = "1.0.0-rc1", features = ["iggy", "sqlx-pg", "redis"] }
```

## Example

```rust
use std::time::Duration;
use wakuwaku::deps::kanau::processor::Processor;
use wakuwaku::{CronJob, CronTick, ServiceBuilder, ServiceCreation};

#[derive(Clone)]
struct SessionStore;

struct ExpireSessions { store: SessionStore }

impl ServiceCreation for ExpireSessions {
    type Dep = SessionStore;
    fn create_service(store: SessionStore) -> Self { Self { store } }
}

impl Processor<CronTick> for ExpireSessions {
    type Output = ();
    type Error = std::convert::Infallible;
    async fn process(&self, _tick: CronTick) -> Result<(), Self::Error> {
        // Scan for expired sessions and delete them.
        Ok(())
    }
}

impl CronJob for ExpireSessions {
    const INTERVAL: Duration = Duration::from_secs(60);
}

#[tokio::main]
async fn main() {
    let (services, cron) = ServiceBuilder::new(SessionStore)
        .cron_job::<ExpireSessions, _>()
        .start();

    // Build gRPC servers and Iggy handlers from `services` the same way:
    // `services.grpc_service::<XxxServer<_>, Impl, _>(XxxServer::new)`,
    // `services.iggy_consumer::<Handler, _, _>()`.
    // Components built from several services, or by hand, need no
    // `ServiceCreation` impl: `.compose_cron_job(|s| ...)`,
    // `.compose_grpc_service(|s| XxxServer::new(...))`,
    // `.compose_iggy_consumer(|s| Arc::new(...))`.
    let _ = services;

    tokio::signal::ctrl_c().await.ok();
    cron.shutdown().await;
}
```

Cron jobs run locally, each on a fixed grid of `INTERVAL`. Runs of one job never overlap, and a run that overruns skips the ticks it covered. Every replica runs its own jobs, so make jobs idempotent or partition their work.

## Migrating from 0.3

- RabbitMQ is gone: `integration::amqp`, `services::amqp_consumer` and `ServiceBuilder::amqp_consumer` are replaced by `wakuwaku::iggy` and `ServiceBuilder::iggy_consumer`.
- `interval_job::IntervalJobExecutionSignal` is replaced by `CronJob`, which runs in the process instead of signalling a consumer.
- `integration::redis`, `integration::sqlx` and `pool` are replaced by `wakuwaku::entity`. `integration::surreal` is removed.
- `wakuwaku::Error` is removed; each crate has its own error type.
- The `tonic` feature is renamed to `grpc`.

## License

MIT
