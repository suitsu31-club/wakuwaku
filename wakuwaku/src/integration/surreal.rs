#[cfg(feature = "tracing-otel")]
use tracing::info;

#[derive(Debug, Clone)]
/// A wrapper around a SurrealDB executor.
pub struct SurrealProcessor {
    executor: surrealdb::Surreal<surrealdb::engine::any::Any>,
}

impl SurrealProcessor {
    /// Creates a new `SurrealProcessor` instance with the provided SurrealDB executor.
    pub fn new(executor: surrealdb::Surreal<surrealdb::engine::any::Any>) -> Self {
        SurrealProcessor { executor }
    }

    /// Borrow the wrapped SurrealDB executor.
    pub fn db(&self) -> &surrealdb::Surreal<surrealdb::engine::any::Any> {
        #[cfg(feature = "tracing-otel")]
        info!(monotonic_counter.sql = 1);
        &self.executor
    }
}
