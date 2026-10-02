#[cfg(feature = "tracing-otel")]
use tracing::info;

#[derive(Debug, Clone)]
/// Lightweight wrapper around a PostgreSQL `sqlx::PgPool`.
pub struct DatabaseProcessor {
    executor: sqlx::PgPool,
}

impl DatabaseProcessor {
    /// Create a new database processor from a `PgPool`.
    pub fn new(executor: sqlx::PgPool) -> Self {
        Self { executor }
    }
    /// Borrow the wrapped `PgPool` executor.
    pub fn db(&self) -> &sqlx::PgPool {
        #[cfg(feature = "tracing-otel")]
        info!(monotonic_counter.sql = 1);
        &self.executor
    }
    /// Alias constructor for building from a `PgPool`.
    pub fn from_pool(pool: sqlx::PgPool) -> Self {
        Self::new(pool)
    }
}
