use crossbeam_queue::ArrayQueue;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

type ConnectionFactoryFut<T, E = anyhow::Error> =
    Pin<Box<dyn Future<Output = Result<T, E>> + Send>>;
type ConnectionFactory<T, E> = Pin<Box<dyn Fn() -> ConnectionFactoryFut<T, E> + Send + Sync>>;
type HealthCheck<T> = Box<dyn Fn(&T) -> bool + Send + Sync>;

struct PoolInner<T, FactoryError = anyhow::Error> {
    idle: ArrayQueue<T>,
    sem: Arc<Semaphore>,
    factory: ConnectionFactory<T, FactoryError>,
    /// Decides whether an idle resource may be handed out again. Without it every resource is
    /// reused.
    health_check: Option<HealthCheck<T>>,
    capacity: usize,
}

impl<T, FE> PoolInner<T, FE> {
    fn is_healthy(&self, resource: &T) -> bool {
        self.health_check
            .as_ref()
            .is_none_or(|check| check(resource))
    }
}

/// Bounded async pool of reusable resources created by a factory function.
///
/// At most `capacity` resources are checked out at the same time. [`get`](Self::get) waits for
/// one to come back instead of creating more.
pub struct Pool<T, FactoryError = anyhow::Error> {
    inner: Arc<PoolInner<T, FactoryError>>,
}

impl<T, FE> Clone for Pool<T, FE> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

/// Checked-out pooled resource with automatic return-on-drop semantics.
///
/// On drop the resource goes back to the pool, unless the pool's health check rejects it; then
/// it is dropped and its slot is freed for a new one.
pub struct Pooled<T, FactoryError = anyhow::Error> {
    inner: Arc<PoolInner<T, FactoryError>>,
    permit: Option<OwnedSemaphorePermit>,
    conn: Option<T>,
}

impl<T, FE> Pooled<T, FE> {
    /// Borrow the underlying pooled resource if it is still connected.
    pub fn get_ref(&self) -> Option<&T> {
        self.conn.as_ref()
    }
    /// Mutably borrow the underlying pooled resource if it is still connected.
    pub fn get_mut(&mut self) -> Option<&mut T> {
        self.conn.as_mut()
    }

    /// Mark the connection is disconnected.
    ///
    /// It will drop the connection. The connection capacity will also be released.
    pub fn disconnect(&mut self) {
        self.conn.take();
        self.permit.take();
    }
}

impl<T, FE> Drop for Pooled<T, FE> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take()
            && self.inner.is_healthy(&conn)
        {
            let _ = self.inner.idle.push(conn);
        }
        let _ = self.permit.take();
    }
}

impl<T, FE> Pool<T, FE> {
    /// Create a new pool with a resource factory and maximum capacity.
    ///
    /// A capacity of 0 is raised to 1.
    pub fn new<F>(factory: Pin<Box<F>>, capacity: usize) -> Self
    where
        F: Fn() -> ConnectionFactoryFut<T, FE> + Send + Sync + 'static,
    {
        Self::build(factory, capacity, None)
    }

    /// Create a pool that only reuses resources `health_check` accepts.
    ///
    /// The check runs when an idle resource is about to be handed out and when a checked-out
    /// one is returned. A rejected resource is dropped, and [`get`](Self::get) creates a new
    /// one in its place.
    pub fn with_health_check<F, H>(factory: Pin<Box<F>>, capacity: usize, health_check: H) -> Self
    where
        F: Fn() -> ConnectionFactoryFut<T, FE> + Send + Sync + 'static,
        H: Fn(&T) -> bool + Send + Sync + 'static,
    {
        Self::build(factory, capacity, Some(Box::new(health_check)))
    }

    fn build<F>(factory: Pin<Box<F>>, capacity: usize, health_check: Option<HealthCheck<T>>) -> Self
    where
        F: Fn() -> ConnectionFactoryFut<T, FE> + Send + Sync + 'static,
    {
        let capacity = capacity.max(1);
        Self {
            inner: Arc::new(PoolInner {
                idle: ArrayQueue::new(capacity),
                sem: Arc::new(Semaphore::new(capacity)),
                factory,
                health_check,
                capacity,
            }),
        }
    }

    /// The most resources that can be checked out at the same time.
    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }
    /// Return the number of currently idle resources in the pool.
    pub fn idle_len(&self) -> usize {
        self.inner.idle.len()
    }
    /// Create a new resource directly via the pool factory.
    ///
    /// The resource is not counted against the pool's capacity and never goes back into it.
    pub async fn factory_create(&self) -> Result<T, FE> {
        (self.inner.factory)().await
    }
    /// Acquire a resource from the pool, creating one if needed.
    ///
    /// Waits while `capacity` resources are checked out.
    pub async fn get(&self) -> PoolingResult<T, FE> {
        let sem = self.inner.sem.clone();
        let Ok(permit) = sem.acquire_owned().await else {
            return PoolingResult::SemanticsError;
        };

        while let Some(conn) = self.inner.idle.pop() {
            if self.inner.is_healthy(&conn) {
                return PoolingResult::Ok(Pooled {
                    inner: self.inner.clone(),
                    permit: Some(permit),
                    conn: Some(conn),
                });
            }
            // A broken resource: drop it and look at the next one.
        }

        match self.factory_create().await {
            Ok(conn) => PoolingResult::Ok(Pooled {
                inner: self.inner.clone(),
                permit: Some(permit),
                conn: Some(conn),
            }),
            Err(err) => PoolingResult::FactoryErr(err),
        }
    }
}

/// Result type for pool acquisition operations.
pub enum PoolingResult<T, FE> {
    /// Successfully acquired a pooled resource.
    Ok(Pooled<T, FE>),
    /// Semaphore acquisition failed unexpectedly.
    SemanticsError,
    /// Resource creation via the factory failed.
    FactoryErr(FE),
}

impl<T, FE: Into<crate::Error>> From<PoolingResult<T, FE>> for Result<Pooled<T, FE>, crate::Error> {
    fn from(result: PoolingResult<T, FE>) -> Self {
        match result {
            PoolingResult::Ok(succ) => Ok(succ),
            PoolingResult::FactoryErr(err) => Err(err.into()),
            PoolingResult::SemanticsError => Err(crate::Error::BusinessPanic(anyhow::anyhow!(
                "Semaphore error"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    struct Resource {
        healthy: Arc<AtomicBool>,
    }

    /// A pool whose factory counts how many resources it created.
    fn counting_pool(capacity: usize) -> (Pool<Resource>, Arc<AtomicUsize>) {
        let created = Arc::new(AtomicUsize::new(0));
        let counter = created.clone();
        let factory = move || {
            counter.fetch_add(1, Ordering::SeqCst);
            Box::pin(async {
                Ok(Resource {
                    healthy: Arc::new(AtomicBool::new(true)),
                })
            }) as ConnectionFactoryFut<Resource>
        };
        let pool = Pool::with_health_check(Box::pin(factory), capacity, |r: &Resource| {
            r.healthy.load(Ordering::SeqCst)
        });
        (pool, created)
    }

    fn checkout(result: PoolingResult<Resource, anyhow::Error>) -> Pooled<Resource> {
        match result {
            PoolingResult::Ok(pooled) => pooled,
            _ => panic!("checkout failed"),
        }
    }

    #[tokio::test]
    async fn get_waits_for_a_free_slot_instead_of_creating_more() {
        let (pool, created) = counting_pool(2);
        let first = checkout(pool.get().await);
        let _second = checkout(pool.get().await);

        let waiting = tokio::spawn({
            let pool = pool.clone();
            async move { checkout(pool.get().await) }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiting.is_finished());

        drop(first);
        let _third = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("a returned resource frees a slot")
            .unwrap();
        assert_eq!(created.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn broken_resources_are_replaced_instead_of_reused() {
        let (pool, created) = counting_pool(1);

        // Breaks while checked out: not put back.
        let pooled = checkout(pool.get().await);
        pooled
            .get_ref()
            .unwrap()
            .healthy
            .store(false, Ordering::SeqCst);
        drop(pooled);
        assert_eq!(pool.idle_len(), 0);

        // Breaks while idle: skipped on the next checkout.
        let pooled = checkout(pool.get().await);
        let healthy = pooled.get_ref().unwrap().healthy.clone();
        drop(pooled);
        assert_eq!(pool.idle_len(), 1);
        healthy.store(false, Ordering::SeqCst);
        let pooled = checkout(pool.get().await);
        assert!(pooled.get_ref().unwrap().healthy.load(Ordering::SeqCst));

        assert_eq!(created.load(Ordering::SeqCst), 3);
    }
}
