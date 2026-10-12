//! Jobs that run locally at a fixed interval.
//!
//! A [`CronJob`] is a [`Processor`] of [`CronTick`]s with a fixed
//! [`INTERVAL`](CronJob::INTERVAL). Cron jobs are usually scans (expire stale
//! rows, retry stuck work, reconcile an index), so the interval is a
//! constant of the job type rather than a calendar expression.
//!
//! [`CronRegisterCenter`] collects jobs in a type-level linked list, the same
//! way [`ServiceBuilder`] does; there is no boxing and no dynamic dispatch.
//! [`start`](CronRegisterCenter::start) spawns one Tokio task per job and
//! returns a [`CronRuntime`].
//!
//! # Schedule
//!
//! Each job runs on its own grid `start + k * INTERVAL`, where `start` is
//! when [`start`](CronRegisterCenter::start) was called:
//!
//! - The first run is at `start`.
//! - Runs of one job never overlap. A run that takes longer than `INTERVAL`
//!   skips the grid points it covered; the next run is at the first grid
//!   point that is not before the run ended. Two consecutive runs therefore
//!   start a whole, non-zero number of intervals apart.
//! - A run that returns an error, or panics, is logged with [`tracing`] and
//!   the job keeps its schedule.
//!
//! An `INTERVAL` of zero has no schedule. Registering such a job fails to
//! compile; the check runs when the registration is instantiated, so
//! `cargo build` reports it and `cargo check` alone does not:
//!
//! ```compile_fail
//! use std::time::Duration;
//! use wakuwaku::deps::kanau::processor::Processor;
//! use wakuwaku::services::cron_job::{CronJob, CronRegisterCenter, CronTick};
//!
//! struct Busy;
//! impl Processor<CronTick> for Busy {
//!     type Output = ();
//!     type Error = std::convert::Infallible;
//!     async fn process(&self, _: CronTick) -> Result<(), Self::Error> { Ok(()) }
//! }
//! impl CronJob for Busy {
//!     const INTERVAL: Duration = Duration::ZERO;
//! }
//!
//! let _ = CronRegisterCenter::new(Busy);
//! ```
//!
//! # Several replicas
//!
//! Every process that starts a job runs it, so with several replicas of a
//! service each run happens once per replica. Make the job idempotent, or
//! partition the work, for example by having each replica scan only its
//! share of the keys, or by taking a lock such as
//! `wakuwaku_entity::redis::collection::lock::Lock` inside the run.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicU64, Ordering};
//! use std::time::Duration;
//! use wakuwaku::deps::kanau::processor::Processor;
//! use wakuwaku::services::ServiceCreation;
//! use wakuwaku::services::builder::ServiceBuilder;
//! use wakuwaku::services::cron_job::{CronJob, CronTick};
//!
//! /// Shared dependency, cheap to clone.
//! #[derive(Clone, Default)]
//! struct SessionStore { expired: Arc<AtomicU64> }
//!
//! struct ExpireSessions { store: SessionStore }
//!
//! impl ServiceCreation for ExpireSessions {
//!     type Dep = SessionStore;
//!     fn create_service(store: SessionStore) -> Self { Self { store } }
//! }
//!
//! impl Processor<CronTick> for ExpireSessions {
//!     type Output = ();
//!     type Error = std::convert::Infallible;
//!     async fn process(&self, _tick: CronTick) -> Result<(), Self::Error> {
//!         // Scan for expired sessions and delete them.
//!         self.store.expired.fetch_add(1, Ordering::Relaxed);
//!         Ok(())
//!     }
//! }
//!
//! impl CronJob for ExpireSessions {
//!     const INTERVAL: Duration = Duration::from_secs(60);
//! }
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let store = SessionStore::default();
//! let (services, cron) = ServiceBuilder::new(store.clone())
//!     .cron_job::<ExpireSessions, _>()
//!     .start();
//!
//! // The service builder is still available to the rest of the app.
//! let _store: &SessionStore = services.provide::<SessionStore, _>();
//!
//! # tokio::time::sleep(Duration::from_millis(10)).await;
//! // Lets the run in progress finish, then stops every job.
//! cron.shutdown().await;
//! # assert_eq!(store.expired.load(Ordering::Relaxed), 1);
//! # }
//! ```
//!
//! A job that needs several registered services, or that is built by hand,
//! is registered with
//! [`compose_cron_job`](ServiceBuilder::compose_cron_job) instead; it needs
//! no [`ServiceCreation`] impl.
//!
//! A job is unit-tested by calling [`process`](Processor::process) with a
//! [`CronTick`] built by hand; no runtime is needed.

use crate::services::ServiceCreation;
use crate::services::builder::{ProvideAt, ServiceBuilder, ServiceBuilderPosition};
use futures_util::FutureExt;
use kanau::processor::Processor;
use std::any::{Any, type_name};
use std::fmt::Display;
use std::panic::AssertUnwindSafe;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tracing::Instrument;

/// The input of one run of a [`CronJob`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CronTick {
    /// Number of earlier runs of this job since its runtime started: `0` for
    /// the first run.
    pub run: u64,
    /// Wall-clock time when the run started.
    pub fired_at: SystemTime,
}

/// A job that runs locally every [`INTERVAL`](Self::INTERVAL).
///
/// The job itself is a [`Processor`] of [`CronTick`]s that outputs `()`.
/// Returned errors are logged with [`tracing`] and don't stop the job. See
/// the [module documentation](self) for the schedule.
pub trait CronJob:
    Processor<CronTick, Output = (), Error: Display> + Send + Sync + 'static
{
    /// Time between two scheduled runs. Must not be zero; registering a job
    /// with a zero interval fails to build.
    const INTERVAL: Duration;
}

/// Rejects a zero [`CronJob::INTERVAL`] at compile time.
const fn assert_interval<Job: CronJob>() {
    assert!(
        !Job::INTERVAL.is_zero(),
        "CronJob::INTERVAL must not be zero"
    );
}

/// Registers cron jobs built from a [`ServiceBuilder`].
///
/// Pairs a service builder (`SHead`, `SChain`) with a [`CronRegisterCenter`]
/// (`Job`, `CChain`) that is being filled from it. Get one from
/// [`ServiceBuilder::cron_job`] or [`ServiceBuilder::compose_cron_job`],
/// register more jobs with [`cron_job`](Self::cron_job) or
/// [`compose_cron_job`](Self::compose_cron_job), then finish with
/// [`start`](Self::start) or [`into_parts`](Self::into_parts). See the
/// [module documentation](self) for an example.
pub struct CronRegisterCenterBuilder<SHead, SChain, Job, CChain> {
    /// The service builder that job dependencies are taken from.
    pub builder: ServiceBuilder<SHead, SChain>,
    register_center: CronRegisterCenter<Job, CChain>,
}

impl<SH, SC, J, CC> CronRegisterCenterBuilder<SH, SC, J, CC> {
    /// Pair an existing service builder with an existing job list.
    ///
    /// Usually you don't need this: [`ServiceBuilder::cron_job`] creates the
    /// builder for you.
    pub fn new(
        builder: ServiceBuilder<SH, SC>,
        register_center: CronRegisterCenter<J, CC>,
    ) -> Self {
        Self {
            builder,
            register_center,
        }
    }

    /// Create `NewJob` from its dependency and register it.
    ///
    /// The dependency, [`NewJob::Dep`](ServiceCreation::Dep), is looked up
    /// in [`builder`](Self::builder) at `Position` and cloned. The job is
    /// created with [`create_service`](ServiceCreation::create_service) and
    /// pushed onto the list the same way as [`CronRegisterCenter::push`].
    ///
    /// `Position` can usually be passed as `_`; it is inferred when exactly
    /// one service of type `NewJob::Dep` is registered. A dependency that was
    /// never registered is a compile error.
    pub fn cron_job<NewJob, Position>(
        self,
    ) -> CronRegisterCenterBuilder<SH, SC, NewJob, CronRegisterCenter<J, CC>>
    where
        NewJob: CronJob + ServiceCreation,
        Position: ServiceBuilderPosition,
        ServiceBuilder<SH, SC>: ProvideAt<NewJob::Dep, Position>,
    {
        let dep = self.builder.provide::<NewJob::Dep, Position>().clone();
        CronRegisterCenterBuilder {
            builder: self.builder,
            register_center: self.register_center.push(NewJob::create_service(dep)),
        }
    }

    /// Build `NewJob` from any registered services and register it.
    ///
    /// `composer` borrows [`builder`](Self::builder), so it can
    /// [`provide`](ServiceBuilder::provide) as many registered services as it
    /// needs, or ignore it and move in a job that was built by hand. The job
    /// needs no [`ServiceCreation`] impl and is pushed onto the list the same
    /// way as [`CronRegisterCenter::push`]. See
    /// [`ServiceBuilder::compose_cron_job`] for an example.
    pub fn compose_cron_job<NewJob: CronJob>(
        self,
        composer: impl FnOnce(&ServiceBuilder<SH, SC>) -> NewJob,
    ) -> CronRegisterCenterBuilder<SH, SC, NewJob, CronRegisterCenter<J, CC>> {
        let job = composer(&self.builder);
        CronRegisterCenterBuilder {
            builder: self.builder,
            register_center: self.register_center.push(job),
        }
    }

    /// Split into the service builder and the unstarted job list.
    ///
    /// Use this to start the jobs later with [`CronRegisterCenter::start`],
    /// or to keep adding jobs that are built by hand with
    /// [`CronRegisterCenter::push`].
    pub fn into_parts(self) -> (ServiceBuilder<SH, SC>, CronRegisterCenter<J, CC>) {
        (self.builder, self.register_center)
    }

    /// Start every registered job and return the service builder together
    /// with the running jobs.
    ///
    /// This is [`into_parts`](Self::into_parts) followed by
    /// [`CronRegisterCenter::start`].
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    pub fn start(self) -> (ServiceBuilder<SH, SC>, CronRuntime)
    where
        CronRegisterCenter<J, CC>: SpawnCronJobs,
    {
        (self.builder, self.register_center.start())
    }
}

/// A node in the type-level list of registered cron jobs.
///
/// `Job` is the job stored at this node and `Chain` is the rest of the list:
/// either another `CronRegisterCenter` or `()` for the first registered job.
/// Build one with [`new`](Self::new), extend it with [`push`](Self::push),
/// and run every job with [`start`](Self::start).
pub struct CronRegisterCenter<Job, Chain = ()> {
    head: Job,
    chain: Chain,
}

impl<Job: CronJob> CronRegisterCenter<Job> {
    /// Start a new list with `head` as its first job.
    pub fn new(head: Job) -> Self {
        const { assert_interval::<Job>() };
        Self { head, chain: () }
    }
}

impl<Job, Chain> CronRegisterCenter<Job, Chain> {
    /// Register another job, returning the extended list.
    pub fn push<Next: CronJob>(self, next: Next) -> CronRegisterCenter<Next, Self> {
        const { assert_interval::<Next>() };
        CronRegisterCenter {
            head: next,
            chain: self,
        }
    }

    /// Spawn one task per registered job, each running its job on its
    /// schedule.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    pub fn start(self) -> CronRuntime
    where
        Self: SpawnCronJobs,
    {
        let (shutdown, stop) = watch::channel(false);
        let mut tasks = JoinSet::new();
        self.spawn_jobs(&mut tasks, &stop);
        CronRuntime { tasks, shutdown }
    }
}

/// Spawns each job of a [`CronRegisterCenter`] chain.
///
/// Implemented for every chain of [`CronJob`]s. It is public only because it
/// appears in the bounds of [`CronRegisterCenter::start`]. Call `start`
/// instead of using it directly.
pub trait SpawnCronJobs {
    /// Spawn the task of every job in the chain onto `tasks`. Each task
    /// stops before its next run once `stop` holds `true`.
    fn spawn_jobs(self, tasks: &mut JoinSet<()>, stop: &watch::Receiver<bool>);
}

impl SpawnCronJobs for () {
    fn spawn_jobs(self, _: &mut JoinSet<()>, _: &watch::Receiver<bool>) {}
}

impl<Job: CronJob, Chain: SpawnCronJobs> SpawnCronJobs for CronRegisterCenter<Job, Chain> {
    fn spawn_jobs(self, tasks: &mut JoinSet<()>, stop: &watch::Receiver<bool>) {
        self.chain.spawn_jobs(tasks, stop);
        tasks.spawn(run_job(self.head, stop.clone()));
    }
}

/// Cron jobs started by [`CronRegisterCenter::start`].
///
/// Call [`shutdown`](Self::shutdown) to stop the jobs gracefully. Dropping
/// this value aborts every job, including runs in progress.
pub struct CronRuntime {
    tasks: JoinSet<()>,
    shutdown: watch::Sender<bool>,
}

impl CronRuntime {
    /// Stop scheduling runs, and wait for the runs in progress to finish.
    pub async fn shutdown(mut self) {
        self.shutdown.send_replace(true);
        while self.tasks.join_next().await.is_some() {}
    }
}

/// Runs `job` on its schedule until `stop` holds `true` or its sender is
/// dropped.
async fn run_job<Job: CronJob>(job: Job, mut stop: watch::Receiver<bool>) {
    let mut scheduled = Instant::now();
    let mut run = 0u64;
    loop {
        tokio::select! {
            biased;
            _ = stop.wait_for(|&stop| stop) => return,
            () = tokio::time::sleep_until(scheduled) => {}
        }
        let tick = CronTick {
            run,
            fired_at: SystemTime::now(),
        };
        run = run.wrapping_add(1);
        execute(&job, tick).await;
        scheduled = next_tick(scheduled, Instant::now(), Job::INTERVAL);
    }
}

/// Run `job` once, logging an error or a panic.
async fn execute<Job: CronJob>(job: &Job, tick: CronTick) {
    let span = tracing::info_span!("cron_job", job = type_name::<Job>(), run = tick.run);
    let outcome = AssertUnwindSafe(job.process(tick))
        .catch_unwind()
        .instrument(span.clone())
        .await;
    let _entered = span.enter();
    match outcome {
        Ok(Ok(())) => {}
        Ok(Err(error)) => tracing::error!(%error, "cron job failed"),
        Err(panic) => tracing::error!(panic = panic_message(&*panic), "cron job panicked"),
    }
}

/// The message of a panic payload, if it has one.
fn panic_message(payload: &(dyn Any + Send)) -> &str {
    payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("non-string panic payload")
}

/// The first grid point `previous + k * period` (`k >= 1`) that is not
/// before `now`.
///
/// `period` must not be zero.
fn next_tick(previous: Instant, now: Instant, period: Duration) -> Instant {
    let behind = now.saturating_duration_since(previous).as_nanos();
    let periods = behind.div_ceil(period.as_nanos()).max(1);
    period
        .as_nanos()
        .checked_mul(periods)
        .and_then(|nanos| u64::try_from(nanos).ok())
        .and_then(|nanos| previous.checked_add(Duration::from_nanos(nanos)))
        // Only reachable centuries away from `previous`.
        .unwrap_or_else(|| now.checked_add(period).unwrap_or(now))
}

#[cfg(test)]
#[allow(clippy::arithmetic_side_effects)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::sync::Mutex;

    const PERIOD: Duration = Duration::from_secs(10);

    #[test]
    fn next_tick_is_one_period_later_when_on_time() {
        let start = Instant::now();
        assert_eq!(next_tick(start, start, PERIOD), start + PERIOD);
        assert_eq!(next_tick(start, start + PERIOD / 2, PERIOD), start + PERIOD);
    }

    #[test]
    fn next_tick_keeps_a_grid_point_reached_exactly() {
        let start = Instant::now();
        assert_eq!(next_tick(start, start + PERIOD, PERIOD), start + PERIOD);
        assert_eq!(
            next_tick(start, start + PERIOD * 3, PERIOD),
            start + PERIOD * 3
        );
    }

    #[test]
    fn next_tick_skips_covered_grid_points() {
        let start = Instant::now();
        let now = start + PERIOD * 2 + PERIOD / 2;
        assert_eq!(next_tick(start, now, PERIOD), start + PERIOD * 3);
    }

    /// Records when each run starts; the first run takes `first_run` long.
    struct Recorder {
        starts: Arc<Mutex<Vec<Instant>>>,
        first_run: Duration,
    }

    impl Processor<CronTick> for Recorder {
        type Output = ();
        type Error = std::convert::Infallible;
        async fn process(&self, tick: CronTick) -> Result<(), Self::Error> {
            self.starts.lock().await.push(Instant::now());
            if tick.run == 0 {
                tokio::time::sleep(self.first_run).await;
            }
            Ok(())
        }
    }

    impl CronJob for Recorder {
        const INTERVAL: Duration = PERIOD;
    }

    #[tokio::test(start_paused = true)]
    async fn runs_on_the_grid_and_skips_ticks_covered_by_a_long_run() {
        let starts = Arc::new(Mutex::new(Vec::new()));
        let origin = Instant::now();
        let cron = CronRegisterCenter::new(Recorder {
            starts: starts.clone(),
            first_run: PERIOD * 2 + PERIOD / 2,
        })
        .start();
        tokio::time::sleep(PERIOD * 5 + PERIOD / 2).await;
        cron.shutdown().await;

        let offsets: Vec<Duration> = starts.lock().await.iter().map(|s| *s - origin).collect();
        assert_eq!(
            offsets,
            [Duration::ZERO, PERIOD * 3, PERIOD * 4, PERIOD * 5]
        );
    }

    /// Fails on run 0, panics on run 1, succeeds afterwards.
    struct Flaky {
        succeeded: Arc<AtomicBool>,
    }

    impl Processor<CronTick> for Flaky {
        type Output = ();
        type Error = &'static str;
        async fn process(&self, tick: CronTick) -> Result<(), Self::Error> {
            match tick.run {
                0 => Err("first run fails"),
                1 => panic!("second run panics"),
                _ => {
                    self.succeeded.store(true, Ordering::SeqCst);
                    Ok(())
                }
            }
        }
    }

    impl CronJob for Flaky {
        const INTERVAL: Duration = PERIOD;
    }

    #[tokio::test(start_paused = true)]
    async fn errors_and_panics_keep_the_schedule() {
        let succeeded = Arc::new(AtomicBool::new(false));
        let cron = CronRegisterCenter::new(Flaky {
            succeeded: succeeded.clone(),
        })
        .start();
        tokio::time::sleep(PERIOD * 2 + PERIOD / 2).await;
        cron.shutdown().await;
        assert!(succeeded.load(Ordering::SeqCst));
    }

    /// Takes half a period, then marks itself finished.
    struct Slow {
        runs: Arc<Mutex<u64>>,
        finished: Arc<AtomicBool>,
    }

    impl Processor<CronTick> for Slow {
        type Output = ();
        type Error = std::convert::Infallible;
        async fn process(&self, _: CronTick) -> Result<(), Self::Error> {
            *self.runs.lock().await += 1;
            tokio::time::sleep(PERIOD / 2).await;
            self.finished.store(true, Ordering::SeqCst);
            Ok(())
        }
    }

    impl CronJob for Slow {
        const INTERVAL: Duration = PERIOD;
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_lets_the_run_in_progress_finish_and_stops_the_schedule() {
        let runs = Arc::new(Mutex::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let cron = CronRegisterCenter::new(Slow {
            runs: runs.clone(),
            finished: finished.clone(),
        })
        .start();
        tokio::time::sleep(PERIOD / 4).await;
        assert!(!finished.load(Ordering::SeqCst));

        cron.shutdown().await;
        assert!(finished.load(Ordering::SeqCst));
        tokio::time::sleep(PERIOD * 3).await;
        assert_eq!(*runs.lock().await, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn every_job_in_the_chain_runs() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(AtomicBool::new(false));
        let cron = CronRegisterCenter::new(Recorder {
            starts: first.clone(),
            first_run: Duration::ZERO,
        })
        .push(Flaky {
            succeeded: second.clone(),
        })
        .start();
        tokio::time::sleep(PERIOD * 2 + PERIOD / 2).await;
        cron.shutdown().await;
        assert_eq!(first.lock().await.len(), 3);
        assert!(second.load(Ordering::SeqCst));
    }
}
