//! Jobs that run locally on an interval.
//!
//! A [`CronJob`] is a [`Processor`] of [`CronTick`]s with a fixed
//! [`INTERVAL`](CronJob::INTERVAL), a [`CronJobInterval`]. Cron jobs are
//! usually scans (expire stale rows, retry stuck work, reconcile an index),
//! so the interval is a constant of the job type rather than a calendar
//! expression.
//!
//! [`CronRegisterCenter`] collects jobs in a type-level linked list, the same
//! way [`ServiceBuilder`] does; there is no boxing and no dynamic dispatch.
//! [`start`](CronRegisterCenter::start) spawns one Tokio task per job and
//! returns a [`CronRuntime`].
//!
//! # Schedule
//!
//! Every job's first run is when [`start`](CronRegisterCenter::start) was
//! called. The variant of [`CronJob::INTERVAL`] decides when the next runs
//! start, for a duration `d`:
//!
//! | `INTERVAL`        | Next run starts                                         | Runs overlap |
//! |-------------------|---------------------------------------------------------|--------------|
//! | `AfterFinish(d)`  | `d` after the previous run finished                     | no           |
//! | `AtLeast(d)`      | `d` after the previous run started, or as soon as it finished if that is later | no |
//! | `AfterStart(d)`   | on the grid `start + k * d`                             | yes          |
//!
//! - With `AfterFinish` and `AtLeast`, two consecutive runs start at least
//!   `d` apart on the monotonic clock, so their [`fired_at`](CronTick::fired_at)
//!   are at least `d` apart unless the wall clock is adjusted in between.
//! - With `AfterStart`, runs start on the grid however long they take; a run
//!   that takes longer than `d` runs concurrently with the next ones. Grid
//!   points missed because the runtime was blocked are skipped rather than
//!   run in a burst. `fired_at` includes the wake-up delay, so consecutive
//!   `fired_at` can be slightly less than `d` apart.
//! - A run that returns an error, or panics, is logged with [`tracing`] and
//!   the job keeps its schedule.
//!
//! A zero duration has no schedule. Registering such a job fails to compile;
//! the check runs when the registration is instantiated, so `cargo build`
//! reports it and `cargo check` alone does not:
//!
//! ```compile_fail
//! use std::time::Duration;
//! use wakuwaku::deps::kanau::processor::Processor;
//! use wakuwaku::services::cron_job::{CronJob, CronJobInterval, CronRegisterCenter, CronTick};
//!
//! struct Busy;
//! impl Processor<CronTick> for Busy {
//!     type Output = ();
//!     type Error = std::convert::Infallible;
//!     async fn process(&self, _: CronTick) -> Result<(), Self::Error> { Ok(()) }
//! }
//! impl CronJob for Busy {
//!     const INTERVAL: CronJobInterval = CronJobInterval::AtLeast(Duration::ZERO);
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
//! `wakuwaku_entity::redis::collection::lock::Lock` inside the run. To run
//! each pass once across replicas, a run can claim it in shared storage with
//! a fence such as "the last claimed run fired at least `d` before this
//! tick's `fired_at`"; use `AfterFinish` or `AtLeast` for such a job, since
//! `AfterStart` runs can fire slightly less than `d` apart.
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
//! use wakuwaku::services::cron_job::{CronJob, CronJobInterval, CronTick};
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
//!     const INTERVAL: CronJobInterval = CronJobInterval::AtLeast(Duration::from_secs(60));
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
use std::sync::Arc;
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

/// How the runs of a [`CronJob`] are spaced. See the
/// [module documentation](self#schedule) for the schedule.
///
/// The duration must not be zero; registering a job with a zero duration
/// fails to build.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CronJobInterval {
    /// Time from the end of a run to the start of the next one. Runs never
    /// overlap.
    AfterFinish(Duration),

    /// Minimum time from the start of a run to the start of the next one.
    /// Runs never overlap: a run that takes longer is followed by the next
    /// one as soon as it finishes, so consecutive starts are
    /// `max(duration, run time)` apart.
    AtLeast(Duration),

    /// Time between the starts of consecutive runs, on a fixed grid. A run
    /// that takes longer runs concurrently with the next ones.
    AfterStart(Duration),
}

impl CronJobInterval {
    /// The duration of any variant.
    const fn duration(self) -> Duration {
        match self {
            Self::AfterFinish(duration) | Self::AtLeast(duration) | Self::AfterStart(duration) => {
                duration
            }
        }
    }
}

/// A job that runs locally on the schedule set by
/// [`INTERVAL`](Self::INTERVAL).
///
/// The job itself is a [`Processor`] of [`CronTick`]s that outputs `()`.
/// Returned errors are logged with [`tracing`] and don't stop the job. See
/// the [module documentation](self) for the schedule.
pub trait CronJob:
    Processor<CronTick, Output = (), Error: Display> + Send + Sync + 'static
{
    /// How runs are spaced. Its duration must not be zero; registering a job
    /// with a zero duration fails to build.
    const INTERVAL: CronJobInterval;
}

/// Rejects a zero [`CronJob::INTERVAL`] at compile time.
const fn assert_interval<Job: CronJob>() {
    assert!(
        !Job::INTERVAL.duration().is_zero(),
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
async fn run_job<Job: CronJob>(job: Job, stop: watch::Receiver<bool>) {
    match Job::INTERVAL {
        CronJobInterval::AfterFinish(pause) => {
            run_serially(job, stop, |_started, finished| later(finished, pause)).await;
        }
        CronJobInterval::AtLeast(period) => {
            run_serially(job, stop, |started, _finished| later(started, period)).await;
        }
        CronJobInterval::AfterStart(period) => run_overlapping(job, stop, period).await,
    }
}

/// Runs `job` one run at a time. `next_start` gets when the previous run
/// started and finished, and returns when the next one starts; a time in the
/// past starts it at once.
async fn run_serially<Job: CronJob>(
    job: Job,
    mut stop: watch::Receiver<bool>,
    next_start: impl Fn(Instant, Instant) -> Instant,
) {
    let mut scheduled = Instant::now();
    let mut run = 0u64;
    loop {
        tokio::select! {
            biased;
            _ = stop.wait_for(|&stop| stop) => return,
            () = tokio::time::sleep_until(scheduled) => {}
        }
        let started = Instant::now();
        let tick = CronTick {
            run,
            fired_at: SystemTime::now(),
        };
        run = run.wrapping_add(1);
        execute(&job, tick).await;
        scheduled = next_start(started, Instant::now());
    }
}

/// Starts a run of `job` every `period` on a grid, each in its own task, so
/// that runs can overlap. Once stopped, waits for the runs in progress.
async fn run_overlapping<Job: CronJob>(
    job: Job,
    mut stop: watch::Receiver<bool>,
    period: Duration,
) {
    let job = Arc::new(job);
    let mut runs = JoinSet::new();
    let mut scheduled = Instant::now();
    let mut run = 0u64;
    loop {
        tokio::select! {
            biased;
            _ = stop.wait_for(|&stop| stop) => break,
            // Reap finished runs so the set doesn't grow; `execute` already
            // logged their outcome.
            Some(_) = runs.join_next(), if !runs.is_empty() => {}
            () = tokio::time::sleep_until(scheduled) => {
                let tick = CronTick {
                    run,
                    fired_at: SystemTime::now(),
                };
                run = run.wrapping_add(1);
                let job = Arc::clone(&job);
                runs.spawn(async move { execute(&*job, tick).await });
                scheduled = next_tick(scheduled, Instant::now(), period);
            }
        }
    }
    while runs.join_next().await.is_some() {}
}

/// `instant + duration`, or `instant` in the unreachable case that it
/// overflows.
fn later(instant: Instant, duration: Duration) -> Instant {
    instant.checked_add(duration).unwrap_or(instant)
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
    use std::marker::PhantomData;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

    /// Picks the [`CronJob::INTERVAL`] of a test job.
    trait Schedule: Send + Sync + 'static {
        const INTERVAL: CronJobInterval;
    }

    struct AfterFinish;
    impl Schedule for AfterFinish {
        const INTERVAL: CronJobInterval = CronJobInterval::AfterFinish(PERIOD);
    }

    struct AtLeast;
    impl Schedule for AtLeast {
        const INTERVAL: CronJobInterval = CronJobInterval::AtLeast(PERIOD);
    }

    struct AfterStart;
    impl Schedule for AfterStart {
        const INTERVAL: CronJobInterval = CronJobInterval::AfterStart(PERIOD);
    }

    /// Records when each run starts; the first run takes `first_run` long,
    /// the others return at once.
    struct Recorder<S> {
        starts: Arc<Mutex<Vec<Instant>>>,
        first_run: Duration,
        schedule: PhantomData<S>,
    }

    impl<S> Recorder<S> {
        fn new(starts: Arc<Mutex<Vec<Instant>>>, first_run: Duration) -> Self {
            Self {
                starts,
                first_run,
                schedule: PhantomData,
            }
        }
    }

    impl<S: Schedule> Processor<CronTick> for Recorder<S> {
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

    impl<S: Schedule> CronJob for Recorder<S> {
        const INTERVAL: CronJobInterval = S::INTERVAL;
    }

    /// Offsets from the start of the run starts of a job whose first run
    /// takes two and a half periods, observed for five and a quarter periods.
    async fn starts_after_a_long_first_run<S: Schedule>() -> Vec<Duration> {
        let starts = Arc::new(Mutex::new(Vec::new()));
        let origin = Instant::now();
        let cron =
            CronRegisterCenter::new(Recorder::<S>::new(starts.clone(), PERIOD * 2 + PERIOD / 2))
                .start();
        tokio::time::sleep(PERIOD * 5 + PERIOD / 4).await;
        cron.shutdown().await;
        let starts = starts.lock().await;
        starts.iter().map(|s| *s - origin).collect()
    }

    #[tokio::test(start_paused = true)]
    async fn after_finish_waits_the_interval_after_each_run_ends() {
        assert_eq!(
            starts_after_a_long_first_run::<AfterFinish>().await,
            [
                Duration::ZERO,
                PERIOD * 3 + PERIOD / 2,
                PERIOD * 4 + PERIOD / 2
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn at_least_starts_after_the_interval_or_when_the_long_run_ends() {
        assert_eq!(
            starts_after_a_long_first_run::<AtLeast>().await,
            [
                Duration::ZERO,
                PERIOD * 2 + PERIOD / 2,
                PERIOD * 3 + PERIOD / 2,
                PERIOD * 4 + PERIOD / 2
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn after_start_keeps_the_grid_and_overlaps_a_long_run() {
        assert_eq!(
            starts_after_a_long_first_run::<AfterStart>().await,
            [
                Duration::ZERO,
                PERIOD,
                PERIOD * 2,
                PERIOD * 3,
                PERIOD * 4,
                PERIOD * 5
            ]
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
        const INTERVAL: CronJobInterval = CronJobInterval::AtLeast(PERIOD);
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

    /// Takes one and a half periods, counting the runs that started and
    /// finished.
    struct Slow<S> {
        started: Arc<AtomicU64>,
        finished: Arc<AtomicU64>,
        schedule: PhantomData<S>,
    }

    impl<S: Schedule> Processor<CronTick> for Slow<S> {
        type Output = ();
        type Error = std::convert::Infallible;
        async fn process(&self, _: CronTick) -> Result<(), Self::Error> {
            self.started.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(PERIOD + PERIOD / 2).await;
            self.finished.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    impl<S: Schedule> CronJob for Slow<S> {
        const INTERVAL: CronJobInterval = S::INTERVAL;
    }

    /// Shuts a [`Slow`] job down at `stop_at`, while no run has finished, and
    /// returns how many runs started and finished, observed three periods
    /// after the shutdown.
    async fn shut_down_slow_job<S: Schedule>(stop_at: Duration) -> (u64, u64) {
        let started = Arc::new(AtomicU64::new(0));
        let finished = Arc::new(AtomicU64::new(0));
        let cron = CronRegisterCenter::new(Slow::<S> {
            started: started.clone(),
            finished: finished.clone(),
            schedule: PhantomData,
        })
        .start();
        tokio::time::sleep(stop_at).await;
        assert_eq!(finished.load(Ordering::SeqCst), 0);

        cron.shutdown().await;
        tokio::time::sleep(PERIOD * 3).await;
        (
            started.load(Ordering::SeqCst),
            finished.load(Ordering::SeqCst),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_lets_the_run_in_progress_finish_and_stops_the_schedule() {
        assert_eq!(shut_down_slow_job::<AtLeast>(PERIOD / 4).await, (1, 1));
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_lets_every_overlapping_run_finish() {
        assert_eq!(
            shut_down_slow_job::<AfterStart>(PERIOD + PERIOD / 4).await,
            (2, 2)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn every_job_in_the_chain_runs() {
        let first = Arc::new(Mutex::new(Vec::new()));
        let second = Arc::new(AtomicBool::new(false));
        let cron = CronRegisterCenter::new(Recorder::<AtLeast>::new(first.clone(), Duration::ZERO))
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
