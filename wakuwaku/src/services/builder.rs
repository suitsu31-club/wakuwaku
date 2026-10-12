//! Compile-time, type-indexed service registry.
//!
//! [`ServiceBuilder`] stores services in a heterogeneous, singly linked list
//! built at the type level. Each [`push`](ServiceBuilder::push) wraps the
//! current builder in a new node, and [`provide`](ServiceBuilder::provide)
//! looks a service up by its type. Lookup is resolved entirely by the trait
//! solver: there is no `Any`, no downcasting, and no runtime map, so asking
//! for a service that was never registered is a compile error rather than a
//! panic.
//!
//! # Example
//!
//! ```
//! use wakuwaku::services::builder::ServiceBuilder;
//!
//! struct Config { name: &'static str }
//! struct Counter(u32);
//!
//! let services = ServiceBuilder::new(Config { name: "api" }).push(Counter(7));
//!
//! // The position index is inferred with `_`.
//! let config: &Config = services.provide::<Config, _>();
//! let counter: &Counter = services.provide::<Counter, _>();
//! assert_eq!(config.name, "api");
//! assert_eq!(counter.0, 7);
//! ```
//!
//! Requesting an unregistered type fails to compile:
//!
//! ```compile_fail
//! use wakuwaku::services::builder::ServiceBuilder;
//!
//! let services = ServiceBuilder::new(1u32);
//! let _: &String = services.provide::<String, _>();
//! ```
//!
//! # Duplicate types
//!
//! When the same type is registered more than once, the index cannot be
//! inferred and must be spelled out. [`Here`] is the most recently pushed
//! service; each [`There`] steps one node further back:
//!
//! ```
//! use wakuwaku::services::builder::{Here, ServiceBuilder, There};
//!
//! let services = ServiceBuilder::new(1u32).push(2u32);
//! assert_eq!(*services.provide::<u32, Here>(), 2);
//! assert_eq!(*services.provide::<u32, There<Here>>(), 1);
//! ```

use crate::services::ServiceCreation;
use crate::services::cron_job::{CronJob, CronRegisterCenter, CronRegisterCenterBuilder};
use std::marker::PhantomData;

/// A node in the type-level service list.
///
/// `T` is the service stored at this node and `Chain` is the rest of the list:
/// either another `ServiceBuilder` or `()` for the first registered service.
/// Construct one with [`ServiceBuilder::new`] and extend it with
/// [`ServiceBuilder::push`]. See the [module documentation](self) for usage.
#[derive(Clone)]
pub struct ServiceBuilder<T, Chain = ()> {
    this: T,
    that: Chain,
}

impl<Head, Chain> ServiceBuilder<Head, Chain> {
    /// Register `new` and return the extended builder.
    ///
    /// The new service becomes the head of the list, so it is found at
    /// position [`Here`]; every previously registered service moves one
    /// [`There`] further away.
    pub fn push<N>(self, new: N) -> ServiceBuilder<N, Self> {
        ServiceBuilder {
            this: new,
            that: self,
        }
    }

    /// Borrow the registered service of type `T`.
    ///
    /// `I` is the service's position in the list. Pass `_` to let the
    /// compiler infer it; this works whenever `T` is registered exactly once.
    /// If `T` is registered several times, name the position explicitly with
    /// [`Here`] / [`There`].
    pub fn provide<T, I>(&self) -> &T
    where
        Self: ProvideAt<T, I>,
        I: ServiceBuilderPosition,
    {
        <Self as ProvideAt<T, I>>::provide_at(self)
    }

    /// Create a service from a registered dependency and register it.
    ///
    /// `Service`'s [`Dep`](ServiceCreation::Dep) is looked up at `Position`
    /// and cloned. The service is then created with
    /// [`create_service`](ServiceCreation::create_service) and pushed, so it
    /// is found at [`Here`] and can itself be the dependency of the
    /// components registered after it.
    ///
    /// `Position` can usually be passed as `_`; it is inferred when the
    /// dependency's type is registered exactly once.
    ///
    /// ```
    /// use wakuwaku::services::ServiceCreation;
    /// use wakuwaku::services::builder::ServiceBuilder;
    ///
    /// #[derive(Clone)]
    /// struct CommonDep { db_url: &'static str }
    ///
    /// struct UserRepo { db_url: &'static str }
    ///
    /// impl ServiceCreation for UserRepo {
    ///     type Dep = CommonDep;
    ///     fn create_service(dep: CommonDep) -> Self { Self { db_url: dep.db_url } }
    /// }
    ///
    /// let services = ServiceBuilder::new(CommonDep { db_url: "postgres://db" })
    ///     .construct_subservice::<UserRepo, _>();
    /// assert_eq!(services.provide::<UserRepo, _>().db_url, "postgres://db");
    /// ```
    pub fn construct_subservice<Service, Position>(self) -> ServiceBuilder<Service, Self>
    where
        Service: ServiceCreation,
        Position: ServiceBuilderPosition,
        Self: ProvideAt<Service::Dep, Position>,
    {
        let new_service_dep = self.provide::<Service::Dep, Position>().clone();
        let new_service = Service::create_service(new_service_dep);
        self.push(new_service)
    }

    /// Build a service from any registered services and register it.
    ///
    /// `composer` borrows the whole builder, so it can
    /// [`provide`](Self::provide) as many registered services as it needs.
    /// Use this when a service needs a combination of dependencies that no
    /// single registered value holds. The result is pushed, so it is found at
    /// [`Here`] and can itself be the dependency of the components registered
    /// after it.
    ///
    /// ```
    /// use wakuwaku::services::builder::ServiceBuilder;
    ///
    /// #[derive(Clone)]
    /// struct IggyClient(&'static str);
    /// #[derive(Clone)]
    /// struct SmtpClient(&'static str);
    ///
    /// struct MailSender { iggy: IggyClient, smtp: SmtpClient }
    ///
    /// let services = ServiceBuilder::new(IggyClient("iggy"))
    ///     .push(SmtpClient("smtp"))
    ///     .compose_subservice(|s| MailSender {
    ///         iggy: s.provide::<IggyClient, _>().clone(),
    ///         smtp: s.provide::<SmtpClient, _>().clone(),
    ///     });
    /// let mailer = services.provide::<MailSender, _>();
    /// assert_eq!((mailer.iggy.0, mailer.smtp.0), ("iggy", "smtp"));
    /// ```
    pub fn compose_subservice<Service>(
        self,
        composer: impl FnOnce(&Self) -> Service,
    ) -> ServiceBuilder<Service, Self> {
        let new_service = composer(&self);
        self.push(new_service)
    }

    /// Create a cron job from a registered dependency and start a job list.
    ///
    /// `Job`'s [`Dep`](ServiceCreation::Dep) is looked up at `Position` and
    /// cloned. The job is then created with
    /// [`create_service`](ServiceCreation::create_service) and becomes the
    /// first entry of a new [`CronRegisterCenter`]. The returned builder
    /// takes ownership of `self`. Chain more `cron_job` calls on it, then
    /// call `start` or `into_parts`.
    ///
    /// `Position` can usually be passed as `_`; it is inferred when the
    /// dependency's type is registered exactly once. See the
    /// [`cron_job` module documentation](super::cron_job) for an example.
    pub fn cron_job<Job, Position>(self) -> CronRegisterCenterBuilder<Head, Chain, Job, ()>
    where
        Job: CronJob + ServiceCreation,
        Position: ServiceBuilderPosition,
        Self: ProvideAt<Job::Dep, Position>,
    {
        let dep = self.provide::<Job::Dep, Position>().clone();
        let register_center = CronRegisterCenter::new(Job::create_service(dep));
        CronRegisterCenterBuilder::new(self, register_center)
    }

    /// Build a cron job from any registered services and start a job list.
    ///
    /// The [`cron_job`](Self::cron_job) counterpart of
    /// [`compose_subservice`](Self::compose_subservice): `composer` borrows
    /// the whole builder, so it can [`provide`](Self::provide) as many
    /// registered services as it needs, or ignore it and move in a job that
    /// was built by hand. The job needs no [`ServiceCreation`] impl and is
    /// not registered as a service. It becomes the first entry of a new
    /// [`CronRegisterCenter`]; chain more jobs on the returned builder, then
    /// call `start` or `into_parts`.
    ///
    /// ```
    /// use std::time::Duration;
    /// use wakuwaku::deps::kanau::processor::Processor;
    /// use wakuwaku::services::builder::ServiceBuilder;
    /// use wakuwaku::services::cron_job::{CronJob, CronTick};
    ///
    /// #[derive(Clone)]
    /// struct Database;
    /// #[derive(Clone)]
    /// struct HttpClient;
    ///
    /// struct RotateKeys { db: Database, http: HttpClient }
    /// impl Processor<CronTick> for RotateKeys {
    ///     type Output = ();
    ///     type Error = std::convert::Infallible;
    ///     async fn process(&self, _: CronTick) -> Result<(), Self::Error> { Ok(()) }
    /// }
    /// impl CronJob for RotateKeys {
    ///     const INTERVAL: Duration = Duration::from_secs(3600);
    /// }
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() {
    /// let (_services, cron) = ServiceBuilder::new(Database)
    ///     .push(HttpClient)
    ///     .compose_cron_job(|s| RotateKeys {
    ///         db: s.provide::<Database, _>().clone(),
    ///         http: s.provide::<HttpClient, _>().clone(),
    ///     })
    ///     .start();
    /// cron.shutdown().await;
    /// # }
    /// ```
    pub fn compose_cron_job<Job: CronJob>(
        self,
        composer: impl FnOnce(&Self) -> Job,
    ) -> CronRegisterCenterBuilder<Head, Chain, Job, ()> {
        let register_center = CronRegisterCenter::new(composer(&self));
        CronRegisterCenterBuilder::new(self, register_center)
    }

    /// Create an Iggy event handler from a registered dependency and start a
    /// handler list.
    ///
    /// `Handler`'s [`Dep`](ServiceCreation::Dep) is looked up at `Position`
    /// and cloned. The handler is then created with
    /// [`create_service`](ServiceCreation::create_service), wrapped in an
    /// `Arc`, and becomes the first entry of a new
    /// [`IggyConsumerRegisterCenter`](wakuwaku_iggy::IggyConsumerRegisterCenter).
    /// The returned builder takes ownership of `self`. Chain more
    /// `iggy_consumer` calls on it, then call `start` or `into_parts`.
    ///
    /// `Event` and `Position` can usually be passed as `_`. The event is
    /// inferred when `Handler` implements
    /// [`EventHandler`](wakuwaku_iggy::EventHandler) for only one event. The
    /// position is inferred when the dependency's type is registered exactly
    /// once. See the
    /// [`iggy_consumer` module documentation](super::iggy_consumer) for an
    /// example.
    #[cfg(feature = "iggy")]
    pub fn iggy_consumer<Handler, Event, Position>(
        self,
    ) -> super::iggy_consumer::IggyConsumerRegisterCenterBuilder<Head, Chain, Handler, Event, ()>
    where
        Handler: wakuwaku_iggy::EventHandler<Event> + ServiceCreation,
        Event: wakuwaku_iggy::Event,
        Position: ServiceBuilderPosition,
        Self: ProvideAt<Handler::Dep, Position>,
    {
        let dep = self.provide::<Handler::Dep, Position>().clone();
        let handler = std::sync::Arc::new(Handler::create_service(dep));
        let register_center = wakuwaku_iggy::IggyConsumerRegisterCenter::new(handler);
        super::iggy_consumer::IggyConsumerRegisterCenterBuilder::new(self, register_center)
    }

    /// Build an Iggy event handler from any registered services and start a
    /// handler list.
    ///
    /// The [`iggy_consumer`](Self::iggy_consumer) counterpart of
    /// [`compose_subservice`](Self::compose_subservice): `composer` borrows
    /// the whole builder, so it can [`provide`](Self::provide) as many
    /// registered services as it needs, or ignore it and move in a handler
    /// that was built by hand. It returns the handler in the [`Arc`] the
    /// handler list stores, so an `Arc` that is shared elsewhere can be
    /// registered as is. The handler needs no [`ServiceCreation`] impl and is
    /// not registered as a service. It becomes the first entry of a new
    /// [`IggyConsumerRegisterCenter`](wakuwaku_iggy::IggyConsumerRegisterCenter);
    /// chain more handlers on the returned builder, then call `start` or
    /// `into_parts`.
    ///
    /// `Handler` and `Event` are inferred from the returned handler when it
    /// implements [`EventHandler`](wakuwaku_iggy::EventHandler) for only one
    /// event; otherwise name the event, e.g.
    /// `.compose_iggy_consumer::<_, MyEvent>(…)`. See the
    /// [`iggy_consumer` module documentation](super::iggy_consumer) for an
    /// example.
    ///
    /// [`Arc`]: std::sync::Arc
    #[cfg(feature = "iggy")]
    pub fn compose_iggy_consumer<Handler, Event>(
        self,
        composer: impl FnOnce(&Self) -> std::sync::Arc<Handler>,
    ) -> super::iggy_consumer::IggyConsumerRegisterCenterBuilder<Head, Chain, Handler, Event, ()>
    where
        Handler: wakuwaku_iggy::EventHandler<Event>,
        Event: wakuwaku_iggy::Event,
    {
        let register_center = wakuwaku_iggy::IggyConsumerRegisterCenter::new(composer(&self));
        super::iggy_consumer::IggyConsumerRegisterCenterBuilder::new(self, register_center)
    }

    /// Create a gRPC service implementation from a registered dependency,
    /// wrap it into its server, and start a server list.
    ///
    /// `ServiceImpl`'s [`Dep`](ServiceCreation::Dep) is looked up at
    /// `Position` and cloned. The implementation is then created with
    /// [`create_service`](ServiceCreation::create_service), turned into a
    /// `Server` with `into_server` (for tonic-generated servers, pass
    /// `XxxServer::new`), and becomes the first entry of a new
    /// [`GrpcServiceRegisterCenter`](super::grpc_service::GrpcServiceRegisterCenter).
    /// The returned builder takes ownership of `self`. Chain more
    /// `grpc_service` calls on it, then call `into_router_fn` or `into_parts`.
    ///
    /// Write a generated server type as `XxxServer<_>`; its type parameter is
    /// inferred from `ServiceImpl`. The position is inferred with `_` when the
    /// dependency's type is registered exactly once. See the
    /// [`grpc_service` module documentation](super::grpc_service#building-servers-from-a-servicebuilder)
    /// for an example.
    #[cfg(feature = "grpc")]
    pub fn grpc_service<Server, ServiceImpl, Position>(
        self,
        into_server: impl FnOnce(ServiceImpl) -> Server,
    ) -> super::grpc_service::GrpcServiceRegisterCenterBuilder<Head, Chain, Server, ()>
    where
        Server: super::grpc_service::RoutableService,
        ServiceImpl: ServiceCreation,
        Position: ServiceBuilderPosition,
        Self: ProvideAt<ServiceImpl::Dep, Position>,
    {
        let service_dep = self.provide::<ServiceImpl::Dep, Position>().clone();
        let server = into_server(ServiceImpl::create_service(service_dep));
        let register_center = super::grpc_service::GrpcServiceRegisterCenter::new(server);
        super::grpc_service::GrpcServiceRegisterCenterBuilder::new(self, register_center)
    }

    /// Build a gRPC server from any registered services and start a server
    /// list.
    ///
    /// The [`grpc_service`](Self::grpc_service) counterpart of
    /// [`compose_subservice`](Self::compose_subservice): `composer` borrows
    /// the whole builder, so it can [`provide`](Self::provide) as many
    /// registered services as it needs, or ignore it and move in a server
    /// whose implementation was assembled by hand. It returns the finished
    /// server (for tonic-generated servers, `XxxServer::new(implementation)`),
    /// whose type is inferred. The implementation needs no
    /// [`ServiceCreation`] impl and is not registered as a service. The
    /// server becomes the first entry of a new
    /// [`GrpcServiceRegisterCenter`](super::grpc_service::GrpcServiceRegisterCenter);
    /// chain more servers on the returned builder, then call
    /// `into_router_fn` or `into_parts`. See the
    /// [`grpc_service` module documentation](super::grpc_service#building-servers-from-a-servicebuilder)
    /// for an example.
    #[cfg(feature = "grpc")]
    pub fn compose_grpc_service<Server>(
        self,
        composer: impl FnOnce(&Self) -> Server,
    ) -> super::grpc_service::GrpcServiceRegisterCenterBuilder<Head, Chain, Server, ()>
    where
        Server: super::grpc_service::RoutableService,
    {
        let register_center = super::grpc_service::GrpcServiceRegisterCenter::new(composer(&self));
        super::grpc_service::GrpcServiceRegisterCenterBuilder::new(self, register_center)
    }
}

impl<T> ServiceBuilder<T> {
    /// Start a new builder holding a single service.
    pub fn new(value: T) -> Self {
        ServiceBuilder {
            this: value,
            that: (),
        }
    }
}

/// Marker for type-level positions within a [`ServiceBuilder`].
///
/// Implemented by [`Here`] and [`There`]; positions are Peano-style naturals
/// counted from the most recently pushed service.
pub trait ServiceBuilderPosition {}

/// Position of the head of the list, i.e. the most recently pushed service.
pub struct Here;

impl ServiceBuilderPosition for Here {}

/// Position one node past `T`, i.e. `There<Here>` is the second most recently
/// pushed service. Never constructed; exists only at the type level.
pub struct There<T>(PhantomData<T>);

impl<T> ServiceBuilderPosition for There<T> {}

/// Proof that a service of type `T` is stored at position `Index`.
///
/// Implemented for every [`ServiceBuilder`] that contains a `T`, once per
/// position where it occurs. Prefer calling [`ServiceBuilder::provide`];
/// use this trait directly as a bound when writing code generic over the
/// builder, e.g. `fn run<S: ProvideAt<Config, I>, I: ServiceBuilderPosition>(s: &S)`.
pub trait ProvideAt<T, Index: ServiceBuilderPosition> {
    /// Borrow the service stored at `Index`.
    fn provide_at(&self) -> &T;
}

/// The head node provides its own service.
impl<T, Chain> ProvideAt<T, Here> for ServiceBuilder<T, Chain> {
    fn provide_at(&self) -> &T {
        &self.this
    }
}

/// Any other position is delegated to the rest of the list.
impl<Head, T, Chain, I> ProvideAt<T, There<I>> for ServiceBuilder<Head, Chain>
where
    Chain: ProvideAt<T, I>,
    I: ServiceBuilderPosition,
{
    fn provide_at(&self) -> &T {
        self.that.provide_at()
    }
}
