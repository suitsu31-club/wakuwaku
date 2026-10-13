//! Registering tonic gRPC servers.
//!
//! [`GrpcServiceRegisterCenter`] is a type-level list of servers
//! (tonic-generated `XxxServer<T>`, or any other [`RoutableService`]), with no
//! boxing and no dynamic dispatch. It is filled from a [`ServiceBuilder`]
//! through a [`GrpcServiceRegisterCenterBuilder`], started by either
//!
//! - [`ServiceBuilder::grpc_service`]: the implementation is created from one
//!   registered dependency through [`ServiceCreation`] and wrapped by the
//!   constructor you pass (`XxxServer::new`), or
//! - [`ServiceBuilder::compose_grpc_service`]: a closure borrows the service
//!   builder and returns the finished server.
//!
//! Both chain in any order. [`into_router_fn`](GrpcServiceRegisterCenterBuilder::into_router_fn)
//! gives back the service builder with a function that adds every server to
//! a tonic [`Router`].
//!
//! # Example
//!
//! ```no_run
//! use wakuwaku::deps::tonic::service::Routes;
//! use wakuwaku::deps::tonic::transport::Server;
//! use wakuwaku::services::ServiceCreation;
//! use wakuwaku::services::builder::ServiceBuilder;
//! # use std::convert::Infallible;
//! # use std::task::{Context, Poll};
//! # use wakuwaku::deps::tonic;
//! # use wakuwaku::deps::tonic::body::Body;
//! # use wakuwaku::deps::tonic::codegen::{BoxFuture, Service, http};
//! # macro_rules! server {
//! #     ($server:ident, $service:ident, $name:literal) => {
//! #         pub trait $service: Send + Sync + 'static {}
//! #         pub struct $server<T> { inner: std::sync::Arc<T> }
//! #         impl<T> $server<T> {
//! #             pub fn new(inner: T) -> Self { Self { inner: std::sync::Arc::new(inner) } }
//! #         }
//! #         impl<T> Clone for $server<T> {
//! #             fn clone(&self) -> Self { Self { inner: self.inner.clone() } }
//! #         }
//! #         impl<T> tonic::server::NamedService for $server<T> { const NAME: &'static str = $name; }
//! #         impl<T: $service> Service<http::Request<Body>> for $server<T> {
//! #             type Response = http::Response<Body>;
//! #             type Error = Infallible;
//! #             type Future = BoxFuture<Self::Response, Self::Error>;
//! #             fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
//! #                 Poll::Ready(Ok(()))
//! #             }
//! #             fn call(&mut self, _: http::Request<Body>) -> Self::Future {
//! #                 Box::pin(async { Ok(http::Response::new(Body::default())) })
//! #             }
//! #         }
//! #     };
//! # }
//! # server!(AdminAuthServiceServer, AdminAuthService, "manage.AdminAuthService");
//! # server!(AccountManageServer, AccountManage, "manage.AccountManage");
//! # server!(UserAuthServer, UserAuth, "auth.UserAuth");
//! # impl AdminAuthService for AdminAuthServiceImpl {}
//! # impl AccountManage for AdminManageServiceImpl {}
//! # impl UserAuth for UserAuthServiceImpl {}
//!
//! // Shared dependencies: cheap to clone (handles, `Arc`s, pools).
//! #[derive(Clone)]
//! struct AdminStore;
//! #[derive(Clone)]
//! struct AuditLayer;
//!
//! // Built from one registered dependency.
//! struct AdminAuthServiceImpl { store: AdminStore }
//! impl ServiceCreation for AdminAuthServiceImpl {
//!     type Dep = AdminStore;
//!     fn create_service(store: AdminStore) -> Self { Self { store } }
//! }
//!
//! // Needs two registered services: composed from the service builder.
//! struct AdminManageServiceImpl { store: AdminStore, audit: AuditLayer }
//!
//! // Assembled before the chain from values that are not registered.
//! struct UserAuthServiceImpl { store: AdminStore, issuer: String }
//!
//! async fn serve() -> Result<(), Box<dyn std::error::Error>> {
//!     let store = AdminStore;
//!     let user_auth = UserAuthServiceImpl { store: store.clone(), issuer: "auth".into() };
//!
//!     let (services, register) = ServiceBuilder::new(store)
//!         .push(AuditLayer)
//!         .grpc_service::<AdminAuthServiceServer<_>, AdminAuthServiceImpl, _>(AdminAuthServiceServer::new)
//!         .compose_grpc_service(|s| {
//!             AccountManageServer::new(AdminManageServiceImpl {
//!                 store: s.provide::<AdminStore, _>().clone(),
//!                 audit: s.provide::<AuditLayer, _>().clone(),
//!             })
//!         })
//!         .compose_grpc_service(|_| UserAuthServer::new(user_auth))
//!         .into_router_fn();
//!
//!     // The service builder is still available to the rest of the app.
//!     let _audit: &AuditLayer = services.provide::<AuditLayer, _>();
//!
//!     let router = Server::builder().add_routes(Routes::default());
//!     register(router).serve("[::1]:50051".parse()?).await?;
//!     Ok(())
//! }
//! ```
//!
//! See the [gRPC guide](https://www.wakuwaku.rs/en/docs/services/grpc).

use crate::services::ServiceCreation;
use crate::services::builder::{ProvideAt, ServiceBuilder, ServiceBuilderPosition};
use std::convert::Infallible;
use tonic::body::Body;
use tonic::codegen::{Service, http};
use tonic::server::NamedService;
use tonic::transport::server::Router;

/// A service that [`Router::add_service`] accepts.
///
/// Implemented for every type that meets those bounds, which includes every
/// tonic-generated server whose implementation is `Send + Sync + 'static`.
/// It only names the bounds; there is nothing to implement.
pub trait RoutableService:
    Service<
        http::Request<Body>,
        Response = http::Response<Body>,
        Error = Infallible,
        Future: Send + 'static,
    > + NamedService
    + Clone
    + Send
    + Sync
    + 'static
{
}

impl<S> RoutableService for S where
    S: Service<
            http::Request<Body>,
            Response = http::Response<Body>,
            Error = Infallible,
            Future: Send + 'static,
        > + NamedService
        + Clone
        + Send
        + Sync
        + 'static
{
}

/// Registers gRPC servers built from a [`ServiceBuilder`].
///
/// Pairs a service builder (`SHead`, `SChain`) with a
/// [`GrpcServiceRegisterCenter`] (`Server`, `GChain`) that is being filled
/// from it. Get one from [`ServiceBuilder::grpc_service`] or
/// [`ServiceBuilder::compose_grpc_service`], register more servers with
/// [`grpc_service`](Self::grpc_service) or
/// [`compose_grpc_service`](Self::compose_grpc_service), then finish with
/// [`into_router_fn`](Self::into_router_fn) or [`into_parts`](Self::into_parts).
/// See the [module documentation](self) for an example.
pub struct GrpcServiceRegisterCenterBuilder<SHead, SChain, Server, GChain> {
    /// The service builder that service dependencies are taken from.
    pub builder: ServiceBuilder<SHead, SChain>,
    register_center: GrpcServiceRegisterCenter<Server, GChain>,
}

impl<SH, SC, S, GC> GrpcServiceRegisterCenterBuilder<SH, SC, S, GC> {
    /// Pair an existing service builder with an existing server list.
    ///
    /// Usually you don't need this: [`ServiceBuilder::grpc_service`] and
    /// [`ServiceBuilder::compose_grpc_service`] create the builder for you.
    pub fn new(
        builder: ServiceBuilder<SH, SC>,
        register_center: GrpcServiceRegisterCenter<S, GC>,
    ) -> Self {
        Self {
            builder,
            register_center,
        }
    }

    /// Create `ServiceImpl` from its dependency and register it as a
    /// `NewServer`.
    ///
    /// The dependency, [`ServiceImpl::Dep`](ServiceCreation::Dep), is looked
    /// up in [`builder`](Self::builder) at `Position` and cloned. The
    /// implementation is created with
    /// [`create_service`](ServiceCreation::create_service), turned into its
    /// server with `into_server`, and pushed onto the list the same way as
    /// [`GrpcServiceRegisterCenter::push`].
    ///
    /// For tonic-generated servers, pass the server's constructor
    /// (`XxxServer::new`) as `into_server` and write the server type as
    /// `XxxServer<_>`; its type parameter is inferred from `ServiceImpl`.
    /// `Position` can usually be passed as `_`; it is inferred when exactly
    /// one service of type `ServiceImpl::Dep` is registered. A dependency that
    /// was never registered is a compile error.
    pub fn grpc_service<NewServer, ServiceImpl, Position>(
        self,
        into_server: impl FnOnce(ServiceImpl) -> NewServer,
    ) -> GrpcServiceRegisterCenterBuilder<SH, SC, NewServer, GrpcServiceRegisterCenter<S, GC>>
    where
        NewServer: RoutableService,
        ServiceImpl: ServiceCreation,
        Position: ServiceBuilderPosition,
        ServiceBuilder<SH, SC>: ProvideAt<ServiceImpl::Dep, Position>,
    {
        let service_dep = self.builder.provide::<ServiceImpl::Dep, Position>().clone();
        let server = into_server(ServiceImpl::create_service(service_dep));
        GrpcServiceRegisterCenterBuilder {
            builder: self.builder,
            register_center: self.register_center.push(server),
        }
    }

    /// Build `NewServer` from any registered services and register it.
    ///
    /// `composer` borrows [`builder`](Self::builder), so it can
    /// [`provide`](ServiceBuilder::provide) as many registered services as it
    /// needs, or ignore it and move in a server whose implementation was
    /// assembled by hand. It returns the finished server (for
    /// tonic-generated servers, `XxxServer::new(implementation)`), whose type
    /// is inferred. The implementation needs no [`ServiceCreation`] impl, and
    /// the server is pushed onto the list the same way as
    /// [`GrpcServiceRegisterCenter::push`].
    pub fn compose_grpc_service<NewServer: RoutableService>(
        self,
        composer: impl FnOnce(&ServiceBuilder<SH, SC>) -> NewServer,
    ) -> GrpcServiceRegisterCenterBuilder<SH, SC, NewServer, GrpcServiceRegisterCenter<S, GC>> {
        let server = composer(&self.builder);
        GrpcServiceRegisterCenterBuilder {
            builder: self.builder,
            register_center: self.register_center.push(server),
        }
    }

    /// Split into the service builder and the server list.
    ///
    /// Use this to extend the list with [`GrpcServiceRegisterCenter::push`]
    /// outside the chain. Within the chain, register a server built by hand
    /// with [`compose_grpc_service`](Self::compose_grpc_service).
    pub fn into_parts(self) -> (ServiceBuilder<SH, SC>, GrpcServiceRegisterCenter<S, GC>) {
        (self.builder, self.register_center)
    }

    /// Return the service builder together with a function that adds every
    /// registered server to a router.
    ///
    /// This is [`into_parts`](Self::into_parts) followed by
    /// [`GrpcServiceRegisterCenter::into_router_fn`].
    ///
    /// # Panics
    ///
    /// The returned function panics if two servers share a
    /// [`NamedService::NAME`].
    pub fn into_router_fn<L>(self) -> (ServiceBuilder<SH, SC>, impl FnOnce(Router<L>) -> Router<L>)
    where
        GrpcServiceRegisterCenter<S, GC>: AddGrpcServices,
    {
        (self.builder, self.register_center.into_router_fn())
    }
}

/// A node in the type-level list of registered gRPC servers.
///
/// `Server` is the server stored at this node and `Chain` is the rest of the
/// list: either another `GrpcServiceRegisterCenter` or `()` for the first
/// registered server. Build one with [`new`](Self::new), extend it with
/// [`push`](Self::push), and finish with
/// [`into_router_fn`](Self::into_router_fn). See the
/// [module documentation](self) for an example.
pub struct GrpcServiceRegisterCenter<Server, Chain = ()> {
    head: Server,
    chain: Chain,
}

impl<Server: RoutableService> GrpcServiceRegisterCenter<Server> {
    /// Start a new list with `head` as its first server.
    pub fn new(head: Server) -> Self {
        Self { head, chain: () }
    }
}

impl<Server, Chain> GrpcServiceRegisterCenter<Server, Chain> {
    /// Register another server, returning the extended list.
    ///
    /// `next` becomes the new head. Servers are still added to the router in
    /// registration order.
    pub fn push<Next: RoutableService>(self, next: Next) -> GrpcServiceRegisterCenter<Next, Self> {
        GrpcServiceRegisterCenter {
            head: next,
            chain: self,
        }
    }

    /// Turn the list into a function that adds every registered server to a
    /// router with [`Router::add_service`], in registration order.
    ///
    /// The router's layer stack `L` is inferred from the router the function
    /// is called with and left untouched, so layers configured on
    /// [`Server`](tonic::transport::Server) before the router was created
    /// apply to every registered server.
    ///
    /// # Panics
    ///
    /// The returned function panics if two servers share a
    /// [`NamedService::NAME`]: [`Router::add_service`] rejects the duplicate
    /// route.
    pub fn into_router_fn<L>(self) -> impl FnOnce(Router<L>) -> Router<L>
    where
        Self: AddGrpcServices,
    {
        move |router| self.add_services(router)
    }
}

/// Adds each server of a [`GrpcServiceRegisterCenter`] chain to a router in
/// registration order.
///
/// Implemented for every chain of [`RoutableService`]s. It is public only
/// because it appears in the bounds of
/// [`GrpcServiceRegisterCenter::into_router_fn`]. Call `into_router_fn`
/// instead of using it directly.
pub trait AddGrpcServices {
    /// Add every server before this node, then this node's server.
    fn add_services<L>(self, router: Router<L>) -> Router<L>;
}

impl AddGrpcServices for () {
    fn add_services<L>(self, router: Router<L>) -> Router<L> {
        router
    }
}

impl<Server: RoutableService, Chain: AddGrpcServices> AddGrpcServices
    for GrpcServiceRegisterCenter<Server, Chain>
{
    fn add_services<L>(self, router: Router<L>) -> Router<L> {
        self.chain.add_services(router).add_service(self.head)
    }
}
