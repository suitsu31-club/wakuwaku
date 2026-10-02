//! Registering a group of tonic gRPC services together.
//!
//! [`GrpcServiceRegisterCenter`] collects gRPC servers in a type-level linked
//! list, the same way [`ServiceBuilder`] does.
//! Every node holds one server (a tonic-generated `XxxServer<T>`, or any other
//! [`RoutableService`]), so a single chain can mix different servers. There is
//! no boxing and no dynamic dispatch.
//!
//! [`into_router_fn`](GrpcServiceRegisterCenter::into_router_fn) turns the
//! chain into an `impl FnOnce(Router<L>) -> Router<L>`. Calling it adds every
//! server to the router with [`Router::add_service`], in registration order.
//! The router's layer stack `L` is not touched, so middleware configured on
//! [`Server`](tonic::transport::Server) before the router was created applies
//! to every registered server.
//!
//! # Example
//!
//! ```no_run
//! use tonic::service::Routes;
//! use tonic::transport::Server;
//! use wakuwaku::services::grpc_service::GrpcServiceRegisterCenter;
//! # use std::convert::Infallible;
//! # use std::task::{Context, Poll};
//! # use tonic::body::Body;
//! # use tonic::codegen::{BoxFuture, Service, http};
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
//! # server!(AdminPersonalServer, AdminPersonal, "manage.AdminPersonal");
//! # server!(UserAuthServer, UserAuth, "auth.UserAuth");
//! # struct AdminAuthServiceImpl;
//! # impl AdminAuthService for AdminAuthServiceImpl {}
//! # struct AdminPersonalServiceImpl;
//! # impl AdminPersonal for AdminPersonalServiceImpl {}
//! # struct UserAuthServiceImpl;
//! # impl UserAuth for UserAuthServiceImpl {}
//!
//! fn check_request(req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
//!     Ok(req)
//! }
//!
//! async fn serve() -> Result<(), Box<dyn std::error::Error>> {
//!     let register = GrpcServiceRegisterCenter::new(AdminAuthServiceServer::new(AdminAuthServiceImpl))
//!         .push(AdminPersonalServer::new(AdminPersonalServiceImpl))
//!         .push(UserAuthServer::new(UserAuthServiceImpl))
//!         .into_router_fn();
//!
//!     // Layers go on the `Server` before the router exists; `register` keeps
//!     // them. `add_routes(Routes::default())` starts from an empty router.
//!     let router = Server::builder()
//!         .layer(tonic::service::InterceptorLayer::new(
//!             check_request as fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
//!         ))
//!         .add_routes(Routes::default());
//!
//!     register(router).serve("[::1]:50051".parse()?).await?;
//!     Ok(())
//! }
//! ```
//!
//! Two servers with the same [`NamedService::NAME`] cannot be served by one
//! router: [`Router::add_service`] panics on the duplicate route when the
//! returned closure is called.
//!
//! # Building servers from a [`ServiceBuilder`]
//!
//! Service implementations usually need shared dependencies such as a
//! database pool, a Redis connection, or an audit layer. Instead of building
//! each server by hand, implement [`ServiceCreation`] for the service
//! implementation and let [`ServiceBuilder::grpc_service`] build it from a
//! dependency that is already registered in the service builder, then wrap it
//! into its server with the constructor you pass in (for tonic-generated
//! servers, `XxxServer::new`).
//!
//! `ServiceBuilder::grpc_service` returns a
//! [`GrpcServiceRegisterCenterBuilder`], which chains more
//! [`grpc_service`](GrpcServiceRegisterCenterBuilder::grpc_service) calls and
//! ends with either:
//!
//! - [`into_router_fn`](GrpcServiceRegisterCenterBuilder::into_router_fn),
//!   which gives back the service builder together with the
//!   `impl FnOnce(Router<L>) -> Router<L>`, or
//! - [`into_parts`](GrpcServiceRegisterCenterBuilder::into_parts), which
//!   returns the service builder and the [`GrpcServiceRegisterCenter`] so you
//!   can [`push`](GrpcServiceRegisterCenter::push) servers that are built by
//!   hand.
//!
//! Each server is created right away, when `grpc_service` is called. Its
//! implementation's [`Dep`](ServiceCreation::Dep) is cloned out of the
//! service builder.
//!
//! ```no_run
//! use tonic::service::Routes;
//! use tonic::transport::Server;
//! use wakuwaku::services::ServiceCreation;
//! use wakuwaku::services::builder::ServiceBuilder;
//! # use std::convert::Infallible;
//! # use std::task::{Context, Poll};
//! # use tonic::body::Body;
//! # use tonic::codegen::{BoxFuture, Service, http};
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
//! # server!(AdminPersonalServer, AdminPersonal, "manage.AdminPersonal");
//! # server!(AccountManageServer, AccountManage, "manage.AccountManage");
//! # server!(UserAuthServer, UserAuth, "auth.UserAuth");
//! # impl AdminAuthService for AdminAuthServiceImpl {}
//! # impl AdminPersonal for AdminPersonalServiceImpl {}
//! # impl AccountManage for AdminManageServiceImpl {}
//! # impl UserAuth for UserAuthServiceImpl {}
//!
//! // Shared dependencies. They are cloned into each service, so keep them
//! // cheap to clone (handles, `Arc`s, pools).
//! #[derive(Clone)]
//! struct AdminStore;
//! #[derive(Clone)]
//! struct AuditLayer;
//! /// Several dependencies are grouped into one registered value.
//! #[derive(Clone)]
//! struct AuditedAdminStore { store: AdminStore, audit: AuditLayer }
//!
//! struct AdminAuthServiceImpl { store: AdminStore }
//! impl ServiceCreation for AdminAuthServiceImpl {
//!     type Dep = AdminStore;
//!     fn create_service(store: AdminStore) -> Self { Self { store } }
//! }
//!
//! struct AdminPersonalServiceImpl { store: AdminStore }
//! impl ServiceCreation for AdminPersonalServiceImpl {
//!     type Dep = AdminStore;
//!     fn create_service(store: AdminStore) -> Self { Self { store } }
//! }
//!
//! struct AdminManageServiceImpl { store: AdminStore, audit: AuditLayer }
//! impl ServiceCreation for AdminManageServiceImpl {
//!     type Dep = AuditedAdminStore;
//!     fn create_service(dep: AuditedAdminStore) -> Self {
//!         Self { store: dep.store, audit: dep.audit }
//!     }
//! }
//!
//! // Needs more than one registered service, so it is built by hand.
//! struct UserAuthServiceImpl { store: AdminStore, audit: AuditLayer }
//!
//! async fn serve() -> Result<(), Box<dyn std::error::Error>> {
//!     let store = AdminStore;
//!     let audit = AuditLayer;
//!
//!     // Type parameters: <Server, ServiceImpl, Position>. The server's own
//!     // type parameter and the dependency's position are inferred with `_`.
//!     // The argument turns the created implementation into its server.
//!     let (services, register) = ServiceBuilder::new(store.clone())
//!         .push(audit.clone())
//!         .push(AuditedAdminStore { store, audit })
//!         .grpc_service::<AdminAuthServiceServer<_>, AdminAuthServiceImpl, _>(AdminAuthServiceServer::new)
//!         .grpc_service::<AdminPersonalServer<_>, AdminPersonalServiceImpl, _>(AdminPersonalServer::new)
//!         .grpc_service::<AccountManageServer<_>, AdminManageServiceImpl, _>(AccountManageServer::new)
//!         .into_parts();
//!
//!     let user_auth = UserAuthServiceImpl {
//!         store: services.provide::<AdminStore, _>().clone(),
//!         audit: services.provide::<AuditLayer, _>().clone(),
//!     };
//!     let register = register
//!         .push(UserAuthServer::new(user_auth))
//!         .into_router_fn();
//!
//!     let router = Server::builder().add_routes(Routes::default());
//!     register(router).serve("[::1]:50051".parse()?).await?;
//!     Ok(())
//! }
//! ```
//!
//! The position must be named with [`Here`](super::builder::Here) /
//! [`There`](super::builder::There) when the dependency's type is registered
//! more than once.

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

/// Registers [`ServiceCreation`] implementations built from a
/// [`ServiceBuilder`] as gRPC servers.
///
/// Pairs a service builder (`SHead`, `SChain`) with a
/// [`GrpcServiceRegisterCenter`] (`Server`, `GChain`) that is being filled
/// from it. Get one from [`ServiceBuilder::grpc_service`], register more
/// servers with [`grpc_service`](Self::grpc_service), then finish with
/// [`into_router_fn`](Self::into_router_fn) or [`into_parts`](Self::into_parts).
/// See the [module documentation](self#building-servers-from-a-servicebuilder)
/// for an example.
pub struct GrpcServiceRegisterCenterBuilder<SHead, SChain, Server, GChain> {
    /// The service builder that service dependencies are taken from.
    pub builder: ServiceBuilder<SHead, SChain>,
    register_center: GrpcServiceRegisterCenter<Server, GChain>,
}

impl<SH, SC, S, GC> GrpcServiceRegisterCenterBuilder<SH, SC, S, GC> {
    /// Pair an existing service builder with an existing server list.
    ///
    /// Usually you don't need this: [`ServiceBuilder::grpc_service`] creates
    /// the builder for you.
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

    /// Split into the service builder and the server list.
    ///
    /// Use this to keep adding servers that are built by hand with
    /// [`GrpcServiceRegisterCenter::push`].
    pub fn into_parts(self) -> (ServiceBuilder<SH, SC>, GrpcServiceRegisterCenter<S, GC>) {
        (self.builder, self.register_center)
    }

    /// Return the service builder together with a function that adds every
    /// registered server to a router.
    ///
    /// This is [`into_parts`](Self::into_parts) followed by
    /// [`GrpcServiceRegisterCenter::into_router_fn`].
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
    /// router, in registration order.
    ///
    /// The router's layer stack `L` is inferred from the router the function
    /// is called with.
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
