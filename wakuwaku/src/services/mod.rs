//! Building a service's components from shared dependencies.
//!
//! [`ServiceBuilder`](builder::ServiceBuilder) holds the shared
//! dependencies. Each kind of component has a register center, a type-level
//! list filled from the service builder either from one registered
//! dependency through [`ServiceCreation`], or by a `compose_*` closure that
//! borrows the service builder.
//!
//! | Component     | Trait                          | Module            | Feature |
//! |---------------|--------------------------------|-------------------|---------|
//! | Cron job      | [`CronJob`](cron_job::CronJob) | [`cron_job`]      |         |
//! | gRPC server   | `grpc_service::RoutableService`| `grpc_service`    | `grpc`  |
//! | Event handler | `wakuwaku_iggy::EventHandler`  | `iggy_consumer`   | `iggy`  |
//!
//! See the [service builder guide](https://www.wakuwaku.rs/en/docs/core/service-builder).

pub mod builder;
pub mod cron_job;

#[cfg(feature = "grpc")]
pub mod grpc_service;

#[cfg(feature = "iggy")]
pub mod iggy_consumer;

/// A component that can be constructed from a single dependency.
///
/// [`ServiceBuilder::cron_job`](builder::ServiceBuilder::cron_job),
/// [`construct_subservice`](builder::ServiceBuilder::construct_subservice),
/// `grpc_service` (feature `grpc`) and `iggy_consumer` (feature `iggy`) look
/// [`Dep`](Self::Dep) up by its type in a
/// [`ServiceBuilder`](builder::ServiceBuilder), clone it, and pass the clone
/// to [`create_service`](Self::create_service). Components that need several
/// dependencies are registered with the `compose_*` methods instead, which
/// need no `ServiceCreation` impl.
pub trait ServiceCreation {
    /// The registered service this one is built from. It is cloned out of
    /// the builder, so it should be cheap to clone (a handle, `Arc`, or
    /// pool).
    type Dep: Clone;

    /// Build the service from its dependency.
    fn create_service(dep: Self::Dep) -> Self;
}
