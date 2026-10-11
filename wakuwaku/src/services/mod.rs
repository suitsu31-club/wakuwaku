//! Building a service's components from shared dependencies.
//!
//! [`ServiceBuilder`](builder::ServiceBuilder) holds the shared
//! dependencies, and every component that implements [`ServiceCreation`] is
//! built from one of them. Each kind of component has its own register
//! center, a type-level list filled straight from the service builder:
//!
//! | Component     | Trait                          | Module            | Feature |
//! |---------------|--------------------------------|-------------------|---------|
//! | Cron job      | [`CronJob`](cron_job::CronJob) | [`cron_job`]      |         |
//! | gRPC server   | `grpc_service::RoutableService`| `grpc_service`    | `grpc`  |
//! | Event handler | `wakuwaku_iggy::EventHandler`  | `iggy_consumer`   | `iggy`  |

pub mod builder;
pub mod cron_job;

#[cfg(feature = "grpc")]
pub mod grpc_service;

#[cfg(feature = "iggy")]
pub mod iggy_consumer;

/// A component that can be constructed from a single dependency.
///
/// [`ServiceBuilder::cron_job`](builder::ServiceBuilder::cron_job),
/// `ServiceBuilder::grpc_service` (feature `grpc`) and
/// `ServiceBuilder::iggy_consumer` (feature `iggy`) use it to create
/// components from services already registered in a
/// [`ServiceBuilder`](builder::ServiceBuilder).
///
/// The dependency is looked up by its type. If a component needs several
/// dependencies, register a struct (or tuple) that holds them all and use
/// that as `Dep`.
pub trait ServiceCreation {
    /// The registered service this one is built from. It is cloned out of
    /// the builder, so it should be cheap to clone (a handle, `Arc`, or
    /// pool).
    type Dep: Clone;

    /// Build the service from its dependency.
    fn create_service(dep: Self::Dep) -> Self;
}
