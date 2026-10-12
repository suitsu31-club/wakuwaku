//! Building a service's components from shared dependencies.
//!
//! [`ServiceBuilder`](builder::ServiceBuilder) holds the shared
//! dependencies. Each kind of component has its own register center, a
//! type-level list filled straight from the service builder in one of two
//! ways: from one registered dependency, for components that implement
//! [`ServiceCreation`] (`cron_job`, `grpc_service`, `iggy_consumer`), or by
//! a closure that borrows the service builder and returns the finished
//! component (`compose_cron_job`, `compose_grpc_service`,
//! `compose_iggy_consumer`), for components built from several services or
//! by hand. Both kinds of registration chain in any order.
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
/// The dependency is looked up by its type. A component that needs several
/// dependencies, or that is assembled by hand, is registered with the
/// `compose_*` methods instead, which need no `ServiceCreation` impl.
pub trait ServiceCreation {
    /// The registered service this one is built from. It is cloned out of
    /// the builder, so it should be cheap to clone (a handle, `Arc`, or
    /// pool).
    type Dep: Clone;

    /// Build the service from its dependency.
    fn create_service(dep: Self::Dep) -> Self;
}
