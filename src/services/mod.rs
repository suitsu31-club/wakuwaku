//! Service composition helpers: a type-indexed service registry, the
//! [`ServiceCreation`] trait for building services from registered
//! dependencies, and, with the `amqprs` feature, grouped RabbitMQ consumer
//! setup.

pub mod builder;

#[cfg(feature = "amqprs")]
pub mod amqp_consumer;

/// A service that can be constructed from a single dependency.
///
/// With the `amqprs` feature, `ServiceBuilder::amqp_consumer` uses it to
/// create consumers from services already registered in a
/// [`ServiceBuilder`](builder::ServiceBuilder).
///
/// The dependency is looked up by its type. If a service needs several
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