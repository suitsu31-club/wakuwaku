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
//! use wakuwaku::service_register::ServiceBuilder;
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

#[cfg(feature = "amqprs")]
use crate::integration::amqp::{AmqpMessageProcessor, AmqpMessageSend};

#[cfg(feature = "amqprs")]
use crate::services::ServiceCreation;
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

    /// Create an AMQP consumer from a registered dependency and start a
    /// consumer list.
    ///
    /// `Consumer`'s [`Dep`](ServiceCreation::Dep) is looked up at `Position`
    /// and cloned. The consumer is then created with
    /// [`create_service`](ServiceCreation::create_service), wrapped in an
    /// `Arc`, and becomes the first entry of a new
    /// [`AmqpConsumerRegisterCenter`](super::amqp_consumer::AmqpConsumerRegisterCenter).
    /// The returned builder takes ownership of `self`. Chain more
    /// `amqp_consumer` calls on it, then call `setup` or `into_parts`.
    ///
    /// `Event` and `Position` can usually be passed as `_`. The event is
    /// inferred when `Consumer` implements [`AmqpMessageProcessor`] for only
    /// one event. The position is inferred when the dependency's type is
    /// registered exactly once. See the
    /// [`amqp_consumer` module documentation](super::amqp_consumer#building-consumers-from-a-servicebuilder)
    /// for an example.
    #[cfg(feature = "amqprs")]
    pub fn amqp_consumer<Consumer, Event, Position>(
        self,
    ) -> super::amqp_consumer::AmqpConsumerRegisterCenterBuilder<Head, Chain, Consumer, Event, ()>
    where
        Position: ServiceBuilderPosition,
        Consumer: AmqpMessageProcessor<Event> + ServiceCreation,
        Event: AmqpMessageSend + kanau::message::MessageDe,
        Self: ProvideAt<Consumer::Dep, Position>,
    {
        let service_dep: Consumer::Dep = self.provide::<Consumer::Dep, Position>().clone();
        let new_service = std::sync::Arc::new(Consumer::create_service(service_dep));
        let register_center = super::amqp_consumer::AmqpConsumerRegisterCenter::new(new_service);
        super::amqp_consumer::AmqpConsumerRegisterCenterBuilder::new(self, register_center)
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
