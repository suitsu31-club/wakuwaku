//! Building Iggy event handlers from a [`ServiceBuilder`].
//!
//! Event handlers usually need shared dependencies such as a database pool
//! or an HTTP client. Register the dependencies in a service builder and
//! build the handlers from it with one of:
//!
//! - [`ServiceBuilder::iggy_consumer`], for a handler that implements
//!   [`ServiceCreation`]: it is created from one registered dependency,
//!   which is cloned out of the service builder, and wrapped in an [`Arc`].
//! - [`ServiceBuilder::compose_iggy_consumer`], for any other handler: a
//!   closure borrows the service builder, takes whatever it needs from it
//!   (or nothing, for a handler set up beforehand), and returns the handler
//!   in an [`Arc`]. The handler needs no `ServiceCreation` impl and is not
//!   registered in the service builder.
//!
//! Both return an [`IggyConsumerRegisterCenterBuilder`], which chains more
//! [`iggy_consumer`](IggyConsumerRegisterCenterBuilder::iggy_consumer) and
//! [`compose_iggy_consumer`](IggyConsumerRegisterCenterBuilder::compose_iggy_consumer)
//! calls in any order and ends with either:
//!
//! - [`start`](IggyConsumerRegisterCenterBuilder::start), which starts
//!   consuming and gives back the service builder together with the
//!   [`ConsumerRuntime`], or
//! - [`into_parts`](IggyConsumerRegisterCenterBuilder::into_parts), which
//!   returns the service builder and the unstarted
//!   [`IggyConsumerRegisterCenter`], so you can start it later.
//!
//! Each handler is created right away, when it is registered. See the
//! [`wakuwaku_iggy`] documentation for how events are consumed.
//!
//! # Example
//!
//! ```no_run
//! use std::hash::DefaultHasher;
//! use std::num::NonZeroUsize;
//! use std::sync::Arc;
//! use wakuwaku::deps::iggy::prelude::{Client, IggyClient};
//! use wakuwaku::deps::kanau::message::{MessageDe, MessageSer};
//! use wakuwaku::iggy::error::HandleError;
//! use wakuwaku::iggy::{
//!     ConsumerConfig, Event, EventAtomicOrdering, EventBody, EventHandler, EventTypeTag,
//!     IggySuperPartitionName, NonAssociative, PartitionKey,
//! };
//! use wakuwaku::services::ServiceCreation;
//! use wakuwaku::services::builder::ServiceBuilder;
//!
//! #[derive(Hash)]
//! struct UserId(u64);
//!
//! impl PartitionKey for UserId {
//!     type PartitionMerge = DefaultHasher;
//!     const SUPER_PARTITION_NAME: IggySuperPartitionName = IggySuperPartitionName("users");
//! }
//!
//! struct UserRegistered { user: u64 }
//!
//! impl MessageSer for UserRegistered {
//!     type SerError = anyhow::Error;
//!     fn to_bytes(self) -> Result<Box<[u8]>, Self::SerError> {
//!         Ok(Box::new(self.user.to_le_bytes()))
//!     }
//! }
//!
//! impl MessageDe for UserRegistered {
//!     type DeError = anyhow::Error;
//!     fn from_bytes(bytes: &[u8]) -> Result<Self, Self::DeError> {
//!         Ok(Self { user: u64::from_le_bytes(bytes.try_into()?) })
//!     }
//! }
//!
//! impl EventBody for UserRegistered {}
//!
//! impl Event for UserRegistered {
//!     const TYPE_TAG: EventTypeTag = EventTypeTag::new(1);
//!     type Key = UserId;
//!     type Algebra = NonAssociative;
//!     fn key(&self) -> UserId { UserId(self.user) }
//!     fn atomic_ordering(&self) -> EventAtomicOrdering { EventAtomicOrdering::Relaxed }
//! }
//!
//! /// Shared dependency, cheap to clone.
//! #[derive(Clone)]
//! struct Mailer;
//!
//! struct SendWelcomeMail { mailer: Mailer }
//!
//! impl ServiceCreation for SendWelcomeMail {
//!     type Dep = Mailer;
//!     fn create_service(mailer: Mailer) -> Self { Self { mailer } }
//! }
//!
//! impl EventHandler<UserRegistered> for SendWelcomeMail {
//!     const MAX_BATCH: NonZeroUsize = NonZeroUsize::MIN;
//!     async fn handle(&self, run: &[UserRegistered]) -> Result<(), HandleError> {
//!         for event in run {
//!             println!("welcome, user {}", event.user);
//!         }
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Arc::new(IggyClient::from_connection_string(
//!         "iggy://iggy:iggy@localhost:8090",
//!     )?);
//!     client.connect().await?;
//!
//!     // Type parameters: <Handler, Event, Position>. Leave the event and the
//!     // dependency's position as `_` to have them inferred.
//!     let (services, consumers) = ServiceBuilder::new(Mailer)
//!         .iggy_consumer::<SendWelcomeMail, _, _>()
//!         .start(client, ConsumerConfig::new("shop", "mailer", vec![1]))
//!         .await?;
//!
//!     let _mailer: &Mailer = services.provide::<Mailer, _>();
//!     tokio::signal::ctrl_c().await?;
//!     consumers.shutdown().await;
//!     Ok(())
//! }
//! ```
//!
//! The event must be named when a handler handles more than one event, e.g.
//! `.iggy_consumer::<MyHandler, MyEvent, _>()`. The position must be named
//! with [`Here`](super::builder::Here) / [`There`](super::builder::There)
//! when the dependency's type is registered more than once.
//!
//! A handler that needs several registered services, or that is set up by
//! hand, is registered with
//! [`compose_iggy_consumer`](ServiceBuilder::compose_iggy_consumer) instead,
//! e.g. `.compose_iggy_consumer(|s| Arc::new(AuditLog { db:
//! s.provide::<Db, _>().clone(), http }))`; it needs no [`ServiceCreation`]
//! impl.

use crate::services::ServiceCreation;
use crate::services::builder::{ProvideAt, ServiceBuilder, ServiceBuilderPosition};
use iggy::prelude::IggyClient;
use std::sync::Arc;
use wakuwaku_iggy::consumer::handler::HandlerList;
use wakuwaku_iggy::error::Error;
use wakuwaku_iggy::{
    ConsumerConfig, ConsumerRuntime, Event, EventHandler, IggyConsumerRegisterCenter,
};

/// Registers Iggy event handlers built from a [`ServiceBuilder`].
///
/// Pairs a service builder (`SHead`, `SChain`) with an
/// [`IggyConsumerRegisterCenter`] (`Handler`, `Ev`, `IChain`) that is being
/// filled from it. Get one from [`ServiceBuilder::iggy_consumer`] or
/// [`ServiceBuilder::compose_iggy_consumer`], register more handlers with
/// [`iggy_consumer`](Self::iggy_consumer) or
/// [`compose_iggy_consumer`](Self::compose_iggy_consumer), then finish with
/// [`start`](Self::start) or [`into_parts`](Self::into_parts). See the
/// [module documentation](self) for an example.
pub struct IggyConsumerRegisterCenterBuilder<SHead, SChain, Handler, Ev, IChain>
where
    Handler: EventHandler<Ev>,
    Ev: Event,
{
    /// The service builder that handler dependencies are taken from.
    pub builder: ServiceBuilder<SHead, SChain>,
    register_center: IggyConsumerRegisterCenter<Handler, Ev, IChain>,
}

impl<SH, SC, H, E, IC> IggyConsumerRegisterCenterBuilder<SH, SC, H, E, IC>
where
    H: EventHandler<E>,
    E: Event,
{
    /// Pair an existing service builder with an existing handler list.
    ///
    /// Usually you don't need this: [`ServiceBuilder::iggy_consumer`]
    /// creates the builder for you.
    pub fn new(
        builder: ServiceBuilder<SH, SC>,
        register_center: IggyConsumerRegisterCenter<H, E, IC>,
    ) -> Self {
        Self {
            builder,
            register_center,
        }
    }

    /// Create `NewHandler` from its dependency and register it.
    ///
    /// The dependency, [`NewHandler::Dep`](ServiceCreation::Dep), is looked
    /// up in [`builder`](Self::builder) at `Position` and cloned. The handler
    /// is created with [`create_service`](ServiceCreation::create_service),
    /// wrapped in an [`Arc`], and pushed onto the list the same way as
    /// [`IggyConsumerRegisterCenter::push`].
    ///
    /// `NewEvent` and `Position` can usually be passed as `_`. `NewEvent` is
    /// inferred when `NewHandler` implements [`EventHandler`] for only one
    /// event. `Position` is inferred when exactly one service of type
    /// `NewHandler::Dep` is registered. A dependency that was never
    /// registered is a compile error.
    pub fn iggy_consumer<NewHandler, NewEvent, Position>(
        self,
    ) -> IggyConsumerRegisterCenterBuilder<
        SH,
        SC,
        NewHandler,
        NewEvent,
        IggyConsumerRegisterCenter<H, E, IC>,
    >
    where
        NewHandler: EventHandler<NewEvent> + ServiceCreation,
        NewEvent: Event,
        Position: ServiceBuilderPosition,
        ServiceBuilder<SH, SC>: ProvideAt<NewHandler::Dep, Position>,
    {
        let dep = self.builder.provide::<NewHandler::Dep, Position>().clone();
        let handler = Arc::new(NewHandler::create_service(dep));
        IggyConsumerRegisterCenterBuilder {
            builder: self.builder,
            register_center: self.register_center.push(handler),
        }
    }

    /// Build `NewHandler` from any registered services and register it.
    ///
    /// `composer` borrows [`builder`](Self::builder), so it can
    /// [`provide`](ServiceBuilder::provide) as many registered services as it
    /// needs, or ignore it and move in a handler that was built by hand. It
    /// returns the handler in the [`Arc`] the list stores. The handler needs
    /// no [`ServiceCreation`] impl and is pushed onto the list the same way as
    /// [`IggyConsumerRegisterCenter::push`].
    ///
    /// `NewHandler` and `NewEvent` are inferred from the returned handler
    /// when it implements [`EventHandler`] for only one event; otherwise name
    /// the event, e.g. `.compose_iggy_consumer::<_, MyEvent>(…)`.
    pub fn compose_iggy_consumer<NewHandler, NewEvent>(
        self,
        composer: impl FnOnce(&ServiceBuilder<SH, SC>) -> Arc<NewHandler>,
    ) -> IggyConsumerRegisterCenterBuilder<
        SH,
        SC,
        NewHandler,
        NewEvent,
        IggyConsumerRegisterCenter<H, E, IC>,
    >
    where
        NewHandler: EventHandler<NewEvent>,
        NewEvent: Event,
    {
        let handler = composer(&self.builder);
        IggyConsumerRegisterCenterBuilder {
            builder: self.builder,
            register_center: self.register_center.push(handler),
        }
    }

    /// Split into the service builder and the unstarted handler list.
    pub fn into_parts(self) -> (ServiceBuilder<SH, SC>, IggyConsumerRegisterCenter<H, E, IC>) {
        (self.builder, self.register_center)
    }
}

impl<SH, SC, H, E, IC> IggyConsumerRegisterCenterBuilder<SH, SC, H, E, IC>
where
    H: EventHandler<E>,
    E: Event,
    IggyConsumerRegisterCenter<H, E, IC>: HandlerList,
{
    /// Start consuming and return the service builder together with the
    /// running consumer.
    ///
    /// This is [`into_parts`](Self::into_parts) followed by
    /// [`IggyConsumerRegisterCenter::start`].
    ///
    /// # Errors
    ///
    /// Same as [`IggyConsumerRegisterCenter::start`]. On error the service
    /// builder is dropped along with the handlers.
    pub async fn start(
        self,
        client: Arc<IggyClient>,
        config: ConsumerConfig,
    ) -> Result<(ServiceBuilder<SH, SC>, ConsumerRuntime), Error> {
        let runtime = self.register_center.start(client, config).await?;
        Ok((self.builder, runtime))
    }
}
