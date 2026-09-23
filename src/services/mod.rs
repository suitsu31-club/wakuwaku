//! Service composition helpers: a type-indexed service registry and, with the
//! `amqprs` feature, grouped RabbitMQ consumer setup.

pub mod builder;

#[cfg(feature = "amqprs")]
pub mod amqp_consumer;