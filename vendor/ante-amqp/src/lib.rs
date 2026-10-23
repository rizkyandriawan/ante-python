//! AMQP 0-9-1 compatibility adapter for Ante.
//!
//! Supported profile: durable queues, default exchange, publish with
//! publisher confirms, consume/get with manual acknowledgements, reject/nack
//! with requeue, QoS prefetch, purge, delete, and channel/connection lifecycle.
//! Named exchanges and broker transactions are intentionally out of scope.

pub mod frame;
pub mod server;

pub use server::{serve, serve_until, serve_until_with_options, ServerOptions};
