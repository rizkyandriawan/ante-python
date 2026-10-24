//! Ante core: named, durable work queues on SQLite.
//!
//! Delivery contract: at-least-once. A message is removed only after it has
//! been acknowledged. In-flight messages whose lease expired, or whose owner
//! vanished, are returned to the ready queue.

mod error;
mod store;

pub use error::{Error, Result};
pub use store::{
    AckOutcome, Ante, AttemptInfo, Delivery, DeliveryToken, MessageState, MessageStatus,
    NewMessage, Options, QueueInfo, RequeueOutcome, DEFAULT_LEASE_MS,
};

pub const SCHEMA_VERSION: u64 = 2;
