//! Ante core: named, durable work queues on SQLite.
//!
//! Delivery contract: at-least-once. A message is removed only after it has
//! been acknowledged. In-flight messages whose lease expired, or whose owner
//! vanished, are returned to the ready queue.

mod error;
mod store;

pub use error::{Error, Result};
pub use store::{
    AckOutcome, Ante, AttemptInfo, Completion, Delivery, DeliveryToken, MessageState,
    MessageStatus, NewMessage, Options, QueueInfo, RequeueOutcome, Settlement, TouchOutcome,
    DEFAULT_HISTORY_MAX_ROWS, DEFAULT_HISTORY_RETENTION_MS, DEFAULT_LEASE_MS,
};

pub const SCHEMA_VERSION: u64 = 2;
