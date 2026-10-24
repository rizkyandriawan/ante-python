use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

use crate::{Error, Result, SCHEMA_VERSION};

const META_NEXT_QUEUE: &str = "next_queue";
const META_NEXT_MESSAGE: &str = "next_message";
const META_NEXT_DELIVERY: &str = "next_delivery";
const META_SCHEMA: &str = "schema_version";
const APPLICATION_ID: i64 = 0x414E_5445;
const READER_CONNECTIONS: usize = 2;
const BUSY_TIMEOUT_MS: u64 = 1_000;

const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS metadata (
    key TEXT PRIMARY KEY NOT NULL,
    value INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS queues (
    id INTEGER PRIMARY KEY NOT NULL,
    name TEXT NOT NULL UNIQUE,
    next_ready_seq INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS entries (
    entry_id INTEGER PRIMARY KEY NOT NULL,
    queue_id INTEGER NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    ready_seq INTEGER NOT NULL,
    payload BLOB NOT NULL,
    content_type TEXT,
    application_message_id TEXT,
    correlation_id TEXT,
    reply_to TEXT,
    state TEXT NOT NULL CHECK (state IN ('ready', 'inflight')),
    delivery_id INTEGER,
    lease_deadline_ms INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    UNIQUE (queue_id, ready_seq),
    CHECK (
        (state = 'ready' AND delivery_id IS NULL AND lease_deadline_ms IS NULL)
        OR
        (state = 'inflight' AND delivery_id IS NOT NULL AND lease_deadline_ms IS NOT NULL)
    )
);
CREATE TABLE IF NOT EXISTS delivery_attempts (
    delivery_id INTEGER PRIMARY KEY NOT NULL,
    entry_id INTEGER NOT NULL,
    queue_id INTEGER NOT NULL,
    queue_name TEXT NOT NULL,
    message_created_at_ms INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN (
        'inflight', 'acked', 'requeued', 'expired', 'recovered', 'discarded'
    ))
);
CREATE INDEX IF NOT EXISTS attempts_entry
    ON delivery_attempts(entry_id, attempt);
CREATE INDEX IF NOT EXISTS attempts_queue
    ON delivery_attempts(queue_id, started_at_ms);
CREATE INDEX IF NOT EXISTS attempts_finished
    ON delivery_attempts(finished_at_ms, delivery_id);
CREATE INDEX IF NOT EXISTS entries_ready
    ON entries(queue_id, ready_seq) WHERE state = 'ready';
CREATE INDEX IF NOT EXISTS entries_inflight
    ON entries(queue_id) WHERE state = 'inflight';
CREATE INDEX IF NOT EXISTS entries_deadline
    ON entries(lease_deadline_ms) WHERE state = 'inflight';
CREATE UNIQUE INDEX IF NOT EXISTS entries_delivery
    ON entries(delivery_id) WHERE delivery_id IS NOT NULL;
"#;

const ATTEMPTS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS delivery_attempts (
    delivery_id INTEGER PRIMARY KEY NOT NULL,
    entry_id INTEGER NOT NULL,
    queue_id INTEGER NOT NULL,
    queue_name TEXT NOT NULL,
    message_created_at_ms INTEGER NOT NULL,
    attempt INTEGER NOT NULL,
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER,
    outcome TEXT NOT NULL CHECK (outcome IN (
        'inflight', 'acked', 'requeued', 'expired', 'recovered', 'discarded'
    ))
);
CREATE INDEX IF NOT EXISTS attempts_entry
    ON delivery_attempts(entry_id, attempt);
CREATE INDEX IF NOT EXISTS attempts_queue
    ON delivery_attempts(queue_id, started_at_ms);
CREATE INDEX IF NOT EXISTS attempts_finished
    ON delivery_attempts(finished_at_ms, delivery_id);
"#;

pub const DEFAULT_LEASE_MS: u64 = 30_000;
pub const DEFAULT_HISTORY_RETENTION_MS: u64 = 7 * 24 * 60 * 60 * 1000;
pub const DEFAULT_HISTORY_MAX_ROWS: usize = 1_000_000;
const HISTORY_PRUNE_BATCH: usize = 10_000;

#[derive(Debug, Clone)]
pub struct Options {
    pub cache_size: usize,
    pub max_payload_bytes: usize,
    pub max_batch_messages: usize,
    pub max_batch_bytes: usize,
    pub lease_ms: u64,
    /// Drop finished attempt rows older than this. Zero disables age pruning.
    pub history_retention_ms: u64,
    /// Keep at most this many finished attempt rows. Zero disables the cap.
    pub history_max_rows: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            cache_size: 64 * 1024 * 1024,
            max_payload_bytes: 8 * 1024 * 1024,
            max_batch_messages: 256,
            max_batch_bytes: 1024 * 1024,
            lease_ms: DEFAULT_LEASE_MS,
            history_retention_ms: DEFAULT_HISTORY_RETENTION_MS,
            history_max_rows: DEFAULT_HISTORY_MAX_ROWS,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct NewMessage {
    pub payload: Vec<u8>,
    pub content_type: Option<String>,
    pub message_id: Option<String>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryToken {
    pub message_id: u64,
    pub delivery_id: u64,
}

impl DeliveryToken {
    pub fn to_wire(&self) -> String {
        format!("{}:{}", self.message_id, self.delivery_id)
    }

    pub fn parse(value: &str) -> Result<Self> {
        let (message_id, delivery_id) = value
            .split_once(':')
            .ok_or_else(|| Error::Invalid("malformed delivery token".into()))?;
        Ok(DeliveryToken {
            message_id: message_id
                .parse()
                .map_err(|_| Error::Invalid("malformed delivery token".into()))?,
            delivery_id: delivery_id
                .parse()
                .map_err(|_| Error::Invalid("malformed delivery token".into()))?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Delivery {
    pub token: DeliveryToken,
    pub queue: String,
    pub queue_id: u64,
    pub message_id: u64,
    pub payload: Vec<u8>,
    pub content_type: Option<String>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub application_message_id: Option<String>,
    pub attempts: u64,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    Acked,
    Stale,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequeueOutcome {
    Requeued,
    Stale,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TouchOutcome {
    Extended,
    Stale,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Settlement {
    Ack,
    Discard,
}

impl Settlement {
    fn outcome(self) -> MessageState {
        match self {
            Settlement::Ack => MessageState::Acked,
            Settlement::Discard => MessageState::Discarded,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completion {
    pub outcome: AckOutcome,
    pub message_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueInfo {
    pub name: String,
    pub id: u64,
    pub ready: u64,
    pub inflight: u64,
}

impl QueueInfo {
    pub fn total(&self) -> u64 {
        self.ready + self.inflight
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageState {
    Ready,
    Inflight,
    Acked,
    Requeued,
    Expired,
    Recovered,
    Discarded,
}

impl MessageState {
    pub fn as_str(self) -> &'static str {
        match self {
            MessageState::Ready => "ready",
            MessageState::Inflight => "inflight",
            MessageState::Acked => "acked",
            MessageState::Requeued => "requeued",
            MessageState::Expired => "expired",
            MessageState::Recovered => "recovered",
            MessageState::Discarded => "discarded",
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "ready" => Ok(MessageState::Ready),
            "inflight" => Ok(MessageState::Inflight),
            "acked" => Ok(MessageState::Acked),
            "requeued" => Ok(MessageState::Requeued),
            "expired" => Ok(MessageState::Expired),
            "recovered" => Ok(MessageState::Recovered),
            "discarded" => Ok(MessageState::Discarded),
            other => Err(Error::Invalid(format!(
                "unknown message state in storage: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptInfo {
    pub attempt: u64,
    pub delivery_id: u64,
    pub started_at_ms: u64,
    pub finished_at_ms: Option<u64>,
    pub outcome: MessageState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageStatus {
    pub queue: String,
    pub message_id: u64,
    pub state: MessageState,
    pub attempts: u64,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub history: Vec<AttemptInfo>,
}

pub struct Ante {
    writer: Mutex<Connection>,
    readers: Mutex<Vec<Connection>>,
    path: PathBuf,
    options: Options,
    _lock: File,
}

impl fmt::Debug for Ante {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ante")
            .field("path", &self.path)
            .field("options", &self.options)
            .finish()
    }
}

impl Ante {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Options::default())
    }

    pub fn open_with(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let lock = acquire_lock(&path)?;
        reject_foreign_file(&path)?;

        let mut writer = Connection::open(&path)?;
        configure(&writer, writer_cache_kib(&options), true)?;
        initialize_or_validate(&mut writer)?;
        let ante = Ante {
            writer: Mutex::new(writer),
            readers: Mutex::new(Vec::new()),
            path,
            options,
            _lock: lock,
        };
        ante.recover_startup()?;
        ante.prune_history()?;

        let mut readers = Vec::with_capacity(READER_CONNECTIONS);
        for _ in 0..READER_CONNECTIONS {
            let reader = Connection::open(&ante.path)?;
            configure(&reader, reader_cache_kib(&ante.options), false)?;
            reader.execute_batch("PRAGMA query_only = ON;")?;
            readers.push(reader);
        }
        *ante.readers.lock().map_err(|_| Error::Poisoned)? = readers;
        Ok(ante)
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    fn writer(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.writer.lock().map_err(|_| Error::Poisoned)
    }

    fn with_reader<T>(&self, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
        let mut readers = self.readers.lock().map_err(|_| Error::Poisoned)?;
        let reader = match readers.pop() {
            Some(reader) => reader,
            None => {
                let reader = Connection::open(&self.path)?;
                configure(&reader, reader_cache_kib(&self.options), false)?;
                reader.execute_batch("PRAGMA query_only = ON;")?;
                reader
            }
        };
        let result = f(&reader);
        readers.push(reader);
        result
    }

    /// Requeue every in-flight message left behind by a previous process.
    fn recover_startup(&self) -> Result<u64> {
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let recovered = requeue_all_inflight(&tx)?;
        tx.commit()?;
        Ok(recovered)
    }

    pub fn declare_queue(&self, name: &str) -> Result<u64> {
        validate_name(name)?;
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<i64> = tx
            .query_row("SELECT id FROM queues WHERE name = ?1", [name], |row| {
                row.get(0)
            })
            .optional()?;
        let id = match existing {
            Some(id) => from_i64(id)?,
            None => {
                let id = next_meta(&tx, META_NEXT_QUEUE)?;
                tx.execute(
                    "INSERT INTO queues (id, name, next_ready_seq) VALUES (?1, ?2, 1)",
                    params![to_i64(id)?, name],
                )?;
                id
            }
        };
        tx.commit()?;
        Ok(id)
    }

    pub fn queue_exists(&self, name: &str) -> Result<bool> {
        Ok(self.queue_id(name)?.is_some())
    }

    /// Stable identity of a queue. A delete-and-recreate produces a new id, so
    /// long-lived consumers can detect that their queue was replaced.
    pub fn queue_id(&self, name: &str) -> Result<Option<u64>> {
        self.with_reader(|reader| {
            let id: Option<i64> = reader
                .query_row("SELECT id FROM queues WHERE name = ?1", [name], |row| {
                    row.get(0)
                })
                .optional()?;
            id.map(from_i64).transpose()
        })
    }

    pub fn publish(&self, queue: &str, message: NewMessage) -> Result<u64> {
        let mut ids = self.publish_batch(queue, vec![message])?;
        Ok(ids.remove(0))
    }

    pub fn publish_batch(&self, queue: &str, messages: Vec<NewMessage>) -> Result<Vec<u64>> {
        if messages.is_empty() {
            return Ok(Vec::new());
        }
        if messages.len() > self.options.max_batch_messages {
            return Err(Error::BatchTooLarge {
                messages: messages.len(),
                max: self.options.max_batch_messages,
            });
        }
        let total_bytes: usize = messages.iter().map(|m| m.payload.len()).sum();
        if total_bytes > self.options.max_batch_bytes {
            return Err(Error::BatchTooManyBytes {
                bytes: total_bytes,
                max: self.options.max_batch_bytes,
            });
        }
        for message in &messages {
            if message.payload.len() > self.options.max_payload_bytes {
                return Err(Error::PayloadTooLarge {
                    size: message.payload.len(),
                    max: self.options.max_payload_bytes,
                });
            }
        }

        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let queue_id: i64 = tx
            .query_row("SELECT id FROM queues WHERE name = ?1", [queue], |row| {
                row.get(0)
            })
            .optional()?
            .ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;
        let ids = insert_messages_tx(&tx, queue_id, messages, now_ms())?;
        tx.commit()?;
        Ok(ids)
    }

    pub fn take(&self, queue: &str, max: usize, lease_ms: u64) -> Result<Vec<Delivery>> {
        Ok(self
            .take_checked(queue, None, max, lease_ms)?
            .unwrap_or_default())
    }

    /// Take only if the queue still has the expected identity. The identity
    /// check and the lease happen in one write transaction, so deleting and
    /// recreating the queue cannot feed a stale consumer a new message.
    /// `None` means the identity no longer matches (or the queue is gone).
    pub fn take_if_queue_id(
        &self,
        queue: &str,
        queue_id: u64,
        max: usize,
        lease_ms: u64,
    ) -> Result<Option<Vec<Delivery>>> {
        self.take_checked(queue, Some(queue_id), max, lease_ms)
    }

    fn take_checked(
        &self,
        queue: &str,
        expected: Option<u64>,
        max: usize,
        lease_ms: u64,
    ) -> Result<Option<Vec<Delivery>>> {
        if max == 0 {
            return Ok(Some(Vec::new()));
        }
        let lease_ms = if lease_ms == 0 {
            self.options.lease_ms
        } else {
            lease_ms
        };
        let now = now_ms();
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let found: Option<i64> = tx
            .query_row("SELECT id FROM queues WHERE name = ?1", [queue], |row| {
                row.get(0)
            })
            .optional()?;
        let queue_id = match found {
            Some(id) => from_i64(id)?,
            None if expected.is_some() => return Ok(None),
            None => return Err(Error::QueueNotFound(queue.to_string())),
        };
        if expected.is_some_and(|expected| expected != queue_id) {
            return Ok(None);
        }

        requeue_expired_tx(&tx, now)?;

        let picked: Vec<PickedEntry> = {
            let mut statement = tx.prepare(
                "SELECT entry_id, payload, content_type, application_message_id,
                        correlation_id, reply_to, attempts, ready_seq, created_at_ms
                 FROM entries
                 WHERE queue_id = ?1 AND state = 'ready'
                 ORDER BY ready_seq
                 LIMIT ?2",
            )?;
            let rows = statement.query_map(params![to_i64(queue_id)?, max as i64], |row| {
                Ok(PickedEntry {
                    entry_id: row.get(0)?,
                    payload: row.get(1)?,
                    content_type: row.get(2)?,
                    application_message_id: row.get(3)?,
                    correlation_id: row.get(4)?,
                    reply_to: row.get(5)?,
                    attempts: row.get(6)?,
                    _ready_seq: row.get(7)?,
                    created_at_ms: row.get(8)?,
                })
            })?;
            let mut out = Vec::new();
            for row in rows {
                out.push(row?);
            }
            out
        };

        if picked.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let mut next_delivery = next_meta(&tx, META_NEXT_DELIVERY)?;
        let mut deliveries = Vec::with_capacity(picked.len());
        {
            let mut update = tx.prepare(
                "UPDATE entries
                 SET state = 'inflight', delivery_id = ?1,
                     lease_deadline_ms = ?2, attempts = attempts + 1,
                     updated_at_ms = ?4
                 WHERE entry_id = ?3 AND state = 'ready'",
            )?;
            for entry in picked {
                let delivery_id = next_delivery;
                next_delivery += 1;
                let deadline_ms = now.saturating_add(lease_ms);
                let changed = update.execute(params![
                    to_i64(delivery_id)?,
                    to_i64(deadline_ms)?,
                    entry.entry_id,
                    to_i64(now)?,
                ])?;
                if changed != 1 {
                    return Err(Error::Invalid(
                        "ready entry changed during claim".to_string(),
                    ));
                }
                tx.execute(
                    "INSERT INTO delivery_attempts
                     (delivery_id, entry_id, queue_id, queue_name,
                      message_created_at_ms, attempt, started_at_ms, finished_at_ms, outcome)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, NULL, 'inflight')",
                    params![
                        to_i64(delivery_id)?,
                        entry.entry_id,
                        queue_id,
                        queue,
                        entry.created_at_ms,
                        entry.attempts + 1,
                        to_i64(now)?,
                    ],
                )?;
                deliveries.push(Delivery {
                    token: DeliveryToken {
                        message_id: from_i64(entry.entry_id)?,
                        delivery_id,
                    },
                    queue: queue.to_string(),
                    queue_id,
                    message_id: from_i64(entry.entry_id)?,
                    payload: entry.payload,
                    content_type: entry.content_type,
                    correlation_id: entry.correlation_id,
                    reply_to: entry.reply_to,
                    application_message_id: entry.application_message_id,
                    attempts: from_i64(entry.attempts)? + 1,
                    deadline_ms,
                });
            }
        }
        set_meta(&tx, META_NEXT_DELIVERY, next_delivery)?;
        tx.commit()?;
        Ok(Some(deliveries))
    }

    pub fn ack(&self, token: &DeliveryToken) -> Result<AckOutcome> {
        let mut outcomes = self.ack_batch(std::slice::from_ref(token))?;
        Ok(outcomes.remove(0))
    }

    pub fn ack_batch(&self, tokens: &[DeliveryToken]) -> Result<Vec<AckOutcome>> {
        self.settle_batch(tokens, MessageState::Acked)
    }

    /// Permanently remove a delivery without treating it as successful.
    pub fn discard_batch(&self, tokens: &[DeliveryToken]) -> Result<Vec<AckOutcome>> {
        self.settle_batch(tokens, MessageState::Discarded)
    }

    pub fn discard(&self, token: &DeliveryToken) -> Result<AckOutcome> {
        let mut outcomes = self.discard_batch(std::slice::from_ref(token))?;
        Ok(outcomes.remove(0))
    }

    /// Extend the lease of an in-flight delivery.
    ///
    /// A stale or unknown token changes nothing. `lease_ms = 0` uses the
    /// engine default. History is unaffected; only the deadline moves.
    pub fn touch(&self, token: &DeliveryToken, lease_ms: u64) -> Result<TouchOutcome> {
        let lease_ms = if lease_ms == 0 {
            self.options.lease_ms
        } else {
            lease_ms
        };
        let now = now_ms();
        let deadline_ms = now.saturating_add(lease_ms);
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<i64> = tx
            .query_row(
                "SELECT delivery_id FROM entries
                 WHERE entry_id = ?1 AND state = 'inflight'",
                [to_i64(token.message_id)?],
                |row| row.get(0),
            )
            .optional()?;
        let outcome = match current {
            None => TouchOutcome::Unknown,
            Some(delivery_id) if from_i64(delivery_id)? != token.delivery_id => TouchOutcome::Stale,
            Some(_) => {
                tx.execute(
                    "UPDATE entries
                     SET lease_deadline_ms = ?1, updated_at_ms = ?2
                     WHERE entry_id = ?3 AND state = 'inflight' AND delivery_id = ?4",
                    params![
                        to_i64(deadline_ms)?,
                        to_i64(now)?,
                        to_i64(token.message_id)?,
                        to_i64(token.delivery_id)?
                    ],
                )?;
                TouchOutcome::Extended
            }
        };
        tx.commit()?;
        Ok(outcome)
    }

    /// Settle one delivery and publish its result in the same transaction.
    ///
    /// A stale or unknown token does not publish. A missing destination queue,
    /// oversized payload, or storage error aborts the whole transaction, so the
    /// input delivery remains in flight.
    pub fn complete(
        &self,
        token: &DeliveryToken,
        queue: &str,
        message: NewMessage,
        settlement: Settlement,
    ) -> Result<Completion> {
        if message.payload.len() > self.options.max_payload_bytes {
            return Err(Error::PayloadTooLarge {
                size: message.payload.len(),
                max: self.options.max_payload_bytes,
            });
        }
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current: Option<i64> = tx
            .query_row(
                "SELECT delivery_id FROM entries
                 WHERE entry_id = ?1 AND state = 'inflight'",
                [to_i64(token.message_id)?],
                |row| row.get(0),
            )
            .optional()?;
        match current {
            None => Ok(Completion {
                outcome: AckOutcome::Unknown,
                message_id: None,
            }),
            Some(delivery_id) if from_i64(delivery_id)? != token.delivery_id => Ok(Completion {
                outcome: AckOutcome::Stale,
                message_id: None,
            }),
            Some(_) => {
                let queue_id: i64 = tx
                    .query_row("SELECT id FROM queues WHERE name = ?1", [queue], |row| {
                        row.get(0)
                    })
                    .optional()?
                    .ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;
                let now = now_ms();
                let message_id = insert_messages_tx(&tx, queue_id, vec![message], now)?.remove(0);
                tx.execute(
                    "UPDATE delivery_attempts
                     SET finished_at_ms = ?1, outcome = ?2
                     WHERE entry_id = ?3 AND delivery_id = ?4
                       AND finished_at_ms IS NULL",
                    params![
                        to_i64(now)?,
                        settlement.outcome().as_str(),
                        to_i64(token.message_id)?,
                        to_i64(token.delivery_id)?
                    ],
                )?;
                tx.execute(
                    "DELETE FROM entries
                     WHERE entry_id = ?1 AND state = 'inflight' AND delivery_id = ?2",
                    params![to_i64(token.message_id)?, to_i64(token.delivery_id)?],
                )?;
                tx.commit()?;
                Ok(Completion {
                    outcome: AckOutcome::Acked,
                    message_id: Some(message_id),
                })
            }
        }
    }

    fn settle_batch(
        &self,
        tokens: &[DeliveryToken],
        outcome: MessageState,
    ) -> Result<Vec<AckOutcome>> {
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_ms();
        let mut outcomes = Vec::with_capacity(tokens.len());
        for token in tokens {
            let current: Option<i64> = tx
                .query_row(
                    "SELECT delivery_id FROM entries
                     WHERE entry_id = ?1 AND state = 'inflight'",
                    [to_i64(token.message_id)?],
                    |row| row.get(0),
                )
                .optional()?;
            match current {
                None => outcomes.push(AckOutcome::Unknown),
                Some(delivery_id) if from_i64(delivery_id)? != token.delivery_id => {
                    outcomes.push(AckOutcome::Stale)
                }
                Some(_) => {
                    tx.execute(
                        "UPDATE delivery_attempts
                         SET finished_at_ms = ?1, outcome = ?2
                         WHERE entry_id = ?3 AND delivery_id = ?4
                           AND finished_at_ms IS NULL",
                        params![
                            to_i64(now)?,
                            outcome.as_str(),
                            to_i64(token.message_id)?,
                            to_i64(token.delivery_id)?
                        ],
                    )?;
                    tx.execute(
                        "DELETE FROM entries
                         WHERE entry_id = ?1 AND state = 'inflight' AND delivery_id = ?2",
                        params![to_i64(token.message_id)?, to_i64(token.delivery_id)?],
                    )?;
                    outcomes.push(AckOutcome::Acked);
                }
            }
        }
        tx.commit()?;
        Ok(outcomes)
    }

    pub fn requeue(&self, token: &DeliveryToken) -> Result<RequeueOutcome> {
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_ms();
        let current: Option<(i64, i64)> = tx
            .query_row(
                "SELECT queue_id, delivery_id FROM entries
                 WHERE entry_id = ?1 AND state = 'inflight'",
                [to_i64(token.message_id)?],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let outcome = match current {
            None => RequeueOutcome::Unknown,
            Some((_queue_id, delivery_id)) if from_i64(delivery_id)? != token.delivery_id => {
                RequeueOutcome::Stale
            }
            Some((queue_id, _)) => {
                requeue_entry(&tx, token.message_id, queue_id, MessageState::Requeued, now)?;
                RequeueOutcome::Requeued
            }
        };
        tx.commit()?;
        Ok(outcome)
    }

    /// Requeue deliveries whose lease expired. Returns how many were returned.
    pub fn recover_expired(&self) -> Result<u64> {
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let recovered = requeue_expired_tx(&tx, now_ms())?;
        tx.commit()?;
        Ok(recovered)
    }

    /// Apply the configured attempt-history retention policy.
    ///
    /// Unfinished attempts are never removed. A zero retention or row cap
    /// disables that limit. Each call is bounded to avoid long write stalls.
    pub fn prune_history(&self) -> Result<u64> {
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let now = now_ms();
        let batch = HISTORY_PRUNE_BATCH as i64;
        let mut removed = 0u64;
        if self.options.history_retention_ms > 0 {
            let cutoff = now.saturating_sub(self.options.history_retention_ms);
            removed += tx.execute(
                "DELETE FROM delivery_attempts
                 WHERE delivery_id IN (
                     SELECT delivery_id FROM delivery_attempts
                     WHERE finished_at_ms IS NOT NULL AND finished_at_ms < ?1
                     ORDER BY finished_at_ms, delivery_id
                     LIMIT ?2
                 )",
                params![to_i64(cutoff)?, batch],
            )? as u64;
        }
        if self.options.history_max_rows > 0 {
            let keep = to_i64(self.options.history_max_rows as u64)?;
            removed += tx.execute(
                "DELETE FROM delivery_attempts
                 WHERE delivery_id IN (
                     SELECT delivery_id FROM delivery_attempts
                     WHERE finished_at_ms IS NOT NULL
                       AND delivery_id NOT IN (
                           SELECT delivery_id FROM delivery_attempts
                           WHERE finished_at_ms IS NOT NULL
                           ORDER BY finished_at_ms DESC, delivery_id DESC
                           LIMIT ?1
                       )
                     ORDER BY finished_at_ms, delivery_id
                     LIMIT ?2
                 )",
                params![keep, batch],
            )? as u64;
        }
        tx.commit()?;
        Ok(removed)
    }

    /// Remove all ready messages from a queue. In-flight messages are untouched.
    pub fn purge(&self, queue: &str) -> Result<u64> {
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let queue_id = queue_id_tx(&tx, queue)?;
        let removed = tx.execute(
            "DELETE FROM entries WHERE queue_id = ?1 AND state = 'ready'",
            [to_i64(queue_id)?],
        )?;
        tx.commit()?;
        Ok(removed as u64)
    }

    /// Delete a queue and every message that belongs to it.
    pub fn delete_queue(&self, queue: &str) -> Result<u64> {
        self.delete_queue_checked(queue, false)?
            .ok_or_else(|| Error::Invalid("queue is not empty".into()))
    }

    /// Delete only if the queue has no ready or in-flight messages.
    /// `None` means the queue was left untouched because it was not empty.
    pub fn delete_queue_if_empty(&self, queue: &str) -> Result<Option<u64>> {
        self.delete_queue_checked(queue, true)
    }

    fn delete_queue_checked(&self, queue: &str, require_empty: bool) -> Result<Option<u64>> {
        let mut writer = self.writer()?;
        let tx = writer.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let queue_id = queue_id_tx(&tx, queue)?;
        let total: i64 = tx.query_row(
            "SELECT COUNT(*) FROM entries WHERE queue_id = ?1",
            [to_i64(queue_id)?],
            |row| row.get(0),
        )?;
        if require_empty && total != 0 {
            return Ok(None);
        }
        tx.execute(
            "UPDATE delivery_attempts
             SET finished_at_ms = ?1, outcome = 'discarded'
             WHERE queue_id = ?2 AND finished_at_ms IS NULL",
            params![to_i64(now_ms())?, to_i64(queue_id)?],
        )?;
        tx.execute("DELETE FROM queues WHERE id = ?1", [to_i64(queue_id)?])?;
        tx.commit()?;
        Ok(Some(total as u64))
    }

    /// Queue name that owns an in-flight delivery token.
    ///
    /// Returns `None` for unknown or stale tokens. Used by network adapters to
    /// authorize settlement without trusting the client-provided token.
    pub fn queue_for_token(&self, token: &DeliveryToken) -> Result<Option<String>> {
        self.with_reader(|reader| {
            let name: Option<String> = reader
                .query_row(
                    "SELECT q.name
                     FROM entries e JOIN queues q ON q.id = e.queue_id
                     WHERE e.entry_id = ?1 AND e.state = 'inflight'
                       AND e.delivery_id = ?2",
                    params![to_i64(token.message_id)?, to_i64(token.delivery_id)?],
                    |row| row.get(0),
                )
                .optional()?;
            Ok(name)
        })
    }

    pub fn info(&self, queue: &str) -> Result<QueueInfo> {
        self.with_reader(|reader| {
            let id: Option<i64> = reader
                .query_row("SELECT id FROM queues WHERE name = ?1", [queue], |row| {
                    row.get(0)
                })
                .optional()?;
            let id = id.ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;
            let (ready, inflight): (i64, i64) = reader.query_row(
                "SELECT
                    COALESCE(SUM(CASE WHEN state = 'ready' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN state = 'inflight' THEN 1 ELSE 0 END), 0)
                 FROM entries WHERE queue_id = ?1",
                [id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            Ok(QueueInfo {
                name: queue.to_string(),
                id: from_i64(id)?,
                ready: ready as u64,
                inflight: inflight as u64,
            })
        })
    }

    pub fn list_queues(&self) -> Result<Vec<QueueInfo>> {
        self.with_reader(|reader| {
            let mut statement = reader.prepare(
                "SELECT q.id, q.name,
                    COALESCE(SUM(CASE WHEN e.state = 'ready' THEN 1 ELSE 0 END), 0),
                    COALESCE(SUM(CASE WHEN e.state = 'inflight' THEN 1 ELSE 0 END), 0)
                 FROM queues q LEFT JOIN entries e ON e.queue_id = q.id
                 GROUP BY q.id, q.name
                 ORDER BY q.name",
            )?;
            let rows = statement.query_map([], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?;
            let mut out = Vec::new();
            for row in rows {
                let (id, name, ready, inflight) = row?;
                out.push(QueueInfo {
                    id: from_i64(id)?,
                    name,
                    ready: ready as u64,
                    inflight: inflight as u64,
                });
            }
            Ok(out)
        })
    }

    /// Current state and attempt history for one message.
    ///
    /// Finished history is retained after ack and after queue deletion. `None`
    /// means the message id is unknown, or its history belongs to another queue.
    pub fn message_status(&self, queue: &str, message_id: u64) -> Result<Option<MessageStatus>> {
        self.with_reader(|reader| {
            let active: Option<(String, String, i64, i64, i64)> = reader
                .query_row(
                    "SELECT q.name, e.state, e.attempts, e.created_at_ms, e.updated_at_ms
                     FROM entries e JOIN queues q ON q.id = e.queue_id
                     WHERE e.entry_id = ?1 AND q.name = ?2",
                    params![to_i64(message_id)?, queue],
                    |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                        ))
                    },
                )
                .optional()?;
            if let Some((queue, state, attempts, created_at_ms, updated_at_ms)) = active {
                return Ok(Some(MessageStatus {
                    queue,
                    message_id,
                    state: MessageState::parse(&state)?,
                    attempts: from_i64(attempts)?,
                    created_at_ms: from_i64(created_at_ms)?,
                    updated_at_ms: from_i64(updated_at_ms)?,
                    history: attempt_history(reader, message_id)?,
                }));
            }

            let finished: Vec<(String, i64, i64, i64, Option<i64>, String)> = {
                let mut statement = reader.prepare(
                    "SELECT queue_name, message_created_at_ms, attempt,
                            started_at_ms, finished_at_ms, outcome
                     FROM delivery_attempts
                     WHERE entry_id = ?1
                     ORDER BY attempt",
                )?;
                let rows = statement.query_map([to_i64(message_id)?], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                })?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(row?);
                }
                out
            };
            let Some((stored_queue, created_at_ms, _, _, _, _)) = finished.first() else {
                return Ok(None);
            };
            if stored_queue != queue {
                return Ok(None);
            }
            let Some((_, _, last_attempt, last_started, last_finished, outcome)) = finished.last()
            else {
                return Ok(None);
            };
            let latest = MessageState::parse(outcome)?;
            let state = match latest {
                MessageState::Acked => MessageState::Acked,
                _ => MessageState::Discarded,
            };
            Ok(Some(MessageStatus {
                queue: stored_queue.clone(),
                message_id,
                state,
                attempts: from_i64(*last_attempt)?,
                created_at_ms: from_i64(*created_at_ms)?,
                updated_at_ms: from_i64(last_finished.unwrap_or(*last_started))?,
                history: attempt_history(reader, message_id)?,
            }))
        })
    }
}

fn attempt_history(reader: &Connection, entry_id: u64) -> Result<Vec<AttemptInfo>> {
    let mut statement = reader.prepare(
        "SELECT attempt, delivery_id, started_at_ms, finished_at_ms, outcome
         FROM delivery_attempts
         WHERE entry_id = ?1
         ORDER BY attempt",
    )?;
    let rows = statement.query_map([to_i64(entry_id)?], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
            row.get::<_, Option<i64>>(3)?,
            row.get::<_, String>(4)?,
        ))
    })?;
    let mut history = Vec::new();
    for row in rows {
        let (attempt, delivery_id, started_at_ms, finished_at_ms, outcome) = row?;
        history.push(AttemptInfo {
            attempt: from_i64(attempt)?,
            delivery_id: from_i64(delivery_id)?,
            started_at_ms: from_i64(started_at_ms)?,
            finished_at_ms: finished_at_ms.map(from_i64).transpose()?,
            outcome: MessageState::parse(&outcome)?,
        });
    }
    Ok(history)
}

struct PickedEntry {
    entry_id: i64,
    payload: Vec<u8>,
    content_type: Option<String>,
    application_message_id: Option<String>,
    correlation_id: Option<String>,
    reply_to: Option<String>,
    attempts: i64,
    _ready_seq: i64,
    created_at_ms: i64,
}

fn acquire_lock(path: &Path) -> Result<File> {
    let lock_path = PathBuf::from(format!("{}.lock", path.display()));
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)?;
    file.try_lock_exclusive().map_err(|error| {
        if error.kind() == std::io::ErrorKind::WouldBlock {
            Error::EngineOpen(path.display().to_string())
        } else {
            Error::from(error)
        }
    })?;
    Ok(file)
}

fn reject_foreign_file(path: &Path) -> Result<()> {
    let metadata = match std::fs::metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(Error::from(error)),
    };
    if !metadata.is_file() {
        return Err(Error::UnsupportedFormat(format!(
            "database path is not a regular file: {}",
            path.display()
        )));
    }
    if metadata.len() == 0 {
        return Ok(());
    }
    let mut magic = [0u8; 16];
    let mut file = File::open(path)?;
    let read = file.read(&mut magic)?;
    if read != magic.len() || &magic != b"SQLite format 3\0" {
        return Err(Error::UnsupportedFormat(format!(
            "{} is not a SQLite database",
            path.display()
        )));
    }
    Ok(())
}

fn configure(connection: &Connection, cache_kib: i64, writable: bool) -> Result<()> {
    connection.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS))?;
    connection.execute_batch("PRAGMA foreign_keys = ON;")?;
    if writable {
        let mode: String =
            connection.query_row("PRAGMA journal_mode = WAL", [], |row| row.get(0))?;
        if !mode.eq_ignore_ascii_case("wal") {
            return Err(Error::Invalid(format!(
                "SQLite refused WAL mode (got {mode})"
            )));
        }
        connection.execute_batch("PRAGMA synchronous = FULL;")?;
    }
    connection.execute_batch(&format!("PRAGMA cache_size = -{cache_kib};"))?;
    Ok(())
}

fn writer_cache_kib(options: &Options) -> i64 {
    ((options.cache_size / 2).max(1024 * 1024) / 1024) as i64
}

fn reader_cache_kib(options: &Options) -> i64 {
    ((options.cache_size / 8).max(512 * 1024) / 1024) as i64
}

fn initialize_or_validate(connection: &mut Connection) -> Result<()> {
    let version: i64 = connection.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    let application_id: i64 =
        connection.query_row("PRAGMA application_id", [], |row| row.get(0))?;
    let table_count: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'",
        [],
        |row| row.get(0),
    )?;

    if version == 0 && table_count == 0 && application_id == 0 {
        let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute_batch(SCHEMA)?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)",
            params![META_SCHEMA, to_i64(SCHEMA_VERSION)?],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, 1)",
            [META_NEXT_QUEUE],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, 1)",
            [META_NEXT_MESSAGE],
        )?;
        tx.execute(
            "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, 1)",
            [META_NEXT_DELIVERY],
        )?;
        tx.execute_batch(&format!(
            "PRAGMA application_id = {APPLICATION_ID}; PRAGMA user_version = {SCHEMA_VERSION};"
        ))?;
        tx.commit()?;
        return Ok(());
    }

    if application_id != APPLICATION_ID {
        return Err(Error::UnsupportedFormat(
            "SQLite database was not created by Ante".into(),
        ));
    }
    let stored: Option<i64> = connection
        .query_row(
            "SELECT value FROM metadata WHERE key = ?1",
            [META_SCHEMA],
            |row| row.get(0),
        )
        .optional()?;
    if version == 1 && stored == Some(1) {
        migrate_v1(connection)?;
        return Ok(());
    }
    if version != SCHEMA_VERSION as i64 {
        return Err(Error::UnsupportedFormat(format!(
            "unsupported database schema version {version}"
        )));
    }
    if stored != Some(SCHEMA_VERSION as i64) {
        return Err(Error::UnsupportedFormat(
            "database metadata schema does not match user_version".into(),
        ));
    }
    connection.execute_batch(
        "CREATE INDEX IF NOT EXISTS attempts_finished
         ON delivery_attempts(finished_at_ms, delivery_id);",
    )?;
    Ok(())
}

fn migrate_v1(connection: &mut Connection) -> Result<()> {
    let tx = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute_batch(
        "ALTER TABLE entries ADD COLUMN created_at_ms INTEGER NOT NULL DEFAULT 0;
         ALTER TABLE entries ADD COLUMN updated_at_ms INTEGER NOT NULL DEFAULT 0;",
    )?;
    tx.execute_batch(ATTEMPTS_SCHEMA)?;
    let now = now_ms();
    tx.execute(
        "UPDATE entries SET created_at_ms = ?1, updated_at_ms = ?1
         WHERE created_at_ms = 0",
        [to_i64(now)?],
    )?;
    tx.execute(
        "INSERT INTO delivery_attempts
         (delivery_id, entry_id, queue_id, queue_name, message_created_at_ms,
          attempt, started_at_ms, finished_at_ms, outcome)
         SELECT e.delivery_id, e.entry_id, e.queue_id, q.name, e.created_at_ms,
                e.attempts, ?1, NULL, 'inflight'
         FROM entries e JOIN queues q ON q.id = e.queue_id
         WHERE e.state = 'inflight'",
        [to_i64(now)?],
    )?;
    tx.execute(
        "UPDATE metadata SET value = ?1 WHERE key = ?2",
        params![to_i64(SCHEMA_VERSION)?, META_SCHEMA],
    )?;
    tx.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION};"))?;
    tx.commit()?;
    Ok(())
}

fn queue_id_tx(tx: &Transaction<'_>, queue: &str) -> Result<u64> {
    let id: Option<i64> = tx
        .query_row("SELECT id FROM queues WHERE name = ?1", [queue], |row| {
            row.get(0)
        })
        .optional()?;
    from_i64(id.ok_or_else(|| Error::QueueNotFound(queue.to_string()))?)
}

fn next_meta(tx: &Transaction<'_>, key: &str) -> Result<u64> {
    let value: Option<i64> = tx
        .query_row("SELECT value FROM metadata WHERE key = ?1", [key], |row| {
            row.get(0)
        })
        .optional()?;
    let value = from_i64(value.ok_or_else(|| Error::Invalid(format!("missing metadata {key}")))?)?;
    set_meta(
        tx,
        key,
        value
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("metadata counter overflow".into()))?,
    )?;
    Ok(value)
}

fn set_meta(tx: &Transaction<'_>, key: &str, value: u64) -> Result<()> {
    tx.execute(
        "INSERT INTO metadata (key, value) VALUES (?1, ?2)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        params![key, to_i64(value)?],
    )?;
    Ok(())
}

fn insert_messages_tx(
    tx: &Transaction<'_>,
    queue_id: i64,
    messages: Vec<NewMessage>,
    now: u64,
) -> Result<Vec<u64>> {
    let mut next_message = next_meta(tx, META_NEXT_MESSAGE)?;
    let mut next_seq = queue_next_seq(tx, queue_id)?;
    let mut ids = Vec::with_capacity(messages.len());
    {
        let mut insert = tx.prepare(
            "INSERT INTO entries
             (entry_id, queue_id, ready_seq, payload, content_type,
              application_message_id, correlation_id, reply_to,
              state, delivery_id, lease_deadline_ms, attempts,
              created_at_ms, updated_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 'ready', NULL, NULL, 0, ?9, ?9)",
        )?;
        for message in messages {
            let entry_id = next_message;
            next_message += 1;
            insert.execute(params![
                to_i64(entry_id)?,
                queue_id,
                to_i64(next_seq)?,
                message.payload,
                message.content_type,
                message.message_id,
                message.correlation_id,
                message.reply_to,
                to_i64(now)?,
            ])?;
            next_seq += 1;
            ids.push(entry_id);
        }
    }
    set_meta(tx, META_NEXT_MESSAGE, next_message)?;
    set_queue_next_seq(tx, queue_id, next_seq)?;
    Ok(ids)
}

fn queue_next_seq(tx: &Transaction<'_>, queue_id: i64) -> Result<u64> {
    let value: i64 = tx.query_row(
        "SELECT next_ready_seq FROM queues WHERE id = ?1",
        [queue_id],
        |row| row.get(0),
    )?;
    from_i64(value)
}

fn set_queue_next_seq(tx: &Transaction<'_>, queue_id: i64, value: u64) -> Result<()> {
    tx.execute(
        "UPDATE queues SET next_ready_seq = ?1 WHERE id = ?2",
        params![to_i64(value)?, queue_id],
    )?;
    Ok(())
}

fn requeue_entry(
    tx: &Transaction<'_>,
    entry_id: u64,
    queue_id: i64,
    outcome: MessageState,
    now: u64,
) -> Result<()> {
    let next = queue_next_seq(tx, queue_id)?;
    tx.execute(
        "UPDATE delivery_attempts
         SET finished_at_ms = ?1, outcome = ?2
         WHERE entry_id = ?3 AND finished_at_ms IS NULL",
        params![to_i64(now)?, outcome.as_str(), to_i64(entry_id)?],
    )?;
    tx.execute(
        "UPDATE entries
         SET state = 'ready', ready_seq = ?1, delivery_id = NULL,
             lease_deadline_ms = NULL, updated_at_ms = ?3
         WHERE entry_id = ?2 AND state = 'inflight'",
        params![to_i64(next)?, to_i64(entry_id)?, to_i64(now)?],
    )?;
    set_queue_next_seq(
        tx,
        queue_id,
        next.checked_add(1)
            .ok_or_else(|| Error::Invalid("ready sequence overflow".into()))?,
    )
}

fn requeue_expired_tx(tx: &Transaction<'_>, now: u64) -> Result<u64> {
    let targets: Vec<(i64, i64)> = {
        let mut statement = tx.prepare(
            "SELECT entry_id, queue_id FROM entries
             WHERE state = 'inflight' AND lease_deadline_ms <= ?1
             ORDER BY entry_id",
        )?;
        let rows = statement.query_map([to_i64(now)?], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        })?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        out
    };
    for (entry_id, queue_id) in &targets {
        requeue_entry(
            tx,
            from_i64(*entry_id)?,
            *queue_id,
            MessageState::Expired,
            now,
        )?;
    }
    Ok(targets.len() as u64)
}

fn requeue_all_inflight(tx: &Transaction<'_>) -> Result<u64> {
    let now = now_ms();
    let targets: Vec<(i64, i64)> = {
        let mut statement = tx.prepare(
            "SELECT entry_id, queue_id FROM entries
             WHERE state = 'inflight'
             ORDER BY entry_id",
        )?;
        let rows =
            statement.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row?);
        }
        out
    };
    for (entry_id, queue_id) in &targets {
        requeue_entry(
            tx,
            from_i64(*entry_id)?,
            *queue_id,
            MessageState::Recovered,
            now,
        )?;
    }
    Ok(targets.len() as u64)
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Invalid("queue name must not be empty".into()));
    }
    if name.len() > 255 {
        return Err(Error::Invalid("queue name is too long".into()));
    }
    if name.chars().any(|c| c.is_control()) {
        return Err(Error::Invalid(
            "queue name contains control characters".into(),
        ));
    }
    Ok(())
}

fn to_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| Error::Invalid("integer exceeds SQLite range".into()))
}

fn from_i64(value: i64) -> Result<u64> {
    u64::try_from(value).map_err(|_| Error::Invalid("negative integer in storage".into()))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
