use std::fmt;
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use redb::{Database, ReadableTable, TableDefinition, WriteTransaction};

use crate::codec::{DeliveryRecord, MessageRecord};
use crate::{Error, Result, SCHEMA_VERSION};

const QUEUES: TableDefinition<&str, u64> = TableDefinition::new("queues");
const SEQ: TableDefinition<u64, u64> = TableDefinition::new("seq");
const MESSAGES: TableDefinition<u64, &[u8]> = TableDefinition::new("messages");
const READY: TableDefinition<(u64, u64), u64> = TableDefinition::new("ready");
const INFLIGHT: TableDefinition<u64, &[u8]> = TableDefinition::new("inflight");
const ATTEMPTS: TableDefinition<u64, u64> = TableDefinition::new("attempts");
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");

const META_NEXT_QUEUE: &str = "next_queue";
const META_NEXT_MESSAGE: &str = "next_message";
const META_NEXT_DELIVERY: &str = "next_delivery";
const META_SCHEMA: &str = "schema_version";

pub const DEFAULT_LEASE_MS: u64 = 30_000;

#[derive(Debug, Clone)]
pub struct Options {
    pub cache_size: usize,
    pub max_payload_bytes: usize,
    pub max_batch_messages: usize,
    pub max_batch_bytes: usize,
    pub lease_ms: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            cache_size: 64 * 1024 * 1024,
            max_payload_bytes: 8 * 1024 * 1024,
            max_batch_messages: 256,
            max_batch_bytes: 1024 * 1024,
            lease_ms: DEFAULT_LEASE_MS,
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

pub struct Ante {
    db: Mutex<Database>,
    options: Options,
}

impl fmt::Debug for Ante {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Ante")
            .field("options", &self.options)
            .finish()
    }
}

impl Ante {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with(path, Options::default())
    }

    pub fn open_with(path: impl AsRef<Path>, options: Options) -> Result<Self> {
        let db = redb::Builder::new()
            .set_cache_size(options.cache_size)
            .create(path.as_ref())?;
        {
            let write = db.begin_write()?;
            {
                let _ = write.open_table(QUEUES)?;
                let _ = write.open_table(SEQ)?;
                let _ = write.open_table(MESSAGES)?;
                let _ = write.open_table(READY)?;
                let _ = write.open_table(INFLIGHT)?;
                let _ = write.open_table(ATTEMPTS)?;
                let _ = write.open_table(META)?;
            }
            write.commit()?;
        }
        let ante = Ante {
            db: Mutex::new(db),
            options,
        };
        ante.check_schema()?;
        ante.recover_startup()?;
        Ok(ante)
    }

    pub fn options(&self) -> &Options {
        &self.options
    }

    fn db(&self) -> Result<std::sync::MutexGuard<'_, Database>> {
        self.db.lock().map_err(|_| Error::Poisoned)
    }

    fn check_schema(&self) -> Result<()> {
        let db = self.db()?;
        let write = db.begin_write()?;
        let current = {
            let meta = write.open_table(META)?;
            let value = meta.get(META_SCHEMA)?.map(|entry| entry.value());
            value
        };
        match current {
            Some(version) if version == SCHEMA_VERSION => {}
            Some(version) => {
                return Err(Error::Invalid(format!(
                    "unsupported schema version {version}"
                )))
            }
            None => {
                let mut meta = write.open_table(META)?;
                meta.insert(META_SCHEMA, SCHEMA_VERSION)?;
                meta.insert(META_NEXT_QUEUE, 1)?;
                meta.insert(META_NEXT_MESSAGE, 1)?;
                meta.insert(META_NEXT_DELIVERY, 1)?;
            }
        }
        write.commit()?;
        Ok(())
    }

    /// Requeue every in-flight message left behind by a previous process.
    fn recover_startup(&self) -> Result<u64> {
        let db = self.db()?;
        let write = db.begin_write()?;
        let recovered = requeue_inflight(&write, |_| true)?;
        if recovered == 0 {
            write.abort()?;
        } else {
            write.commit()?;
        }
        Ok(recovered)
    }

    pub fn declare_queue(&self, name: &str) -> Result<u64> {
        validate_name(name)?;
        let db = self.db()?;
        let write = db.begin_write()?;
        let existing = {
            let queues = write.open_table(QUEUES)?;
            let value = queues.get(name)?.map(|entry| entry.value());
            value
        };
        let id = match existing {
            Some(id) => id,
            None => {
                let id = {
                    let meta = write.open_table(META)?;
                    let value = meta
                        .get(META_NEXT_QUEUE)?
                        .map(|entry| entry.value())
                        .unwrap_or(1);
                    value
                };
                {
                    let mut meta = write.open_table(META)?;
                    meta.insert(META_NEXT_QUEUE, id + 1)?;
                }
                {
                    let mut queues = write.open_table(QUEUES)?;
                    queues.insert(name, id)?;
                }
                {
                    let mut seq = write.open_table(SEQ)?;
                    seq.insert(id, 1)?;
                }
                id
            }
        };
        write.commit()?;
        Ok(id)
    }

    pub fn queue_exists(&self, name: &str) -> Result<bool> {
        Ok(self.queue_id(name)?.is_some())
    }

    /// Stable identity of a queue. A delete-and-recreate produces a new id, so
    /// long-lived consumers can detect that their queue was replaced.
    pub fn queue_id(&self, name: &str) -> Result<Option<u64>> {
        let db = self.db()?;
        let read = db.begin_read()?;
        let queues = read.open_table(QUEUES)?;
        Ok(queues.get(name)?.map(|entry| entry.value()))
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

        let db = self.db()?;
        let write = db.begin_write()?;
        let ids;
        {
            let found = {
                let queues = write.open_table(QUEUES)?;
                let value = queues.get(queue)?.map(|entry| entry.value());
                value
            };
            let queue_id = found.ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;
            let mut messages_table = write.open_table(MESSAGES)?;
            let mut ready = write.open_table(READY)?;
            let mut seq = write.open_table(SEQ)?;
            let mut meta = write.open_table(META)?;

            let mut next_message = meta.get(META_NEXT_MESSAGE)?.map(|v| v.value()).unwrap_or(1);
            let mut next_seq = seq.get(queue_id)?.map(|v| v.value()).unwrap_or(1);
            let mut out = Vec::with_capacity(messages.len());

            for message in messages {
                let record = MessageRecord {
                    payload: message.payload,
                    content_type: message.content_type,
                    message_id: message.message_id,
                    correlation_id: message.correlation_id,
                    reply_to: message.reply_to,
                };
                let encoded = record.encode();
                let message_id = next_message;
                next_message += 1;
                messages_table.insert(message_id, encoded.as_slice())?;
                ready.insert((queue_id, next_seq), message_id)?;
                next_seq += 1;
                out.push(message_id);
            }

            seq.insert(queue_id, next_seq)?;
            meta.insert(META_NEXT_MESSAGE, next_message)?;
            ids = out;
        }
        write.commit()?;
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
        let db = self.db()?;
        let write = db.begin_write()?;

        let found = {
            let queues = write.open_table(QUEUES)?;
            let value = queues.get(queue)?.map(|entry| entry.value());
            value
        };
        let queue_id = match found {
            Some(id) => id,
            None if expected.is_some() => return Ok(None),
            None => return Err(Error::QueueNotFound(queue.to_string())),
        };
        if expected.is_some_and(|expected| expected != queue_id) {
            return Ok(None);
        }

        requeue_inflight(&write, |record| record.deadline_ms <= now)?;

        let picked: Vec<(u64, u64)> = {
            let ready = write.open_table(READY)?;
            let range = ready.range((queue_id, 0u64)..=(queue_id, u64::MAX))?;
            let mut out = Vec::with_capacity(max);
            for item in range.take(max) {
                let (key, value) = item?;
                out.push((key.value().1, value.value()));
            }
            out
        };

        if picked.is_empty() {
            write.abort()?;
            return Ok(Some(Vec::new()));
        }

        let mut deliveries = Vec::with_capacity(picked.len());
        {
            let mut ready = write.open_table(READY)?;
            let mut inflight = write.open_table(INFLIGHT)?;
            let mut attempts_table = write.open_table(ATTEMPTS)?;
            let messages_table = write.open_table(MESSAGES)?;
            let mut meta = write.open_table(META)?;
            let mut next_delivery = meta
                .get(META_NEXT_DELIVERY)?
                .map(|v| v.value())
                .unwrap_or(1);

            for (seq, message_id) in picked {
                ready.remove((queue_id, seq))?;
                let attempts = attempts_table
                    .get(message_id)?
                    .map(|v| v.value())
                    .unwrap_or(0)
                    + 1;
                attempts_table.insert(message_id, attempts)?;

                let raw = messages_table.get(message_id)?.ok_or_else(|| {
                    Error::Invalid("ready entry points at a missing message".into())
                })?;
                let record = MessageRecord::decode(raw.value())?;

                let delivery_id = next_delivery;
                next_delivery += 1;
                let deadline_ms = now.saturating_add(lease_ms);
                let inflight_record = DeliveryRecord {
                    delivery_id,
                    queue_id,
                    deadline_ms,
                };
                let encoded = inflight_record.encode();
                inflight.insert(message_id, encoded.as_slice())?;

                deliveries.push(Delivery {
                    token: DeliveryToken {
                        message_id,
                        delivery_id,
                    },
                    queue: queue.to_string(),
                    queue_id,
                    message_id,
                    payload: record.payload,
                    content_type: record.content_type,
                    correlation_id: record.correlation_id,
                    reply_to: record.reply_to,
                    application_message_id: record.message_id,
                    attempts,
                    deadline_ms,
                });
            }
            meta.insert(META_NEXT_DELIVERY, next_delivery)?;
        }
        write.commit()?;
        Ok(Some(deliveries))
    }

    pub fn ack(&self, token: &DeliveryToken) -> Result<AckOutcome> {
        let mut outcomes = self.ack_batch(std::slice::from_ref(token))?;
        Ok(outcomes.remove(0))
    }

    pub fn ack_batch(&self, tokens: &[DeliveryToken]) -> Result<Vec<AckOutcome>> {
        if tokens.is_empty() {
            return Ok(Vec::new());
        }
        let db = self.db()?;
        let write = db.begin_write()?;
        let mut outcomes = Vec::with_capacity(tokens.len());
        {
            let mut inflight = write.open_table(INFLIGHT)?;
            let mut messages = write.open_table(MESSAGES)?;
            let mut attempts = write.open_table(ATTEMPTS)?;
            for token in tokens {
                let existing = {
                    let raw = inflight.get(token.message_id)?;
                    raw.map(|value| DeliveryRecord::decode(value.value()))
                        .transpose()?
                };
                match existing {
                    None => outcomes.push(AckOutcome::Unknown),
                    Some(record) if record.delivery_id != token.delivery_id => {
                        outcomes.push(AckOutcome::Stale)
                    }
                    Some(_) => {
                        inflight.remove(token.message_id)?;
                        messages.remove(token.message_id)?;
                        attempts.remove(token.message_id)?;
                        outcomes.push(AckOutcome::Acked);
                    }
                }
            }
        }
        write.commit()?;
        Ok(outcomes)
    }

    pub fn requeue(&self, token: &DeliveryToken) -> Result<RequeueOutcome> {
        let db = self.db()?;
        let write = db.begin_write()?;
        let outcome = {
            let mut inflight = write.open_table(INFLIGHT)?;
            let existing = {
                let raw = inflight.get(token.message_id)?;
                raw.map(|value| DeliveryRecord::decode(value.value()))
                    .transpose()?
            };
            match existing {
                None => RequeueOutcome::Unknown,
                Some(record) if record.delivery_id != token.delivery_id => RequeueOutcome::Stale,
                Some(record) => {
                    inflight.remove(token.message_id)?;
                    push_ready(&write, token.message_id, record.queue_id)?;
                    RequeueOutcome::Requeued
                }
            }
        };
        write.commit()?;
        Ok(outcome)
    }

    /// Requeue deliveries whose lease expired. Returns how many were returned.
    pub fn recover_expired(&self) -> Result<u64> {
        let now = now_ms();
        let db = self.db()?;
        let write = db.begin_write()?;
        let recovered = requeue_inflight(&write, |record| record.deadline_ms <= now)?;
        if recovered == 0 {
            write.abort()?;
        } else {
            write.commit()?;
        }
        Ok(recovered)
    }

    /// Remove all ready messages from a queue. In-flight messages are untouched.
    pub fn purge(&self, queue: &str) -> Result<u64> {
        let db = self.db()?;
        let write = db.begin_write()?;
        let found = {
            let queues = write.open_table(QUEUES)?;
            let value = queues.get(queue)?.map(|entry| entry.value());
            value
        };
        let queue_id = found.ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;
        let entries: Vec<(u64, u64)> = {
            let ready = write.open_table(READY)?;
            let mut out = Vec::new();
            for item in ready.range((queue_id, 0u64)..=(queue_id, u64::MAX))? {
                let (key, value) = item?;
                out.push((key.value().1, value.value()));
            }
            out
        };
        if entries.is_empty() {
            write.abort()?;
            return Ok(0);
        }
        {
            let mut ready = write.open_table(READY)?;
            let mut messages = write.open_table(MESSAGES)?;
            let mut attempts = write.open_table(ATTEMPTS)?;
            for (seq, message_id) in &entries {
                ready.remove((queue_id, *seq))?;
                messages.remove(*message_id)?;
                attempts.remove(*message_id)?;
            }
        }
        write.commit()?;
        Ok(entries.len() as u64)
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
        let db = self.db()?;
        let write = db.begin_write()?;
        let found = {
            let queues = write.open_table(QUEUES)?;
            let value = queues.get(queue)?.map(|entry| entry.value());
            value
        };
        let queue_id = found.ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;

        let ready_entries: Vec<(u64, u64)> = {
            let ready = write.open_table(READY)?;
            let mut out = Vec::new();
            for item in ready.range((queue_id, 0u64)..=(queue_id, u64::MAX))? {
                let (key, value) = item?;
                out.push((key.value().1, value.value()));
            }
            out
        };
        let inflight_entries: Vec<u64> = {
            let inflight = write.open_table(INFLIGHT)?;
            let mut out = Vec::new();
            for item in inflight.iter()? {
                let (key, value) = item?;
                let record = DeliveryRecord::decode(value.value())?;
                if record.queue_id == queue_id {
                    out.push(key.value());
                }
            }
            out
        };

        if require_empty && (!ready_entries.is_empty() || !inflight_entries.is_empty()) {
            write.abort()?;
            return Ok(None);
        }

        {
            let mut ready = write.open_table(READY)?;
            let mut inflight = write.open_table(INFLIGHT)?;
            let mut messages = write.open_table(MESSAGES)?;
            let mut attempts = write.open_table(ATTEMPTS)?;
            for (seq, message_id) in &ready_entries {
                ready.remove((queue_id, *seq))?;
                messages.remove(*message_id)?;
                attempts.remove(*message_id)?;
            }
            for message_id in &inflight_entries {
                inflight.remove(*message_id)?;
                messages.remove(*message_id)?;
                attempts.remove(*message_id)?;
            }
        }
        {
            let mut queues = write.open_table(QUEUES)?;
            queues.remove(queue)?;
        }
        {
            let mut seq = write.open_table(SEQ)?;
            seq.remove(queue_id)?;
        }

        write.commit()?;
        Ok(Some((ready_entries.len() + inflight_entries.len()) as u64))
    }

    /// Queue name that owns an in-flight delivery token.
    ///
    /// Returns `None` for unknown or stale tokens. Used by network adapters to
    /// authorize settlement without trusting the client-provided token.
    pub fn queue_for_token(&self, token: &DeliveryToken) -> Result<Option<String>> {
        let db = self.db()?;
        let read = db.begin_read()?;
        let inflight = read.open_table(INFLIGHT)?;
        let Some(raw) = inflight.get(token.message_id)? else {
            return Ok(None);
        };
        let record = DeliveryRecord::decode(raw.value())?;
        if record.delivery_id != token.delivery_id {
            return Ok(None);
        }
        let queues = read.open_table(QUEUES)?;
        for entry in queues.iter()? {
            let (name, id) = entry?;
            if id.value() == record.queue_id {
                return Ok(Some(name.value().to_string()));
            }
        }
        Ok(None)
    }

    pub fn info(&self, queue: &str) -> Result<QueueInfo> {
        let db = self.db()?;
        let read = db.begin_read()?;
        let found = {
            let queues = read.open_table(QUEUES)?;
            queues.get(queue)?.map(|value| value.value())
        };
        let id = found.ok_or_else(|| Error::QueueNotFound(queue.to_string()))?;

        let ready_table = read.open_table(READY)?;
        let mut ready = 0u64;
        for item in ready_table.range((id, 0u64)..=(id, u64::MAX))? {
            item?;
            ready += 1;
        }
        drop(ready_table);

        let inflight_table = read.open_table(INFLIGHT)?;
        let mut inflight = 0u64;
        for item in inflight_table.iter()? {
            let (_, value) = item?;
            let record = DeliveryRecord::decode(value.value())?;
            if record.queue_id == id {
                inflight += 1;
            }
        }

        Ok(QueueInfo {
            name: queue.to_string(),
            id,
            ready,
            inflight,
        })
    }

    pub fn list_queues(&self) -> Result<Vec<QueueInfo>> {
        let names: Vec<String> = {
            let db = self.db()?;
            let read = db.begin_read()?;
            let queues = read.open_table(QUEUES)?;
            let mut out = Vec::new();
            for item in queues.iter()? {
                let (name, _) = item?;
                out.push(name.value().to_string());
            }
            out
        };
        names.into_iter().map(|name| self.info(&name)).collect()
    }
}

fn push_ready(write: &WriteTransaction, message_id: u64, queue_id: u64) -> Result<()> {
    let mut ready = write.open_table(READY)?;
    let mut seq = write.open_table(SEQ)?;
    let next = seq.get(queue_id)?.map(|v| v.value()).unwrap_or(1);
    ready.insert((queue_id, next), message_id)?;
    seq.insert(queue_id, next + 1)?;
    Ok(())
}

fn requeue_inflight(
    write: &WriteTransaction,
    predicate: impl Fn(DeliveryRecord) -> bool,
) -> Result<u64> {
    let targets: Vec<(u64, DeliveryRecord)> = {
        let inflight = write.open_table(INFLIGHT)?;
        let mut out = Vec::new();
        for item in inflight.iter()? {
            let (key, value) = item?;
            let record = DeliveryRecord::decode(value.value())?;
            if predicate(record) {
                out.push((key.value(), record));
            }
        }
        out
    };
    if targets.is_empty() {
        return Ok(0);
    }
    let mut ready = write.open_table(READY)?;
    let mut seq = write.open_table(SEQ)?;
    let mut inflight = write.open_table(INFLIGHT)?;
    for (message_id, record) in &targets {
        let next = seq.get(record.queue_id)?.map(|v| v.value()).unwrap_or(1);
        ready.insert((record.queue_id, next), *message_id)?;
        seq.insert(record.queue_id, next + 1)?;
        inflight.remove(*message_id)?;
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

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
