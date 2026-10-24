use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ante_core::{Ante, MessageState, NewMessage};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ante-history-test-{}-{}-{}",
            std::process::id(),
            id,
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&path).unwrap();
        TempDir { path }
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn message(payload: &[u8]) -> NewMessage {
    NewMessage {
        payload: payload.to_vec(),
        ..Default::default()
    }
}

#[test]
fn status_tracks_each_attempt_through_ack() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    let id = ante.publish("jobs", message(b"work")).unwrap();

    let ready = ante.message_status("jobs", id).unwrap().unwrap();
    assert_eq!(ready.state, MessageState::Ready);
    assert_eq!(ready.attempts, 0);
    assert!(ready.created_at_ms > 0);
    assert!(ready.history.is_empty());

    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);
    let inflight = ante.message_status("jobs", id).unwrap().unwrap();
    assert_eq!(inflight.state, MessageState::Inflight);
    assert_eq!(inflight.attempts, 1);
    assert_eq!(inflight.history.len(), 1);
    assert_eq!(inflight.history[0].outcome, MessageState::Inflight);
    assert_eq!(inflight.history[0].finished_at_ms, None);

    ante.requeue(&delivery.token).unwrap();
    let requeued = ante.message_status("jobs", id).unwrap().unwrap();
    assert_eq!(requeued.state, MessageState::Ready);
    assert_eq!(requeued.history.len(), 1);
    assert_eq!(requeued.history[0].outcome, MessageState::Requeued);
    assert!(requeued.history[0].finished_at_ms.is_some());

    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);
    ante.ack(&delivery.token).unwrap();
    let acked = ante.message_status("jobs", id).unwrap().unwrap();
    assert_eq!(acked.state, MessageState::Acked);
    assert_eq!(acked.attempts, 2);
    assert_eq!(acked.history.len(), 2);
    assert_eq!(acked.history[1].outcome, MessageState::Acked);
    assert!(acked.history[1].finished_at_ms.is_some());
    assert_eq!(ante.info("jobs").unwrap().total(), 0);

    assert!(ante.message_status("jobs", id + 1).unwrap().is_none());
    assert!(ante.message_status("other", id).unwrap().is_none());
}

#[test]
fn expired_attempt_is_recorded_before_redelivery() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    let id = ante.publish("jobs", message(b"slow")).unwrap();

    let _delivery = ante.take("jobs", 1, 1).unwrap().remove(0);
    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(ante.recover_expired().unwrap(), 1);

    let status = ante.message_status("jobs", id).unwrap().unwrap();
    assert_eq!(status.state, MessageState::Ready);
    assert_eq!(status.attempts, 1);
    assert_eq!(status.history[0].outcome, MessageState::Expired);
}

#[test]
fn discard_is_distinct_from_ack_in_history() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    let id = ante.publish("jobs", message(b"poison")).unwrap();
    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);

    ante.discard(&delivery.token).unwrap();
    let status = ante.message_status("jobs", id).unwrap().unwrap();
    assert_eq!(status.state, MessageState::Discarded);
    assert_eq!(status.history[0].outcome, MessageState::Discarded);
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
}

#[test]
fn deleting_a_queue_discards_unfinished_attempts_but_keeps_history() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    let id = ante.publish("jobs", message(b"orphan")).unwrap();
    let _delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);

    ante.delete_queue("jobs").unwrap();
    let status = ante.message_status("jobs", id).unwrap().unwrap();
    assert_eq!(status.state, MessageState::Discarded);
    assert_eq!(status.history.len(), 1);
    assert!(status.history[0].finished_at_ms.is_some());
}

#[test]
fn deleting_a_queue_marks_ready_messages_discarded() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    let id = ante.publish("jobs", message(b"requeued")).unwrap();
    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);
    ante.requeue(&delivery.token).unwrap();

    ante.delete_queue("jobs").unwrap();
    let status = ante.message_status("jobs", id).unwrap().unwrap();
    assert_eq!(status.state, MessageState::Discarded);
    assert_eq!(status.history[0].outcome, MessageState::Requeued);
}

#[test]
fn version_one_database_is_migrated_with_timestamps() {
    let temp = TempDir::new();
    let path = temp.path.join("ante.sqlite3");
    {
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute_batch(
                r#"
                CREATE TABLE metadata (key TEXT PRIMARY KEY NOT NULL, value INTEGER NOT NULL);
                INSERT INTO metadata VALUES ('schema_version', 1);
                INSERT INTO metadata VALUES ('next_queue', 2);
                INSERT INTO metadata VALUES ('next_message', 2);
                INSERT INTO metadata VALUES ('next_delivery', 1);
                CREATE TABLE queues (
                    id INTEGER PRIMARY KEY NOT NULL,
                    name TEXT NOT NULL UNIQUE,
                    next_ready_seq INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE entries (
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
                    UNIQUE (queue_id, ready_seq)
                );
                INSERT INTO queues VALUES (1, 'jobs', 2);
                INSERT INTO entries
                    (entry_id, queue_id, ready_seq, payload, state, attempts)
                VALUES (1, 1, 1, x'6f6c64', 'ready', 0);
                PRAGMA application_id = 0x414E5445;
                PRAGMA user_version = 1;
                "#,
            )
            .unwrap();
    }

    let ante = Ante::open(&path).unwrap();
    let status = ante.message_status("jobs", 1).unwrap().unwrap();
    assert_eq!(status.state, MessageState::Ready);
    assert!(status.created_at_ms > 0);

    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);
    ante.ack(&delivery.token).unwrap();
    drop(ante);

    let connection = rusqlite::Connection::open(&path).unwrap();
    let version: i64 = connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(version, 2);
    let outcome: String = connection
        .query_row(
            "SELECT outcome FROM delivery_attempts WHERE entry_id = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(outcome, "acked");
}
