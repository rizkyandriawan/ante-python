use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ante_core::{Ante, MessageState, NewMessage, Options};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ante-retention-test-{}-{}-{}",
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

fn ack_one(ante: &Ante) -> u64 {
    let id = ante.publish("jobs", message(b"job")).unwrap();
    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);
    ante.ack(&delivery.token).unwrap();
    id
}

#[test]
fn age_limit_removes_finished_history() {
    let temp = TempDir::new();
    let ante = Ante::open_with(
        temp.path.join("ante.sqlite3"),
        Options {
            history_retention_ms: 1,
            history_max_rows: 0,
            ..Default::default()
        },
    )
    .unwrap();
    ante.declare_queue("jobs").unwrap();
    let id = ack_one(&ante);

    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(ante.prune_history().unwrap(), 1);
    assert!(ante.message_status("jobs", id).unwrap().is_none());
}

#[test]
fn row_limit_keeps_the_newest_finished_attempts() {
    let temp = TempDir::new();
    let ante = Ante::open_with(
        temp.path.join("ante.sqlite3"),
        Options {
            history_retention_ms: 0,
            history_max_rows: 2,
            ..Default::default()
        },
    )
    .unwrap();
    ante.declare_queue("jobs").unwrap();
    let first = ack_one(&ante);
    let second = ack_one(&ante);
    let third = ack_one(&ante);

    assert_eq!(ante.prune_history().unwrap(), 1);
    assert!(ante.message_status("jobs", first).unwrap().is_none());
    assert_eq!(
        ante.message_status("jobs", second).unwrap().unwrap().state,
        MessageState::Acked
    );
    assert_eq!(
        ante.message_status("jobs", third).unwrap().unwrap().state,
        MessageState::Acked
    );
}

#[test]
fn unfinished_attempts_are_never_pruned() {
    let temp = TempDir::new();
    let ante = Ante::open_with(
        temp.path.join("ante.sqlite3"),
        Options {
            history_retention_ms: 1,
            history_max_rows: 1,
            ..Default::default()
        },
    )
    .unwrap();
    ante.declare_queue("jobs").unwrap();
    let id = ante.publish("jobs", message(b"running")).unwrap();
    let _delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);

    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(ante.prune_history().unwrap(), 0);
    let status = ante.message_status("jobs", id).unwrap().unwrap();
    assert_eq!(status.state, MessageState::Inflight);
    assert_eq!(status.history.len(), 1);
}

#[test]
fn startup_applies_the_retention_policy() {
    let temp = TempDir::new();
    let path = temp.path.join("ante.sqlite3");
    let id = {
        let ante = Ante::open(&path).unwrap();
        ante.declare_queue("jobs").unwrap();
        ack_one(&ante)
    };
    std::thread::sleep(Duration::from_millis(5));

    let ante = Ante::open_with(
        &path,
        Options {
            history_retention_ms: 1,
            ..Default::default()
        },
    )
    .unwrap();
    assert!(ante.message_status("jobs", id).unwrap().is_none());
}
