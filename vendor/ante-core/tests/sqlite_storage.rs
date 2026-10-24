use std::path::PathBuf;
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

use ante_core::{Ante, Error, NewMessage};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ante-sqlite-test-{}-{}-{}",
            std::process::id(),
            id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
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
fn second_owner_is_rejected_without_recovering_the_first() {
    let temp = TempDir::new();
    let path = temp.path.join("ante.sqlite3");
    let first = Ante::open(&path).unwrap();
    first.declare_queue("jobs").unwrap();
    first.publish("jobs", message(b"owned")).unwrap();
    let delivery = first.take("jobs", 1, 30_000).unwrap().remove(0);

    let error = Ante::open(&path).unwrap_err();
    assert!(matches!(error, Error::EngineOpen(_)));

    assert_eq!(
        first.ack(&delivery.token).unwrap(),
        ante_core::AckOutcome::Acked
    );
    assert_eq!(first.info("jobs").unwrap().total(), 0);
}

#[test]
fn owner_lock_is_released_when_the_engine_closes() {
    let temp = TempDir::new();
    let path = temp.path.join("ante.sqlite3");
    {
        let ante = Ante::open(&path).unwrap();
        ante.declare_queue("jobs").unwrap();
    }
    let ante = Ante::open(&path).unwrap();
    assert!(ante.queue_exists("jobs").unwrap());
}

#[test]
fn a_non_sqlite_file_is_rejected_and_left_untouched() {
    let temp = TempDir::new();
    let path = temp.path.join("ante.redb");
    let original = b"not a sqlite database";
    std::fs::write(&path, original).unwrap();

    let error = Ante::open(&path).unwrap_err();
    assert!(matches!(error, Error::UnsupportedFormat(_)));
    assert_eq!(std::fs::read(&path).unwrap(), original);
}

#[test]
fn purge_does_not_remove_inflight_entries() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.publish("jobs", message(b"one")).unwrap();
    ante.publish("jobs", message(b"two")).unwrap();
    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);

    assert_eq!(ante.purge("jobs").unwrap(), 1);
    let info = ante.info("jobs").unwrap();
    assert_eq!(info.ready, 0);
    assert_eq!(info.inflight, 1);
    assert_eq!(ante.delete_queue("jobs").unwrap(), 1);
    assert!(!ante.queue_exists("jobs").unwrap());
    assert_eq!(
        ante.ack(&delivery.token).unwrap(),
        ante_core::AckOutcome::Unknown
    );
}

#[test]
fn invalid_batch_stores_nothing() {
    let temp = TempDir::new();
    let options = ante_core::Options {
        max_payload_bytes: 4,
        ..ante_core::Options::default()
    };
    let ante = Ante::open_with(temp.path.join("ante.sqlite3"), options).unwrap();
    ante.declare_queue("jobs").unwrap();

    let error = ante
        .publish_batch("jobs", vec![message(b"ok"), message(b"too-large")])
        .unwrap_err();
    assert!(matches!(error, Error::PayloadTooLarge { .. }));
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
}

#[test]
fn ack_batch_reports_stale_and_unknown_without_settling_them() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.publish("jobs", message(b"one")).unwrap();
    let first = ante.take("jobs", 1, 30_000).unwrap().remove(0);
    ante.requeue(&first.token).unwrap();
    let second = ante.take("jobs", 1, 30_000).unwrap().remove(0);

    let missing = ante_core::DeliveryToken {
        message_id: 999_999,
        delivery_id: 1,
    };
    let outcomes = ante
        .ack_batch(&[first.token, missing, second.token])
        .unwrap();
    assert_eq!(
        outcomes,
        vec![
            ante_core::AckOutcome::Stale,
            ante_core::AckOutcome::Unknown,
            ante_core::AckOutcome::Acked,
        ]
    );
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
}

#[test]
fn crash_child() {
    let Ok(path) = std::env::var("ANTE_CRASH_DB") else {
        return;
    };
    let ante = Ante::open(path).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.publish("jobs", message(b"survives")).unwrap();
    let _ = ante.take("jobs", 1, 30_000).unwrap();
    std::process::exit(0);
}

#[test]
fn confirmed_publish_survives_process_exit() {
    let temp = TempDir::new();
    let path = temp.path.join("ante.sqlite3");
    let status = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "crash_child", "--nocapture"])
        .env("ANTE_CRASH_DB", &path)
        .status()
        .unwrap();
    assert!(status.success());

    let ante = Ante::open(&path).unwrap();
    let info = ante.info("jobs").unwrap();
    assert_eq!(info.ready, 1);
    assert_eq!(info.inflight, 0);
    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);
    assert_eq!(delivery.payload, b"survives");
}
