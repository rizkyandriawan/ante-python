use std::path::PathBuf;
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
