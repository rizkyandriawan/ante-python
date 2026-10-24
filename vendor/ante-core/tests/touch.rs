use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ante_core::{Ante, MessageState, NewMessage, TouchOutcome};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ante-touch-test-{}-{}-{}",
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
fn touch_prevents_lease_expiry_without_changing_history() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    let id = ante.publish("jobs", message(b"long")).unwrap();
    let delivery = ante.take("jobs", 1, 1).unwrap().remove(0);

    assert_eq!(
        ante.touch(&delivery.token, 1000).unwrap(),
        TouchOutcome::Extended
    );
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(ante.recover_expired().unwrap(), 0);
    let status = ante.message_status("jobs", id).unwrap().unwrap();
    assert_eq!(status.state, MessageState::Inflight);
    assert_eq!(status.history.len(), 1);
    assert_eq!(status.history[0].outcome, MessageState::Inflight);
}

#[test]
fn zero_lease_uses_the_engine_default() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.publish("jobs", message(b"default")).unwrap();
    let delivery = ante.take("jobs", 1, 1).unwrap().remove(0);

    assert_eq!(
        ante.touch(&delivery.token, 0).unwrap(),
        TouchOutcome::Extended
    );
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(ante.recover_expired().unwrap(), 0);
}

#[test]
fn stale_and_unknown_tokens_do_not_extend() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.publish("jobs", message(b"one")).unwrap();

    let first = ante.take("jobs", 1, 1000).unwrap().remove(0);
    ante.requeue(&first.token).unwrap();
    let second = ante.take("jobs", 1, 1000).unwrap().remove(0);
    assert_eq!(ante.touch(&first.token, 5000).unwrap(), TouchOutcome::Stale);
    assert_eq!(
        ante.touch(
            &ante_core::DeliveryToken {
                message_id: 999,
                delivery_id: 999
            },
            5000
        )
        .unwrap(),
        TouchOutcome::Unknown
    );
    ante.ack(&second.token).unwrap();
    assert_eq!(
        ante.touch(&second.token, 5000).unwrap(),
        TouchOutcome::Unknown
    );
}
