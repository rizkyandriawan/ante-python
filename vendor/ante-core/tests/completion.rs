use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use ante_core::{Ante, Error, MessageState, NewMessage, Settlement};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ante-completion-test-{}-{}-{}",
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
fn complete_publishes_the_result_and_acks_in_one_transaction() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.declare_queue("results").unwrap();
    let job_id = ante.publish("jobs", message(b"input")).unwrap();
    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);

    let completion = ante
        .complete(
            &delivery.token,
            "results",
            message(b"output"),
            Settlement::Ack,
        )
        .unwrap();
    assert_eq!(completion.outcome, ante_core::AckOutcome::Acked);
    assert!(completion.message_id.is_some());

    assert_eq!(ante.info("jobs").unwrap().total(), 0);
    assert_eq!(ante.info("results").unwrap().ready, 1);
    let status = ante.message_status("jobs", job_id).unwrap().unwrap();
    assert_eq!(status.state, MessageState::Acked);
    let result = ante.take("results", 1, 30_000).unwrap().remove(0);
    assert_eq!(result.payload, b"output");
    ante.ack(&result.token).unwrap();
}

#[test]
fn stale_completion_does_not_publish() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.declare_queue("results").unwrap();
    ante.publish("jobs", message(b"input")).unwrap();
    let first = ante.take("jobs", 1, 30_000).unwrap().remove(0);
    ante.requeue(&first.token).unwrap();
    let _second = ante.take("jobs", 1, 30_000).unwrap().remove(0);

    let completion = ante
        .complete(&first.token, "results", message(b"stale"), Settlement::Ack)
        .unwrap();
    assert_eq!(completion.outcome, ante_core::AckOutcome::Stale);
    assert!(completion.message_id.is_none());
    assert_eq!(ante.info("results").unwrap().total(), 0);
    assert_eq!(ante.info("jobs").unwrap().total(), 1);
}

#[test]
fn missing_destination_aborts_without_settling_the_input() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.publish("jobs", message(b"input")).unwrap();
    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);

    let error = ante
        .complete(
            &delivery.token,
            "missing",
            message(b"output"),
            Settlement::Ack,
        )
        .unwrap_err();
    assert!(matches!(error, Error::QueueNotFound(_)));
    assert_eq!(ante.info("jobs").unwrap().inflight, 1);

    ante.complete(
        &delivery.token,
        "jobs",
        message(b"same queue"),
        Settlement::Ack,
    )
    .unwrap();
    assert_eq!(ante.info("jobs").unwrap().ready, 1);
    assert_eq!(ante.info("jobs").unwrap().inflight, 0);
}

#[test]
fn complete_can_discard_instead_of_ack() {
    let temp = TempDir::new();
    let ante = Ante::open(temp.path.join("ante.sqlite3")).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.declare_queue("failed").unwrap();
    let job_id = ante.publish("jobs", message(b"input")).unwrap();
    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);

    ante.complete(
        &delivery.token,
        "failed",
        message(b"failure"),
        Settlement::Discard,
    )
    .unwrap();
    let status = ante.message_status("jobs", job_id).unwrap().unwrap();
    assert_eq!(status.state, MessageState::Discarded);
    assert_eq!(ante.info("failed").unwrap().ready, 1);
}
