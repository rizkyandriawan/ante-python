use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use ante_core::{AckOutcome, Ante, DeliveryToken, Error, NewMessage, Options, RequeueOutcome};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDb {
    dir: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "ante-core-test-{}-{}-{}",
            std::process::id(),
            id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        TempDb { dir }
    }
    fn path(&self) -> PathBuf {
        self.dir.join("ante.sqlite3")
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn message(payload: &[u8]) -> NewMessage {
    NewMessage {
        payload: payload.to_vec(),
        ..Default::default()
    }
}

#[test]
fn publish_take_ack_removes_message() {
    let tmp = TempDb::new();
    let ante = Ante::open(tmp.path()).unwrap();
    ante.declare_queue("jobs").unwrap();

    let id = ante.publish("jobs", message(b"hello")).unwrap();
    assert_eq!(ante.info("jobs").unwrap().ready, 1);

    let deliveries = ante.take("jobs", 1, 1000).unwrap();
    assert_eq!(deliveries.len(), 1);
    assert_eq!(deliveries[0].message_id, id);
    assert_eq!(deliveries[0].payload, b"hello");
    assert_eq!(ante.info("jobs").unwrap().inflight, 1);

    assert_eq!(ante.ack(&deliveries[0].token).unwrap(), AckOutcome::Acked);
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
}

#[test]
fn stale_ack_does_not_complete_a_redelivery() {
    let tmp = TempDb::new();
    let ante = Ante::open(tmp.path()).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.publish("jobs", message(b"x")).unwrap();

    let first = ante.take("jobs", 1, 1000).unwrap().remove(0);
    assert_eq!(
        ante.requeue(&first.token).unwrap(),
        RequeueOutcome::Requeued
    );

    let second = ante.take("jobs", 1, 1000).unwrap().remove(0);
    assert_ne!(first.token.delivery_id, second.token.delivery_id);

    assert_eq!(ante.ack(&first.token).unwrap(), AckOutcome::Stale);
    assert_eq!(ante.info("jobs").unwrap().inflight, 1);

    assert_eq!(ante.ack(&second.token).unwrap(), AckOutcome::Acked);
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
}

#[test]
fn messages_survive_reopen() {
    let tmp = TempDb::new();
    {
        let ante = Ante::open(tmp.path()).unwrap();
        ante.declare_queue("jobs").unwrap();
        ante.publish("jobs", message(b"durable")).unwrap();
    }
    let ante = Ante::open(tmp.path()).unwrap();
    let info = ante.info("jobs").unwrap();
    assert_eq!(info.ready, 1);

    let delivery = ante.take("jobs", 1, 1000).unwrap().remove(0);
    assert_eq!(delivery.payload, b"durable");
}

#[test]
fn reopen_requeues_abandoned_inflight_messages() {
    let tmp = TempDb::new();
    {
        let ante = Ante::open(tmp.path()).unwrap();
        ante.declare_queue("jobs").unwrap();
        ante.publish("jobs", message(b"crashed")).unwrap();
        let delivery = ante.take("jobs", 1, 60_000).unwrap().remove(0);
        assert_eq!(delivery.payload, b"crashed");
        // Process "dies" without acking.
    }
    let ante = Ante::open(tmp.path()).unwrap();
    let info = ante.info("jobs").unwrap();
    assert_eq!(info.inflight, 0);
    assert_eq!(info.ready, 1);
    let delivery = ante.take("jobs", 1, 1000).unwrap().remove(0);
    assert_eq!(delivery.payload, b"crashed");
}

#[test]
fn expired_lease_is_requeued() {
    let tmp = TempDb::new();
    let ante = Ante::open(tmp.path()).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.publish("jobs", message(b"slow")).unwrap();

    let delivery = ante.take("jobs", 1, 1).unwrap().remove(0);
    std::thread::sleep(std::time::Duration::from_millis(10));
    assert_eq!(ante.recover_expired().unwrap(), 1);

    let again = ante.take("jobs", 1, 1000).unwrap().remove(0);
    assert_eq!(again.message_id, delivery.message_id);
    assert_ne!(again.token.delivery_id, delivery.token.delivery_id);
}

#[test]
fn queues_are_isolated() {
    let tmp = TempDb::new();
    let ante = Ante::open(tmp.path()).unwrap();
    ante.declare_queue("a").unwrap();
    ante.declare_queue("b").unwrap();

    ante.publish("a", message(b"one")).unwrap();
    ante.publish("b", message(b"two")).unwrap();

    let from_a = ante.take("a", 10, 1000).unwrap();
    assert_eq!(from_a.len(), 1);
    assert_eq!(from_a[0].payload, b"one");
    assert_eq!(ante.info("a").unwrap().ready, 0);
    assert_eq!(ante.info("b").unwrap().ready, 1);
}

#[test]
fn batch_publish_and_ack_are_atomic_per_call() {
    let tmp = TempDb::new();
    let ante = Ante::open(tmp.path()).unwrap();
    ante.declare_queue("jobs").unwrap();

    let ids = ante
        .publish_batch(
            "jobs",
            (0..10)
                .map(|i| message(format!("m{i}").as_bytes()))
                .collect(),
        )
        .unwrap();
    assert_eq!(ids.len(), 10);

    let deliveries = ante.take("jobs", 10, 1000).unwrap();
    assert_eq!(deliveries.len(), 10);
    let tokens: Vec<DeliveryToken> = deliveries.iter().map(|d| d.token.clone()).collect();
    let outcomes = ante.ack_batch(&tokens).unwrap();
    assert!(outcomes.iter().all(|o| *o == AckOutcome::Acked));
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
}

#[test]
fn rejects_unknown_queue_and_oversized_payload() {
    let tmp = TempDb::new();
    let options = Options {
        max_payload_bytes: 4,
        ..Options::default()
    };
    let ante = Ante::open_with(tmp.path(), options).unwrap();

    assert!(matches!(
        ante.publish("missing", message(b"x")),
        Err(Error::QueueNotFound(_))
    ));

    ante.declare_queue("jobs").unwrap();
    assert!(matches!(
        ante.publish("jobs", message(b"too-big")),
        Err(Error::PayloadTooLarge { .. })
    ));
}

#[test]
fn preserves_order_within_a_queue() {
    let tmp = TempDb::new();
    let ante = Ante::open(tmp.path()).unwrap();
    ante.declare_queue("jobs").unwrap();
    for i in 0..5 {
        ante.publish("jobs", message(format!("{i}").as_bytes()))
            .unwrap();
    }
    let deliveries = ante.take("jobs", 5, 1000).unwrap();
    let payloads: Vec<String> = deliveries
        .iter()
        .map(|d| String::from_utf8(d.payload.clone()).unwrap())
        .collect();
    assert_eq!(payloads, ["0", "1", "2", "3", "4"]);
}

#[test]
fn keeps_utf8_properties() {
    let tmp = TempDb::new();
    let ante = Ante::open(tmp.path()).unwrap();
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        NewMessage {
            payload: b"{}".to_vec(),
            content_type: Some("application/json".into()),
            correlation_id: Some("corr-1".into()),
            reply_to: Some("reply".into()),
            ..Default::default()
        },
    )
    .unwrap();

    let delivery = ante.take("jobs", 1, 1000).unwrap().remove(0);
    assert_eq!(delivery.content_type.as_deref(), Some("application/json"));
    assert_eq!(delivery.correlation_id.as_deref(), Some("corr-1"));
    assert_eq!(delivery.reply_to.as_deref(), Some("reply"));
}

#[test]
fn token_roundtrip() {
    let token = DeliveryToken {
        message_id: 42,
        delivery_id: 7,
    };
    assert_eq!(DeliveryToken::parse(&token.to_wire()).unwrap(), token);
    assert!(DeliveryToken::parse("nope").is_err());
}

#[test]
fn conditional_delete_requires_an_empty_queue() {
    let temp = TempDb::new();
    let ante = Ante::open(temp.path()).unwrap();
    let first_id = ante.declare_queue("guarded").unwrap();
    ante.publish("guarded", message(b"one")).unwrap();
    assert_eq!(ante.delete_queue_if_empty("guarded").unwrap(), None);
    assert_eq!(ante.info("guarded").unwrap().ready, 1);

    let delivery = ante.take("guarded", 1, 30_000).unwrap().remove(0);
    assert_eq!(ante.delete_queue_if_empty("guarded").unwrap(), None);
    ante.ack(&delivery.token).unwrap();

    assert_eq!(ante.delete_queue_if_empty("guarded").unwrap(), Some(0));
    assert!(!ante.queue_exists("guarded").unwrap());

    let second_id = ante.declare_queue("guarded").unwrap();
    assert_ne!(first_id, second_id, "recreated queue must have a new id");
}

#[test]
fn identity_checked_take_cannot_pull_from_a_recreated_queue() {
    let temp = TempDb::new();
    let ante = Ante::open(temp.path()).unwrap();
    let old_id = ante.declare_queue("recycled").unwrap();
    ante.delete_queue("recycled").unwrap();
    let new_id = ante.declare_queue("recycled").unwrap();
    assert_ne!(old_id, new_id);
    ante.publish("recycled", message(b"new")).unwrap();

    assert!(ante
        .take_if_queue_id("recycled", old_id, 1, 30_000)
        .unwrap()
        .is_none());
    assert_eq!(ante.info("recycled").unwrap().ready, 1);

    let taken = ante
        .take_if_queue_id("recycled", new_id, 1, 30_000)
        .unwrap()
        .unwrap();
    assert_eq!(taken.len(), 1);
    assert_eq!(taken[0].payload, b"new");
}
