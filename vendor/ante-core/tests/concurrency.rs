use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ante_core::{AckOutcome, Ante, NewMessage};

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(prefix: &str) -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "{prefix}-{}-{}-{}",
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

fn open() -> (TempDir, Arc<Ante>) {
    let dir = TempDir::new("ante-concurrency");
    let ante = Arc::new(Ante::open(dir.path.join("ante.sqlite3")).unwrap());
    (dir, ante)
}

#[test]
fn concurrent_publishers_do_not_lose_or_corrupt() {
    let (_dir, ante) = open();
    ante.declare_queue("jobs").unwrap();

    let writers: usize = 8;
    let per_writer: usize = 200;
    let mut handles = Vec::new();
    for writer in 0..writers {
        let ante = ante.clone();
        handles.push(std::thread::spawn(move || {
            for index in 0..per_writer {
                let payload = format!("{writer}:{index}").into_bytes();
                ante.publish(
                    "jobs",
                    NewMessage {
                        payload,
                        ..Default::default()
                    },
                )
                .unwrap();
            }
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    let info = ante.info("jobs").unwrap();
    assert_eq!(info.ready as usize, writers * per_writer);
    assert_eq!(info.inflight, 0);

    let mut seen = HashSet::new();
    let mut processed = 0;
    while processed < writers * per_writer {
        let deliveries = ante.take("jobs", 64, 5_000).unwrap();
        assert!(!deliveries.is_empty());
        let tokens: Vec<_> = deliveries
            .iter()
            .map(|delivery| delivery.token.clone())
            .collect();
        for delivery in &deliveries {
            assert!(seen.insert(delivery.payload.clone()), "duplicate payload");
        }
        let outcomes = ante.ack_batch(&tokens).unwrap();
        assert!(outcomes.iter().all(|outcome| *outcome == AckOutcome::Acked));
        processed += deliveries.len();
    }
    assert_eq!(seen.len(), writers * per_writer);
    let info = ante.info("jobs").unwrap();
    assert_eq!(info.total(), 0);
}

#[test]
fn concurrent_workers_share_the_queue_without_duplicates() {
    let (_dir, ante) = open();
    ante.declare_queue("jobs").unwrap();

    let total = 4_000usize;
    for chunk in 0..(total / 100) {
        let messages: Vec<NewMessage> = (0..100)
            .map(|offset| NewMessage {
                payload: format!("job-{}", chunk * 100 + offset).into_bytes(),
                ..Default::default()
            })
            .collect();
        ante.publish_batch("jobs", messages).unwrap();
    }

    let processed = Arc::new(AtomicUsize::new(0));
    let worker_counts = Arc::new(Mutex::new(HashMap::<usize, usize>::new()));
    let seen = Arc::new(Mutex::new(HashSet::new()));
    let duplicates = Arc::new(AtomicUsize::new(0));
    let deadline = Instant::now() + Duration::from_secs(30);
    let workers = 8;
    let start = Arc::new(Barrier::new(workers));

    let mut handles = Vec::new();
    for worker in 0..workers {
        let ante = ante.clone();
        let processed = processed.clone();
        let worker_counts = worker_counts.clone();
        let seen = seen.clone();
        let duplicates = duplicates.clone();
        let start = start.clone();
        handles.push(std::thread::spawn(move || {
            start.wait();
            let mut mine = 0;
            while processed.load(Ordering::Relaxed) < total {
                if Instant::now() > deadline {
                    return;
                }
                let deliveries = ante.take("jobs", 16, 10_000).unwrap();
                if deliveries.is_empty() {
                    std::thread::sleep(Duration::from_millis(2));
                    continue;
                }
                {
                    let mut seen = seen.lock().unwrap();
                    for delivery in &deliveries {
                        if !seen.insert(delivery.payload.clone()) {
                            duplicates.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
                let tokens: Vec<_> = deliveries
                    .iter()
                    .map(|delivery| delivery.token.clone())
                    .collect();
                let outcomes = ante.ack_batch(&tokens).unwrap();
                let acked = outcomes.iter().filter(|o| **o == AckOutcome::Acked).count();
                processed.fetch_add(acked, Ordering::Relaxed);
                mine += acked;
            }
            worker_counts.lock().unwrap().insert(worker, mine);
        }));
    }
    for handle in handles {
        handle.join().unwrap();
    }

    assert_eq!(
        duplicates.load(Ordering::Relaxed),
        0,
        "a message was delivered twice"
    );
    assert_eq!(processed.load(Ordering::Relaxed), total);
    assert_eq!(seen.lock().unwrap().len(), total);
    assert_eq!(ante.info("jobs").unwrap().total(), 0);

    // Fairness is deliberately not a contract: batches and the storage lock
    // can let one worker dominate. Only require that concurrency actually
    // happened and that the correctness invariants above held.
    let counts = worker_counts.lock().unwrap();
    let active = counts.values().filter(|count| **count > 0).count();
    assert!(
        active >= 2,
        "only {active}/{workers} workers got work: {counts:?}"
    );
}

#[test]
fn expired_lease_redelivers_and_stale_ack_is_rejected() {
    let (_dir, ante) = open();
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        NewMessage {
            payload: b"slow".to_vec(),
            ..Default::default()
        },
    )
    .unwrap();

    let first = ante.take("jobs", 1, 100).unwrap().remove(0);
    assert_eq!(first.attempts, 1);
    assert_eq!(ante.info("jobs").unwrap().inflight, 1);

    std::thread::sleep(Duration::from_millis(150));
    assert_eq!(ante.recover_expired().unwrap(), 1u64);
    assert_eq!(ante.info("jobs").unwrap().ready, 1);

    let second = ante.take("jobs", 1, 30_000).unwrap().remove(0);
    assert_eq!(second.attempts, 2);
    assert_eq!(second.message_id, first.message_id);

    assert_eq!(ante.ack(&first.token).unwrap(), AckOutcome::Stale);
    assert_eq!(
        ante.info("jobs").unwrap().inflight,
        1,
        "stale ack must not complete the redelivery"
    );
    assert_eq!(ante.ack(&second.token).unwrap(), AckOutcome::Acked);
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
}

#[test]
fn only_one_of_many_racing_acks_wins() {
    let (_dir, ante) = open();
    ante.declare_queue("jobs").unwrap();
    ante.publish(
        "jobs",
        NewMessage {
            payload: b"one".to_vec(),
            ..Default::default()
        },
    )
    .unwrap();
    let delivery = ante.take("jobs", 1, 30_000).unwrap().remove(0);

    let contenders = 8;
    let barrier = Arc::new(Barrier::new(contenders));
    let mut handles = Vec::new();
    for _ in 0..contenders {
        let ante = ante.clone();
        let token = delivery.token.clone();
        let barrier = barrier.clone();
        handles.push(std::thread::spawn(move || {
            barrier.wait();
            ante.ack(&token).unwrap()
        }));
    }
    let outcomes: Vec<AckOutcome> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap())
        .collect();

    let acked = outcomes
        .iter()
        .filter(|outcome| **outcome == AckOutcome::Acked)
        .count();
    assert_eq!(acked, 1, "{outcomes:?}");
    assert_eq!(ante.info("jobs").unwrap().total(), 0);
}
