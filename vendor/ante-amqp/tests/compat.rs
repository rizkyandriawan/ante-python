use std::io::{self, ErrorKind};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ante_amqp::frame::{
    method_payload, read_frame, write_frame, Encoder, TableValue, BASIC_GET, BASIC_PUBLISH,
    CHANNEL_OPEN, CHANNEL_OPEN_OK, CLASS_BASIC, CLASS_CHANNEL, CLASS_CONNECTION, CONNECTION_CLOSE,
    CONNECTION_OPEN, CONNECTION_OPEN_OK, CONNECTION_START, CONNECTION_START_OK, CONNECTION_TUNE,
    CONNECTION_TUNE_OK, FRAME_BODY, FRAME_HEADER, FRAME_HEARTBEAT, FRAME_METHOD,
};
use ante_core::Ante;
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicCancelOptions, BasicConsumeOptions, BasicGetOptions, BasicNackOptions,
    BasicPublishOptions, BasicQosOptions, ConfirmSelectOptions, QueueDeclareOptions,
    QueueDeleteOptions, QueuePurgeOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let id = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ante-amqp-test-{}-{}-{}",
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

struct Harness {
    url: String,
    port: u16,
    ante: Arc<Ante>,
    shutdown: Option<oneshot::Sender<()>>,
    server: tokio::task::JoinHandle<()>,
    _dir: TempDir,
}

impl Harness {
    async fn start() -> Self {
        Self::start_with_options(ante_amqp::ServerOptions::default()).await
    }

    async fn start_with_lease(lease_ms: u64) -> Self {
        Self::start_with_options(ante_amqp::ServerOptions {
            lease_ms,
            ..Default::default()
        })
        .await
    }

    async fn start_with_options(options: ante_amqp::ServerOptions) -> Self {
        let dir = TempDir::new();
        let ante = Arc::new(Ante::open(dir.path.join("ante.sqlite3")).unwrap());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let (tx, rx) = oneshot::channel();
        let server_ante = ante.clone();
        let server = tokio::spawn(async move {
            ante_amqp::serve_until_with_options(listener, server_ante, options, rx)
                .await
                .unwrap();
        });
        Harness {
            url: format!("amqp://guest:guest@127.0.0.1:{port}/%2f"),
            port,
            ante,
            shutdown: Some(tx),
            server,
            _dir: dir,
        }
    }

    async fn connection(&self) -> Connection {
        Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .expect("connect")
    }

    fn info(&self, queue: &str) -> ante_core::QueueInfo {
        self.ante.info(queue).unwrap()
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        self.server.abort();
    }
}

async fn declare(channel: &lapin::Channel, queue: &str) {
    channel
        .queue_declare(
            queue,
            QueueDeclareOptions {
                durable: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .expect("queue declare");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lapin_publish_consume_ack() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "jobs").await;

    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();

    channel
        .basic_publish(
            "",
            "jobs",
            BasicPublishOptions::default(),
            b"hello from amqp",
            BasicProperties::default()
                .with_delivery_mode(2)
                .with_content_type("text/plain".into()),
        )
        .await
        .unwrap();

    assert!(channel.wait_for_confirms().await.unwrap().is_empty());
    assert_eq!(harness.info("jobs").ready, 1);

    let mut consumer = channel
        .basic_consume(
            "jobs",
            "worker-1",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("delivery timeout")
        .expect("consumer ended")
        .expect("delivery error");

    assert_eq!(delivery.data, b"hello from amqp");
    assert_eq!(delivery.routing_key.as_str(), "jobs");
    assert_eq!(
        delivery
            .properties
            .content_type()
            .as_ref()
            .map(|value| value.as_str()),
        Some("text/plain")
    );

    delivery.ack(BasicAckOptions::default()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(harness.info("jobs").total(), 0);

    connection.close(200, "bye").await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_message_survives_reconnect() {
    let harness = Harness::start().await;
    {
        let connection = harness.connection().await;
        let channel = connection.create_channel().await.unwrap();
        declare(&channel, "durable").await;
        channel
            .basic_publish(
                "",
                "durable",
                BasicPublishOptions::default(),
                b"survive",
                BasicProperties::default().with_delivery_mode(2),
            )
            .await
            .unwrap();
        connection.close(200, "bye").await.unwrap();
    }

    assert_eq!(harness.info("durable").ready, 1);

    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "durable").await;

    let message = tokio::time::timeout(
        Duration::from_secs(5),
        channel.basic_get("durable", BasicGetOptions::default()),
    )
    .await
    .expect("get timeout")
    .unwrap()
    .expect("message");
    assert_eq!(message.data, b"survive");
    message.ack(BasicAckOptions::default()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconnect_requeues_unacked_delivery() {
    let harness = Harness::start().await;
    {
        let connection = harness.connection().await;
        let channel = connection.create_channel().await.unwrap();
        declare(&channel, "recover").await;
        channel
            .basic_publish(
                "",
                "recover",
                BasicPublishOptions::default(),
                b"unacked",
                BasicProperties::default(),
            )
            .await
            .unwrap();

        let mut consumer = channel
            .basic_consume(
                "recover",
                "worker",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        let delivery = consumer.next().await.unwrap().unwrap();
        assert_eq!(delivery.data, b"unacked");
        // Drop the connection without acking.
        drop(delivery);
        connection.close(200, "crash").await.unwrap();
    }

    tokio::time::sleep(Duration::from_millis(200)).await;
    let info = harness.info("recover");
    assert_eq!(info.inflight, 0);
    assert_eq!(info.ready, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nack_requeues_for_another_attempt() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "retry").await;
    channel
        .basic_publish(
            "",
            "retry",
            BasicPublishOptions::default(),
            b"try-again",
            BasicProperties::default(),
        )
        .await
        .unwrap();

    let mut consumer = channel
        .basic_consume(
            "retry",
            "worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let delivery = consumer.next().await.unwrap().unwrap();
    channel
        .basic_nack(
            delivery.delivery_tag,
            BasicNackOptions {
                requeue: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let again = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("redelivery timeout")
        .expect("consumer ended")
        .expect("delivery");
    assert_eq!(again.data, b"try-again");
    assert!(again.redelivered);
    again.ack(BasicAckOptions::default()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn get_empty_on_unknown_message() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "empty").await;

    let message = channel
        .basic_get("empty", BasicGetOptions::default())
        .await
        .unwrap();
    assert!(message.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_consumers_share_a_queue_without_duplicates() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let producer = connection.create_channel().await.unwrap();
    declare(&producer, "shared").await;
    producer
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for index in 0..100 {
        producer
            .basic_publish(
                "",
                "shared",
                BasicPublishOptions::default(),
                format!("job-{index}").as_bytes(),
                BasicProperties::default(),
            )
            .await
            .unwrap();
    }
    assert!(producer.wait_for_confirms().await.unwrap().is_empty());

    let mut handles = Vec::new();
    for worker in 0..4 {
        let channel = connection.create_channel().await.unwrap();
        channel
            .basic_qos(1, BasicQosOptions::default())
            .await
            .unwrap();
        let tag = format!("worker-{worker}");
        let mut consumer = channel
            .basic_consume(
                "shared",
                &tag,
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .unwrap();
        handles.push(tokio::spawn(async move {
            let mut mine = Vec::new();
            loop {
                match tokio::time::timeout(Duration::from_secs(2), consumer.next()).await {
                    Err(_) | Ok(None) => break,
                    Ok(Some(Err(err))) => panic!("consumer error: {err}"),
                    Ok(Some(Ok(delivery))) => {
                        mine.push(delivery.data.clone());
                        delivery.ack(BasicAckOptions::default()).await.unwrap();
                    }
                }
            }
            mine
        }));
    }

    let mut all = Vec::new();
    let mut per_worker = Vec::new();
    for handle in handles {
        let mine = handle.await.unwrap();
        per_worker.push(mine.len());
        all.extend(mine);
    }
    assert_eq!(
        all.len(),
        100,
        "expected 100 deliveries, got {}; per worker {per_worker:?}",
        all.len()
    );
    all.sort();
    all.dedup();
    assert_eq!(
        all.len(),
        100,
        "duplicate or missing messages; per worker {per_worker:?}"
    );
    assert!(
        per_worker.iter().all(|count| *count > 0),
        "unused consumer: {per_worker:?}"
    );

    let info = harness.info("shared");
    assert_eq!(info.ready, 0);
    assert_eq!(info.inflight, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_ack_acknowledges_all_prior_tags() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "batch-ack").await;
    channel
        .basic_qos(8, BasicQosOptions::default())
        .await
        .unwrap();
    for index in 0..5 {
        channel
            .basic_publish(
                "",
                "batch-ack",
                BasicPublishOptions::default(),
                format!("m{index}").as_bytes(),
                BasicProperties::default(),
            )
            .await
            .unwrap();
    }

    let mut consumer = channel
        .basic_consume(
            "batch-ack",
            "worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut last_tag = 0;
    for _ in 0..5 {
        let delivery = consumer.next().await.unwrap().unwrap();
        last_tag = delivery.delivery_tag;
    }
    assert_eq!(harness.info("batch-ack").inflight, 5);

    channel
        .basic_ack(last_tag, BasicAckOptions { multiple: true })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(harness.info("batch-ack").total(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_consumers_on_one_channel_respect_prefetch() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let producer = connection.create_channel().await.unwrap();
    declare(&producer, "prefetch").await;
    producer
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();
    for index in 0..3 {
        producer
            .basic_publish(
                "",
                "prefetch",
                BasicPublishOptions::default(),
                format!("p{index}").as_bytes(),
                BasicProperties::default(),
            )
            .await
            .unwrap();
    }
    assert!(producer.wait_for_confirms().await.unwrap().is_empty());

    let channel = connection.create_channel().await.unwrap();
    channel
        .basic_qos(1, BasicQosOptions::default())
        .await
        .unwrap();
    let mut first = channel
        .basic_consume(
            "prefetch",
            "first",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let mut second = channel
        .basic_consume(
            "prefetch",
            "second",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let mut acked = 0;
    while acked < 3 {
        let delivery = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::select! {
                delivery = first.next() => delivery,
                delivery = second.next() => delivery,
            }
        })
        .await
        .expect("delivery timeout")
        .expect("consumer ended")
        .expect("delivery error");

        let extra = tokio::time::timeout(Duration::from_millis(250), async {
            tokio::select! {
                delivery = first.next() => delivery,
                delivery = second.next() => delivery,
            }
        })
        .await;
        assert!(
            extra.is_err(),
            "prefetch=1 was violated by a second delivery"
        );

        delivery.ack(BasicAckOptions::default()).await.unwrap();
        acked += 1;
    }

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(harness.info("prefetch").total(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lease_expiry_redelivers_without_blocking_the_channel() {
    let harness = Harness::start_with_lease(150).await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "lease").await;
    channel
        .basic_qos(1, BasicQosOptions::default())
        .await
        .unwrap();
    channel
        .basic_publish(
            "",
            "lease",
            BasicPublishOptions::default(),
            b"slow",
            BasicProperties::default(),
        )
        .await
        .unwrap();

    let mut consumer = channel
        .basic_consume(
            "lease",
            "worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let first = consumer.next().await.unwrap().unwrap();
    assert_eq!(first.data, b"slow");
    assert!(!first.redelivered);
    // Do not ack: let the lease expire and the channel capacity recover.

    let second = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("lease was not recovered")
        .expect("consumer ended")
        .expect("delivery error");
    assert_eq!(second.data, b"slow");
    assert!(second.redelivered);
    second.ack(BasicAckOptions::default()).await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(harness.info("lease").total(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ack_zero_multiple_acknowledges_all_outstanding() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "ack-all").await;
    channel
        .basic_qos(10, BasicQosOptions::default())
        .await
        .unwrap();
    for index in 0..3 {
        channel
            .basic_publish(
                "",
                "ack-all",
                BasicPublishOptions::default(),
                format!("a{index}").as_bytes(),
                BasicProperties::default(),
            )
            .await
            .unwrap();
    }
    let mut consumer = channel
        .basic_consume(
            "ack-all",
            "worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    for _ in 0..3 {
        consumer.next().await.unwrap().unwrap();
    }
    assert_eq!(harness.info("ack-all").inflight, 3);

    channel
        .basic_ack(0, BasicAckOptions { multiple: true })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(harness.info("ack-all").total(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consume_preserves_message_id() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "properties").await;
    channel
        .basic_publish(
            "",
            "properties",
            BasicPublishOptions::default(),
            b"body",
            BasicProperties::default()
                .with_message_id("job-42".into())
                .with_content_type("text/plain".into()),
        )
        .await
        .unwrap();

    let mut consumer = channel
        .basic_consume(
            "properties",
            "worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(
        delivery
            .properties
            .message_id()
            .as_ref()
            .map(|value| value.as_str()),
        Some("job-42")
    );
    assert_eq!(
        delivery
            .properties
            .content_type()
            .as_ref()
            .map(|value| value.as_str()),
        Some("text/plain")
    );
    delivery.ack(BasicAckOptions::default()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_shutdown_stops_active_connections_and_requeues() {
    let dir = TempDir::new();
    let ante = Arc::new(Ante::open(dir.path.join("ante.sqlite3")).unwrap());
    ante.declare_queue("shutdown").unwrap();
    ante.publish(
        "shutdown",
        ante_core::NewMessage {
            payload: b"pending".to_vec(),
            ..Default::default()
        },
    )
    .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, rx) = oneshot::channel();
    let server = tokio::spawn(ante_amqp::serve_until(listener, ante.clone(), rx));

    let url = format!("amqp://guest:guest@127.0.0.1:{port}/%2f");
    let connection = Connection::connect(&url, ConnectionProperties::default())
        .await
        .unwrap();
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "shutdown").await;
    let mut consumer = channel
        .basic_consume(
            "shutdown",
            "worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let delivery = consumer.next().await.unwrap().unwrap();
    assert_eq!(delivery.data, b"pending");
    assert_eq!(ante.info("shutdown").unwrap().inflight, 1);

    tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("serve_until did not stop with an active connection")
        .unwrap()
        .unwrap();

    let info = ante.info("shutdown").unwrap();
    assert_eq!(info.inflight, 0);
    assert_eq!(info.ready, 1);
    let _ = dir;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_ack_for_expired_get_closes_the_channel() {
    let harness = Harness::start_with_lease(100).await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "late").await;
    channel
        .basic_publish(
            "",
            "late",
            BasicPublishOptions::default(),
            b"expired",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    channel
        .queue_declare(
            "late",
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    let message = channel
        .basic_get("late", BasicGetOptions::default())
        .await
        .unwrap()
        .expect("message");
    let tag = message.delivery_tag;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        if harness.info("late").inflight == 0 && harness.info("late").ready == 1 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "lease was never reclaimed"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    channel
        .basic_ack(tag, BasicAckOptions::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = channel
        .queue_declare(
            "late",
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await;
    assert!(result.is_err(), "late ack should have closed the channel");
    assert_eq!(harness.info("late").total(), 1, "message must not be lost");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn conditional_delete_refuses_non_empty_and_used_queues() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "guarded").await;
    channel
        .basic_publish(
            "",
            "guarded",
            BasicPublishOptions::default(),
            b"payload",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    channel
        .queue_declare(
            "guarded",
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    let rejected = channel
        .queue_delete(
            "guarded",
            QueueDeleteOptions {
                if_empty: true,
                ..Default::default()
            },
        )
        .await;
    assert!(rejected.is_err(), "if_empty must reject a non-empty queue");
    assert_eq!(harness.info("guarded").ready, 1, "message was deleted");

    let channel = connection.create_channel().await.unwrap();
    channel
        .queue_purge("guarded", QueuePurgeOptions::default())
        .await
        .unwrap();
    channel
        .queue_delete(
            "guarded",
            QueueDeleteOptions {
                if_empty: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!harness.ante.queue_exists("guarded").unwrap());

    declare(&channel, "busy").await;
    let _consumer = channel
        .basic_consume(
            "busy",
            "busy-worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let rejected = channel
        .queue_delete(
            "busy",
            QueueDeleteOptions {
                if_unused: true,
                ..Default::default()
            },
        )
        .await;
    assert!(rejected.is_err(), "if_unused must reject a used queue");
    assert!(harness.ante.queue_exists("busy").unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_queue_does_not_leave_its_consumer_subscribed() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "recycled").await;
    channel
        .basic_publish(
            "",
            "recycled",
            BasicPublishOptions::default(),
            b"first",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    channel
        .queue_declare(
            "recycled",
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    let mut old = channel
        .basic_consume(
            "recycled",
            "old-worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let delivery = old.next().await.unwrap().unwrap();
    delivery.ack(BasicAckOptions::default()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;

    channel
        .queue_delete("recycled", QueueDeleteOptions::default())
        .await
        .unwrap();
    declare(&channel, "recycled").await;
    channel
        .basic_publish(
            "",
            "recycled",
            BasicPublishOptions::default(),
            b"second",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    channel
        .queue_declare(
            "recycled",
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    match tokio::time::timeout(Duration::from_millis(600), old.next()).await {
        Ok(None) | Ok(Some(Err(_))) => {}
        Err(_) => panic!("server did not send basic.cancel"),
        Ok(Some(Ok(delivery))) => {
            panic!("the old consumer received {:?}", delivery.data)
        }
    }
    assert_eq!(harness.info("recycled").ready, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_consumer_tags_are_rejected() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "tags").await;

    let mut first = channel
        .basic_consume(
            "tags",
            "same-tag",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let second = channel
        .basic_consume(
            "tags",
            "same-tag",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await;
    assert!(second.is_err(), "duplicate consumer tag must be rejected");

    match tokio::time::timeout(Duration::from_millis(300), first.next()).await {
        Err(_) | Ok(None) | Ok(Some(Err(_))) => {}
        Ok(Some(Ok(delivery))) => {
            panic!("closed consumer delivered {:?}", delivery.data)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negotiated_frame_max_splits_large_deliveries() {
    let harness = Harness::start().await;
    let url = format!("{}?frame_max=4096", harness.url);
    let connection = Connection::connect(&url, ConnectionProperties::default())
        .await
        .unwrap();
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "large").await;
    channel
        .confirm_select(ConfirmSelectOptions::default())
        .await
        .unwrap();

    let payload = vec![b'x'; 20_000];
    channel
        .basic_publish(
            "",
            "large",
            BasicPublishOptions::default(),
            &payload,
            BasicProperties::default(),
        )
        .await
        .unwrap();
    assert!(channel.wait_for_confirms().await.unwrap().is_empty());

    let mut consumer = channel
        .basic_consume(
            "large",
            "large-worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("delivery timeout")
        .expect("consumer ended")
        .expect("delivery error");
    assert_eq!(delivery.data.len(), payload.len());
    delivery.ack(BasicAckOptions::default()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negotiated_channel_max_limits_channel_creation() {
    let harness = Harness::start().await;
    let url = format!("{}?channel_max=1", harness.url);
    let connection = Connection::connect(&url, ConnectionProperties::default())
        .await
        .unwrap();
    let first = connection.create_channel().await;
    assert!(first.is_ok(), "channel 1 must be accepted");
    let second = connection.create_channel().await;
    assert!(second.is_err(), "channel 2 exceeds the negotiated maximum");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_ack_with_unknown_tag_does_not_ack_messages() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "unknown-tag").await;
    for payload in [b"one".as_slice(), b"two".as_slice()] {
        channel
            .basic_publish(
                "",
                "unknown-tag",
                BasicPublishOptions::default(),
                payload,
                BasicProperties::default(),
            )
            .await
            .unwrap();
    }
    channel
        .queue_declare(
            "unknown-tag",
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();
    for _ in 0..2 {
        channel
            .basic_get("unknown-tag", BasicGetOptions::default())
            .await
            .unwrap()
            .expect("message");
    }

    channel
        .basic_ack(999, BasicAckOptions { multiple: true })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;

    let result = channel
        .queue_declare(
            "unknown-tag",
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await;
    assert!(
        result.is_err(),
        "unknown multiple tag must close the channel"
    );
    assert_eq!(harness.info("unknown-tag").total(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn if_unused_sees_consumers_from_other_connections() {
    let harness = Harness::start().await;
    let consumer_connection = harness.connection().await;
    let consumer_channel = consumer_connection.create_channel().await.unwrap();
    declare(&consumer_channel, "shared").await;
    let _consumer = consumer_channel
        .basic_consume(
            "shared",
            "worker-a",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let other = harness.connection().await;
    let other_channel = other.create_channel().await.unwrap();
    other_channel
        .basic_publish(
            "",
            "shared",
            BasicPublishOptions::default(),
            b"keep-me",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    other_channel
        .queue_declare(
            "shared",
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    let rejected = other_channel
        .queue_delete(
            "shared",
            QueueDeleteOptions {
                if_unused: true,
                ..Default::default()
            },
        )
        .await;
    assert!(
        rejected.is_err(),
        "if_unused must see consumers on other connections"
    );
    assert!(harness.ante.queue_exists("shared").unwrap());
    assert_eq!(harness.info("shared").total(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_consumer_cleanup_frees_its_tag_and_registration() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "reuse").await;
    let _old = channel
        .basic_consume(
            "reuse",
            "reuse-tag",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let other = harness.connection().await;
    let other_channel = other.create_channel().await.unwrap();
    other_channel
        .queue_delete("reuse", QueueDeleteOptions::default())
        .await
        .unwrap();
    declare(&other_channel, "reuse").await;
    tokio::time::sleep(Duration::from_millis(250)).await;

    let second = channel
        .basic_consume(
            "reuse",
            "reuse-tag",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await;
    assert!(
        second.is_ok(),
        "tag still reserved after the old consumer exited"
    );

    let rejected = other_channel
        .queue_delete(
            "reuse",
            QueueDeleteOptions {
                if_unused: true,
                ..Default::default()
            },
        )
        .await;
    assert!(
        rejected.is_err(),
        "new subscription must be visible in the registry"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_queue_through_another_connection_cancels_consumers() {
    let harness = Harness::start().await;
    let consumer_connection = harness.connection().await;
    let consumer_channel = consumer_connection.create_channel().await.unwrap();
    declare(&consumer_channel, "remote-delete").await;
    let mut consumer = consumer_channel
        .basic_consume(
            "remote-delete",
            "remote-worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let other = harness.connection().await;
    let other_channel = other.create_channel().await.unwrap();
    other_channel
        .queue_delete("remote-delete", QueueDeleteOptions::default())
        .await
        .unwrap();
    declare(&other_channel, "remote-delete").await;
    other_channel
        .basic_publish(
            "",
            "remote-delete",
            BasicPublishOptions::default(),
            b"new-work",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    other_channel
        .queue_declare(
            "remote-delete",
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap();

    match tokio::time::timeout(Duration::from_millis(600), consumer.next()).await {
        Ok(None) | Ok(Some(Err(_))) => {}
        Err(_) => panic!("server did not send basic.cancel"),
        Ok(Some(Ok(delivery))) => {
            panic!("stale consumer received {:?}", delivery.data)
        }
    }
    assert_eq!(harness.info("remote-delete").ready, 1);

    // The tag must be free immediately after the notification.
    let mut again = consumer_channel
        .basic_consume(
            "remote-delete",
            "remote-worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .expect("tag was not released after cancellation");
    let delivery = tokio::time::timeout(Duration::from_secs(5), again.next())
        .await
        .expect("new delivery timeout")
        .expect("consumer ended")
        .expect("delivery error");
    assert_eq!(delivery.data, b"new-work");
    delivery.ack(BasicAckOptions::default()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_consumer_does_not_block_if_unused_after_recreate() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "identity").await;
    channel
        .basic_qos(1, BasicQosOptions::default())
        .await
        .unwrap();
    let mut consumer = channel
        .basic_consume(
            "identity",
            "stale-worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    channel
        .basic_publish(
            "",
            "identity",
            BasicPublishOptions::default(),
            b"leased",
            BasicProperties::default(),
        )
        .await
        .unwrap();

    // Hold the delivery without acking so the prefetch window stays full.
    let _delivery = tokio::time::timeout(Duration::from_secs(5), consumer.next())
        .await
        .expect("delivery timeout")
        .expect("consumer ended")
        .expect("delivery error");

    // Bypass AMQP: delete and recreate while prefetch is full, so the old
    // consumer cannot notice through a take.
    harness.ante.delete_queue("identity").unwrap();
    harness.ante.declare_queue("identity").unwrap();

    match tokio::time::timeout(Duration::from_millis(600), consumer.next()).await {
        Ok(None) | Ok(Some(Err(_))) => {}
        Err(_) => panic!("native delete did not notify the consumer"),
        Ok(Some(Ok(delivery))) => {
            panic!("stale consumer received {:?}", delivery.data)
        }
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let deleted = channel
        .queue_delete(
            "identity",
            QueueDeleteOptions {
                if_unused: true,
                ..Default::default()
            },
        )
        .await;
    assert!(
        deleted.is_ok(),
        "a consumer of the deleted queue blocked if_unused"
    );

    // Recreate and resubscribe on the same channel. The old delivery must not
    // still occupy the prefetch window.
    declare(&channel, "identity").await;
    let mut fresh = channel
        .basic_consume(
            "identity",
            "fresh-worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    channel
        .basic_publish(
            "",
            "identity",
            BasicPublishOptions::default(),
            b"fresh-work",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(2), fresh.next())
        .await
        .expect("old outstanding delivery still consumed the prefetch slot")
        .expect("consumer ended")
        .expect("delivery error");
    assert_eq!(delivery.data, b"fresh-work");
    delivery.ack(BasicAckOptions::default()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_leaves_no_consumer_running() {
    let mut harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "leak").await;
    channel
        .basic_publish(
            "",
            "leak",
            BasicPublishOptions::default(),
            b"first",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    let _consumer = channel
        .basic_consume(
            "leak",
            "leak-worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while harness.info("leak").inflight == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "delivery never became in-flight"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    harness.shutdown.take().unwrap().send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(7), &mut harness.server)
        .await
        .expect("serve_until did not stop")
        .unwrap();

    harness
        .ante
        .publish(
            "leak",
            ante_core::NewMessage {
                payload: b"second".to_vec(),
                ..Default::default()
            },
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let info = harness.info("leak");
    assert_eq!(
        info.total(),
        2,
        "a consumer kept taking work after shutdown"
    );
    assert_eq!(info.inflight, 0);
}

struct RawClient {
    stream: TcpStream,
}

fn raw_method(frame: &[u8]) -> (u16, u16) {
    (
        u16::from_be_bytes([frame[0], frame[1]]),
        u16::from_be_bytes([frame[2], frame[3]]),
    )
}

fn close_code(payload: &[u8]) -> u16 {
    u16::from_be_bytes([payload[4], payload[5]])
}

/// A closed connection is EOF, or a transport error caused by the peer
/// hanging up. A decoding error or an extra frame is not acceptable.
fn is_connection_closed(result: &io::Result<Option<(u8, u16, Vec<u8>)>>) -> bool {
    match result {
        Ok(None) => true,
        Ok(Some(_)) => false,
        Err(err) => matches!(
            err.kind(),
            ErrorKind::ConnectionReset
                | ErrorKind::BrokenPipe
                | ErrorKind::ConnectionAborted
                | ErrorKind::UnexpectedEof
        ),
    }
}

impl RawClient {
    async fn connect(port: u16) -> Self {
        Self::connect_with_heartbeat(port, 0).await
    }

    async fn connect_with_heartbeat(port: u16, heartbeat: u16) -> Self {
        let mut stream = TcpStream::connect(("127.0.0.1", port)).await.unwrap();
        stream.write_all(b"AMQP\x00\x00\x09\x01").await.unwrap();

        let start = read_frame(&mut stream, 131_072).await.unwrap().unwrap();
        assert_eq!(
            raw_method(&start.payload),
            (CLASS_CONNECTION, CONNECTION_START)
        );

        let mut args = Encoder::new();
        args.table(&[("product", TableValue::String("raw-test"))])
            .shortstr("PLAIN")
            .longstr(b"\0guest\0guest")
            .shortstr("en_US");
        write_frame(
            &mut stream,
            FRAME_METHOD,
            0,
            &method_payload(CLASS_CONNECTION, CONNECTION_START_OK, &args.finish()),
        )
        .await
        .unwrap();

        let tune = read_frame(&mut stream, 131_072).await.unwrap().unwrap();
        assert_eq!(
            raw_method(&tune.payload),
            (CLASS_CONNECTION, CONNECTION_TUNE)
        );

        let mut args = Encoder::new();
        args.u16(16).u32(131_072).u16(heartbeat);
        write_frame(
            &mut stream,
            FRAME_METHOD,
            0,
            &method_payload(CLASS_CONNECTION, CONNECTION_TUNE_OK, &args.finish()),
        )
        .await
        .unwrap();

        let mut args = Encoder::new();
        args.shortstr("/").shortstr("").u8(0);
        write_frame(
            &mut stream,
            FRAME_METHOD,
            0,
            &method_payload(CLASS_CONNECTION, CONNECTION_OPEN, &args.finish()),
        )
        .await
        .unwrap();

        let open = read_frame(&mut stream, 131_072).await.unwrap().unwrap();
        assert_eq!(
            raw_method(&open.payload),
            (CLASS_CONNECTION, CONNECTION_OPEN_OK)
        );
        RawClient { stream }
    }

    async fn method(&mut self, channel: u16, class: u16, method: u16, args: &[u8]) {
        write_frame(
            &mut self.stream,
            FRAME_METHOD,
            channel,
            &method_payload(class, method, args),
        )
        .await
        .unwrap();
    }

    async fn read(&mut self) -> io::Result<Option<(u8, u16, Vec<u8>)>> {
        Ok(read_frame(&mut self.stream, 131_072)
            .await?
            .map(|frame| (frame.kind, frame.channel, frame.payload)))
    }

    async fn expect_method(&mut self, channel: u16, class: u16, method: u16) -> Vec<u8> {
        let (kind, got_channel, payload) = self
            .read()
            .await
            .expect("read failed")
            .expect("connection closed");
        assert_eq!((kind, got_channel), (FRAME_METHOD, channel));
        assert_eq!(raw_method(&payload), (class, method));
        payload
    }

    async fn open_channel(&mut self, channel: u16) {
        let mut args = Encoder::new();
        args.shortstr("");
        self.method(channel, CLASS_CHANNEL, CHANNEL_OPEN, &args.finish())
            .await;
        self.expect_method(channel, CLASS_CHANNEL, CHANNEL_OPEN_OK)
            .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_timeout_closes_a_silent_peer() {
    let harness = Harness::start_with_options(ante_amqp::ServerOptions {
        heartbeat_ms: 1000,
        ..Default::default()
    })
    .await;
    let mut raw = RawClient::connect_with_heartbeat(harness.port, 1).await;

    let heartbeat = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let (kind, channel, _) = raw.read().await.unwrap().unwrap();
            if kind == FRAME_HEARTBEAT {
                assert_eq!(channel, 0);
                break;
            }
            assert_eq!(kind, FRAME_METHOD);
        }
    })
    .await;
    heartbeat.expect("server did not send a heartbeat");

    let close = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match raw.read().await.unwrap() {
                None => return None,
                Some((FRAME_HEARTBEAT, 0, _)) => continue,
                Some((FRAME_METHOD, 0, payload)) => {
                    assert_eq!(raw_method(&payload), (CLASS_CONNECTION, CONNECTION_CLOSE));
                    return Some(close_code(&payload));
                }
                Some(other) => panic!("unexpected frame while waiting for close: {other:?}"),
            }
        }
    })
    .await
    .expect("server did not close a silent peer")
    .expect("connection closed without a connection.close frame");
    assert_eq!(close, 320);
}

#[tokio::test(flavor = "multi_thread")]
async fn heartbeat_timeout_requeues_an_inflight_delivery() {
    let harness = Harness::start_with_options(ante_amqp::ServerOptions {
        heartbeat_ms: 1000,
        ..Default::default()
    })
    .await;
    harness.ante.declare_queue("jobs").unwrap();
    harness
        .ante
        .publish(
            "jobs",
            ante_core::NewMessage {
                payload: b"long job".to_vec(),
                ..Default::default()
            },
        )
        .unwrap();

    let mut raw = RawClient::connect_with_heartbeat(harness.port, 1).await;
    raw.open_channel(1).await;
    let mut args = Encoder::new();
    args.u16(0).shortstr("jobs").u8(0);
    raw.method(1, CLASS_BASIC, BASIC_GET, &args.finish()).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while harness.info("jobs").inflight != 1 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "get did not take the job"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    tokio::time::sleep(Duration::from_millis(3000)).await;
    let info = harness.info("jobs");
    assert_eq!(info.inflight, 0);
    assert_eq!(info.ready, 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_body_terminates_connection() {
    let harness = Harness::start().await;
    let mut raw = RawClient::connect(harness.port).await;
    raw.open_channel(1).await;

    let mut args = Encoder::new();
    args.u16(0).shortstr("").shortstr("missing").u8(0);
    raw.method(1, CLASS_BASIC, BASIC_PUBLISH, &args.finish())
        .await;

    let mut header = Encoder::new();
    header.u16(CLASS_BASIC).u16(0).u64(1).u16(0);
    write_frame(&mut raw.stream, FRAME_HEADER, 1, &header.finish())
        .await
        .unwrap();

    write_frame(&mut raw.stream, FRAME_BODY, 1, b"ab")
        .await
        .unwrap();
    let close = raw
        .expect_method(0, CLASS_CONNECTION, CONNECTION_CLOSE)
        .await;
    assert_eq!(close_code(&close), 501);

    let _ = write_frame(&mut raw.stream, FRAME_BODY, 1, b"cd").await;
    let next = tokio::time::timeout(Duration::from_secs(2), raw.read())
        .await
        .expect("connection kept processing frames after a fatal body error");
    assert!(
        is_connection_closed(&next),
        "unexpected result after connection close: {next:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn reopening_an_open_channel_is_a_connection_error() {
    let harness = Harness::start().await;
    let mut raw = RawClient::connect(harness.port).await;
    raw.open_channel(1).await;

    let mut args = Encoder::new();
    args.shortstr("");
    raw.method(1, CLASS_CHANNEL, CHANNEL_OPEN, &args.finish())
        .await;
    let close = raw
        .expect_method(0, CLASS_CONNECTION, CONNECTION_CLOSE)
        .await;
    assert_eq!(close_code(&close), 504);

    let next = tokio::time::timeout(Duration::from_secs(2), raw.read())
        .await
        .expect("connection stayed open after duplicate channel.open");
    assert!(
        is_connection_closed(&next),
        "unexpected result after connection close: {next:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn second_content_header_is_rejected_without_storing_the_partial_body() {
    let harness = Harness::start().await;
    harness.ante.declare_queue("phase").unwrap();
    let mut raw = RawClient::connect(harness.port).await;
    raw.open_channel(1).await;

    let mut args = Encoder::new();
    args.u16(0).shortstr("").shortstr("phase").u8(0);
    raw.method(1, CLASS_BASIC, BASIC_PUBLISH, &args.finish())
        .await;

    let mut header = Encoder::new();
    header.u16(CLASS_BASIC).u16(0).u64(10).u16(0);
    write_frame(&mut raw.stream, FRAME_HEADER, 1, &header.finish())
        .await
        .unwrap();
    write_frame(&mut raw.stream, FRAME_BODY, 1, b"abc")
        .await
        .unwrap();

    let mut second = Encoder::new();
    second.u16(CLASS_BASIC).u16(0).u64(0).u16(0);
    write_frame(&mut raw.stream, FRAME_HEADER, 1, &second.finish())
        .await
        .unwrap();

    let close = raw
        .expect_method(0, CLASS_CONNECTION, CONNECTION_CLOSE)
        .await;
    assert_eq!(close_code(&close), 505);
    let next = tokio::time::timeout(Duration::from_secs(2), raw.read())
        .await
        .expect("connection stayed open after a repeated content header");
    assert!(
        is_connection_closed(&next),
        "unexpected result after connection close: {next:?}"
    );
    assert_eq!(
        harness.info("phase").total(),
        0,
        "a partial, malformed publish was stored"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_delete_releases_get_prefetch_on_another_connection() {
    let harness = Harness::start().await;
    let first = harness.connection().await;
    let second = harness.connection().await;
    let first_channel = first.create_channel().await.unwrap();
    let second_channel = second.create_channel().await.unwrap();

    declare(&first_channel, "cross").await;
    first_channel
        .basic_qos(1, BasicQosOptions::default())
        .await
        .unwrap();
    first_channel
        .basic_publish(
            "",
            "cross",
            BasicPublishOptions::default(),
            b"old",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    let held = first_channel
        .basic_get("cross", BasicGetOptions::default())
        .await
        .unwrap()
        .expect("message");
    assert_eq!(held.data, b"old");
    // Deliberately leave it unacked: it occupies the only prefetch slot.

    second_channel
        .queue_delete("cross", QueueDeleteOptions::default())
        .await
        .unwrap();
    declare(&second_channel, "cross").await;
    second_channel
        .basic_publish(
            "",
            "cross",
            BasicPublishOptions::default(),
            b"new",
            BasicProperties::default(),
        )
        .await
        .unwrap();

    let mut fresh = first_channel
        .basic_consume(
            "cross",
            "fresh",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(2), fresh.next())
        .await
        .expect("stale outstanding delivery still held the prefetch slot")
        .expect("consumer ended")
        .expect("delivery error");
    assert_eq!(delivery.data, b"new");
    delivery.ack(BasicAckOptions::default()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_delete_with_an_active_consumer_completes() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "deadlock").await;
    let _consumer = channel
        .basic_consume(
            "deadlock",
            "worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();

    let deleted = tokio::time::timeout(
        Duration::from_secs(2),
        channel.queue_delete("deadlock", QueueDeleteOptions::default()),
    )
    .await
    .expect("queue.delete deadlocked with an active consumer")
    .unwrap();
    assert_eq!(deleted, 0);

    declare(&channel, "after-delete").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_delete_releases_get_prefetch_without_an_active_consumer() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "native-cross").await;
    channel
        .basic_qos(1, BasicQosOptions::default())
        .await
        .unwrap();
    channel
        .basic_publish(
            "",
            "native-cross",
            BasicPublishOptions::default(),
            b"old",
            BasicProperties::default(),
        )
        .await
        .unwrap();
    let held = channel
        .basic_get("native-cross", BasicGetOptions::default())
        .await
        .unwrap()
        .expect("message");
    assert_eq!(held.data, b"old");
    // No consumer exists to observe the identity change.

    harness.ante.delete_queue("native-cross").unwrap();
    harness.ante.declare_queue("native-cross").unwrap();
    harness
        .ante
        .publish(
            "native-cross",
            ante_core::NewMessage {
                payload: b"new".to_vec(),
                ..Default::default()
            },
        )
        .unwrap();

    let mut fresh = channel
        .basic_consume(
            "native-cross",
            "fresh",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(2), fresh.next())
        .await
        .expect("native delete left a stale prefetch slot")
        .expect("consumer ended")
        .expect("delivery error");
    assert_eq!(delivery.data, b"new");
    delivery.ack(BasicAckOptions::default()).await.unwrap();
}

async fn consumer_count(channel: &lapin::Channel, queue: &str) -> u32 {
    channel
        .queue_declare(
            queue,
            QueueDeclareOptions {
                passive: true,
                ..Default::default()
            },
            FieldTable::default(),
        )
        .await
        .unwrap()
        .consumer_count()
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_declare_reports_active_consumers() {
    let harness = Harness::start().await;
    let connection = harness.connection().await;
    let channel = connection.create_channel().await.unwrap();
    declare(&channel, "counted").await;

    assert_eq!(consumer_count(&channel, "counted").await, 0);

    let _consumer = channel
        .basic_consume(
            "counted",
            "worker",
            BasicConsumeOptions::default(),
            FieldTable::default(),
        )
        .await
        .unwrap();
    assert_eq!(
        consumer_count(&channel, "counted").await,
        1,
        "consume-ok did not register the consumer"
    );

    channel
        .basic_cancel("worker", BasicCancelOptions::default())
        .await
        .unwrap();
    assert_eq!(
        consumer_count(&channel, "counted").await,
        0,
        "basic.cancel left the consumer registered"
    );
}
