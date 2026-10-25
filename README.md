# Ante for Python

A durable work queue embedded **inside your Python process**, backed by Rust,
SQLite, and PyO3. No daemon, socket, URL, or executable to manage.

The consumer API follows Pika's `BlockingChannel` naming and callback shape.
`concurrency` adds a bounded worker pool for parallel jobs in one process.

## Install from source

Requires Python 3.9+ and a Rust toolchain when building from source:

```bash
python -m pip install .
```

The extension uses CPython's stable ABI (`abi3-py39`). A built platform wheel
can be installed without a Rust toolchain. This repository does not publish
wheels to PyPI yet.

## Pika-style consumer

```python
from ante import Ante

with Ante.open() as connection:
    channel = connection.channel()
    channel.queue_declare(queue="jobs", durable=True)
    channel.basic_qos(prefetch_count=32)
    channel.confirm_delivery()

    def on_message(ch, method, properties, body):
        print(body)  # bytes, like pika
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(
        queue="jobs",
        on_message_callback=on_message,
        auto_ack=False,
        concurrency=8,
    )

    channel.basic_publish(exchange="", routing_key="jobs", body=b"hello")
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
```

`Ante.open()` stores data in `.ante/ante.sqlite3` under the working directory.
`ANTE_DB` or `Ante.open("./data/queue.sqlite3")` optionally overrides it. Open once
at app startup and share that instance across threads. Close at app shutdown.
One process owns a database file; several Python processes cannot open it
simultaneously.

### Concurrency and acknowledgements

- Default: one callback worker per consumer, preserving dispatch order.
- `concurrency=8`: at most eight callback workers for that consumer. You can
  also set a channel default with `connection.channel(concurrency=8)`.
- `prefetch_count=32`: at most 32 outstanding, unacknowledged deliveries per
  consumer, including queued callbacks. Returning without an ack keeps its slot
  occupied until settlement or lease expiration.
- `auto_ack=False`: callback must explicitly ack/nack/reject. A successful
  callback return alone does **not** ack in the Pika-style API.
- `auto_ack=True`: settle before invoking the callback; callback failure cannot
  recover an already-acked message.
- `basic_ack`, `basic_nack`, and `basic_reject` are safe from worker threads.
  `delivery_tag` belongs to the channel that delivered the message.
- `stop_consuming()` is safe inside a callback: it requests cancellation; the
  dispatcher drains workers. Closing a channel requeues remaining unacked
  deliveries. Active Python callbacks cannot be forcibly killed; consumer
  shutdown waits up to its timeout (10 seconds by default).
- Callback exceptions stop the dispatcher and propagate from `start_consuming`.
  Unacked deliveries remain pending until explicitly settled, channel close,
  or lease expiry. The high-level `consume` API separately retains its retry/drop
  policy; the Pika-style channel does not silently retry or discard on failure.
- QoS follows RabbitMQ: `global_qos=False` captures a per-consumer limit when
  registering; `global_qos=True` adds a shared channel limit. Zero (the default)
  means no unacked limit. Auto-ACK consumers and `basic_get` ignore QoS. Internal
  dispatch batches and queued callbacks remain bounded.

Threads overlap network/disk I/O. Pure Python CPU work remains limited by the
GIL on standard CPython. Rust storage operations release the GIL while running;
SQLite transactions are serialized inside the engine. Parallel callbacks can
complete out of order. Delivery is **at-least-once**: jobs must tolerate retries.
Channel deliveries use the same 30-minute lease as the AMQP endpoint. The
high-level API defaults to 30 seconds; configure `lease_ms` for longer jobs. Call `delivery.touch()` to extend the lease while a handler is
still working; renewal does not create a new attempt.

The concurrency design follows Pika's official threaded-consumer examples:
[Pika 1.3.2](https://github.com/pika/pika/blob/1.3.2/examples/basic_consumer_threaded.py)
uses worker threads plus `add_callback_threadsafe` to settle on its I/O thread;
[Pika main](https://github.com/pika/pika/blob/main/examples/basic_consumer_threaded.py)
uses per-channel workers and thread-safe channel operations. Ante implements a
bounded pool and synchronization around its in-process engine; it has no network
I/O thread requiring an ack callback hop. This is an independent implementation,
not bundled Pika code, and does not require Pika as a dependency.

## Channel operations

| Operation | API |
| --- | --- |
| Declare / inspect queue | `queue_declare(queue, passive=False, durable=False)` |
| Set per-consumer outstanding limit | `basic_qos(prefetch_count=32)` |
| Enable publisher confirms | `confirm_delivery()` (embedded publishes always wait for commit) |
| Publish | `basic_publish(exchange="", routing_key="jobs", body=b"...")` → `None` |
| Unroutable callback | `add_on_return_callback(callback)` |
| Register consumer | `basic_consume(queue, on_message_callback, auto_ack=False, concurrency=8)` |
| Run dispatcher / stop | `start_consuming()` / `stop_consuming(consumer_tag=None)` |
| Cancel one consumer | `basic_cancel(consumer_tag)` |
| Poll a message | `basic_get(queue, auto_ack=False)` → `(method, properties, body)` |
| Acknowledge | `basic_ack(delivery_tag, multiple=False)` |
| Retry / discard | `basic_nack(delivery_tag, multiple=False, requeue=True)` |
| Reject one | `basic_reject(delivery_tag, requeue=True)` |
| Recover unacked deliveries | `basic_recover(requeue=True)` |
| Purge / delete | `queue_purge(queue)` / `queue_delete(queue)` |
| Close | `close()` |

Queue operations return objects with `.method` fields such as `message_count`.
An empty `basic_get` returns `(None, None, None)`. `multiple=True` settles all
outstanding tags up to `delivery_tag`; tag `0` means all outstanding tags.

`BasicProperties` supports `content_type`, `message_id`, `correlation_id`,
`reply_to`, and `delivery_mode` (all Ante messages are persisted regardless).
Methods expose `delivery_tag`, `consumer_tag`, `routing_key`, `exchange`,
`redelivered`, and Ante's `attempts` extension.

This is a **Pika-style API over Ante**, not full RabbitMQ compatibility.
The supported profile is the default exchange (`""`, routing key = queue name)
and named queues. All messages/queues remain durable even with `durable=False`.
Exclusive/auto-delete queues, queue arguments, byte QoS, custom exchanges, and
conditional queue deletion in the direct adapter raise errors. The AMQP listener
supports conditional deletion. Register consumers before starting the dispatcher.

Channel publishing accepts bytes or UTF-8 strings, preserves the supported
properties (including missing versus empty values), and returns `None`, like
Pika. For JSON/dicts or Ante message IDs, use `Ante.publish`, which retains its
high-level encoding and integer return value.

Publishing to a missing queue with `mandatory=False` drops the message, like
RabbitMQ. With `mandatory=True`, confirm mode raises `ante.UnroutableError`
(with `.messages`); without confirms it invokes registered return callbacks.
High-level `Ante.publish` continues to raise `QUEUE_NOT_FOUND`.

The embedded API uses `AnteError` rather than Pika's protocol exception
hierarchy, and does not emulate every wire-level channel closure after invalid
calls. Callback dispatch runs on Ante worker threads; cancellation drains workers
and `basic_cancel` returns `None`, rather than Pika's buffered-message list.
The supported method parameter names/defaults match Pika, with optional
keyword-only Ante extensions. `BasicProperties` is a supported-field subset, not all AMQP properties.
Use Pika against `serve_amqp()` when full client-side protocol behavior is needed.
Manual AMQP `basic_ack` has no broker reply; use a subsequent RPC/recovery check
when testing durability, rather than treating the method return as a storage ACK.

The shared contract suite is `tests/test_pika_contract.py`. It always checks the
direct adapter and the embedded AMQP listener. To include a standalone Ante
and a RabbitMQ reference, set `ANTE_STANDALONE_AMQP=127.0.0.1:15679` and
`ANTE_REFERENCE_AMQP=127.0.0.1:5672` when running unittest. These test endpoints
must use disposable storage; the suite creates/deletes unique test queues.

## High-level API and Ante features

```python
from ante import Ante

with Ante.open() as ante:
    ante.declare("jobs")
    ante.publish("jobs", {"task": "resize", "id": 42})
    ante.publish_batch("jobs", [{"n": 1}, b"raw", "text"])

    def handle(payload, delivery):
        print(payload, delivery.attempts)
        # Successful return acknowledges automatically in this high-level API.

    consumer = ante.consume(
        "jobs", handle,
        prefetch=32,
        concurrency=8,
        lease_ms=60_000,
        max_attempts=5,
        # Optional: on_error=lambda error, delivery: "requeue" or "drop"
    )
    # Keep your app alive / serve requests here.
    consumer.stop()
```

`ante.run(queue, handler, **options)` is the blocking equivalent of `consume`.
`consume` starts a dispatcher in the background and returns a `Consumer` with
`stop()`, `wait()`, `queue`, and `active` (running plus queued handlers).
For FastAPI, open/close Ante in the lifespan handler and run Uvicorn with one
worker process. The synchronous storage API can be used from sync endpoints or
via `asyncio.to_thread` in async endpoints.

Additional storage operations:

- `take(queue, max=1, lease_ms=30000, wait_ms=0, parse=None)` → deliveries.
  Each `Delivery` has `touch(lease_ms=None)`, which extends its original lease
  by default.
- `Delivery.ack()` / `Delivery.requeue()`; delivery includes raw bytes, decoded
  payload, token, attempts, content type, correlation/reply metadata, application
  message ID, and lease deadline.
- `ack(*tokens)` → per-token `AckResult`; `requeue(token)`.
- `info(queue)`, `list_queues()`, `queue_exists(queue)`.
- `message_status(queue, message_id)` returns `state`, timestamps, and retained
  attempt history, or `None` for an unknown or pruned message. History survives
  ack and queue deletion.
- `Ante.open(..., history_retention_ms=..., history_max_rows=...)` configures
  bounded history; `prune_history()` applies it immediately. Defaults are
  7 days and 1,000,000 finished attempts; zero disables a limit.
- `delivery.discard()` permanently drops a message and records `discarded`
  instead of `acked`; the high-level `on_error` drop policy uses it.
- `delivery.complete(queue, result, ...)` publishes the result and settles the
  delivery in one transaction. Pass `discard=True` for terminal failures.
- `purge(queue)`, `delete_queue(queue)`, `recover_expired()`.

`publish` encodes dict/list values as JSON, strings as UTF-8, and bytes-like
values as binary. Batch publishing preserves each message's content type.
`parse(raw, content_type)` optionally overrides payload decoding.
The dispatcher polls the embedded store with a 20ms maximum idle interval;
`take(wait_ms=...)` uses a local wake event for published/requeued messages.
An optional AMQP listener shares the same engine in-process:

```python
with Ante.open() as ante:
    host, port = ante.serve_amqp(host="127.0.0.1", port=5672)
    # External pika/lapin/amqplib clients can publish/consume the same queues.
    # ante.stop_amqp() stops it explicitly; close() stops and joins it as well.
```

Plain `Ante.open()` still opens no socket. The listener supports Ante's AMQP
subset (default exchange, durable queues, confirms, manual ack/nack, prefetch),
not the complete RabbitMQ broker feature set. Its default profile is plaintext
and unauthenticated on loopback.

For TLS and queue-scoped credentials, pass a security file:

```python
with Ante.open() as ante:
    host, port = ante.serve_amqp(
        "127.0.0.1", 5671, security_path="/etc/myapp/security.json"
    )
```

The file is loaded before the socket binds; a users configuration without TLS is
rejected, and secure mode has no plaintext fallback. The JSON schema, permission
matrix, Argon2id hash generation, and client examples are documented in Ante's
[security guide](https://github.com/rizkyandriawan/ante/blob/main/docs/security.md).
Use port `0` for an OS-assigned port.

### AMQP 1.0 listener

`serve_amqp1()` starts the separate AMQP 1.0 endpoint on the same embedded
engine. It supports the same secure profile, sender/receiver links, and
settlement mapping described in Ante's
[AMQP 1.0 guide](https://github.com/rizkyandriawan/ante/blob/main/docs/amqp1.md):

```python
with Ante.open() as ante:
    host, port = ante.serve_amqp1(port=0)
    # AMQP 1.0 clients can target queue addresses directly.
    ante.stop_amqp1()
```

The AMQP 0-9-1 and AMQP 1.0 listeners can run at the same time; `close()`
stops and joins both. The plaintext profile has no SASL layer, so clients that
enable SASL by default must disable it when no `security_path` is supplied.

## Development / verification

```bash
python -m venv .venv
. .venv/bin/activate
pip install maturin coverage ruff pika
maturin develop --release
python -m unittest discover -s tests -v
coverage run --source=ante -m unittest discover -s tests
coverage report -m
ruff check src tests
cargo fmt --all --check
cargo clippy --release --workspace -- -D warnings
cargo test --release --workspace --exclude ante-python
maturin build --release
maturin sdist
```

`vendor/ante-core`, `vendor/ante-amqp`, and `vendor/ante-security` are
MIT-licensed Rust crates copied from Ante; their sources and tests are unchanged.
See `vendor/README.md` for provenance. The copies make this repository build
independently of a sibling Ante checkout.
