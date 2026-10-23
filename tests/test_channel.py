import os
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from ante import Ante, AnteError, BasicProperties


class ChannelTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="ante-ffi-")
        self.path = Path(self.directory.name) / "queue.redb"
        self.ante = Ante.open(self.path)
        self.ch = self.ante.channel()
        self.ch.queue_declare("jobs")

    def tearDown(self):
        self.ante.close()
        self.directory.cleanup()

    def start(self):
        errors = []

        def run():
            try:
                self.ch.start_consuming()
            except BaseException as error:
                errors.append(error)

        thread = threading.Thread(target=run)
        thread.start()
        self.addCleanup(self.finish, thread, errors)
        return thread

    def finish(self, thread, errors):
        self.ch.stop_consuming()
        thread.join(5)
        self.assertFalse(thread.is_alive(), "dispatcher failed to stop")
        self.assertEqual(errors, [])

    def test_parallel_workers_same_process(self):
        self.ch.basic_qos(prefetch_count=4)
        barrier = threading.Barrier(4, timeout=3)
        done = threading.Event()
        lock = threading.Lock()
        pids, tids, tags = set(), set(), set()

        def callback(ch, method, props, body):
            with lock:
                pids.add(os.getpid())
                tids.add(threading.get_ident())
                tags.add(method.delivery_tag)
            barrier.wait()
            ch.basic_ack(method.delivery_tag)
            with lock:
                if self.ante.info("jobs")["total"] == 0:
                    done.set()
                    ch.stop_consuming()

        self.ch.basic_consume("jobs", callback, concurrency=4)
        self.ante.publish_batch("jobs", [b"a", b"b", b"c", b"d"])
        self.start()
        self.assertTrue(done.wait(5))
        self.assertEqual(pids, {os.getpid()})
        self.assertEqual(len(tids), 4)
        self.assertEqual(len(tags), 4)

    def test_manual_ack_prefetch_stays_full_after_callback_returns(self):
        self.ch.basic_qos(prefetch_count=1)
        first, second = threading.Event(), threading.Event()
        methods = []

        def callback(ch, method, props, body):
            methods.append(method)
            if len(methods) == 1:
                first.set()  # Intentionally do not ack on return.
            else:
                ch.basic_ack(method.delivery_tag)
                second.set()
                ch.stop_consuming()

        self.ch.basic_consume("jobs", callback, concurrency=2)
        self.ante.publish_batch("jobs", ["first", "second"])
        self.start()
        self.assertTrue(first.wait(3))
        self.assertFalse(second.wait(0.15), "prefetch ignored the unacked delivery")
        self.assertEqual(self.ante.info("jobs")["inflight"], 1)
        self.ch.basic_ack(methods[0].delivery_tag)  # Cross-thread settlement.
        self.assertTrue(second.wait(3))

    def test_get_metadata_nack_reject(self):
        self.ch.basic_publish(
            "",
            "jobs",
            b"\x00\xff",
            BasicProperties(
                content_type="application/octet-stream",
                message_id="app-id",
                correlation_id="request-id",
                reply_to="responses",
            ),
        )
        method, props, body = self.ch.basic_get("jobs")
        self.assertEqual(body, b"\x00\xff")
        self.assertEqual(props.message_id, "app-id")
        self.assertEqual(props.correlation_id, "request-id")
        self.assertEqual(props.reply_to, "responses")
        self.ch.basic_nack(method.delivery_tag, requeue=True)
        method, _, _ = self.ch.basic_get("jobs")
        self.assertTrue(method.redelivered)
        self.assertEqual(method.attempts, 2)
        self.ch.basic_reject(method.delivery_tag, requeue=False)
        self.assertEqual(self.ch.basic_get("jobs"), (None, None, None))

    def test_multiple_ack_and_channel_tag_scope(self):
        self.ante.publish_batch("jobs", [1, 2, 3])
        methods = [self.ch.basic_get("jobs")[0] for _ in range(3)]
        other = self.ante.channel()
        with self.assertRaises(AnteError):
            other.basic_ack(methods[0].delivery_tag)
        self.ch.basic_ack(methods[1].delivery_tag, multiple=True)
        self.assertEqual(self.ante.info("jobs")["total"], 1)
        self.ch.basic_nack(0, multiple=True, requeue=False)
        self.assertEqual(self.ante.info("jobs")["total"], 0)

    def test_auto_ack_before_callback(self):
        done = threading.Event()
        totals = []

        def callback(ch, method, props, body):
            totals.append(self.ante.info("jobs")["total"])
            done.set()
            ch.stop_consuming()

        self.ch.basic_consume("jobs", callback, auto_ack=True)
        self.ante.publish("jobs", "hello")
        self.start()
        self.assertTrue(done.wait(3))
        self.assertEqual(totals, [0])

    def test_close_requeues_unacked(self):
        self.ante.publish("jobs", "hello")
        self.ch.basic_get("jobs")
        self.ch.close()
        self.assertEqual(self.ante.info("jobs")["ready"], 1)
        with self.assertRaises(AnteError):
            self.ch.basic_get("jobs")

    def test_queue_operations_and_unsupported_options(self):
        self.ante.publish_batch("jobs", [1, 2])
        self.assertEqual(
            self.ch.queue_declare("jobs", passive=True).method.message_count, 2
        )
        self.assertEqual(self.ch.queue_purge("jobs").method.message_count, 2)
        self.assertEqual(self.ch.queue_delete("jobs").method.message_count, 0)
        self.assertFalse(self.ante.queue_exists("jobs"))
        with self.assertRaises(AnteError):
            self.ch.queue_declare("jobs", passive=True)
        with self.assertRaises(NotImplementedError):
            self.ch.basic_publish("custom", "jobs", b"x")
        with self.assertRaises(NotImplementedError):
            self.ch.queue_declare("jobs", exclusive=True)

    def test_default_single_worker_serial(self):
        self.ch.basic_qos(prefetch_count=4)
        done = threading.Event()
        order = []

        def callback(ch, method, props, body):
            time.sleep(0.01)
            order.append(body)
            ch.basic_ack(method.delivery_tag)
            if len(order) == 4:
                done.set()
                ch.stop_consuming()

        self.ch.basic_consume("jobs", callback)
        self.ante.publish_batch("jobs", ["1", "2", "3", "4"])
        self.start()
        self.assertTrue(done.wait(3))
        self.assertEqual(order, [b"1", b"2", b"3", b"4"])

    def test_cancel_one_consumer_keeps_other_running(self):
        self.ch.queue_declare("other")
        done = threading.Event()
        callbacks = []

        def first(ch, method, props, body):
            callbacks.append("first")
            ch.basic_ack(method.delivery_tag)
            ch.basic_cancel("first")
            self.ante.publish("other", "next")

        def second(ch, method, props, body):
            callbacks.append("second")
            ch.basic_ack(method.delivery_tag)
            done.set()
            ch.stop_consuming()

        self.ch.basic_consume("jobs", first, consumer_tag="first")
        self.ch.basic_consume("other", second, consumer_tag="second")
        self.ante.publish("jobs", "start")
        self.start()
        self.assertTrue(done.wait(3))
        self.assertEqual(callbacks, ["first", "second"])


class LifecycleTests(unittest.TestCase):
    def test_concurrent_publishers_and_consumer_no_loss(self):
        with tempfile.TemporaryDirectory() as directory:
            with Ante.open(Path(directory) / "db") as ante:
                received = []
                lock = threading.Lock()
                done = threading.Event()
                count = 80

                def handler(payload, delivery):
                    with lock:
                        received.append(payload)
                        if len(received) == count:
                            done.set()

                consumer = ante.consume("jobs", handler, prefetch=16, concurrency=4)
                errors = []

                def publish(offset):
                    try:
                        for i in range(20):
                            ante.publish("jobs", offset + i)
                    except BaseException as error:
                        errors.append(error)

                publishers = [
                    threading.Thread(target=publish, args=(i * 20,)) for i in range(4)
                ]
                for publisher in publishers:
                    publisher.start()
                for publisher in publishers:
                    publisher.join(5)
                    self.assertFalse(publisher.is_alive())
                self.assertEqual(errors, [])
                self.assertTrue(done.wait(5))
                consumer.stop()
                self.assertEqual(sorted(received), list(range(count)))
                self.assertEqual(ante.info("jobs")["total"], 0)

    def test_deleted_queue_stops_consumer_before_recreated_queue(self):
        with tempfile.TemporaryDirectory() as directory:
            with Ante.open(Path(directory) / "db") as ante:
                consumer = ante.consume("jobs", lambda *_: self.fail("stale consumer"))
                ante.delete_queue("jobs")
                ante.declare("jobs")
                ante.publish("jobs", "new queue")
                self.assertTrue(consumer._stopped.wait(3))
                consumer.stop()
                self.assertEqual(ante.info("jobs")["ready"], 1)

    def test_waiting_take_wakes_for_publisher_and_batch_keeps_types(self):
        with tempfile.TemporaryDirectory() as directory:
            with Ante.open(Path(directory) / "db") as ante:
                ante.declare("jobs")
                received = []
                taker = threading.Thread(
                    target=lambda: received.extend(
                        ante.take("jobs", max=3, wait_ms=1000)
                    )
                )
                taker.start()
                ante.publish_batch("jobs", [{"n": 1}, b"\xff", "text"])
                taker.join(3)
                self.assertFalse(taker.is_alive())
                self.assertEqual(
                    [d.payload for d in received], [{"n": 1}, b"\xff", "text"]
                )
                for delivery in received:
                    delivery.ack()
                    delivery.ack()  # Delivery-level settlement is idempotent.
                self.assertEqual(ante.list_queues()[0]["total"], 0)

    def test_zero_config_has_no_socket_or_subprocess(self):
        with tempfile.TemporaryDirectory() as directory:
            old_cwd = Path.cwd()
            try:
                os.chdir(directory)
                with (
                    patch.dict(os.environ, {}, clear=True),
                    patch("subprocess.Popen", side_effect=AssertionError("spawn")),
                    patch("socket.socket", side_effect=AssertionError("socket")),
                ):
                    with Ante.open() as ante:
                        ante.declare("jobs")
                        ante.publish("jobs", "persisted")
                self.assertTrue(Path(".ante/ante.redb").is_file())
                with Ante.open() as ante:
                    self.assertEqual(ante.take("jobs")[0].payload, "persisted")
            finally:
                os.chdir(old_cwd)

    def test_lease_expiration_and_stale_ack(self):
        with tempfile.TemporaryDirectory() as directory:
            with Ante.open(Path(directory) / "db") as ante:
                ante.declare("jobs")
                ante.publish("jobs", "hello")
                [old] = ante.take("jobs", lease_ms=20)
                time.sleep(0.04)
                [new] = ante.take("jobs")
                self.assertEqual(new.attempts, 2)
                self.assertNotEqual(new.token, old.token)
                with self.assertRaises(AnteError) as raised:
                    old.ack()
                self.assertEqual(raised.exception.code, "STALE_DELIVERY")
                new.ack()

    def test_closed_engine_and_reopen(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "db"
            ante = Ante.open(path)
            ante.declare("jobs")
            ante.close()
            ante.close()
            with self.assertRaises(AnteError):
                ante.publish("jobs", "closed")
            with Ante.open(path) as reopened:
                self.assertTrue(reopened.queue_exists("jobs"))


if __name__ == "__main__":
    unittest.main()
