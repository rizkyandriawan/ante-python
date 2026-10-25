"""Identical channel calls against embedded, standalone, and RabbitMQ.

ANTE_STANDALONE_AMQP and ANTE_REFERENCE_AMQP are optional host:port endpoints.
The embedded adapter and embedded AMQP listener always run.
"""

import os
import inspect
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path

import pika

from ante import Ante, AnteError, Channel, UnroutableError


class PikaContractTests(unittest.TestCase):
    def test_supported_method_signatures_match_pika(self):
        for name in (
            "queue_declare",
            "basic_qos",
            "basic_publish",
            "basic_consume",
            "basic_get",
            "basic_ack",
            "basic_nack",
            "basic_reject",
            "basic_cancel",
            "stop_consuming",
            "queue_purge",
            "queue_delete",
            "basic_recover",
            "confirm_delivery",
            "close",
        ):
            expected = inspect.signature(
                getattr(pika.adapters.blocking_connection.BlockingChannel, name)
            )
            actual = inspect.signature(getattr(Channel, name))
            with self.subTest(method=name):
                for parameter in expected.parameters.values():
                    found = actual.parameters[parameter.name]
                    self.assertEqual(found.kind, parameter.kind)
                    self.assertEqual(found.default, parameter.default)
                for parameter in actual.parameters.values():
                    if parameter.name not in expected.parameters:
                        self.assertEqual(parameter.kind, inspect.Parameter.KEYWORD_ONLY)

    def test_separate_pika_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            with Ante.open(Path(directory) / "queue.sqlite3") as ante:
                host, port = ante.serve_amqp(port=0)
                addresses = [(host, port)]
                if os.getenv("ANTE_STANDALONE_AMQP"):
                    host, port = os.environ["ANTE_STANDALONE_AMQP"].rsplit(":", 1)
                    addresses.append((host, int(port)))
                script = """
import pika, sys
host, port, queue, role = sys.argv[1:]
with pika.BlockingConnection(pika.ConnectionParameters(host, int(port))) as connection:
    ch = connection.channel()
    ch.queue_declare(queue, durable=True)
    if role == "producer":
        ch.confirm_delivery()
        for n in range(100):
            ch.basic_publish("", queue, str(n).encode(), mandatory=True)
    else:
        for n in range(100):
            method, _, body = ch.basic_get(queue)
            assert body == str(n).encode(), (n, body)
            ch.basic_ack(method.delivery_tag)
        assert ch.basic_get(queue) == (None, None, None)
        ch.queue_delete(queue)
"""
                for host, port in addresses:
                    queue = "process-" + uuid.uuid4().hex
                    for role in ("producer", "consumer"):
                        subprocess.run(
                            [
                                sys.executable,
                                "-c",
                                script,
                                host,
                                str(port),
                                queue,
                                role,
                            ],
                            check=True,
                            capture_output=True,
                            timeout=20,
                        )

    def test_same_producer_and_consumer_calls(self):
        with tempfile.TemporaryDirectory() as directory:
            with Ante.open(Path(directory) / "queue.sqlite3") as ante:
                endpoints = [("embedded", ante.channel, ante.channel)]
                host, port = ante.serve_amqp(port=0)
                addresses = [("embedded-amqp", host, port)]
                for variable in ("ANTE_STANDALONE_AMQP", "ANTE_REFERENCE_AMQP"):
                    if os.getenv(variable):
                        host, port = os.environ[variable].rsplit(":", 1)
                        addresses.append((variable, host, int(port)))
                connections = []
                try:
                    for name, host, port in addresses:
                        # Independent producer/consumer connections.
                        producer = pika.BlockingConnection(
                            pika.ConnectionParameters(host, port)
                        )
                        consumer = pika.BlockingConnection(
                            pika.ConnectionParameters(host, port)
                        )
                        connections.extend([producer, consumer])
                        endpoints.append((name, producer.channel, consumer.channel))
                    for name, producer_factory, consumer_factory in endpoints:
                        with self.subTest(adapter=name):
                            self.exercise(
                                producer_factory(), consumer_factory(), consumer_factory
                            )
                finally:
                    for connection in connections:
                        if connection.is_open:
                            connection.close()

    def exercise(self, producer, consumer, consumer_factory):
        queue = "contract-" + uuid.uuid4().hex
        producer.queue_declare(queue=queue, durable=True)
        producer.confirm_delivery()
        properties = pika.BasicProperties(
            content_type="application/octet-stream",
            message_id="identity",
            correlation_id="request",
            reply_to="responses",
            delivery_mode=2,
        )
        self.assertIsNone(
            producer.basic_publish("", queue, b"\x00\xff", properties, mandatory=True)
        )
        method, received, body = consumer.basic_get(queue, auto_ack=False)
        self.assertEqual(body, b"\x00\xff")
        for attribute in ("content_type", "message_id", "correlation_id", "reply_to"):
            self.assertEqual(
                getattr(received, attribute), getattr(properties, attribute)
            )
        consumer.basic_nack(method.delivery_tag, requeue=True)
        # An RPC is a protocol ordering barrier after asynchronous settlement.
        consumer.queue_declare(queue, passive=True)
        method, _, body = consumer.basic_get(queue)
        self.assertTrue(method.redelivered)
        consumer.basic_reject(method.delivery_tag, requeue=False)
        self.assertEqual(consumer.basic_get(queue), (None, None, None))

        producer.basic_publish(
            "", queue, b"", pika.BasicProperties(content_type="", message_id="")
        )
        method, received, body = consumer.basic_get(queue)
        self.assertEqual(body, b"")
        self.assertEqual(received.content_type, "")
        self.assertEqual(received.message_id, "")
        consumer.basic_ack(method.delivery_tag)

        producer.basic_publish("", queue, b"recover")
        consumer.basic_get(queue)
        consumer.basic_recover(requeue=True)
        method, received, body = consumer.basic_get(queue)
        self.assertEqual(body, b"recover")
        self.assertIsNone(received.content_type)
        self.assertTrue(method.redelivered)
        consumer.basic_ack(method.delivery_tag)

        producer.basic_publish("", queue, b"close-unacked")
        consumer.basic_get(queue)
        consumer.close()
        consumer = consumer_factory()
        method, _, body = consumer.basic_get(queue)
        self.assertEqual(body, b"close-unacked")
        self.assertTrue(method.redelivered)
        consumer.basic_ack(method.delivery_tag)

        for body in (b"one", b"two", b"three"):
            producer.basic_publish("", queue, body)
        tags = [consumer.basic_get(queue)[0].delivery_tag for _ in range(3)]
        consumer.basic_ack(tags[-1], multiple=True)
        consumer.queue_declare(queue, passive=True)
        self.assertEqual(consumer.basic_get(queue), (None, None, None))

        with self.assertRaises((pika.exceptions.UnroutableError, UnroutableError)):
            producer.basic_publish("", queue + "-missing", b"return", mandatory=True)
        producer.basic_publish("", queue + "-missing", b"discard", mandatory=False)

        # Two consumers each get one outstanding message with non-global QoS.
        consumer.basic_qos(prefetch_count=1)
        seen = []

        def callback(ch, method, props, body):
            seen.append(method)
            if len(seen) == 2:
                ch.basic_ack(0, multiple=True)
                ch.stop_consuming()

        consumer.basic_consume(queue, callback, consumer_tag="first")
        consumer.basic_consume(queue, callback, consumer_tag="second")
        producer.basic_publish("", queue, b"first")
        producer.basic_publish("", queue, b"second")
        self.run_consuming(consumer)
        self.assertEqual(len(seen), 2)
        consumer.close()
        consumer = consumer_factory()

        # Zero must not become an implicit limit of 16 or 32.
        consumer.basic_qos(prefetch_count=0)
        seen = []

        def unlimited(ch, method, props, body):
            seen.append(method)
            if len(seen) == 40:
                ch.basic_ack(0, multiple=True)
                ch.stop_consuming()

        consumer.basic_consume(queue, unlimited)
        for index in range(40):
            producer.basic_publish("", queue, str(index).encode())
        self.run_consuming(consumer)
        self.assertEqual(len(seen), 40)
        # Drain any callback settlement before queue deletion.
        self.assertEqual(
            consumer.queue_declare(queue, passive=True).method.message_count, 0
        )
        consumer.queue_delete(queue)
        consumer.close()
        producer.close()

    def run_consuming(self, consumer):
        # Bound hangs on credit regressions without timing successful dispatch.
        if hasattr(consumer, "connection"):
            timer = consumer.connection.call_later(5, consumer.stop_consuming)
        else:
            timer = threading.Timer(5, consumer.stop_consuming)
            timer.start()
        try:
            consumer.start_consuming()
        finally:
            if hasattr(consumer, "connection"):
                consumer.connection.remove_timeout(timer)
            else:
                timer.cancel()


class EmbeddedRegressionTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.ante = Ante.open(Path(self.directory.name) / "queue.sqlite3")
        self.addCleanup(self.directory.cleanup)
        self.addCleanup(self.ante.close)
        self.channel = self.ante.channel()
        self.channel.queue_declare("jobs")

    def test_reject_records_discard_not_ack(self):
        identifier = self.ante.publish("jobs", b"drop")
        method, _, _ = self.channel.basic_get("jobs")
        self.channel.basic_reject(method.delivery_tag, requeue=False)
        self.assertEqual(
            self.ante.message_status("jobs", identifier)["state"], "discarded"
        )

    def test_listener_rejects_unsupported_queue_features(self):
        host, port = self.ante.serve_amqp(port=0)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.addCleanup(connection.close)
        for options in (
            {"exclusive": True},
            {"auto_delete": True},
            {"arguments": {"x-message-ttl": 1000}},
        ):
            channel = connection.channel()
            with self.assertRaises(pika.exceptions.ChannelClosedByBroker) as error:
                channel.queue_declare("unsupported", **options)
            self.assertEqual(error.exception.reply_code, 540)
        self.assertFalse(self.ante.queue_exists("unsupported"))

    def test_failed_ack_closes_amqp_channel_and_requeues_claim(self):
        self.ante.publish("jobs", b"pending")
        host, port = self.ante.serve_amqp(port=0)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.addCleanup(connection.close)
        channel = connection.channel()
        method, _, _ = channel.basic_get("jobs")
        with sqlite3.connect(Path(self.directory.name) / "queue.sqlite3") as db:
            db.execute(
                "CREATE TRIGGER fail_ack BEFORE DELETE ON entries BEGIN SELECT RAISE(ABORT, 'injected settlement failure'); END"
            )
        channel.basic_ack(method.delivery_tag)
        with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
            channel.queue_declare("jobs", passive=True)
        self.assertEqual(self.ante.info("jobs")["ready"], 1)
        self.assertEqual(
            self.ante.message_status("jobs", 1)["history"][0]["outcome"], "requeued"
        )

    def test_queue_lookup_error_is_not_a_successful_publish(self):
        host, port = self.ante.serve_amqp(port=0)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.addCleanup(connection.close)
        channel = connection.channel()
        channel.confirm_delivery()
        path = Path(self.directory.name) / "queue.sqlite3"
        with sqlite3.connect(path) as db:
            db.execute("ALTER TABLE queues RENAME TO unavailable_queues")
        try:
            with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
                channel.basic_publish("", "jobs", b"not confirmed", mandatory=False)
        finally:
            with sqlite3.connect(path) as db:
                db.execute("ALTER TABLE unavailable_queues RENAME TO queues")
        self.assertEqual(self.ante.info("jobs")["total"], 0)

    def test_failed_auto_ack_stops_channel_without_losing_claim(self):
        self.ante.publish("jobs", b"pending")
        host, port = self.ante.serve_amqp(port=0)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.addCleanup(connection.close)
        channel = connection.channel()
        with sqlite3.connect(Path(self.directory.name) / "queue.sqlite3") as db:
            db.execute(
                "CREATE TRIGGER fail_ack BEFORE DELETE ON entries BEGIN SELECT RAISE(ABORT, 'injected auto ack failure'); END"
            )
        # The body may already have been delivered before the server detects
        # auto-ACK failure; the next RPC must expose the failed channel.
        try:
            channel.basic_get("jobs", auto_ack=True)
            with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
                channel.queue_declare("jobs", passive=True)
        except pika.exceptions.ChannelClosedByBroker:
            pass
        self.assertEqual(self.ante.info("jobs")["total"], 1)
        self.assertEqual(self.ante.info("jobs")["ready"], 1)

    def test_failed_discard_exposes_error_and_preserves_message(self):
        self.ante.publish("jobs", b"pending")
        host, port = self.ante.serve_amqp(port=0)
        connection = pika.BlockingConnection(pika.ConnectionParameters(host, port))
        self.addCleanup(connection.close)
        channel = connection.channel()
        method, _, _ = channel.basic_get("jobs")
        with sqlite3.connect(Path(self.directory.name) / "queue.sqlite3") as db:
            db.execute(
                "CREATE TRIGGER fail_discard BEFORE DELETE ON entries BEGIN SELECT RAISE(ABORT, 'injected discard failure'); END"
            )
        channel.basic_nack(method.delivery_tag, requeue=False)
        with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
            channel.queue_declare("jobs", passive=True)
        self.assertEqual(self.ante.info("jobs")["ready"], 1)

    def test_confirmed_publish_and_claim_survive_process_exit(self):
        path = Path(self.directory.name) / "crash.sqlite3"
        subprocess.run(
            [
                sys.executable,
                "-c",
                """
import os, sys
from ante import Ante
ante = Ante.open(sys.argv[1])
ch = ante.channel()
ch.queue_declare("jobs")
ch.confirm_delivery()
ch.basic_publish("", "jobs", b"survives")
ch.basic_get("jobs")
os._exit(0)
""",
                str(path),
            ],
            check=True,
            timeout=10,
        )
        with Ante.open(path) as recovered:
            ch = recovered.channel()
            method, _, body = ch.basic_get("jobs")
            self.assertEqual(body, b"survives")
            self.assertTrue(method.redelivered)
            ch.basic_ack(method.delivery_tag)

    def test_unknown_multiple_tag_settles_nothing(self):
        self.ante.publish_batch("jobs", [b"one", b"two"])
        first = self.channel.basic_get("jobs")[0]
        self.channel.basic_get("jobs")
        with self.assertRaises(AnteError):
            self.channel.basic_ack(first.delivery_tag + 100, multiple=True)
        self.assertEqual(self.ante.info("jobs")["inflight"], 2)

    def test_callback_failure_surfaces_without_retry_or_drop(self):
        identifier = self.ante.publish("jobs", b"pending")

        def fail(*_args):
            raise ValueError("callback failed")

        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume("jobs", fail)
        with self.assertRaisesRegex(ValueError, "callback failed"):
            self.channel.start_consuming()
        self.assertEqual(self.ante.message_status("jobs", identifier)["attempts"], 1)
        self.assertEqual(self.ante.info("jobs")["inflight"], 1)
        self.channel.close()
        self.assertEqual(self.ante.info("jobs")["ready"], 1)

    def test_global_qos_limits_two_consumers(self):
        self.channel.basic_qos(prefetch_count=1, global_qos=True)
        seen = []
        first = threading.Event()

        def callback(ch, method, props, body):
            seen.append(method)
            first.set()

        self.channel.basic_consume("jobs", callback)
        self.channel.basic_consume("jobs", callback)
        self.ante.publish_batch("jobs", [b"one", b"two"])
        thread = threading.Thread(target=self.channel.start_consuming)
        thread.start()
        try:
            self.assertTrue(first.wait(3))
            time.sleep(0.1)
            self.assertEqual(len(seen), 1)
            self.channel.basic_ack(seen[0].delivery_tag)
            deadline = time.monotonic() + 3
            while len(seen) < 2 and time.monotonic() < deadline:
                time.sleep(0.01)
            self.assertEqual(len(seen), 2)
        finally:
            self.channel.stop_consuming()
            thread.join(5)
            self.assertFalse(thread.is_alive())
