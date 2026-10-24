import tempfile
import threading
import time
import unittest
from pathlib import Path

from ante import Ante


def wait_for(predicate, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return False


class SmokeTests(unittest.TestCase):
    def setUp(self):
        self._dir = tempfile.TemporaryDirectory(prefix="ante-py-")
        self.ante = Ante.open(Path(self._dir.name) / "ante.sqlite3")
        self.ante.declare("jobs")

    def tearDown(self):
        self.ante.close()
        self._dir.cleanup()

    def test_publish_take_ack(self):
        message_id = self.ante.publish("jobs", {"task": "resize", "id": 42})
        self.assertGreater(message_id, 0)

        [delivery] = self.ante.take("jobs", max=1, wait_ms=1000)
        self.assertEqual(delivery.payload, {"task": "resize", "id": 42})
        self.assertEqual(delivery.attempts, 1)
        delivery.ack()

        info = self.ante.info("jobs")
        self.assertEqual(info["ready"], 0)
        self.assertEqual(info["inflight"], 0)

    def test_message_status_and_history(self):
        message_id = self.ante.publish("jobs", {"n": 1})
        status = self.ante.message_status("jobs", message_id)
        self.assertEqual(status["state"], "ready")
        self.assertEqual(status["attempts"], 0)
        self.assertEqual(status["history"], [])

        [delivery] = self.ante.take("jobs", max=1, wait_ms=1000)
        status = self.ante.message_status("jobs", message_id)
        self.assertEqual(status["state"], "inflight")
        self.assertEqual(status["history"][0]["outcome"], "inflight")
        self.assertIsNone(status["history"][0]["finished_at_ms"])

        delivery.ack()
        status = self.ante.message_status("jobs", message_id)
        self.assertEqual(status["state"], "acked")
        self.assertEqual(status["history"][0]["outcome"], "acked")
        self.assertIsNotNone(status["history"][0]["finished_at_ms"])
        self.assertIsNone(self.ante.message_status("jobs", message_id + 1))

    def test_publish_batch(self):
        ids = self.ante.publish_batch("jobs", [{"n": 1}, {"n": 2}, {"n": 3}])
        self.assertEqual(len(ids), 3)
        deliveries = self.ante.take("jobs", max=3, wait_ms=1000)
        self.assertEqual([d.payload["n"] for d in deliveries], [1, 2, 3])
        self.assertTrue(
            all(result.ok for result in self.ante.ack(*[d.token for d in deliveries]))
        )
        self.assertEqual(self.ante.info("jobs")["total"], 0)

    def test_consumer(self):
        received = []
        done = threading.Event()

        def handler(payload, delivery):
            received.append(payload["n"])
            if len(received) == 3:
                done.set()

        consumer = self.ante.consume("jobs", handler, prefetch=2)
        for index in range(3):
            self.ante.publish("jobs", {"n": index})
        self.assertTrue(done.wait(5), "consumer did not receive three messages")
        consumer.stop()

        self.assertEqual(sorted(received), [0, 1, 2])
        self.assertTrue(
            wait_for(lambda: self.ante.info("jobs")["total"] == 0),
            "acked deliveries were not released",
        )

    def test_parallel_handlers(self):
        workers = 4
        lock = threading.Lock()
        barrier = threading.Barrier(workers, timeout=5)
        done = threading.Event()
        running = 0
        peak = 0
        completed = 0

        def handler(_payload, _delivery):
            nonlocal running, peak, completed
            with lock:
                running += 1
                peak = max(peak, running)
            try:
                barrier.wait()
            finally:
                with lock:
                    running -= 1
                    completed += 1
                    if completed == workers:
                        done.set()

        consumer = self.ante.consume("jobs", handler, prefetch=8, concurrency=workers)
        for index in range(workers):
            self.ante.publish("jobs", {"n": index})
        self.assertTrue(done.wait(5), "handlers did not run in parallel")
        self.assertEqual(peak, workers)
        consumer.stop()

    def test_retry_increments_attempts(self):
        seen = []
        done = threading.Event()

        def handler(payload, delivery):
            seen.append(delivery.attempts)
            if delivery.attempts == 1:
                raise RuntimeError("transient")
            done.set()

        consumer = self.ante.consume("jobs", handler, prefetch=1, max_attempts=3)
        self.ante.publish("jobs", {"n": 1})
        self.assertTrue(done.wait(5), "message was not retried")
        self.assertEqual(seen[:2], [1, 2])
        consumer.stop()

    def test_drop_policy_discards_poison_message(self):
        consumer = self.ante.consume(
            "jobs",
            lambda _payload, _delivery: (_ for _ in ()).throw(RuntimeError("bad")),
            prefetch=1,
            on_error=lambda _error, _delivery: "drop",
        )
        message_id = self.ante.publish("jobs", {"n": 1})
        self.assertTrue(
            wait_for(lambda: self.ante.info("jobs")["total"] == 0),
            "poison message was not dropped",
        )
        status = self.ante.message_status("jobs", message_id)
        self.assertEqual(status["state"], "discarded")
        consumer.stop()


if __name__ == "__main__":
    unittest.main()
