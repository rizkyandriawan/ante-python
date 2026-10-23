import tempfile
import unittest
from pathlib import Path

import pika

from ante import Ante, AnteError


class AmqpTests(unittest.TestCase):
    def test_publish_confirm_shared_engine_and_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "db"
            with Ante.open(path) as ante:
                host, port = ante.serve_amqp(port=0)
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host, port)
                )
                try:
                    channel = connection.channel()
                    channel.queue_declare("jobs", durable=True)
                    channel.confirm_delivery()
                    channel.basic_publish("", "jobs", b"from-amqp")
                    [delivery] = ante.take("jobs")
                    self.assertEqual(delivery.raw, b"from-amqp")
                    delivery.ack()
                    ante.publish("jobs", b"from-ffi")
                    method, _, body = channel.basic_get("jobs")
                    self.assertEqual(body, b"from-ffi")
                    channel.basic_ack(method.delivery_tag)
                finally:
                    connection.close()
                with self.assertRaises(AnteError):
                    ante.serve_amqp(port=0)
                ante.stop_amqp()
                ante.serve_amqp(port=port)
            with Ante.open(path) as reopened:
                self.assertEqual(reopened.info("jobs")["total"], 0)
                reopened.serve_amqp(port=port)


if __name__ == "__main__":
    unittest.main()
