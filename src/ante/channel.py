"""Pika-style operations over the in-process Ante engine (no AMQP transport)."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from types import SimpleNamespace
from typing import TYPE_CHECKING

from .errors import AnteError

if TYPE_CHECKING:
    from .client import Ante, Delivery


@dataclass
class BasicProperties:
    content_type: str | None = None
    message_id: str | None = None
    correlation_id: str | None = None
    reply_to: str | None = None
    delivery_mode: int = 2  # Ante messages are always durable.


class Channel:
    def __init__(self, ante: Ante, *, concurrency: int = 1) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        self._ante = ante
        self._concurrency = concurrency
        self._prefetch = 16
        self._specs: dict[str, tuple[str, Callable, bool, int]] = {}
        self._consumers: dict[str, object] = {}
        self._deliveries: dict[int, Delivery] = {}
        self._lock = threading.Lock()
        self._settle_lock = threading.RLock()
        self._done = threading.Event()
        self._done.set()
        self._local = threading.local()
        self._close_requested = False
        self._next_tag = 1
        self._stop = threading.Event()
        self._running = False
        self.is_open = True

    def _check(self) -> None:
        if not self.is_open or self._ante._closed:
            raise AnteError("CLOSED", "channel is closed")

    def queue_declare(
        self,
        queue: str,
        passive: bool = False,
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: dict | None = None,
    ):
        self._check()
        if exclusive or auto_delete or arguments:
            raise NotImplementedError(
                "Ante supports named durable queues without arguments"
            )
        if not passive:
            self._ante.declare(queue)
        info = self._ante.info(queue)
        count = sum(c.queue == queue for c in self._ante._consumers)
        return SimpleNamespace(
            method=SimpleNamespace(
                queue=queue, message_count=info["ready"], consumer_count=count
            )
        )

    def basic_qos(
        self, prefetch_size: int = 0, prefetch_count: int = 16, global_qos: bool = False
    ) -> None:
        self._check()
        if prefetch_size or global_qos:
            raise NotImplementedError("use per-consumer prefetch_count")
        if prefetch_count < 1:
            raise ValueError("prefetch_count must be positive")
        if self._running:
            raise RuntimeError("configure QoS before start_consuming")
        self._prefetch = prefetch_count

    def basic_publish(
        self,
        exchange: str,
        routing_key: str,
        body,
        properties: BasicProperties | None = None,
        mandatory: bool = False,
    ) -> int:
        self._check()
        if exchange:
            raise NotImplementedError("Ante supports only the default exchange ('')")
        props = properties or BasicProperties()
        return self._ante.publish(
            routing_key,
            body,
            content_type=props.content_type,
            message_id=props.message_id,
            correlation_id=props.correlation_id,
            reply_to=props.reply_to,
        )

    def basic_consume(
        self,
        queue: str,
        on_message_callback: Callable,
        auto_ack: bool = False,
        exclusive: bool = False,
        consumer_tag: str | None = None,
        arguments: dict | None = None,
        *,
        concurrency: int | None = None,
    ) -> str:
        self._check()
        if exclusive or arguments:
            raise NotImplementedError(
                "exclusive consumers and arguments are unsupported"
            )
        if self._running:
            raise RuntimeError("register consumers before start_consuming")
        workers = self._concurrency if concurrency is None else concurrency
        if not callable(on_message_callback):
            raise TypeError("on_message_callback must be callable")
        if workers < 1:
            raise ValueError("concurrency must be at least 1")
        tag = consumer_tag or f"ctag-{len(self._specs) + 1}"
        if tag in self._specs:
            raise ValueError(f"duplicate consumer tag: {tag}")
        self._ante.info(queue)  # Require declaration, like basic_consume in pika.
        self._specs[tag] = (queue, on_message_callback, auto_ack, workers)
        return tag

    def _message(self, delivery: Delivery, queue: str, consumer_tag: str | None):
        with self._lock:
            now_ms = time.time() * 1000
            self._deliveries = {
                tag: d
                for tag, d in self._deliveries.items()
                if not d.settled and d.deadline_ms > now_ms
            }
            tag = self._next_tag
            self._next_tag += 1
            self._deliveries[tag] = delivery
        method = SimpleNamespace(
            delivery_tag=tag,
            consumer_tag=consumer_tag,
            redelivered=delivery.attempts > 1,
            exchange="",
            routing_key=queue,
            attempts=delivery.attempts,
        )
        props = BasicProperties(
            content_type=delivery.content_type,
            message_id=delivery.application_message_id,
            correlation_id=delivery.correlation_id,
            reply_to=delivery.reply_to,
        )
        return method, props, delivery.raw

    def start_consuming(self) -> None:
        self._check()
        if self._running:
            raise RuntimeError("channel is already consuming")
        self._stop.clear()
        self._done.clear()
        self._running = True
        try:
            for tag, (queue, callback, auto_ack, workers) in list(self._specs.items()):
                if self._stop.is_set():
                    break
                ready = threading.Event()

                def handler(
                    _payload,
                    delivery,
                    *,
                    queue=queue,
                    tag=tag,
                    callback=callback,
                    auto_ack=auto_ack,
                    ready=ready,
                ):
                    ready.wait()  # Register the consumer before callbacks can cancel it.
                    method, props, body = self._message(delivery, queue, tag)
                    if auto_ack:
                        self.basic_ack(method.delivery_tag)
                    self._local.in_callback = True
                    try:
                        callback(self, method, props, body)
                    finally:
                        self._local.in_callback = False

                self._consumers[tag] = self._ante.consume(
                    queue,
                    handler,
                    prefetch=self._prefetch,
                    concurrency=workers,
                    declare=False,
                    parse=lambda raw, _kind: raw,
                    ack_on_return=False,
                )
                ready.set()
            while self._consumers and not self._stop.wait(0.05):
                if not any(not c._stopped.is_set() for c in self._consumers.values()):
                    break
        finally:
            for consumer in list(self._consumers.values()):
                consumer.stop()
            self._consumers.clear()
            self._running = False
            self._done.set()
            if self._close_requested:
                self.close()

    def basic_get(self, queue: str, auto_ack: bool = False):
        self._check()
        deliveries = self._ante.take(queue, parse=lambda raw, _kind: raw)
        if not deliveries:
            return None, None, None
        delivery = deliveries[0]
        method, props, body = self._message(delivery, queue, None)
        method.message_count = self._ante.info(queue)["ready"]
        if auto_ack:
            self.basic_ack(method.delivery_tag)
        return method, props, body

    def _select(self, delivery_tag: int, multiple: bool) -> list[tuple[int, Delivery]]:
        self._check()
        with self._lock:
            if multiple:
                selected = [
                    (tag, d)
                    for tag, d in self._deliveries.items()
                    if delivery_tag == 0 or tag <= delivery_tag
                ]
            else:
                selected = (
                    [(delivery_tag, self._deliveries[delivery_tag])]
                    if delivery_tag in self._deliveries
                    else []
                )
        if not selected and not (multiple and delivery_tag == 0):
            raise AnteError("UNKNOWN_DELIVERY", str(delivery_tag))
        return selected

    def basic_ack(self, delivery_tag: int = 0, multiple: bool = False) -> None:
        with self._settle_lock:
            for tag, delivery in self._select(delivery_tag, multiple):
                delivery.ack()
                with self._lock:
                    self._deliveries.pop(tag, None)

    def basic_nack(
        self, delivery_tag: int = 0, multiple: bool = False, requeue: bool = True
    ) -> None:
        with self._settle_lock:
            for tag, delivery in self._select(delivery_tag, multiple):
                if requeue:
                    delivery.requeue()
                else:
                    delivery.ack()
                with self._lock:
                    self._deliveries.pop(tag, None)

    def basic_reject(self, delivery_tag: int, requeue: bool = True) -> None:
        self.basic_nack(delivery_tag, requeue=requeue)

    def basic_cancel(self, consumer_tag: str) -> None:
        self.stop_consuming(consumer_tag)
        self._specs.pop(consumer_tag, None)

    def stop_consuming(self, consumer_tag: str | None = None) -> None:
        if consumer_tag is None:
            self._stop.set()
        for tag, consumer in list(self._consumers.items()):
            if consumer_tag is None or tag == consumer_tag:
                consumer.request_stop()

    def queue_purge(self, queue: str):
        self._check()
        return SimpleNamespace(
            method=SimpleNamespace(message_count=self._ante.purge(queue))
        )

    def queue_delete(self, queue: str, if_unused: bool = False, if_empty: bool = False):
        self._check()
        if if_unused or if_empty:
            raise NotImplementedError("conditional queue deletion is unsupported")
        return SimpleNamespace(
            method=SimpleNamespace(message_count=self._ante.delete_queue(queue))
        )

    def close(self) -> None:
        if not self.is_open:
            return
        self.stop_consuming()
        if getattr(self._local, "in_callback", False):
            self._close_requested = True
            return
        self._done.wait()
        with self._settle_lock:
            for tag, delivery in list(self._deliveries.items()):
                if not delivery.settled:
                    try:
                        delivery.requeue()
                    except AnteError as error:
                        if error.code not in {"STALE_DELIVERY", "UNKNOWN_DELIVERY"}:
                            raise
                self._deliveries.pop(tag, None)
        self.is_open = False
        self._ante._channels.discard(self)

    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        self.close()
