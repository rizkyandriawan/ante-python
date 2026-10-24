from __future__ import annotations

import os
import threading
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ._native import Engine, NativeError
from .codec import Parser, decode_payload, encode_payload
from .errors import AnteError

if TYPE_CHECKING:
    from .consumer import Consumer


@dataclass
class AckResult:
    ok: bool
    code: str | None = None
    message: str = ""


@dataclass
class Delivery:
    token: str
    message_id: int
    attempts: int
    raw: bytes
    content_type: str | None
    correlation_id: str | None
    reply_to: str | None
    payload: Any
    application_message_id: str | None
    deadline_ms: int
    _ante: Ante = field(repr=False)
    _lease_ms: int = field(default=30_000, repr=False)
    _settled: bool = field(default=False, repr=False)

    @property
    def settled(self) -> bool:
        return self._settled

    def ack(self) -> None:
        if self._settled:
            return
        [result] = self._ante.ack(self.token)
        if not result.ok:
            raise AnteError(result.code or "ACK", result.message)
        self._settled = True

    def requeue(self) -> None:
        if self._settled:
            return
        self._ante.requeue(self.token)
        self._settled = True

    def discard(self) -> None:
        """Remove the delivery without treating it as successful."""
        if self._settled:
            return
        outcome = self._ante._call("discard", self.token)
        if outcome != "OK":
            raise AnteError(outcome, "cannot discard delivery")
        self._settled = True

    def touch(self, lease_ms: int | None = None) -> None:
        """Extend the in-flight lease for this delivery."""
        self._ante.touch(
            self.token, self._lease_ms if lease_ms is None else lease_ms
        )

    def complete(
        self,
        queue: str,
        payload: Any,
        *,
        content_type: str | None = None,
        message_id: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        discard: bool = False,
    ) -> int:
        """Publish the result to ``queue`` and settle this delivery atomically."""
        if self._settled:
            raise AnteError("SETTLED", "delivery is already settled")
        result = self._ante.complete(
            self.token,
            queue,
            payload,
            content_type=content_type,
            message_id=message_id,
            correlation_id=correlation_id,
            reply_to=reply_to,
            discard=discard,
        )
        self._settled = True
        return result


Handler = Callable[[Any, Delivery], None]
ErrorPolicy = Callable[[BaseException, Delivery], str]


class Ante:
    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._consumers: set[Consumer] = set()
        self._channels: set[Any] = set()
        self._closed = False
        self._wake = threading.Event()

    @classmethod
    def open(
        cls,
        db: str | os.PathLike[str] | None = None,
        *,
        history_retention_ms: int | None = None,
        history_max_rows: int | None = None,
    ) -> Ante:
        path = _database_path(db)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            return cls(Engine(str(path), history_retention_ms, history_max_rows))
        except NativeError as error:
            raise AnteError(*error.args) from error

    def _call(self, method: str, *args: Any) -> Any:
        if self._closed:
            raise AnteError("CLOSED", "client is closed")
        try:
            return getattr(self._engine, method)(*args)
        except NativeError as error:
            raise AnteError(*error.args) from error

    def __enter__(self) -> Ante:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def serve_amqp(
        self,
        host: str = "127.0.0.1",
        port: int = 5672,
        *,
        security_path: str | os.PathLike[str] | None = None,
    ) -> tuple[str, int]:
        """Listen for AMQP clients on the same embedded queue engine.

        Passing ``security_path`` enables TLS, credential authentication, and
        queue-scoped permissions from the JSON security file.
        """
        return self._call(
            "serve_amqp",
            host,
            port,
            None if security_path is None else os.fspath(security_path),
        )

    def stop_amqp(self) -> None:
        self._call("stop_amqp")

    def declare(self, queue: str) -> int:
        return self._call("declare", queue)

    def publish(
        self,
        queue: str,
        payload: Any,
        *,
        content_type: str | None = None,
        message_id: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
    ) -> int:
        body, default_type = encode_payload(payload)
        result = self._call(
            "publish",
            queue,
            body,
            (
                content_type or default_type,
                message_id or "",
                correlation_id or "",
                reply_to or "",
            ),
        )
        self._wake.set()
        return result

    def complete(
        self,
        token: str,
        queue: str,
        payload: Any,
        *,
        content_type: str | None = None,
        message_id: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        discard: bool = False,
    ) -> int:
        """Settle a delivery and publish its result atomically."""
        body, default_type = encode_payload(payload)
        result = self._call(
            "complete",
            token,
            queue,
            body,
            (
                content_type or default_type,
                message_id or "",
                correlation_id or "",
                reply_to or "",
            ),
            discard,
        )
        self._wake.set()
        return result

    def publish_batch(self, queue: str, payloads: Iterable[Any]) -> list[int]:
        result = self._call(
            "publish_batch", queue, [encode_payload(p) for p in payloads]
        )
        self._wake.set()
        return result

    def take(
        self,
        queue: str,
        *,
        max: int = 1,
        lease_ms: int = 30_000,
        wait_ms: int = 0,
        parse: Parser | None = None,
    ) -> list[Delivery]:
        if max < 1 or lease_ms < 0 or wait_ms < 0:
            raise ValueError(
                "max must be positive; lease_ms and wait_ms must be nonnegative"
            )
        deadline = time.monotonic() + wait_ms / 1000
        while True:
            self._wake.clear()
            deliveries = self._take_now(queue, max, lease_ms, parse)
            if deliveries or time.monotonic() >= deadline:
                return deliveries
            self._wake.wait(min(0.02, deadline - time.monotonic()))

    def _take_now(self, queue, max, lease_ms, parse=None, queue_id=None):
        records = self._call("take", queue, max, lease_ms, queue_id)
        return [self._delivery(record, parse, lease_ms) for record in records]

    def _delivery(
        self, record: Any, parse: Parser | None, lease_ms: int = 30_000
    ) -> Delivery:
        raw = bytes(record.payload)
        return Delivery(
            token=record.token,
            message_id=record.message_id,
            attempts=record.attempts,
            raw=raw,
            content_type=record.content_type,
            correlation_id=record.correlation_id,
            reply_to=record.reply_to,
            application_message_id=record.application_message_id,
            deadline_ms=record.deadline_ms,
            payload=decode_payload(raw, record.content_type, parse),
            _ante=self,
        )

    def ack(self, *tokens: str) -> list[AckResult]:
        if not tokens:
            return []
        return [
            AckResult(code == "OK", None if code == "OK" else code)
            for code in self._call("ack", list(tokens))
        ]

    def requeue(self, token: str) -> None:
        outcome = self._call("requeue", token)
        if outcome != "OK":
            raise AnteError(outcome, "cannot requeue delivery")
        self._wake.set()

    def discard(self, token: str) -> None:
        outcome = self._call("discard", token)
        if outcome != "OK":
            raise AnteError(outcome, "cannot discard delivery")

    def info(self, queue: str) -> dict[str, Any]:
        return _info(self._call("info", queue))

    def list_queues(self) -> list[dict[str, Any]]:
        return [_info(info) for info in self._call("list_queues")]

    def message_status(self, queue: str, message_id: int) -> dict[str, Any] | None:
        """Return the current state and retained attempt history for a message."""
        status = self._call("message_status", queue, message_id)
        if status is None:
            return None
        return {
            "queue": status.queue,
            "message_id": status.message_id,
            "state": status.state,
            "attempts": status.attempts,
            "created_at_ms": status.created_at_ms,
            "updated_at_ms": status.updated_at_ms,
            "history": [
                {
                    "attempt": attempt.attempt,
                    "delivery_id": attempt.delivery_id,
                    "started_at_ms": attempt.started_at_ms,
                    "finished_at_ms": attempt.finished_at_ms,
                    "outcome": attempt.outcome,
                }
                for attempt in status.history
            ],
        }

    def channel(self, *, concurrency: int = 1):
        from .channel import Channel

        if self._closed:
            raise AnteError("CLOSED", "client is closed")
        channel = Channel(self, concurrency=concurrency)
        self._channels.add(channel)
        return channel

    def queue_exists(self, queue: str) -> bool:
        return self._call("queue_exists", queue)

    def touch(self, token: str, lease_ms: int = 30_000) -> None:
        outcome = self._call("touch", token, lease_ms)
        if outcome != "OK":
            raise AnteError(outcome, "cannot extend delivery lease")

    def prune_history(self) -> int:
        """Apply the attempt-history retention policy and return rows removed."""
        return self._call("prune_history")

    def recover_expired(self) -> int:
        return self._call("recover_expired")

    def purge(self, queue: str) -> int:
        return self._call("purge", queue)

    def delete_queue(self, queue: str) -> int:
        return self._call("delete_queue", queue)

    def consume(
        self,
        queue: str,
        handler: Handler,
        *,
        prefetch: int = 16,
        concurrency: int | None = None,
        lease_ms: int = 30_000,
        wait_ms: int = 5_000,
        parse: Parser | None = None,
        on_error: ErrorPolicy | None = None,
        max_attempts: int | None = 5,
        declare: bool = True,
        shutdown_timeout: float = 10.0,
        ack_on_return: bool = True,
    ) -> Consumer:
        from .consumer import Consumer

        if self._closed:
            raise AnteError("CLOSED", "client is closed")
        if declare:
            self.declare(queue)
        consumer = Consumer(
            self,
            queue,
            handler,
            prefetch=prefetch,
            concurrency=concurrency,
            lease_ms=lease_ms,
            wait_ms=wait_ms,
            parse=parse,
            on_error=on_error,
            max_attempts=max_attempts,
            shutdown_timeout=shutdown_timeout,
            ack_on_return=ack_on_return,
        )
        self._consumers.add(consumer)
        try:
            consumer.start()
        except Exception:
            self._consumers.discard(consumer)
            raise
        return consumer

    def run(
        self,
        queue: str,
        handler: Handler,
        **options: Any,
    ) -> None:
        consumer = self.consume(queue, handler, **options)
        try:
            consumer.wait()
        except KeyboardInterrupt:
            pass
        finally:
            consumer.stop()

    def _forget(self, consumer: Consumer) -> None:
        self._consumers.discard(consumer)

    def close(self) -> None:
        if self._closed:
            return
        for channel in list(self._channels):
            channel.close()
        for consumer in list(self._consumers):
            consumer.stop()
        self._engine.close()
        self._closed = True
        self._wake.set()


def _info(info: Any) -> dict[str, Any]:
    return {
        "name": info.name,
        "id": info.id,
        "ready": info.ready,
        "inflight": info.inflight,
        "total": info.ready + info.inflight,
    }


def _database_path(db: str | os.PathLike[str] | None) -> Path:
    if db is not None:
        return Path(db).expanduser()
    configured = os.environ.get("ANTE_DB")
    return (
        Path(configured).expanduser()
        if configured
        else Path.cwd() / ".ante" / "ante.sqlite3"
    )
