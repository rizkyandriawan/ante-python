from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING

from .codec import Parser
from .errors import AnteError

if TYPE_CHECKING:
    from .client import Ante, Delivery, Handler

log = logging.getLogger("ante")


class Consumer:
    def __init__(
        self,
        ante: Ante,
        queue: str,
        handler: Handler,
        *,
        prefetch: int,
        concurrency: int | None,
        lease_ms: int,
        wait_ms: int,
        parse: Parser | None,
        on_error: Callable[[BaseException, Delivery], str] | None,
        max_attempts: int | None,
        shutdown_timeout: float,
        ack_on_return: bool,
    ) -> None:
        if prefetch < 1:
            raise ValueError("prefetch must be at least 1")
        workers = concurrency if concurrency is not None else prefetch
        if workers < 1:
            raise ValueError("concurrency must be at least 1")
        self._ante = ante
        self._queue = queue
        self._queue_id = ante.info(queue)["id"]
        self._handler = handler
        self._prefetch = prefetch
        self._concurrency = workers
        self._lease_ms = lease_ms
        self._wait_ms = wait_ms
        self._parse = parse
        self._on_error = on_error
        self._max_attempts = max_attempts
        if lease_ms < 0 or wait_ms < 0 or shutdown_timeout < 0:
            raise ValueError("timeouts must be nonnegative")
        self._ack_on_return = ack_on_return
        self._shutdown_timeout = shutdown_timeout

        self._stopped = threading.Event()
        self._condition = threading.Condition()
        self._active = 0
        self._outstanding: dict[str, Delivery] = {}
        self._thread: threading.Thread | None = None
        self._executor: ThreadPoolExecutor | None = None

    @property
    def queue(self) -> str:
        return self._queue

    @property
    def active(self) -> int:
        with self._condition:
            return self._active

    def start(self) -> Consumer:
        if self._thread is not None:
            return self
        self._executor = ThreadPoolExecutor(
            max_workers=self._concurrency,
            thread_name_prefix="ante",
        )
        self._thread = threading.Thread(
            target=self._loop,
            name=f"ante-{self._queue}",
            daemon=True,
        )
        self._thread.start()
        return self

    def wait(self) -> None:
        if self._thread is not None:
            self._thread.join()

    def stop(self) -> None:
        self.request_stop()
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join(self._shutdown_timeout)

        deadline = time.monotonic() + self._shutdown_timeout
        with self._condition:
            while self._active > 0 and time.monotonic() < deadline:
                self._condition.wait(0.1)
            remaining = self._active
        if remaining:
            log.warning(
                "ante: %s consumer stopped with %d handler(s) still running",
                self._queue,
                remaining,
            )

        if self._executor is not None:
            self._executor.shutdown(wait=False, cancel_futures=False)
            self._executor = None
        self._ante._forget(self)

    def request_stop(self) -> None:
        self._stopped.set()
        with self._condition:
            self._condition.notify_all()

    def __enter__(self) -> Consumer:
        return self.start()

    def __exit__(self, *_exc: object) -> None:
        self.stop()

    def _loop(self) -> None:
        while not self._stopped.is_set():
            with self._condition:
                now_ms = time.time() * 1000
                self._outstanding = {
                    token: delivery
                    for token, delivery in self._outstanding.items()
                    if not delivery.settled and delivery.deadline_ms > now_ms
                }
                capacity = self._prefetch - len(self._outstanding)
                if capacity <= 0:
                    self._condition.wait(0.02)
            if self._stopped.is_set() or capacity <= 0:
                continue

            try:
                deliveries = self._ante._take_now(
                    self._queue,
                    max=min(capacity, 256),
                    lease_ms=self._lease_ms,
                    parse=self._parse,
                    queue_id=self._queue_id,
                )
                if not deliveries:
                    self._stopped.wait(
                        min(0.02, self._wait_ms / 1000) if self._wait_ms else 0.02
                    )
                    continue
            except AnteError as error:
                if self._stopped.is_set() or error.code in {
                    "CLOSED",
                    "QUEUE_NOT_FOUND",
                }:
                    self.request_stop()
                    break
                log.warning("ante: take failed on %s: %s", self._queue, error)
                self._stopped.wait(0.5)
                continue
            for delivery in deliveries:
                with self._condition:
                    self._active += 1
                    self._outstanding[delivery.token] = delivery
                assert self._executor is not None
                self._executor.submit(self._handle, delivery)

    def _handle(self, delivery: Delivery) -> None:
        try:
            try:
                self._handler(delivery.payload, delivery)
            except Exception as error:
                self._fail(error, delivery)
            else:
                if self._ack_on_return and not delivery.settled:
                    try:
                        delivery.ack()
                    except (AnteError, OSError, ConnectionError) as error:
                        log.warning(
                            "ante: could not ack %s on %s: %s",
                            delivery.token,
                            self._queue,
                            error,
                        )
        finally:
            with self._condition:
                self._active -= 1
                self._condition.notify_all()

    def _fail(self, error: Exception, delivery: Delivery) -> None:
        if delivery.settled:
            return
        try:
            action = "requeue"
            if self._on_error is not None:
                action = self._on_error(error, delivery)
            elif (
                self._max_attempts is not None
                and delivery.attempts >= self._max_attempts
            ):
                action = "drop"

            log.warning(
                "ante: handler failed on %s (attempt %d, %s): %s",
                self._queue,
                delivery.attempts,
                action,
                error,
            )
            if action == "requeue":
                delivery.requeue()
            else:
                delivery.ack()
        except AnteError as settle_error:
            log.warning(
                "ante: could not settle delivery %s on %s: %s",
                delivery.token,
                self._queue,
                settle_error,
            )
        except (OSError, ConnectionError) as settle_error:
            log.warning(
                "ante: settle connection lost on %s: %s",
                self._queue,
                settle_error,
            )
