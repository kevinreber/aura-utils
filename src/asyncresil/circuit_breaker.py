"""Circuit breaker for protecting downstream services from cascading failures."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import Literal, ParamSpec, TypeVar

logger = logging.getLogger(__name__)

P = ParamSpec("P")
T = TypeVar("T")

State = Literal["closed", "open", "half_open"]


class CircuitBreakerOpen(Exception):
    """Raised when a call is rejected because the breaker is open."""


class CircuitBreaker:
    """Three-state circuit breaker.

    - ``closed``: calls pass through. After ``failure_threshold`` consecutive
      failures, transitions to ``open``.
    - ``open``: calls are rejected with ``CircuitBreakerOpen`` until
      ``recovery_timeout`` seconds elapse, then transitions to ``half_open``.
    - ``half_open``: up to ``half_open_max_calls`` trial calls run concurrently.
      A successful trial closes the breaker; a failed trial reopens it.

    The ``clock`` parameter is the source of monotonic time. Tests inject a
    fake clock to advance time without sleeping.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        half_open_max_calls: int = 1,
        name: str | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be >= 1")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout must be >= 0")

        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls
        self._name = name
        self._clock = clock

        self._state: State = "closed"
        self._failure_count = 0
        self._opened_at = 0.0
        self._half_open_in_flight = 0
        self._lock = asyncio.Lock()

    @property
    def state(self) -> State:
        return self._state

    async def call(
        self,
        fn: Callable[P, Awaitable[T]],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> T:
        async with self._lock:
            self._maybe_transition_to_half_open()

            if self._state == "open":
                raise CircuitBreakerOpen(self._reject_message())

            if (
                self._state == "half_open"
                and self._half_open_in_flight >= self._half_open_max_calls
            ):
                raise CircuitBreakerOpen(self._reject_message())

            if self._state == "half_open":
                self._half_open_in_flight += 1

        try:
            result = await fn(*args, **kwargs)
        except Exception:
            async with self._lock:
                self._record_failure()
            raise

        async with self._lock:
            self._record_success()
        return result

    def _maybe_transition_to_half_open(self) -> None:
        if (
            self._state == "open"
            and self._clock() - self._opened_at >= self._recovery_timeout
        ):
            self._transition("half_open")

    def _record_success(self) -> None:
        if self._state == "half_open":
            self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
            self._transition("closed")
        elif self._state == "closed":
            self._failure_count = 0

    def _record_failure(self) -> None:
        if self._state == "half_open":
            self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
            self._transition("open")
        elif self._state == "closed":
            self._failure_count += 1
            if self._failure_count >= self._failure_threshold:
                self._transition("open")

    def _transition(self, new_state: State) -> None:
        if new_state == self._state:
            return
        old = self._state
        self._state = new_state
        if new_state == "closed":
            self._failure_count = 0
            self._half_open_in_flight = 0
        elif new_state == "open":
            self._opened_at = self._clock()
            self._half_open_in_flight = 0
        else:
            self._half_open_in_flight = 0

        logger.info(
            "Circuit breaker %s: %s -> %s",
            self._name or "<unnamed>",
            old,
            new_state,
        )

    def _reject_message(self) -> str:
        suffix = f" '{self._name}'" if self._name else ""
        return f"circuit breaker{suffix} is open"
