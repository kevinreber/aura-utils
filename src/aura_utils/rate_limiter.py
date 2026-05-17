"""Token-bucket rate limiter for async code."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """Token-bucket limiter for capping operations per unit time.

    The bucket starts full at ``capacity`` and refills at ``rate`` tokens per
    second. Each acquire removes ``tokens`` from the bucket. ``acquire`` blocks
    until enough tokens are available; ``try_acquire`` is non-blocking.

    Refill is lazy — tokens are computed from elapsed clock time on demand,
    not by a background task.

    Intended for use within a single asyncio event loop. Not thread-safe:
    ``try_acquire`` is lock-free and relies on asyncio's cooperative scheduling
    for atomicity, so concurrent access from multiple OS threads (e.g. via
    ``loop.run_in_executor``) can corrupt state.

    The ``clock`` and ``_sleep`` parameters exist so tests can drive time
    deterministically without real ``asyncio.sleep`` calls.
    """

    def __init__(
        self,
        *,
        rate: float,
        capacity: int,
        clock: Callable[[], float] = time.monotonic,
        _sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if capacity <= 0:
            raise ValueError("capacity must be > 0")

        self._rate = rate
        self._capacity = capacity
        self._clock = clock
        self._sleep = _sleep

        self._tokens: float = float(capacity)
        self._last_refill = clock()
        self._lock = asyncio.Lock()

    def try_acquire(self, tokens: int = 1) -> bool:
        # No lock acquisition: this method must remain await-free so concurrent
        # coroutines cannot interleave with its body. Adding any await below
        # would race with `acquire`'s critical section.
        self._validate_tokens(tokens)
        self._refill()
        if self._tokens >= tokens:
            self._tokens -= tokens
            return True
        return False

    async def acquire(self, tokens: int = 1) -> None:
        self._validate_tokens(tokens)
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                wait = (tokens - self._tokens) / self._rate
            logger.debug(
                "limiter blocking for %.3fs awaiting %d token(s)", wait, tokens
            )
            await self._sleep(wait)

    def _refill(self) -> None:
        now = self._clock()
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(
                float(self._capacity),
                self._tokens + elapsed * self._rate,
            )
            self._last_refill = now

    def _validate_tokens(self, tokens: int) -> None:
        if tokens <= 0:
            raise ValueError("tokens must be > 0")
        if tokens > self._capacity:
            raise ValueError(
                f"tokens ({tokens}) cannot exceed capacity ({self._capacity})"
            )
