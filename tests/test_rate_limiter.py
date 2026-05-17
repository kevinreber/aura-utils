"""Tests for TokenBucketRateLimiter."""

from __future__ import annotations

import asyncio

import pytest

from asyncresil import TokenBucketRateLimiter
from fakes import FakeClock


class FakeSleeper:
    """Records sleep calls and advances a bound FakeClock by the slept amount.

    Yields to the event loop at the end so concurrent coroutines actually
    interleave under `asyncio.gather` instead of running to completion one
    at a time.
    """

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.calls: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.calls.append(delay)
        self.clock.advance(delay)
        await asyncio.sleep(0)


def _new_limiter(
    *, rate: float = 10.0, capacity: int = 5
) -> tuple[TokenBucketRateLimiter, FakeClock, FakeSleeper]:
    clock = FakeClock()
    sleeper = FakeSleeper(clock)
    limiter = TokenBucketRateLimiter(
        rate=rate, capacity=capacity, clock=clock, _sleep=sleeper
    )
    return limiter, clock, sleeper


# ---- try_acquire ----


def test_starts_full() -> None:
    limiter, _, _ = _new_limiter(capacity=5)
    assert limiter.try_acquire(5) is True


def test_try_acquire_consumes_a_token() -> None:
    limiter, _, _ = _new_limiter(capacity=3)
    assert limiter.try_acquire(1) is True
    assert limiter.try_acquire(1) is True
    assert limiter.try_acquire(1) is True
    assert limiter.try_acquire(1) is False


def test_try_acquire_multiple_tokens_at_once() -> None:
    limiter, _, _ = _new_limiter(capacity=10)
    assert limiter.try_acquire(7) is True
    assert limiter.try_acquire(4) is False  # only 3 left
    assert limiter.try_acquire(3) is True


def test_try_acquire_refills_lazily() -> None:
    limiter, clock, _ = _new_limiter(rate=10.0, capacity=5)
    assert limiter.try_acquire(5) is True
    assert limiter.try_acquire(1) is False
    clock.advance(0.5)  # 5 tokens worth at rate=10/sec
    assert limiter.try_acquire(5) is True


def test_bucket_caps_at_capacity_no_overflow() -> None:
    limiter, clock, _ = _new_limiter(rate=10.0, capacity=5)
    clock.advance(1000.0)  # plenty of time, bucket should still cap at 5
    assert limiter.try_acquire(5) is True
    assert limiter.try_acquire(1) is False


# ---- acquire (blocking) ----


async def test_acquire_returns_immediately_when_tokens_available() -> None:
    limiter, _, sleeper = _new_limiter(capacity=5)
    await limiter.acquire(3)
    assert sleeper.calls == []  # never had to wait


async def test_acquire_waits_when_empty_then_succeeds() -> None:
    limiter, _, sleeper = _new_limiter(rate=10.0, capacity=5)
    # Drain the bucket
    assert limiter.try_acquire(5) is True
    # Now ask for 5 more — needs to wait 5/10 = 0.5s
    await limiter.acquire(5)
    assert sleeper.calls == [0.5]


async def test_acquire_partial_bucket_waits_only_for_deficit() -> None:
    """If 2 tokens are available and we need 5, wait should be (5-2)/rate."""
    limiter, _, sleeper = _new_limiter(rate=10.0, capacity=5)
    assert limiter.try_acquire(3) is True  # leaves 2 tokens
    await limiter.acquire(5)
    assert sleeper.calls == [0.3]  # (5-2) / 10


async def test_acquire_re_loops_if_woken_too_early() -> None:
    """When a sibling takes tokens during our sleep, ``acquire`` must
    recompute and sleep again — not return with insufficient tokens."""
    clock = FakeClock()

    class StealingSleeper:
        """On the first sleep, drain the bucket after waking. Forces the
        acquire loop to compute a new wait and sleep again."""

        def __init__(self) -> None:
            self.calls: list[float] = []
            self.limiter: TokenBucketRateLimiter | None = None

        async def __call__(self, delay: float) -> None:
            self.calls.append(delay)
            clock.advance(delay)
            if len(self.calls) == 1 and self.limiter is not None:
                self.limiter.try_acquire(5)

    sleeper = StealingSleeper()
    limiter = TokenBucketRateLimiter(
        rate=10.0, capacity=5, clock=clock, _sleep=sleeper
    )
    sleeper.limiter = limiter

    assert limiter.try_acquire(5) is True  # drain
    await limiter.acquire(5)
    # First wait: 0.5s for 5 tokens; sibling stole them → second 0.5s wait
    assert sleeper.calls == [0.5, 0.5]


# ---- concurrency ----


async def test_two_concurrent_acquires_both_eventually_succeed() -> None:
    limiter, _, _ = _new_limiter(rate=10.0, capacity=5)
    assert limiter.try_acquire(5) is True

    results: list[str] = []

    async def grab(name: str) -> None:
        await limiter.acquire(3)
        results.append(name)

    await asyncio.gather(grab("a"), grab("b"))
    assert sorted(results) == ["a", "b"]


# ---- validation ----


def test_invalid_rate_raises() -> None:
    with pytest.raises(ValueError, match="rate"):
        TokenBucketRateLimiter(rate=0, capacity=5)
    with pytest.raises(ValueError, match="rate"):
        TokenBucketRateLimiter(rate=-1.0, capacity=5)


def test_invalid_capacity_raises() -> None:
    with pytest.raises(ValueError, match="capacity"):
        TokenBucketRateLimiter(rate=1.0, capacity=0)
    with pytest.raises(ValueError, match="capacity"):
        TokenBucketRateLimiter(rate=1.0, capacity=-3)


def test_try_acquire_zero_or_negative_tokens_raises() -> None:
    limiter, _, _ = _new_limiter()
    with pytest.raises(ValueError, match="tokens"):
        limiter.try_acquire(0)
    with pytest.raises(ValueError, match="tokens"):
        limiter.try_acquire(-1)


def test_try_acquire_more_than_capacity_raises() -> None:
    limiter, _, _ = _new_limiter(capacity=5)
    with pytest.raises(ValueError, match="capacity"):
        limiter.try_acquire(6)


async def test_acquire_more_than_capacity_raises() -> None:
    limiter, _, _ = _new_limiter(capacity=5)
    with pytest.raises(ValueError, match="capacity"):
        await limiter.acquire(100)
