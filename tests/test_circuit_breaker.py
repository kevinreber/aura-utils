"""Tests for CircuitBreaker — full state machine + concurrency."""

from __future__ import annotations

import asyncio

import pytest

from aura_utils import CircuitBreaker, CircuitBreakerOpen


class FakeClock:
    """A controllable monotonic clock for tests."""

    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, delta: float) -> None:
        self.t += delta


async def _ok() -> str:
    return "ok"


async def _boom() -> str:
    raise RuntimeError("downstream failure")


# ---- initial state ----


def test_starts_closed() -> None:
    breaker = CircuitBreaker()
    assert breaker.state == "closed"


# ---- closed state ----


async def test_passes_calls_through_in_closed_state() -> None:
    breaker = CircuitBreaker(failure_threshold=3)
    assert await breaker.call(_ok) == "ok"
    assert breaker.state == "closed"


async def test_failures_below_threshold_stay_closed() -> None:
    breaker = CircuitBreaker(failure_threshold=3)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_boom)
    assert breaker.state == "closed"


async def test_consecutive_failures_open_breaker_at_threshold() -> None:
    breaker = CircuitBreaker(failure_threshold=3)
    for _ in range(3):
        with pytest.raises(RuntimeError):
            await breaker.call(_boom)
    assert breaker.state == "open"


async def test_success_resets_consecutive_failure_count() -> None:
    """Two failures, a success, then two more failures — should still be closed."""
    breaker = CircuitBreaker(failure_threshold=3)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_boom)
    await breaker.call(_ok)
    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(_boom)
    assert breaker.state == "closed"


# ---- open state ----


async def test_open_breaker_rejects_with_circuit_breaker_open() -> None:
    breaker = CircuitBreaker(failure_threshold=1)
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    assert breaker.state == "open"
    with pytest.raises(CircuitBreakerOpen):
        await breaker.call(_ok)


async def test_open_breaker_message_includes_name() -> None:
    breaker = CircuitBreaker(failure_threshold=1, name="payments")
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    with pytest.raises(CircuitBreakerOpen, match="'payments'"):
        await breaker.call(_ok)


async def test_open_stays_open_before_recovery_timeout() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=30.0, clock=clock)
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    clock.advance(29.999)
    with pytest.raises(CircuitBreakerOpen):
        await breaker.call(_ok)
    assert breaker.state == "open"


# ---- open → half-open ----


async def test_open_transitions_to_half_open_after_recovery_timeout() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=30.0, clock=clock)
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    assert breaker.state == "open"
    clock.advance(30.0)
    # Next call triggers the transition and runs as a half-open trial
    assert await breaker.call(_ok) == "ok"
    assert breaker.state == "closed"


# ---- half-open ----


async def test_half_open_success_closes_breaker() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0, clock=clock)
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    clock.advance(10.0)
    await breaker.call(_ok)
    assert breaker.state == "closed"


async def test_half_open_failure_reopens_breaker() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0, clock=clock)
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    clock.advance(10.0)
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    assert breaker.state == "open"


async def test_half_open_failure_resets_recovery_timer() -> None:
    """A failed half-open trial restarts the recovery_timeout window."""
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=10.0, clock=clock)
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    clock.advance(10.0)
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    # Re-opened. The 10s window starts over from now, not from the original open.
    clock.advance(9.999)
    with pytest.raises(CircuitBreakerOpen):
        await breaker.call(_ok)
    clock.advance(0.001)
    assert await breaker.call(_ok) == "ok"


async def test_half_open_capacity_rejects_extra_concurrent_probes() -> None:
    """With half_open_max_calls=1, only one probe runs at a time. Extra concurrent
    callers get CircuitBreakerOpen until the in-flight probe resolves."""
    clock = FakeClock()
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=5.0,
        half_open_max_calls=1,
        clock=clock,
    )
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    clock.advance(5.0)

    ready = asyncio.Event()
    gate = asyncio.Event()

    async def slow_ok() -> str:
        ready.set()
        await gate.wait()
        return "ok"

    probe = asyncio.create_task(breaker.call(slow_ok))
    await ready.wait()  # probe is now holding the half-open slot

    with pytest.raises(CircuitBreakerOpen):
        await breaker.call(_ok)

    gate.set()
    await probe
    assert breaker.state == "closed"


async def test_half_open_capacity_2_allows_two_concurrent_probes() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(
        failure_threshold=1,
        recovery_timeout=5.0,
        half_open_max_calls=2,
        clock=clock,
    )
    with pytest.raises(RuntimeError):
        await breaker.call(_boom)
    clock.advance(5.0)

    started = 0
    gate = asyncio.Event()

    async def slow_ok() -> str:
        nonlocal started
        started += 1
        await gate.wait()
        return "ok"

    p1 = asyncio.create_task(breaker.call(slow_ok))
    p2 = asyncio.create_task(breaker.call(slow_ok))
    while started < 2:
        await asyncio.sleep(0)

    with pytest.raises(CircuitBreakerOpen):
        await breaker.call(_ok)

    gate.set()
    await asyncio.gather(p1, p2)


# ---- arg/return propagation ----


async def test_call_passes_args_and_kwargs_and_returns_result() -> None:
    breaker = CircuitBreaker()

    async def add(a: int, b: int, *, c: int = 0) -> int:
        return a + b + c

    assert await breaker.call(add, 2, 3, c=5) == 10


# ---- concurrency under load ----


async def test_concurrent_failure_storm_no_state_corruption() -> None:
    """100 concurrent failing calls — the lock must keep state coherent.
    Final state is 'open'; the breaker doesn't get stuck or skip transitions."""
    breaker = CircuitBreaker(failure_threshold=5)

    async def attempt() -> bool:
        try:
            await breaker.call(_boom)
            return True
        except (RuntimeError, CircuitBreakerOpen):
            return False

    await asyncio.gather(*[attempt() for _ in range(100)])
    assert breaker.state == "open"


# ---- validation ----


@pytest.mark.parametrize("threshold", [0, -1, -100])
def test_invalid_failure_threshold_raises(threshold: int) -> None:
    with pytest.raises(ValueError, match="failure_threshold"):
        CircuitBreaker(failure_threshold=threshold)


def test_invalid_half_open_max_calls_raises() -> None:
    with pytest.raises(ValueError, match="half_open_max_calls"):
        CircuitBreaker(half_open_max_calls=0)


def test_invalid_recovery_timeout_raises() -> None:
    with pytest.raises(ValueError, match="recovery_timeout"):
        CircuitBreaker(recovery_timeout=-1.0)
