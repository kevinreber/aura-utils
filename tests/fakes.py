"""Shared test fakes for time-driven primitives."""

from __future__ import annotations


class FakeClock:
    """Controllable monotonic clock for tests."""

    def __init__(self, t: float = 0.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, delta: float) -> None:
        self.t += delta
