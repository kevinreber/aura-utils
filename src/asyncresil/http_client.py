"""Async HTTP client with retries, exponential backoff, and jitter."""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Callable
from types import TracebackType
from typing import Any

import httpx

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS = frozenset({500, 502, 503, 504})


def _default_jitter(delay: float) -> float:
    return delay + random.uniform(-delay * 0.1, delay * 0.1)


class AsyncHTTPClient:
    """Async HTTP client with retries on 5xx responses and network errors.

    Non-2xx responses are returned to the caller as-is — this client never
    calls ``raise_for_status`` on the user's behalf. Wrap with
    ``response.raise_for_status()`` at the call site if that semantic is wanted.
    """

    def __init__(
        self,
        *,
        timeout: float = 10.0,
        max_retries: int = 3,
        backoff_factor: float = 0.5,
        _jitter: Callable[[float], float] = _default_jitter,
    ) -> None:
        self._timeout = timeout
        self._max_retries = max_retries
        self._backoff_factor = backoff_factor
        self._jitter = _jitter
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> AsyncHTTPClient:
        self._client = httpx.AsyncClient(timeout=self._timeout)
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        await self.close()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: Any) -> httpx.Response:
        return await self._request("POST", url, **kwargs)

    async def _request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        if self._client is None:
            raise RuntimeError(
                "AsyncHTTPClient must be used as an async context manager: "
                "`async with AsyncHTTPClient() as client: ...`"
            )

        total_attempts = self._max_retries + 1
        for attempt in range(total_attempts):
            try:
                response = await self._client.request(method, url, **kwargs)
            except httpx.RequestError as e:
                if attempt == self._max_retries:
                    logger.error(
                        "Request error on %s %s after %d attempts: %s",
                        method, url, total_attempts, e,
                    )
                    raise
                logger.warning(
                    "Request error on %s %s (attempt %d/%d), retrying: %s",
                    method, url, attempt + 1, total_attempts, e,
                )
                await self._sleep(attempt)
                continue

            if (
                response.status_code in _RETRYABLE_STATUS
                and attempt < self._max_retries
            ):
                logger.warning(
                    "Retryable status %d on %s %s (attempt %d/%d), retrying",
                    response.status_code, method, url, attempt + 1, total_attempts,
                )
                await self._sleep(attempt)
                continue

            return response

        raise RuntimeError("unreachable: retry loop exited without returning")

    async def _sleep(self, attempt: int) -> None:
        delay = self._backoff_factor * (2 ** attempt)
        delay = self._jitter(delay)
        await asyncio.sleep(max(delay, 0.0))
