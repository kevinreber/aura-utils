"""Tests for AsyncHTTPClient."""

from __future__ import annotations

import httpx
import pytest
import respx

from aura_utils import AsyncHTTPClient

URL = "https://api.example.com/"


def _no_delay(_: float) -> float:
    return 0.0


@pytest.fixture
def client() -> AsyncHTTPClient:
    return AsyncHTTPClient(
        timeout=1.0,
        max_retries=3,
        backoff_factor=0.5,
        _jitter=_no_delay,
    )


# ---- success path ----


@respx.mock
async def test_get_returns_response(client: AsyncHTTPClient) -> None:
    respx.get(URL).mock(return_value=httpx.Response(200, json={"ok": True}))
    async with client:
        response = await client.get(URL)
    assert response.status_code == 200
    assert response.json() == {"ok": True}


@respx.mock
async def test_post_returns_response(client: AsyncHTTPClient) -> None:
    respx.post(URL).mock(return_value=httpx.Response(201, json={"id": 1}))
    async with client:
        response = await client.post(URL, json={"a": 1})
    assert response.status_code == 201


# ---- retry behavior ----


@respx.mock
async def test_5xx_retries_then_succeeds(client: AsyncHTTPClient) -> None:
    route = respx.get(URL).mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(503),
            httpx.Response(200),
        ]
    )
    async with client:
        response = await client.get(URL)
    assert response.status_code == 200
    assert route.call_count == 3


@respx.mock
async def test_5xx_returned_when_retries_exhausted(client: AsyncHTTPClient) -> None:
    """When retries are exhausted, the last 5xx response is returned, not raised."""
    route = respx.get(URL).mock(return_value=httpx.Response(500))
    async with client:
        response = await client.get(URL)
    assert response.status_code == 500
    assert route.call_count == 4  # 1 initial + 3 retries


@respx.mock
async def test_4xx_does_not_retry(client: AsyncHTTPClient) -> None:
    route = respx.get(URL).mock(return_value=httpx.Response(404))
    async with client:
        response = await client.get(URL)
    assert response.status_code == 404
    assert route.call_count == 1


@respx.mock
async def test_request_error_retried_then_succeeds(client: AsyncHTTPClient) -> None:
    route = respx.get(URL).mock(
        side_effect=[
            httpx.ConnectError("boom"),
            httpx.Response(200),
        ]
    )
    async with client:
        response = await client.get(URL)
    assert response.status_code == 200
    assert route.call_count == 2


@respx.mock
async def test_request_error_raised_when_retries_exhausted(
    client: AsyncHTTPClient,
) -> None:
    route = respx.get(URL).mock(side_effect=httpx.ConnectError("boom"))
    async with client:
        with pytest.raises(httpx.ConnectError):
            await client.get(URL)
    assert route.call_count == 4


# ---- jitter injection ----


@respx.mock
async def test_jitter_called_with_exponential_delays() -> None:
    seen: list[float] = []

    def recording_jitter(delay: float) -> float:
        seen.append(delay)
        return 0.0

    respx.get(URL).mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(500),
            httpx.Response(200),
        ]
    )
    client = AsyncHTTPClient(
        max_retries=3,
        backoff_factor=0.5,
        _jitter=recording_jitter,
    )
    async with client:
        await client.get(URL)

    # Two retries fired → 0.5 * 2**0, 0.5 * 2**1
    assert seen == [0.5, 1.0]


@respx.mock
async def test_jitter_not_called_on_success() -> None:
    seen: list[float] = []

    def recording_jitter(delay: float) -> float:
        seen.append(delay)
        return 0.0

    respx.get(URL).mock(return_value=httpx.Response(200))
    client = AsyncHTTPClient(_jitter=recording_jitter)
    async with client:
        await client.get(URL)
    assert seen == []


# ---- lifecycle ----


async def test_used_outside_context_manager_raises() -> None:
    client = AsyncHTTPClient()
    with pytest.raises(RuntimeError, match="async context manager"):
        await client.get(URL)


async def test_close_is_idempotent() -> None:
    client = AsyncHTTPClient()
    async with client:
        pass
    await client.close()  # second close is a no-op


async def test_timeout_passed_to_underlying_client() -> None:
    client = AsyncHTTPClient(timeout=2.5)
    async with client:
        assert client._client is not None
        # httpx.Timeout exposes per-phase fields; all four default to the same value
        assert client._client.timeout.read == 2.5
