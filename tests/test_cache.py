"""Tests for MemoryCache and RedisCache."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import AsyncMock

import fakeredis
import fakeredis.aioredis
import pytest

from asyncresil import Cache, MemoryCache, RedisCache
from fakes import FakeClock

if TYPE_CHECKING:
    from redis.asyncio import Redis as _AsyncRedis


# ---- Protocol conformance (structural typing — no runtime assert needed,
#      mypy validates these at type-check time) ----


def test_memory_cache_satisfies_protocol() -> None:
    cache: Cache = MemoryCache()
    assert cache is not None


def test_redis_cache_satisfies_protocol() -> None:
    cache: Cache = RedisCache(redis_client=fakeredis.aioredis.FakeRedis())
    assert cache is not None


# ============================================================
# MemoryCache
# ============================================================


# ---- basic get/set/delete ----


async def test_memory_get_returns_none_for_missing() -> None:
    cache = MemoryCache()
    assert await cache.get("missing") is None


async def test_memory_set_then_get_returns_value() -> None:
    cache = MemoryCache()
    await cache.set("k", "v")
    assert await cache.get("k") == "v"


async def test_memory_stores_arbitrary_python_objects() -> None:
    """MemoryCache stores by reference — no serialization."""
    cache = MemoryCache()
    value = {"nested": [1, 2, {"deep": True}]}
    await cache.set("k", value)
    got = await cache.get("k")
    assert got is value  # identity preserved


async def test_memory_overwrite_replaces_value() -> None:
    cache = MemoryCache()
    await cache.set("k", "first")
    await cache.set("k", "second")
    assert await cache.get("k") == "second"


async def test_memory_delete_removes_value() -> None:
    cache = MemoryCache()
    await cache.set("k", "v")
    await cache.delete("k")
    assert await cache.get("k") is None


async def test_memory_delete_missing_key_is_silent() -> None:
    cache = MemoryCache()
    await cache.delete("missing")  # no exception


# ---- TTL ----


async def test_memory_no_ttl_means_no_expiry() -> None:
    clock = FakeClock()
    cache = MemoryCache(clock=clock)
    await cache.set("k", "v")
    clock.advance(1_000_000)
    assert await cache.get("k") == "v"


async def test_memory_ttl_expires_value() -> None:
    clock = FakeClock()
    cache = MemoryCache(clock=clock)
    await cache.set("k", "v", ttl=10.0)
    clock.advance(9.9)
    assert await cache.get("k") == "v"
    clock.advance(0.1)
    assert await cache.get("k") is None


async def test_memory_expired_entry_is_evicted_on_get() -> None:
    clock = FakeClock()
    cache = MemoryCache(clock=clock)
    await cache.set("k", "v", ttl=1.0)
    clock.advance(2.0)
    await cache.get("k")  # evicts
    # internal store should be empty
    assert cache._store == {}


async def test_memory_set_resets_ttl() -> None:
    clock = FakeClock()
    cache = MemoryCache(clock=clock)
    await cache.set("k", "v", ttl=10.0)
    clock.advance(9.0)
    await cache.set("k", "v2", ttl=10.0)  # reset
    clock.advance(5.0)
    assert await cache.get("k") == "v2"


# ---- validation ----


@pytest.mark.parametrize("bad_ttl", [0, -1.0, -0.001])
async def test_memory_invalid_ttl_raises(bad_ttl: float) -> None:
    cache = MemoryCache()
    with pytest.raises(ValueError, match="ttl"):
        await cache.set("k", "v", ttl=bad_ttl)


# ============================================================
# RedisCache (with fakeredis)
# ============================================================


@pytest.fixture
async def redis_client() -> "_AsyncRedis":
    # Per-test FakeServer for isolation — otherwise FakeRedis() instances
    # share a process-wide default server and tests can leak state.
    return fakeredis.aioredis.FakeRedis(server=fakeredis.FakeServer())


# ---- basic get/set/delete ----


async def test_redis_get_returns_none_for_missing(
    redis_client: "_AsyncRedis",
) -> None:
    cache = RedisCache(redis_client=redis_client)
    assert await cache.get("missing") is None


async def test_redis_set_then_get_returns_value(
    redis_client: "_AsyncRedis",
) -> None:
    cache = RedisCache(redis_client=redis_client)
    await cache.set("k", "v")
    assert await cache.get("k") == "v"


async def test_redis_overwrite_replaces_value(
    redis_client: "_AsyncRedis",
) -> None:
    cache = RedisCache(redis_client=redis_client)
    await cache.set("k", "first")
    await cache.set("k", "second")
    assert await cache.get("k") == "second"


async def test_redis_delete_removes_value(
    redis_client: "_AsyncRedis",
) -> None:
    cache = RedisCache(redis_client=redis_client)
    await cache.set("k", "v")
    await cache.delete("k")
    assert await cache.get("k") is None


async def test_redis_delete_missing_key_is_silent(
    redis_client: "_AsyncRedis",
) -> None:
    cache = RedisCache(redis_client=redis_client)
    await cache.delete("missing")  # no exception


# ---- JSON serialization roundtrip ----


@pytest.mark.parametrize(
    "value",
    [
        "string",
        42,
        3.14,
        True,
        None,
        [1, 2, 3],
        {"a": 1, "b": [2, 3], "c": {"nested": True}},
    ],
)
async def test_redis_json_roundtrip(
    redis_client: "_AsyncRedis", value: object
) -> None:
    cache = RedisCache(redis_client=redis_client)
    await cache.set("k", value)
    assert await cache.get("k") == value


# ---- TTL: verify correct PX is sent to redis ----


async def test_redis_set_without_ttl_uses_set_without_expiry() -> None:
    mock = AsyncMock()
    cache = RedisCache(redis_client=mock)
    await cache.set("k", "v")
    mock.set.assert_awaited_once_with("k", '"v"')


async def test_redis_set_with_ttl_uses_px_milliseconds() -> None:
    mock = AsyncMock()
    cache = RedisCache(redis_client=mock)
    await cache.set("k", "v", ttl=0.5)
    mock.set.assert_awaited_once_with("k", '"v"', px=500)


async def test_redis_set_with_subsecond_ttl_clamps_to_1ms() -> None:
    mock = AsyncMock()
    cache = RedisCache(redis_client=mock)
    await cache.set("k", "v", ttl=0.0001)  # 0.1ms; clamps to 1ms
    mock.set.assert_awaited_once_with("k", '"v"', px=1)


# ---- TTL: integration with fakeredis ----


async def test_redis_ttl_actually_expires(redis_client: "_AsyncRedis") -> None:
    """End-to-end TTL through fakeredis. Uses a short real-time wait."""
    import asyncio

    cache = RedisCache(redis_client=redis_client)
    await cache.set("k", "v", ttl=0.05)  # 50ms
    assert await cache.get("k") == "v"
    await asyncio.sleep(0.1)
    assert await cache.get("k") is None


# ---- validation ----


@pytest.mark.parametrize("bad_ttl", [0, -1.0, -0.001])
async def test_redis_invalid_ttl_raises(
    redis_client: "_AsyncRedis", bad_ttl: float
) -> None:
    cache = RedisCache(redis_client=redis_client)
    with pytest.raises(ValueError, match="ttl"):
        await cache.set("k", "v", ttl=bad_ttl)


# ---- raw bytes handling (some Redis clients return bytes) ----


async def test_redis_decodes_bytes_response() -> None:
    """If the underlying client returns bytes (no decode_responses), we still parse JSON."""
    mock = AsyncMock()
    mock.get.return_value = b'"hello"'
    cache = RedisCache(redis_client=mock)
    assert await cache.get("k") == "hello"


# ---- malformed content handling ----


async def test_redis_malformed_json_returns_none_as_miss() -> None:
    """A key whose value isn't valid JSON (schema migration, foreign writer)
    should behave as a cache miss, not crash."""
    mock = AsyncMock()
    mock.get.return_value = b"not valid json{{{"
    cache = RedisCache(redis_client=mock)
    assert await cache.get("k") is None


async def test_redis_malformed_json_logs_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    import logging

    mock = AsyncMock()
    mock.get.return_value = b"not valid json"
    cache = RedisCache(redis_client=mock)
    with caplog.at_level(logging.WARNING, logger="asyncresil.cache"):
        await cache.get("k")
    assert any("non-JSON content" in record.message for record in caplog.records)


async def test_redis_unserializable_value_raises_with_helpful_message(
    redis_client: "_AsyncRedis",
) -> None:
    from datetime import datetime

    cache = RedisCache(redis_client=redis_client)
    with pytest.raises(TypeError, match="JSON-serializable"):
        await cache.set("k", datetime.now())
