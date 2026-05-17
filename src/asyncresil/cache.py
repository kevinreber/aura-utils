"""TTL cache with pluggable backends — in-memory and Redis."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from redis.asyncio import Redis as _AsyncRedis

logger = logging.getLogger(__name__)


class Cache(Protocol):
    """Async key-value cache with optional TTL.

    Implementations must be safe to call from a single asyncio event loop.
    Values stored via ``set`` must be retrievable via ``get`` until they
    expire (TTL) or are explicitly deleted. Missing or expired keys return
    ``None``.

    .. note::

       ``None`` is reserved as the "missing key" sentinel. Callers should
       not store ``None`` as a value — a subsequent ``get`` cannot be
       distinguished from a cache miss. To cache "no result" semantics
       (negative caching), wrap the value (e.g., ``{"value": None}``) at
       the call site.
    """

    async def get(self, key: str) -> Any | None: ...

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None: ...

    async def delete(self, key: str) -> None: ...


def _validate_ttl(ttl: float | None) -> None:
    if ttl is not None and ttl <= 0:
        raise ValueError("ttl must be > 0 or None")


class MemoryCache:
    """In-process TTL cache. No external dependencies.

    Stores Python objects by reference — no serialization, so values are
    returned exactly as they were stored (same identity for mutable values).
    Expiry is checked lazily on ``get``; expired entries are evicted then.

    Intended for use within a single asyncio event loop. Not thread-safe:
    methods are lock-free and rely on asyncio's cooperative scheduling for
    atomicity, so concurrent access from multiple OS threads can corrupt
    the internal store.
    """

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._clock = clock
        self._store: dict[str, tuple[Any, float | None]] = {}

    async def get(self, key: str) -> Any | None:
        entry = self._store.get(key)
        if entry is None:
            return None
        value, expires_at = entry
        if expires_at is not None and self._clock() >= expires_at:
            del self._store[key]
            return None
        return value

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        _validate_ttl(ttl)
        expires_at = self._clock() + ttl if ttl is not None else None
        self._store[key] = (value, expires_at)

    async def delete(self, key: str) -> None:
        self._store.pop(key, None)


class RedisCache:
    """Redis-backed cache. Requires the ``redis`` extra.

    Install with ``pip install 'asyncresil[redis]'``. The caller owns the
    ``redis.asyncio.Redis`` client (auth, pool config, sentinel, cluster, etc.);
    this class only knows how to ``GET`` / ``SET`` / ``DEL`` keys on it.

    Values are JSON-encoded on the wire, so anything stored must be
    JSON-serializable. For cross-backend portability with ``MemoryCache``,
    keep values in the JSON-safe subset (no datetimes, no custom classes,
    no sets) unless you're sure you'll only ever use ``MemoryCache``.

    ``get`` treats malformed JSON content (e.g., from a schema migration or
    a key collision with another writer) as a cache miss — returns ``None``
    and logs a warning. Set errors raise.
    """

    def __init__(self, *, redis_client: _AsyncRedis) -> None:
        self._redis = redis_client

    async def get(self, key: str) -> Any | None:
        raw = await self._redis.get(key)
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("cache miss: non-JSON content at key %r", key)
            return None

    async def set(self, key: str, value: Any, ttl: float | None = None) -> None:
        _validate_ttl(ttl)
        try:
            serialized = json.dumps(value)
        except TypeError as e:
            raise TypeError(
                f"RedisCache values must be JSON-serializable; "
                f"got {type(value).__name__}: {e}"
            ) from e
        if ttl is None:
            await self._redis.set(key, serialized)
        else:
            # PX takes milliseconds; clamp to 1ms minimum for sub-millisecond TTLs.
            px = max(1, int(ttl * 1000))
            await self._redis.set(key, serialized, px=px)

    async def delete(self, key: str) -> None:
        await self._redis.delete(key)
