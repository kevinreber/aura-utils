from asyncresil import (
    AsyncHTTPClient,
    Cache,
    CircuitBreaker,
    CircuitBreakerOpen,
    MemoryCache,
    RedisCache,
    TokenBucketRateLimiter,
    __version__,
)


def test_public_api_imports() -> None:
    assert AsyncHTTPClient is not None
    assert Cache is not None
    assert CircuitBreaker is not None
    assert CircuitBreakerOpen is not None
    assert MemoryCache is not None
    assert RedisCache is not None
    assert TokenBucketRateLimiter is not None
    assert __version__ == "0.0.1"
