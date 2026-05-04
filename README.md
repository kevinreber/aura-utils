# aura-utils

Async resilience utilities for Python services. A small, focused toolkit
extracted from production code so the same primitives can be shared across
projects instead of re-implemented per repo.

## Modules

| Module | What it does | Why it exists |
|---|---|---|
| `http_client` | `httpx`-based async client with retries, exponential backoff, and timeouts | Transient network failures shouldn't surface to callers — retry the obvious cases. |
| `circuit_breaker` | State machine (`closed` → `open` → `half_open`) that fails fast when a downstream is broken | Stops cascading failures and retry storms when a dependency is down. |
| `rate_limiter` | Token-bucket limiter for async code | Stay under third-party API limits, smooth bursts. |
| `cache` | `Cache` protocol with `MemoryCache` (default) and `RedisCache` (optional dep) | Skip repeat work — cut latency, cost, and downstream load. |

The four compose into a typical outbound call chain:

```
caller → cache → rate_limiter → circuit_breaker → http_client → external service
```

Each layer protects the layers below from unnecessary load.

## Install

```bash
pip install aura-utils                # core (in-memory cache only)
pip install 'aura-utils[redis]'       # adds Redis-backed cache
```

Requires Python 3.11+.

## Intended consumers

Internal services that need a shared resilience layer:

- **Aura** — MCP server (the original source of these utilities)
- **Eval Framework** — uses the cache + rate limiter for batched LLM evaluation
- **Agentic Engine** — uses the circuit breaker around tool calls
- **LLM Gateway** — uses the http client + circuit breaker for upstream provider calls

## Status

`0.0.1` — early alpha. API is not yet stable.

## License

MIT — see [LICENSE](LICENSE).
