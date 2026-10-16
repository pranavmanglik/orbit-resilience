# Orbit Resilience

`orbit-resilience` is an optional package of bounded async resilience utilities: deadlines,
token-bucket backpressure, retry policies, circuit breakers, bulkheads, and failure classification.
It depends on Orbit Core for the shared structured `OrbitError` and `OrbitProblem` contracts; its
policies remain optional and are not enabled by installing Core.
`resilient_call` validates all supplied policies and the operation before rate-limit capacity is
acquired, so a malformed local configuration cannot consume provider quota.

```bash
pip install orbit-resilience
```

The package is provider-neutral. Classify vendor-specific exceptions through `FailureClassifier`,
retry only operations with documented idempotency semantics, and keep attempts within the caller's
deadline. `RetryPolicy` uses exponential backoff with a default ±20% jitter; set `jitter_ratio=0`
when deterministic delays are required. Provider adapters should classify throttling responses as
transient only when retrying that operation is safe. Deadlines, retries, bulkheads, and token-bucket
limits are process-local. A circuit breaker requires a `CircuitStateStore` by default so separate
workers do not silently make independent outage decisions; `orbit-cache-redis` provides
`RedisCircuitStateStore`. Set `local_only=True` only when process-local behavior is intentional,
such as a test or single-process deployment. Use the same stable circuit name and policy values in
every worker, and set the probe lease longer than the operation deadline. Shared-state failures fail
closed. Redis replication or failover can lose recent state, so this adapter does not provide
consensus guarantees.

Retries may repeat remote side effects, and the synchronous-callable form cannot interrupt blocking
work on the event loop. The local LRU token bucket is best-effort backpressure, not a security
quota or abuse-prevention control; use a shared limiter for cross-worker or adversarial quotas.

`AsyncRateLimiter` waits asynchronously for local token-bucket capacity before a service operation
proceeds. Its per-instance buckets are process-local and do not coordinate across worker processes;
use a separately installed adapter when a shared or distributed quota is required. Core's own
nonblocking limiter for HTTP/Admin responses remains part of `orbit-core` and can reject requests
with `429` without making request handling wait for refill.

```python
from orbit_resilience import AsyncRateLimiter, resilient_call

partner_api_limit = AsyncRateLimiter(limit=60, period=60)
result = await resilient_call(
    call_partner_api,
    rate_limiter=partner_api_limit,
    rate_limit_key="partner-api",
    rate_limit_timeout=2.0,
)
```

When combined with retries, each provider attempt acquires its own token. A rate-limit wait timeout
is classified as local resource pressure, not as an upstream transient failure, so the default
retry policy does not repeat it.

Runtime rejections and shared-state failures derive from `ResilienceError`, which carries a stable
problem code, runtime category, and bounded context. Circuit-open, bulkhead, and rate-limit errors
also preserve their familiar `RuntimeError` or `TimeoutError` classification.

## Status

This package is alpha; its API is not stable and it is not yet a published release. It supports
Python 3.11 through 3.14.

## Documentation

The package-specific guides cover [architecture](docs/architecture/overview.md), [operations and security](docs/operations/README.md), and [development](docs/development/README.md), with [security guidance](docs/security/overview.md). The [documentation index](docs/README.md) links to the full package overview and project policies.

## Development

```bash
python -m pip install -e '.[dev]'
pytest
ruff check .
mypy
```

Licensed under Apache-2.0.

