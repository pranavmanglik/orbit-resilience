# Orbit Resilience: security

These utilities change how application operations wait and repeat. They do not provide
authentication, authorization, distributed abuse prevention, or transactional rollback.

## Rate limits are not access controls

`AsyncRateLimiter` is process-local and stores a bounded LRU set of keys. When key capacity is
exceeded, old bucket state is evicted. An attacker able to create high-cardinality keys can cause
quota history to be discarded; multiple workers also maintain independent buckets. Do not use it
as the sole control for login throttling, billing quotas, abuse prevention, or other security
requirements. Use a shared limiter with an explicitly reviewed failure mode and enforce
authorization independently.

Rate-limit keys are retained in memory. Do not include passwords, access tokens, or unnecessary
personal data. Keep keys stable and low-cardinality, and avoid logging raw key values.

## Retries and side effects

Retries can duplicate an operation whose remote completion was not observed by the caller.
Protect non-idempotent writes with server-supported idempotency keys or application deduplication.
Do not retry authentication failures, validation failures, or unknown exceptions without a
provider-specific reason. Cancellation is propagated to the caller, but cancellation does not
roll back a request already accepted by a remote service.

## Deadlines and resource bounds

Deadlines constrain asynchronous waiting and execution. A blocking synchronous function can stall
the event loop and cannot be interrupted by `asyncio.timeout`; use an async client or a bounded
executor. Bound bulkhead waiters and concurrency at the application level, and use the bulkhead
with client pool limits so queued work does not exhaust memory.

## Diagnostics

These classes do not log operation arguments or caught exceptions. Application instrumentation
should record safe operation names and classifications without recording credentials, request
bodies, personal data, or unbounded rate-limit keys.

Follow the repository [security policy](../../SECURITY.md) for private vulnerability reporting.
This guidance is not a security review of an application that composes these utilities.
