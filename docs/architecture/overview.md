# Orbit Resilience: architecture and contracts

## Scope

`orbit-resilience` is an optional Python library of async controls: deadlines, exponential retries,
circuit breakers, concurrency bulkheads, failure classification, and token-bucket backpressure.
Deadlines, retries, bulkheads, and token-bucket state are process-local. A circuit breaker requires
the `CircuitStateStore` contract by default; `local_only=True` is an explicit opt-in for tests and
single-process applications. `orbit-cache-redis` provides an optional Redis adapter with leased
half-open probes and epoch checks for stale completions. Redis replication or failover can lose
recent breaker updates; this is coordination, not consensus. The package depends on Core's
structured error contract, but its policies remain optional and it adds no provider SDK. Core
retains its own request, lifecycle, and cleanup timeout controls.

The library helps applications compose client-side policies around an operation. It is not a
service mesh, proxy, security boundary, or guarantee that a remote side effect did not complete
when a timeout or connection error occurs.

## Composing `resilient_call`

Options are validated before rate-limit admission or operation execution. For each attempt, the
call order is:

```text
Deadline scope (overall budget)
  └─ Retry loop (classified transient failures only)
       └─ RateLimiter.acquire (once per attempt, if configured)
            └─ Bulkhead admission (if configured)
                 └─ CircuitBreaker.call (if configured with bulkhead)
                      └─ operation
```

When there is a circuit but no bulkhead, the circuit wraps the operation directly. Bulkhead
rejection and rate-limit wait timeout are local resource pressure and are not retried by the
default classifier. Circuit-open rejections are not operation failures. The deadline includes
rate-limit waits, bulkhead waits, retry backoff, and asynchronous operation time.

Synchronous callables are supported for fast local work. They run on the event-loop thread and
cannot be interrupted by an asyncio deadline while they block. Use an async client or explicitly
move blocking work to a bounded executor.

## Failure classification and retries

The default classifier retries only common transient network/time-out exceptions. Unknown and
permanent exceptions are not retried. Provider adapters can supply a `FailureClassifier` for
vendor-specific exception types. A classifier should be deterministic, fast, and side-effect-free.

`RetryPolicy.max_attempts` counts the initial call. Exponential backoff is capped by `max_delay`
after proportional jitter is applied. The default `jitter_ratio=0.2` spreads a delay by up to 20%
in either direction; the ratio is configurable from zero to one. Jitter uses the process random
generator and is for load spreading, not cryptographic randomness. Cancellation propagates
immediately and is never retried. Retrying can duplicate a remote side effect if the server
completed it before the client observed a failure. Use retries only for idempotent operations or
operations protected by an idempotency key; this package does not create such keys or coordinate
transactions.

## Circuit breaker

`CircuitBreaker` tracks consecutive operation exceptions. Without a store, state is local to one
instance. With a store, the adapter atomically coordinates admission, completion, and half-open
probe leases across workers. Epochs prevent an older in-flight completion from overwriting a newer
open circuit. Cancellation is not recorded as a dependency failure; a cancelled shared probe is
released by owner token, with the bounded lease as crash recovery. Direct `CircuitBreaker.call()`
counts any exception raised by its operation; validate application input before entering it.

Shared-store errors fail closed and do not fall back to local state. If persistence fails after the
operation has been invoked, `CircuitStateStoreError.operation_attempted` is true: the caller must
assume the operation may have taken effect and avoid blind retries. Shared snapshot properties are
last-observed values; call `refresh_state()` for diagnostics, never for admission decisions.
Keep the probe lease longer than the maximum operation deadline plus expected state-write latency;
if a lease expires early, another worker can start a second recovery probe while the first call is
still running.

Operational errors derive from `ResilienceError`, a Core `OrbitError` containing a stable problem
code, runtime category, and bounded context. Circuit-open, bulkhead-full, and rate-limit timeout
errors keep their standard Python runtime/timeout base classes for existing callers.

## Bulkhead and rate limiter

`Bulkhead` bounds concurrent operations in one process. Its optional timeout limits admission wait,
not the operation duration. It releases capacity after success, exception, or cancellation.

`AsyncRateLimiter` is an in-memory token bucket with bounded LRU key storage. It allows an initial
burst up to `limit`, refills at `limit / period`, and does not reserve tokens for sleeping waiters
or guarantee FIFO fairness. LRU eviction drops a key's quota history; high-cardinality key churn
can therefore reset effective per-key limits. This is best-effort local backpressure, not an abuse
prevention or billing quota. Use a separately managed shared limiter when quota correctness across
processes or adversarial keys matters.

## Source layout and status

- `src/orbit_resilience/resilience.py`: deadline, jittered retry, classifier, circuit breaker
  contract, and bulkhead.
- `src/orbit_resilience/rate_limit.py`: local token bucket and rate-limit protocol.
- `src/orbit_resilience/_limits.py`: common finite-number and capacity checks.
- `src/orbit_resilience/errors.py`: structured resilience error base.

The package declares Python `>=3.11,<3.15` and depends on `orbit-core` for its shared error model.
It is alpha; the API
may change before a stable release.
