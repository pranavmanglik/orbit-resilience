# Orbit Resilience: operations

This package has no environment-variable configuration, provider clients, or background lifecycle.
Construct policies alongside the application component that uses them. All in-memory state is
local to one Python process and is lost on restart.

## Choosing values

- Set the overall `Deadline` from the caller's remaining request or job budget, leaving time for
  response handling and cleanup.
- Count `RetryPolicy.max_attempts` including the initial attempt. Keep backoff under the remaining
  deadline and avoid retrying operations that cannot safely repeat. Its default `jitter_ratio=0.2`
  spreads each delay by up to 20%; set the ratio to zero only when deterministic timing is needed.
- Size a `Bulkhead` for the dependency's concurrency and connection-pool limits. Its timeout is
  only an admission-wait bound.
- Choose `CircuitBreaker.failure_threshold` and `recovery_timeout` based on dependency behavior.
  Separate breaker instances isolate unrelated dependencies. A shared `CircuitStateStore` is
  required by default; use `local_only=True` only when one-process state is intentional.
- Install a shared adapter such as `orbit-cache-redis` and use the same stable circuit name and
  policy values in every worker. Set `probe_lease_timeout` above the operation's maximum deadline
  plus completion-write latency. Shared-store errors fail closed; persistence failure after
  invocation sets `operation_attempted=True`, so inspect the operation's idempotency contract
  before retrying.
- `AsyncRateLimiter` limits one in-process instance. Its LRU key bound trades memory for discarded
  key history; it cannot provide tenant quotas across workers or withstand key-cardinality churn
  as a strict quota.

## Composition behavior

Use `resilient_call` to validate policy types and arguments once before work begins. Every retry
reacquires rate capacity. The deadline includes that wait and retry backoff. Bulkhead capacity is
released in `finally`, including cancellation. A bulkhead rejection does not count against the
circuit or trigger a retry under the default classifier.

Apply one policy composition per logical operation. Avoid wrapping an already retried client in a
second retry policy; nested retries multiply attempts and delay. Track attempts, rate-wait time,
bulkhead rejections, breaker state, and deadline exhaustion with application metrics.

## Failure and idempotency

Choose the retry classifier from the client's actual exception contract. Provider adapters may
classify a throttling response as transient when their SDK identifies it reliably and the operation
is safe to repeat; do not retry every HTTP 429 or provider exception indiscriminately. The default
does not retry unknown exceptions. Do not translate caller cancellation into a transient provider
error. A timeout means the caller stopped waiting; it does not prove the remote side effect was
rolled back. Use idempotency keys or a deduplication strategy before retrying writes, payments,
message publishes, or other non-repeatable operations.

## Pre-release operational checks

Exercise concurrent load, cancellation while waiting and while running, expired deadlines,
backoff under an overall budget, half-open recovery, key churn, and multiple worker processes.
Confirm that metrics and logs do not expose secrets or high-cardinality raw keys. These local
utilities do not validate distributed quota behavior or remote-service failover. The Redis adapter
coordinates breaker state atomically while Redis is available, but replication or failover may lose
recent writes. Use a consensus-backed store if losing a recent breaker update would violate a
safety requirement.
