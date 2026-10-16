# Orbit Resilience: development

The package uses Core's structured error contract and implements optional async policies. Preserve
bounded state, monotonic timing, and cancellation behavior. Changes to composition order are public
behavior and should be reflected in tests and docs.

## Local setup and checks

From the repository root:

```bash
python -m pip install -e '.[dev]' build
pytest
ruff check src tests
ruff format --check src tests
mypy
python -m build
```

Most tests use local coroutines and no external services. Tests for a custom `CircuitStateStore`
should use a deterministic fake for contract and failure behavior. Timing tests should prefer events
and generous bounds over exact scheduler timing. Retry-jitter tests replace the random draw and
sleep function to check bounds without introducing flaky wall-clock assertions. The Redis adapter
has separate integration tests that run only when a disposable Redis test URL is configured; do not
infer failover guarantees from a single Redis instance.

## Change guidance

- Validate policy objects and operation callbacks before consuming rate, breaker, or bulkhead state.
- Do not catch `asyncio.CancelledError` as an ordinary failure or retry it.
- Keep timeout, backoff, and token refill arithmetic finite and bounded.
- Test races between in-flight operations and circuit transitions; stale completions must not
  overwrite a newer breaker epoch.
- Keep backpressure, circuit-open, and local queue rejection distinct from upstream transient
  failures so policy composition does not amplify local overload.
- Document whether behavior is process-local, lossy under key eviction, or coordinated across
  workers.

Follow the contribution guide, governance, code of conduct, and security policy. Keep tests free of
real credentials and report exact local validation separately from hosted CI evidence.
