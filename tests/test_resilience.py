# Copyright 2026-present Orbit Contributors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Verify retry, circuit-breaker, bulkhead, deadline, and failure-classification contracts."""

import asyncio

import pytest
from orbit.errors import ErrorCategory, OrbitError

from orbit_resilience import (
    AsyncRateLimiter,
    Bulkhead,
    BulkheadFullError,
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    CircuitStateStoreError,
    Deadline,
    RateLimitExceededError,
    ResilienceError,
    RetryPolicy,
    resilient_call,
    retry,
)
from orbit_resilience.resilience import DefaultFailureClassifier, FailureKind


def test_operational_errors_use_core_problem_contract_and_keep_builtin_categories() -> None:
    errors: list[Exception] = [
        CircuitOpenError(),
        CircuitStateStoreError("state write failed", operation_attempted=True),
        BulkheadFullError(),
        RateLimitExceededError(0.25),
    ]
    assert all(isinstance(error, OrbitError) for error in errors)
    assert all(isinstance(error, ResilienceError) for error in errors)
    assert all(error.problem.category is ErrorCategory.RUNTIME for error in errors)
    assert isinstance(errors[0], RuntimeError)
    assert isinstance(errors[2], TimeoutError)
    assert isinstance(errors[3], TimeoutError)
    assert errors[1].problem.context["operation_attempted"] is True
    assert errors[3].problem.context["retry_after_seconds"] == 0.25


@pytest.mark.asyncio
async def test_retry_is_bounded_and_cancellation_safe():
    attempts = 0

    async def operation():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ValueError("temporary")
        return "ok"

    assert await retry(operation, RetryPolicy(max_attempts=3, initial_delay=0)) == "ok"
    assert attempts == 3


@pytest.mark.asyncio
async def test_retry_backoff_saturates_after_exponentiation_overflow():
    attempts = 0

    async def operation():
        nonlocal attempts
        attempts += 1
        if attempts < 4:
            raise ConnectionError("temporary")
        return "ok"

    policy = RetryPolicy(max_attempts=4, initial_delay=0.001, multiplier=1e308, max_delay=0.01)
    assert await retry(operation, policy) == "ok"
    assert attempts == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(("factor", "expected"), [(0.5, [0.5, 1.0]), (1.5, [1.5, 2.0])])
async def test_retry_applies_bounded_jitter_before_the_maximum_cap(
    monkeypatch: pytest.MonkeyPatch, factor: float, expected: list[float]
) -> None:
    delays: list[float] = []
    attempts = 0

    async def fake_sleep(delay: float) -> None:
        delays.append(delay)

    async def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionError("temporary")
        return "ok"

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    monkeypatch.setattr("orbit_resilience.resilience.random.uniform", lambda low, high: factor)

    result = await retry(
        operation,
        RetryPolicy(
            max_attempts=3,
            initial_delay=1,
            multiplier=2,
            max_delay=2,
            jitter_ratio=0.5,
        ),
    )

    assert result == "ok"
    assert delays == expected


@pytest.mark.parametrize(
    "factory",
    [
        lambda: Deadline(10**400),
        lambda: Deadline(1, started_at=10**400),
        lambda: RetryPolicy(initial_delay=10**400),
        lambda: RetryPolicy(multiplier=10**400),
        lambda: RetryPolicy(max_delay=10**400),
        lambda: CircuitBreaker(recovery_timeout=10**400),
        lambda: Bulkhead(1, timeout=10**400),
        lambda: CircuitBreaker(failure_threshold=1_000_001),
        lambda: Bulkhead(1_000_001),
    ],
)
def test_reliability_boundaries_reject_unrepresentably_large_integers(factory) -> None:
    with pytest.raises(ValueError):
        factory()


def test_circuit_breaker_requires_shared_state_or_explicit_local_mode() -> None:
    """Prevent applications from silently creating per-process breaker decisions."""
    with pytest.raises(ValueError, match="requires a shared state_store"):
        CircuitBreaker()

    local = CircuitBreaker(local_only=True)
    assert local.state is CircuitState.CLOSED
    with pytest.raises(TypeError, match="local_only"):
        CircuitBreaker(local_only=1)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_circuit_breaker_opens_and_recovers():
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=0.01, local_only=True)
    with pytest.raises(AttributeError):
        breaker.failure_threshold = 1  # type: ignore[misc]
    with pytest.raises(AttributeError):
        breaker.recovery_timeout = 1  # type: ignore[misc]

    async def failing():
        raise RuntimeError("down")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)
    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        await breaker.call(lambda: "blocked")
    await asyncio.sleep(0.02)
    assert await breaker.call(lambda: "recovered") == "recovered"
    assert breaker.state is CircuitState.CLOSED


@pytest.mark.asyncio
async def test_circuit_breaker_rejects_non_callable_without_counting_failure() -> None:
    """A caller configuration error must not open a dependency circuit."""
    breaker = CircuitBreaker(failure_threshold=1, local_only=True)
    with pytest.raises(TypeError, match="callable"):
        await breaker.call(None)  # type: ignore[arg-type]
    assert breaker.state is CircuitState.CLOSED
    assert breaker.failures == 0
    assert await breaker.call(lambda: "healthy") == "healthy"


@pytest.mark.asyncio
async def test_circuit_breaker_allows_only_one_half_open_probe():
    breaker = CircuitBreaker(failure_threshold=1, recovery_timeout=0.001, local_only=True)

    async def failing():
        raise RuntimeError("down")

    with pytest.raises(RuntimeError):
        await breaker.call(failing)
    await asyncio.sleep(0.01)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def probe():
        entered.set()
        await release.wait()
        return "ok"

    first = asyncio.create_task(breaker.call(probe))
    await entered.wait()
    with pytest.raises(CircuitOpenError, match="probing"):
        await breaker.call(lambda: "second")
    release.set()
    assert await first == "ok"


@pytest.mark.asyncio
async def test_circuit_breaker_ignores_stale_success_after_newer_failures_open_it():
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout=1, local_only=True)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_success():
        entered.set()
        await release.wait()
        return "stale success"

    in_flight = asyncio.create_task(breaker.call(slow_success))
    await entered.wait()

    async def failing():
        raise RuntimeError("down")

    for _ in range(2):
        with pytest.raises(RuntimeError):
            await breaker.call(failing)
    assert breaker.state is CircuitState.OPEN

    release.set()
    assert await in_flight == "stale success"
    assert breaker.state is CircuitState.OPEN
    with pytest.raises(CircuitOpenError):
        await breaker.call(lambda: "blocked")


@pytest.mark.asyncio
async def test_bulkhead_limits_concurrency_and_releases_on_failure():
    bulkhead = Bulkhead(1, timeout=0.01)
    with pytest.raises(AttributeError):
        bulkhead.limit = 2  # type: ignore[misc]
    with pytest.raises(AttributeError):
        bulkhead.timeout = 1  # type: ignore[misc]
    entered = asyncio.Event()
    release = asyncio.Event()

    async def first():
        entered.set()
        await release.wait()
        return 1

    task = asyncio.create_task(bulkhead.call(first))
    await entered.wait()
    with pytest.raises(TimeoutError):
        await bulkhead.call(lambda: 2)
    release.set()
    assert await task == 1
    assert await bulkhead.call(lambda: 3) == 3


@pytest.mark.asyncio
async def test_bulkhead_rejects_non_callable_before_admission() -> None:
    """Invalid local callbacks fail before waiting for shared concurrency capacity."""
    bulkhead = Bulkhead(1)
    with pytest.raises(TypeError, match="callable"):
        await bulkhead.call(None)  # type: ignore[arg-type]
    assert await bulkhead.call(lambda: "slot remains available") == "slot remains available"


@pytest.mark.asyncio
async def test_bulkhead_capacity_rejection_does_not_trip_circuit_or_retry() -> None:
    """Local admission timeouts are resource failures, not provider outages."""
    bulkhead = Bulkhead(1, timeout=0.01)
    circuit = CircuitBreaker(failure_threshold=1, local_only=True)
    entered = asyncio.Event()
    release = asyncio.Event()
    provider_calls = 0

    async def occupy_capacity() -> None:
        entered.set()
        await release.wait()

    async def provider_operation() -> None:
        nonlocal provider_calls
        provider_calls += 1

    holder = asyncio.create_task(bulkhead.call(occupy_capacity))
    await entered.wait()
    try:
        with pytest.raises(BulkheadFullError):
            await resilient_call(provider_operation, circuit=circuit, bulkhead=bulkhead)
        assert provider_calls == 0
        assert circuit.state is CircuitState.CLOSED
        assert DefaultFailureClassifier().classify(BulkheadFullError()) is FailureKind.RESOURCE
    finally:
        release.set()
        await holder


@pytest.mark.asyncio
async def test_deadline_composes_child_budget_and_times_out():
    deadline = Deadline(0.01)
    child = deadline.child(1)
    assert child.timeout <= 1
    with pytest.raises(TimeoutError):
        async with deadline.scope():
            await asyncio.sleep(1)
    assert deadline.expired


@pytest.mark.asyncio
async def test_deadline_child_reports_expired_parent() -> None:
    deadline = Deadline(0.001)
    await asyncio.sleep(0.01)
    with pytest.raises(TimeoutError, match="expired"):
        deadline.child(1)


@pytest.mark.asyncio
async def test_expired_deadline_does_not_start_synchronous_operation() -> None:
    """An expired budget is rejected before a synchronous operation can have side effects."""
    deadline = Deadline(0.001)
    await asyncio.sleep(0.01)
    invoked = False

    def operation() -> None:
        nonlocal invoked
        invoked = True

    with pytest.raises(TimeoutError, match="expired"):
        await resilient_call(operation, deadline=deadline)

    assert not invoked


@pytest.mark.asyncio
async def test_resilient_call_composes_retry_and_classification() -> None:
    attempts = 0

    async def operation() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 2:
            raise TimeoutError("temporary")
        return "ok"

    result = await resilient_call(
        operation,
        retry_policy=RetryPolicy(max_attempts=3, initial_delay=0),
    )
    assert result == "ok"
    assert attempts == 2


@pytest.mark.asyncio
async def test_resilient_call_does_not_retry_permanent_failures() -> None:
    attempts = 0

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError("invalid")

    with pytest.raises(ValueError):
        await resilient_call(
            operation,
            retry_policy=RetryPolicy(max_attempts=5, initial_delay=0),
        )
    assert attempts == 1


@pytest.mark.asyncio
async def test_resilient_call_honors_falsey_classifier_and_validates_contract() -> None:
    """A falsey classifier remains authoritative and malformed classifiers fail early."""

    class PermanentClassifier:
        def __bool__(self) -> bool:
            return False

        def classify(self, _error: Exception) -> FailureKind:
            return FailureKind.PERMANENT

    attempts = 0

    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        raise ConnectionError("provider unavailable")

    with pytest.raises(ConnectionError):
        await resilient_call(
            operation,
            retry_policy=RetryPolicy(max_attempts=3, initial_delay=0),
            classifier=PermanentClassifier(),
        )
    assert attempts == 1

    with pytest.raises(TypeError, match="classifier"):
        await resilient_call(operation, classifier=object())  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_retry_rejects_a_malformed_policy_before_starting_work() -> None:
    """Retry policy objects are validated before the first operation invocation."""
    started = False

    def operation() -> None:
        nonlocal started
        started = True

    with pytest.raises(TypeError, match="RetryPolicy"):
        await retry(operation, policy=object())  # type: ignore[arg-type]
    assert not started


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "option",
    [
        {"deadline": object()},
        {"retry_policy": object()},
        {"circuit": object()},
        {"bulkhead": object()},
    ],
)
async def test_resilient_call_validates_options_before_rate_admission(option) -> None:
    """Invalid composition fails before consuming quota or invoking provider work."""
    limiter = AsyncRateLimiter(1, 60)
    invoked = False

    def operation() -> None:
        nonlocal invoked
        invoked = True

    with pytest.raises(TypeError):
        await resilient_call(
            operation,
            rate_limiter=limiter,
            rate_limit_key="provider",
            **option,
        )

    assert limiter.key_count == 0
    assert not invoked


@pytest.mark.asyncio
@pytest.mark.parametrize("cost", [True, 1.0])
async def test_unused_rate_limit_cost_still_requires_integer_contract(cost) -> None:
    """A misleading unused option must not pass validation through bool/int equality."""
    invoked = False

    def operation() -> None:
        nonlocal invoked
        invoked = True

    with pytest.raises(ValueError, match="rate_limit_cost"):
        await resilient_call(operation, rate_limit_cost=cost)  # type: ignore[arg-type]
    assert not invoked


@pytest.mark.asyncio
async def test_resilient_call_rejects_noncallable_before_rate_admission() -> None:
    """An invalid operation cannot consume a provider's rate capacity."""
    limiter = AsyncRateLimiter(1, 60)
    with pytest.raises(TypeError, match="operation"):
        await resilient_call(
            None,  # type: ignore[arg-type]
            rate_limiter=limiter,
            rate_limit_key="provider",
        )
    assert limiter.key_count == 0


def test_failure_classifier_and_policy_validation():
    classifier = DefaultFailureClassifier()
    assert classifier.classify(TimeoutError()) is FailureKind.TRANSIENT
    assert classifier.classify(MemoryError()) is FailureKind.RESOURCE
    assert classifier.classify(ValueError()) is FailureKind.PERMANENT
    assert classifier.classify(RuntimeError()) is FailureKind.UNKNOWN
    with pytest.raises(ValueError):
        Deadline(0)
    with pytest.raises(ValueError):
        Deadline(float("inf"))
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
    with pytest.raises(ValueError):
        RetryPolicy(initial_delay=float("nan"))
    with pytest.raises(ValueError):
        RetryPolicy(max_delay=float("inf"))
    with pytest.raises(ValueError):
        Bulkhead(0)
    with pytest.raises(ValueError):
        Bulkhead(1, timeout=float("nan"))
    with pytest.raises(ValueError):
        CircuitBreaker(recovery_timeout=0)
    with pytest.raises(ValueError):
        CircuitBreaker(recovery_timeout=float("nan"))
    with pytest.raises(ValueError):
        Deadline(1).child(float("inf"))
    with pytest.raises(ValueError):
        Deadline(1, started_at=float("nan"))


@pytest.mark.parametrize(
    "factory",
    [
        lambda: Deadline(True),
        lambda: Deadline("1"),
        lambda: Deadline(1).child(True),
        lambda: Deadline(1).child("1"),
        lambda: RetryPolicy(max_attempts=True),
        lambda: RetryPolicy(max_attempts="1"),
        lambda: RetryPolicy(max_attempts=1_000_001),
        lambda: RetryPolicy(initial_delay=True),
        lambda: RetryPolicy(initial_delay="1"),
        lambda: RetryPolicy(multiplier=True),
        lambda: RetryPolicy(multiplier="1"),
        lambda: RetryPolicy(max_delay=True),
        lambda: RetryPolicy(max_delay="1"),
        lambda: RetryPolicy(jitter_ratio=True),
        lambda: RetryPolicy(jitter_ratio=-0.1),
        lambda: RetryPolicy(jitter_ratio=1.1),
        lambda: CircuitBreaker(failure_threshold=True),
        lambda: CircuitBreaker(failure_threshold="1"),
        lambda: CircuitBreaker(recovery_timeout=True),
        lambda: CircuitBreaker(recovery_timeout="1"),
        lambda: Bulkhead(True),
        lambda: Bulkhead("1"),
        lambda: Bulkhead(1, timeout=True),
        lambda: Bulkhead(1, timeout="1"),
    ],
)
def test_reliability_policies_reject_boolean_numeric_limits(factory):
    with pytest.raises(ValueError):
        factory()


@pytest.mark.asyncio
async def test_retry_predicate_and_bulkhead_releases_sync_failure():
    attempts = 0

    async def operation():
        nonlocal attempts
        attempts += 1
        raise ConnectionError("down")

    with pytest.raises(ConnectionError):
        await retry(
            operation, RetryPolicy(max_attempts=3, initial_delay=0), retry_if=lambda _: False
        )
    assert attempts == 1
    bulkhead = Bulkhead(1)

    def fail():
        raise RuntimeError("failure")

    with pytest.raises(RuntimeError):
        await bulkhead.call(fail)
    assert await bulkhead.call(lambda: "available") == "available"


@pytest.mark.asyncio
async def test_retry_rejects_noncallable_operations_and_predicates() -> None:
    with pytest.raises(TypeError, match="operation"):
        await retry(None)  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="retry_if"):
        await retry(lambda: None, retry_if=1)  # type: ignore[arg-type]
