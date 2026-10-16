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
"""Cancellation-safe retry, circuit-breaker and concurrency-isolation primitives."""

from __future__ import annotations

import asyncio
import inspect
import random
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from enum import StrEnum
from time import monotonic
from typing import Protocol, TypeVar, runtime_checkable

from orbit_resilience._limits import MAX_RESILIENCE_CAPACITY, is_finite_number
from orbit_resilience.errors import ResilienceError
from orbit_resilience.rate_limit import (
    RateLimiter,
    RateLimitExceededError,
    validate_rate_limit_key,
)

T = TypeVar("T")
Operation = Callable[[], T | Awaitable[T]]


class FailureKind(StrEnum):
    """Classification used to decide whether an operation may be retried."""

    TRANSIENT = "transient"
    PERMANENT = "permanent"
    RESOURCE = "resource"
    UNKNOWN = "unknown"


class BulkheadFullError(ResilienceError, TimeoutError):
    """Raised when a bulkhead cannot admit an operation before its queue deadline."""

    def __init__(self, message: str = "Bulkhead admission capacity was exhausted.") -> None:
        super().__init__(
            "resilience.bulkhead-full",
            message,
        )


@runtime_checkable
class FailureClassifier(Protocol):
    """Adapter boundary for classifying failures without coupling to provider exception types."""

    def classify(self, error: Exception) -> FailureKind:
        """Return the operational category of an exception."""


class DefaultFailureClassifier:
    """Conservative defaults for common transport and programming failures."""

    def classify(self, error: Exception) -> FailureKind:
        """Classify common failures without treating unknown exceptions as retryable."""
        if isinstance(error, (BulkheadFullError, RateLimitExceededError)):
            return FailureKind.RESOURCE
        if isinstance(error, (TimeoutError, ConnectionError, OSError)):
            return FailureKind.TRANSIENT
        if isinstance(error, MemoryError):
            return FailureKind.RESOURCE
        if isinstance(error, (ValueError, TypeError, KeyError)):
            return FailureKind.PERMANENT
        return FailureKind.UNKNOWN


@dataclass(frozen=True)
class Deadline:
    """Monotonic deadline that composes parent and child timeout budgets."""

    timeout: float
    started_at: float = field(default_factory=monotonic)

    def __post_init__(self) -> None:
        if (
            isinstance(self.timeout, bool)
            or not isinstance(self.timeout, (int, float))
            or not is_finite_number(self.timeout)
            or self.timeout <= 0
            or isinstance(self.started_at, bool)
            or not isinstance(self.started_at, (int, float))
            or not is_finite_number(self.started_at)
        ):
            raise ValueError("Deadline timeout and start time must be finite and valid.")

    @property
    def remaining(self) -> float:
        """Return seconds remaining, clamped to zero after expiry."""
        return max(0.0, self.timeout - (monotonic() - self.started_at))

    @property
    def expired(self) -> bool:
        """Return whether no budget remains."""
        return self.remaining <= 0

    def child(self, timeout: float) -> Deadline:
        """Create a deadline bounded by both this deadline and ``timeout``."""
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not is_finite_number(timeout)
            or timeout <= 0
        ):
            raise ValueError("Child deadline timeout must be positive.")
        if self.expired:
            raise TimeoutError("Parent deadline has expired.")
        return Deadline(min(timeout, self.remaining))

    @asynccontextmanager
    async def scope(self) -> AsyncIterator[None]:
        """Bound an async operation by the remaining budget."""
        remaining = self.remaining
        if remaining <= 0:
            raise TimeoutError("Deadline has expired.")
        async with asyncio.timeout(remaining):
            yield


@dataclass(frozen=True)
class RetryPolicy:
    """Finite exponential retry schedule with proportional jitter.

    ``jitter_ratio`` randomizes each non-zero backoff in the inclusive range around its
    exponential delay, then ``max_delay`` caps the result. Set it to zero for deterministic
    timing in applications that specifically require it.
    """

    max_attempts: int = 3
    initial_delay: float = 0.1
    multiplier: float = 2.0
    max_delay: float = 30.0
    jitter_ratio: float = 0.2

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_attempts, bool)
            or not isinstance(self.max_attempts, int)
            or not 1 <= self.max_attempts <= MAX_RESILIENCE_CAPACITY
            or isinstance(self.initial_delay, bool)
            or not isinstance(self.initial_delay, (int, float))
            or not is_finite_number(self.initial_delay)
            or self.initial_delay < 0
            or isinstance(self.multiplier, bool)
            or not isinstance(self.multiplier, (int, float))
            or not is_finite_number(self.multiplier)
            or self.multiplier < 1
            or isinstance(self.max_delay, bool)
            or not isinstance(self.max_delay, (int, float))
            or not is_finite_number(self.max_delay)
            or self.max_delay < 0
            or isinstance(self.jitter_ratio, bool)
            or not isinstance(self.jitter_ratio, (int, float))
            or not is_finite_number(self.jitter_ratio)
            or not 0 <= self.jitter_ratio <= 1
        ):
            raise ValueError("Retry policy values are invalid.")


def _retry_delay(policy: RetryPolicy, attempt: int) -> float:
    """Return a capped, optionally jittered backoff without unbounded exponentiation."""
    if policy.initial_delay == 0 or policy.max_delay == 0:
        return 0.0
    if policy.initial_delay >= policy.max_delay:
        bounded = policy.max_delay
    else:
        try:
            delay = policy.initial_delay * policy.multiplier**attempt
        except OverflowError:
            delay = policy.max_delay
        bounded = min(policy.max_delay, delay) if is_finite_number(delay) else policy.max_delay
    if policy.jitter_ratio:
        factor = random.uniform(1 - policy.jitter_ratio, 1 + policy.jitter_ratio)
        bounded = min(policy.max_delay, max(0.0, bounded * factor))
    return float(bounded)


async def retry(
    operation: Operation[T],
    policy: RetryPolicy | None = None,
    *,
    retry_if: Callable[[Exception], bool] | None = None,
) -> T:
    """Run an operation with bounded backoff; cancellation always propagates immediately."""
    if not callable(operation):
        raise TypeError("operation must be callable.")
    if retry_if is not None and not callable(retry_if):
        raise TypeError("retry_if must be callable.")
    if policy is None:
        policy = RetryPolicy()
    elif not isinstance(policy, RetryPolicy):
        raise TypeError("policy must be a RetryPolicy.")
    for attempt in range(policy.max_attempts):
        try:
            result = operation()
            return await result if inspect.isawaitable(result) else result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if attempt + 1 >= policy.max_attempts or (retry_if is not None and not retry_if(exc)):
                raise
            delay = _retry_delay(policy, attempt)
            if delay:
                await asyncio.sleep(delay)
    raise AssertionError("unreachable")


async def resilient_call(
    operation: Operation[T],
    *,
    deadline: Deadline | None = None,
    retry_policy: RetryPolicy | None = None,
    circuit: CircuitBreaker | None = None,
    bulkhead: Bulkhead | None = None,
    classifier: FailureClassifier | None = None,
    rate_limiter: RateLimiter | None = None,
    rate_limit_key: str | None = None,
    rate_limit_cost: int = 1,
    rate_limit_timeout: float | None = None,
) -> T:
    """Compose rate, deadline, retry, circuit and bulkhead policies with cancellation preserved.

    Rate-limit capacity is acquired for each provider attempt, before circuit and bulkhead
    admission. A configured retry therefore consumes capacity for every attempt.
    """
    if not callable(operation):
        raise TypeError("operation must be callable.")
    if deadline is not None and not isinstance(deadline, Deadline):
        raise TypeError("deadline must be a Deadline.")
    if retry_policy is not None and not isinstance(retry_policy, RetryPolicy):
        raise TypeError("retry_policy must be a RetryPolicy.")
    if circuit is not None and not isinstance(circuit, CircuitBreaker):
        raise TypeError("circuit must be a CircuitBreaker.")
    if bulkhead is not None and not isinstance(bulkhead, Bulkhead):
        raise TypeError("bulkhead must be a Bulkhead.")
    if (
        isinstance(rate_limit_cost, bool)
        or not isinstance(rate_limit_cost, int)
        or not 1 <= rate_limit_cost <= MAX_RESILIENCE_CAPACITY
    ):
        raise ValueError("rate_limit_cost must be a positive bounded integer.")

    if rate_limiter is not None:
        if not isinstance(rate_limiter, RateLimiter):
            raise TypeError("rate_limiter must implement the asynchronous RateLimiter contract.")
        if rate_limit_key is None:
            raise ValueError("rate_limit_key is required when rate_limiter is configured.")
        validate_rate_limit_key(rate_limit_key)
        if rate_limit_timeout is not None and (
            isinstance(rate_limit_timeout, bool)
            or not isinstance(rate_limit_timeout, (int, float))
            or not is_finite_number(rate_limit_timeout)
            or rate_limit_timeout < 0
        ):
            raise ValueError("rate_limit_timeout must be finite and nonnegative.")
    elif rate_limit_key is not None or rate_limit_cost != 1 or rate_limit_timeout is not None:
        raise ValueError("Rate-limit options require a rate_limiter.")

    if classifier is None:
        selected: FailureClassifier = DefaultFailureClassifier()
    elif not isinstance(classifier, FailureClassifier) or not callable(classifier.classify):
        raise TypeError("classifier must provide a callable classify method.")
    else:
        selected = classifier

    async def protected() -> T:
        """Apply rate, circuit, and bulkhead admission before invoking the operation."""
        if rate_limiter is not None:
            assert rate_limit_key is not None  # validated before any operation starts
            await rate_limiter.acquire(
                rate_limit_key,
                cost=rate_limit_cost,
                timeout=rate_limit_timeout,
            )
        if circuit is not None and bulkhead is not None:
            # Acquire local capacity before entering the breaker. A rejected waiter never
            # reached the provider and must not count as a dependency failure.
            return await bulkhead.call(lambda: circuit.call(operation))
        if circuit is not None:
            return await circuit.call(operation)
        if bulkhead is not None:
            return await bulkhead.call(operation)
        result = operation()
        return await result if inspect.isawaitable(result) else result

    async def execute() -> T:
        """Retry only failures classified as transient by the selected classifier."""
        return await retry(
            protected,
            retry_policy,
            retry_if=lambda error: selected.classify(error) is FailureKind.TRANSIENT,
        )

    if deadline is None:
        return await execute()
    async with deadline.scope():
        return await execute()


class CircuitState(StrEnum):
    """Circuit breaker state."""

    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half-open"


class CircuitOpenError(ResilienceError, RuntimeError):
    """Raised when a circuit rejects work while open."""

    def __init__(
        self,
        message: str = "Circuit breaker is open.",
        *,
        code: str = "resilience.circuit-open",
    ) -> None:
        super().__init__(code, message)


class CircuitStateStoreError(ResilienceError, RuntimeError):
    """A shared circuit-state backend failed; no local-only fallback is attempted."""

    def __init__(self, message: str, *, operation_attempted: bool = False) -> None:
        """Record whether an operation was already invoked before state persistence failed."""
        self.operation_attempted = operation_attempted
        super().__init__(
            "resilience.circuit-state-store-failed",
            message,
            context={"operation_attempted": operation_attempted},
        )


@dataclass(frozen=True, slots=True)
class CircuitPermit:
    """One shared-state admission result; the optional probe token is intentionally redacted."""

    epoch: int
    probe_token: str | None = field(default=None, repr=False)

    def __post_init__(self) -> None:
        if (
            isinstance(self.epoch, bool)
            or not isinstance(self.epoch, int)
            or self.epoch < 0
            or (
                self.probe_token is not None
                and (
                    not isinstance(self.probe_token, str)
                    or not self.probe_token
                    or len(self.probe_token) > 256
                )
            )
        ):
            raise ValueError("Circuit permit is invalid.")


@dataclass(frozen=True, slots=True)
class CircuitSnapshot:
    """Immutable state observed from a local or shared circuit breaker."""

    state: CircuitState
    failures: int
    epoch: int

    def __post_init__(self) -> None:
        if (
            not isinstance(self.state, CircuitState)
            or isinstance(self.failures, bool)
            or not isinstance(self.failures, int)
            or not 0 <= self.failures <= MAX_RESILIENCE_CAPACITY
            or isinstance(self.epoch, bool)
            or not isinstance(self.epoch, int)
            or self.epoch < 0
        ):
            raise ValueError("Circuit snapshot is invalid.")


@runtime_checkable
class CircuitStateStore(Protocol):
    """Atomic shared-state contract for process-independent circuit breakers.

    Adapters must serialize admission and completion transitions, use epochs to ignore stale
    completions, and allow at most one valid half-open probe at a time. Probe admission is leased
    for a bounded duration so a crashed worker cannot hold the circuit indefinitely. Store
    failures must raise a sanitized error; callers must never silently fall back to local state.
    """

    async def acquire(
        self,
        name: str,
        *,
        failure_threshold: int,
        recovery_timeout: float,
        probe_lease_timeout: float,
    ) -> CircuitPermit | None:
        """Admit work or return ``None`` while the circuit is open or already probing."""

    async def record(
        self,
        name: str,
        permit: CircuitPermit,
        *,
        succeeded: bool,
        failure_threshold: int,
        recovery_timeout: float,
    ) -> CircuitSnapshot:
        """Apply a completion only if the permit still belongs to the current breaker epoch."""

    async def abandon_probe(self, name: str, permit: CircuitPermit) -> None:
        """Release a matching half-open probe after cancellation without changing failure state."""

    async def snapshot(self, name: str) -> CircuitSnapshot:
        """Return the latest shared state for diagnostics and health reporting."""


def _valid_circuit_name(name: str | None) -> bool:
    """Require a stable, bounded identifier that cannot carry arbitrary log or key data."""
    return (
        isinstance(name, str)
        and 1 <= len(name) <= 128
        and all(
            character.isascii() and (character.isalnum() or character in "._:-")
            for character in name
        )
    )


class CircuitBreaker:
    """Protect an operation with local or adapter-backed shared circuit state.

    A shared ``state_store`` and stable ``name`` are required by default so independently hosted
    workers make admission decisions from the same state. ``local_only=True`` explicitly selects
    process-local state for tests and single-process deployments. Shared-backend errors fail
    closed; the breaker never falls back to local state for admission.
    """

    def __init__(
        self,
        *,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        state_store: CircuitStateStore | None = None,
        name: str | None = None,
        probe_lease_timeout: float = 30.0,
        local_only: bool = False,
    ) -> None:
        if state_store is not None and not isinstance(state_store, CircuitStateStore):
            raise TypeError("state_store must implement the CircuitStateStore contract.")
        if (
            isinstance(failure_threshold, bool)
            or not isinstance(failure_threshold, int)
            or not 1 <= failure_threshold <= MAX_RESILIENCE_CAPACITY
            or isinstance(recovery_timeout, bool)
            or not isinstance(recovery_timeout, (int, float))
            or not is_finite_number(recovery_timeout)
            or recovery_timeout <= 0
            or isinstance(probe_lease_timeout, bool)
            or not isinstance(probe_lease_timeout, (int, float))
            or not is_finite_number(probe_lease_timeout)
            or not 0.1 <= probe_lease_timeout <= 600
            or (state_store is not None and not _valid_circuit_name(name))
            or (state_store is None and name is not None)
        ):
            raise ValueError("Circuit breaker values are invalid.")
        if not isinstance(local_only, bool):
            raise TypeError("local_only must be a boolean.")
        if state_store is None and not local_only:
            raise ValueError(
                "CircuitBreaker requires a shared state_store; pass local_only=True only when "
                "process-local state is intentional."
            )
        if state_store is not None and local_only:
            raise ValueError("local_only cannot be combined with a shared state_store.")
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._state = CircuitState.CLOSED
        self._failures = 0
        self._opened_at = 0.0
        self._probe_active = False
        # Every transition to OPEN or HALF_OPEN advances the epoch. An operation
        # that began in an older epoch must not overwrite a newer breaker state
        # when it completes later.
        self._epoch = 0
        self._lock = asyncio.Lock()
        self._state_store = state_store
        self._name = name
        self._probe_lease_timeout = float(probe_lease_timeout)

    @property
    def failure_threshold(self) -> int:
        """Return the fixed consecutive-failure threshold."""
        return self._failure_threshold

    @property
    def recovery_timeout(self) -> float:
        """Return the fixed half-open recovery delay."""
        return self._recovery_timeout

    @property
    def state(self) -> CircuitState:
        """Return the current breaker state for diagnostics and health reporting."""
        return self._state

    @property
    def failures(self) -> int:
        """Return consecutive failures recorded since the last successful call."""
        return self._failures

    async def refresh_state(self) -> CircuitSnapshot:
        """Refresh the current state snapshot, including the shared state when configured.

        The synchronous ``state`` and ``failures`` properties are last-observed values in shared
        mode. Use this method for an up-to-date diagnostic snapshot; an observation does not grant
        admission and must not be used to make a separate call decision.
        """
        if self._state_store is None:
            return CircuitSnapshot(self._state, self._failures, self._epoch)
        assert self._name is not None
        try:
            snapshot = await self._state_store.snapshot(self._name)
        except asyncio.CancelledError:
            raise
        except Exception:
            raise CircuitStateStoreError("Circuit state store snapshot failed.") from None
        if not isinstance(snapshot, CircuitSnapshot):
            raise CircuitStateStoreError("Circuit state store returned an invalid snapshot.")
        self._apply_snapshot(snapshot)
        return snapshot

    async def call(self, operation: Operation[T]) -> T:
        """Execute a callable operation or reject it while the circuit is open.

        Validate local input before changing breaker state. Provider or operation exceptions count
        toward the consecutive-failure threshold; cancellation propagates without changing it.
        """
        if not callable(operation):
            raise TypeError("operation must be callable.")
        if self._state_store is not None:
            return await self._shared_call(operation)
        probe = False
        async with self._lock:
            if self._state is CircuitState.OPEN:
                if monotonic() - self._opened_at < self.recovery_timeout:
                    raise CircuitOpenError("Circuit breaker is open.")
                self._state = CircuitState.HALF_OPEN
                self._epoch += 1
            if self._state is CircuitState.HALF_OPEN:
                if self._probe_active:
                    raise CircuitOpenError(
                        "Circuit breaker is probing recovery.",
                        code="resilience.circuit-probe-active",
                    )
                self._probe_active = True
                probe = True
            epoch = self._epoch
        if probe:
            try:
                return await self._attempt(operation, epoch)
            finally:
                async with self._lock:
                    self._probe_active = False
        return await self._attempt(operation, epoch)

    async def _shared_call(self, operation: Operation[T]) -> T:
        """Admit and record one call through the configured atomic shared-state store."""
        store = self._state_store
        name = self._name
        assert store is not None and name is not None
        try:
            permit = await store.acquire(
                name,
                failure_threshold=self.failure_threshold,
                recovery_timeout=self.recovery_timeout,
                probe_lease_timeout=self._probe_lease_timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise CircuitStateStoreError("Circuit state store admission failed.") from None
        if permit is None:
            raise CircuitOpenError(
                "Circuit breaker is open or already probing recovery.",
                code="resilience.circuit-open",
            )
        if not isinstance(permit, CircuitPermit):
            raise CircuitStateStoreError("Circuit state store returned an invalid permit.")

        try:
            result = operation()
            value = await result if inspect.isawaitable(result) else result
        except asyncio.CancelledError:
            if permit.probe_token is not None:
                try:
                    await asyncio.shield(store.abandon_probe(name, permit))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    # The adapter's bounded probe lease eventually releases a crashed/cancelled
                    # owner's admission. Preserve the operation's cancellation either way.
                    pass
            raise
        except Exception:
            snapshot = await self._record_shared_outcome(store, name, permit, succeeded=False)
            self._apply_snapshot(snapshot)
            raise
        snapshot = await self._record_shared_outcome(store, name, permit, succeeded=True)
        self._apply_snapshot(snapshot)
        return value

    async def _record_shared_outcome(
        self, store: CircuitStateStore, name: str, permit: CircuitPermit, *, succeeded: bool
    ) -> CircuitSnapshot:
        """Persist a completion and preserve explicit ambiguity if that write fails."""
        try:
            snapshot = await store.record(
                name,
                permit,
                succeeded=succeeded,
                failure_threshold=self.failure_threshold,
                recovery_timeout=self.recovery_timeout,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            raise CircuitStateStoreError(
                "Circuit state store could not record an attempted operation.",
                operation_attempted=True,
            ) from None
        if not isinstance(snapshot, CircuitSnapshot):
            raise CircuitStateStoreError(
                "Circuit state store returned an invalid completion snapshot.",
                operation_attempted=True,
            )
        return snapshot

    def _apply_snapshot(self, snapshot: CircuitSnapshot) -> None:
        """Update the last-observed public properties from a validated immutable snapshot."""
        self._state = snapshot.state
        self._failures = snapshot.failures
        self._epoch = snapshot.epoch

    async def _attempt(self, operation: Operation[T], epoch: int) -> T:
        try:
            result = operation()
            value = await result if inspect.isawaitable(result) else result
        except asyncio.CancelledError:
            raise
        except Exception:
            async with self._lock:
                # An older operation may finish after recovery has already
                # entered a new half-open epoch. It must not fail the current
                # probe or alter the newer failure count.
                if epoch != self._epoch:
                    raise
                self._failures += 1
                # A half-open probe must reopen on its first failure. Also avoid
                # refreshing OPEN's timestamp because of an older in-flight call.
                should_open = self._state is CircuitState.HALF_OPEN or (
                    self._state is CircuitState.CLOSED and self._failures >= self.failure_threshold
                )
                if should_open:
                    self._state = CircuitState.OPEN
                    self._opened_at = monotonic()
                    self._epoch += 1
            raise
        else:
            async with self._lock:
                # A completion from a previous epoch cannot close a breaker
                # that has since opened because of newer failures.
                if epoch == self._epoch and self._state is not CircuitState.OPEN:
                    self._failures = 0
                    self._state = CircuitState.CLOSED
            return value


class Bulkhead:
    """Bound concurrent operations and optionally reject waiters after a timeout."""

    def __init__(self, limit: int, *, timeout: float | None = None) -> None:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_RESILIENCE_CAPACITY
            or (
                timeout is not None
                and (
                    isinstance(timeout, bool)
                    or not isinstance(timeout, (int, float))
                    or not is_finite_number(timeout)
                    or timeout < 0
                )
            )
        ):
            raise ValueError("Bulkhead values are invalid.")
        self._limit = limit
        self._timeout = timeout
        self._semaphore = asyncio.Semaphore(limit)

    @property
    def limit(self) -> int:
        """Return the fixed concurrency capacity."""
        return self._limit

    @property
    def timeout(self) -> float | None:
        """Return the fixed waiter timeout."""
        return self._timeout

    async def call(self, operation: Operation[T]) -> T:
        """Run one callable operation under the concurrency limit.

        Validate the local callback before waiting for a slot. The semaphore is released in a
        ``finally`` block for success, failure, and cancellation after admission.
        """
        if not callable(operation):
            raise TypeError("operation must be callable.")
        if self.timeout is None:
            await self._semaphore.acquire()
        else:
            try:
                await asyncio.wait_for(self._semaphore.acquire(), timeout=self.timeout)
            except TimeoutError as exc:
                raise BulkheadFullError("Bulkhead capacity wait exceeded its deadline.") from exc
        try:
            result = operation()
            return await result if inspect.isawaitable(result) else result
        finally:
            self._semaphore.release()


__all__ = [
    "Bulkhead",
    "BulkheadFullError",
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitPermit",
    "CircuitSnapshot",
    "CircuitState",
    "CircuitStateStore",
    "CircuitStateStoreError",
    "Deadline",
    "DefaultFailureClassifier",
    "FailureClassifier",
    "FailureKind",
    "RetryPolicy",
    "retry",
    "resilient_call",
]
