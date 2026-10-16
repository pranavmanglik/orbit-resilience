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
"""Async token-bucket backpressure for optional Orbit service integrations."""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from dataclasses import dataclass
from time import monotonic
from typing import Protocol, runtime_checkable
from unicodedata import category

from orbit_resilience._limits import MAX_RESILIENCE_CAPACITY, is_finite_number
from orbit_resilience.errors import ResilienceError

_MAX_PERIOD = 31_536_000.0


@runtime_checkable
class RateLimiter(Protocol):
    """Adapter contract for asynchronous per-key rate-limit backpressure."""

    async def acquire(self, key: str, *, cost: int = 1, timeout: float | None = None) -> None:
        """Wait for capacity or raise ``RateLimitExceededError`` when the wait expires."""


@dataclass
class _Bucket:
    """Mutable token balance and monotonic refill timestamp for one key."""

    tokens: float
    updated_at: float


class RateLimitExceededError(ResilienceError, TimeoutError):
    """Raised when the caller's allowed wait expires before a token is available."""

    def __init__(self, retry_after: float) -> None:
        if (
            isinstance(retry_after, bool)
            or not isinstance(retry_after, (int, float))
            or not is_finite_number(retry_after)
            or retry_after < 0
        ):
            raise ValueError("retry_after must be finite and nonnegative.")
        self.retry_after = float(retry_after)
        super().__init__(
            "resilience.rate-limit-exceeded",
            "Rate-limit wait exceeded its configured timeout.",
            context={"retry_after_seconds": self.retry_after},
        )


def validate_rate_limit_key(key: str) -> None:
    """Validate the shared bounded identity contract used by every rate-limit adapter."""
    if (
        not isinstance(key, str)
        or not key
        or len(key) > 255
        or any(category(character) == "Cc" for character in key)
    ):
        raise ValueError("Rate-limit keys must be control-free and at most 255 characters.")


class AsyncRateLimiter:
    """Wait for token-bucket capacity before allowing asynchronous work to proceed.

    The limiter is process-local and instance-local. It bounds retained keys with least-recently
    used eviction and does not reserve future tokens for sleeping callers, so cancellation never
    consumes capacity. It makes no FIFO fairness guarantee; applications needing shared quotas
    across workers should provide a distributed adapter instead.
    """

    def __init__(self, limit: int, period: float, *, max_keys: int = 10_000) -> None:
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= MAX_RESILIENCE_CAPACITY
            or isinstance(period, bool)
            or not isinstance(period, (int, float))
            or not is_finite_number(period)
            or not 0 < period <= _MAX_PERIOD
            or isinstance(max_keys, bool)
            or not isinstance(max_keys, int)
            or not 1 <= max_keys <= MAX_RESILIENCE_CAPACITY
        ):
            raise ValueError("Rate limiter values are invalid.")
        rate = limit / period
        if not is_finite_number(rate) or rate <= 0:
            raise ValueError("Rate limiter refill arithmetic is outside safe bounds.")
        self._limit = limit
        self._period = float(period)
        self._rate = rate
        self._max_keys = max_keys
        self._buckets: OrderedDict[str, _Bucket] = OrderedDict()
        self._lock = asyncio.Lock()

    @property
    def limit(self) -> int:
        """Return the maximum burst size in tokens."""
        return self._limit

    @property
    def period(self) -> float:
        """Return the interval over which a full bucket refills."""
        return self._period

    @property
    def key_count(self) -> int:
        """Return the number of currently retained identities."""
        return len(self._buckets)

    async def acquire(self, key: str, *, cost: int = 1, timeout: float | None = None) -> None:
        """Consume ``cost`` tokens, waiting for refill up to an optional timeout.

        The first ``limit`` calls for a new key can proceed immediately. Subsequent calls wait
        only as long as necessary for their requested cost. ``timeout=0`` permits an immediately
        available token but rejects a call that would need to wait.
        """
        self._validate_key(key)
        if isinstance(cost, bool) or not isinstance(cost, int) or not 1 <= cost <= self.limit:
            raise ValueError("Rate-limit cost must be within the configured limit.")
        if timeout is not None and (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not is_finite_number(timeout)
            or timeout < 0
        ):
            raise ValueError("Rate-limit timeout must be finite and nonnegative.")

        started_at = monotonic()
        first_attempt = True
        last_delay = 0.0
        while True:
            if not first_attempt and timeout is not None:
                remaining = timeout - (monotonic() - started_at)
                if remaining <= 0:
                    raise RateLimitExceededError(last_delay)
            delay = await self._try_consume(key, cost)
            if delay == 0:
                return
            first_attempt = False
            last_delay = delay
            if timeout is not None:
                remaining = timeout - (monotonic() - started_at)
                if remaining <= 0:
                    raise RateLimitExceededError(delay)
                await asyncio.sleep(min(delay, remaining))
            else:
                await asyncio.sleep(delay)

    async def reset(self, key: str) -> bool:
        """Remove one key's bucket and report whether it existed."""
        self._validate_key(key)
        async with self._lock:
            return self._buckets.pop(key, None) is not None

    async def _try_consume(self, key: str, cost: int) -> float:
        """Consume tokens under the state lock or return seconds until capacity is ready."""
        now = monotonic()
        async with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                if len(self._buckets) >= self._max_keys:
                    self._buckets.popitem(last=False)
                bucket = _Bucket(float(self.limit), now)
                self._buckets[key] = bucket
            else:
                elapsed = max(0.0, now - bucket.updated_at)
                bucket.tokens = min(float(self.limit), bucket.tokens + elapsed * self._rate)
                bucket.updated_at = max(bucket.updated_at, now)
                self._buckets.move_to_end(key)

            if bucket.tokens >= cost:
                bucket.tokens -= cost
                return 0.0
            return (cost - bucket.tokens) / self._rate

    @staticmethod
    def _validate_key(key: str) -> None:
        """Reject empty, oversized, or control-character-containing identity keys."""
        validate_rate_limit_key(key)


__all__ = ["AsyncRateLimiter", "RateLimitExceededError", "RateLimiter", "validate_rate_limit_key"]
