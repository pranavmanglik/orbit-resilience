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
"""Optional retry, deadline, rate-limit, circuit-breaker, and bulkhead controls for Orbit apps."""

from orbit_resilience.errors import ResilienceError
from orbit_resilience.rate_limit import AsyncRateLimiter, RateLimiter, RateLimitExceededError
from orbit_resilience.resilience import (
    Bulkhead,
    BulkheadFullError,
    CircuitBreaker,
    CircuitOpenError,
    CircuitPermit,
    CircuitSnapshot,
    CircuitState,
    CircuitStateStore,
    CircuitStateStoreError,
    Deadline,
    DefaultFailureClassifier,
    FailureClassifier,
    FailureKind,
    RetryPolicy,
    resilient_call,
    retry,
)

__all__ = [
    "Bulkhead",
    "BulkheadFullError",
    "AsyncRateLimiter",
    "CircuitBreaker",
    "CircuitOpenError",
    "CircuitPermit",
    "CircuitSnapshot",
    "CircuitState",
    "CircuitStateStore",
    "CircuitStateStoreError",
    "DefaultFailureClassifier",
    "Deadline",
    "FailureClassifier",
    "FailureKind",
    "RetryPolicy",
    "ResilienceError",
    "RateLimitExceededError",
    "RateLimiter",
    "resilient_call",
    "retry",
]
