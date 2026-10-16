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
"""Structured exception base for optional resilience controls."""

from orbit.errors import ErrorCategory, OrbitError, OrbitProblem


class ResilienceError(OrbitError):
    """Base for sanitized resilience failures with Core's structured problem contract."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        context: dict[str, object] | None = None,
    ) -> None:
        """Attach stable machine-readable identity and bounded operational context."""
        super().__init__(
            OrbitProblem(
                code=code,
                message=message,
                category=ErrorCategory.RUNTIME,
                context=context or {},
            )
        )


__all__ = ["ResilienceError"]
