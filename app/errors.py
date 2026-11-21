"""Domain errors translated to HTTP responses by the API layer."""

from __future__ import annotations

from typing import Any


class ApiError(Exception):
    """An expected, client-visible error with a stable machine-readable code."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        **extra: Any,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.extra = extra

    def as_detail(self) -> dict[str, Any]:
        detail: dict[str, Any] = {"code": self.code, "message": self.message}
        detail.update(self.extra)
        return detail


class NoFeasiblePlan(ApiError):
    """Raised when the requested partition count / byte cap cannot be satisfied."""

    def __init__(self, reason: str, message: str, **extra: Any) -> None:
        super().__init__(
            422,
            "no_feasible_plan",
            message,
            reason=reason,
            **extra,
        )
        self.reason = reason
