"""Error types with distinguishable reason codes for the shard map service."""
from __future__ import annotations

from enum import Enum


class RejectReason(str, Enum):
    """Machine-distinguishable rejection reasons."""

    UNAUTHORIZED_SHARD = "unauthorized_shard"
    CONCURRENCY_EXCEEDED = "concurrency_exceeded"
    VERSION_CONFLICT = "version_conflict"
    INVALID_PLAN = "invalid_plan"
    REBALANCE_IN_PROGRESS = "rebalance_in_progress"
    NOT_FOUND = "not_found"
    INVALID_STATE = "invalid_state"


class ShardMapError(Exception):
    """Base error for the shard map service."""


class RejectedError(ShardMapError):
    """Raised when a request is rejected *before* any mutation happens."""

    def __init__(self, reason: RejectReason, message: str = "") -> None:
        super().__init__(message or reason.value)
        self.reason = reason
