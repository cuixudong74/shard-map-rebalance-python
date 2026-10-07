"""Per-caller shard-range authorization and concurrency quotas."""
from __future__ import annotations

import threading
from dataclasses import dataclass

from .errors import RejectedError, RejectReason


@dataclass(frozen=True)
class CallerPolicy:
    caller: str
    allowed_shards: frozenset[str] | None = None  # None => all shards
    max_concurrent_ops: int = 4


class QuotaManager:
    """Checks happen *before* any mutation; rejections carry a
    distinguishable RejectReason."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._policies: dict[str, CallerPolicy] = {}
        self._inflight: dict[str, int] = {}

    def set_policy(self, policy: CallerPolicy) -> None:
        with self._lock:
            self._policies[policy.caller] = policy

    def _policy(self, caller: str) -> CallerPolicy:
        # Unknown callers get a deny-nothing default with modest quota.
        return self._policies.get(caller, CallerPolicy(caller=caller))

    def check_shards(self, caller: str, shards: set[str]) -> None:
        """Raise RejectedError(UNAUTHORIZED_SHARD) if out of range."""
        policy = self._policy(caller)
        if policy.allowed_shards is None:
            return
        denied = sorted(shards - policy.allowed_shards)
        if denied:
            raise RejectedError(
                RejectReason.UNAUTHORIZED_SHARD,
                f"caller {caller!r} may not touch shards {denied}")

    def acquire(self, caller: str) -> None:
        """Raise RejectedError(CONCURRENCY_EXCEEDED) if over quota."""
        policy = self._policy(caller)
        with self._lock:
            n = self._inflight.get(caller, 0)
            if n >= policy.max_concurrent_ops:
                raise RejectedError(
                    RejectReason.CONCURRENCY_EXCEEDED,
                    f"caller {caller!r} at {n}/{policy.max_concurrent_ops} ops")
            self._inflight[caller] = n + 1

    def release(self, caller: str) -> None:
        with self._lock:
            n = self._inflight.get(caller, 0)
            if n <= 1:
                self._inflight.pop(caller, None)
            else:
                self._inflight[caller] = n - 1
