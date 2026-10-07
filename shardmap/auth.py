"""Caller-scoped permissions, operation quotas and audit log."""
from __future__ import annotations

import threading
import time
from collections import deque

from .models import ErrorCode, ShardMapError


class CallerPolicy:
    """Per-caller limits.

    allowed_prefixes: caller may only touch shard ids starting with one of
    these prefixes; empty tuple means unrestricted.
    max_concurrent_ops: upper bound on simultaneously open operations.
    """

    def __init__(self, allowed_prefixes: tuple[str, ...] = (),
                 max_concurrent_ops: int = 8) -> None:
        self.allowed_prefixes = tuple(allowed_prefixes)
        self.max_concurrent_ops = max_concurrent_ops

    def allows(self, shard_id: str) -> bool:
        return (not self.allowed_prefixes
                or any(shard_id.startswith(p) for p in self.allowed_prefixes))


class AuditLog:
    """Bounded ring buffer of operation records for traceability."""

    def __init__(self, capacity: int = 10000) -> None:
        self._lock = threading.Lock()
        self._entries: deque[dict] = deque(maxlen=capacity)

    def record(self, caller: str, action: str, detail: str,
               ok: bool, code: str = "") -> None:
        with self._lock:
            self._entries.append({
                "ts": time.time(), "caller": caller, "action": action,
                "detail": detail, "ok": ok, "code": code,
            })

    def entries(self) -> list[dict]:
        with self._lock:
            return list(self._entries)


class AccessController:
    """Checks permissions and quotas BEFORE any map mutation happens."""

    def __init__(self, policies: dict[str, CallerPolicy] | None = None,
                 audit: AuditLog | None = None) -> None:
        self._policies = policies if policies is not None else {}
        self._audit = audit or AuditLog()
        self._lock = threading.Lock()
        self._inflight: dict[str, int] = {}

    @property
    def audit(self) -> AuditLog:
        return self._audit

    def _policy_for(self, caller: str) -> CallerPolicy:
        return self._policies.get(caller, CallerPolicy())

    def check(self, caller: str, action: str, shard_ids: list[str]) -> None:
        """Open an operation slot after validating permission and quota.

        Raises ShardMapError with a distinguishable code; nothing in the map
        has been touched at this point. On success the caller must later call
        op_done() exactly once.
        """
        policy = self._policy_for(caller)
        denied = [s for s in shard_ids if not policy.allows(s)]
        if denied:
            self._audit.record(caller, action, f"denied shards={denied}",
                               ok=False, code=ErrorCode.PERMISSION_DENIED.value)
            raise ShardMapError(
                ErrorCode.PERMISSION_DENIED,
                f"caller {caller!r} may not operate on shards {denied}")
        with self._lock:
            used = self._inflight.get(caller, 0)
            if used >= policy.max_concurrent_ops:
                self._audit.record(caller, action,
                                   f"quota exceeded ({used}/{policy.max_concurrent_ops})",
                                   ok=False, code=ErrorCode.QUOTA_EXCEEDED.value)
                raise ShardMapError(
                    ErrorCode.QUOTA_EXCEEDED,
                    f"caller {caller!r} has {used} ops in flight, "
                    f"limit {policy.max_concurrent_ops}")
            self._inflight[caller] = used + 1
        self._audit.record(caller, action, f"shards={sorted(shard_ids)}", ok=True)

    def op_done(self, caller: str) -> None:
        with self._lock:
            n = self._inflight.get(caller, 0)
            if n <= 1:
                self._inflight.pop(caller, None)
            else:
                self._inflight[caller] = n - 1
