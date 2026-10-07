"""Bounded in-memory audit log of operations."""
from __future__ import annotations

import threading
import time
from collections import deque


class AuditLog:
    """Ring buffer of operation records; bounded memory by construction."""

    def __init__(self, capacity: int = 1024) -> None:
        self._lock = threading.Lock()
        self._entries: deque[dict] = deque(maxlen=capacity)

    def record(self, op: str, caller: str, detail: dict) -> None:
        with self._lock:
            self._entries.append({
                "ts": time.time(),
                "op": op,
                "caller": caller,
                "detail": detail,
            })

    def entries(self) -> list[dict]:
        with self._lock:
            return list(self._entries)
