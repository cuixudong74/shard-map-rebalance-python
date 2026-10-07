"""Versioned shard map store with copy-on-write commits and reader pinning."""
from __future__ import annotations

import threading

from .errors import RejectedError, RejectReason
from .models import MapVersion


class VersionedMapStore:
    """Keeps committed map versions.

    - Commits are atomic: a version is either fully visible or not at all,
      so concurrent readers never see a mix of old and new ownership.
    - Readers pin the version they are using; GC never collects a pinned
      version, so long-running readers are not affected by retention.
    """

    def __init__(self, max_retained_versions: int = 16) -> None:
        if max_retained_versions < 1:
            raise ValueError("max_retained_versions must be >= 1")
        self._lock = threading.RLock()
        self._versions: dict[int, MapVersion] = {}
        self._current: int = 0
        self._pins: dict[int, int] = {}
        self._max_retained = max_retained_versions
        self._listeners: list = []

    def add_listener(self, fn) -> None:
        """Register fn(MapVersion) called on every commit (for snapshots)."""
        with self._lock:
            self._listeners.append(fn)

    def restore(self, mv: MapVersion) -> None:
        """Load a persisted snapshot into an empty store (crash recovery)."""
        with self._lock:
            if self._versions:
                raise RejectedError(RejectReason.INVALID_STATE,
                                    "restore only allowed on an empty store")
            self._versions[mv.version] = mv
            self._current = mv.version

    @property
    def current_version(self) -> int:
        with self._lock:
            return self._current

    def current(self) -> MapVersion:
        with self._lock:
            return self._versions[self._current]

    def get(self, version: int) -> MapVersion | None:
        with self._lock:
            return self._versions.get(version)

    def commit(self, mv: MapVersion) -> MapVersion:
        """Install ``mv`` as the new current version (must be current+1)."""
        with self._lock:
            if mv.version != self._current + 1:
                raise RejectedError(
                    RejectReason.VERSION_CONFLICT,
                    f"expected version {self._current + 1}, got {mv.version}",
                )
            self._versions[mv.version] = mv
            self._current = mv.version
            listeners = list(self._listeners)
            self.gc()
        for fn in listeners:
            fn(mv)
        return mv

    def pin(self, version: int) -> MapVersion:
        """Pin and return a version; caller must later release() it."""
        with self._lock:
            mv = self._versions.get(version)
            if mv is None:
                raise RejectedError(
                    RejectReason.NOT_FOUND, f"version {version} not found")
            self._pins[version] = self._pins.get(version, 0) + 1
            return mv

    def release(self, version: int) -> None:
        with self._lock:
            n = self._pins.get(version, 0)
            if n <= 1:
                self._pins.pop(version, None)
            else:
                self._pins[version] = n - 1

    def gc(self) -> list[int]:
        """Drop old unpinned versions beyond retention; return dropped ids."""
        with self._lock:
            keep_from = self._current - self._max_retained + 1
            dropped = [v for v in self._versions
                       if v < keep_from and self._pins.get(v, 0) == 0]
            for v in dropped:
                del self._versions[v]
            return sorted(dropped)

    def retained_versions(self) -> list[int]:
        with self._lock:
            return sorted(self._versions)
