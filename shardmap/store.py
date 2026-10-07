"""Versioned shard-map store with bounded history and reader pinning."""
from __future__ import annotations

import threading
from collections.abc import Sequence

from .models import ErrorCode, Shard, ShardMapError, ShardMapVersion


class VersionedStore:
    """Holds committed map versions.

    - commit() publishes a new immutable version atomically; readers only
      ever observe a fully committed version, never a mix of old and new.
    - Readers pin a version via acquire()/release(); pinned versions are
      never garbage-collected, so long-lived readers stay consistent.
    - History is bounded: unpinned versions older than the newest
      `max_versions` are dropped.
    """

    def __init__(self, max_versions: int = 16) -> None:
        if max_versions < 1:
            raise ValueError("max_versions must be >= 1")
        self._lock = threading.RLock()
        self._versions: dict[int, ShardMapVersion] = {}
        self._pins: dict[int, int] = {}
        self._max_versions = max_versions
        self._latest = 0

    @property
    def latest_version(self) -> int:
        with self._lock:
            return self._latest

    def current(self) -> ShardMapVersion:
        with self._lock:
            if self._latest == 0:
                raise ShardMapError(ErrorCode.NOT_FOUND, "no committed map yet")
            return self._versions[self._latest]

    def acquire(self, version: int | None = None) -> ShardMapVersion:
        """Pin and return a version (latest when version is None)."""
        with self._lock:
            v = self._latest if version is None else version
            if v not in self._versions:
                raise ShardMapError(ErrorCode.NOT_FOUND,
                                    f"version {v} no longer retained")
            self._pins[v] = self._pins.get(v, 0) + 1
            return self._versions[v]

    def release(self, version: int) -> None:
        with self._lock:
            n = self._pins.get(version, 0)
            if n <= 1:
                self._pins.pop(version, None)
            else:
                self._pins[version] = n - 1
            self._gc_locked()

    def commit(self, shards: Sequence[Shard],
               assignment: dict[str, str]) -> ShardMapVersion:
        with self._lock:
            self._validate(shards, assignment)
            self._latest += 1
            snap = ShardMapVersion(
                version=self._latest,
                shards=tuple(sorted(shards, key=lambda s: (s.start, s.shard_id))),
                assignment=dict(assignment),
            )
            self._versions[snap.version] = snap
            self._gc_locked()
            return snap

    def load(self, version: ShardMapVersion) -> None:
        """Install an already-committed version (crash recovery)."""
        with self._lock:
            self._validate(version.shards, version.assignment)
            self._versions[version.version] = version
            self._latest = max(self._latest, version.version)

    def needs_refresh(self, version: int) -> bool:
        with self._lock:
            return version != self._latest

    def known_versions(self) -> list[int]:
        with self._lock:
            return sorted(self._versions)

    def _gc_locked(self) -> None:
        while len(self._versions) > self._max_versions:
            oldest = min(self._versions)
            if oldest == self._latest or self._pins.get(oldest, 0) > 0:
                break
            del self._versions[oldest]

    @staticmethod
    def _validate(shards: Sequence[Shard], assignment: dict[str, str]) -> None:
        ordered = sorted(shards, key=lambda s: s.start)
        if not ordered or ordered[0].start != 0:
            raise ShardMapError(ErrorCode.INVALID, "shards must cover slot 0")
        prev_end = 0
        from .models import HASH_SPACE
        for s in ordered:
            if s.start != prev_end or s.end <= s.start or s.end > HASH_SPACE:
                raise ShardMapError(
                    ErrorCode.INVALID,
                    f"shards must tile the ring without gaps/overlaps: {s}")
            prev_end = s.end
        if prev_end != HASH_SPACE:
            raise ShardMapError(ErrorCode.INVALID,
                                "shards must cover the whole hash space")
        ids = {s.shard_id for s in ordered}
        if set(assignment) != ids:
            raise ShardMapError(ErrorCode.INVALID,
                                "assignment must cover exactly the shard set")
