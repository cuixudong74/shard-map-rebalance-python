"""Core type definitions for shard map versioning and rebalancing.

The key space is a fixed ring of HASH_SPACE slots. Shards are contiguous,
non-overlapping ranges [start, end) that tile the whole ring. A committed
ShardMapVersion is immutable: readers always observe one complete version.
"""
from __future__ import annotations

import enum
import time
from dataclasses import dataclass, field

HASH_SPACE = 1 << 32


class ShardState(enum.Enum):
    ACTIVE = "active"
    MIGRATING = "migrating"
    SPLITTING = "splitting"
    MERGING = "merging"


@dataclass(frozen=True)
class Shard:
    shard_id: str
    start: int  # inclusive
    end: int    # exclusive, start < end <= HASH_SPACE

    def contains(self, slot: int) -> bool:
        return self.start <= slot < self.end


@dataclass(frozen=True)
class ShardMapVersion:
    """An immutable, fully-committed shard map."""
    version: int
    shards: tuple[Shard, ...]
    assignment: dict[str, str]  # shard_id -> node_id
    created_at: float = field(default_factory=time.time)

    def owner_of(self, shard_id: str) -> str:
        return self.assignment[shard_id]

    def shard_for_slot(self, slot: int) -> Shard:
        for shard in self.shards:
            if shard.contains(slot):
                return shard
        raise KeyError(f"no shard covers slot {slot}")


class MigrationPhase(enum.Enum):
    PREPARE = "prepare"
    COPYING = "copying"
    CATCHING_UP = "catching_up"
    SWAPPING = "swapping"
    DONE = "done"
    ABORTED = "aborted"


@dataclass
class MigrationTask:
    shard_id: str
    source: str
    target: str
    phase: MigrationPhase = MigrationPhase.PREPARE
    copied: int = 0
    total: int = 0

    @property
    def progress(self) -> float:
        if self.phase is MigrationPhase.DONE:
            return 1.0
        return self.copied / self.total if self.total else 0.0


@dataclass(frozen=True)
class Move:
    shard_id: str
    source: str
    target: str


@dataclass(frozen=True)
class RebalancePlan:
    plan_id: str
    base_version: int
    moves: tuple[Move, ...]


class RebalanceStatus(enum.Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    FAILED = "failed"


class ErrorCode(enum.Enum):
    PERMISSION_DENIED = "permission_denied"
    QUOTA_EXCEEDED = "quota_exceeded"
    CONFLICT = "conflict"
    NOT_FOUND = "not_found"
    INVALID = "invalid"


class ShardMapError(Exception):
    def __init__(self, code: ErrorCode, message: str):
        super().__init__(message)
        self.code = code
        self.message = message
