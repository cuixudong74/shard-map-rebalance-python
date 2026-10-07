"""Core type definitions for versioned shard maps and rebalancing."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from types import MappingProxyType
from typing import Mapping

NodeId = str
ShardId = str


class ShardState(str, Enum):
    """Lifecycle state of a single shard inside a map version."""

    ACTIVE = "active"          # normal, owned by exactly one node
    MIGRATING = "migrating"    # copy in progress; source still authoritative
    SPLITTING = "splitting"    # parent being split; parent still authoritative
    MERGING = "merging"        # children being merged; children still authoritative


class TaskState(str, Enum):
    PENDING = "pending"
    COPYING = "copying"
    DONE = "done"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobState(str, Enum):
    RUNNING = "running"
    COMMITTED = "committed"
    ABORTED = "aborted"


@dataclass(frozen=True)
class ShardRecord:
    shard_id: ShardId
    node: NodeId
    state: ShardState = ShardState.ACTIVE
    # For shards produced by a split / consumed by a merge.
    parent: ShardId | None = None
    children: tuple[ShardId, ...] = ()


@dataclass(frozen=True)
class MapVersion:
    """An immutable, fully-committed shard map version."""

    version: int
    shards: Mapping[ShardId, ShardRecord]
    created_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        # Freeze the mapping so readers can never observe mutation.
        object.__setattr__(self, "shards", MappingProxyType(dict(self.shards)))

    def owner_of(self, shard_id: ShardId) -> NodeId:
        return self.shards[shard_id].node


@dataclass(frozen=True)
class Move:
    shard_id: ShardId
    src: NodeId
    dst: NodeId


@dataclass(frozen=True)
class SplitOp:
    parent: ShardId
    children: tuple[ShardId, ...]


@dataclass(frozen=True)
class MergeOp:
    children: tuple[ShardId, ...]
    target: ShardId


@dataclass(frozen=True)
class RebalancePlan:
    """A deterministic plan: same inputs always yield the same plan."""

    plan_id: str
    base_version: int
    moves: tuple[Move, ...] = ()
    splits: tuple[SplitOp, ...] = ()
    merges: tuple[MergeOp, ...] = ()
    # shard_id -> node for the final target assignment of every moved shard.
    target_assignment: Mapping[ShardId, NodeId] = field(default_factory=dict)


@dataclass
class ShardTask:
    """Progress record for one shard's migration inside a job."""

    shard_id: ShardId
    src: NodeId
    dst: NodeId
    state: TaskState = TaskState.PENDING
    bytes_total: int = 0
    bytes_done: int = 0
    error: str | None = None


@dataclass
class RebalanceJob:
    job_id: str
    plan: RebalancePlan
    state: JobState = JobState.RUNNING
    tasks: dict[ShardId, ShardTask] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)
    finished_at: float | None = None
