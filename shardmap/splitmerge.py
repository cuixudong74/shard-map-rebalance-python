"""Shard split / merge planning, routing and version application.

Invariants:
- Before commit, the parent (split) or the children (merge) remain the
  authoritative owners; new requests are routed to them via ``route``.
- After commit, exactly one record owns each shard id: the children
  (split) or the target (merge). No shard id is ever owned by two
  authoritative records in the same committed version.
"""
from __future__ import annotations

from .errors import RejectedError, RejectReason
from .models import (
    MapVersion,
    MergeOp,
    ShardId,
    ShardRecord,
    ShardState,
    SplitOp,
)


def plan_split(mv: MapVersion, parent: ShardId,
               children: tuple[ShardId, ...]) -> SplitOp:
    rec = mv.shards.get(parent)
    if rec is None:
        raise RejectedError(RejectReason.NOT_FOUND, f"shard {parent} unknown")
    if rec.state is not ShardState.ACTIVE:
        raise RejectedError(RejectReason.INVALID_STATE,
                            f"shard {parent} is {rec.state.value}")
    if len(children) < 2 or len(set(children)) != len(children):
        raise RejectedError(RejectReason.INVALID_PLAN,
                            "split needs >= 2 distinct children")
    clash = [c for c in children if c in mv.shards]
    if clash:
        raise RejectedError(RejectReason.INVALID_PLAN,
                            f"children already exist: {clash}")
    return SplitOp(parent=parent, children=tuple(sorted(children)))


def plan_merge(mv: MapVersion, children: tuple[ShardId, ...],
               target: ShardId) -> MergeOp:
    if len(children) < 2 or len(set(children)) != len(children):
        raise RejectedError(RejectReason.INVALID_PLAN,
                            "merge needs >= 2 distinct children")
    for c in children:
        rec = mv.shards.get(c)
        if rec is None:
            raise RejectedError(RejectReason.NOT_FOUND, f"shard {c} unknown")
        if rec.state is not ShardState.ACTIVE:
            raise RejectedError(RejectReason.INVALID_STATE,
                                f"shard {c} is {rec.state.value}")
    if target in mv.shards:
        raise RejectedError(RejectReason.INVALID_PLAN,
                            f"target {target} already exists")
    return MergeOp(children=tuple(sorted(children)), target=target)


def route(mv: MapVersion, shard_id: ShardId) -> ShardId:
    """Resolve the authoritative shard for a request to ``shard_id``.

    - ACTIVE shard: itself.
    - SPLITTING parent: still the parent (children not yet authoritative).
    - MERGING children: still the children; a request naming the merge
      target is routed to the smallest still-authoritative child id
      (deterministic policy, documented in README).
    - Unknown id that is a pending split child: routed to its parent.
    """
    rec = mv.shards.get(shard_id)
    if rec is not None:
        if rec.state in (ShardState.ACTIVE, ShardState.SPLITTING,
                         ShardState.MERGING, ShardState.MIGRATING):
            return rec.shard_id
        return rec.shard_id
    # Pending split child -> parent.
    for cand in sorted(mv.shards):
        r = mv.shards[cand]
        if r.state is ShardState.SPLITTING and shard_id in r.children:
            return r.shard_id
    # Pending merge target -> smallest merging child (deterministic).
    merging = sorted(
        r.shard_id for r in mv.shards.values()
        if r.state is ShardState.MERGING
    )
    if merging:
        return merging[0]
    raise RejectedError(RejectReason.NOT_FOUND, f"shard {shard_id} unknown")


def mark_splitting(mv: MapVersion, op: SplitOp) -> dict[ShardId, ShardRecord]:
    """New shard table with the parent marked SPLITTING (pre-commit)."""
    shards = dict(mv.shards)
    rec = shards[op.parent]
    shards[op.parent] = ShardRecord(
        shard_id=rec.shard_id, node=rec.node, state=ShardState.SPLITTING,
        parent=rec.parent, children=op.children)
    return shards


def apply_split(mv: MapVersion, op: SplitOp, node: str) -> dict[ShardId, ShardRecord]:
    """New shard table after a split commits: children own the data."""
    shards = dict(mv.shards)
    del shards[op.parent]
    for c in op.children:
        shards[c] = ShardRecord(shard_id=c, node=node,
                                state=ShardState.ACTIVE, parent=op.parent)
    return shards


def mark_merging(mv: MapVersion, op: MergeOp) -> dict[ShardId, ShardRecord]:
    shards = dict(mv.shards)
    for c in op.children:
        rec = shards[c]
        shards[c] = ShardRecord(
            shard_id=rec.shard_id, node=rec.node, state=ShardState.MERGING,
            parent=rec.parent, children=rec.children)
    return shards


def apply_merge(mv: MapVersion, op: MergeOp, node: str) -> dict[ShardId, ShardRecord]:
    shards = dict(mv.shards)
    for c in op.children:
        del shards[c]
    shards[op.target] = ShardRecord(shard_id=op.target, node=node,
                                    state=ShardState.ACTIVE,
                                    parent=None,
                                    children=tuple(sorted(op.children)))
    return shards
