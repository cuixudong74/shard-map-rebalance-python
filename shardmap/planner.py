"""Deterministic minimal-move rebalance planner."""
from __future__ import annotations

from .models import ErrorCode, Move, RebalancePlan, ShardMapError, ShardMapVersion


def compute_plan(base: ShardMapVersion, target_nodes: list[str],
                 plan_id: str) -> RebalancePlan:
    """Compute a deterministic, minimal-move plan that spreads the shards of
    `base` evenly over `target_nodes`.

    - Minimal: a shard already on a surviving node is kept unless the node is
      over its fair share; moves = sum(max(0, load-capacity)) + orphaned.
    - Deterministic: every iteration is over sorted shard ids / sorted node
      ids, so identical inputs always produce identical plans regardless of
      dict or set traversal order.
    """
    targets = sorted(set(target_nodes))
    if not targets:
        raise ShardMapError(ErrorCode.INVALID, "target node set is empty")
    shards = sorted(base.shards, key=lambda s: s.shard_id)
    n, k = len(shards), len(targets)
    base_share, extra = divmod(n, k)
    capacity = {node: base_share + (1 if i < extra else 0)
                for i, node in enumerate(targets)}

    load = {node: 0 for node in targets}
    free: list[str] = []
    for shard in shards:
        owner = base.assignment[shard.shard_id]
        if owner in capacity and load[owner] < capacity[owner]:
            load[owner] += 1  # keep in place: no data movement
        else:
            free.append(shard.shard_id)

    moves: list[Move] = []
    for sid in free:  # free is already in sorted order
        for node in targets:  # sorted: deterministic fill order
            if load[node] < capacity[node]:
                load[node] += 1
                moves.append(Move(sid, base.assignment[sid], node))
                break
        else:  # pragma: no cover - capacities sum to n, unreachable
            raise ShardMapError(ErrorCode.INVALID, "no capacity left")
    return RebalancePlan(plan_id=plan_id, base_version=base.version,
                         moves=tuple(moves))
