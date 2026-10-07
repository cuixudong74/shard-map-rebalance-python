"""Deterministic, minimal-move rebalance planner."""
from __future__ import annotations

import hashlib

from .errors import RejectedError, RejectReason
from .models import MapVersion, Move, NodeId, RebalancePlan


def _fingerprint(base: MapVersion, target_nodes: list[NodeId]) -> str:
    h = hashlib.sha256()
    h.update(str(base.version).encode())
    for n in sorted(target_nodes):
        h.update(b"\x00")
        h.update(n.encode())
    return h.hexdigest()[:16]


def compute_plan(base: MapVersion, target_nodes: list[NodeId],
                 plan_id: str | None = None) -> RebalancePlan:
    """Compute a deterministic minimal-move plan.

    Guarantees:
    - Same ``base`` and same ``target_nodes`` always yield the same plan,
      regardless of dict/set iteration order (everything is sorted first).
    - Minimal moves: shards already on a node that stays in the cluster are
      only moved when required to balance counts; each shard moves at most
      once and never to a node already at its target count.
    """
    nodes = sorted(set(target_nodes))
    if not nodes:
        raise RejectedError(RejectReason.INVALID_PLAN, "no target nodes")
    shards = sorted(base.shards)
    if not shards:
        raise RejectedError(RejectReason.INVALID_PLAN, "map has no shards")

    total = len(shards)
    per, extra = divmod(total, len(nodes))

    # Current load per surviving node (nodes not in `nodes` contribute 0).
    current: dict[NodeId, int] = {n: 0 for n in nodes}
    for s in shards:
        owner = base.shards[s].node
        if owner in current:
            current[owner] += 1

    # Decide which nodes get `per + 1` shards: prefer the nodes that already
    # hold the most shards (minimises moves); break ties by node id so the
    # choice is deterministic.
    ranked = sorted(nodes, key=lambda n: (-current[n], n))
    target_count = {n: per for n in nodes}
    for n in ranked[:extra]:
        target_count[n] += 1

    # Pool of shards that must move: everything on removed nodes, plus
    # shards evicted from overloaded surviving nodes. Evict highest shard
    # ids first (deterministic; keeps low ids stable).
    pool: list[str] = []
    kept: dict[NodeId, list[str]] = {n: [] for n in nodes}
    for s in shards:
        owner = base.shards[s].node
        if owner not in kept:
            pool.append(s)
        else:
            kept[owner].append(s)
    for n in nodes:
        excess = len(kept[n]) - target_count[n]
        if excess > 0:
            kept[n].sort()
            pool.extend(kept[n][-excess:])
            del kept[n][-excess:]

    # Fill deficient nodes in sorted-node order with sorted pooled shards.
    pool.sort()
    moves: list[Move] = []
    assignment: dict[str, NodeId] = {}
    idx = 0
    for n in nodes:
        deficit = target_count[n] - len(kept[n])
        for _ in range(deficit):
            s = pool[idx]
            idx += 1
            moves.append(Move(shard_id=s, src=base.shards[s].node, dst=n))
            assignment[s] = n
    assert idx == len(pool), "planner internal imbalance"

    pid = plan_id or f"plan-v{base.version}-{_fingerprint(base, nodes)}"
    return RebalancePlan(
        plan_id=pid,
        base_version=base.version,
        moves=tuple(moves),
        target_assignment=assignment,
    )
