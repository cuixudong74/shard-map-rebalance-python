"""Shard split and merge: pure functions producing a new shard set and
assignment. Coverage invariants (full tiling, no gaps/overlaps) are checked
again at commit time by VersionedStore."""
from __future__ import annotations

from .models import ErrorCode, Shard, ShardMapError, ShardMapVersion


def _find(base: ShardMapVersion, shard_id: str) -> Shard:
    for s in base.shards:
        if s.shard_id == shard_id:
            return s
    raise ShardMapError(ErrorCode.NOT_FOUND, f"unknown shard {shard_id}")


def split_shard(base: ShardMapVersion, shard_id: str,
                new_id_a: str, new_id_b: str
                ) -> tuple[tuple[Shard, ...], dict[str, str]]:
    """Split `shard_id` at its range midpoint. Both children inherit the
    parent's owner, so every slot keeps exactly one authoritative owner and
    no data moves during the split itself."""
    target = _find(base, shard_id)
    existing = {s.shard_id for s in base.shards}
    for nid in (new_id_a, new_id_b):
        if nid in existing:
            raise ShardMapError(ErrorCode.CONFLICT, f"shard id {nid} exists")
    if target.end - target.start < 2:
        raise ShardMapError(ErrorCode.INVALID, "shard too small to split")
    mid = (target.start + target.end) // 2
    owner = base.assignment[shard_id]
    shards = tuple(s for s in base.shards if s.shard_id != shard_id) + (
        Shard(new_id_a, target.start, mid),
        Shard(new_id_b, mid, target.end),
    )
    assignment = dict(base.assignment)
    del assignment[shard_id]
    assignment[new_id_a] = owner
    assignment[new_id_b] = owner
    return shards, assignment


def merge_shards(base: ShardMapVersion, shard_id_a: str, shard_id_b: str,
                 new_id: str) -> tuple[tuple[Shard, ...], dict[str, str]]:
    """Merge two adjacent shards into one. Both must currently share the
    same owner (rebalance first if not), so the merged shard has a single
    authoritative owner and no slot loses or duplicates its mapping."""
    a, b = _find(base, shard_id_a), _find(base, shard_id_b)
    if a.shard_id == b.shard_id:
        raise ShardMapError(ErrorCode.INVALID, "cannot merge a shard with itself")
    if new_id in {s.shard_id for s in base.shards} - {a.shard_id, b.shard_id}:
        raise ShardMapError(ErrorCode.CONFLICT, f"shard id {new_id} exists")
    if a.end == b.start:
        lo, hi = a, b
    elif b.end == a.start:
        lo, hi = b, a
    else:
        raise ShardMapError(ErrorCode.INVALID, "shards are not adjacent")
    owner_a, owner_b = base.assignment[a.shard_id], base.assignment[b.shard_id]
    if owner_a != owner_b:
        raise ShardMapError(ErrorCode.INVALID,
                            "shards have different owners; rebalance first")
    shards = tuple(s for s in base.shards
                   if s.shard_id not in (a.shard_id, b.shard_id)) + (
        Shard(new_id, lo.start, hi.end),)
    assignment = dict(base.assignment)
    del assignment[a.shard_id]
    del assignment[b.shard_id]
    assignment[new_id] = owner_a
    return shards, assignment
