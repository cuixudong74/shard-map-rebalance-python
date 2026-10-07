"""Planner: determinism, minimal moves, balanced results."""
from collections import Counter

import pytest

from shardmap.errors import RejectedError
from shardmap.models import MapVersion, ShardRecord
from shardmap.planner import compute_plan


def make_map(shards_per_node):
    shards = {}
    i = 0
    for node, count in shards_per_node.items():
        for _ in range(count):
            sid = f"shard-{i:04d}"
            shards[sid] = ShardRecord(shard_id=sid, node=node)
            i += 1
    return MapVersion(version=1, shards=shards)


def test_deterministic_regardless_of_input_order():
    mv = make_map({"n1": 4, "n2": 4})
    p1 = compute_plan(mv, ["n1", "n2", "n3"])
    p2 = compute_plan(mv, ["n3", "n2", "n1"])
    p3 = compute_plan(mv, list({"n2", "n3", "n1"}))
    assert p1.moves == p2.moves == p3.moves
    assert p1.plan_id == p2.plan_id == p3.plan_id


def test_minimal_moves_on_node_add():
    mv = make_map({"n1": 5, "n2": 5})
    plan = compute_plan(mv, ["n1", "n2", "n3"])
    # 10 shards / 3 nodes -> 4,3,3; at most 4 shards must move.
    assert len(plan.moves) <= 4
    final = Counter()
    moved = {m.shard_id: m.dst for m in plan.moves}
    for sid, rec in mv.shards.items():
        final[moved.get(sid, rec.node)] += 1
    assert max(final.values()) - min(final.values()) <= 1


def test_node_removal_moves_all_its_shards():
    mv = make_map({"n1": 3, "n2": 3, "n3": 2})
    plan = compute_plan(mv, ["n1", "n2"])
    moved_srcs = {m.src for m in plan.moves}
    assert "n3" in moved_srcs
    assert len(plan.moves) == 2  # exactly n3's shards, already balanced else
    for m in plan.moves:
        assert m.dst in ("n1", "n2")


def test_no_moves_when_already_balanced():
    mv = make_map({"n1": 4, "n2": 4})
    plan = compute_plan(mv, ["n1", "n2"])
    assert plan.moves == ()


def test_every_shard_moves_at_most_once():
    mv = make_map({"n1": 9, "n2": 1})
    plan = compute_plan(mv, ["n1", "n2", "n3", "n4"])
    ids = [m.shard_id for m in plan.moves]
    assert len(ids) == len(set(ids))


def test_invalid_inputs_rejected():
    mv = make_map({"n1": 2})
    with pytest.raises(RejectedError):
        compute_plan(mv, [])
    with pytest.raises(RejectedError):
        compute_plan(MapVersion(version=1, shards={}), ["n1"])
