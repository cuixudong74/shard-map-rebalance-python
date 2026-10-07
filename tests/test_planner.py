"""Req 2: deterministic, minimal-move rebalance plans."""
import itertools

from shardmap.models import HASH_SPACE, Shard, ShardMapVersion
from shardmap.planner import compute_plan


def make_base(n_shards=12, nodes=("n1", "n2")):
    step = HASH_SPACE // n_shards
    shards = tuple(
        Shard(f"s{i}", i * step,
              HASH_SPACE if i == n_shards - 1 else (i + 1) * step)
        for i in range(n_shards))
    assignment = {s.shard_id: nodes[i % len(nodes)]
                  for i, s in enumerate(shards)}
    return ShardMapVersion(version=1, shards=shards, assignment=assignment)


def test_plan_is_deterministic_regardless_of_input_order():
    base = make_base()
    plans = []
    for perm in itertools.permutations(["n1", "n2", "n3"]):
        plans.append(compute_plan(base, list(perm), "p").moves)
    assert all(p == plans[0] for p in plans)


def test_node_add_moves_minimum():
    base = make_base(12, ("n1", "n2"))  # 6 + 6
    plan = compute_plan(base, ["n1", "n2", "n3"], "p")
    # 12 shards / 3 nodes = 4 each -> exactly 2 shards leave each old node
    assert len(plan.moves) == 4
    assert all(m.target == "n3" for m in plan.moves)


def test_node_remove_only_orphans_move():
    base = make_base(12, ("n1", "n2", "n3"))  # 4 each
    plan = compute_plan(base, ["n1", "n2"], "p")
    assert len(plan.moves) == 4  # only the shards that were on n3
    assert all(m.source == "n3" for m in plan.moves)


def test_same_input_same_plan():
    base = make_base()
    a = compute_plan(base, ["n2", "n3", "n1"], "p")
    b = compute_plan(base, ["n2", "n3", "n1"], "p")
    assert a == b


def test_empty_target_rejected():
    import pytest
    from shardmap.models import ShardMapError
    with pytest.raises(ShardMapError):
        compute_plan(make_base(), [], "p")
