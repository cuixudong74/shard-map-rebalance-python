"""Req 3: split/merge keep coverage complete, ownership never lost/duplicated."""
import pytest

from shardmap.models import HASH_SPACE, ErrorCode, Shard, ShardMapError
from shardmap.splitmerge import merge_shards, split_shard
from shardmap.store import VersionedStore


def make_base():
    step = HASH_SPACE // 4
    shards = [Shard(f"s{i}", i * step,
                    HASH_SPACE if i == 3 else (i + 1) * step) for i in range(4)]
    store = VersionedStore()
    store.commit(shards, {s.shard_id: f"n{i % 2 + 1}" for i, s in enumerate(shards)})
    return store


def assert_tiling(shards):
    ordered = sorted(shards, key=lambda s: s.start)
    assert ordered[0].start == 0 and ordered[-1].end == HASH_SPACE
    for a, b in zip(ordered, ordered[1:]):
        assert a.end == b.start  # no gap, no overlap


def test_split_preserves_coverage_and_ownership():
    store = make_base()
    base = store.current()
    shards, assignment = split_shard(base, "s0", "s0a", "s0b")
    assert_tiling(shards)
    # children inherit parent owner: no slot loses or duplicates its owner
    assert assignment["s0a"] == assignment["s0b"] == base.assignment["s0"]
    assert "s0" not in assignment
    snap = store.commit(shards, assignment)  # store re-validates tiling
    assert snap.shard_for_slot(1).shard_id == "s0a"


def test_merge_preserves_coverage_and_ownership():
    store = make_base()
    base = store.current()
    shards, assignment = split_shard(base, "s0", "s0a", "s0b")
    store.commit(shards, assignment)
    shards, assignment = merge_shards(store.current(), "s0a", "s0b", "s0")
    assert_tiling(shards)
    assert assignment["s0"] == base.assignment["s0"]


def test_merge_rejects_non_adjacent_and_cross_owner():
    store = make_base()
    base = store.current()
    with pytest.raises(ShardMapError) as e1:
        merge_shards(base, "s0", "s2", "x")
    assert e1.value.code is ErrorCode.INVALID
    with pytest.raises(ShardMapError) as e2:  # s0 on n1, s1 on n2
        merge_shards(base, "s0", "s1", "x")
    assert e2.value.code is ErrorCode.INVALID


def test_split_rejects_duplicate_ids():
    store = make_base()
    with pytest.raises(ShardMapError) as e:
        split_shard(store.current(), "s0", "s1", "s1b")
    assert e.value.code is ErrorCode.CONFLICT
