"""Split/merge: ownership invariants, data preservation, routing."""
import pytest

from shardmap.coordinator import Coordinator
from shardmap.errors import RejectedError, RejectReason
from shardmap.models import ShardState
from shardmap.splitmerge import route


@pytest.fixture()
def coord(tmp_path):
    c = Coordinator(tmp_path / "d")
    c.bootstrap(["n1", "n2"], 4)
    return c


def seed_shard(coord, shard, keys=20):
    mv = coord.current_map()
    node = mv.owner_of(shard)
    for i in range(keys):
        coord.backend.write(node, shard, f"key-{i}", f"val-{i}".encode())


def all_keys(coord, shards, node):
    out = {}
    for s in shards:
        out.update(coord.backend.shard_items(node, s))
    return out


def test_split_preserves_all_data_no_duplication(coord):
    seed_shard(coord, "shard-0000", 50)
    before = coord.backend.shard_items("n1", "shard-0000")
    mv = coord.split("shard-0000", ("shard-0000-a", "shard-0000-b"),
                     "n2", "admin")
    # parent gone, children authoritative on n2
    assert "shard-0000" not in mv.shards
    assert mv.owner_of("shard-0000-a") == "n2"
    assert mv.owner_of("shard-0000-b") == "n2"
    after = all_keys(coord, ["shard-0000-a", "shard-0000-b"], "n2")
    assert after == before  # nothing lost, nothing duplicated


def test_merge_preserves_all_data(coord):
    seed_shard(coord, "shard-0000", 10)
    seed_shard(coord, "shard-0001", 10)
    before = all_keys(coord, ["shard-0000", "shard-0001"], "n1")
    mv = coord.merge(("shard-0000", "shard-0001"), "shard-00m", "n2", "admin")
    assert "shard-0000" not in mv.shards
    assert "shard-0001" not in mv.shards
    assert mv.owner_of("shard-00m") == "n2"
    assert coord.backend.shard_items("n2", "shard-00m") == before


def test_routing_during_split_points_to_parent(coord):
    seed_shard(coord, "shard-0000")
    mv = coord.current_map()
    from shardmap.splitmerge import mark_splitting, plan_split
    from shardmap.models import MapVersion
    op = plan_split(mv, "shard-0000", ("shard-0000-a", "shard-0000-b"))
    marked = MapVersion(version=mv.version + 1,
                        shards=mark_splitting(mv, op))
    # parent still authoritative; pending children route to parent
    assert route(marked, "shard-0000") == "shard-0000"
    assert route(marked, "shard-0000-a") == "shard-0000"
    assert marked.shards["shard-0000"].state is ShardState.SPLITTING


def test_single_authoritative_owner_at_all_times(coord):
    seed_shard(coord, "shard-0000")
    for v in range(1, coord.store.current_version + 1):
        mv = coord.store.get(v)
        owners = [sid for sid, r in mv.shards.items()
                  if r.state is ShardState.ACTIVE]
        assert len(owners) == len(set(owners))
    mv = coord.split("shard-0000", ("a1", "a2"), "n1", "admin")
    # after commit, exactly one record owns each keyspace id
    assert mv.shards["a1"].state is ShardState.ACTIVE
    assert mv.shards["a2"].state is ShardState.ACTIVE


def test_split_invalid_inputs_rejected(coord):
    with pytest.raises(RejectedError) as ei:
        coord.split("nope", ("x1", "x2"), "n1", "admin")
    assert ei.value.reason is RejectReason.NOT_FOUND
    with pytest.raises(RejectedError):
        coord.split("shard-0000", ("only",), "n1", "admin")
    with pytest.raises(RejectedError):
        coord.split("shard-0000", ("shard-0001", "x"), "n1", "admin")


def test_merge_invalid_inputs_rejected(coord):
    with pytest.raises(RejectedError):
        coord.merge(("shard-0000",), "t", "n1", "admin")
    with pytest.raises(RejectedError):
        coord.merge(("shard-0000", "missing"), "t", "n1", "admin")
    with pytest.raises(RejectedError):
        coord.merge(("shard-0000", "shard-0001"), "shard-0002", "n1", "admin")


def test_cannot_split_shard_not_active(coord):
    from shardmap.models import MapVersion
    from shardmap.splitmerge import mark_splitting, plan_split
    mv = coord.current_map()
    op = plan_split(mv, "shard-0000", ("s0a", "s0b"))
    marked = MapVersion(version=mv.version + 1, shards=mark_splitting(mv, op))
    with pytest.raises(RejectedError) as ei:
        plan_split(marked, "shard-0000", ("x", "y"))
    assert ei.value.reason is RejectReason.INVALID_STATE
    # after a real split commits, children are ACTIVE and can split again
    coord.split("shard-0000", ("s0a", "s0b"), "n1", "admin")
    mv = coord.split("s0a", ("s0a1", "s0a2"), "n1", "admin")
    assert mv.owner_of("s0a1") == "n1"
