"""Crash/restart recovery: journal + snapshot leave a self-consistent map."""
import time

import pytest

from shardmap.backend import InMemoryBackend
from shardmap.coordinator import Coordinator
from shardmap.models import JobState, ShardState


def wait_for(cond, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.02)
    return False


def seed(coord, keys=4):
    mv = coord.current_map()
    for sid, rec in mv.shards.items():
        for i in range(keys):
            coord.backend.write(rec.node, sid, f"k{i}", f"v-{sid}-{i}".encode())


def test_unfinished_rebalance_identified_and_aborted(tmp_path):
    d = tmp_path / "d"
    backend = InMemoryBackend()
    a = Coordinator(d, shard_timeout_s=60.0, node_delays={"n1": 30.0},
                    backend=backend)
    a.bootstrap(["n1", "n2"], 6)
    seed(a)
    a.start_rebalance(["n1", "n2", "n3"], caller="admin")
    # wait until at least one shard committed, then "crash" (abandon a)
    assert wait_for(lambda: any(
        r.get("type") == "shard_committed" for r in a.journal.read_all()))
    del a

    b = Coordinator(d, backend=backend)  # restart: snapshot + journal
    result = b.recover()
    assert any(a_.startswith("job:") and a_.endswith("aborted")
               for a_ in result["actions"])
    mv = b.current_map()
    assert len(mv.shards) == 6  # self-consistent, no half-migrated state
    for sid, rec in mv.shards.items():
        assert rec.state is ShardState.ACTIVE
    # recovery is idempotent
    assert b.recover()["actions"] == []


def test_unfinished_rebalance_resumed_by_policy(tmp_path):
    d = tmp_path / "d"
    backend = InMemoryBackend()
    a = Coordinator(d, shard_timeout_s=60.0, node_delays={"n1": 30.0},
                    backend=backend)
    a.bootstrap(["n1", "n2"], 6)
    seed(a)
    job = a.start_rebalance(["n1", "n2", "n3"], caller="admin")
    assert wait_for(lambda: any(
        r.get("type") == "shard_committed" for r in a.journal.read_all()))
    total_moves = len(job.plan.moves)
    del a

    b = Coordinator(d, recovery_policy="resume", backend=backend)
    result = b.recover()
    assert any(a_.endswith("running") or "job:" in a_
               for a_ in result["actions"])
    b.executor.wait(10)
    prog = b.executor.progress()
    assert prog.state is JobState.COMMITTED
    # remaining moves were all completed
    done = sum(1 for t in prog.tasks.values() if t.state.value == "done")
    assert done == len(prog.tasks)
    assert done <= total_moves
    mv = b.current_map()
    for sid, rec in mv.shards.items():
        assert len(b.backend.shard_items(rec.node, sid)) == 4


def test_interrupted_split_rolled_back(tmp_path):
    d = tmp_path / "d"
    backend = InMemoryBackend()
    a = Coordinator(d, backend=backend)
    a.bootstrap(["n1", "n2"], 4)
    seed(a)
    # crash the split after the SPLITTING version is committed
    orig = a.backend.write

    def boom(*args, **kwargs):
        raise RuntimeError("simulated crash")
    a.backend.write = boom
    with pytest.raises(RuntimeError):
        a.split("shard-0000", ("s0a", "s0b"), "n2", "admin")
    a.backend.write = orig
    assert a.current_map().shards["shard-0000"].state is ShardState.SPLITTING
    del a

    b = Coordinator(d, backend=backend)
    result = b.recover()
    assert "split:shard-0000:aborted" in result["actions"]
    mv = b.current_map()
    # parent is authoritative again, children never appeared
    assert mv.shards["shard-0000"].state is ShardState.ACTIVE
    assert "s0a" not in mv.shards and "s0b" not in mv.shards
    # data intact and readable via routing
    version, shard, node = b.route_request("shard-0000")
    assert shard == "shard-0000"
    assert len(b.backend.shard_items(node, shard)) == 4


def test_interrupted_merge_rolled_back(tmp_path):
    d = tmp_path / "d"
    backend = InMemoryBackend()
    a = Coordinator(d, backend=backend)
    a.bootstrap(["n1", "n2"], 4)
    seed(a)
    orig = a.backend.merge_shards

    def boom(*args, **kwargs):
        raise RuntimeError("simulated crash")
    a.backend.merge_shards = boom
    with pytest.raises(RuntimeError):
        a.merge(("shard-0000", "shard-0001"), "sm", "n2", "admin")
    a.backend.merge_shards = orig
    assert a.current_map().shards["shard-0000"].state is ShardState.MERGING
    del a

    b = Coordinator(d, backend=backend)
    result = b.recover()
    assert "merge:sm:aborted" in result["actions"]
    mv = b.current_map()
    assert mv.shards["shard-0000"].state is ShardState.ACTIVE
    assert mv.shards["shard-0001"].state is ShardState.ACTIVE
    assert "sm" not in mv.shards
    for sid in ("shard-0000", "shard-0001"):
        node = mv.owner_of(sid)
        assert len(b.backend.shard_items(node, sid)) == 4


def test_snapshot_roundtrip(tmp_path):
    d = tmp_path / "d"
    a = Coordinator(d)
    a.bootstrap(["n1", "n2"], 4)
    a.split("shard-0000", ("s0a", "s0b"), "n1", "admin")
    v = a.store.current_version
    del a
    b = Coordinator(d)
    assert b.store.current_version == v
    assert b.current_map().owner_of("s0a") == "n1"
