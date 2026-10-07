"""Online rebalance: progress, cancel, timeout, slow-node isolation."""
import threading
import time

import pytest

from shardmap.coordinator import Coordinator
from shardmap.errors import RejectedError, RejectReason
from shardmap.models import JobState, TaskState


@pytest.fixture()
def coord(tmp_path):
    c = Coordinator(tmp_path / "d", max_parallel_shards=4, shard_timeout_s=0.5)
    c.bootstrap(["n1", "n2"], 8)
    return c


def seed(coord, shards=None, keys=5):
    mv = coord.current_map()
    for sid, rec in mv.shards.items():
        if shards and sid not in shards:
            continue
        for i in range(keys):
            coord.backend.write(rec.node, sid, f"k{i}", f"v-{sid}-{i}".encode())


def test_rebalance_commits_and_data_follows(coord):
    seed(coord)
    job = coord.start_rebalance(["n1", "n2", "n3"], caller="admin")
    coord.executor.wait(10)
    prog = coord.executor.progress()
    assert prog.state is JobState.COMMITTED
    assert all(t.state is TaskState.DONE for t in prog.tasks.values())
    mv = coord.current_map()
    # every moved shard's data is readable at the new owner
    for m in job.plan.moves:
        items = coord.backend.shard_items(mv.owner_of(m.shard_id), m.shard_id)
        assert len(items) == 5


def test_reads_uninterrupted_and_never_partial(coord):
    seed(coord)
    coord.executor._node_delays["n1"] = 0.05  # slow down copies
    job = coord.start_rebalance(["n1", "n2", "n3"], caller="admin")
    errors = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            version, shard, node = coord.route_request("shard-0000")
            mv = coord.store.get(version)
            if mv is None or mv.owner_of(shard) != node:
                errors.append(("inconsistent", version, shard, node))
            # data must be fully present at the authoritative node
            items = coord.backend.shard_items(node, shard)
            if items and len(items) != 5:
                errors.append(("partial", version, len(items)))

    threads = [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    coord.executor.wait(10)
    stop.set()
    for t in threads:
        t.join()
    assert not errors


def test_slow_node_does_not_block_other_shards(tmp_path):
    c = Coordinator(tmp_path / "d", max_parallel_shards=4,
                    shard_timeout_s=5.0, node_delays={"n1": 1.0})
    c.bootstrap(["n1", "n2"], 8)
    seed(c)
    # 8 shards over 5 nodes -> both n1 (slow) and n2 (fast) must give some up.
    c.start_rebalance(["n1", "n2", "n3", "n4", "n5"], caller="admin")
    deadline = time.time() + 2.0
    done_on_fast = 0
    while time.time() < deadline:
        prog = c.executor.progress()
        done_on_fast = sum(
            1 for t in prog.tasks.values()
            if t.state is TaskState.DONE and t.src == "n2")
        if done_on_fast:
            break
        time.sleep(0.02)
    assert done_on_fast > 0  # n2's shards finished while n1 was still slow
    c.executor.wait(10)


def test_cancel_leaves_consistent_map(coord):
    seed(coord)
    coord.executor._node_delays["n2"] = 0.2
    coord.start_rebalance(["n1", "n2", "n3"], caller="admin")
    coord.executor.cancel("admin")
    coord.executor.wait(10)
    prog = coord.executor.progress()
    assert prog.state is JobState.ABORTED
    mv = coord.current_map()
    # map is self-consistent: each shard has exactly one authoritative owner
    assert len(mv.shards) == 8
    # every shard's data is fully available at its authoritative owner
    for sid, rec in mv.shards.items():
        items = coord.backend.shard_items(rec.node, sid)
        assert len(items) == 5, (sid, rec.node, len(items))


def test_timeout_marks_task_failed_map_stays_consistent(tmp_path):
    c = Coordinator(tmp_path / "d", max_parallel_shards=2,
                    shard_timeout_s=0.1, node_delays={"n1": 2.0})
    c.bootstrap(["n1", "n2"], 6)
    seed(c)
    c.start_rebalance(["n2", "n3"], caller="admin")
    c.executor.wait(15)
    prog = c.executor.progress()
    assert prog.state is JobState.ABORTED
    failed = [t for t in prog.tasks.values() if t.state is TaskState.FAILED]
    assert failed and all("timeout" in t.error for t in failed)
    mv = c.current_map()
    assert len(mv.shards) == 6
    for sid, rec in mv.shards.items():
        assert len(c.backend.shard_items(rec.node, sid)) == 5


def test_concurrent_rebalance_rejected(coord):
    coord.executor._node_delays["n1"] = 0.3
    coord.start_rebalance(["n1", "n2", "n3"], caller="admin")
    try:
        with pytest.raises(RejectedError) as ei:
            coord.start_rebalance(["n1", "n2"], caller="admin")
        assert ei.value.reason is RejectReason.REBALANCE_IN_PROGRESS
    finally:
        coord.executor.cancel("admin")
        coord.executor.wait(10)


def test_stale_plan_rejected(coord):
    plan = coord.plan_rebalance(["n1", "n2", "n3"])
    coord.bootstrap  # noqa: B018 - map already bootstrapped
    # advance the map so the plan's base version is stale
    coord.split("shard-0000", ("shard-0000-a", "shard-0000-b"), "n1", "admin")
    with pytest.raises(RejectedError) as ei:
        coord.executor.submit(plan, "admin")
    assert ei.value.reason is RejectReason.VERSION_CONFLICT
