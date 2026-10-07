"""Req 4: online execution, progress, isolation between shards, cancel/timeout."""
import threading
import time

from shardmap.executor import DataPlane, MigrationExecutor
from shardmap.models import Move, RebalancePlan, RebalanceStatus


def test_progress_and_completion():
    dp = DataPlane()
    dp.seed("n1", "s0", [1, 2, 3, 4])
    ex = MigrationExecutor(dp, max_concurrent=2)
    plan = RebalancePlan("p", 1, (Move("s0", "n1", "n2"),))
    jid = ex.run_plan(plan, lambda m: None, lambda m: None)
    assert ex.wait(jid) == RebalanceStatus.COMPLETED
    status, tasks = ex.status(jid)
    assert tasks[0].progress == 1.0
    assert dp.read_owner_view("n2", "s0") == [1, 2, 3, 4]
    assert dp.read_owner_view("n1", "s0") == []


def test_slow_node_does_not_block_other_shards():
    dp = DataPlane()
    dp.seed("slow", "big", list(range(200)))
    dp.seed("n1", "small", [7])
    ex = MigrationExecutor(dp, max_concurrent=2)
    # simulate slowness by patching delay only for the big shard's copy
    orig_copy = dp.copy_shard

    def copy_shard(source, target, shard_id, **kw):
        if shard_id == "big":
            dp._item_delay = 0.01
            try:
                return orig_copy(source, target, shard_id, **kw)
            finally:
                dp._item_delay = 0.0
        return orig_copy(source, target, shard_id, **kw)

    dp.copy_shard = copy_shard
    plan = RebalancePlan("p", 1, (Move("big", "slow", "n2"),
                                  Move("small", "n1", "n3")))
    jid = ex.run_plan(plan, lambda m: None, lambda m: None)
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        _, tasks = ex.status(jid)
        small = next(t for t in tasks if t.shard_id == "small")
        if small.phase.value == "done":
            break
        time.sleep(0.01)
    assert small.phase.value == "done"  # finished while big still copying
    _, tasks = ex.status(jid)
    big = next(t for t in tasks if t.shard_id == "big")
    assert big.phase.value != "done"
    ex.cancel(jid)
    ex.wait(jid)


def test_cancel_leaves_no_partial_state():
    dp = DataPlane(item_delay=0.02)
    dp.seed("n1", "s0", list(range(100)))
    ex = MigrationExecutor(dp, max_concurrent=1)
    committed = []
    plan = RebalancePlan("p", 1, (Move("s0", "n1", "n2"),))
    jid = ex.run_plan(plan, committed.append, lambda m: None)
    time.sleep(0.1)  # mid-copy
    ex.cancel(jid)
    assert ex.wait(jid) == RebalanceStatus.CANCELLED
    assert not committed  # map never switched
    assert dp.read_owner_view("n2", "s0") == []      # partial copy discarded
    assert dp.read_owner_view("n1", "s0") == list(range(100))  # source intact


def test_timeout_aborts_shard_without_touching_map():
    dp = DataPlane(item_delay=0.05)
    dp.seed("n1", "s0", list(range(100)))
    ex = MigrationExecutor(dp, max_concurrent=1, per_shard_timeout=0.1)
    committed = []
    plan = RebalancePlan("p", 1, (Move("s0", "n1", "n2"),))
    jid = ex.run_plan(plan, committed.append, lambda m: None)
    assert ex.wait(jid) == RebalanceStatus.FAILED
    assert not committed
    assert dp.read_owner_view("n2", "s0") == []
    assert len(dp.read_owner_view("n1", "s0")) == 100
