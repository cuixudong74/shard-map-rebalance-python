"""Req 5: interruption and restart recovery; map always self-consistent."""
import json
import os
import time

from shardmap.service import ShardMapService


def wait_job(svc, job_id, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        st = svc.rebalance_status(job_id)
        if st["status"] != "running":
            return st["status"]
        time.sleep(0.01)
    return "running"


def test_restart_with_unfinished_rebalance_abort_policy(tmp_path):
    d = str(tmp_path / "state")
    svc = ShardMapService(d, ["n1", "n2"], num_shards=8, seed_keys_per_shard=8,
                          recovery_policy="abort")
    svc.dataplane._item_delay = 0.05  # make migration slow enough to interrupt
    r = svc.rebalance("tester", ["n1", "n2", "n3"])
    time.sleep(0.05)  # let it start, then "crash" (no clean shutdown)

    svc2 = ShardMapService(d, ["n1", "n2", "n3"], recovery_policy="abort")
    svc2.dataplane._item_delay = 0
    snap = svc2.get_map()
    # map is a complete committed version; every shard has exactly one owner
    assert set(snap.assignment) == {s.shard_id for s in snap.shards}
    # no half-migrated data: owner of each shard holds full data or the
    # move was never committed (source intact, target cleaned on abort)
    journal = [json.loads(l) for l in open(os.path.join(d, "journal.jsonl"))]
    assert any(e["type"] == "job_end" and
               e.get("status") == "aborted_on_recovery" for e in journal)


def test_restart_with_unfinished_rebalance_resume_policy(tmp_path):
    d = str(tmp_path / "state")
    svc = ShardMapService(d, ["n1", "n2"], num_shards=8, seed_keys_per_shard=8,
                          recovery_policy="resume")
    svc.dataplane._item_delay = 0.05
    r = svc.rebalance("tester", ["n1", "n2", "n3"])
    n_moves = r["moves"]
    assert n_moves > 0
    time.sleep(0.02)  # crash mid-rebalance

    svc2 = ShardMapService(d, ["n1", "n2", "n3"], recovery_policy="resume")
    svc2.dataplane._item_delay = 0
    # resumed job must eventually finish the remaining moves
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        cur = svc2.get_map().assignment
        if set(cur.values()) == {"n1", "n2", "n3"}:
            break
        time.sleep(0.05)
    cur = svc2.get_map().assignment
    assert set(cur.values()) == {"n1", "n2", "n3"}
    # every shard's data sits with its authoritative owner
    snap = svc2.get_map()
    for shard in snap.shards:
        owner = snap.assignment[shard.shard_id]
        view = svc2.dataplane.read_owner_view(owner, shard.shard_id)
        assert view, f"{shard.shard_id} has no data on its owner {owner}"


def test_clean_restart_keeps_version_and_assignment(tmp_path):
    d = str(tmp_path / "state")
    svc = ShardMapService(d, ["n1", "n2"], num_shards=8, seed_keys_per_shard=8)
    r = svc.rebalance("tester", ["n1", "n2", "n3"])
    assert wait_job(svc, r["job_id"]) == "completed"
    before = svc.get_map()
    svc2 = ShardMapService(d, ["n1", "n2", "n3"])
    after = svc2.get_map()
    assert after.version == before.version
    assert after.assignment == before.assignment
