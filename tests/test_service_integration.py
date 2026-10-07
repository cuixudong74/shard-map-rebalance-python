"""Req 1+4: during online migration readers are never interrupted and never
routed to a shard copy that has not finished migrating."""
import threading
import time

from shardmap.service import ShardMapService


def test_reads_during_rebalance_always_hit_complete_owner_data(tmp_path):
    svc = ShardMapService(str(tmp_path / "s"), ["n1", "n2"], num_shards=4,
                          seed_keys_per_shard=32)
    svc.dataplane._item_delay = 0.005  # slow copies so reads overlap migration
    base = svc.get_map()
    expected = {s.shard_id: sorted(range(s.start, s.end,
                                         max(1, (s.end - s.start) // 32)))
                for s in base.shards}
    svc.release_map(base.version)
    violations = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            snap = svc.get_map("reader")
            try:
                for shard in snap.shards:
                    owner = snap.assignment[shard.shard_id]
                    view = svc.dataplane.read_owner_view(owner, shard.shard_id)
                    # the authoritative owner must hold the shard's full data
                    if view != expected[shard.shard_id]:
                        violations.append((snap.version, shard.shard_id))
            finally:
                svc.release_map(snap.version)

    readers = [threading.Thread(target=reader) for _ in range(4)]
    for t in readers:
        t.start()
    r = svc.rebalance("tester", ["n1", "n2", "n3"])
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        if svc.rebalance_status(r["job_id"])["status"] != "running":
            break
        time.sleep(0.02)
    stop.set()
    for t in readers:
        t.join()
    assert svc.rebalance_status(r["job_id"])["status"] == "completed"
    assert not violations


def test_concurrent_split_and_reads_keep_single_authority(tmp_path):
    svc = ShardMapService(str(tmp_path / "s"), ["n1", "n2"], num_shards=4,
                          seed_keys_per_shard=8)
    violations = []
    stop = threading.Event()

    def reader():
        while not stop.is_set():
            snap = svc.get_map("reader")
            try:
                ids = [s.shard_id for s in snap.shards]
                if len(ids) != len(set(ids)):  # duplicate authority
                    violations.append(ids)
                if set(snap.assignment) != set(ids):  # dangling mapping
                    violations.append(snap.assignment)
            finally:
                svc.release_map(snap.version)

    readers = [threading.Thread(target=reader) for _ in range(3)]
    for t in readers:
        t.start()
    for i in range(4):
        v = svc.get_map().version
        svc.split("tester", "shard-0000")
        svc.merge("tester", "shard-0000-a", "shard-0000-b",
                  new_id="shard-0000")
    stop.set()
    for t in readers:
        t.join()
    assert not violations
