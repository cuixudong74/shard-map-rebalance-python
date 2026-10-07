"""Req 1: versioned map, readers see only complete committed versions."""
import threading

from shardmap.models import HASH_SPACE, Shard
from shardmap.store import VersionedStore


def make_shards(n=4):
    step = HASH_SPACE // n
    return [Shard(f"s{i}", i * step, HASH_SPACE if i == n - 1 else (i + 1) * step)
            for i in range(n)]


def test_readers_never_see_mixed_assignment():
    store = VersionedStore(max_versions=8)
    shards = make_shards()
    store.commit(shards, {s.shard_id: "n1" for s in shards})
    errors = []

    def writer():
        for i in range(200):
            # each commit flips ALL shards atomically to one node
            node = "n2" if i % 2 else "n1"
            store.commit(shards, {s.shard_id: node for s in shards})

    def reader():
        for _ in range(500):
            snap = store.acquire()
            try:
                owners = set(snap.assignment.values())
                if len(owners) != 1:  # mixed old/new ownership = broken
                    errors.append(snap.assignment)
            finally:
                store.release(snap.version)

    threads = [threading.Thread(target=writer)] + [
        threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors


def test_needs_refresh():
    store = VersionedStore()
    shards = make_shards()
    v1 = store.commit(shards, {s.shard_id: "n1" for s in shards})
    assert not store.needs_refresh(v1.version)
    store.commit(shards, {s.shard_id: "n2" for s in shards})
    assert store.needs_refresh(v1.version)
    assert not store.needs_refresh(store.latest_version)
