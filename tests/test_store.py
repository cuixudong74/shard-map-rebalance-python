"""Version semantics: atomic commits, reader pinning, bounded retention."""
import threading

import pytest

from shardmap.errors import RejectedError, RejectReason
from shardmap.models import MapVersion, ShardRecord
from shardmap.store import VersionedMapStore


def make_map(version, n_shards=4, node="n1"):
    return MapVersion(version=version, shards={
        f"s{i}": ShardRecord(shard_id=f"s{i}", node=node)
        for i in range(n_shards)
    })


def test_commit_must_be_sequential():
    store = VersionedMapStore()
    store.commit(make_map(1))
    with pytest.raises(RejectedError) as ei:
        store.commit(make_map(3))
    assert ei.value.reason is RejectReason.VERSION_CONFLICT


def test_readers_never_see_mixed_ownership():
    store = VersionedMapStore(max_retained_versions=4)
    store.commit(make_map(1, node="n1"))
    violations = []

    def writer():
        for v in range(2, 200):
            node = "n1" if v % 2 == 0 else "n2"
            store.commit(make_map(v, node=node))

    def reader():
        for _ in range(500):
            mv = store.current()
            owners = {r.node for r in mv.shards.values()}
            if len(owners) != 1:  # mixed old/new ownership
                violations.append((mv.version, owners))

    threads = [threading.Thread(target=writer)] + [
        threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not violations


def test_pinned_version_survives_gc():
    store = VersionedMapStore(max_retained_versions=2)
    store.commit(make_map(1))
    pinned = store.pin(1)
    for v in range(2, 10):
        store.commit(make_map(v))
    assert 1 in store.retained_versions()  # pinned -> retained
    assert store.get(1) is pinned
    store.release(1)
    store.gc()
    assert store.get(1) is None  # unpinned -> collected


def test_retention_is_bounded():
    store = VersionedMapStore(max_retained_versions=3)
    for v in range(1, 20):
        store.commit(make_map(v))
    kept = store.retained_versions()
    assert len(kept) <= 3
    assert kept[-1] == 19


def test_pin_unknown_version_rejected():
    store = VersionedMapStore()
    store.commit(make_map(1))
    with pytest.raises(RejectedError) as ei:
        store.pin(99)
    assert ei.value.reason is RejectReason.NOT_FOUND
