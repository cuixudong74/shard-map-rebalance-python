"""Req 7: bounded memory, old versions GC'd, pinned readers unaffected."""
import pytest

from shardmap.models import ErrorCode, ShardMapError
from shardmap.store import VersionedStore
from tests.test_versioning import make_shards


def test_history_is_bounded():
    store = VersionedStore(max_versions=4)
    shards = make_shards()
    for i in range(20):
        store.commit(shards, {s.shard_id: "n1" for s in shards})
    assert len(store.known_versions()) <= 4


def test_pinned_version_survives_gc_and_is_readable():
    store = VersionedStore(max_versions=2)
    shards = make_shards()
    store.commit(shards, {s.shard_id: "n1" for s in shards})
    pinned = store.acquire(1)
    for _ in range(10):
        store.commit(shards, {s.shard_id: "n2" for s in shards})
    again = store.acquire(1)  # still readable while pinned
    assert again.assignment == pinned.assignment
    store.release(again.version)
    store.release(pinned.version)
    store.commit(shards, {s.shard_id: "n2" for s in shards})
    with pytest.raises(ShardMapError) as ei:
        store.acquire(1)
    assert ei.value.code is ErrorCode.NOT_FOUND
