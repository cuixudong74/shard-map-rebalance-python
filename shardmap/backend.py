"""Local simulated node storage backend (no external services)."""
from __future__ import annotations

import threading

from .models import NodeId, ShardId


class InMemoryBackend:
    """Per-node in-memory key/value stores keyed by shard.

    Stands in for real node storage so migration (copy-then-swap) can be
    exercised and verified locally without external services or credentials.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._data: dict[NodeId, dict[ShardId, dict[str, bytes]]] = {}

    def write(self, node: NodeId, shard: ShardId, key: str, value: bytes) -> None:
        with self._lock:
            self._data.setdefault(node, {}).setdefault(shard, {})[key] = value

    def read(self, node: NodeId, shard: ShardId, key: str) -> bytes | None:
        with self._lock:
            return self._data.get(node, {}).get(shard, {}).get(key)

    def shard_items(self, node: NodeId, shard: ShardId) -> dict[str, bytes]:
        with self._lock:
            return dict(self._data.get(node, {}).get(shard, {}))

    def copy_shard(self, src: NodeId, dst: NodeId, shard: ShardId) -> int:
        """Copy all data of a shard src->dst; returns bytes copied."""
        with self._lock:
            items = dict(self._data.get(src, {}).get(shard, {}))
            self._data.setdefault(dst, {}).setdefault(shard, {}).update(items)
            return sum(len(k.encode()) + len(v) for k, v in items.items())

    def merge_shards(self, srcs: list[tuple[NodeId, ShardId]], dst: NodeId,
                     target: ShardId) -> int:
        """Copy many source shards into one target shard; returns bytes."""
        total = 0
        with self._lock:
            tgt = self._data.setdefault(dst, {}).setdefault(target, {})
            for node, shard in srcs:
                items = dict(self._data.get(node, {}).get(shard, {}))
                tgt.update(items)
                total += sum(len(k.encode()) + len(v) for k, v in items.items())
        return total

    def delete_shard(self, node: NodeId, shard: ShardId) -> None:
        with self._lock:
            self._data.get(node, {}).pop(shard, None)
