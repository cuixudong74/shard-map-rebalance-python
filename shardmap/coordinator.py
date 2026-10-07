"""Coordinator: high-level shard map operations with local persistence.

Owns the versioned store, backend, journal, quotas, audit log and the
rebalance executor. Persists every committed version to a snapshot file
so a restarted process can reload the map and recover unfinished work
from the journal.
"""
from __future__ import annotations

import hashlib
import json
import threading
from contextlib import contextmanager
from pathlib import Path

from .audit import AuditLog
from .backend import InMemoryBackend
from .errors import RejectedError, RejectReason
from .executor import RebalanceExecutor
from .journal import Journal
from .models import (
    MapVersion,
    MergeOp,
    NodeId,
    RebalanceJob,
    ShardId,
    ShardRecord,
    ShardState,
    SplitOp,
)
from .planner import compute_plan
from .quotas import CallerPolicy, QuotaManager
from .splitmerge import (
    apply_merge,
    apply_split,
    mark_merging,
    mark_splitting,
    plan_merge,
    plan_split,
    route,
)
from .store import VersionedMapStore


def _child_for_key(children: tuple[ShardId, ...], key: str) -> ShardId:
    """Deterministic key -> child shard assignment for splits."""
    h = int(hashlib.sha256(key.encode()).hexdigest(), 16)
    return children[h % len(children)]


class Coordinator:
    def __init__(self, workdir: str | Path, max_retained_versions: int = 16,
                 max_parallel_shards: int = 4, shard_timeout_s: float = 30.0,
                 recovery_policy: str = "abort",
                 node_delays: dict[str, float] | None = None,
                 audit_capacity: int = 1024,
                 backend: InMemoryBackend | None = None) -> None:
        self._lock = threading.RLock()
        self.workdir = Path(workdir)
        self.workdir.mkdir(parents=True, exist_ok=True)
        self.snapshot_path = self.workdir / "snapshot.json"
        self.store = VersionedMapStore(max_retained_versions)
        # The backend simulates node-local storage; in production it lives
        # on the nodes and survives a coordinator restart, so tests inject
        # a shared instance to model that.
        self.backend = backend if backend is not None else InMemoryBackend()
        self.journal = Journal(self.workdir / "journal.jsonl")
        self.audit = AuditLog(audit_capacity)
        self.quotas = QuotaManager()
        self.executor = RebalanceExecutor(
            self.store, self.backend, self.journal, self.audit,
            max_parallel_shards=max_parallel_shards,
            shard_timeout_s=shard_timeout_s,
            recovery_policy=recovery_policy,
            node_delays=node_delays)
        self.store.add_listener(self._persist)
        snap = self._load_snapshot()
        if snap is not None:
            self.store.restore(snap)

    # ---------------------------------------------------------- persistence

    def _persist(self, mv: MapVersion) -> None:
        payload = {
            "version": mv.version,
            "shards": {
                sid: {"node": r.node, "state": r.state.value,
                      "parent": r.parent, "children": list(r.children)}
                for sid, r in sorted(mv.shards.items())
            },
        }
        tmp = self.snapshot_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        tmp.replace(self.snapshot_path)

    def _load_snapshot(self) -> MapVersion | None:
        if not self.snapshot_path.exists():
            return None
        payload = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
        shards = {
            sid: ShardRecord(shard_id=sid, node=r["node"],
                             state=ShardState(r["state"]), parent=r["parent"],
                             children=tuple(r["children"]))
            for sid, r in payload["shards"].items()
        }
        return MapVersion(version=payload["version"], shards=shards)

    # -------------------------------------------------------------- reading

    def current_map(self) -> MapVersion:
        return self.store.current()

    @contextmanager
    def pinned_map(self, version: int | None = None):
        """Yield a pinned map version; safe against GC while held."""
        v = version if version is not None else self.store.current_version
        mv = self.store.pin(v)
        try:
            yield mv
        finally:
            self.store.release(v)

    def route_request(self, shard_id: ShardId) -> tuple[int, ShardId, NodeId]:
        """Resolve a request to (version, authoritative shard, node)."""
        mv = self.store.current()
        authoritative = route(mv, shard_id)
        return mv.version, authoritative, mv.owner_of(authoritative)

    # ------------------------------------------------------------ bootstrap

    def bootstrap(self, nodes: list[NodeId], num_shards: int,
                  caller: str = "system") -> MapVersion:
        with self._lock:
            if self.store.current_version != 0:
                raise RejectedError(RejectReason.INVALID_STATE,
                                    "map already initialised")
            if not nodes or num_shards < 1:
                raise RejectedError(RejectReason.INVALID_PLAN,
                                    "need nodes and >= 1 shard")
            ordered = sorted(set(nodes))
            shards = {}
            for i in range(num_shards):
                sid = f"shard-{i:04d}"
                shards[sid] = ShardRecord(shard_id=sid,
                                          node=ordered[i % len(ordered)])
            mv = self.store.commit(MapVersion(version=1, shards=shards))
            self.audit.record("bootstrap", caller,
                              {"nodes": ordered, "shards": num_shards})
            return mv

    # ------------------------------------------------------------ rebalance

    def plan_rebalance(self, target_nodes: list[NodeId]) -> object:
        return compute_plan(self.store.current(), target_nodes)

    def start_rebalance(self, target_nodes: list[NodeId],
                        caller: str) -> RebalanceJob:
        plan = compute_plan(self.store.current(), target_nodes)
        self.quotas.check_shards(caller, {m.shard_id for m in plan.moves})
        self.quotas.acquire(caller)
        try:
            job = self.executor.submit(plan, caller)
        except Exception:
            self.quotas.release(caller)
            raise
        # Release the caller slot when the job finishes.
        threading.Thread(target=self._release_when_done,
                         args=(caller, job), daemon=True).start()
        return job

    def _release_when_done(self, caller: str, job: RebalanceJob) -> None:
        self.executor.wait()
        self.quotas.release(caller)

    # ----------------------------------------------------------- split/merge

    def split(self, parent: ShardId, children: tuple[ShardId, ...],
              node: NodeId, caller: str) -> MapVersion:
        self.quotas.check_shards(caller, {parent, *children})
        self.quotas.acquire(caller)
        try:
            with self._lock:  # serialize split/merge against each other
                mv = self.store.current()
                op = plan_split(mv, parent, children)
                self.journal.append({
                    "type": "split_start", "parent": parent,
                    "children": list(op.children), "node": node,
                    "base_version": mv.version})
                self.audit.record("split_start", caller,
                                  {"parent": parent, "children": list(children)})
                # Phase 1: mark parent SPLITTING (still authoritative).
                self.store.commit(MapVersion(
                    version=mv.version + 1, shards=mark_splitting(mv, op)))
                # Phase 2: copy data to children (deterministic key split).
                items = self.backend.shard_items(mv.owner_of(parent), parent)
                for k, v in items.items():
                    self.backend.write(node, _child_for_key(op.children, k),
                                       k, v)
                # Phase 3: commit children as authoritative.
                cur = self.store.current()
                out = self.store.commit(MapVersion(
                    version=cur.version + 1,
                    shards=apply_split(cur, op, node)))
                self.journal.append({"type": "split_committed",
                                     "parent": parent,
                                     "version": out.version})
                self._gc_source_data(mv.owner_of(parent), parent, mv.version)
                return out
        finally:
            self.quotas.release(caller)

    def merge(self, children: tuple[ShardId, ...], target: ShardId,
              node: NodeId, caller: str) -> MapVersion:
        self.quotas.check_shards(caller, {*children, target})
        self.quotas.acquire(caller)
        try:
            with self._lock:
                mv = self.store.current()
                op = plan_merge(mv, children, target)
                self.journal.append({
                    "type": "merge_start", "children": list(op.children),
                    "target": target, "node": node,
                    "base_version": mv.version})
                self.audit.record("merge_start", caller,
                                  {"children": list(children),
                                   "target": target})
                self.store.commit(MapVersion(
                    version=mv.version + 1, shards=mark_merging(mv, op)))
                srcs = [(mv.owner_of(c), c) for c in op.children]
                self.backend.merge_shards(srcs, node, target)
                cur = self.store.current()
                out = self.store.commit(MapVersion(
                    version=cur.version + 1,
                    shards=apply_merge(cur, op, node)))
                self.journal.append({"type": "merge_committed",
                                     "target": target,
                                     "version": out.version})
                for owner, c in srcs:
                    self._gc_source_data(owner, c, mv.version)
                return out
        finally:
            self.quotas.release(caller)

    def _gc_source_data(self, node: NodeId, shard: ShardId,
                        authoritative_until: int) -> None:
        """Delete source data only if no pinned reader still needs it."""
        pins = self.store._pins
        oldest = min(pins, default=None)
        if oldest is not None and oldest <= authoritative_until:
            return
        self.backend.delete_shard(node, shard)

    # -------------------------------------------------------------- recovery

    def recover(self) -> dict:
        """Identify unfinished work after a restart and resolve it per
        policy. Always leaves the map at a self-consistent version."""
        actions: list[str] = []
        records = self.journal.read_all()
        split_done = {r.get("parent") for r in records
                      if r.get("type") in ("split_committed", "split_aborted")}
        merge_done = {r.get("target") for r in records
                      if r.get("type") in ("merge_committed", "merge_aborted")}
        for r in records:
            t = r.get("type")
            if t == "split_start" and r["parent"] not in split_done:
                self._recover_split(r)
                actions.append(f"split:{r['parent']}:aborted")
            elif t == "merge_start" and r["target"] not in merge_done:
                self._recover_merge(r)
                actions.append(f"merge:{r['target']}:aborted")
        job = self.executor.recover()
        if job is not None:
            actions.append(f"job:{job.job_id}:{job.state.value}")
        if actions:
            self.audit.record("recover", "system", {"actions": actions})
        return {"actions": actions}

    def _recover_split(self, rec: dict) -> None:
        """A split interrupted before commit: children never became
        authoritative, so roll the parent back to ACTIVE and drop any
        partial child copies."""
        with self._lock:
            cur = self.store.current()
            parent = rec["parent"]
            shard_rec = cur.shards.get(parent)
            if shard_rec is not None and shard_rec.state is ShardState.SPLITTING:
                shards = dict(cur.shards)
                shards[parent] = ShardRecord(shard_id=parent,
                                             node=shard_rec.node)
                self.store.commit(MapVersion(version=cur.version + 1,
                                             shards=shards))
            for c in rec["children"]:
                self.backend.delete_shard(rec["node"], c)
            self.journal.append({"type": "split_aborted",
                                 "parent": parent})

    def _recover_merge(self, rec: dict) -> None:
        with self._lock:
            cur = self.store.current()
            shards = dict(cur.shards)
            changed = False
            for c in rec["children"]:
                r = shards.get(c)
                if r is not None and r.state is ShardState.MERGING:
                    shards[c] = ShardRecord(shard_id=c, node=r.node)
                    changed = True
            if changed:
                self.store.commit(MapVersion(version=cur.version + 1,
                                             shards=shards))
            self.backend.delete_shard(rec["node"], rec["target"])
            self.journal.append({"type": "merge_aborted",
                                 "target": rec["target"]})

    # ---------------------------------------------------------------- quotas

    def set_policy(self, policy: CallerPolicy) -> None:
        self.quotas.set_policy(policy)
        self.audit.record("set_policy", "system",
                          {"caller": policy.caller,
                           "max_concurrent_ops": policy.max_concurrent_ops,
                           "allowed_shards": (sorted(policy.allowed_shards)
                                              if policy.allowed_shards
                                              is not None else None)})
