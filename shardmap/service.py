"""Shard-map service: orchestrates store, planner, split/merge, executor,
journal persistence and crash recovery. Fully local: no external services."""
from __future__ import annotations

import json
import os
import threading
import time
import uuid

from .auth import AccessController, AuditLog, CallerPolicy
from .executor import DataPlane, MigrationExecutor
from .models import (HASH_SPACE, ErrorCode, Move, RebalancePlan,
                     RebalanceStatus, Shard, ShardMapError, ShardMapVersion)
from .planner import compute_plan
from .splitmerge import merge_shards, split_shard
from .store import VersionedStore

_STATE_FILE = "state.json"
_JOURNAL_FILE = "journal.jsonl"


def _initial_shards(num_shards: int) -> list[Shard]:
    step = HASH_SPACE // num_shards
    shards = []
    for i in range(num_shards):
        start = i * step
        end = HASH_SPACE if i == num_shards - 1 else (i + 1) * step
        shards.append(Shard(f"shard-{i:04d}", start, end))
    return shards


class ShardMapService:
    def __init__(self, state_dir: str, nodes: list[str], num_shards: int = 16,
                 max_versions: int = 16, max_concurrent_migrations: int = 4,
                 per_shard_timeout: float = 30.0,
                 recovery_policy: str = "abort",
                 policies: dict[str, CallerPolicy] | None = None,
                 dataplane: DataPlane | None = None,
                 seed_keys_per_shard: int = 64) -> None:
        if recovery_policy not in ("abort", "resume"):
            raise ValueError("recovery_policy must be 'abort' or 'resume'")
        self._dir = state_dir
        os.makedirs(state_dir, exist_ok=True)
        self._recovery_policy = recovery_policy
        self._store = VersionedStore(max_versions=max_versions)
        self._dp = dataplane or DataPlane()
        self._executor = MigrationExecutor(
            self._dp, max_concurrent=max_concurrent_migrations,
            per_shard_timeout=per_shard_timeout)
        self._access = AccessController(policies)
        self._write_lock = threading.Lock()
        self._journal_lock = threading.Lock()
        self._jobs_lock = threading.Lock()
        self._jobs: dict[str, dict] = {}  # job_id -> {plan, caller, status}

        if os.path.exists(self._state_path):
            self._load_state()
        else:
            shards = _initial_shards(num_shards)
            assignment = {s.shard_id: nodes[i % len(nodes)]
                          for i, s in enumerate(shards)}
            for s in shards:  # deterministic sample data for local repro
                step = max(1, (s.end - s.start) // seed_keys_per_shard)
                self._dp.seed(assignment[s.shard_id], s.shard_id,
                              list(range(s.start, s.end, step)))
            self._store.commit(shards, assignment)
            self._persist_state()
        self.recover()

    # ------------------------------------------------------------------ read
    def get_map(self, caller: str = "anonymous",
                version: int | None = None) -> ShardMapVersion:
        snap = self._store.acquire(version)
        self._access.audit.record(caller, "get_map",
                                  f"version={snap.version}", ok=True)
        return snap

    def release_map(self, version: int) -> None:
        self._store.release(version)

    def needs_refresh(self, version: int) -> bool:
        return self._store.needs_refresh(version)

    @property
    def audit(self) -> AuditLog:
        return self._access.audit

    @property
    def dataplane(self) -> DataPlane:
        return self._dp

    # ----------------------------------------------------------------- write
    def rebalance(self, caller: str, target_nodes: list[str]) -> dict:
        base = self._store.current()
        plan = compute_plan(base, target_nodes, plan_id=uuid.uuid4().hex[:12])
        shard_ids = [m.shard_id for m in plan.moves]
        self._access.check(caller, "rebalance", shard_ids)  # before any change
        if not plan.moves:
            self._access.op_done(caller)
            return {"job_id": None, "plan": plan, "moves": 0}
        job_id = self._executor.run_plan(
            plan, on_commit=self._commit_move, on_abort=self._abort_move)
        with self._jobs_lock:
            self._jobs[job_id] = {"plan": plan, "caller": caller,
                                  "status": RebalanceStatus.RUNNING.value}
        self._journal({"type": "plan_start", "job_id": job_id,
                       "caller": caller,
                       "plan": [m.__dict__ for m in plan.moves],
                       "base_version": plan.base_version})
        threading.Thread(target=self._watch_job, args=(job_id, caller),
                         daemon=True).start()
        return {"job_id": job_id, "plan": plan, "moves": len(plan.moves)}

    def split(self, caller: str, shard_id: str,
              new_id_a: str | None = None, new_id_b: str | None = None) -> int:
        self._access.check(caller, "split", [shard_id])
        try:
            with self._write_lock:
                base = self._store.current()
                new_id_a = new_id_a or f"{shard_id}-a"
                new_id_b = new_id_b or f"{shard_id}-b"
                shards, assignment = split_shard(base, shard_id,
                                                 new_id_a, new_id_b)
                owner = base.assignment[shard_id]
                # Children inherit data view locally (same node, no transfer).
                slots = self._dp.read_owner_view(owner, shard_id)
                parent = next(s for s in base.shards if s.shard_id == shard_id)
                mid = (parent.start + parent.end) // 2
                self._dp.seed(owner, new_id_a, [k for k in slots if k < mid])
                self._dp.seed(owner, new_id_b, [k for k in slots if k >= mid])
                self._dp.drop(owner, shard_id)
                snap = self._store.commit(shards, assignment)
                self._persist_state()
                self._journal({"type": "split", "shard": shard_id,
                               "into": [new_id_a, new_id_b],
                               "version": snap.version})
                return snap.version
        finally:
            self._access.op_done(caller)

    def merge(self, caller: str, shard_id_a: str, shard_id_b: str,
              new_id: str | None = None) -> int:
        self._access.check(caller, "merge", [shard_id_a, shard_id_b])
        try:
            with self._write_lock:
                base = self._store.current()
                new_id = new_id or f"{shard_id_a}+{shard_id_b}"
                shards, assignment = merge_shards(base, shard_id_a,
                                                  shard_id_b, new_id)
                owner = assignment[new_id]
                slots = (self._dp.read_owner_view(owner, shard_id_a)
                         + self._dp.read_owner_view(owner, shard_id_b))
                self._dp.seed(owner, new_id, slots)
                self._dp.drop(owner, shard_id_a)
                self._dp.drop(owner, shard_id_b)
                snap = self._store.commit(shards, assignment)
                self._persist_state()
                self._journal({"type": "merge",
                               "from": [shard_id_a, shard_id_b],
                               "into": new_id, "version": snap.version})
                return snap.version
        finally:
            self._access.op_done(caller)

    # ------------------------------------------------------------- job admin
    def rebalance_status(self, job_id: str) -> dict:
        try:
            status, tasks = self._executor.status(job_id)
        except KeyError:
            with self._jobs_lock:
                job = self._jobs.get(job_id)
            if job is None:
                raise ShardMapError(ErrorCode.NOT_FOUND, f"unknown job {job_id}")
            return {"job_id": job_id, "status": job["status"], "tasks": []}
        return {"job_id": job_id, "status": status.value,
                "tasks": [{"shard_id": t.shard_id, "source": t.source,
                           "target": t.target, "phase": t.phase.value,
                           "progress": t.progress} for t in tasks]}

    def cancel_rebalance(self, job_id: str) -> None:
        try:
            self._executor.cancel(job_id)
        except KeyError:
            raise ShardMapError(ErrorCode.NOT_FOUND, f"unknown job {job_id}")

    # ------------------------------------------------------------- internals
    def _commit_move(self, move: Move) -> None:
        with self._write_lock:
            base = self._store.current()
            assignment = dict(base.assignment)
            assignment[move.shard_id] = move.target
            snap = self._store.commit(base.shards, assignment)
            self._persist_state()
            self._journal({"type": "move_committed", "shard": move.shard_id,
                           "target": move.target, "version": snap.version})

    def _abort_move(self, move: Move) -> None:
        self._journal({"type": "move_aborted", "shard": move.shard_id,
                       "source": move.source, "target": move.target})

    def _watch_job(self, job_id: str, caller: str) -> None:
        status = self._executor.wait(job_id, timeout=3600)
        with self._jobs_lock:
            if job_id in self._jobs:
                self._jobs[job_id]["status"] = status.value
        self._journal({"type": "job_end", "job_id": job_id,
                       "status": status.value})
        self._access.op_done(caller)

    # ---------------------------------------------------------- persistence
    @property
    def _state_path(self) -> str:
        return os.path.join(self._dir, _STATE_FILE)

    @property
    def _journal_path(self) -> str:
        return os.path.join(self._dir, _JOURNAL_FILE)

    def _persist_state(self) -> None:
        snap = self._store.current()
        payload = {
            "version": snap.version,
            "shards": [s.__dict__ for s in snap.shards],
            "assignment": snap.assignment,
            "dataplane": self._dp._data,
        }
        tmp = self._state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(payload, f)
        os.replace(tmp, self._state_path)  # atomic publish

    def _load_state(self) -> None:
        with open(self._state_path) as f:
            payload = json.load(f)
        shards = tuple(Shard(**s) for s in payload["shards"])
        self._store.load(ShardMapVersion(version=payload["version"],
                                         shards=shards,
                                         assignment=payload["assignment"]))
        self._dp._data = {node: {sid: list(slots) for sid, slots in m.items()}
                          for node, m in payload.get("dataplane", {}).items()}

    def _journal(self, entry: dict) -> None:
        entry = {"ts": time.time(), **entry}
        with self._journal_lock:
            with open(self._journal_path, "a") as f:
                f.write(json.dumps(entry) + "\n")

    def recover(self) -> None:
        """Identify unfinished rebalances from the journal and resolve them
        per recovery_policy. The committed map in state.json is always
        self-consistent (each committed move was a full version), so 'abort'
        only needs to close the job; 'resume' re-runs the remaining moves."""
        if not os.path.exists(self._journal_path):
            return
        open_plans: dict[str, dict] = {}
        done_shards: dict[str, set] = {}
        with open(self._journal_path) as f:
            for line in f:
                try:
                    e = json.loads(line)
                except json.JSONDecodeError:
                    continue  # torn tail write: ignore
                t = e.get("type")
                if t == "plan_start":
                    open_plans[e["job_id"]] = e
                    done_shards[e["job_id"]] = set()
                elif t == "move_committed":
                    for jid, plan in open_plans.items():
                        for m in plan["plan"]:
                            if m["shard_id"] == e["shard"]:
                                done_shards[jid].add(e["shard"])
                elif t == "job_end":
                    open_plans.pop(e["job_id"], None)
        for jid, plan in open_plans.items():
            remaining = [Move(m["shard_id"], m["source"], m["target"])
                         for m in plan["plan"]
                         if m["shard_id"] not in done_shards[jid]]
            if self._recovery_policy == "resume" and remaining:
                current = self._store.current().assignment
                todo = [m for m in remaining
                        if current.get(m.shard_id) != m.target]
                if todo:
                    p = RebalancePlan(plan_id=jid + "-resume",
                                      base_version=self._store.latest_version,
                                      moves=tuple(todo))
                    new_jid = self._executor.run_plan(
                        p, on_commit=self._commit_move,
                        on_abort=self._abort_move)
                    with self._jobs_lock:
                        self._jobs[new_jid] = {
                            "plan": p, "caller": plan.get("caller", "recovery"),
                            "status": RebalanceStatus.RUNNING.value}
                    threading.Thread(target=self._watch_job,
                                     args=(new_jid, plan.get("caller",
                                                             "recovery")),
                                     daemon=True).start()
                    self._journal({"type": "resumed", "from_job": jid,
                                   "job_id": new_jid, "moves": len(todo)})
            else:
                self._journal({"type": "job_end", "job_id": jid,
                               "status": "aborted_on_recovery"})
