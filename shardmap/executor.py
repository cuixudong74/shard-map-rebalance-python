"""Online rebalance executor: copy-then-swap with progress, cancel, timeout.

Protocol per shard move:
1. COPY: copy shard data src -> dst while the committed map still points
   at src (reads/writes uninterrupted, readers never see partial data).
2. SWAP: commit a brand-new map version where the shard points at dst.
   Versions are atomic, so no reader ever observes mixed ownership.
3. Source data is removed only when no pinned reader version can still
   reference it.

A slow node occupies at most one worker; other shards keep migrating.
Cancel/timeout never leave the map in an intermediate state: every
committed version is fully self-consistent, and uncommitted copies are
harmless garbage on the destination.
"""
from __future__ import annotations

import copy
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .audit import AuditLog
from .backend import InMemoryBackend
from .errors import RejectedError, RejectReason
from .journal import Journal
from .models import (
    JobState,
    MapVersion,
    Move,
    RebalanceJob,
    RebalancePlan,
    ShardRecord,
    ShardTask,
    TaskState,
)
from .store import VersionedMapStore


class RebalanceExecutor:
    def __init__(self, store: VersionedMapStore, backend: InMemoryBackend,
                 journal: Journal, audit: AuditLog,
                 max_parallel_shards: int = 4,
                 shard_timeout_s: float = 30.0,
                 recovery_policy: str = "abort",
                 node_delays: dict[str, float] | None = None) -> None:
        if recovery_policy not in ("abort", "resume"):
            raise ValueError("recovery_policy must be 'abort' or 'resume'")
        self._lock = threading.RLock()
        self._store = store
        self._backend = backend
        self._journal = journal
        self._audit = audit
        self._max_parallel = max_parallel_shards
        self._shard_timeout = shard_timeout_s
        self._recovery_policy = recovery_policy
        self._node_delays = dict(node_delays or {})  # test hook: slow nodes
        self._job: RebalanceJob | None = None
        self._cancel = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------ API

    def submit(self, plan: RebalancePlan, caller: str) -> RebalanceJob:
        with self._lock:
            if self._job is not None and self._job.state is JobState.RUNNING:
                raise RejectedError(RejectReason.REBALANCE_IN_PROGRESS,
                                    f"job {self._job.job_id} still running")
            cur = self._store.current_version
            if plan.base_version != cur:
                raise RejectedError(
                    RejectReason.VERSION_CONFLICT,
                    f"plan base {plan.base_version} != current {cur}")
            job = RebalanceJob(
                job_id=f"{plan.plan_id}-{int(time.time() * 1000)}", plan=plan)
            job.tasks = {
                m.shard_id: ShardTask(shard_id=m.shard_id, src=m.src, dst=m.dst)
                for m in plan.moves
            }
            self._job = job
            self._cancel = threading.Event()
        self._journal.append({
            "type": "job_start", "job_id": job.job_id,
            "base_version": plan.base_version,
            "moves": [[m.shard_id, m.src, m.dst] for m in plan.moves],
        })
        self._audit.record("rebalance_submit", caller,
                           {"job_id": job.job_id, "moves": len(plan.moves)})
        self._thread = threading.Thread(target=self._run, args=(job,),
                                        daemon=True, name=f"rebal-{job.job_id}")
        self._thread.start()
        return job

    def progress(self) -> RebalanceJob | None:
        with self._lock:
            return copy.deepcopy(self._job)

    def cancel(self, caller: str) -> None:
        with self._lock:
            job = self._job
            if job is None or job.state is not JobState.RUNNING:
                raise RejectedError(RejectReason.INVALID_STATE,
                                    "no running job to cancel")
            self._cancel.set()
        self._audit.record("rebalance_cancel", caller, {"job_id": job.job_id})

    def wait(self, timeout: float | None = None) -> None:
        t = self._thread
        if t is not None:
            t.join(timeout)

    # -------------------------------------------------------------- recovery

    def recover(self) -> RebalanceJob | None:
        """After restart: find an unfinished job in the journal and either
        safely abort it (default) or resume the remaining moves, according
        to ``recovery_policy``. The map is always left self-consistent."""
        records = self._journal.read_all()
        starts = [r for r in records if r.get("type") == "job_start"]
        if not starts:
            return None
        last = starts[-1]
        jid = last["job_id"]
        closed = {r.get("job_id") for r in records
                  if r.get("type") in ("job_committed", "job_aborted")}
        if jid in closed:
            return None
        committed = {r["shard_id"] for r in records
                     if r.get("type") == "shard_committed"
                     and r.get("job_id") == jid}
        remaining = [Move(shard_id=s, src=src, dst=dst)
                     for s, src, dst in last["moves"] if s not in committed]
        plan = RebalancePlan(
            plan_id=f"{jid}-recovery", base_version=self._store.current_version,
            moves=tuple(remaining))
        if self._recovery_policy == "resume" and remaining:
            self._audit.record("rebalance_recover_resume", "system",
                               {"job_id": jid, "remaining": len(remaining)})
            return self.submit(plan, caller="system")
        self._journal.append({"type": "job_aborted", "job_id": jid,
                              "reason": "recovery_abort"})
        self._audit.record("rebalance_recover_abort", "system",
                           {"job_id": jid, "dropped": len(remaining)})
        job = RebalanceJob(job_id=jid, plan=plan, state=JobState.ABORTED,
                           finished_at=time.time())
        with self._lock:
            self._job = job
        return job

    # ------------------------------------------------------------- internals

    def _run(self, job: RebalanceJob) -> None:
        try:
            with ThreadPoolExecutor(max_workers=self._max_parallel) as pool:
                futs = [pool.submit(self._run_task, job, t)
                        for t in job.tasks.values()]
                for f in futs:
                    f.result()
            failed = [t for t in job.tasks.values()
                      if t.state is TaskState.FAILED]
            cancelled = self._cancel.is_set()
            with self._lock:
                for t in job.tasks.values():
                    if t.state is TaskState.PENDING:
                        t.state = TaskState.CANCELLED
                job.finished_at = time.time()
                if cancelled or failed:
                    job.state = JobState.ABORTED
                    reason = "cancelled" if cancelled else "task_failed"
                    self._journal.append({"type": "job_aborted",
                                          "job_id": job.job_id,
                                          "reason": reason})
                else:
                    job.state = JobState.COMMITTED
                    self._journal.append({
                        "type": "job_committed", "job_id": job.job_id,
                        "final_version": self._store.current_version})
                self._audit.record("rebalance_finished", "system", {
                    "job_id": job.job_id, "state": job.state.value})
        except Exception as exc:  # pragma: no cover - defensive
            with self._lock:
                job.state = JobState.ABORTED
                job.finished_at = time.time()
            self._journal.append({"type": "job_aborted",
                                  "job_id": job.job_id,
                                  "reason": f"internal:{exc}"})

    def _run_task(self, job: RebalanceJob, task: ShardTask) -> None:
        if self._cancel.is_set():
            task.state = TaskState.CANCELLED
            return
        task.state = TaskState.COPYING
        try:
            if not self._copy_with_timeout(task):
                task.state = TaskState.FAILED
                task.error = f"timeout after {self._shard_timeout}s"
                return
        except Exception as exc:
            task.state = TaskState.FAILED
            task.error = str(exc)
            return
        if self._cancel.is_set():
            task.state = TaskState.CANCELLED
            return
        self._commit_move(job, task)

    def _copy_with_timeout(self, task: ShardTask) -> bool:
        done = threading.Event()
        err: list[BaseException] = []

        def work() -> None:
            try:
                delay = self._node_delays.get(task.src, 0.0)
                if delay:
                    time.sleep(delay)
                n = self._backend.copy_shard(task.src, task.dst, task.shard_id)
                task.bytes_total = task.bytes_done = n
            except BaseException as exc:  # noqa: BLE001
                err.append(exc)
            finally:
                done.set()

        threading.Thread(target=work, daemon=True).start()
        if not done.wait(self._shard_timeout):
            return False
        if err:
            raise err[0]
        return True

    def _commit_move(self, job: RebalanceJob, task: ShardTask) -> None:
        with self._lock:
            cur = self._store.current()
            shards = dict(cur.shards)
            rec = shards[task.shard_id]
            shards[task.shard_id] = ShardRecord(
                shard_id=rec.shard_id, node=task.dst,
                state=rec.state, parent=rec.parent, children=rec.children)
            mv = self._store.commit(MapVersion(
                version=cur.version + 1, shards=shards))
            task.state = TaskState.DONE
            self._journal.append({
                "type": "shard_committed", "job_id": job.job_id,
                "shard_id": task.shard_id, "version": mv.version})
            self._cleanup_source(cur, task)

    def _cleanup_source(self, prev: MapVersion, task: ShardTask) -> None:
        """Drop the source copy only when no pinned reader version can
        still reference it."""
        if task.src == task.dst:
            return
        with self._lock:
            pinned = self._store._pins  # internal, same package intent
            oldest_pin = min(pinned, default=None)
            if oldest_pin is not None and oldest_pin <= prev.version:
                return  # a reader may still read from the source
        self._backend.delete_shard(task.src, task.shard_id)
