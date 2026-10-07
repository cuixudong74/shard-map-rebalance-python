"""Online migration executor.

Each move runs as an independent per-shard state machine on its own thread,
bounded by a concurrency semaphore, so one slow node never blocks other
shards. The authoritative map is only switched (on_commit) after the shard's
data is fully copied; cancel/timeout/failure before that point simply
discards the partial copy (on_abort) and the map stays self-consistent.
"""
from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable

from .models import (MigrationPhase, MigrationTask, Move, RebalancePlan,
                     RebalanceStatus)


class DataPlane:
    """Local, in-memory stand-in for node data transfer (no external deps).

    Data is node -> shard_id -> sorted list of slots (keys) held on that node.
    """

    def __init__(self, item_delay: float = 0.0) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, dict[str, list[int]]] = {}
        self._item_delay = item_delay  # test hook: simulate a slow node

    def seed(self, node: str, shard_id: str, slots: list[int]) -> None:
        with self._lock:
            self._data.setdefault(node, {})[shard_id] = sorted(slots)

    def copy_shard(self, source: str, target: str, shard_id: str,
                   should_stop: Callable[[], bool] = lambda: False,
                   on_item: Callable[[int], None] = lambda n: None) -> int:
        """Copy slots one by one; returns copied count. Raises
        MigrationAborted if should_stop() becomes true mid-copy."""
        with self._lock:
            slots = list(self._data.get(source, {}).get(shard_id, []))
        copied = 0
        for slot in slots:
            if should_stop():
                raise MigrationAborted(f"copy of {shard_id} stopped")
            if self._item_delay:
                time.sleep(self._item_delay)
            with self._lock:
                self._data.setdefault(target, {}).setdefault(shard_id, [])
                self._data[target][shard_id].append(slot)
            copied += 1
            on_item(copied)
        return copied

    def drop(self, node: str, shard_id: str) -> None:
        with self._lock:
            self._data.get(node, {}).pop(shard_id, None)

    def read_owner_view(self, node: str, shard_id: str) -> list[int]:
        with self._lock:
            return sorted(self._data.get(node, {}).get(shard_id, []))


class MigrationAborted(Exception):
    pass


class _Job:
    def __init__(self, plan: RebalancePlan) -> None:
        self.plan = plan
        self.tasks = [MigrationTask(m.shard_id, m.source, m.target)
                      for m in plan.moves]
        self.cancel_event = threading.Event()
        self.lock = threading.Lock()
        self.failed = False


class MigrationExecutor:
    def __init__(self, dataplane: DataPlane, max_concurrent: int = 4,
                 per_shard_timeout: float = 30.0) -> None:
        self._dp = dataplane
        self._sem = threading.Semaphore(max_concurrent)
        self._timeout = per_shard_timeout
        self._lock = threading.Lock()
        self._jobs: dict[str, _Job] = {}

    def run_plan(self, plan: RebalancePlan,
                 on_commit: Callable[[Move], None],
                 on_abort: Callable[[Move], None]) -> str:
        job_id = uuid.uuid4().hex[:12]
        job = _Job(plan)
        with self._lock:
            self._jobs[job_id] = job
        for move, task in zip(plan.moves, job.tasks):
            t = threading.Thread(target=self._run_move,
                                 args=(job, move, task, on_commit, on_abort),
                                 daemon=True)
            t.start()
        return job_id

    def _run_move(self, job: _Job, move: Move, task: MigrationTask,
                  on_commit, on_abort) -> None:
        with self._sem:
            deadline = time.monotonic() + self._timeout
            try:
                task.phase = MigrationPhase.COPYING
                with self._dp._lock:
                    task.total = len(
                        self._dp._data.get(move.source, {}).get(move.shard_id, []))

                def should_stop() -> bool:
                    return (job.cancel_event.is_set()
                            or time.monotonic() > deadline)

                copied = self._dp.copy_shard(
                    move.source, move.target, move.shard_id,
                    should_stop=should_stop,
                    on_item=lambda n: setattr(task, "copied", n))
                task.copied = copied
                # Data fully on target: swap authoritative owner atomically.
                task.phase = MigrationPhase.SWAPPING
                on_commit(move)
                self._dp.drop(move.source, move.shard_id)
                task.phase = MigrationPhase.DONE
            except Exception:
                # Cancel, timeout or copy error: discard partial target data,
                # map still points at source -> always self-consistent.
                self._dp.drop(move.target, move.shard_id)
                task.phase = MigrationPhase.ABORTED
                with job.lock:
                    job.failed = True
                try:
                    on_abort(move)
                except Exception:
                    pass

    def status(self, job_id: str) -> tuple[RebalanceStatus, list[MigrationTask]]:
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        tasks = job.tasks
        if all(t.phase is MigrationPhase.DONE for t in tasks):
            return RebalanceStatus.COMPLETED, list(tasks)
        if any(t.phase is MigrationPhase.ABORTED for t in tasks):
            if all(t.phase in (MigrationPhase.DONE, MigrationPhase.ABORTED)
                   for t in tasks):
                return (RebalanceStatus.CANCELLED if job.cancel_event.is_set()
                        else RebalanceStatus.FAILED), list(tasks)
        return RebalanceStatus.RUNNING, list(tasks)

    def cancel(self, job_id: str) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
        if job is None:
            raise KeyError(job_id)
        job.cancel_event.set()

    def wait(self, job_id: str, timeout: float = 10.0) -> RebalanceStatus:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status, _ = self.status(job_id)
            if status is not RebalanceStatus.RUNNING:
                return status
            time.sleep(0.01)
        return RebalanceStatus.RUNNING
