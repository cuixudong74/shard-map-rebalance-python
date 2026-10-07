"""FastAPI wiring for the shard map service."""
from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from .coordinator import Coordinator
from .errors import RejectedError
from .models import JobState, MapVersion, RebalanceJob
from .quotas import CallerPolicy


class BootstrapReq(BaseModel):
    nodes: list[str]
    num_shards: int
    caller: str = "system"


class RebalanceReq(BaseModel):
    target_nodes: list[str]
    caller: str


class CancelReq(BaseModel):
    caller: str


class SplitReq(BaseModel):
    parent: str
    children: list[str]
    node: str
    caller: str


class MergeReq(BaseModel):
    children: list[str]
    target: str
    node: str
    caller: str


class PolicyReq(BaseModel):
    caller: str
    allowed_shards: list[str] | None = None
    max_concurrent_ops: int = 4


def _version_json(mv: MapVersion) -> dict:
    return {
        "version": mv.version,
        "shards": {sid: {"node": r.node, "state": r.state.value}
                   for sid, r in sorted(mv.shards.items())},
    }


def _job_json(job: RebalanceJob | None) -> dict | None:
    if job is None:
        return None
    return {
        "job_id": job.job_id,
        "state": job.state.value,
        "base_version": job.plan.base_version,
        "tasks": {sid: {"src": t.src, "dst": t.dst, "state": t.state.value,
                        "bytes_total": t.bytes_total,
                        "bytes_done": t.bytes_done, "error": t.error}
                  for sid, t in sorted(job.tasks.items())},
    }


def create_app(workdir: str | Path | None = None, **config) -> FastAPI:
    workdir = workdir or os.environ.get("SHARDMAP_WORKDIR", "./data")
    coord = Coordinator(workdir, **config)
    app = FastAPI(title="shard-map-rebalance")
    app.state.coordinator = coord

    @app.exception_handler(RejectedError)
    async def rejected(_req: Request, exc: RejectedError) -> JSONResponse:
        return JSONResponse(status_code=409,
                            content={"error": exc.reason.value,
                                     "message": str(exc)})

    @app.get("/map")
    def get_current_map() -> dict:
        return _version_json(coord.current_map())

    @app.get("/map/{version}")
    def get_map_version(version: int) -> dict:
        mv = coord.store.get(version)
        if mv is None:
            return JSONResponse(status_code=404,
                                content={"error": "not_found"})
        return _version_json(mv)

    @app.get("/route/{shard_id}")
    def route_shard(shard_id: str) -> dict:
        version, shard, node = coord.route_request(shard_id)
        return {"version": version, "shard": shard, "node": node}

    @app.post("/bootstrap")
    def bootstrap(req: BootstrapReq) -> dict:
        return _version_json(coord.bootstrap(req.nodes, req.num_shards,
                                             caller=req.caller))

    @app.post("/rebalance")
    def start_rebalance(req: RebalanceReq) -> dict:
        job = coord.start_rebalance(req.target_nodes, req.caller)
        return _job_json(job)

    @app.get("/rebalance")
    def rebalance_progress() -> dict | None:
        return _job_json(coord.executor.progress())

    @app.post("/rebalance/cancel")
    def cancel_rebalance(req: CancelReq) -> dict:
        coord.executor.cancel(req.caller)
        return {"cancelled": True}

    @app.post("/split")
    def split(req: SplitReq) -> dict:
        mv = coord.split(req.parent, tuple(req.children), req.node, req.caller)
        return _version_json(mv)

    @app.post("/merge")
    def merge(req: MergeReq) -> dict:
        mv = coord.merge(tuple(req.children), req.target, req.node, req.caller)
        return _version_json(mv)

    @app.post("/recover")
    def recover() -> dict:
        return coord.recover()

    @app.get("/audit")
    def audit() -> list[dict]:
        return coord.audit.entries()

    @app.post("/policies")
    def set_policy(req: PolicyReq) -> dict:
        coord.set_policy(CallerPolicy(
            caller=req.caller,
            allowed_shards=(frozenset(req.allowed_shards)
                            if req.allowed_shards is not None else None),
            max_concurrent_ops=req.max_concurrent_ops))
        return {"ok": True}

    return app
