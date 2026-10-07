"""FastAPI HTTP surface for the shard-map service."""
from __future__ import annotations

import os

from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from .models import ShardMapError
from .service import ShardMapService


class RebalanceRequest(BaseModel):
    target_nodes: list[str]


class SplitRequest(BaseModel):
    shard_id: str
    new_id_a: str | None = None
    new_id_b: str | None = None


class MergeRequest(BaseModel):
    shard_id_a: str
    shard_id_b: str
    new_id: str | None = None


def _version_payload(snap, svc: ShardMapService) -> dict:
    return {
        "version": snap.version,
        "shards": [{"shard_id": s.shard_id, "start": s.start, "end": s.end}
                   for s in snap.shards],
        "assignment": snap.assignment,
        "latest": svc._store.latest_version,
    }


def create_app(state_dir: str = ".shardmap-state",
               nodes: list[str] | None = None,
               num_shards: int = 16,
               recovery_policy: str | None = None,
               **service_kwargs) -> FastAPI:
    svc = ShardMapService(
        state_dir, nodes or ["node-1", "node-2"],
        num_shards=num_shards,
        recovery_policy=recovery_policy
        or os.environ.get("SHARDMAP_RECOVERY_POLICY", "abort"),
        **service_kwargs)
    app = FastAPI(title="shard-map-rebalance")
    app.state.service = svc

    def handle_error(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ShardMapError as e:
            status = {"permission_denied": 403, "quota_exceeded": 429,
                      "not_found": 404, "conflict": 409}.get(e.code.value, 400)
            raise HTTPException(status_code=status,
                                detail={"code": e.code.value,
                                        "message": e.message})

    @app.get("/map")
    def get_map(version: int | None = None,
                x_caller: str = Header(default="anonymous")):
        snap = handle_error(svc.get_map, x_caller, version)
        try:
            return _version_payload(snap, svc)
        finally:
            svc.release_map(snap.version)

    @app.get("/map/refresh")
    def needs_refresh(version: int):
        return {"version": version, "needs_refresh": svc.needs_refresh(version)}

    @app.post("/rebalance")
    def rebalance(req: RebalanceRequest,
                  x_caller: str = Header(default="anonymous")):
        r = handle_error(svc.rebalance, x_caller, req.target_nodes)
        return {"job_id": r["job_id"], "moves": r["moves"],
                "plan": [m.__dict__ for m in r["plan"].moves]}

    @app.get("/rebalance/{job_id}")
    def rebalance_status(job_id: str):
        return handle_error(svc.rebalance_status, job_id)

    @app.post("/rebalance/{job_id}/cancel")
    def cancel(job_id: str, x_caller: str = Header(default="anonymous")):
        handle_error(svc.cancel_rebalance, job_id)
        return {"job_id": job_id, "cancelled": True}

    @app.post("/split")
    def split(req: SplitRequest, x_caller: str = Header(default="anonymous")):
        v = handle_error(svc.split, x_caller, req.shard_id,
                         req.new_id_a, req.new_id_b)
        return {"version": v}

    @app.post("/merge")
    def merge(req: MergeRequest, x_caller: str = Header(default="anonymous")):
        v = handle_error(svc.merge, x_caller, req.shard_id_a,
                         req.shard_id_b, req.new_id)
        return {"version": v}

    @app.get("/audit")
    def audit():
        return {"entries": svc.audit.entries()}

    return app
