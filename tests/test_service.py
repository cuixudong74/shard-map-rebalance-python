"""HTTP API smoke tests via FastAPI TestClient."""
import pytest
from fastapi.testclient import TestClient

from shardmap.service import create_app


@pytest.fixture()
def client(tmp_path):
    app = create_app(tmp_path / "http", shard_timeout_s=0.5)
    with TestClient(app) as c:
        yield c


def test_bootstrap_map_route_flow(client):
    r = client.post("/bootstrap", json={"nodes": ["n1", "n2"],
                                        "num_shards": 4})
    assert r.status_code == 200
    assert r.json()["version"] == 1

    r = client.get("/map")
    assert r.json()["version"] == 1
    assert len(r.json()["shards"]) == 4

    r = client.get("/route/shard-0000")
    body = r.json()
    assert body["shard"] == "shard-0000"
    assert body["node"] in ("n1", "n2")

    r = client.get("/map/1")
    assert r.status_code == 200


def test_rebalance_flow_over_http(client):
    client.post("/bootstrap", json={"nodes": ["n1", "n2"], "num_shards": 6})
    r = client.post("/rebalance",
                    json={"target_nodes": ["n1", "n2", "n3"],
                          "caller": "admin"})
    assert r.status_code == 200
    job_id = r.json()["job_id"]
    client.app.state.coordinator.executor.wait(10)
    r = client.get("/rebalance")
    assert r.json()["job_id"] == job_id
    assert r.json()["state"] == "committed"
    assert client.get("/map").json()["version"] > 1


def test_split_merge_over_http(client):
    client.post("/bootstrap", json={"nodes": ["n1"], "num_shards": 2})
    r = client.post("/split", json={"parent": "shard-0000",
                                    "children": ["a", "b"],
                                    "node": "n1", "caller": "admin"})
    assert r.status_code == 200
    assert "a" in r.json()["shards"]
    r = client.post("/merge", json={"children": ["a", "b"], "target": "m",
                                    "node": "n1", "caller": "admin"})
    assert r.status_code == 200
    assert "m" in r.json()["shards"]


def test_rejection_returns_reason(client):
    client.post("/bootstrap", json={"nodes": ["n1"], "num_shards": 2})
    client.post("/policies", json={"caller": "limited",
                                   "allowed_shards": ["shard-0000"],
                                   "max_concurrent_ops": 1})
    r = client.post("/split", json={"parent": "shard-0001",
                                    "children": ["x", "y"],
                                    "node": "n1", "caller": "limited"})
    assert r.status_code == 409
    assert r.json()["error"] == "unauthorized_shard"


def test_audit_endpoint(client):
    client.post("/bootstrap", json={"nodes": ["n1"], "num_shards": 2})
    r = client.get("/audit")
    assert r.status_code == 200
    assert any(e["op"] == "bootstrap" for e in r.json())
