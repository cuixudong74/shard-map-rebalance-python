"""Req 6: per-caller shard scope, concurrency quota, distinguishable
rejections BEFORE any map change, full audit trail."""
import pytest

from shardmap.auth import CallerPolicy
from shardmap.models import ErrorCode, ShardMapError
from shardmap.service import ShardMapService


def make_service(tmp_path):
    return ShardMapService(
        str(tmp_path / "state"), ["n1", "n2"], num_shards=4,
        seed_keys_per_shard=4,
        policies={"alice": CallerPolicy(("shard-000",), max_concurrent_ops=1)})


def test_out_of_scope_shard_rejected_before_mutation(tmp_path):
    svc = make_service(tmp_path)
    before = svc.get_map().version
    with pytest.raises(ShardMapError) as e:
        svc.split("alice", "shard-0012")  # outside 'shard-000' prefix
    assert e.value.code is ErrorCode.PERMISSION_DENIED
    assert svc.get_map().version == before  # map untouched


def test_quota_rejected_with_distinguishable_code(tmp_path):
    svc = make_service(tmp_path)
    svc.dataplane._item_delay = 0.05
    svc.rebalance("alice", ["n1", "n2", "n3"])  # occupies alice's single slot
    before = svc.get_map().version
    with pytest.raises(ShardMapError) as e:
        svc.split("alice", "shard-0000")
    assert e.value.code is ErrorCode.QUOTA_EXCEEDED
    assert svc.get_map().version == before


def test_rejection_reasons_are_distinguishable(tmp_path):
    svc = make_service(tmp_path)
    with pytest.raises(ShardMapError) as e1:
        svc.split("alice", "other-shard")
    with pytest.raises(ShardMapError) as e2:
        svc.split("alice", "shard-0009")  # in scope but unknown
    assert e1.value.code is ErrorCode.PERMISSION_DENIED
    assert e2.value.code is ErrorCode.NOT_FOUND
    assert e1.value.code != e2.value.code


def test_audit_trail_records_allowed_and_denied(tmp_path):
    svc = make_service(tmp_path)
    with pytest.raises(ShardMapError):
        svc.split("alice", "other-shard")
    svc.split("alice", "shard-0000")
    entries = svc.audit.entries()
    denied = [e for e in entries if not e["ok"]]
    allowed = [e for e in entries if e["ok"] and e["action"] == "split"]
    assert denied and denied[0]["code"] == "permission_denied"
    assert allowed and allowed[0]["caller"] == "alice"
