"""Authorization, concurrency quotas, and audit trail."""
import pytest

from shardmap.coordinator import Coordinator
from shardmap.errors import RejectedError, RejectReason
from shardmap.quotas import CallerPolicy, QuotaManager


@pytest.fixture()
def coord(tmp_path):
    c = Coordinator(tmp_path / "d")
    c.bootstrap(["n1", "n2"], 4)
    return c


def test_unauthorized_shard_rejected_before_mutation(coord):
    coord.set_policy(CallerPolicy(caller="limited",
                                  allowed_shards=frozenset({"shard-0000"})))
    before = coord.store.current_version
    with pytest.raises(RejectedError) as ei:
        coord.split("shard-0001", ("x1", "x2"), "n1", caller="limited")
    assert ei.value.reason is RejectReason.UNAUTHORIZED_SHARD
    assert coord.store.current_version == before  # nothing changed


def test_unauthorized_rebalance_rejected(coord):
    coord.set_policy(CallerPolicy(caller="limited",
                                  allowed_shards=frozenset({"shard-0000"})))
    before = coord.store.current_version
    # rebalancing to 3 nodes would move shards outside the caller's range
    with pytest.raises(RejectedError) as ei:
        coord.start_rebalance(["n1", "n2", "n3"], caller="limited")
    assert ei.value.reason is RejectReason.UNAUTHORIZED_SHARD
    assert coord.store.current_version == before


def test_authorized_caller_succeeds(coord):
    coord.set_policy(CallerPolicy(caller="limited",
                                  allowed_shards=frozenset(
                                      {"shard-0000", "s0a", "s0b"})))
    mv = coord.split("shard-0000", ("s0a", "s0b"), "n1", caller="limited")
    assert mv.owner_of("s0a") == "n1"


def test_concurrency_quota_enforced_and_released():
    q = QuotaManager()
    q.set_policy(CallerPolicy(caller="c", max_concurrent_ops=2))
    q.acquire("c")
    q.acquire("c")
    with pytest.raises(RejectedError) as ei:
        q.acquire("c")
    assert ei.value.reason is RejectReason.CONCURRENCY_EXCEEDED
    q.release("c")
    q.acquire("c")  # slot freed


def test_rejection_reasons_are_distinguishable(coord):
    coord.set_policy(CallerPolicy(caller="limited", allowed_shards=frozenset()))
    with pytest.raises(RejectedError) as e1:
        coord.split("shard-0000", ("a", "b"), "n1", caller="limited")
    with pytest.raises(RejectedError) as e2:
        coord.split("missing", ("a", "b"), "n1", caller="admin")
    assert e1.value.reason is RejectReason.UNAUTHORIZED_SHARD
    assert e2.value.reason is RejectReason.NOT_FOUND
    assert e1.value.reason != e2.value.reason


def test_audit_trail_records_operations(coord):
    coord.split("shard-0000", ("s0a", "s0b"), "n1", caller="admin")
    coord.merge(("s0a", "s0b"), "sm", "n2", caller="admin")
    ops = [e["op"] for e in coord.audit.entries()]
    assert "bootstrap" in ops
    assert "split_start" in ops
    assert "merge_start" in ops
    entries = coord.audit.entries()
    assert all("ts" in e and "caller" in e for e in entries)


def test_audit_log_is_bounded():
    from shardmap.audit import AuditLog
    log = AuditLog(capacity=10)
    for i in range(100):
        log.record("op", "c", {"i": i})
    assert len(log.entries()) == 10
