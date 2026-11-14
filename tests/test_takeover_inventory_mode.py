"""Pure checks for explicit compact evidence across takeover review/apply."""

from contextlib import nullcontext
from types import SimpleNamespace

import pytest

from daylily_tapdb import sequence_fence as fence
from daylily_tapdb.audit_inventory import MODE
from daylily_tapdb.identity_inventory import IdentityInventoryError, seal_receipt
from tests.test_sequence_protection import inventory


@pytest.mark.parametrize("compact", [False, True])
def test_takeover_plan_seals_mode_and_apply_restores_it_before_mutation(
    monkeypatch, tmp_path, compact
):
    target = dict(inventory()["target"])
    if compact:
        target["inventory_mode"] = MODE
    target["inventory_limits"] = {
        "max_rows": 3_000_000,
        "max_receipt_bytes": 128 * 1024**2,
        "max_row_bytes": 8 * 1024**2,
    }
    origin = {
        "phase": "acquire_intent",
        "original_acl": {"entries": []},
        "provider_evidence": {"provider_role": "provider"},
        "physical_target": inventory()["physical_target"],
        "connection_backend": {"pid": 1, "backend_start": "reviewed"},
        "sequence_mappings": {},
    }
    epoch = SimpleNamespace(receipt_id="original-intent", detail=origin)
    control = SimpleNamespace(in_transaction=lambda: False, begin=nullcontext)
    monkeypatch.setattr(fence, "read_fence_history", lambda path: ([epoch], None))
    monkeypatch.setattr(fence, "active_epoch", lambda *a: epoch)
    monkeypatch.setattr(fence, "retained_floors", lambda *a: [])
    monkeypatch.setattr(fence, "control_state", lambda *a: {
        "database_oid": 12345, "operator_role": "operator",
        "control_database": "postgres", "datallowconn": False,
    })
    for name in ("lock_session", "unlock_session", "_verify_origin", "census", "_restorable_acl"):
        monkeypatch.setattr(fence, name, lambda *a, **k: None)
    monkeypatch.setattr(fence, "provider_evidence", lambda *a, **k: {
        "provider_role": "provider", "provider_contract": {},
    })
    monkeypatch.setattr(fence, "worker_baseline", lambda *a, **k: {})
    monkeypatch.setattr(fence, "acl_snapshot", lambda *a: {"entries": []})
    plan = fence.build_writer_fence_takeover_plan(
        control, target=target, receipts_dir=tmp_path,
        fence_intent_receipt_id="original-intent",
    )
    assert plan.get("inventory_mode") == (MODE if compact else None)
    assert plan["inventory_limits"] == target["inventory_limits"]

    class BeforeMutation(Exception):
        pass

    def replan(*args, **kwargs):
        assert kwargs["target"].get("inventory_mode") == (MODE if compact else None)
        assert kwargs["target"]["inventory_limits"] == target["inventory_limits"]
        raise BeforeMutation

    monkeypatch.setattr(fence, "build_writer_fence_takeover_plan", replan)
    with pytest.raises(BeforeMutation):
        fence.apply_writer_fence_takeover(
            control, plan, receipts_dir=tmp_path,
            target_connection_factory=lambda: pytest.fail("No target connection allowed"),
        )


@pytest.mark.parametrize("mode", [None, "unsupported"])
def test_unknown_explicit_mode_refused_before_control_or_journal_access(tmp_path, mode):
    target = dict(inventory()["target"], inventory_mode=mode)
    control = SimpleNamespace(in_transaction=lambda: False)
    with pytest.raises(IdentityInventoryError, match="Unknown explicit inventory_mode"):
        fence.build_writer_fence_takeover_plan(
            control, target=target, receipts_dir=tmp_path,
            fence_intent_receipt_id="unobserved",
        )
    plan = seal_receipt({
        "schema_version": fence.TAKEOVER_VERSION, "phase": "planned",
        "source_receipts_dir": str(tmp_path), "target": inventory()["target"],
        "sequence_mappings": {}, "inventory_mode": mode,
    })
    with pytest.raises(IdentityInventoryError, match="Unknown explicit inventory_mode"):
        fence.apply_writer_fence_takeover(
            control, plan, receipts_dir=tmp_path,
            target_connection_factory=lambda: pytest.fail("No target connection allowed"),
        )
