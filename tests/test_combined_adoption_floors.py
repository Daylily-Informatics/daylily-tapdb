"""Authored regression checks for combined adoption; no live service fixtures."""

from contextlib import contextmanager, nullcontext
from types import SimpleNamespace

import pytest

from daylily_tapdb.identity_inventory import seal_receipt
from daylily_tapdb import adoption_allocators, integrity_lifecycle, sequences
from daylily_tapdb.backup.receipts import Actor, read_receipts, write_receipt
from daylily_tapdb.backup.recovery import retained_recovery_state


def inventory(*, last=8, called=False, assigned=None, cache=1, maximum=101):
    return seal_receipt({
        "schema_version": "tapdb-sequence-inventory/v1", "schema_name": "objects",
        "target": {"engine_type": "local", "host": "localhost", "port": 15438,
                   "database": "explicit_fixture", "schema_name": "objects",
                   "config_identity": "/tmp/explicit-combined-adoption.yaml",
                   "domain_code": "Z", "owner_repo_name": "test-owner"},
        "physical_target": {"database": "explicit_fixture", "database_oid": 12345,
                            "server_address": "127.0.0.1", "server_port": 15438},
        "sequence_mappings": {}, "missing_generators": [],
        "sequences": [{"name": "template_uid_seq", "increment_by": 3,
                       "min_value": 2, "start_value": 2, "max_value": maximum,
                       "cache_size": cache, "cycle": False, "last_value": last,
                       "is_called": called, "allocated_floor": last if called else None,
                       "assigned_floor": assigned, "owner": "operator", "dependencies": [],
                       "mapping": {"kind": "owned_column", "columns": []}}],
    })


def advance_plan(**kwargs):
    return sequences.build_sequence_advance_plan(
        inventory(**kwargs), floors=[{"name": "template_uid_seq", "value": 8,
                                      "source": "sequence_advance_intent"}])


def test_equality_is_advanced_before_reserved_template_allocation():
    plan = advance_plan()
    assert not sequences.verify_sequence_floors(plan["inventory"], floors=plan["floors"])["ok"]
    assert plan["advances"] == [{"name": "template_uid_seq", "floor": 8, "next_value": 11}]
    budget = sequences.build_sequence_allocation_reservation(
        plan, allocation_counts={"template_uid_seq": 2})
    assert budget["floors"] == [{"name": "template_uid_seq", "value": 17,
                                  "source": "native_allocation_reservation"}]


def test_reservation_covers_nonunit_increment_and_full_cache_block():
    budget = sequences.build_sequence_allocation_reservation(
        advance_plan(cache=4), allocation_counts={"template_uid_seq": 2})
    assert budget["floors"][0]["value"] == 23
    with pytest.raises(sequences.SequenceProtectionError, match="capacity"):
        sequences.build_sequence_allocation_reservation(
            advance_plan(cache=4, maximum=20), allocation_counts={"template_uid_seq": 2})


def test_shared_native_prefix_aggregates_template_and_audit_calls():
    base = inventory()["sequences"][0]
    states = []
    for table in ("generic_template", "audit_log"):
        states.append({**base, "name": table + "_uid_seq", "mapping": {
            "kind": "owned_column", "columns": [{"schema_name": "objects",
            "table_name": table, "column_name": "uid", "dependency_type": "i"}]}})
    for prefix in ("TPX", "WX", "WSX", "XX", "AY", "MSG", "GVR", "XRF"):
        states.append({**base, "name": prefix.lower() + "_instance_seq",
                       "mapping": {"kind": "prefix", "prefix": prefix}})
    conn = SimpleNamespace(execute=lambda *_: SimpleNamespace(mappings=lambda: [
        {"entity": "audit_log", "prefix": "TPX"},
        {"entity": "generic_template", "prefix": "TPX"}]))
    cfg = {"schema_name": "objects", "domain_code": "Z", "owner_repo_name": "test-owner"}
    templates = [{"instance_prefix": "GVR"}, {"instance_prefix": "XRF"}]
    paths = adoption_allocators.allocation_paths(conn, cfg, {"sequences": states}, templates)
    assert paths["allocation_counts"] == {
        "audit_log_uid_seq": 2, "generic_template_uid_seq": 2, "tpx_instance_seq": 4}
    with pytest.raises(sequences.SequenceProtectionError, match="creation is unsupported"):
        adoption_allocators.allocation_paths(conn, cfg, {"sequences": states[:-1]}, templates)


@pytest.mark.parametrize("unsupported", ["domain", "table_am", "default", "index_function", "index_am"])
def test_allocation_surface_stops_unsupported_insert_hooks_read_only(monkeypatch, unsupported):
    relations = [{"oid": i, "name": name, "kind": "r", "partition": False,
                  "access_method": "custom" if unsupported == "table_am" else "heap",
                  "handler_schema": "pg_catalog"}
                 for i, name in enumerate(("audit_log", "generic_template"), 1)]
    types = [{"column_name": name, "type_name": typename, "type_schema": "pg_catalog",
              "type_kind": "d" if unsupported == "domain" else "b", "collation_schema": None}
             for name, typename in (("uid", "int8"), ("euid", "text"))]
    columns = [{"name": "uid", "identity": "d", "generated": "", "default": None},
               {"name": "euid", "identity": "", "generated": "",
                "default": "custom_allocator()" if unsupported == "default" else None}]
    reads = []

    def execute(statement, *_):
        sql = str(statement)
        reads.append(sql)
        assert sql.startswith(("SELECT", "WITH"))
        if sql.startswith("SELECT c.oid"):
            return SimpleNamespace(mappings=lambda: relations)
        if sql.startswith("SELECT a.attname"):
            return SimpleNamespace(mappings=lambda: types)
        if sql.startswith("WITH expressions"):
            return SimpleNamespace(scalar_one=lambda: int(unsupported == "index_function"))
        if sql.startswith("SELECT count(*) FROM pg_index"):
            return SimpleNamespace(scalar_one=lambda: int(unsupported == "index_am"))
        return SimpleNamespace(scalar_one=lambda: 0, mappings=lambda: [])

    monkeypatch.setattr(adoption_allocators, "catalog_capture_context", lambda *_, **__: nullcontext())
    monkeypatch.setattr(adoption_allocators, "catalog_columns", lambda *_, **__: columns)
    with pytest.raises(sequences.SequenceProtectionError):
        adoption_allocators.capture_allocation_surface(SimpleNamespace(execute=execute), {"schema_name": "objects"})
    assert reads


def pending_fixture(tmp_path, *, cache=1):
    plan = advance_plan(cache=cache)
    reservation = sequences.build_sequence_allocation_reservation(
        plan, allocation_counts={"template_uid_seq": 2})
    floors = plan["floors"] + [{"name": "template_uid_seq", "value": 11,
                               "source": "sequence_advance_intent"}] + reservation["floors"]
    intent = write_receipt(tmp_path, operation="sequence_advance", status="intent",
        actor=Actor("cli", "operator"), detail={"phase": "intent", "plan": plan,
        "floors": floors, "allocation_reservation": reservation, "operation_sha256": "a" * 64})
    before = inventory(last=11, cache=cache)
    result = seal_receipt({"schema_version": "tapdb-sequence-apply/v1",
        "phase": "applied_pending_commit", "plan_sha256": plan["sha256"],
        "intent_receipt_id": intent.receipt_id, "inventory": before,
        "verification": sequences.verify_sequence_floors(before, floors=plan["floors"]),
        "floors": floors, "allocation_reservation": reservation, "operation_sha256": "a" * 64})
    write_receipt(tmp_path, operation="sequence_advance", status="pending_commit",
        actor=Actor("cli", "operator"), detail={"phase": "applied_pending_commit",
        "result": result, "floors": floors})
    return intent, result


def finalize(monkeypatch, tmp_path, result, after, count=2):
    monkeypatch.setattr(sequences, "capture_sequence_inventory", lambda *_, **__: after)
    monkeypatch.setattr(sequences, "validate_writer_fence", lambda *_, **__: {"operator_role": "operator"})
    return sequences.finalize_sequence_allocation(
        object(), result, writer_fence={}, receipts_dir=tmp_path,
        operation_sha256="a" * 64, adoption_epoch="00000000-0000-0000-0000-000000000001",
        allocation_counts={"template_uid_seq": count})


@pytest.mark.parametrize(("cache", "last", "assigned"), [(1, 14, 14), (4, 20, 14)])
def test_final_receipt_has_actual_inventory_and_original_recovery_linkage(
    monkeypatch, tmp_path, cache, last, assigned
):
    intent, pending = pending_fixture(tmp_path, cache=cache)
    after = inventory(last=last, called=True, assigned=assigned, cache=cache)
    final = finalize(monkeypatch, tmp_path, pending, after)
    assert final["inventory"] == after != pending["inventory"]
    assert final["intent_receipt_id"] == intent.receipt_id
    assert final["plan_sha256"] == pending["plan_sha256"]
    assert final["preallocation_result_sha256"] == pending["sha256"]
    assert final["verification"]["floors"] == pending["verification"]["floors"]
    committed = sequences.record_sequence_advance_outcome(
        final, receipts_dir=tmp_path, outcome="committed", actor="operator")
    assert committed["inventory"] == after
    assert sequences._prior_sequence_intent_resolved(read_receipts(tmp_path), intent)
    retained = retained_recovery_state(tmp_path, target=after["target"])
    assert all(floor in retained["floors"] for floor in pending["allocation_reservation"]["floors"])


def test_skipped_templates_keep_allocator_state_but_preserve_reservations(monkeypatch, tmp_path):
    _, pending = pending_fixture(tmp_path)
    final = finalize(monkeypatch, tmp_path, pending, pending["inventory"], count=0)
    assert final["inventory"] == pending["inventory"]
    assert final["floors"] == pending["floors"]


def test_pretemplate_result_cannot_be_committed(tmp_path):
    _, pending = pending_fixture(tmp_path)
    with pytest.raises(sequences.SequenceProtectionError, match="actual final inventory"):
        sequences.record_sequence_advance_outcome(
            pending, receipts_dir=tmp_path, outcome="committed", actor="operator")


def test_excess_allocation_stops_with_original_durable_reservations(monkeypatch, tmp_path):
    intent, pending = pending_fixture(tmp_path)
    with pytest.raises(sequences.SequenceProtectionError, match="reservation"):
        finalize(monkeypatch, tmp_path, pending, inventory(last=17, called=True, assigned=17))
    assert not sequences._prior_sequence_intent_resolved(read_receipts(tmp_path), intent)
    assert all(floor in retained_recovery_state(tmp_path, target=pending["inventory"]["target"])["floors"]
               for floor in pending["allocation_reservation"]["floors"])


def test_changed_history_cannot_be_resealed_as_less_history(monkeypatch, tmp_path):
    _, pending = pending_fixture(tmp_path)
    checkpoint = adoption_allocators._checkpoint(tmp_path)
    history = {"receipts_dir": str(tmp_path), "recovery_family": None,
               "checkpoints": {str(tmp_path): checkpoint}, "floors": [], "inventory_sha256": []}
    with pytest.raises(sequences.SequenceProtectionError, match="omitted or changed"):
        adoption_allocators.require_adoption_history(history, pending["inventory"])


def test_only_exact_own_acquisition_suffix_is_accepted(monkeypatch, tmp_path):
    target, physical = inventory()["target"], inventory()["physical_target"]
    fence = {"intent_receipt_id": "original-intent", "target": target, "physical_target": physical}
    rows = [SimpleNamespace(receipt_id="original-intent", operation="sequence_writer_fence", detail={
                "phase": "acquire_intent", "target": target, "physical_target": physical}),
            SimpleNamespace(receipt_id="original-acquired", operation="sequence_writer_fence", detail={
                "phase": "acquired", "receipt": fence})]
    monkeypatch.setattr(adoption_allocators, "read_fence_history", lambda _: (rows, {"tip": "acquired"}))
    monkeypatch.setattr(adoption_allocators, "retained_recovery_state", lambda *_, **__: {
        "floors": [], "inventories": []})
    history = {"receipts_dir": str(tmp_path), "recovery_family": None,
               "checkpoints": {str(tmp_path): {"head": None, "receipts": []}},
               "floors": [], "inventory_sha256": []}
    adoption_allocators.require_adoption_history(history, inventory(), fence_receipt=fence)
    rows.append(SimpleNamespace(receipt_id="unrelated", operation="sequence_writer_fence", detail={}))
    with pytest.raises(sequences.SequenceProtectionError, match="Unrelated journal activity"):
        adoption_allocators.require_adoption_history(history, inventory(), fence_receipt=fence)


@pytest.mark.parametrize("surface_drift", [False, True])
def test_real_apply_checks_surface_then_advances_before_any_asset(monkeypatch, tmp_path, surface_drift):
    advance = advance_plan()
    surface = seal_receipt({"schema_version": "tapdb-adoption-allocation-surface/v1", "relations": []})
    paths = {"allocation_counts": {"template_uid_seq": 2}}
    snapshot = {"format": "tapdb.integrity-adoption/v2", "sequences": advance["inventory"],
                "new_core_templates": [{}, {}]}
    plan = seal_receipt({"schema_version": "tapdb.integrity-adoption/v2", **snapshot,
                        "history": {"receipts_dir": str(tmp_path), "recovery_family": None,
                                    "floors": advance["floors"]},
                        "allocator_plan": advance, "allocation_paths": paths,
                        "allocation_surface": surface,
                        "allocation_reservation": sequences.build_sequence_allocation_reservation(
                            advance, allocation_counts=paths["allocation_counts"])})
    fence = seal_receipt({"schema_version": "tapdb-writer-fence/v1", "phase": "acquired", "fence": {}})
    events = []
    conn = SimpleNamespace(execute=lambda *_: events.append("lock"),
                           exec_driver_sql=lambda *_: pytest.fail("DDL occurred before reviewed advancement"))
    monkeypatch.setattr(integrity_lifecycle, "_context", lambda *_, **__: None)
    monkeypatch.setattr(integrity_lifecycle, "validate_writer_fence", lambda *_, **__: None)
    monkeypatch.setattr(integrity_lifecycle, "_adoption_snapshot", lambda *_, **__: snapshot)
    monkeypatch.setattr(integrity_lifecycle, "capture_allocation_surface", lambda *_, **__: {} if surface_drift else surface)
    monkeypatch.setattr(integrity_lifecycle, "require_adoption_history", lambda *_, **__: events.append("history"))
    monkeypatch.setattr(integrity_lifecycle, "allocation_paths", lambda *_, **__: paths)

    def advance_stops(*_, **__):
        events.append("advance")
        raise RuntimeError("intent/advance stopped")

    monkeypatch.setattr(sequences, "apply_sequence_advance_plan", advance_stops)
    with pytest.raises((ValueError, RuntimeError), match="surface changed|intent/advance stopped"):
        integrity_lifecycle.apply_adoption(conn, {"schema_name": "objects"}, plan=plan,
                                           fence_receipt=fence, receipts_dir=tmp_path)
    assert ("advance" in events) is not surface_drift


@pytest.mark.parametrize("failure", ["apply", "commit", "outcome", None])
def test_adoption_commits_once_before_outcome_and_never_releases_failure(monkeypatch, tmp_path, failure):
    from daylily_tapdb import runtime_principal
    events = []
    plan = seal_receipt({"schema_version": "tapdb.integrity-adoption/v2",
                        "history": {"receipts_dir": str(tmp_path), "recovery_family": None},
                        "sequences": inventory()})

    class Connection:
        @contextmanager
        def begin(self):
            events.append("begin")
            try:
                yield self
                if failure == "commit":
                    raise RuntimeError("commit acknowledgement unknown")
                events.append("commit")
            except BaseException:
                events.append("transaction_exit_failure")
                raise

    @contextmanager
    def session(*_, **__):
        yield Connection()

    def apply(*_, **__):
        events.append("advance_then_adopt_and_finalize")
        if failure == "apply":
            raise RuntimeError("native allocation stopped")
        return {"allocator_receipt": {"allocation_finalized": True}}

    def record(*_, **__):
        events.append("outcome")
        if failure == "outcome":
            raise OSError("external outcome unavailable")
        return {"phase": "committed"}

    monkeypatch.setattr(runtime_principal, "operator_session", session)
    monkeypatch.setattr(integrity_lifecycle, "require_adoption_history", lambda *_, **__: None)
    monkeypatch.setattr(integrity_lifecycle, "apply_adoption", apply)
    monkeypatch.setattr(sequences, "acquire_database_writer_fence",
                        lambda *_, **__: events.append("acquire") or {"sha256": "b" * 64})
    monkeypatch.setattr(sequences, "record_sequence_advance_outcome", record)
    monkeypatch.setattr(sequences, "release_database_writer_fence", lambda *_, **__: events.append("release"))
    invoke = lambda: integrity_lifecycle.adopt_integrity(
        {"database": "fixture", "operator_user": "operator"}, {"database": "control"},
        plan=plan, receipts_dir=tmp_path)
    if failure:
        with pytest.raises((RuntimeError, OSError)):
            invoke()
        assert "release" not in events
        if failure in {"apply", "commit"}:
            assert "outcome" not in events
    else:
        assert invoke()["fence_released"] is True
        assert events == ["acquire", "begin", "advance_then_adopt_and_finalize", "commit", "outcome", "release"]
