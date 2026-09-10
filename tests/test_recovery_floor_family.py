"""Explicit physical lineage, not database names, governs recovery floors."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest

from daylily_tapdb.backup.errors import BackupVerificationError
from daylily_tapdb.backup.receipts import read_receipts, write_receipt
from daylily_tapdb.backup.recovery import (
    begin_recovery,
    build_recovery_family,
    finish_recovery,
    observe_recovery,
    recovery_family_state,
    require_family_member,
    require_retained_definitions,
    validate_recovery_family,
)
from daylily_tapdb.identity_inventory import seal_receipt
from daylily_tapdb.sequences import build_sequence_advance_plan
from tests.test_recovery_floor import ACTOR, TARGET, inventory

FAMILY_ID = "01111111-2222-4333-8444-555555555555"


def _family(tmp_path):
    roots = [tmp_path / name for name in ("origin", "replacement-a", "replacement-b")]
    for root in roots:
        root.mkdir()
    return build_recovery_family(
        family_id=FAMILY_ID, origin_inventory=inventory(), receipts_dirs=roots
    ), roots


def _replacement(name, oid, *, last=22):
    result = deepcopy(inventory(last=last))
    result["target"]["database"] = name
    result["physical_target"].update(database=name, database_oid=oid)
    return seal_receipt(result)


def _observe(monkeypatch, root, intent, captured):
    monkeypatch.setattr(
        "daylily_tapdb.identity_inventory.physical_target",
        lambda connection, target: captured["physical_target"],
    )
    return observe_recovery(object(), root, intent, actor=ACTOR, inventory=captured)


def test_builder_is_read_only_and_preserves_explicit_roots(tmp_path):
    family, roots = _family(tmp_path)
    assert family["family_id"] == FAMILY_ID
    assert family["receipts_dirs"] == sorted(str(root) for root in roots)
    assert family["origin"]["physical_target"] == inventory()["physical_target"]
    assert all(list(root.iterdir()) == [] for root in roots)
    assert validate_recovery_family(family, required_directory=roots[1]) == family


@pytest.mark.parametrize("value", ["", "not-a-uuid", None, 123, FAMILY_ID.upper()])
def test_builder_rejects_noncanonical_uuid(tmp_path, value):
    # Use alphabetic UUID digits so upper-case has a distinct representation.
    if value == FAMILY_ID.upper():
        value = "AAAAAAAA-2222-4333-8444-555555555555"
    with pytest.raises(BackupVerificationError, match="UUID"):
        build_recovery_family(
            family_id=value, origin_inventory=inventory(), receipts_dirs=[tmp_path]
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "checksum",
        "version",
        "extra",
        "origin",
        "physical",
        "oid",
        "port",
        "address",
        "target",
    ],
)
def test_descriptor_rejects_noncanonical_or_tampered_identity(tmp_path, mutation):
    family, _ = _family(tmp_path)
    changed = deepcopy(family)
    if mutation == "checksum":
        changed["family_id"] = "aaaaaaaa-2222-4333-8444-555555555555"
    elif mutation == "version":
        changed["schema_version"] = "unsupported"
    elif mutation == "extra":
        changed["inferred_root"] = "forbidden"
    elif mutation == "origin":
        changed["origin"]["extra"] = "forbidden"
    elif mutation == "physical":
        changed["origin"]["physical_target"]["guessed"] = 1
    elif mutation == "oid":
        changed["origin"]["physical_target"]["database_oid"] = True
    elif mutation == "port":
        changed["origin"]["physical_target"]["server_port"] = 5433
    elif mutation == "address":
        changed["origin"]["physical_target"]["server_address"] = ""
    else:
        changed["origin"]["target"]["sequence_mappings"] = {}
    if mutation != "checksum":
        changed = seal_receipt(changed)
    with pytest.raises(BackupVerificationError):
        validate_recovery_family(changed)


@pytest.mark.parametrize(
    "kind", ["missing", "relative", "duplicate", "string", "empty", "unsorted"]
)
def test_family_roots_are_never_discovered_or_silently_dropped(tmp_path, kind):
    family, roots = _family(tmp_path)
    values = {
        "missing": [str(tmp_path / "missing")],
        "relative": ["relative"],
        "duplicate": [str(roots[0]), str(roots[0])],
        "string": str(roots[0]),
        "empty": [],
        "unsorted": list(reversed(family["receipts_dirs"])),
    }
    with pytest.raises(BackupVerificationError):
        validate_recovery_family(
            seal_receipt({**family, "receipts_dirs": values[kind]})
        )
    with pytest.raises(BackupVerificationError, match="outside"):
        validate_recovery_family(family, required_directory=tmp_path)


def test_family_rejects_corrupt_declared_journal(tmp_path):
    family, roots = _family(tmp_path)
    (roots[1] / "000001-invalid.json").write_text("invalid", encoding="utf-8")
    with pytest.raises(BackupVerificationError, match="unreadable"):
        recovery_family_state(family)


def test_same_database_name_different_oid_is_not_a_family_member(tmp_path):
    family, roots = _family(tmp_path)
    impostor = _replacement(TARGET["database"], 999, last=988)
    plan = build_sequence_advance_plan(impostor, floors=[])
    write_receipt(
        roots[1],
        operation="sequence_advance",
        status="intent",
        actor=ACTOR,
        detail={
            "phase": "intent",
            "plan": plan,
            "floors": [{"name": "history_uid_seq", "value": 999, "source": "foreign"}],
        },
    )
    with pytest.raises(BackupVerificationError, match="not an evidenced"):
        require_family_member(family, impostor)
    state = recovery_family_state(family)
    assert state["floors"] == []
    assert state["inventories"] == []
    assert state["members"] == [family["origin"]]


def test_second_replacement_retains_first_replacement_aborted_floors(
    monkeypatch, tmp_path
):
    family, roots = _family(tmp_path)
    first = _replacement("replacement_a", 201, last=78)
    intent = begin_recovery(
        roots[1],
        target=first["target"],
        inventories=[inventory()],
        evidence={"purpose": "fenced_source_recovery"},
        actor=ACTOR,
        recovery_family=family,
    )
    with pytest.raises(BackupVerificationError, match="unresolved"):
        recovery_family_state(family)
    assert first["physical_target"] not in [
        item["physical_target"]
        for item in recovery_family_state(family, require_terminal=False)["members"]
    ]
    _observe(monkeypatch, roots[1], intent, first)
    finish_recovery(
        roots[1],
        intent,
        phase="aborted",
        actor=ACTOR,
        inventory=first,
        reservations=[
            {"name": "history_uid_seq", "value": 85, "source": "aborted_attempt"}
        ],
    )
    require_family_member(family, first)

    second = _replacement("replacement_b", 202)
    next_intent = begin_recovery(
        roots[2],
        target=second["target"],
        inventories=[inventory()],
        evidence={"purpose": "fenced_source_recovery"},
        actor=ACTOR,
        recovery_family=family,
    )
    assert max(item["value"] for item in next_intent["floors"]) == 85
    assert first in next_intent["inventories"]
    _observe(monkeypatch, roots[2], next_intent, second)
    finish_recovery(
        roots[2], next_intent, phase="aborted", actor=ACTOR, inventory=second
    )
    state = recovery_family_state(family)
    assert len(state["members"]) == 3
    assert max(item["value"] for item in state["floors"]) == 85
    assert all(
        row.detail["recovery_family"] == family
        for root in roots
        for row in read_receipts(root)
    )


def test_new_generator_from_first_replacement_cannot_disappear(monkeypatch, tmp_path):
    family, roots = _family(tmp_path)
    first = _replacement("replacement_a", 201)
    first["sequences"].append(
        inventory(name="new_history_uid_seq", last=78)["sequences"][0]
    )
    first = seal_receipt(first)
    intent = begin_recovery(
        roots[1],
        target=first["target"],
        inventories=[inventory()],
        evidence={"purpose": "fenced_source_recovery"},
        actor=ACTOR,
        recovery_family=family,
    )
    _observe(monkeypatch, roots[1], intent, first)
    finish_recovery(roots[1], intent, phase="aborted", actor=ACTOR, inventory=first)
    state = recovery_family_state(family)
    assert any(item["name"] == "new_history_uid_seq" for item in state["floors"])
    with pytest.raises(BackupVerificationError, match="retained generator is missing"):
        require_retained_definitions(inventory(), state["inventories"])


def test_family_descriptor_cannot_fork_with_same_uuid(tmp_path):
    family, roots = _family(tmp_path)
    altered = seal_receipt({**family, "receipts_dirs": [str(roots[0])]})
    write_receipt(
        roots[0],
        operation="backup_create",
        status="succeeded",
        actor=ACTOR,
        detail={"recovery_family": altered},
    )
    with pytest.raises(BackupVerificationError, match="changed within"):
        recovery_family_state(family)


def test_observation_cannot_join_without_matching_durable_intent(monkeypatch, tmp_path):
    family, roots = _family(tmp_path)
    first = _replacement("replacement_a", 201)
    intent = begin_recovery(
        roots[1],
        target=first["target"],
        inventories=[inventory()],
        evidence={"purpose": "fenced_source_recovery"},
        actor=ACTOR,
        recovery_family=family,
    )
    with pytest.raises(BackupVerificationError, match="pending recovery intent"):
        _observe(monkeypatch, roots[1], {**intent, "intent_sha256": "changed"}, first)
    monkeypatch.setattr(
        "daylily_tapdb.identity_inventory.physical_target",
        lambda *_: inventory()["physical_target"],
    )
    with pytest.raises(BackupVerificationError, match="physical identity is invalid"):
        observe_recovery(object(), roots[1], intent, actor=ACTOR, inventory=first)


def test_terminal_outcome_cannot_invent_a_replacement_member(tmp_path):
    family, roots = _family(tmp_path)
    first = _replacement("replacement_a", 201)
    intent = begin_recovery(
        roots[1],
        target=first["target"],
        inventories=[inventory()],
        evidence={"purpose": "fenced_source_recovery"},
        actor=ACTOR,
        recovery_family=family,
    )
    with pytest.raises(BackupVerificationError, match="not an evidenced"):
        finish_recovery(
            roots[1], intent, phase="committed", actor=ACTOR, inventory=first
        )


def test_origin_pre_family_pending_recovery_blocks_new_family_attempt(tmp_path):
    family, roots = _family(tmp_path)
    intent = begin_recovery(
        roots[0],
        target=TARGET,
        inventories=[inventory()],
        evidence={"purpose": "fenced_migration"},
        actor=ACTOR,
    )
    with pytest.raises(BackupVerificationError, match="unresolved"):
        recovery_family_state(family)
    finish_recovery(
        roots[0], intent, phase="aborted", actor=ACTOR, inventory=inventory(last=78)
    )
    assert max(item["value"] for item in recovery_family_state(family)["floors"]) == 78


def test_pending_allocator_reservations_require_observed_or_known_outcome(tmp_path):
    family, roots = _family(tmp_path)
    plan = build_sequence_advance_plan(inventory(), floors=[])
    floor = {
        "name": "history_uid_seq",
        "value": 29,
        "source": "sequence_advance_intent",
    }
    intent = write_receipt(
        roots[0],
        operation="sequence_advance",
        status="intent",
        actor=ACTOR,
        detail={"phase": "intent", "plan": plan, "floors": [floor]},
    )
    with pytest.raises(BackupVerificationError, match="unresolved"):
        recovery_family_state(family)
    quarantine = seal_receipt(
        {
            "schema_version": "tapdb-writer-fence-takeover/v1",
            "phase": "quarantined",
            **family["origin"],
            "sequence_inventory": inventory(),
            "recovery_family": family,
        }
    )
    write_receipt(
        roots[0],
        operation="sequence_writer_fence",
        status="succeeded",
        actor=ACTOR,
        detail={
            "phase": "quarantined",
            "receipt": quarantine,
            "recovery_family": family,
        },
    )
    write_receipt(
        roots[0],
        operation="sequence_advance",
        status="reconciled_observed",
        actor=ACTOR,
        detail={
            "phase": "reconciled_observed",
            "intent_receipt_id": intent.receipt_id,
            "plan_sha256": plan["sha256"],
            "target": TARGET,
            "observed_inventory": inventory(),
            "floors": [floor],
            "prior_transaction_outcome": "unknown",
            "reconciliation_receipt_sha256": quarantine["sha256"],
            "recovery_family": family,
        },
    )
    state = recovery_family_state(family)
    assert not state["pending"]
    assert floor in state["floors"]
    assert inventory() in state["inventories"]


def test_family_tags_are_scoped_to_the_explicit_journal_root(tmp_path):
    family, roots = _family(tmp_path)
    moment = datetime(2026, 9, 10, tzinfo=UTC)
    unrelated = write_receipt(
        roots[0],
        operation="sequence_writer_fence",
        status="succeeded",
        actor=ACTOR,
        detail={"phase": "acl_quarantined", "target": TARGET},
        now=moment,
    )
    tagged = write_receipt(
        roots[1],
        operation="sequence_writer_fence",
        status="succeeded",
        actor=ACTOR,
        detail={"phase": "acquired", **family["origin"], "recovery_family": family},
        now=moment,
    )
    assert unrelated.receipt_id == tagged.receipt_id
    assert recovery_family_state(family)["members"] == [family["origin"]]


def test_allocator_outcome_cannot_resolve_another_roots_identical_receipt_id(tmp_path):
    family, roots = _family(tmp_path)
    moment = datetime(2026, 9, 10, tzinfo=UTC)
    plan = build_sequence_advance_plan(inventory(), floors=[])
    intents = [
        write_receipt(
            root,
            operation="sequence_advance",
            status="intent",
            actor=ACTOR,
            detail={"phase": "intent", "plan": plan, "floors": []},
            now=moment,
        )
        for root in roots[:2]
    ]
    assert intents[0].receipt_id == intents[1].receipt_id
    write_receipt(
        roots[0],
        operation="sequence_advance",
        status="committed",
        actor=ACTOR,
        detail={
            "phase": "committed",
            "result": {
                "inventory": inventory(),
                "intent_receipt_id": intents[0].receipt_id,
            },
            "floors": [],
        },
    )
    state = recovery_family_state(family, require_terminal=False)
    assert state["pending"] == {
        f"{roots[1]}:allocator:{intents[1].receipt_id}": intents[1].receipt_id
    }
    with pytest.raises(BackupVerificationError, match="unresolved"):
        recovery_family_state(family)


def test_rehearsal_plan_binds_family_floors_but_not_unrelated_journal_heads(tmp_path):
    from daylily_tapdb.backup import service, verify

    settings = {"config_dir": str(tmp_path)}
    directory = service.receipts_directory(settings)
    directory.mkdir(parents=True)
    family = build_recovery_family(
        family_id=FAMILY_ID, origin_inventory=inventory(), receipts_dirs=[directory]
    )
    cfg = {**TARGET, "config_path": TARGET["config_identity"]}

    def plan():
        return verify._recovery_plan_evidence(
            cfg,
            settings,
            target_database="replacement",
            target_schema=TARGET["schema_name"],
            recovery_source={
                "purpose": "isolated_rehearsal",
                "recovery_family": family,
            },
            writer_fence=None,
        )

    before = plan()
    write_receipt(directory, operation="backup_verify", status="succeeded", actor=ACTOR)
    assert plan() == before
    intent = begin_recovery(
        directory,
        target=TARGET,
        inventories=[inventory()],
        evidence={"purpose": "fenced_migration"},
        actor=ACTOR,
        recovery_family=family,
    )
    finish_recovery(
        directory, intent, phase="aborted", actor=ACTOR, inventory=inventory(last=78)
    )
    assert plan()["family_state_sha256"] != before["family_state_sha256"]
