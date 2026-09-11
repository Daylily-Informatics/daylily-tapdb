"""Interrupted takeover and lost release acknowledgement use explicit public recovery."""

from __future__ import annotations

import json
import subprocess
import sys
from contextlib import contextmanager
from uuid import uuid4

import pytest
from sqlalchemy import text

from daylily_tapdb.backup.receipts import read_receipts
from daylily_tapdb.backup.recovery import build_recovery_family, recovery_family_state
from daylily_tapdb.sequence_fence import acl_snapshot
from daylily_tapdb.sequences import (
    SequenceProtectionError,
    acquire_database_writer_fence,
    apply_sequence_advance_plan,
    apply_writer_fence_takeover,
    build_sequence_advance_plan,
    build_writer_fence_takeover_plan,
    reconcile_writer_fence_release,
    record_sequence_advance_outcome,
    release_database_writer_fence,
    validate_writer_quarantine,
)
from tests.test_sequence_protection_pg import allocator as allocator_fixture
from tests.test_sequence_protection_pg import capture
from tests.test_sequence_protection_pg import control_connection as control_fixture
from tests.test_sequence_protection_takeover_pg import kill_owner


@pytest.fixture(name="allocator")
def owned_allocator(pg_instance):
    yield from allocator_fixture.__wrapped__(pg_instance)


@pytest.fixture(name="control_connection")
def owned_control(pg_instance):
    yield from control_fixture.__wrapped__(pg_instance)


def finish(engine, target, control, receipt, directory):
    with engine.connect() as connection:
        with connection.begin():
            proof = validate_writer_quarantine(
                connection,
                control_connection=control,
                quarantine_receipt=receipt,
                receipts_dir=directory,
            )
            assert proof["ok"]
            current = capture(connection, target)
            reviewed = build_sequence_advance_plan(
                current, floors=receipt["retained_floors"]
            )
        acquired = acquire_database_writer_fence(
            connection,
            control_connection=control,
            inventory=current,
            receipts_dir=directory,
            quarantine_receipt=receipt,
        )
        with connection.begin():
            applied = apply_sequence_advance_plan(
                connection,
                reviewed,
                writer_fence=acquired["fence"],
                receipts_dir=directory,
            )
        result = record_sequence_advance_outcome(
            applied, receipts_dir=directory, outcome="committed", actor="tapdb_operator"
        )
        return release_database_writer_fence(
            connection,
            acquired,
            control_connection=control,
            result=result,
            receipts_dir=directory,
        )


def test_failed_reconnect_leaves_only_operator_quarantine_and_requires_new_review(
    allocator, control_connection, tmp_path
):
    engine, target = allocator
    with engine.begin() as connection:
        connection.execute(
            text(f'GRANT CONNECT ON DATABASE "{target["database"]}" TO PUBLIC')
        )
    origin = kill_owner(engine, target, control_connection, tmp_path, "uncommitted")
    reviewed = build_writer_fence_takeover_plan(
        control_connection,
        target=target,
        receipts_dir=tmp_path,
        fence_intent_receipt_id=origin.receipt_id,
    )

    @contextmanager
    def failed_factory():
        raise RuntimeError("injected operator reconnect failure")
        yield  # pragma: no cover

    with pytest.raises(RuntimeError, match="reconnect"):
        apply_writer_fence_takeover(
            control_connection,
            reviewed,
            target_connection_factory=failed_factory,
            receipts_dir=tmp_path,
        )
    with control_connection.begin():
        assert (
            control_connection.execute(
                text("SELECT datallowconn FROM pg_database WHERE datname=:name"),
                {"name": target["database"]},
            ).scalar_one()
            is True
        )
        current_acl = acl_snapshot(
            control_connection, reviewed["physical_target"]["database_oid"]
        )
        assert not any(
            row["grantee"] == 0 and row["privilege_type"] == "CONNECT"
            for row in current_acl["entries"]
        )
    # This is a new explicit reviewed operation, never an automatic retry.
    latest = next(
        item
        for item in reversed(read_receipts(tmp_path))
        if item.detail.get("phase") == "takeover_intent"
    )
    control_engine = control_connection.engine
    control_connection.close()
    with control_engine.connect() as new_control:
        with pytest.raises(SequenceProtectionError, match="latest unresolved"):
            apply_writer_fence_takeover(
                new_control,
                reviewed,
                target_connection_factory=lambda: engine.connect(),
                receipts_dir=tmp_path,
            )
        new_review = build_writer_fence_takeover_plan(
            new_control,
            target=target,
            receipts_dir=tmp_path,
            fence_intent_receipt_id=latest.receipt_id,
        )
        receipt = apply_writer_fence_takeover(
            new_control,
            new_review,
            target_connection_factory=lambda: engine.connect(),
            receipts_dir=tmp_path,
        )
        assert receipt["origin_fence_intent_receipt_id"] == origin.receipt_id
        assert receipt["predecessor_intent_receipt_id"] == latest.receipt_id
        assert (
            finish(engine, target, new_control, receipt, tmp_path)["phase"]
            == "released"
        )


RELEASE_CHILD = """
import json, os, sys
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import daylily_tapdb.sequence_fence as gate
from daylily_tapdb.sequences import *
p=json.load(sys.stdin)
engine=create_engine(p['dsn'],poolclass=NullPool,isolation_level='REPEATABLE READ')
control=create_engine(p['control_dsn'],poolclass=NullPool,isolation_level='REPEATABLE READ')
with engine.connect() as c,control.connect() as ctl:
    with c.begin():
        inv=capture_sequence_inventory(c,schema_name=p['target']['schema_name'],target=p['target'])
    acquired=acquire_database_writer_fence(c,control_connection=ctl,inventory=inv,receipts_dir=Path(p['journal']),recovery_family=p.get('family'))
    with c.begin():
        applied=apply_sequence_advance_plan(c,build_sequence_advance_plan(inv,floors=[],recovery_family=p.get('family')),writer_fence=acquired['fence'],receipts_dir=Path(p['journal']))
    result=record_sequence_advance_outcome(applied,receipts_dir=Path(p['journal']),outcome='committed',actor='tapdb_operator')
    original=gate.journal
    def die_before_ack(*args,**kwargs):
        if kwargs['phase']=='released':
            os._exit(74)
        return original(*args,**kwargs)
    gate.journal=die_before_ack
    release_database_writer_fence(c,acquired,control_connection=ctl,result=result,receipts_dir=Path(p['journal']))
"""


@pytest.mark.parametrize("family_bound", [False, True])
def test_release_commit_then_process_death_is_reconciled_without_database_mutation(
    allocator, control_connection, tmp_path, family_bound
):
    engine, target = allocator
    family = None
    if family_bound:
        with engine.begin() as connection:
            current = capture(connection, target)
        family = build_recovery_family(
            family_id=str(uuid4()), origin_inventory=current, receipts_dirs=[tmp_path]
        )
    payload = {
        "dsn": engine.url.render_as_string(hide_password=False),
        "control_dsn": control_connection.engine.url.render_as_string(
            hide_password=False
        ),
        "target": target,
        "journal": str(tmp_path),
        "family": family,
    }
    result = subprocess.run(
        [sys.executable, "-c", RELEASE_CHILD],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 74, result.stderr
    intent = read_receipts(tmp_path)[-1]
    assert intent.detail["phase"] == "release_intent"
    count = len(read_receipts(tmp_path))
    reviewed = reconcile_writer_fence_release(
        control_connection,
        release_intent_receipt_id=intent.receipt_id,
        receipts_dir=tmp_path,
        recovery_family=family,
    )
    assert reviewed["phase"] == "release_reconciliation_planned"
    assert len(read_receipts(tmp_path)) == count
    with pytest.raises(SequenceProtectionError, match="unchanged reviewed"):
        reconcile_writer_fence_release(
            control_connection,
            release_intent_receipt_id=intent.receipt_id,
            receipts_dir=tmp_path,
            dry_run=False,
            recovery_family=family,
        )
    receipt = reconcile_writer_fence_release(
        control_connection,
        release_intent_receipt_id=intent.receipt_id,
        receipts_dir=tmp_path,
        dry_run=False,
        preflight_receipt=reviewed,
        recovery_family=family,
    )
    assert receipt["phase"] == "release_reconciled"
    assert receipt["observation"] == "original_acl_and_open_gate"
    assert receipt["physical_target"] == intent.detail["physical_target"]
    if family is not None:
        assert receipt["recovery_family"] == family
        assert recovery_family_state(family)["pending"] == {}
    with pytest.raises(SequenceProtectionError, match="resolved"):
        reconcile_writer_fence_release(
            control_connection,
            release_intent_receipt_id=intent.receipt_id,
            receipts_dir=tmp_path,
            recovery_family=family,
        )


TAKEOVER_CHILD = """
import json, os, sys
from contextlib import contextmanager
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
import daylily_tapdb.sequence_fence as gate
from daylily_tapdb.sequences import apply_writer_fence_takeover
p=json.load(sys.stdin)
engine=create_engine(p['dsn'],poolclass=NullPool,isolation_level='REPEATABLE READ')
control=create_engine(p['control_dsn'],poolclass=NullPool,isolation_level='REPEATABLE READ')
original=gate.journal
def checkpoint(*args,**kwargs):
    phase=kwargs['phase']
    if (p['phase'],phase) in {('acl_commit','acl_quarantined'),('open_commit','quarantined')}:
        os._exit(75)
    result=original(*args,**kwargs)
    if (p['phase'],phase) in {('intent','takeover_intent'),('open_intent','quarantine_open_intent'),('quarantine_receipt','quarantined')}:
        os._exit(75)
    return result
gate.journal=checkpoint
if p['phase']=='closed_recapture':
    gate.capture_identity_inventory=lambda *a,**k: os._exit(75)
@contextmanager
def target_session():
    if p['phase']=='reconnect':
        os._exit(75)
    with engine.connect() as c:
        yield c
with control.connect() as ctl:
    apply_writer_fence_takeover(ctl,p['reviewed'],target_connection_factory=target_session,receipts_dir=Path(p['journal']))
raise RuntimeError('requested failure checkpoint was not reached')
"""


@pytest.mark.parametrize(
    "phase",
    [
        "intent",
        "acl_commit",
        "reconnect",
        "closed_recapture",
        "open_intent",
        "open_commit",
        "quarantine_receipt",
    ],
)
def test_every_takeover_process_loss_window_retains_closed_or_operator_only_gate(
    allocator, control_connection, tmp_path, phase
):
    engine, target = allocator
    with engine.begin() as connection:
        connection.execute(
            text(f'GRANT CONNECT ON DATABASE "{target["database"]}" TO PUBLIC')
        )
    origin = kill_owner(engine, target, control_connection, tmp_path, "uncommitted")
    reviewed = build_writer_fence_takeover_plan(
        control_connection,
        target=target,
        receipts_dir=tmp_path,
        fence_intent_receipt_id=origin.receipt_id,
    )
    payload = {
        "dsn": engine.url.render_as_string(hide_password=False),
        "control_dsn": control_connection.engine.url.render_as_string(
            hide_password=False
        ),
        "reviewed": reviewed,
        "journal": str(tmp_path),
        "phase": phase,
    }
    child = subprocess.run(
        [sys.executable, "-c", TAKEOVER_CHILD],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert child.returncode == 75, child.stderr
    with control_connection.begin():
        allowed = control_connection.execute(
            text("SELECT datallowconn FROM pg_database WHERE datname=:name"),
            {"name": target["database"]},
        ).scalar_one()
        if allowed:
            acl = acl_snapshot(
                control_connection, reviewed["physical_target"]["database_oid"]
            )
            assert not any(
                row["grantee"] == 0 and row["privilege_type"] == "CONNECT"
                for row in acl["entries"]
            )
        else:
            assert allowed is False
    with pytest.raises(SequenceProtectionError, match="latest unresolved"):
        apply_writer_fence_takeover(
            control_connection,
            reviewed,
            target_connection_factory=lambda: engine.connect(),
            receipts_dir=tmp_path,
        )
    latest = next(
        row
        for row in reversed(read_receipts(tmp_path))
        if row.detail.get("phase") == "takeover_intent"
    )
    new_review = build_writer_fence_takeover_plan(
        control_connection,
        target=target,
        receipts_dir=tmp_path,
        fence_intent_receipt_id=latest.receipt_id,
    )
    quarantine = apply_writer_fence_takeover(
        control_connection,
        new_review,
        target_connection_factory=lambda: engine.connect(),
        receipts_dir=tmp_path,
    )
    assert quarantine["origin_fence_intent_receipt_id"] == origin.receipt_id
    assert max(row["value"] for row in quarantine["retained_floors"]) == 23
    assert (
        finish(engine, target, control_connection, quarantine, tmp_path)["phase"]
        == "released"
    )
