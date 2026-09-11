"""Real process death and explicit operator-quarantine recovery, never cleanup proof."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from contextlib import contextmanager

import pytest
from sqlalchemy import text

from daylily_tapdb.backup.receipts import read_receipts
from daylily_tapdb.sequence_fence import acl_snapshot
from daylily_tapdb.sequences import (
    acquire_database_writer_fence,
    apply_sequence_advance_plan,
    apply_writer_fence_takeover,
    build_sequence_advance_plan,
    build_writer_fence_takeover_plan,
    record_sequence_advance_outcome,
    release_database_writer_fence,
    sequence_next_value,
)
from tests.test_sequence_protection_pg import allocator as allocator_fixture
from tests.test_sequence_protection_pg import capture
from tests.test_sequence_protection_pg import control_connection as control_fixture


@pytest.fixture(name="allocator")
def owned_allocator(pg_instance):
    yield from allocator_fixture.__wrapped__(pg_instance)


@pytest.fixture(name="control_connection")
def owned_control(pg_instance):
    yield from control_fixture.__wrapped__(pg_instance)


CHILD = """
import json, os, sys
from pathlib import Path
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool
from daylily_tapdb.sequences import *
p = json.load(sys.stdin)
engine = create_engine(p['dsn'], poolclass=NullPool, isolation_level='REPEATABLE READ')
control = create_engine(p['control_dsn'], poolclass=NullPool, isolation_level='REPEATABLE READ')
with engine.connect() as c, control.connect() as ctl:
    with c.begin():
        inventory = capture_sequence_inventory(c, schema_name=p['target']['schema_name'], target=p['target'])
    gate = acquire_database_writer_fence(c, control_connection=ctl, inventory=inventory, receipts_dir=Path(p['journal']))
    if p['phase'] != 'acquire':
        tx = c.begin()
        plan = build_sequence_advance_plan(inventory, floors=[{'name':'objects_uid_seq','value':20,'source':'reviewed_external_reservation'}])
        apply_sequence_advance_plan(c, plan, writer_fence=gate['fence'], receipts_dir=Path(p['journal']))
        if p['phase'] == 'commit':
            tx.commit()
    os._exit(73)
"""


def kill_owner(engine, target, control_connection, directory, phase):
    payload = {
        "dsn": engine.url.render_as_string(hide_password=False),
        "control_dsn": control_connection.engine.url.render_as_string(
            hide_password=False
        ),
        "target": target,
        "journal": str(directory),
        "phase": phase,
    }
    result = subprocess.run(
        [sys.executable, "-c", CHILD],
        input=json.dumps(payload),
        text=True,
        capture_output=True,
        timeout=30,
        env=os.environ.copy(),
        check=False,
    )
    assert result.returncode == 73, result.stderr
    return next(
        item
        for item in read_receipts(directory)
        if item.detail.get("phase") == "acquire_intent"
    )


@pytest.mark.parametrize("phase", ["acquire", "uncommitted", "commit"])
def test_new_process_takeover_preserves_all_floors_and_restores_exact_acl(
    allocator, control_connection, tmp_path, phase
):
    engine, target = allocator
    with engine.begin() as connection:
        connection.execute(
            text(f'GRANT CONNECT ON DATABASE "{target["database"]}" TO PUBLIC')
        )
        initial = capture(connection, target)
        original_acl = acl_snapshot(
            connection, initial["physical_target"]["database_oid"]
        )
    origin = kill_owner(engine, target, control_connection, tmp_path, phase)
    reviewed = build_writer_fence_takeover_plan(
        control_connection,
        target=target,
        receipts_dir=tmp_path,
        fence_intent_receipt_id=origin.receipt_id,
    )
    assert reviewed["observed_connections_allowed"] is False
    assert reviewed["original_acl"] == original_acl

    @contextmanager
    def target_session():
        with engine.connect() as connection:
            yield connection

    receipt = apply_writer_fence_takeover(
        control_connection,
        reviewed,
        target_connection_factory=target_session,
        receipts_dir=tmp_path,
    )
    assert receipt["phase"] == "quarantined"
    assert receipt["old_operation_outcome"] == "unresolved_observed_only"
    assert receipt["requires_new_review"] is True
    if phase != "acquire":
        assert max(floor["value"] for floor in receipt["retained_floors"]) == 23
    with engine.connect() as connection:
        with connection.begin():
            assert (
                connection.execute(
                    text(
                        "SELECT datallowconn FROM pg_database WHERE datname=current_database()"
                    )
                ).scalar_one()
                is True
            )
            acl = acl_snapshot(connection, initial["physical_target"]["database_oid"])
            assert not any(
                item["grantee"] == 0 and item["privilege_type"] == "CONNECT"
                for item in acl["entries"]
            )
            current = capture(connection, target)
            fresh = build_sequence_advance_plan(
                current, floors=receipt["retained_floors"]
            )
        acquired = acquire_database_writer_fence(
            connection,
            control_connection=control_connection,
            inventory=current,
            receipts_dir=tmp_path,
            quarantine_receipt=receipt,
        )
        with connection.begin():
            result = apply_sequence_advance_plan(
                connection, fresh, writer_fence=acquired["fence"], receipts_dir=tmp_path
            )
        committed = record_sequence_advance_outcome(
            result, receipts_dir=tmp_path, outcome="committed", actor="tapdb_operator"
        )
        released = release_database_writer_fence(
            connection,
            acquired,
            control_connection=control_connection,
            result=committed,
            receipts_dir=tmp_path,
        )
        assert released["phase"] == "released"
        with connection.begin():
            assert (
                acl_snapshot(connection, initial["physical_target"]["database_oid"])[
                    "entries"
                ]
                == original_acl["entries"]
            )
            if phase != "acquire":
                assert (
                    sequence_next_value(capture(connection, target)["sequences"][0])
                    == 26
                )
