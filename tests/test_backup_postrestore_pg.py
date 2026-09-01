"""Post-restore verification and rehearsal against a real PostgreSQL.

Two things are proven for each check: that it passes on a genuinely good
restore, and that it *fails* when the thing it names is actually broken. A
check that only ever passes is worse than no check, because it reads as
assurance.

Runs against the ``pg_instance`` fixture -- an ephemeral cluster under pytest's
tmp dir, torn down afterwards.
"""

from __future__ import annotations

import shutil

import pytest
from sqlalchemy import text
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.backup import introspect, postrestore, service, verify
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db_config import get_backup_settings, get_db_config

runner = CliRunner()

pytestmark = pytest.mark.skipif(
    not shutil.which("pg_dump") or not shutil.which("pg_restore"),
    reason="pg_dump/pg_restore not on PATH",
)


@pytest.fixture(autouse=True)
def _context(pg_instance, monkeypatch):
    monkeypatch.setenv("HOME", str(pg_instance["base"]))
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        config_path=pg_instance["config_path"],
    )
    monkeypatch.setattr(cli_mod, "PID_FILE", pg_instance["base"] / "ui.pid")
    monkeypatch.setattr(cli_mod, "LOG_FILE", pg_instance["base"] / "ui.log")
    yield
    clear_cli_context()


@pytest.fixture(scope="module")
def _schema_applied(pg_instance):
    config = str(pg_instance["config_path"])
    applied = runner.invoke(app, ["--config", config, "db", "schema", "apply"])
    assert applied.exit_code == 0, applied.output
    seeded = runner.invoke(
        app, ["--config", config, "db", "data", "seed", "--skip-existing"]
    )
    assert seeded.exit_code == 0, seeded.output
    return True


@pytest.fixture
def env(pg_instance, _schema_applied, tmp_path):
    cfg = dict(get_db_config())
    settings = dict(get_backup_settings())
    settings["config_dir"] = str(tmp_path)
    settings["storage_uri"] = f"file://{tmp_path / 'store'}"
    return cfg, settings


@pytest.fixture
def backup(env):
    cfg, settings = env
    return service.create_backup(cfg, settings)


@pytest.fixture
def restored(env, backup):
    """A real isolated restore, dropped afterwards."""
    cfg, settings = env
    result = verify.restore_backup(cfg, settings, backup_id=backup.backup_id)
    yield result
    from daylily_tapdb.backup import engine as eng

    eng.run_command(
        eng.build_psql_command(
            cfg,
            sql=f'DROP DATABASE IF EXISTS "{result.target_database}"',
            database="postgres",
        ),
        env=eng.client_env(cfg),
    )


def _session(cfg, database, schema):
    probe = dict(cfg, database=database, schema_name=schema)
    return service.open_session(probe, app_username="pytest")


# ---------------------------------------------------------------------------
# the suite on a good restore
# ---------------------------------------------------------------------------


def test_every_plan_check_is_present(env, backup, restored):
    cfg, settings = env
    ids = {c.id for c in restored.checks}

    # Plan section 3.6 names eight checks; these are those plus two the
    # implementation splits out (EUID format, prefix projection).
    for required in (
        "rowcounts.exact",
        "refs.template_instance",
        "lineage.integrity",
        "audit.continuity",
        "euid.uniqueness",
        "euid.format",
        "sequences.high_water",
        "sequences.prefix_projection",
        "schema.drift",
        "objects.representative",
    ):
        assert required in ids, f"post-restore suite is missing {required}"


def test_a_good_restore_passes_everything(env, restored):
    failures = [c.to_payload() for c in restored.checks if c.failed]

    assert failures == []
    assert restored.ok


def test_no_check_silently_skips_on_a_real_restore(env, restored):
    # A skip is legitimate for absent objects, but a suite that is mostly
    # skips is not verifying a restore.
    statuses = [c.status for c in restored.checks]

    assert statuses.count("pass") >= 8


# ---------------------------------------------------------------------------
# each check must be able to fail
# ---------------------------------------------------------------------------


def test_rowcounts_detects_a_missing_row(env, backup, restored):
    cfg, settings = env
    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup.backup_id)
    manifest = service._load_manifest(storage, prefix)
    manifest.row_counts["generic_template"] += 1  # claim one more than exists

    with _session(cfg, restored.target_database, cfg["schema_name"]) as conn:
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_rowcounts(session, manifest, cfg["schema_name"])

    assert check.failed
    assert "generic_template" in check.data


def test_template_references_detects_an_orphan(env, restored):
    """The check defends against a restore that lost its constraints.

    ``generic_instance.template_uid`` has a foreign key, so an orphan cannot
    be created while that constraint stands. Dropping it first is exactly the
    scenario the check exists for: an artifact restored without its
    constraints would otherwise look healthy on row counts alone.
    """
    cfg, _ = env
    schema = cfg["schema_name"]

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=True) as session:
            session.execute(
                text(
                    f'ALTER TABLE "{schema}".generic_instance '
                    "DROP CONSTRAINT IF EXISTS generic_instance_template_uid_fkey"
                )
            )
            session.execute(
                text(
                    f'UPDATE "{schema}".generic_instance '
                    "SET template_uid = 999999999 "
                    f'WHERE uid = (SELECT min(uid) FROM "{schema}".generic_instance)'
                )
            )

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_template_references(session, schema)

    assert check.failed
    assert check.data["orphans"] >= 1


def test_euid_uniqueness_detects_a_cross_table_duplicate(env, restored):
    """The real risk is duplication *across* tables.

    Each table has a UNIQUE constraint on ``euid``, so a within-table
    duplicate is impossible. Nothing prevents the same EUID appearing in two
    different tables, though -- which is precisely why the check unions across
    every EUID-bearing table instead of trusting per-table constraints.
    """
    cfg, _ = env
    schema = cfg["schema_name"]

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=True) as session:
            template_euid = session.execute(
                text(f'SELECT euid FROM "{schema}".generic_template LIMIT 1')
            ).scalar()
            session.execute(
                text(
                    f'UPDATE "{schema}".generic_instance SET euid = :e '
                    f'WHERE uid = (SELECT min(uid) FROM "{schema}".generic_instance)'
                ),
                {"e": template_euid},
            )

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_euid_uniqueness(session, schema)

    assert check.failed
    assert check.data["duplicates"] >= 1


def test_sequence_high_water_detects_a_regression(env, backup, restored):
    cfg, settings = env
    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup.backup_id)
    manifest = service._load_manifest(storage, prefix)

    from daylily_tapdb.backup.manifest import SequenceState

    # Claim the backup had a far higher value than the restore carries: this
    # is exactly the shape of an EUID-reuse regression.
    manifest.sequences = [
        SequenceState(name="tpx_instance_seq", last_value=10**9, is_called=True)
    ]

    with _session(cfg, restored.target_database, cfg["schema_name"]) as conn:
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_sequence_high_water(
                session, manifest, cfg["schema_name"]
            )

    assert check.failed
    assert "tpx_instance_seq" in check.data


def test_prefix_projection_warns_but_does_not_block(env, restored):
    """A sequence behind its data is advisory, not a failure.

    ``ensure_instance_prefix_sequence`` reconciles to
    ``GREATEST(max(euid_seq) + 1, current)`` before issuing, so this is
    self-healing. It also arises normally after re-running ``db schema
    apply``, and failing on it would block recovery for a condition TAPDB
    fixes itself.
    """
    cfg, _ = env
    schema = cfg["schema_name"]

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=True) as session:
            session.execute(text(f"SELECT setval('\"{schema}\".tpx_instance_seq', 1)"))

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_prefix_sequences_ahead(session, schema)

    assert check.status == "warn"
    assert not check.failed
    assert "tpx" in check.data


def test_high_water_is_the_binding_no_reuse_guarantee(env, backup, restored):
    """The check that *does* block: a restore must not lose sequence progress.

    Unlike the projection above, this compares against what the backup
    recorded, and nothing reconciles a restore that silently rewound.
    """
    cfg, settings = env
    schema = cfg["schema_name"]
    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup.backup_id)
    manifest = service._load_manifest(storage, prefix)

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=True) as session:
            session.execute(text(f"SELECT setval('\"{schema}\".tpx_instance_seq', 1)"))
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_sequence_high_water(session, manifest, schema)

    assert check.failed
    assert "tpx_instance_seq" in check.data


def test_schema_drift_detects_a_hand_made_tapdb_object(env, restored):
    """The tripwire for changes made outside the migration path.

    Only TAPDB-namespaced names are flagged: a foreign table co-located in the
    schema by another application is deliberately tolerated (it is still
    captured and restored, it simply is not part of TAPDB's expected
    inventory). A ``tapdb_``-prefixed table created by hand is the case this
    guards.
    """
    cfg, _ = env
    schema = cfg["schema_name"]

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=True) as session:
            session.execute(
                text(
                    f'CREATE TABLE "{schema}".tapdb_hand_made (uid bigint primary key)'
                )
            )

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_schema_drift(session, cfg, schema)

    assert check.failed
    assert check.data["unexpected"]["tables"] >= 1


def test_schema_drift_tolerates_a_foreign_table(env, restored):
    cfg, _ = env
    schema = cfg["schema_name"]

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=True) as session:
            session.execute(
                text(
                    f'CREATE TABLE "{schema}".some_other_apps_table '
                    "(uid bigint primary key)"
                )
            )

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_schema_drift(session, cfg, schema)

    # Refusing here would block recovery for a table TAPDB does not own and
    # has nonetheless backed up correctly.
    assert not check.failed


def test_representative_objects_skips_non_addressable_samples(env, backup, restored):
    cfg, settings = env
    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup.backup_id)
    manifest = service._load_manifest(storage, prefix)

    with _session(cfg, restored.target_database, cfg["schema_name"]) as conn:
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_representative_objects(session, manifest)

    # audit_log carries EUIDs but records events, not objects; requiring the
    # object lookup to resolve one would fail for an irrelevant reason.
    assert check.status in ("pass", "skip")
    if check.status == "pass":
        assert check.data["checked"] > 0


def test_representative_objects_detects_an_unresolvable_euid(env, backup, restored):
    cfg, settings = env
    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup.backup_id)
    manifest = service._load_manifest(storage, prefix)
    manifest.representative_objects = [
        {"table": "generic_template", "euid": "TPX_NOPE", "euid_seq": 1}
    ]

    with _session(cfg, restored.target_database, cfg["schema_name"]) as conn:
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_representative_objects(session, manifest)

    assert check.failed
    assert "TPX_NOPE" in check.data["unresolved"]


# ---------------------------------------------------------------------------
# rehearsal
# ---------------------------------------------------------------------------


def test_rehearsal_verifies_and_tears_down(env, backup):
    cfg, settings = env

    evidence = verify.rehearse_restore(cfg, settings, backup_id=backup.backup_id)

    assert evidence.ok, [c.to_payload() for c in evidence.checks if c.failed]
    assert evidence.checks
    assert not evidence.kept

    from daylily_tapdb.backup import engine as eng

    found = eng.run_command(
        eng.build_psql_command(
            cfg,
            sql=f"SELECT 1 FROM pg_database WHERE datname = '{evidence.database}'",
            database="postgres",
        ),
        env=eng.client_env(cfg),
    )
    assert found.stdout.strip() == "", "rehearsal database was not torn down"


def test_rehearsal_is_recorded_as_a_rehearsal_not_a_restore(env, backup):
    """The restore trail must answer "what has been restored here?" honestly.

    A rehearsal never touches live data; logging it as a restore would put
    drills into the record an operator reads during an incident.
    """
    from daylily_tapdb.backup.receipts import read_receipts

    cfg, settings = env
    verify.rehearse_restore(cfg, settings, backup_id=backup.backup_id)

    receipts = read_receipts(service.receipts_directory(settings))
    operations = [r.operation for r in receipts]

    assert "backup_rehearse" in operations
    assert "backup_restore" not in operations


def test_a_failed_rehearsal_is_recorded_as_failed(env, backup, monkeypatch):
    from daylily_tapdb.backup.receipts import read_receipts

    cfg, settings = env

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated restore failure")

    monkeypatch.setattr(verify, "_restore_archive", _boom)
    verify.rehearse_restore(cfg, settings, backup_id=backup.backup_id)

    rehearsals = [
        r
        for r in read_receipts(service.receipts_directory(settings))
        if r.operation == "backup_rehearse"
    ]

    assert rehearsals and not rehearsals[-1].succeeded


def test_rehearsal_writes_durable_evidence(env, backup, tmp_path):
    import json

    cfg, settings = env

    evidence = verify.rehearse_restore(cfg, settings, backup_id=backup.backup_id)

    assert evidence.evidence_key
    stored = json.loads((tmp_path / "store" / evidence.evidence_key).read_text())
    assert stored["backup_id"] == backup.backup_id
    assert stored["ok"] is True
    assert stored["checks"]


def test_rehearsal_evidence_lands_under_the_documented_key(env, backup):
    cfg, settings = env

    evidence = verify.rehearse_restore(cfg, settings, backup_id=backup.backup_id)

    assert "/rehearsals/" in evidence.evidence_key
    assert evidence.evidence_key.endswith(".json")
    assert backup.backup_id in evidence.evidence_key


def test_rehearsal_leaves_live_data_untouched(env, backup):
    from daylily_tapdb.backup import introspect

    cfg, settings = env

    def counts():
        with service.open_session(cfg, app_username="pytest") as conn:
            with conn.session_scope(commit=False) as session:
                return introspect.capture_row_counts(session, cfg["schema_name"])

    before = counts()
    verify.rehearse_restore(cfg, settings, backup_id=backup.backup_id)

    assert counts() == before


def test_rehearsal_keeps_the_database_when_asked(env, backup):
    cfg, settings = env

    evidence = verify.rehearse_restore(
        cfg, settings, backup_id=backup.backup_id, keep=True
    )

    assert evidence.kept
    from daylily_tapdb.backup import engine as eng

    eng.run_command(
        eng.build_psql_command(
            cfg,
            sql=f'DROP DATABASE IF EXISTS "{evidence.database}"',
            database="postgres",
        ),
        env=eng.client_env(cfg),
    )


def test_rehearsal_writes_evidence_even_when_it_fails(env, backup, monkeypatch):
    import json

    cfg, settings = env

    def _boom(*args, **kwargs):
        raise RuntimeError("simulated restore failure")

    monkeypatch.setattr(verify, "_restore_archive", _boom)

    evidence = verify.rehearse_restore(cfg, settings, backup_id=backup.backup_id)

    # A failed rehearsal is the most valuable kind; losing its record would
    # defeat the purpose of running one.
    assert not evidence.ok
    assert evidence.error
    assert evidence.evidence_key
    stored = json.loads(
        (
            __import__("pathlib").Path(settings["config_dir"])
            / "store"
            / evidence.evidence_key
        ).read_text()
    )
    assert stored["ok"] is False
    assert stored["error"]


def test_rehearsal_dry_run_creates_nothing(env, backup):
    cfg, settings = env

    evidence = verify.rehearse_restore(
        cfg, settings, backup_id=backup.backup_id, dry_run=True
    )

    assert evidence.dry_run
    from daylily_tapdb.backup import engine as eng

    found = eng.run_command(
        eng.build_psql_command(
            cfg,
            sql=f"SELECT 1 FROM pg_database WHERE datname = '{evidence.database}'",
            database="postgres",
        ),
        env=eng.client_env(cfg),
    )
    assert found.stdout.strip() == ""


def test_high_water_compares_next_value_not_last_value(env, backup, restored):
    """The comparison that makes no-EUID-reuse true.

    The discriminating case: a restored sequence whose `last_value` *equals*
    what the backup recorded but whose `is_called` is false. It is therefore
    poised to reissue the value the backup says was already handed out, while
    a `last_value` comparison sees them as equal and reports healthy. That is
    the exact bug found and fixed once already; reverting the comparison
    survived every test.
    """
    cfg, settings = env
    schema = cfg["schema_name"]
    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup.backup_id)
    manifest = service._load_manifest(storage, prefix)

    recorded = next(
        (
            seq
            for seq in manifest.sequences
            if seq.is_called and seq.last_value is not None
        ),
        None,
    )
    assert recorded is not None, "no called sequence in the manifest to exercise"

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=True) as session:
            # Same last_value, is_called flipped -> next value is one lower.
            session.execute(
                text(
                    f'SELECT setval(\'"{schema}"."{recorded.name}"\', '
                    f"{recorded.last_value}, false)"
                )
            )
        with conn.session_scope(commit=False) as session:
            live = {q.name: q for q in introspect.capture_sequences(session, schema)}
            check = postrestore.check_sequence_high_water(session, manifest, schema)

    current = live[recorded.name]
    assert current.last_value == recorded.last_value, "premise: last_value matches"
    assert current.next_value < recorded.next_value, "premise: next_value is behind"
    assert check.failed, (
        "a sequence poised to reissue an already-issued value passed "
        f"({check.to_payload()})"
    )


def test_audit_continuity_uses_next_value_not_last_value(env, backup, restored):
    """The identity sequence must be ahead of max(uid), measured correctly.

    `audit_log_uid_seq` at `last_value=N, is_called=false` issues N next, so
    with `max(uid)=N` the next audit insert collides on the primary key --
    while `last_value < max_uid` reports healthy. This is a *blocking* check
    that gates the in-place rollback, so it asserted something positively
    false.
    """
    cfg, settings = env
    schema = cfg["schema_name"]
    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup.backup_id)
    manifest = service._load_manifest(storage, prefix)

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=False) as session:
            max_uid = session.execute(
                text(f'SELECT max(uid) FROM "{schema}".audit_log')
            ).scalar()
        assert max_uid, "no audit rows to exercise the check"

        with conn.session_scope(commit=True) as session:
            # Poised to issue exactly max(uid): the next insert collides.
            session.execute(
                text(
                    f"SELECT setval('\"{schema}\".audit_log_uid_seq', "
                    f"{int(max_uid)}, false)"
                )
            )
        with conn.session_scope(commit=False) as session:
            check = postrestore.check_audit_continuity(session, manifest, schema)

    assert check.failed, f"a colliding identity sequence passed: {check.to_payload()}"


def test_a_failing_lookup_reports_the_cause_not_just_the_symptom(env, restored):
    """ "12 of 12 do not resolve" is true and useless.

    When every sample fails it is almost never the data -- it is the lookup
    path erroring identically each time. Against a real Aurora restore this
    reported total non-resolution when the cause was
    `column generic_template.validator_ref does not exist`: a schema older
    than the code. An operator chasing that message would look for missing
    rows instead of a schema version mismatch.
    """
    cfg, settings = env
    schema = cfg["schema_name"]
    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, restored.backup_id)
    manifest = service._load_manifest(storage, prefix)

    import daylily_tapdb.services.object_lookup as lookup

    def _boom(session, euid):
        raise RuntimeError("simulated lookup failure")

    with _session(cfg, restored.target_database, schema) as conn:
        with conn.session_scope(commit=False) as session:
            real = lookup.find_object_by_euid
            lookup.find_object_by_euid = _boom
            try:
                check = postrestore.check_representative_objects(session, manifest)
            finally:
                lookup.find_object_by_euid = real

    assert check.failed
    assert "simulated lookup failure" in check.detail, check.detail
    assert check.data.get("lookup_error"), check.data
