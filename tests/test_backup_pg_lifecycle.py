"""End-to-end backup lifecycle against a real PostgreSQL.

The other pg test files each prove one layer works. This one proves the
properties issue #89 actually asks for, at the level an operator would
recognise them -- and deliberately does not restate what is already covered
elsewhere. In particular, in-place rollback at both failure points is proven in
``test_backup_restore_pg.py`` and is not repeated here.

What is here is the integration-level evidence that was missing:

* **scope** -- a table in another schema is not captured;
* **isolation** -- a neighbouring schema survives an in-place restore byte for
  byte;
* **read-only** -- plan and verify move no rows, no sequences and write no
  audit entries;
* **refusal before mutation** -- a corrupt archive and a backward version are
  both refused with a canary row still in place;
* **sequence continuity** -- an instance created after a restore gets an EUID
  strictly beyond anything the backup knew about, so no identifier is reused;
* **no residue** -- a completed staged swap leaves exactly one schema.

Runs against the ``pg_instance`` fixture -- an ephemeral cluster under pytest's
tmp dir, torn down afterwards. Nothing here touches a shared or remote
database.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from sqlalchemy import text
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.backup import engine, introspect, service, verify
from daylily_tapdb.backup import manifest as manifest_mod
from daylily_tapdb.backup.errors import BackupError, BackupVerificationError
from daylily_tapdb.backup.receipts import read_receipts, verify_receipt_chain
from daylily_tapdb.backup.verify import MODE_IN_PLACE, RestoreOptions
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


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _exec(
    cfg,
    sql: str,
    *,
    commit: bool = True,
    database: str | None = None,
    schema_name: str | None = None,
    connection_role: str = "runtime",
):
    """Run a statement, returning rows when there are any.

    DDL returns no result set, and asking one for rows raises rather than
    yielding an empty list -- so branch on ``returns_rows`` instead.
    """
    probe = dict(cfg)
    if database:
        probe["database"] = database
    if schema_name:
        probe["schema_name"] = schema_name
    with service.open_session(
        probe, app_username="pytest", connection_role=connection_role
    ) as conn:
        with conn.session_scope(commit=commit) as session:
            result = session.execute(text(sql))
            return result.fetchall() if result.returns_rows else []


def _state(cfg, *, database: str | None = None):
    """Row counts and sequence high-water marks for the configured schema."""
    probe = dict(cfg, database=database) if database else cfg
    schema = str(cfg["schema_name"])
    with service.open_session(probe, app_username="pytest") as conn:
        with conn.session_scope(commit=False) as session:
            return (
                introspect.capture_row_counts(session, schema),
                {
                    s.name: s.last_value
                    for s in introspect.capture_sequences(session, schema)
                },
            )


def _schemas(cfg):
    return {
        r[0]
        for r in _exec(
            cfg,
            "SELECT schema_name FROM information_schema.schemata",
            commit=False,
            connection_role="operator",
        )
    }


def _max_euid_seq(cfg, *, database: str | None = None, prefix: str = "GVR") -> int:
    schema = str(cfg["schema_name"])
    rows = _exec(
        cfg,
        f'SELECT COALESCE(MAX(euid_seq), 0) FROM "{schema}".generic_instance '
        f"WHERE euid_prefix = '{prefix}'",
        commit=False,
        database=database,
    )
    return int(rows[0][0])


def _template_uid(cfg, *, database: str | None = None, prefix: str = "GVR") -> int:
    """A template whose instances carry ``prefix``.

    The EUID prefix comes from the template, not from a column the caller sets
    -- ``set_generic_instance_euid`` raises without one -- so an insert has to
    go through a real template the way the application does.
    """
    schema = str(cfg["schema_name"])
    rows = _exec(
        cfg,
        f'SELECT uid FROM "{schema}".generic_template '
        f"WHERE instance_prefix = '{prefix}' ORDER BY uid LIMIT 1",
        commit=False,
        database=database,
    )
    assert rows, f"no template with instance_prefix {prefix}"
    return int(rows[0][0])


def _new_instance(cfg, *, database: str | None = None, name: str, prefix: str = "GVR"):
    """Insert an instance and return its trigger-assigned EUID and sequence."""
    schema = str(cfg["schema_name"])
    template_uid = _template_uid(cfg, database=database, prefix=prefix)
    rows = _exec(
        cfg,
        f'INSERT INTO "{schema}".generic_instance '
        "(template_uid, name, polymorphic_discriminator, category, type, "
        "subtype, version, bstatus) "
        f"VALUES ({template_uid}, '{name}', 'generic_instance', 'governance', "
        "'validator', 'lifecycle-test', '1', 'active') "
        "RETURNING euid, euid_seq",
        database=database,
    )
    return rows[0][0], int(rows[0][1])


@pytest.fixture
def canary(env):
    """A row created after the backup, used to prove nothing was mutated."""
    cfg, _settings = env
    euid, _seq = _new_instance(cfg, name="canary-do-not-touch")
    return euid


def _canary_present(cfg, euid: str, *, database: str | None = None) -> bool:
    schema = str(cfg["schema_name"])
    rows = _exec(
        cfg,
        f"SELECT COUNT(*) FROM \"{schema}\".generic_instance WHERE euid = '{euid}'",
        commit=False,
        database=database,
    )
    return int(rows[0][0]) == 1


@pytest.fixture
def dropped_database(env):
    """Drop any database an isolated restore created, however the test ends."""
    created: list[str] = []
    yield created
    cfg, _settings = env
    from daylily_tapdb.backup import engine as eng

    for name in created:
        eng.run_command(
            eng.build_psql_command(
                cfg, sql=f'DROP DATABASE IF EXISTS "{name}"', database="postgres"
            ),
            env=eng.client_env(cfg),
        )


# ---------------------------------------------------------------------------
# scope: what a backup does and does not capture
# ---------------------------------------------------------------------------


def test_a_table_in_another_schema_is_not_captured(env):
    """Backups are schema-scoped. A stranger in `public` must stay out.

    Capturing it would make the archive claim ownership of data TapDB does not
    manage, and a restore would then recreate someone else's table.
    """
    cfg, settings = env
    _exec(
        cfg,
        "CREATE TABLE IF NOT EXISTS public.stranger (id int)",
        connection_role="operator",
    )
    _exec(
        cfg,
        "INSERT INTO public.stranger VALUES (1)",
        connection_role="operator",
    )

    try:
        manifest = service.create_backup(cfg, settings).manifest

        assert "stranger" not in manifest.row_counts
        assert manifest.content_inventory["schema_names_seen"] == [cfg["schema_name"]]
    finally:
        _exec(
            cfg,
            "DROP TABLE IF EXISTS public.stranger",
            connection_role="operator",
        )


def test_every_managed_table_is_captured(env):
    """The complement: nothing in the managed schema is silently omitted."""
    cfg, settings = env
    schema = str(cfg["schema_name"])
    live = {
        r[0]
        for r in _exec(
            cfg,
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'",
            commit=False,
            connection_role="operator",
        )
    }

    manifest = service.create_backup(cfg, settings).manifest

    assert live, "probe found no tables; the assertion below would be vacuous"
    assert live == set(manifest.row_counts), (
        f"missing={sorted(live - set(manifest.row_counts))} "
        f"extra={sorted(set(manifest.row_counts) - live)}"
    )


# ---------------------------------------------------------------------------
# isolation: a neighbouring schema is not collateral damage
# ---------------------------------------------------------------------------


def test_an_in_place_restore_leaves_a_neighbouring_schema_untouched(env, backup):
    """An in-place restore replaces only its configured schema."""
    cfg, settings = env
    _exec(cfg, "CREATE SCHEMA IF NOT EXISTS neighbour", connection_role="operator")
    _exec(
        cfg,
        "CREATE TABLE IF NOT EXISTS neighbour.ledger (id int, note text)",
        connection_role="operator",
    )
    _exec(
        cfg,
        "INSERT INTO neighbour.ledger VALUES (1, 'not yours')",
        connection_role="operator",
    )

    try:
        before = _exec(
            cfg,
            "SELECT id, note FROM neighbour.ledger ORDER BY id",
            commit=False,
            connection_role="operator",
        )

        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
        )

        after = _exec(
            cfg,
            "SELECT id, note FROM neighbour.ledger ORDER BY id",
            commit=False,
            connection_role="operator",
        )
        assert after == before
        assert "neighbour" in _schemas(cfg)
    finally:
        _exec(
            cfg,
            "DROP SCHEMA IF EXISTS neighbour CASCADE",
            connection_role="operator",
        )


# ---------------------------------------------------------------------------
# read-only: plan and verify are safe to run against production
# ---------------------------------------------------------------------------


def test_plan_and_verify_move_nothing(env, backup):
    """Operators are told these are safe on a live target. Prove it.

    Row counts alone are too weak -- a sequence that advanced or an audit row
    that appeared would both mean the "read-only" claim is false while counts
    stayed level.
    """
    cfg, settings = env
    schema = str(cfg["schema_name"])

    def _audit_count():
        return int(
            _exec(cfg, f'SELECT COUNT(*) FROM "{schema}".audit_log', commit=False)[0][0]
        )

    before_counts, before_seqs = _state(cfg)
    before_audit = _audit_count()

    service.plan_backup(cfg, settings)
    service.verify_backup(cfg, settings, backup_id=backup.backup_id)
    verify.plan_restore(cfg, settings, backup_id=backup.backup_id)
    verify.plan_restore(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
    )

    after_counts, after_seqs = _state(cfg)
    assert after_counts == before_counts
    assert after_seqs == before_seqs, "a sequence advanced during a read-only operation"
    assert _audit_count() == before_audit, "a read-only operation wrote an audit row"


# ---------------------------------------------------------------------------
# refusal before mutation
# ---------------------------------------------------------------------------


def test_a_corrupted_archive_is_refused_with_the_canary_intact(
    env, backup, canary, tmp_path
):
    """Corruption must be caught before the target is touched at all."""
    cfg, settings = env
    archive = tmp_path / "store" / backup.storage_prefix / engine.DEFAULT_ARTIFACT_NAME
    assert archive.exists(), f"no artifact at {archive}"

    raw = bytearray(archive.read_bytes())
    for offset in range(len(raw) // 2, min(len(raw) // 2 + 512, len(raw))):
        raw[offset] ^= 0xFF
    archive.write_bytes(bytes(raw))

    before_counts, before_seqs = _state(cfg)

    with pytest.raises(BackupError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
        )

    assert _canary_present(cfg, canary), "the target was modified despite refusal"
    assert _state(cfg) == (before_counts, before_seqs)


def test_a_backward_version_restore_is_refused(env, backup, canary, monkeypatch):
    """PostgreSQL restores forward only; preflight must catch the reverse.

    ``assert_restore_target_is_new_enough`` is unit-tested in
    ``test_backup_engine.py``, but a correct function that nothing calls is the
    failure mode this subsystem has already hit once. This proves the guard is
    actually wired into preflight, by making the *target* look older than the
    server the backup came from.
    """
    cfg, settings = env
    real_server_version = introspect.server_version

    def _ancient(session):
        return dict(real_server_version(session), server_version="9.6.0")

    monkeypatch.setattr(introspect, "server_version", _ancient)

    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)
    version_checks = [c for c in plan.checks if c.id == "version.compatible"]
    assert version_checks, "preflight has no version check at all"
    assert version_checks[0].failed, "a backward restore passed preflight"

    before = _state(cfg)
    with pytest.raises(BackupError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
        )

    assert _canary_present(cfg, canary)
    assert _state(cfg) == before


# ---------------------------------------------------------------------------
# sequence continuity: no EUID is ever reused
# ---------------------------------------------------------------------------


def test_an_instance_created_after_a_restore_never_reuses_an_euid(
    env, backup, dropped_database
):
    """The no-reuse guarantee, exercised rather than inspected.

    A restore rewinds rows. If sequences rewound with them, the next insert
    would mint an identifier a consumer already holds, and it would resolve to
    a *different* object -- silent corruption. Post-restore reconciliation must
    push the sequence past the backup's high-water mark, so the next EUID is
    strictly greater and the insert cannot collide.
    """
    cfg, settings = env
    high_water_at_backup = _max_euid_seq(cfg)

    # Advance the live sequence well past the backup, then restore the older
    # state into an isolated copy.
    for index in range(3):
        _new_instance(cfg, name=f"after-backup-{index}")
    assert _max_euid_seq(cfg) > high_water_at_backup

    result = verify.restore_backup(cfg, settings, backup_id=backup.backup_id)
    dropped_database.append(result.target_database)

    restored_max = _max_euid_seq(cfg, database=result.target_database)
    euid, seq = _new_instance(cfg, database=result.target_database, name="post-restore")

    assert seq > restored_max, (
        f"new instance reused sequence space: {seq} <= {restored_max}"
    )
    assert seq > high_water_at_backup, (
        "the sequence rewound to before the backup's high-water mark; "
        f"{seq} <= {high_water_at_backup}"
    )
    assert euid, "trigger did not assign an EUID"


def test_a_rewound_sequence_cannot_reuse_an_euid(env, backup, dropped_database):
    """The adversarial case behind the advisory `sequences.prefix_projection`.

    That check only WARNs when a prefix sequence sits below its data, on the
    grounds that reuse is impossible anyway. This proves the grounds rather
    than asserting them: force the sequence all the way back to 1 -- worse
    than any real restore could leave it -- and confirm the next insert is
    *rejected* rather than handed an identifier a consumer already holds.

    A loud failure here is the correct outcome. Silent reuse would be the
    corruption the whole no-reuse guarantee exists to prevent.
    """
    cfg, settings = env
    schema = str(cfg["schema_name"])
    result = verify.restore_backup(cfg, settings, backup_id=backup.backup_id)
    dropped_database.append(result.target_database)

    highest = _max_euid_seq(cfg, database=result.target_database)
    assert highest > 0, "no GVR instances restored; the rewind below proves nothing"

    _exec(
        cfg,
        f"SELECT setval('\"{schema}\".gvr_instance_seq', 1)",
        database=result.target_database,
    )

    with pytest.raises(Exception) as excinfo:
        _new_instance(cfg, database=result.target_database, name="after-a-rewind")

    assert "duplicate key" in str(excinfo.value).lower(), str(excinfo.value)[:300]


def test_a_restored_database_still_enforces_euid_uniqueness(
    env, backup, dropped_database
):
    """The constraint itself must survive the restore, not just the data."""
    cfg, settings = env
    schema = str(cfg["schema_name"])
    result = verify.restore_backup(cfg, settings, backup_id=backup.backup_id)
    dropped_database.append(result.target_database)

    existing = _exec(
        cfg,
        f'SELECT euid FROM "{schema}".generic_instance LIMIT 1',
        commit=False,
        database=result.target_database,
    )[0][0]

    with pytest.raises(Exception):
        _exec(
            cfg,
            f'INSERT INTO "{schema}".generic_instance '
            "(euid, euid_prefix, name, polymorphic_discriminator) "
            f"VALUES ('{existing}', 'GVR', 'dup', 'generic_instance')",
            database=result.target_database,
        )


# ---------------------------------------------------------------------------
# the staged swap leaves nothing behind
# ---------------------------------------------------------------------------


def test_a_completed_swap_leaves_exactly_one_schema_and_no_residue(env, backup):
    """A successful in-place restore leaves no working schemas behind."""
    cfg, settings = env
    schema = str(cfg["schema_name"])

    verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
        confirm_target=service.target_label(cfg),
    )

    schemas = _schemas(cfg)
    assert schema in schemas
    residue = [
        name
        for name in schemas
        if name.startswith(f"{schema}_")
        and ("restoring" in name or "superseded" in name)
    ]
    assert residue == [], f"staged-restore working schemas survived: {residue}"


def test_keeping_the_superseded_schema_is_opt_in_and_visible(env, backup):
    cfg, settings = env
    schema = str(cfg["schema_name"])

    verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE, keep_superseded=True),
        confirm_target=service.target_label(cfg),
    )

    kept = [
        name
        for name in _schemas(cfg)
        if name.startswith(f"{schema}_") and "superseded" in name
    ]
    try:
        assert len(kept) == 1, f"expected exactly one kept schema, got {kept}"
        assert schema in _schemas(cfg)
    finally:
        for name in kept:
            _exec(
                cfg,
                f'DROP SCHEMA IF EXISTS "{name}" CASCADE',
                connection_role="operator",
            )


# ---------------------------------------------------------------------------
# the whole lifecycle, in the order an operator would run it
# ---------------------------------------------------------------------------


def test_the_full_lifecycle_round_trips_with_an_intact_audit_trail(
    env, dropped_database
):
    """create -> verify -> rehearse -> restore, with receipts that verify.

    Each step is covered in isolation elsewhere; what this adds is that the
    sequence works end to end and leaves one unbroken hash chain behind.
    """
    cfg, settings = env

    created = service.create_backup(cfg, settings)
    assert created.backup_id

    report = service.verify_backup(cfg, settings, backup_id=created.backup_id)
    assert report.ok, report.to_payload()

    evidence = verify.rehearse_restore(cfg, settings, backup_id=created.backup_id)
    assert evidence.ok, evidence.to_payload()

    storage = service.storage_for(settings)
    assert storage.exists(evidence.evidence_key), (
        f"rehearsal evidence missing at {evidence.evidence_key}"
    )

    restored = verify.restore_backup(cfg, settings, backup_id=created.backup_id)
    dropped_database.append(restored.target_database)
    assert restored.ok, [c.to_payload() for c in restored.checks if c.failed]
    assert not restored.quarantined

    receipts = read_receipts(service.receipts_directory(settings))
    chain = verify_receipt_chain(receipts)
    assert chain.ok, chain.to_payload()

    operations = [r.operation for r in receipts]
    for expected in (
        "backup_create",
        "backup_verify",
        "backup_rehearse",
        "backup_restore",
    ):
        assert expected in operations, f"{expected} left no receipt; got {operations}"


def test_a_rehearsal_is_not_recorded_as_a_restore(env, backup):
    """Drills must not pollute the restore trail an auditor reads."""
    cfg, settings = env

    verify.rehearse_restore(cfg, settings, backup_id=backup.backup_id)

    receipts = read_receipts(service.receipts_directory(settings))
    restores = [r for r in receipts if r.operation == "backup_restore"]
    assert restores == [], "a rehearsal was logged as a real restore"


# ---------------------------------------------------------------------------
# in-place restore must not reissue an EUID minted after the backup
# ---------------------------------------------------------------------------


def test_an_in_place_restore_never_reissues_an_euid_minted_after_the_backup(
    env, backup
):
    cfg, settings = env

    doomed_euid, doomed_seq = _new_instance(cfg, name="minted-after-the-backup")

    verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
        confirm_target=service.target_label(cfg),
    )

    assert not _canary_present(cfg, doomed_euid)

    reissued_euid, reissued_seq = _new_instance(cfg, name="minted-after-the-restore")

    assert reissued_euid != doomed_euid, (
        f"EUID {doomed_euid} was reissued to a different object -- a consumer "
        "holding it now resolves to the wrong row"
    )
    assert reissued_seq > doomed_seq, (
        f"sequence went backwards: {reissued_seq} <= {doomed_seq}"
    )


def test_the_in_place_restore_reports_which_sequences_it_advanced(env, backup):
    cfg, settings = env
    _new_instance(cfg, name="minted-after-the-backup")

    result = verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
        confirm_target=service.target_label(cfg),
    )

    advanced = result.sequences_advanced
    assert advanced, "a sequence had to move; nothing was reported"
    assert "gvr_instance_seq" in advanced, sorted(advanced)
    moved = advanced["gvr_instance_seq"]
    assert moved["to_next"] > moved["from_next"], moved


def test_json_paths_the_runbook_tells_operators_to_use_actually_exist(
    pg_instance, env, backup
):
    """A documented `jq` path must resolve against real CLI output.

    The runbook shipped `tapdb --json backup list | jq '.status.receipt_chain'`
    while the CLI's list payload had no `status` key at all -- the command ran
    and returned nothing. `test_docs_contracts.py` validates that commands and
    flags exist, but it cannot see output shape, so the check belongs here
    where a real database is available.
    """
    import json
    import re
    import subprocess
    import sys

    runbook = (
        Path(__file__).resolve().parents[1] / "docs" / "backup-and-recovery.md"
    ).read_text()

    paths = set(re.findall(r"jq '\.([A-Za-z0-9_.\[\]]+)'", runbook))
    assert paths, "no jq paths found in the runbook; this test would be vacuous"

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "daylily_tapdb.cli",
            "--config",
            str(pg_instance["config_path"]),
            "--json",
            "backup",
            "list",
        ],
        capture_output=True,
        text=True,
        cwd=str(Path(__file__).resolve().parents[1]),
        timeout=300,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    payload = json.loads(result.stdout)

    missing = []
    for path in paths:
        node = payload
        for part in path.split("."):
            if isinstance(node, dict) and part in node:
                node = node[part]
            else:
                missing.append(path)
                break
    assert missing == [], f"runbook documents jq paths that do not exist: {missing}"


def test_no_check_reports_a_verdict_without_saying_why(env, backup):
    """Every non-skipped check must explain itself.

    A bare ``✓ version.compatible:`` reads as though the line failed to
    render, and when the same check *fails* the operator gets a verdict with
    no numbers to act on. Two checks shipped this way.
    """
    cfg, settings = env

    results = []
    results += service.plan_backup(cfg, settings).checks
    results += service.verify_backup(cfg, settings, backup_id=backup.backup_id).checks
    for mode in ("isolated", "in-place"):
        results += verify.plan_restore(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=mode),
        ).checks

    assert results, "no checks collected; this test would be vacuous"
    silent = sorted(
        {c.id for c in results if c.status != "skip" and not str(c.detail).strip()}
    )
    assert silent == [], f"checks report a verdict with no detail: {silent}"


# ---------------------------------------------------------------------------
# the schema-drift gate on create
# ---------------------------------------------------------------------------


@pytest.fixture
def drifted(env):
    """A TAPDB-namespaced object created outside the migration path."""
    cfg, _settings = env
    schema = str(cfg["schema_name"])
    _exec(
        cfg,
        f'CREATE TABLE "{schema}".tapdb_hand_made (uid bigint primary key)',
        connection_role="operator",
    )
    yield
    _exec(
        cfg,
        f'DROP TABLE IF EXISTS "{schema}".tapdb_hand_made',
        connection_role="operator",
    )


def test_a_drifted_schema_refuses_to_be_backed_up(env, drifted):
    """A backup of a schema nobody can explain is a backup nobody can trust.

    Found by mutation testing: disabling the drift gate broke no test, so the
    refusal itself -- a documented safety property -- had no coverage. The
    *detection* was tested in post-restore verification; the *gate on create*
    was not.
    """
    cfg, settings = env

    with pytest.raises(BackupError) as excinfo:
        service.create_backup(cfg, settings)

    assert "drift" in str(excinfo.value).lower(), str(excinfo.value)


def test_allow_drift_is_the_documented_way_through(env, drifted):
    """The gate must be an override, not a dead end -- recovery depends on it.

    The pre-restore safety backup uses `allow_drift=True` for exactly this
    reason: it has to capture whatever is on the target right now.
    """
    cfg, settings = env

    result = service.create_backup(cfg, settings, allow_drift=True)

    assert result.backup_id
    assert "tapdb_hand_made" in result.manifest.row_counts


def test_plan_reports_drift_without_blocking_and_strict_makes_it_blocking(env, drifted):
    """`plan` is advisory by default and blocking under `--strict`."""
    cfg, settings = env

    lenient = service.plan_backup(cfg, settings)
    strict = service.plan_backup(cfg, settings, strict_drift=True)

    def _drift(plan):
        return next(c for c in plan.checks if c.id == "schema.drift")

    assert _drift(lenient).status == "warn", _drift(lenient).to_payload()
    assert _drift(strict).status == "fail", _drift(strict).to_payload()
    assert lenient.ok is True
    assert strict.ok is False


def test_a_clean_schema_does_not_trip_the_drift_gate(env):
    """Guards the guard: the tests above would pass if drift were always on."""
    cfg, settings = env

    plan = service.plan_backup(cfg, settings)
    drift = next(c for c in plan.checks if c.id == "schema.drift")

    assert drift.status == "pass", drift.to_payload()


# ---------------------------------------------------------------------------
# --target-schema
# ---------------------------------------------------------------------------


def test_restoring_under_a_different_schema_name_actually_renames(
    env, backup, dropped_database
):
    """`--target-schema` must produce the schema it names.

    It was exposed on the CLI and the admin API with the help text "Rename the
    restored schema to this" and did nothing: a custom-format archive recreates
    the schema it was captured from, and no restore path renamed it. Preflight
    passed clean, the restore landed under the *source* name, and post-restore
    verification then ran against a schema that did not exist -- reporting
    "a sequence would reissue already-used identifiers", the subsystem's
    loudest alarm, for a target-schema typo.
    """
    cfg, settings = env
    source_schema = str(cfg["schema_name"])
    renamed = "tapdb_alt_target"

    result = verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(target_schema=renamed),
    )
    dropped_database.append(result.target_database)

    schemas = {
        r[0]
        for r in _exec(
            cfg,
            "SELECT schema_name FROM information_schema.schemata",
            commit=False,
            database=result.target_database,
            schema_name=renamed,
        )
    }
    assert renamed in schemas, f"requested schema absent; got {sorted(schemas)}"
    assert source_schema not in schemas, "the source schema name survived the rename"

    # And the restore is genuinely healthy under the new name -- not merely
    # renamed, but verified there.
    assert result.ok, [c.to_payload() for c in result.checks if c.failed]
    assert not result.quarantined
    rows = _exec(
        cfg,
        f'SELECT count(*) FROM "{renamed}".generic_instance',
        commit=False,
        database=result.target_database,
        schema_name=renamed,
    )
    assert int(rows[0][0]) > 0, "renamed schema has no data"


def test_the_staged_plan_shows_the_rename_it_will_perform(env, backup):
    """A mutation the operator confirms has to appear in the step list."""
    cfg, settings = env

    plan = verify.plan_restore(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(target_schema="tapdb_alt_target"),
    )

    assert plan.source_schema == str(cfg["schema_name"])
    assert any("RENAME TO tapdb_alt_target" in step for step in plan.steps), plan.steps


def test_no_rename_step_when_the_schema_name_is_unchanged(env, backup):
    """Guards the guard: the assertion above must not pass for every plan."""
    cfg, settings = env

    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)

    assert not any("RENAME TO" in step for step in plan.steps), plan.steps


# ---------------------------------------------------------------------------
# in-place rollback
# ---------------------------------------------------------------------------


def test_failing_post_restore_checks_roll_the_original_schema_back(
    env, backup, monkeypatch
):
    cfg, settings = env
    before_counts, before_seqs = _state(cfg)
    before_schemas = _schemas(cfg)

    def _one_failure(*args, **kwargs):
        return [
            service.CheckResult(
                id="rowcounts.exact", status="fail", detail="simulated mismatch"
            )
        ]

    monkeypatch.setattr(verify, "_post_restore_checks", _one_failure)

    with pytest.raises(BackupError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
        )

    assert _state(cfg) == (before_counts, before_seqs)
    assert _schemas(cfg) == before_schemas, "a staged schema survived the rollback"


def test_a_rollback_that_cannot_restore_the_original_says_so_loudly(
    env, backup, monkeypatch
):
    cfg, settings = env
    real_admin_sql = verify._admin_sql

    def _fail_the_rename(cfg_, sql, **kwargs):
        if "RENAME TO" in sql and "superseded" not in sql.split("RENAME TO")[1]:
            raise BackupVerificationError("simulated rename failure")
        return real_admin_sql(cfg_, sql, **kwargs)

    monkeypatch.setattr(
        verify,
        "_post_restore_checks",
        lambda *a, **k: (_ for _ in ()).throw(
            BackupVerificationError("simulated verification failure")
        ),
    )
    monkeypatch.setattr(verify, "_admin_sql", _fail_the_rename)

    with pytest.raises(BackupError) as excinfo:
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
        )

    message = str(excinfo.value)
    assert "rollback could not restore" in message, message
    assert "by hand" in message, message
    detail = getattr(excinfo.value, "detail", {}) or {}
    assert detail.get("superseded_schema"), detail

    failures = [
        entry
        for entry in read_receipts(service.receipts_directory(settings))
        if entry.operation == "backup_restore" and not entry.succeeded
    ]
    assert failures, "a failed restore must still write a receipt"
    safety_id = failures[-1].detail.get("safety_backup_id")
    assert safety_id
    safety_manifest = service._load_manifest(
        service.storage_for(settings),
        service.find_backup_prefix(cfg, service.storage_for(settings), safety_id),
    )
    assert safety_manifest.provenance == {
        "created_by": manifest_mod.PROVENANCE_RESTORE,
        "restored_backup_id": backup.backup_id,
    }

    monkeypatch.undo()
    superseded = detail["superseded_schema"]
    schema = str(cfg["schema_name"])
    _exec(
        cfg,
        f'DROP SCHEMA IF EXISTS "{schema}" CASCADE',
        connection_role="operator",
    )
    _exec(
        cfg,
        f'ALTER SCHEMA "{superseded}" RENAME TO "{schema}"',
        connection_role="operator",
    )
    assert schema in _schemas(cfg)
    assert not any("_superseded_" in name for name in _schemas(cfg))


# ---------------------------------------------------------------------------
# guarantees that survived mutation testing -- each was entirely unprotected
# ---------------------------------------------------------------------------


def test_a_deep_read_catches_corruption_a_checksum_cannot(env, backup, tmp_path):
    """The deep read as the *sole* remaining defence.

    Plan section 6 asks for "flipped bytes **and** a falsified checksum" -- the
    case where the manifest agrees with the damaged artifact and only reading
    the archive can tell. Existing tests assert `asset.checksum` fails, or
    merely that `archive.deep_read` is present, so replacing the deep read with
    an unconditional pass survived the whole suite.
    """
    import json

    cfg, settings = env
    prefix = backup.storage_prefix
    artifact = tmp_path / "store" / prefix / engine.DEFAULT_ARTIFACT_NAME

    raw = bytearray(artifact.read_bytes())
    for offset in range(len(raw) // 2, min(len(raw) // 2 + 4096, len(raw))):
        raw[offset] ^= 0xFF
    artifact.write_bytes(bytes(raw))

    # Re-point the manifest at the damaged bytes so the checksum check passes
    # and only a real read can find the damage.
    manifest_path = tmp_path / "store" / prefix / "manifest.json"
    payload = json.loads(manifest_path.read_text())
    for asset in payload["included_assets"]:
        if asset["name"] == engine.DEFAULT_ARTIFACT_NAME:
            asset["sha256"] = manifest_mod.sha256_file(artifact)
            asset["bytes"] = artifact.stat().st_size
    manifest_bytes = manifest_mod.canonical_bytes(payload)
    manifest_path.write_bytes(manifest_bytes)
    (tmp_path / "store" / prefix / "manifest.sha256").write_text(
        manifest_mod.sha256_hex(manifest_bytes)
    )

    report = service.verify_backup(cfg, settings, backup_id=backup.backup_id)

    checksum = next(c for c in report.checks if c.id.startswith("asset.checksum"))
    deep = next(c for c in report.checks if c.id == "archive.deep_read")
    assert not checksum.failed, "checksum should agree; the deep read is the test"
    assert deep.failed, "deep read passed on a corrupted archive"
    assert not report.ok


def test_a_half_written_backup_is_removed_not_left_behind(env, monkeypatch):
    """ "Leave no half-written backup behind to be mistaken for a good one."

    Replacing the cleanup with `pass` survived every test.
    """
    cfg, settings = env
    before = {e.backup_id for e in service.list_backups(cfg, settings).entries}

    def _boom(*args, **kwargs):
        raise BackupVerificationError("simulated publish failure")

    monkeypatch.setattr(service, "_publish", _boom)

    with pytest.raises(BackupError):
        service.create_backup(cfg, settings)

    after = {e.backup_id for e in service.list_backups(cfg, settings).entries}
    assert after == before, f"a partial backup survived: {sorted(after - before)}"
    storage = service.storage_for(settings)
    assert not storage.list_keys(""), (
        ("storage still holds keys from the failed create") if not before else True
    )


def test_a_create_that_fails_its_own_verification_does_not_publish(env, monkeypatch):
    """ "A create is not trusted until it has verified its own artifact."

    Mutating the raise to `if False` survived every test.
    """
    cfg, settings = env
    before = {e.backup_id for e in service.list_backups(cfg, settings).entries}

    real_verify = service.verify_backup

    def _always_bad(cfg_, settings_, **kwargs):
        report = real_verify(cfg_, settings_, **kwargs)
        return service.VerifyReport(
            backup_id=report.backup_id,
            level=report.level,
            checks=[
                service.CheckResult(
                    id="archive.deep_read", status="fail", detail="simulated"
                )
            ],
        )

    monkeypatch.setattr(service, "verify_backup", _always_bad)

    with pytest.raises(BackupError):
        service.create_backup(cfg, settings)

    after = {e.backup_id for e in service.list_backups(cfg, settings).entries}
    assert after == before, "a backup that failed its own verification was published"


def test_a_cross_domain_restore_is_refused(env, backup):
    """`identity.match` must *block*, not merely appear in a list of check ids.

    Turning it into a WARN survived every test -- the only place it was
    mentioned was inside an expected-ids list.
    """
    cfg, settings = env
    foreign = dict(cfg, domain_code="Q", owner_repo_name="someone-elses-repo")

    plan = verify.plan_restore(foreign, settings, backup_id=backup.backup_id)

    identity = next(c for c in plan.checks if c.id == "identity.match")
    assert identity.failed, identity.to_payload()
    assert not plan.ok

    with pytest.raises(BackupError):
        verify.restore_backup(
            foreign,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(foreign),
        )


def test_the_override_lets_a_cross_domain_restore_through(env, backup):
    """Guards the guard: the refusal above must be the flag doing work."""
    cfg, settings = env
    foreign = dict(cfg, domain_code="Q", owner_repo_name="someone-elses-repo")

    plan = verify.plan_restore(
        foreign,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(allow_identity_mismatch=True),
    )

    identity = next(c for c in plan.checks if c.id == "identity.match")
    assert not identity.failed, identity.to_payload()


def test_a_dump_that_is_not_schema_scoped_is_rejected(env, monkeypatch):
    """Issue #89 item 6's runtime proof: the archive states its own scope.

    Disabling the assertion survived every test.
    """
    cfg, settings = env

    class _WideInventory:
        entries: list = []

        @staticmethod
        def schema_names_seen():
            return ["tapdb_testdb", "someone_elses_schema"]

        @staticmethod
        def to_payload():
            return {}

    monkeypatch.setattr(service.engine, "parse_toc", lambda _text: _WideInventory())

    with pytest.raises(BackupError, match="not scoped"):
        service.create_backup(cfg, settings)


def test_an_invalid_template_pack_is_not_published(env, monkeypatch):
    """Exported packs are validated before publication.

    Disabling the check survived every test, so a pack that fails its own
    schema could have been written and later loaded as if it were good.
    """
    from daylily_tapdb.backup import template_pack

    cfg, settings = env
    before = {e.backup_id for e in service.list_backups(cfg, settings).entries}

    monkeypatch.setattr(
        template_pack, "validate_template_pack", lambda _pack: ["simulated problem"]
    )

    with pytest.raises(BackupError, match="validation"):
        service.create_backup(cfg, settings, backup_class="template-pack")

    after = {e.backup_id for e in service.list_backups(cfg, settings).entries}
    assert after == before, "an invalid template pack was published"


def test_a_staged_plan_cannot_be_replayed_with_an_override(env, backup):
    """The fingerprint must cover the flags that turn a block into a pass.

    Staging with defaults *blocked* on `identity.match`; applying the same
    fingerprint with `allow_identity_mismatch` was accepted, because the
    fingerprint hashed neither override flag. That let a refused plan's
    confirmation authorise one domain's data being restored over another's --
    the precise thing re-staging exists to prevent.
    """
    cfg, settings = env
    foreign = dict(cfg, domain_code="Q", owner_repo_name="someone-elses-repo")

    strict = verify.plan_restore(foreign, settings, backup_id=backup.backup_id)
    lenient = verify.plan_restore(
        foreign,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(allow_identity_mismatch=True),
    )

    # The override flips this specific check; the foreign target still trips
    # `governance.prefixes`, so comparing `.ok` would prove nothing.
    def _identity(plan):
        return next(c for c in plan.checks if c.id == "identity.match")

    assert _identity(strict).failed
    assert not _identity(lenient).failed
    assert strict.plan_fingerprint != lenient.plan_fingerprint, (
        "a blocked plan and an overridden one share a fingerprint"
    )

    # And the stale one is genuinely refused rather than merely different.
    with pytest.raises(BackupError):
        verify.restore_backup(
            foreign,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(allow_identity_mismatch=True),
            plan_fingerprint=strict.plan_fingerprint,
        )


def test_unknown_migrations_also_change_the_fingerprint(env, backup):
    """The other override flag, for the same reason."""
    cfg, settings = env

    default = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)
    permissive = verify.plan_restore(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(allow_unknown_migrations=True),
    )

    assert default.plan_fingerprint != permissive.plan_fingerprint


def test_unclaimable_prefixes_block_by_default_and_can_be_overridden(env, backup):
    """The gate must be passable, but only deliberately.

    Every TapDB schema carries the base `TPX` prefix plus client-specific ones,
    and those live in different governance registries -- so with no override
    this check blocked *every* restore in a real deployment, with no way
    through at all. It is the only preflight failure that had no flag, while
    `identity.match` and `migrations.known` both did.

    Found by running against a real Aurora cluster; no unit test could have
    surfaced it, because the fixtures use a registry that claims everything.
    """
    cfg, settings = env
    # A governance context that owns nothing the backup carries.
    foreign = dict(cfg, domain_code="Q", owner_repo_name="unrelated-repo")

    blocked = verify.plan_restore(foreign, settings, backup_id=backup.backup_id)
    prefixes = next(c for c in blocked.checks if c.id == "governance.prefixes")
    assert prefixes.failed, prefixes.to_payload()

    allowed = verify.plan_restore(
        foreign,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(allow_unclaimable_prefixes=True),
    )
    relaxed = next(c for c in allowed.checks if c.id == "governance.prefixes")
    assert not relaxed.failed, relaxed.to_payload()
    assert relaxed.status == "warn", "an overridden gate must still be visible"

    # And the override changes the fingerprint, so a blocked plan's
    # confirmation cannot be replayed with it.
    assert blocked.plan_fingerprint != allowed.plan_fingerprint
