"""Restore preflight and execution against a real PostgreSQL.

The properties under test are safety properties: preflight completes before
anything is mutated, a failed in-place restore leaves the original schema
exactly as it was, and a confirmation can only ever authorise the operation it
was shown.

Runs against the ``pg_instance`` fixture -- an ephemeral cluster under pytest's
tmp dir, torn down afterwards.
"""

from __future__ import annotations

import shutil
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.backup import service, verify
from daylily_tapdb.backup.errors import (
    BackupPolicyBlockedError,
    BackupVerificationError,
    RestoreConfirmationError,
    RestoreStageStaleError,
)
from daylily_tapdb.backup.verify import (
    MODE_IN_PLACE,
    MODE_ISOLATED,
    RestoreOptions,
)
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


def _live_state(cfg, schema=None):
    """Snapshot row counts and sequence values for the configured schema."""
    from daylily_tapdb.backup import introspect

    target = schema or str(cfg["schema_name"])
    with service.open_session(
        cfg, app_username="pytest", connection_role="operator"
    ) as conn:
        with conn.session_scope(commit=False) as session:
            return (
                introspect.capture_row_counts(session, target),
                {
                    s.name: s.last_value
                    for s in introspect.capture_sequences(session, target)
                },
            )


def _schemas(cfg):
    with service.open_session(
        cfg, app_username="pytest", connection_role="operator"
    ) as conn:
        with conn.session_scope(commit=False) as session:
            rows = session.execute(
                text("SELECT schema_name FROM information_schema.schemata")
            ).scalars()
            return set(rows)


# ---------------------------------------------------------------------------
# plan_restore
# ---------------------------------------------------------------------------


def test_plan_reports_steps_and_a_confirmation_label(env, backup):
    cfg, settings = env

    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)

    assert plan.ok, [c.to_payload() for c in plan.blocking]
    assert plan.required_confirm_target == service.target_label(cfg)
    assert plan.steps
    assert plan.mode == MODE_ISOLATED


def test_plan_never_mutates_the_target(env, backup):
    cfg, settings = env
    before_counts, before_seqs = _live_state(cfg)
    before_schemas = _schemas(cfg)

    verify.plan_restore(cfg, settings, backup_id=backup.backup_id)

    after_counts, after_seqs = _live_state(cfg)
    assert (after_counts, after_seqs) == (before_counts, before_seqs)
    assert _schemas(cfg) == before_schemas


def test_plan_runs_the_corruption_gate(env, backup):
    cfg, settings = env

    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)

    assert any(c.id == "archive.deep_read" for c in plan.checks)
    assert any(c.id == "manifest.checksum" for c in plan.checks)


def test_plan_fails_on_a_corrupted_artifact(env, backup, tmp_path):
    cfg, settings = env
    artifact = tmp_path / "store" / backup.storage_prefix / "tapdb.dump"
    raw = bytearray(artifact.read_bytes())
    for offset in range(len(raw) // 2, len(raw) // 2 + 512):
        raw[offset] ^= 0xFF
    artifact.write_bytes(bytes(raw))

    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)

    assert not plan.ok


def test_in_place_plan_describes_the_rename_aside_flow(env, backup):
    cfg, settings = env

    plan = verify.plan_restore(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
    )
    steps = " ".join(plan.steps).lower()

    assert "safety backup" in steps
    assert "rename" in steps
    assert "drop schema" in steps
    assert steps.index("rename") < steps.index("drop schema")


def test_unknown_mode_is_rejected():
    with pytest.raises(ValueError, match="Unknown restore mode"):
        RestoreOptions(mode="sideways").normalized_mode()


# ---------------------------------------------------------------------------
# injection and identifier safety
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "hostile",
    [
        "x'; DROP DATABASE postgres; --",
        'evil"; DROP SCHEMA public CASCADE; --',
        "has spaces",
        "-- comment",
    ],
)
def test_hostile_target_database_names_are_rejected(hostile):
    # These names reach DDL and a catalog lookup; validation is the first
    # layer and quoting at the call site is the second.
    with pytest.raises(Exception):
        RestoreOptions(target_database=hostile).validated_target_database()


@pytest.mark.parametrize(
    "hostile",
    ["x'; DROP SCHEMA tapdb CASCADE; --", 'a"b', "no spaces allowed"],
)
def test_hostile_target_schema_names_are_rejected(hostile):
    with pytest.raises(Exception):
        RestoreOptions(target_schema=hostile).validated_target_schema()


def test_ordinary_names_pass_validation():
    options = RestoreOptions(
        target_database="tapdb_restore_probe", target_schema="tapdb_alt"
    )

    assert options.validated_target_database() == "tapdb_restore_probe"
    assert options.validated_target_schema() == "tapdb_alt"


def test_a_quoted_literal_neutralises_embedded_quotes():
    from daylily_tapdb.backup.introspect import quote_literal

    assert quote_literal("O'Brien") == "'O''Brien'"
    assert quote_literal("x'; DROP DATABASE y; --") == ("'x''; DROP DATABASE y; --'")


def test_the_database_existence_probe_survives_a_quote_in_the_name(env):
    # Proves the probe uses literal quoting: a name containing a quote must
    # produce a well-formed query that simply finds nothing.
    cfg, _ = env

    assert verify._database_exists(cfg, "no'such'database") is False


# ---------------------------------------------------------------------------
# target emptiness (isolated mode)
# ---------------------------------------------------------------------------


def test_emptiness_check_runs_for_isolated_restores(env, backup):
    cfg, settings = env

    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)
    check = next(c for c in plan.checks if c.id == "target.empty")

    # It must actually run -- not silently skip, which would leave the guard
    # dead while looking present.
    assert check.status == "pass"
    assert "template0" in check.detail


def test_emptiness_check_inspects_the_target_not_the_current_database(env, backup):
    cfg, settings = env

    # The *current* database plainly contains the schema. Asking it instead of
    # the restore target would fail every isolated restore.
    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)

    assert plan.ok, [c.to_payload() for c in plan.blocking]


def test_a_prepopulated_target_database_is_refused(env, backup):
    cfg, settings = env
    # The current database already holds the schema; pointing an isolated
    # restore at it must be refused rather than overwriting.
    plan = verify.plan_restore(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(target_database=str(cfg["database"])),
    )
    check = next(c for c in plan.checks if c.id == "target.empty")

    assert check.status == "fail"
    assert not plan.ok


def test_every_preflight_check_from_the_plan_is_present(env, backup):
    cfg, settings = env

    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)
    ids = {c.id for c in plan.checks}

    # Plan section 3.5 enumerates nine preflight concerns; each must be
    # represented by a check that actually ran.
    for required in (
        "archive.deep_read",  # 1 corruption gate
        "manifest.checksum",
        "version.compatible",  # 2 target >= source
        "client.pg_restore",  # 2 client >= dump
        "migrations.known",  # 3
        "identity.match",  # 4
        "identity.data_scope",
        "governance.registries",  # 5 registry checksums
        "governance.prefixes",  # 5 prefix claimability
        "target.empty",  # 6
        "target.createdb",  # 7
        "rls.roles",  # 8
        "storage.capacity",  # 9
    ):
        assert required in ids, f"preflight is missing {required}"


def test_full_archive_is_physically_complete_for_both_restore_modes(env, backup):
    cfg, settings = env

    isolated = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)
    isolated_scope = next(c for c in isolated.checks if c.id == "identity.data_scope")
    assert isolated_scope.status == "pass"
    assert isolated_scope.data["physical_schema_complete"] is True

    in_place = verify.plan_restore(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
    )
    in_place_scope = next(c for c in in_place.checks if c.id == "identity.data_scope")
    assert in_place_scope.status == "pass"
    assert in_place_scope.data["physical_schema_complete"] is True


def test_full_archive_is_not_reinterpreted_as_tenant_filtered(env, backup):
    cfg, settings = env
    tenant_cfg = dict(cfg)
    tenant_cfg["tenant_id"] = "00000000-0000-4000-8000-000000000001"

    plan = verify.plan_restore(tenant_cfg, settings, backup_id=backup.backup_id)
    check = next(c for c in plan.checks if c.id == "identity.data_scope")

    assert check.status == "pass"
    assert check.data["mode"] == "physical_schema"


def test_rls_check_passes_for_the_canonical_forced_policies(env, backup):
    cfg, settings = env
    # Fresh schema apply consumes canonical rls.sql, so a real archive must
    # carry and validate the forced-RLS policies.
    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)
    check = next(c for c in plan.checks if c.id == "rls.roles")

    assert check.status == "pass"
    assert "policies" in check.detail


def test_rls_check_reads_the_archive_when_policies_are_present(env, backup):
    """Exercise the archive-reading path against a real artifact.

    The manifest's policy count is what gates this branch, so forcing it makes
    the check actually render the archive back to SQL and resolve roles -- the
    behaviour that a policy-counting implementation could never produce.
    """
    cfg, settings = env
    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup.backup_id)
    manifest = service._load_manifest(storage, prefix)
    manifest.content_inventory["counts_by_kind"]["POLICY"] = 3

    import tempfile
    from pathlib import Path as _Path

    staged = _Path(tempfile.mkdtemp())
    archive = storage.get_file(
        f"{prefix}/{manifest.included_assets[0].name}", staged / "tapdb.dump"
    )

    with service.open_session(cfg, app_username="pytest") as conn:
        with conn.session_scope(commit=False) as session:
            check = verify._check_rls_roles(session, manifest, archive_path=archive)

    # The archive really was rendered and parsed; TAPDB's policies name no
    # roles, so the honest verdict is "applies to PUBLIC".
    assert check.status == "pass"
    assert "PUBLIC" in check.detail


def test_prefix_claimability_resolves_real_prefixes(env, backup):
    cfg, settings = env

    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)
    check = next(c for c in plan.checks if c.id == "governance.prefixes")

    assert check.status in ("pass", "skip")
    if check.status == "pass":
        assert check.data["prefixes"]
        assert check.data["unclaimable"] == {}


def test_an_unclaimable_prefix_fails_preflight(env, backup):
    cfg, settings = env
    # A target claiming a different owner repo has no right to these prefixes.
    hostile = dict(cfg, owner_repo_name="some-other-repo")

    plan = verify.plan_restore(hostile, settings, backup_id=backup.backup_id)
    check = next(c for c in plan.checks if c.id == "governance.prefixes")

    if check.status != "skip":
        assert check.status == "fail"
        assert check.data["unclaimable"]


def test_emptiness_is_skipped_for_in_place(env, backup):
    cfg, settings = env

    plan = verify.plan_restore(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
    )
    check = next(c for c in plan.checks if c.id == "target.empty")

    assert check.status == "skip"


# ---------------------------------------------------------------------------
# fingerprint / staleness
# ---------------------------------------------------------------------------


def test_fingerprint_is_stable_across_replans(env, backup):
    cfg, settings = env
    options = RestoreOptions(mode=MODE_IN_PLACE)

    first = verify.plan_restore(
        cfg, settings, backup_id=backup.backup_id, options=options
    )
    second = verify.plan_restore(
        cfg, settings, backup_id=backup.backup_id, options=options
    )

    assert first.plan_fingerprint == second.plan_fingerprint


def test_the_fingerprint_survives_the_clock_advancing(env, backup):
    """Staging and applying are separated by however long a human takes.

    The isolated target name is part of the fingerprint, so deriving it from
    the clock made every stage/apply pair more than a second apart fail as
    `stale_stage`. This passed when the two calls happened to land in the same
    second, which is precisely how it hid.
    """
    from datetime import timedelta

    cfg, settings = env
    base = datetime(2026, 7, 29, 12, 0, 0, tzinfo=UTC)

    first = verify.plan_restore(cfg, settings, backup_id=backup.backup_id, now=base)
    later = verify.plan_restore(
        cfg, settings, backup_id=backup.backup_id, now=base + timedelta(hours=3)
    )

    assert first.target_database == later.target_database
    assert first.plan_fingerprint == later.plan_fingerprint


def test_the_isolated_target_name_is_derived_from_the_backup(env, backup):
    cfg, settings = env

    plan = verify.plan_restore(cfg, settings, backup_id=backup.backup_id)

    assert plan.target_database == verify.isolated_database_name(
        cfg, backup_id=backup.backup_id
    )
    # PostgreSQL truncates identifiers past 63 characters, which would make
    # two long backup ids collide on one database.
    assert len(plan.target_database) <= 63


def test_different_backups_get_different_isolated_targets(env):
    cfg, settings = env

    first = verify.isolated_database_name(cfg, backup_id="full-aaa")
    second = verify.isolated_database_name(cfg, backup_id="full-bbb")

    assert first != second


def test_fingerprint_changes_with_the_mode(env, backup):
    cfg, settings = env

    isolated = verify.plan_restore(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_ISOLATED, target_database="fixed_name"),
    )
    in_place = verify.plan_restore(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
    )

    assert isolated.plan_fingerprint != in_place.plan_fingerprint


def test_a_stale_fingerprint_is_refused(env, backup):
    cfg, settings = env

    with pytest.raises(RestoreStageStaleError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
            plan_fingerprint="stale-value",
        )


def test_a_stale_fingerprint_is_refused_before_any_mutation(env, backup):
    cfg, settings = env
    before = _live_state(cfg)

    with pytest.raises(RestoreStageStaleError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
            plan_fingerprint="stale-value",
        )

    assert _live_state(cfg) == before


# ---------------------------------------------------------------------------
# confirmation and policy
# ---------------------------------------------------------------------------


def test_in_place_without_confirmation_is_refused(env, backup):
    cfg, settings = env

    with pytest.raises(RestoreConfirmationError) as excinfo:
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
        )

    assert excinfo.value.detail["required_confirm_target"] == service.target_label(cfg)


def test_in_place_with_the_wrong_label_is_refused(env, backup):
    cfg, settings = env

    with pytest.raises(RestoreConfirmationError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target="some/other/target@db",
        )


def test_a_refused_confirmation_mutates_nothing(env, backup):
    cfg, settings = env
    before = _live_state(cfg)

    with pytest.raises(RestoreConfirmationError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target="wrong",
        )

    assert _live_state(cfg) == before


def test_a_blocked_policy_refuses_outright(env, backup):
    cfg, settings = env
    cfg["destructive_operations"] = "blocked"

    with pytest.raises(BackupPolicyBlockedError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
        )


def test_an_allowed_policy_needs_no_typed_label(env, backup):
    cfg, settings = env
    cfg["destructive_operations"] = "allowed"

    result = verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
        dry_run=True,
    )

    assert result.dry_run


def test_isolated_restore_needs_no_confirmation(env, backup):
    cfg, settings = env

    result = verify.restore_backup(
        cfg, settings, backup_id=backup.backup_id, dry_run=True
    )

    assert result.dry_run
    assert result.mode == MODE_ISOLATED


# ---------------------------------------------------------------------------
# dry run
# ---------------------------------------------------------------------------


def test_dry_run_mutates_nothing(env, backup):
    cfg, settings = env
    before = _live_state(cfg)
    before_schemas = _schemas(cfg)

    result = verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
        confirm_target=service.target_label(cfg),
        dry_run=True,
    )

    assert result.dry_run
    assert _live_state(cfg) == before
    assert _schemas(cfg) == before_schemas


# ---------------------------------------------------------------------------
# isolated restore execution
# ---------------------------------------------------------------------------


def _drop_db(cfg, name):
    from daylily_tapdb.backup import engine as eng

    eng.run_command(
        eng.build_psql_command(
            cfg, sql=f'DROP DATABASE IF EXISTS "{name}"', database="postgres"
        ),
        env=eng.client_env(cfg),
    )


def test_isolated_restore_recreates_the_schema_in_a_new_database(env, backup):
    cfg, settings = env
    result = None
    try:
        result = verify.restore_backup(cfg, settings, backup_id=backup.backup_id)

        assert result.ok, [c.to_payload() for c in result.checks if c.failed]
        assert result.target_database != cfg["database"]
        assert result.receipt_id

        probe = dict(cfg, database=result.target_database)
        counts, _ = _live_state(probe, schema=cfg["schema_name"])
        assert counts == backup.manifest.row_counts
        with service.open_session(
            probe, app_username="pytest", connection_role="operator"
        ) as conn:
            with conn.session_scope(commit=False) as session:
                rows = session.execute(
                    text(
                        "SELECT p.proname, setting FROM pg_proc p "
                        "JOIN pg_namespace n ON n.oid = p.pronamespace "
                        "CROSS JOIN LATERAL unnest(p.proconfig) AS setting "
                        "WHERE n.nspname = :schema "
                        "AND p.proname IN ('tapdb_assert_runtime_role', "
                        "'record_insert', 'tapdb_validate_lineage_endpoint_scope', "
                        "'soft_delete_row') "
                        "AND split_part(setting, '=', 1) = 'search_path'"
                    ),
                    {"schema": cfg["schema_name"]},
                ).all()
        assert {name for name, _setting in rows} == {
            "tapdb_assert_runtime_role",
            "record_insert",
            "tapdb_validate_lineage_endpoint_scope",
            "soft_delete_row",
        }
        assert {setting for _name, setting in rows} == {
            f"search_path={cfg['schema_name']}, pg_catalog, pg_temp"
        }
    finally:
        if result is not None:
            _drop_db(cfg, result.target_database)


def test_isolated_restore_leaves_live_data_untouched(env, backup):
    cfg, settings = env
    before = _live_state(cfg)
    result = None
    try:
        result = verify.restore_backup(cfg, settings, backup_id=backup.backup_id)
    finally:
        if result is not None:
            _drop_db(cfg, result.target_database)

    assert _live_state(cfg) == before


def test_a_failed_isolated_restore_drops_the_database_it_created(
    env, backup, monkeypatch
):
    cfg, settings = env

    def _boom(*args, **kwargs):
        raise BackupVerificationError("simulated pg_restore failure")

    monkeypatch.setattr(verify, "_restore_archive", _boom)

    with pytest.raises(BackupVerificationError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(target_database="tapdb_isolated_failure_probe"),
        )

    # No half-populated database may survive to be mistaken for a recovery.
    from daylily_tapdb.backup import engine as eng

    found = eng.run_command(
        eng.build_psql_command(
            cfg,
            sql="SELECT 1 FROM pg_database "
            "WHERE datname = 'tapdb_isolated_failure_probe'",
            database="postgres",
        ),
        env=eng.client_env(cfg),
    )
    assert found.stdout.strip() == ""


# ---------------------------------------------------------------------------
# in-place restore execution -- the destructive path
# ---------------------------------------------------------------------------


def test_in_place_restore_replaces_the_schema_and_takes_a_safety_backup(env, backup):
    cfg, settings = env
    before_counts, _ = _live_state(cfg)

    result = verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE),
        confirm_target=service.target_label(cfg),
    )

    assert result.ok, [c.to_payload() for c in result.checks if c.failed]
    assert result.safety_backup_id, "in-place must take a safety backup first"
    after_counts, _ = _live_state(cfg)
    assert after_counts == before_counts
    assert not any("_superseded_" in name for name in _schemas(cfg))


def test_in_place_keeps_the_superseded_schema_when_asked(env, backup):
    cfg, settings = env

    result = verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=RestoreOptions(mode=MODE_IN_PLACE, keep_superseded=True),
        confirm_target=service.target_label(cfg),
    )

    assert result.superseded_schema
    assert result.superseded_schema in _schemas(cfg)
    with service.open_session(
        cfg, app_username="pytest", connection_role="operator"
    ) as conn:
        with conn.session_scope(commit=True) as session:
            session.execute(
                text(f'DROP SCHEMA IF EXISTS "{result.superseded_schema}" CASCADE')
            )


@pytest.mark.parametrize(
    "failing_attr, message",
    [
        ("_restore_archive", "simulated pg_restore failure"),
        ("_post_restore_checks", "simulated verification failure"),
    ],
)
def test_a_failed_in_place_restore_leaves_the_original_intact(
    env, backup, monkeypatch, failing_attr, message
):
    cfg, settings = env
    before_counts, before_seqs = _live_state(cfg)
    before_schemas = _schemas(cfg)

    def _boom(*args, **kwargs):
        raise BackupVerificationError(message)

    monkeypatch.setattr(verify, failing_attr, _boom)

    with pytest.raises(BackupVerificationError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
        )

    after_counts, after_seqs = _live_state(cfg)
    assert after_counts == before_counts
    assert after_seqs == before_seqs
    assert _schemas(cfg) == before_schemas


def test_a_failed_in_place_restore_still_leaves_a_safety_backup(
    env, backup, monkeypatch
):
    cfg, settings = env

    def _boom(*args, **kwargs):
        raise BackupVerificationError("simulated failure")

    monkeypatch.setattr(verify, "_restore_archive", _boom)
    before = len(service.list_backups(cfg, settings).entries)

    with pytest.raises(BackupVerificationError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
        )

    assert len(service.list_backups(cfg, settings).entries) > before


def test_a_failed_restore_writes_a_failure_receipt(env, backup, monkeypatch):
    from daylily_tapdb.backup.receipts import read_receipts

    cfg, settings = env

    def _boom(*args, **kwargs):
        raise BackupVerificationError("simulated failure")

    monkeypatch.setattr(verify, "_restore_archive", _boom)

    with pytest.raises(BackupVerificationError):
        verify.restore_backup(
            cfg,
            settings,
            backup_id=backup.backup_id,
            options=RestoreOptions(mode=MODE_IN_PLACE),
            confirm_target=service.target_label(cfg),
        )

    receipts = read_receipts(service.receipts_directory(settings))
    restores = [r for r in receipts if r.operation == "backup_restore"]
    assert restores and not restores[-1].succeeded
