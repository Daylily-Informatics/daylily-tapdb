"""Admin API for the backup lifecycle.

The status codes are the contract here -- a client distinguishes "forbidden by
policy" (403) from "conflicts with current state" (409) from "the artifact is
corrupt" (422), and each implies a different next action. The other property
under test is that the API and the GUI share one apply path rather than two
that agree by coincidence.
"""

from __future__ import annotations

import shutil

import pytest
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from admin import backups as backups_api
from daylily_tapdb.backup import service, verify, views
from daylily_tapdb.backup.errors import (
    BackupNotFoundError,
    BackupPolicyBlockedError,
    BackupVerificationError,
    BackupVersionMismatchError,
    RestoreConfirmationError,
    RestoreStageStaleError,
)
from daylily_tapdb.cli import framework_app
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
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        config_path=pg_instance["config_path"],
    )
    config = str(pg_instance["config_path"])
    applied = runner.invoke(
        framework_app, ["--config", config, "db", "schema", "apply"]
    )
    assert applied.exit_code == 0, applied.output
    seeded = runner.invoke(
        framework_app, ["--config", config, "db", "data", "seed", "--skip-existing"]
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


class _FakeRequest:
    def __init__(self, user=None):
        self.state = type("S", (), {"user": user or {}})()


def _actor():
    return backups_api.api_actor(_FakeRequest({"email": "admin@example.com"}))


# ---------------------------------------------------------------------------
# reference and option validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "ref", ["../etc/passwd", "a/b", "with space", "semi;colon", "", "quote'"]
)
def test_a_malformed_reference_is_rejected(ref):
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        backups_api.validate_ref(ref)

    assert excinfo.value.status_code == 400


@pytest.mark.parametrize("ref", ["full-20260728T120000Z-abc123", "a.b_c-1"])
def test_well_formed_references_are_accepted(ref):
    assert backups_api.validate_ref(ref) == ref


def test_an_unknown_backup_class_is_a_400():
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        backups_api.validate_class("nonsense")

    assert excinfo.value.status_code == 400


def test_hostile_restore_options_are_a_400():
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as excinfo:
        backups_api.restore_options_from(
            {"target_database": "x'; DROP DATABASE postgres; --"}
        )

    assert excinfo.value.status_code == 400


# ---------------------------------------------------------------------------
# error -> status mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "error, status",
    [
        (BackupNotFoundError("nope"), 404),
        (BackupPolicyBlockedError("blocked"), 403),
        (RestoreConfirmationError("wrong label"), 409),
        (RestoreStageStaleError("stale"), 409),
        (BackupVersionMismatchError("older"), 409),
        (BackupVerificationError("corrupt"), 422),
    ],
)
def test_each_typed_error_maps_to_its_documented_status(error, status):
    assert backups_api.http_error(error).status_code == status


def test_an_unexpected_error_is_a_500_not_a_client_error():
    # An error we did not anticipate is not the caller's fault.
    assert backups_api.as_http(RuntimeError("boom")).status_code == 500


def test_the_error_code_travels_in_the_body():
    detail = backups_api.http_error(RestoreStageStaleError("stale")).detail

    assert detail["error"] == "stale_stage"


# ---------------------------------------------------------------------------
# read-only endpoints
# ---------------------------------------------------------------------------


def test_list_returns_backups_with_a_status_block(env, backup):
    cfg, settings = env

    payload = backups_api.list_payload(cfg, settings)

    assert payload["count"] >= 1
    assert payload["status"]["status"] in ("ok", "stale", "failing", "never_run")
    assert payload["status"]["receipt_chain"]["ok"] is True


def test_status_reports_never_run_before_any_backup(env):
    cfg, settings = env

    payload = backups_api.status_payload(cfg, settings)

    assert payload["status"] == "never_run"
    assert payload["target_label"] == service.target_label(cfg)


def test_status_reports_ok_after_a_backup(env, backup):
    cfg, settings = env

    payload = backups_api.status_payload(cfg, settings)

    assert payload["status"] == "ok"
    assert payload["last_success_backup_id"] == backup.backup_id


def test_status_surfaces_a_broken_receipt_chain(env, backup, tmp_path):
    """A status that says "healthy" while its audit trail is broken is worse
    than no status at all.

    The receipt being tampered with must already have a successor. A hash
    chain detects edits to links that something later points at; editing the
    tip before the next receipt is written simply means the next one chains to
    the edited state -- inherent to the construction, not a defect.
    """
    import json
    import os

    cfg, settings = env
    service.create_backup(cfg, settings)  # give the first receipt a successor

    receipts = sorted((tmp_path / "backups" / "receipts").glob("*.json"))
    assert len(receipts) >= 2, "need a chained successor for tampering to show"
    os.chmod(receipts[0], 0o600)
    payload = json.loads(receipts[0].read_text())
    payload["backup_id"] = "tampered"
    receipts[0].write_text(json.dumps(payload, indent=2, sort_keys=True))

    block = backups_api.status_payload(cfg, settings)

    assert block["receipt_chain"]["ok"] is False
    assert block["receipt_chain"]["findings"]


def test_plan_is_read_only(env, tmp_path):
    cfg, settings = env

    payload = backups_api.plan_payload(cfg, settings)

    assert payload["ok"] is True
    assert not (tmp_path / "store").exists()


# ---------------------------------------------------------------------------
# create / verify
# ---------------------------------------------------------------------------


def test_create_returns_a_manifest(env):
    cfg, settings = env

    payload = backups_api.create_payload(
        cfg, settings, body={"note": "via api"}, actor=_actor()
    )

    assert payload["manifest"]["row_counts"]
    assert payload["receipt_id"]


def test_create_records_the_api_surface_on_the_receipt(env):
    from daylily_tapdb.backup.receipts import read_receipts

    cfg, settings = env
    backups_api.create_payload(cfg, settings, body={}, actor=_actor())

    receipt = read_receipts(service.receipts_directory(settings))[-1]

    assert receipt.actor.surface == "api"
    assert receipt.actor.username == "admin@example.com"


def test_verify_returns_the_report_when_sound(env, backup):
    cfg, settings = env

    payload = backups_api.verify_payload(
        cfg, settings, ref=backup.backup_id, actor=_actor()
    )

    assert payload["ok"] is True


def test_verify_is_422_when_the_artifact_is_corrupt(env, backup, tmp_path):
    from fastapi import HTTPException

    cfg, settings = env
    artifact = tmp_path / "store" / backup.storage_prefix / "tapdb.dump"
    raw = bytearray(artifact.read_bytes())
    for offset in range(len(raw) // 2, len(raw) // 2 + 512):
        raw[offset] ^= 0xFF
    artifact.write_bytes(bytes(raw))

    with pytest.raises(HTTPException) as excinfo:
        backups_api.verify_payload(cfg, settings, ref=backup.backup_id, actor=_actor())

    assert excinfo.value.status_code == 422
    assert excinfo.value.detail["error"] == "backup_verification_failed"


def test_verify_of_an_unknown_backup_is_404(env):
    from fastapi import HTTPException

    cfg, settings = env

    with pytest.raises(HTTPException) as excinfo:
        backups_api.verify_payload(cfg, settings, ref="full-nope", actor=_actor())

    assert excinfo.value.status_code == 404


# ---------------------------------------------------------------------------
# stage / apply matrix
# ---------------------------------------------------------------------------


def test_stage_is_read_only_and_returns_the_fingerprint(env, backup):
    cfg, settings = env

    payload = backups_api.stage_payload(
        cfg, settings, ref=backup.backup_id, body={"mode": "in-place"}
    )

    assert payload["plan_fingerprint"]
    assert payload["required_confirm_target"] == service.target_label(cfg)
    assert payload["steps"]


@pytest.mark.parametrize(
    "body, missing",
    [
        ({}, ["plan_fingerprint", "confirm_target"]),
        ({"plan_fingerprint": "x"}, ["confirm_target"]),
        ({"confirm_target": "x"}, ["plan_fingerprint"]),
    ],
)
def test_apply_without_both_fields_is_400(env, backup, body, missing):
    from fastapi import HTTPException

    cfg, settings = env

    with pytest.raises(HTTPException) as excinfo:
        backups_api.apply_payload(
            cfg,
            settings,
            ref=backup.backup_id,
            body={**body, "mode": "in-place"},
            actor=_actor(),
        )

    # A missing field is a malformed request, distinct from a field that is
    # present and wrong.
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail["missing"] == missing


def test_apply_with_a_wrong_label_is_409(env, backup):
    from fastapi import HTTPException

    cfg, settings = env
    staged = backups_api.stage_payload(
        cfg, settings, ref=backup.backup_id, body={"mode": "in-place"}
    )

    with pytest.raises(HTTPException) as excinfo:
        backups_api.apply_payload(
            cfg,
            settings,
            ref=backup.backup_id,
            body={
                "mode": "in-place",
                "plan_fingerprint": staged["plan_fingerprint"],
                "confirm_target": "some/other/target@db",
            },
            actor=_actor(),
        )

    assert excinfo.value.status_code == 409
    assert excinfo.value.detail["error"] == "confirm_target_mismatch"


def test_apply_with_a_stale_fingerprint_is_409(env, backup):
    from fastapi import HTTPException

    cfg, settings = env

    with pytest.raises(HTTPException) as excinfo:
        backups_api.apply_payload(
            cfg,
            settings,
            ref=backup.backup_id,
            body={
                "mode": "in-place",
                "plan_fingerprint": "no-longer-valid",
                "confirm_target": service.target_label(cfg),
            },
            actor=_actor(),
        )

    assert excinfo.value.status_code == 409
    assert excinfo.value.detail["error"] == "stale_stage"


def test_apply_is_403_when_policy_blocks_destructive_operations(env, backup):
    from fastapi import HTTPException

    cfg, settings = env
    cfg["destructive_operations"] = "blocked"
    staged = backups_api.stage_payload(
        cfg, settings, ref=backup.backup_id, body={"mode": "in-place"}
    )

    with pytest.raises(HTTPException) as excinfo:
        backups_api.apply_payload(
            cfg,
            settings,
            ref=backup.backup_id,
            body={
                "mode": "in-place",
                "plan_fingerprint": staged["plan_fingerprint"],
                "confirm_target": service.target_label(cfg),
            },
            actor=_actor(),
        )

    assert excinfo.value.status_code == 403
    assert excinfo.value.detail["error"] == "destructive_operations_blocked"


def test_a_refused_apply_mutates_nothing(env, backup):
    from fastapi import HTTPException

    from daylily_tapdb.backup import introspect

    cfg, settings = env

    def counts():
        with service.open_session(cfg, app_username="pytest") as conn:
            with conn.session_scope(commit=False) as session:
                return introspect.capture_row_counts(session, cfg["schema_name"])

    before = counts()
    with pytest.raises(HTTPException):
        backups_api.apply_payload(
            cfg,
            settings,
            ref=backup.backup_id,
            body={
                "mode": "in-place",
                "plan_fingerprint": "stale",
                "confirm_target": service.target_label(cfg),
            },
            actor=_actor(),
        )

    assert counts() == before


def test_an_isolated_apply_succeeds_end_to_end(env, backup):
    cfg, settings = env
    staged = backups_api.stage_payload(cfg, settings, ref=backup.backup_id, body={})

    payload = backups_api.apply_payload(
        cfg,
        settings,
        ref=backup.backup_id,
        body={
            "plan_fingerprint": staged["plan_fingerprint"],
            "confirm_target": service.target_label(cfg),
        },
        actor=_actor(),
    )

    assert payload["ok"] is True
    from daylily_tapdb.backup import engine as eng

    eng.run_command(
        eng.build_psql_command(
            cfg,
            sql=f'DROP DATABASE IF EXISTS "{payload["target_database"]}"',
            database="postgres",
        ),
        env=eng.client_env(cfg),
    )


# ---------------------------------------------------------------------------
# rehearse
# ---------------------------------------------------------------------------


def test_rehearse_returns_an_evidence_pointer(env, backup, tmp_path):
    cfg, settings = env

    payload = backups_api.rehearse_payload(
        cfg, settings, ref=backup.backup_id, body={}, actor=_actor()
    )

    assert payload["ok"] is True
    assert payload["evidence_key"]
    assert (tmp_path / "store" / payload["evidence_key"]).is_file()


# ---------------------------------------------------------------------------
# the shared-path contract
# ---------------------------------------------------------------------------


def test_the_api_applies_through_the_shared_view(monkeypatch, env, backup):
    """The API must not have its own apply logic.

    Plan section 4.2: the apply flow lives in
    ``views.apply_restore_from_review`` so the API and the GUI are literally
    the same code path.
    """
    cfg, settings = env
    called: dict = {}

    def _spy(*args, **kwargs):
        called.update(kwargs)
        raise RestoreStageStaleError("intercepted")

    monkeypatch.setattr(views, "apply_restore_from_review", _spy)
    monkeypatch.setattr(backups_api.views, "apply_restore_from_review", _spy)

    from fastapi import HTTPException

    with pytest.raises(HTTPException):
        backups_api.apply_payload(
            cfg,
            settings,
            ref=backup.backup_id,
            body={"plan_fingerprint": "f", "confirm_target": "t"},
            actor=_actor(),
        )

    assert called["backup_id"] == backup.backup_id
    assert called["plan_fingerprint"] == "f"
    assert called["confirm_target"] == "t"


def test_the_admin_adapter_never_shells_out():
    import ast
    from pathlib import Path

    names: set[str] = set()
    for node in ast.walk(ast.parse(Path("admin/backups.py").read_text())):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module)

    assert "subprocess" not in names
    assert "daylily_tapdb.backup.engine" not in names


def test_views_stay_free_of_web_frameworks():
    import ast
    from pathlib import Path

    names: set[str] = set()
    for node in ast.walk(ast.parse(Path("daylily_tapdb/backup/views.py").read_text())):
        if isinstance(node, ast.Import):
            names.update(a.name.split(".")[0] for a in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])

    # views.py is shared by the API and the GUI; importing either surface's
    # framework would make it unusable by the other.
    for framework in ("fastapi", "typer", "jinja2", "starlette"):
        assert framework not in names


def test_apply_view_requires_both_fields_independently_of_the_api():
    """The guard lives in the view, not only in the HTTP adapter.

    A future surface calling the view directly gets the same protection.
    """
    with pytest.raises(RestoreStageStaleError):
        views.apply_restore_from_review(
            {
                "client_id": "c",
                "database_name": "d",
                "schema_name": "s",
                "database": "db",
            },
            {},
            backup_id="x",
            plan_fingerprint=None,
            confirm_target="label",
        )

    # Only in-place has its label checked, so only in-place may demand one.
    with pytest.raises(RestoreConfirmationError):
        views.apply_restore_from_review(
            {
                "client_id": "c",
                "database_name": "d",
                "schema_name": "s",
                "database": "db",
            },
            {},
            backup_id="x",
            plan_fingerprint="f",
            confirm_target=None,
            options=verify.RestoreOptions(mode="in-place"),
        )


# ---------------------------------------------------------------------------
# health -- the HTTP form of the alerting contract
# ---------------------------------------------------------------------------


def test_health_returns_200_when_everything_is_fine(env, backup):
    cfg, settings = env

    payload = backups_api.health_payload(cfg, settings)

    assert payload["status"] in {"ok", "warn"}
    assert payload["ok"] is True
    assert payload["checks"]


def test_a_failing_target_is_200_not_5xx(env):
    """The decision worth defending: a broken backup is not a broken service.

    Returning 5xx here would put a correctly-functioning detector behind every
    proxy, retry layer and uptime monitor between the caller and this service,
    each of which reads 5xx as "sick, retry". The finding would be retried,
    rate-limited, and eventually surface as a TapDB outage rather than as the
    backup problem it is.
    """
    cfg, settings = env  # no backups created -- health.inventory fails

    payload = backups_api.health_payload(cfg, settings)

    assert payload["status"] == "failing"
    assert payload["ok"] is False
    assert "health.inventory" in payload["failing"]


def test_health_that_cannot_answer_is_503(env):
    """503 is reserved for "no verdict", the HTTP analogue of exit 2."""
    from fastapi import HTTPException

    cfg, settings = env
    settings["storage_uri"] = "s3://"

    with pytest.raises(HTTPException) as excinfo:
        backups_api.health_payload(cfg, settings)

    assert excinfo.value.status_code == 503
    assert excinfo.value.detail["error"] == "health_unavailable"
    # Same shape as the 200 body. A caller told to "read `status` from the
    # body" must not hit a KeyError on the one path that means "I could not
    # answer" -- that is the response most likely to be parsed by a monitor.
    assert excinfo.value.detail["status"] == "unavailable"
    assert excinfo.value.detail["ok"] is False
    assert "checks" in excinfo.value.detail


def test_the_api_and_cli_agree_on_the_verdict(env, backup):
    """Same implementation, so the same answer -- asserted, not assumed."""
    cfg, settings = env
    report = service.health_report(cfg, settings)

    payload = backups_api.health_payload(cfg, settings)

    assert payload == report.to_payload()
