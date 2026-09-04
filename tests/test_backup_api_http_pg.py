"""The backup API over real HTTP, as an authenticated admin.

The other API tests call the adapter functions directly, which verifies the
handlers but not the wiring: a route forwarding the wrong argument, dropping a
query parameter, or mis-declaring a status code would pass all of them. These
go through the ASGI app so the whole path is exercised.

Uses the ``pg_instance`` fixture -- an ephemeral cluster under pytest's tmp
dir, torn down afterwards.
"""

from __future__ import annotations

import shutil

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.backup import service
from daylily_tapdb.cli import framework_app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db_config import get_backup_settings, get_db_config
from daylily_tapdb.gui import create_tapdb_gui_app
from daylily_tapdb.web.bridge import TapdbHostBridge

runner = CliRunner()

pytestmark = pytest.mark.skipif(
    not shutil.which("pg_dump") or not shutil.which("pg_restore"),
    reason="pg_dump/pg_restore not on PATH",
)

ADMIN = {"email": "admin@example.com", "role": "admin"}


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
def client(env, monkeypatch):
    """An admin-authenticated client wired to isolated backup storage."""
    cfg, settings = env
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_backup_settings",
        lambda *args, **kwargs: settings,
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.get_db_config",
        lambda *args, **kwargs: cfg,
    )
    bridge = TapdbHostBridge(
        auth_mode="host_session",
        service_name="testclient",
        resolve_user=lambda _request: dict(ADMIN),
    )
    return TestClient(
        create_tapdb_gui_app(config_path=str(cfg["config_path"]), host_bridge=bridge),
        base_url="https://localhost",
        raise_server_exceptions=False,
    )


@pytest.fixture
def backup(env):
    cfg, settings = env
    return service.create_backup(cfg, settings)


# ---------------------------------------------------------------------------
# read-only routes
# ---------------------------------------------------------------------------


def test_get_backups_returns_listing_and_status(client, backup):
    response = client.get("/api/admin/backups")

    assert response.status_code == 200
    payload = response.json()
    assert payload["count"] >= 1
    assert payload["status"]["status"] == "ok"


def test_get_backups_honours_the_class_query_parameter(client, backup):
    """Verifies the alias wiring: the query is `class`, the kwarg is not."""
    matching = client.get("/api/admin/backups", params={"class": "full"}).json()
    other = client.get("/api/admin/backups", params={"class": "template-pack"}).json()

    assert matching["count"] >= 1
    assert other["count"] == 0


def test_get_backups_rejects_an_unknown_class(client):
    response = client.get("/api/admin/backups", params={"class": "nonsense"})

    assert response.status_code == 400


def test_get_backups_honours_limit(client, env):
    cfg, settings = env
    for _ in range(3):
        service.create_backup(cfg, settings)

    assert (
        len(client.get("/api/admin/backups", params={"limit": 2}).json()["backups"])
        == 2
    )


def test_get_status(client, backup):
    response = client.get("/api/admin/backups/status")

    assert response.status_code == 200
    assert response.json()["receipt_chain"]["ok"] is True


def test_get_plan_is_read_only(client, tmp_path):
    response = client.get("/api/admin/backups/plan")

    assert response.status_code == 200
    assert response.json()["ok"] is True
    assert not (tmp_path / "store").exists()


def test_get_plan_accepts_strict(client):
    response = client.get("/api/admin/backups/plan", params={"strict": "true"})

    assert response.status_code == 200


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


def test_post_backups_returns_201(client):
    response = client.post("/api/admin/backups", json={"note": "via http"})

    assert response.status_code == 201
    assert response.json()["manifest"]["timestamps"]["note"] == "via http"


def test_post_backups_accepts_an_empty_body(client):
    # The route must tolerate no body at all, not 422 on it.
    response = client.post("/api/admin/backups")

    assert response.status_code == 201


def test_post_backups_records_the_api_actor(client, env):
    from daylily_tapdb.backup.receipts import read_receipts

    _cfg, settings = env
    client.post("/api/admin/backups", json={})

    receipt = read_receipts(service.receipts_directory(settings))[-1]
    assert receipt.actor.surface == "api"
    assert receipt.actor.username == ADMIN["email"]


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


def test_post_verify_passes_the_ref_through(client, backup):
    response = client.post(f"/api/admin/backups/{backup.backup_id}/verify")

    assert response.status_code == 200
    assert response.json()["ok"] is True


def test_post_verify_is_422_on_corruption(client, backup, tmp_path):
    artifact = tmp_path / "store" / backup.storage_prefix / "tapdb.dump"
    raw = bytearray(artifact.read_bytes())
    for offset in range(len(raw) // 2, len(raw) // 2 + 512):
        raw[offset] ^= 0xFF
    artifact.write_bytes(bytes(raw))

    response = client.post(f"/api/admin/backups/{backup.backup_id}/verify")

    assert response.status_code == 422
    assert response.json()["detail"]["error"] == "backup_verification_failed"


def test_post_verify_honours_the_level_field(client, backup):
    response = client.post(
        f"/api/admin/backups/{backup.backup_id}/verify", json={"level": "quick"}
    )

    assert response.status_code == 200
    assert response.json()["level"] == "quick"


def test_post_verify_rejects_an_unknown_level(client, backup):
    response = client.post(
        f"/api/admin/backups/{backup.backup_id}/verify", json={"level": "medium"}
    )

    assert response.status_code == 400


def test_post_verify_of_an_unknown_backup_is_404(client):
    response = client.post("/api/admin/backups/full-nope/verify")

    assert response.status_code == 404


def test_a_malformed_ref_in_the_path_is_400(client):
    response = client.post("/api/admin/backups/has%20space/verify")

    assert response.status_code == 400


# ---------------------------------------------------------------------------
# stage / apply over HTTP
# ---------------------------------------------------------------------------


def test_post_stage_returns_the_fingerprint_and_label(client, backup):
    response = client.post(
        f"/api/admin/backups/{backup.backup_id}/restore/stage",
        json={"mode": "in-place"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["plan_fingerprint"]
    assert payload["required_confirm_target"]


def test_post_apply_without_fields_is_400(client, backup):
    response = client.post(
        f"/api/admin/backups/{backup.backup_id}/restore/apply",
        json={"mode": "in-place"},
    )

    assert response.status_code == 400
    assert response.json()["detail"]["missing"] == [
        "plan_fingerprint",
        "confirm_target",
    ]


def test_post_apply_with_a_wrong_label_is_409(client, backup):
    staged = client.post(
        f"/api/admin/backups/{backup.backup_id}/restore/stage",
        json={"mode": "in-place"},
    ).json()

    response = client.post(
        f"/api/admin/backups/{backup.backup_id}/restore/apply",
        json={
            "mode": "in-place",
            "plan_fingerprint": staged["plan_fingerprint"],
            "confirm_target": "wrong/label@db",
        },
    )

    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "confirm_target_mismatch"


def test_post_apply_with_a_stale_fingerprint_is_409(client, backup):
    staged = client.post(
        f"/api/admin/backups/{backup.backup_id}/restore/stage",
        json={"mode": "in-place"},
    ).json()

    response = client.post(
        f"/api/admin/backups/{backup.backup_id}/restore/apply",
        json={
            "mode": "in-place",
            "plan_fingerprint": "stale",
            "confirm_target": staged["required_confirm_target"],
        },
    )

    assert response.status_code == 409
    assert response.json()["detail"]["error"] == "stale_stage"


def test_an_isolated_apply_succeeds_over_http(client, backup, env):
    cfg, _settings = env
    staged = client.post(
        f"/api/admin/backups/{backup.backup_id}/restore/stage", json={}
    ).json()

    response = client.post(
        f"/api/admin/backups/{backup.backup_id}/restore/apply",
        json={
            "plan_fingerprint": staged["plan_fingerprint"],
            "confirm_target": staged["required_confirm_target"],
        },
    )

    assert response.status_code == 200, response.text
    payload = response.json()
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


def test_apply_rejects_hostile_restore_options(client, backup):
    response = client.post(
        f"/api/admin/backups/{backup.backup_id}/restore/apply",
        json={
            "target_database": "x'; DROP DATABASE postgres; --",
            "plan_fingerprint": "f",
            "confirm_target": "t",
        },
    )

    assert response.status_code == 400


# ---------------------------------------------------------------------------
# rehearse
# ---------------------------------------------------------------------------


def test_post_rehearse_returns_evidence(client, backup, tmp_path):
    response = client.post(f"/api/admin/backups/{backup.backup_id}/rehearse")

    assert response.status_code == 200
    payload = response.json()
    assert payload["ok"] is True
    assert (tmp_path / "store" / payload["evidence_key"]).is_file()


def test_rehearse_over_http_is_recorded_as_a_rehearsal(client, backup, env):
    from daylily_tapdb.backup.receipts import read_receipts

    _cfg, settings = env
    client.post(f"/api/admin/backups/{backup.backup_id}/rehearse")

    operations = [
        r.operation for r in read_receipts(service.receipts_directory(settings))
    ]

    assert "backup_rehearse" in operations
    assert "backup_restore" not in operations


# ---------------------------------------------------------------------------
# cross-surface agreement
# ---------------------------------------------------------------------------


def test_the_api_and_the_service_see_the_same_inventory(client, backup, env):
    cfg, settings = env

    over_http = {
        b["backup_id"] for b in client.get("/api/admin/backups").json()["backups"]
    }
    direct = {e.backup_id for e in service.list_backups(cfg, settings).entries}

    assert over_http == direct
    assert backup.backup_id in over_http
