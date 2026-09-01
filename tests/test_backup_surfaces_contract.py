"""The three surfaces reach one service -- proven at runtime, not by review.

Plan section 4.2 claims the CLI, the admin API and the embedded GUI are thin
adapters over ``daylily_tapdb.backup``. Every other test checks one surface in
isolation, which cannot detect the failure that matters here: two surfaces that
each work correctly while calling *different* code, agreeing today and drifting
tomorrow.

The method is to replace one service function with a spy and then drive all
three surfaces at it. A surface that reaches a different function simply never
appears in the recorded calls, and the test names which one went missing.

The spy records and then raises a ``BackupError`` rather than returning a
fabricated result. That is deliberate: hand-written fakes of service return
values drifted from reality once already in this work, and a contract test that
depends on the shape of a fake is a test that can quietly stop meaning
anything. Raising exercises each surface's real error path and asserts only the
thing under test -- that the call happened at all.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from admin import backups as backups_api
from daylily_tapdb.backup import service as service_mod
from daylily_tapdb.backup import verify as verify_mod
from daylily_tapdb.backup.errors import BackupError
from daylily_tapdb.backup.receipts import SURFACE_API, Actor
from daylily_tapdb.cli import app
from daylily_tapdb.gui import create_tapdb_gui_app
from daylily_tapdb.web.bridge import TapdbHostBridge

runner = CliRunner()

BACKUP_ID = "full-20260729T120000Z-abc123"

CFG = {
    "client_id": "testclient",
    "database_name": "testdb",
    "schema_name": "tapdb_prod",
    "database": "tapdb_shared",
    "domain_code": "Z",
    "owner_repo_name": "daylily-tapdb",
    "domain_registry_path": "daylily_tapdb/etc/domain_code_registry.json",
    "prefix_ownership_registry_path": (
        "daylily_tapdb/etc/prefix_ownership_registry.json"
    ),
}
SETTINGS = {"config_dir": "/tmp", "storage_uri": ""}


class _ReachedError(BackupError):
    """Raised by the spy once it has recorded the call."""

    code = "spy_reached"


@pytest.fixture
def spy(monkeypatch):
    """Replace a service function with a recorder, across all three surfaces.

    Returns ``install(module, name) -> calls``. Because every surface resolves
    these as module attributes at call time, patching the attribute once
    catches the CLI, the API and the GUI together.
    """
    calls: list[dict[str, Any]] = []

    def install(module, name: str):
        def _spy(*args, **kwargs):
            calls.append({"args": args, "kwargs": kwargs})
            raise _ReachedError(f"{name} reached")

        monkeypatch.setattr(module, name, _spy)
        return calls

    # The CLI resolves configuration through these; the GUI through the router.
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config", lambda *a, **k: dict(CFG)
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_backup_settings",
        lambda *a, **k: dict(SETTINGS),
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.get_db_config", lambda *a, **k: dict(CFG)
    )
    return install


@pytest.fixture
def gui_client():
    bridge = TapdbHostBridge(
        auth_mode="host_session",
        login_url="/login",
        resolve_user=lambda _request: {
            "username": "admin@example.com",
            "email": "admin@example.com",
            "role": "admin",
        },
    )
    return TestClient(
        create_tapdb_gui_app(config_path="/tmp/tapdb-config.yaml", host_bridge=bridge),
        base_url="https://localhost",
    )


def _api_actor():
    return Actor(surface=SURFACE_API, username="admin@example.com")


def _swallow(fn):
    """Run a surface call, ignoring the spy's deliberate failure."""
    try:
        fn()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Each operation: all three surfaces, one shared function
# ---------------------------------------------------------------------------


def test_create_converges_on_one_service_function(spy, gui_client):
    calls = spy(service_mod, "create_backup")

    _swallow(lambda: runner.invoke(app, ["backup", "create"], catch_exceptions=True))
    _swallow(
        lambda: backups_api.create_payload(
            dict(CFG), dict(SETTINGS), body={}, actor=_api_actor()
        )
    )
    _swallow(
        lambda: gui_client.post("/admin/backups/create", data={"backup_class": "full"})
    )

    assert len(calls) == 3, (
        f"expected CLI, API and GUI to reach service.create_backup; got {len(calls)}"
    )


def test_verify_converges_on_one_service_function(spy, gui_client):
    calls = spy(service_mod, "verify_backup")

    _swallow(lambda: runner.invoke(app, ["backup", "verify", "--backup-id", BACKUP_ID]))
    _swallow(
        lambda: backups_api.verify_payload(
            dict(CFG), dict(SETTINGS), ref=BACKUP_ID, actor=_api_actor()
        )
    )
    _swallow(lambda: gui_client.post(f"/admin/backups/{BACKUP_ID}/verify"))

    assert len(calls) == 3, f"service.verify_backup reached {len(calls)}/3 times"


def test_list_converges_on_one_service_function(spy, gui_client):
    calls = spy(service_mod, "list_backups")

    _swallow(lambda: runner.invoke(app, ["backup", "list"]))
    _swallow(lambda: backups_api.list_payload(dict(CFG), dict(SETTINGS)))
    _swallow(lambda: gui_client.get("/api/admin/backups"))

    assert len(calls) == 3, f"service.list_backups reached {len(calls)}/3 times"


def test_restore_staging_converges_on_one_service_function(spy, gui_client):
    calls = spy(verify_mod, "plan_restore")

    _swallow(
        lambda: runner.invoke(app, ["backup", "restore-plan", "--backup-id", BACKUP_ID])
    )
    _swallow(
        lambda: backups_api.stage_payload(
            dict(CFG), dict(SETTINGS), ref=BACKUP_ID, body={}
        )
    )
    _swallow(lambda: gui_client.get(f"/api/admin/backups/{BACKUP_ID}/restore"))

    assert len(calls) == 3, f"verify.plan_restore reached {len(calls)}/3 times"


def test_restore_apply_converges_on_one_service_function(spy, gui_client):
    """The most important one: the destructive path must not have two versions."""
    calls = spy(verify_mod, "restore_backup")

    _swallow(
        lambda: runner.invoke(
            app,
            [
                "backup",
                "restore",
                "--backup-id",
                BACKUP_ID,
                "--mode",
                "isolated",
            ],
        )
    )
    _swallow(
        lambda: backups_api.apply_payload(
            dict(CFG),
            dict(SETTINGS),
            ref=BACKUP_ID,
            body={"plan_fingerprint": "f", "mode": "isolated"},
            actor=_api_actor(),
        )
    )
    _swallow(
        lambda: gui_client.post(
            f"/admin/backups/{BACKUP_ID}/restore",
            data={"plan_fingerprint": "f", "mode": "isolated"},
        )
    )

    assert len(calls) == 3, f"verify.restore_backup reached {len(calls)}/3 times"


def test_rehearse_converges_on_one_service_function(spy, gui_client):
    calls = spy(verify_mod, "rehearse_restore")

    _swallow(
        lambda: runner.invoke(app, ["backup", "rehearse", "--backup-id", BACKUP_ID])
    )
    _swallow(
        lambda: backups_api.rehearse_payload(
            dict(CFG), dict(SETTINGS), ref=BACKUP_ID, body={}, actor=_api_actor()
        )
    )
    _swallow(lambda: gui_client.post(f"/admin/backups/{BACKUP_ID}/rehearse"))

    assert len(calls) == 3, f"verify.rehearse_restore reached {len(calls)}/3 times"


def test_the_spy_would_notice_a_surface_that_went_its_own_way(spy, gui_client):
    """Guard the guard.

    Every test above passes by counting to three. If the spy could not detect
    absence, they would all pass vacuously -- so drive only two surfaces and
    confirm the count actually falls short.
    """
    calls = spy(service_mod, "list_backups")

    _swallow(lambda: runner.invoke(app, ["backup", "list"]))
    _swallow(lambda: backups_api.list_payload(dict(CFG), dict(SETTINGS)))

    assert len(calls) == 2


# ---------------------------------------------------------------------------
# No surface reimplements the engine
# ---------------------------------------------------------------------------

SURFACE_FILES = [
    Path("daylily_tapdb/cli/backup.py"),
    Path("admin/backups.py"),
    Path("daylily_tapdb/gui/router.py"),
]


def _imports(path: Path) -> set[str]:
    import ast

    names: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            names.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            names.add(node.module.split(".")[0])
            names.add(node.module)
    return names


@pytest.mark.parametrize("path", SURFACE_FILES, ids=lambda p: p.name)
def test_no_surface_imports_the_engine_or_shells_out(path: Path):
    """A surface that builds its own pg_dump is a surface that can drift.

    ``gui/router.py`` is a whole-file check by necessity -- it holds the rest
    of the GUI too -- which makes it a slightly stronger claim than needed and
    is fine: nothing in that file should be shelling out to PostgreSQL.
    """
    imported = _imports(path)

    assert "subprocess" not in imported, f"{path} shells out directly"
    assert "daylily_tapdb.backup.engine" not in imported, (
        f"{path} reaches past the service into the engine"
    )


def _code_only(path: Path) -> str:
    """The file's source with comments and string literals removed.

    These modules explain in their docstrings that they must never build a
    ``pg_dump`` command -- so a plain substring search finds the promise and
    reports it as the violation. Tokenising leaves only executable code.
    """
    import io
    import tokenize

    kept: list[str] = []
    with path.open("rb") as handle:
        for token in tokenize.tokenize(io.BytesIO(handle.read()).readline):
            if token.type in (tokenize.COMMENT, tokenize.STRING):
                continue
            kept.append(token.string)
    return " ".join(kept)


@pytest.mark.parametrize("path", SURFACE_FILES, ids=lambda p: p.name)
def test_no_surface_names_a_postgres_binary(path: Path):
    """Catches a shell-out built without importing subprocess by name."""
    code = _code_only(path)

    for binary in ("pg_dump", "pg_restore", "psql"):
        assert binary not in code, f"{path} references {binary} in code"


def test_the_binary_check_ignores_prose_but_not_code(tmp_path: Path):
    """Guard the guard: prove tokenising did not defeat the check entirely."""
    prose = tmp_path / "prose.py"
    prose.write_text('"""Never call pg_dump here."""\nx = 1\n')
    assert "pg_dump" not in _code_only(prose)

    real = tmp_path / "real.py"
    real.write_text("pg_dump = 1\n")
    assert "pg_dump" in _code_only(real)


def test_health_converges_on_one_service_function(spy):
    """CLI and API must reach the same ``health_report``.

    Only two surfaces here, deliberately: health has no GUI route. The page
    already renders the status block, and a second, subtly different verdict
    on screen is exactly the divergence this file exists to prevent.

    The risk this guards is specific. The API has to translate a verdict into
    an HTTP status, and the obvious shortcut -- deciding "failing" from the
    status block the page already has -- would let the two surfaces disagree
    about whether a target is recoverable while both looked correct.
    """
    calls = spy(service_mod, "health_report")

    _swallow(lambda: runner.invoke(app, ["backup", "health"], catch_exceptions=True))
    _swallow(lambda: backups_api.health_payload(dict(CFG), dict(SETTINGS)))

    assert len(calls) == 2, (
        f"expected CLI and API to reach service.health_report; got {len(calls)}"
    )
