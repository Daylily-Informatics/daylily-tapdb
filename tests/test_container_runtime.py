from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path

import pytest
import yaml
from fastapi import FastAPI
from fastapi.testclient import TestClient

import daylily_tapdb.admin_health as admin_health_mod
import daylily_tapdb.cli.admin_server as admin_server_mod
import daylily_tapdb.container_entry as container_entry_mod
from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.web.bridge import TapdbHostBridge
from daylily_tapdb.web.factory import TapdbHostBridgeMount

ROOT = Path(__file__).resolve().parents[1]


def test_container_runtime_files_are_explicit() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8")
    entrypoint = ROOT / "docker" / "entrypoint.sh"

    assert (
        "uv sync --frozen --no-dev --extra admin --extra cli --extra aurora"
        in dockerfile
    )
    assert "postgresql-client" in dockerfile
    assert 'CMD ["python", "-m", "daylily_tapdb.container_entry"]' in dockerfile
    assert "python:3.12-slim-bookworm" in dockerfile
    assert ":latest" not in dockerfile
    assert ".git" in dockerignore
    assert entrypoint.exists()
    assert entrypoint.stat().st_mode & 0o111


def test_container_entry_builds_foreground_admin_server_argv() -> None:
    argv = container_entry_mod.build_admin_server_argv(
        {
            "TAPDB_CONFIG_PATH": "/config/tapdb.yaml",
            "TAPDB_ADMIN_HOST": "0.0.0.0",
            "TAPDB_ADMIN_PORT": "8910",
            "TAPDB_ADMIN_TLS_MODE": "http",
            "TAPDB_ADMIN_HTTP_CONTEXT": "local-compose",
        }
    )

    assert argv[:3] == [
        container_entry_mod.sys.executable,
        "-m",
        "daylily_tapdb.cli.admin_server",
    ]
    assert "--config" in argv
    assert "/config/tapdb.yaml" in argv
    assert "--env" not in argv
    assert "--tls-mode" in argv
    assert "http" in argv
    assert "--ssl-keyfile" not in argv

    https_argv = container_entry_mod.build_admin_server_argv(
        {
            "TAPDB_CONFIG_PATH": "/config/tapdb.yaml",
            "TAPDB_ADMIN_HOST": "0.0.0.0",
            "TAPDB_ADMIN_PORT": "8910",
            "TAPDB_ADMIN_TLS_MODE": "https",
            "TAPDB_ADMIN_TLS_KEYFILE": "/tls/key.pem",
            "TAPDB_ADMIN_TLS_CERTFILE": "/tls/cert.pem",
        }
    )
    assert "--ssl-keyfile" in https_argv
    assert "/tls/key.pem" in https_argv
    assert "--ssl-certfile" in https_argv
    assert "/tls/cert.pem" in https_argv


def test_container_entry_rejects_missing_inputs_and_non_compose_http() -> None:
    with pytest.raises(RuntimeError, match="TAPDB_CONFIG_PATH is required"):
        container_entry_mod.build_admin_server_argv({})

    base = {
        "TAPDB_CONFIG_PATH": "/config/tapdb.yaml",
        "TAPDB_ADMIN_HOST": "0.0.0.0",
        "TAPDB_ADMIN_PORT": "8910",
        "TAPDB_ADMIN_TLS_MODE": "http",
    }
    with pytest.raises(RuntimeError, match="only allowed for local Compose"):
        container_entry_mod.build_admin_server_argv(base)

    with pytest.raises(RuntimeError, match="must be an integer"):
        container_entry_mod.build_admin_server_argv(
            {**base, "TAPDB_ADMIN_PORT": "not-a-port"}
        )


def test_admin_server_tls_mode_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.delenv("TAPDB_ADMIN_TLS_MODE", raising=False)
    with pytest.raises(RuntimeError, match="TAPDB_ADMIN_TLS_MODE is required"):
        admin_server_mod._resolve_tls_mode(None)

    monkeypatch.setenv("TAPDB_ADMIN_TLS_MODE", "http")
    monkeypatch.delenv("TAPDB_ADMIN_HTTP_CONTEXT", raising=False)
    with pytest.raises(RuntimeError, match="only allowed for local Compose"):
        admin_server_mod._resolve_tls_mode(None)

    monkeypatch.setenv("TAPDB_ADMIN_HTTP_CONTEXT", "local-compose")
    assert admin_server_mod._resolve_tls_mode(None) == "http"
    assert (
        admin_server_mod._uvicorn_tls_kwargs(
            tls_mode="http",
            ssl_keyfile=None,
            ssl_certfile=None,
        )
        == {}
    )

    with pytest.raises(RuntimeError, match="requires --ssl-keyfile"):
        admin_server_mod._uvicorn_tls_kwargs(
            tls_mode="https",
            ssl_keyfile=None,
            ssl_certfile=None,
        )

    key_path = tmp_path / "key.pem"
    cert_path = tmp_path / "cert.pem"
    key_path.write_text("key", encoding="utf-8")
    cert_path.write_text("cert", encoding="utf-8")
    assert admin_server_mod._uvicorn_tls_kwargs(
        tls_mode="https",
        ssl_keyfile=str(key_path),
        ssl_certfile=str(cert_path),
    ) == {"ssl_keyfile": str(key_path), "ssl_certfile": str(cert_path)}


def test_admin_health_routes_probe_explicit_config_and_db(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()
    calls: dict[str, object] = {}

    class _Scalar:
        def scalar(self):
            return 1

    class _Session:
        def execute(self, statement):
            calls["statement"] = str(statement)
            return _Scalar()

    class _Conn:
        app_username = ""

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @contextmanager
        def session_scope(self):
            yield _Session()

    monkeypatch.setattr(
        admin_health_mod,
        "get_db_config",
        lambda config_path: (
            calls.update({"config_path": config_path})
            or {"engine_type": "compose", "database": "tapdb_dev"}
        ),
    )
    monkeypatch.setattr(
        admin_health_mod,
        "get_runtime_db",
        lambda config_path: calls.update({"runtime": config_path}) or _Conn(),
    )

    admin_health_mod.install_tapdb_admin_health_routes(
        app,
        config_path="/tmp/tapdb.yaml",
    )
    client = TestClient(app)

    assert client.get("/healthz").json()["status"] == "ok"
    ready = client.get("/readyz")
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"
    resolved_config_path = str(Path("/tmp/tapdb.yaml").resolve())
    assert calls["config_path"] == resolved_config_path
    assert calls["runtime"] == resolved_config_path
    assert calls["statement"] == "SELECT 1"


def test_admin_readyz_returns_503_with_exact_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = FastAPI()

    def _fail(*_args, **_kwargs):
        raise RuntimeError("No TAPDB config found at /missing/tapdb.yaml.")

    monkeypatch.setattr(admin_health_mod, "get_db_config", _fail)
    admin_health_mod.install_tapdb_admin_health_routes(
        app,
        config_path="/missing/tapdb.yaml",
    )

    response = TestClient(app).get("/readyz")
    assert response.status_code == 503
    assert response.json()["status"] == "not_ready"
    assert response.json()["error"] == "No TAPDB config found at /missing/tapdb.yaml."


def test_compose_engine_type_allows_explicit_postgres_service_host(
    tmp_path: Path,
) -> None:
    domain_registry = tmp_path / "domain_code_registry.json"
    prefix_registry = tmp_path / "prefix_ownership_registry.json"
    domain_registry.write_text(
        '{"version":"0.4.0","domains":{"Z":{"name":"test-localhost"}}}\n',
        encoding="utf-8",
    )
    prefix_registry.write_text(
        (
            '{"version":"0.4.0","ownership":{"Z":{"TPX":{"issuer_app_code":"daylily-tapdb"},'
            '"EDG":{"issuer_app_code":"daylily-tapdb"},'
            '"ADT":{"issuer_app_code":"daylily-tapdb"},'
            '"SYS":{"issuer_app_code":"daylily-tapdb"},'
            '"MSG":{"issuer_app_code":"daylily-tapdb"}}}}\n'
        ),
        encoding="utf-8",
    )
    cfg_path = tmp_path / "tapdb-config.yaml"
    cfg_path.write_text(
        yaml.safe_dump(
            {
                "meta": {
                    "config_version": 4,
                    "client_id": "compose",
                    "database_name": "tapdb",
                    "owner_repo_name": "daylily-tapdb",
                    "domain_registry_path": str(domain_registry),
                    "prefix_ownership_registry_path": str(prefix_registry),
                },
                "target": {
                    "engine_type": "compose",
                    "host": "postgres",
                    "port": "5432",
                    "ui_port": "8910",
                    "domain_code": "Z",
                    "user": "tapdb",
                    "password": "tapdbpw",
                    "database": "tapdb_dev",
                    "schema_name": "tapdb_compose_dev",
                },
                "safety": {
                    "safety_tier": "local",
                    "destructive_operations": "blocked",
                },
            }
        ),
        encoding="utf-8",
    )
    os.chmod(cfg_path, 0o600)

    cfg = get_db_config(config_path=cfg_path)

    assert cfg["engine_type"] == "compose"
    assert cfg["host"] == "postgres"
    assert cfg["port"] == "5432"
    assert cfg["database"] == "tapdb_dev"
    assert cfg["schema_name"] == "tapdb_compose_dev"


def test_host_bridge_allows_health_and_readiness_without_user() -> None:
    app = FastAPI()

    @app.get("/healthz")
    async def _healthz():
        return {"status": "ok"}

    @app.get("/readyz")
    async def _readyz():
        return {"status": "ready"}

    wrapped = TapdbHostBridgeMount(
        app,
        TapdbHostBridge(auth_mode="host_session", service_name="dewey"),
    )
    client = TestClient(wrapped)

    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").status_code == 200
    assert client.get("/admin", follow_redirects=False).status_code == 302
