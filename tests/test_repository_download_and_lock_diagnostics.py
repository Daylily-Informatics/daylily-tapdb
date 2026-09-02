from __future__ import annotations

import json
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

import admin.auth as admin_auth
import admin.main as admin_main
import daylily_tapdb.advisory_locks as locks
import daylily_tapdb.gui.router as gui_router
from daylily_tapdb.gui import create_tapdb_gui_app
from daylily_tapdb.templates.repository import repository_pack_bytes
from daylily_tapdb.web.bridge import TapdbHostBridge


class _ScalarRows:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return iter(self._rows)


class _RepositorySession:
    def __init__(self, rows):
        self._rows = rows

    def execute(self, _statement):
        return _ScalarRows(self._rows)


class _Connection:
    def __init__(self, session):
        self.session = session
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def session_scope(self, commit=False):
        del commit
        yield self.session


def _template(name: str, subtype: str):
    return SimpleNamespace(
        name=name,
        polymorphic_discriminator="generic_template",
        category="sample",
        type="specimen",
        subtype=subtype,
        version="1.0",
        instance_prefix="SMP",
        instance_polymorphic_identity="generic_instance",
        validator_ref="UNIVERSAL_PASS@1",
        bstatus="active",
        json_addl={"properties": {"label": ""}},
        json_addl_schema=None,
        is_singleton=False,
        uid=42,
        euid="persisted-template-id",
        domain_code="Z",
        issuer_app_code="owner-repo",
        euid_seq=7,
        machine_uuid="database-only-identity",
        tenant_id="tenant-secret-object",
    )


def _configured_target():
    return {
        "client_id": "service",
        "domain_code": "Z",
        "owner_repo_name": "owner-repo",
    }


def test_repository_download_bytes_are_canonical_deterministic_and_identity_free():
    first = repository_pack_bytes(
        _RepositorySession([_template("Zulu", "z"), _template("Alpha", "a")]),
        domain_code="Z",
        issuer_app_code="owner-repo",
    )
    replay = repository_pack_bytes(
        _RepositorySession([_template("Alpha", "a"), _template("Zulu", "z")]),
        domain_code="Z",
        issuer_app_code="owner-repo",
    )

    assert first == replay
    assert first.endswith(b"\n")
    payload = json.loads(first)
    assert payload["format"] == "tapdb.repository-template-pack/v1"
    assert [item["subtype"] for item in payload["templates"]] == ["a", "z"]
    rendered = first.decode()
    for forbidden in (
        "persisted-template-id",
        "database-only-identity",
        "tenant-secret-object",
        '"uid"',
        '"euid"',
        '"domain_code"',
        '"issuer_app_code"',
        '"euid_seq"',
        '"machine_uuid"',
        '"tenant_id"',
    ):
        assert forbidden not in rendered


def _embedded_client(monkeypatch, *, role: str = "admin"):
    connection = _Connection(object())
    captured = {}
    content = b'{"format":"tapdb.repository-template-pack/v1","templates":[]}\n'

    def _bytes(session, **kwargs):
        captured.update(session=session, **kwargs)
        return content

    monkeypatch.setattr(gui_router, "get_db", lambda _path: connection)
    monkeypatch.setattr(
        gui_router, "get_db_config", lambda config_path: _configured_target()
    )
    monkeypatch.setattr(gui_router, "repository_pack_bytes", _bytes)
    bridge = TapdbHostBridge(
        auth_mode="host_session",
        login_url="/login",
        resolve_user=lambda _request: {
            "username": f"{role}@example.com",
            "email": f"{role}@example.com",
            "role": role,
        },
    )
    client = TestClient(
        create_tapdb_gui_app(config_path="/tmp/tapdb-config.yaml", host_bridge=bridge),
        base_url="https://localhost",
    )
    return client, connection, captured, content


def test_embedded_gui_admin_downloads_exact_template_as_attachment(monkeypatch):
    client, connection, captured, content = _embedded_client(monkeypatch)

    response = client.get(
        "/api/templates/repository/download?euid=persisted-template-id"
    )

    assert response.status_code == 200
    assert response.content == content
    assert response.headers["content-type"].startswith("application/json")
    assert response.headers["content-disposition"] == (
        'attachment; filename="tapdb-repository-template-pack.json"'
    )
    assert response.headers["x-content-type-options"] == "nosniff"
    assert connection.app_username == "admin@example.com"
    assert captured == {
        "session": connection.session,
        "domain_code": "Z",
        "issuer_app_code": "owner-repo",
        "template_euid": "persisted-template-id",
    }


def test_embedded_gui_download_is_admin_only(monkeypatch):
    client, _connection, captured, _content = _embedded_client(monkeypatch, role="user")

    response = client.get("/api/templates/repository/download")

    assert response.status_code == 403
    assert captured == {}


def test_legacy_admin_download_uses_same_serializer_and_admin_auth(monkeypatch):
    async def _admin(_request):
        return {
            "uid": 1,
            "username": "admin@example.com",
            "email": "admin@example.com",
            "role": "admin",
            "require_password_change": False,
        }

    connection = _Connection(object())
    captured = {}
    content = b'{"format":"tapdb.repository-template-pack/v1","templates":[]}\n'

    def _bytes(session, **kwargs):
        captured.update(session=session, **kwargs)
        return content

    monkeypatch.setattr(admin_auth, "get_current_user", _admin)
    monkeypatch.setattr(admin_main, "get_current_user", _admin)
    monkeypatch.setattr(admin_main, "get_db", lambda: connection)
    monkeypatch.setattr(admin_main, "get_db_config", _configured_target)
    monkeypatch.setattr(admin_main, "repository_pack_bytes", _bytes)

    response = TestClient(admin_main.app).get(
        "/api/templates/repository/download?euid=persisted-template-id"
    )

    assert response.status_code == 200
    assert response.content == content
    assert response.headers["content-disposition"] == (
        'attachment; filename="tapdb-repository-template-pack.json"'
    )
    assert connection.app_username == "admin@example.com"
    assert captured["template_euid"] == "persisted-template-id"
    assert captured["session"] is connection.session


class _FalseResult:
    @staticmethod
    def scalar_one():
        return False


class _PostgresSession:
    in_transaction = staticmethod(lambda: True)

    @staticmethod
    def get_bind():
        return SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

    @staticmethod
    def execute(_statement, _parameters):
        return _FalseResult()


def test_advisory_timeout_carries_redacted_actual_wait_receipt(monkeypatch):
    monotonic = iter((10.0, 10.075))
    monkeypatch.setattr(locks.time, "monotonic", lambda: next(monotonic))

    with pytest.raises(locks.AdvisoryLockTimeoutError) as raised:
        locks.acquire_transaction_advisory_lock(
            _PostgresSession(),
            "tenant-secret-namespace",
            "tenant-secret-object",
            timeout_seconds=0.05,
        )

    receipt = raised.value.receipt
    assert receipt.acquired is False
    assert receipt.wait_ms == 75
    assert receipt.timeout_ms == 50
    assert receipt.algorithm == "sha256-framed-signed-int64-v1"
    assert len(receipt.lock_fingerprint) == 64
    assert raised.value.diagnostic == receipt.to_payload()
    rendered = f"{raised.value} {json.dumps(raised.value.diagnostic)}"
    assert "tenant-secret-namespace" not in rendered
    assert "tenant-secret-object" not in rendered


def test_postgresql_timeout_receipt_is_redacted_across_separate_sessions(
    pg_instance,
):
    engine = create_engine(pg_instance["operator_dsn"])
    raw_namespace = "tenant-secret-namespace"
    raw_object = "tenant-secret-object"
    try:
        with Session(engine) as holder, Session(engine) as contender:
            with holder.begin():
                acquired = locks.acquire_transaction_advisory_lock(
                    holder, raw_namespace, raw_object
                )
                assert acquired.acquired is True
                with contender.begin():
                    with pytest.raises(locks.AdvisoryLockTimeoutError) as raised:
                        locks.acquire_transaction_advisory_lock(
                            contender,
                            raw_namespace,
                            raw_object,
                            timeout_seconds=0.03,
                            poll_interval_seconds=0.005,
                        )
                receipt = raised.value.receipt
                assert receipt.acquired is False
                assert receipt.wait_ms >= 30
                assert receipt.timeout_ms == 30
                assert receipt.lock_fingerprint == acquired.lock_fingerprint
                rendered = f"{raised.value} {json.dumps(raised.value.diagnostic)}"
                assert raw_namespace not in rendered
                assert raw_object not in rendered

            with contender.begin():
                after_release = locks.acquire_transaction_advisory_lock(
                    contender,
                    raw_namespace,
                    raw_object,
                    timeout_seconds=0,
                )
                assert after_release.acquired is True
                assert after_release.lock_fingerprint == acquired.lock_fingerprint
    finally:
        engine.dispose()


def test_postgresql_different_key_is_nonblocking_and_rollback_leaves_no_lock(
    pg_instance,
):
    engine = create_engine(pg_instance["operator_dsn"])
    try:
        with Session(engine) as holder, Session(engine) as contender:
            holder.begin()
            holder_pid = holder.execute(text("SELECT pg_backend_pid()")).scalar_one()
            first = locks.acquire_transaction_advisory_lock(
                holder, "tapdb.test", "first-object"
            )

            with contender.begin():
                contender_pid = contender.execute(
                    text("SELECT pg_backend_pid()")
                ).scalar_one()
                different = locks.acquire_transaction_advisory_lock(
                    contender,
                    "tapdb.test",
                    "different-object",
                    timeout_seconds=0,
                )
                assert different.acquired is True
                assert different.lock_fingerprint != first.lock_fingerprint

            holder.rollback()

            with contender.begin():
                after_rollback = locks.acquire_transaction_advisory_lock(
                    contender,
                    "tapdb.test",
                    "first-object",
                    timeout_seconds=0,
                )
                assert after_rollback.acquired is True
                assert after_rollback.lock_fingerprint == first.lock_fingerprint

            with holder.begin():
                remaining = holder.execute(
                    text(
                        "SELECT count(*) FROM pg_locks "
                        "WHERE locktype = 'advisory' AND pid IN (:holder, :contender)"
                    ),
                    {"holder": holder_pid, "contender": contender_pid},
                ).scalar_one()
                assert remaining == 0
    finally:
        engine.dispose()


def test_template_page_documents_download_and_server_side_export_distinction():
    html = (gui_router.TEMPLATES_DIR / "templates.html").read_text(encoding="utf-8")
    assert "Download Canonical Pack" in html
    assert "creates no server-side files" in html
    assert "immutable provenance receipt" in html
