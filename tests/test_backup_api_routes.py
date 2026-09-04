"""Canonical GUI backup-API registration and authorization contracts."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from admin.auth import PERMISSIONS, get_user_permissions
from daylily_tapdb.gui import create_tapdb_gui_router

BACKUP_ROUTES = [
    ("GET", "/api/admin/backups"),
    ("GET", "/api/admin/backups/status"),
    ("GET", "/api/admin/backups/health"),
    ("GET", "/api/admin/backups/plan"),
    ("POST", "/api/admin/backups"),
    ("POST", "/api/admin/backups/abc/verify"),
    ("POST", "/api/admin/backups/abc/restore/stage"),
    ("POST", "/api/admin/backups/abc/restore/apply"),
    ("POST", "/api/admin/backups/abc/rehearse"),
]


@pytest.fixture
def app():
    value = FastAPI()
    router = create_tapdb_gui_router(config_path="/tmp/config.yaml")
    value.state.canonical_router = router
    value.include_router(router)
    return value


@pytest.fixture
def client(app):
    return TestClient(app, raise_server_exceptions=False)


def test_all_nine_backup_operations_are_mounted_on_the_canonical_stack(app):
    mounted = {
        (method, route.path)
        for route in app.state.canonical_router.routes
        for method in getattr(route, "methods", set()) - {"HEAD", "OPTIONS"}
    }
    expected = {
        ("GET", "/api/admin/backups"),
        ("GET", "/api/admin/backups/status"),
        ("GET", "/api/admin/backups/health"),
        ("GET", "/api/admin/backups/plan"),
        ("POST", "/api/admin/backups"),
        ("POST", "/api/admin/backups/{ref}/verify"),
        ("POST", "/api/admin/backups/{ref}/restore/stage"),
        ("POST", "/api/admin/backups/{ref}/restore/apply"),
        ("POST", "/api/admin/backups/{ref}/rehearse"),
    }
    assert expected <= mounted
    assert not any(path.startswith("/api/backups") for _method, path in mounted)


def test_create_is_declared_201(app):
    route = next(
        item
        for item in app.state.canonical_router.routes
        if getattr(item, "path", "") == "/api/admin/backups"
        and "POST" in getattr(item, "methods", set())
    )
    assert route.status_code == 201


@pytest.fixture
def as_user(monkeypatch):
    import admin.auth as auth_mod

    def set_user(user):
        async def current(_request):
            return user

        monkeypatch.setattr(auth_mod, "get_current_user", current)

    return set_user


@pytest.mark.parametrize("method,path", BACKUP_ROUTES)
def test_anonymous_api_callers_receive_401(client, as_user, method, path):
    as_user(None)
    assert client.request(method, path, follow_redirects=False).status_code == 401


@pytest.mark.parametrize("method,path", BACKUP_ROUTES)
def test_non_admin_callers_receive_403(client, as_user, method, path):
    as_user({"email": "user@example.com", "role": "user"})
    assert client.request(method, path, follow_redirects=False).status_code == 403


@pytest.mark.parametrize("method,path", BACKUP_ROUTES)
def test_password_change_gate_precedes_backup_operations(client, as_user, method, path):
    as_user(
        {
            "email": "admin@example.com",
            "role": "admin",
            "require_password_change": True,
        }
    )
    response = client.request(method, path, follow_redirects=False)
    assert response.status_code == 403
    assert response.json()["detail"] == "tapdb_gui_password_change_required"


def test_backup_permission_remains_admin_only():
    assert "can_manage_backups" in PERMISSIONS["admin"]
    assert "can_manage_backups" in PERMISSIONS["user"]
    assert get_user_permissions({"role": "admin"})["can_manage_backups"] is True
    assert get_user_permissions({"role": "user"})["can_manage_backups"] is False
    assert get_user_permissions(None) == {}


def test_every_json_route_delegates_to_the_shared_backup_adapter():
    tree = ast.parse(Path("daylily_tapdb/gui/router.py").read_text())
    handlers = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef))
        and node.name.startswith("backups_")
        and node.name.endswith("_api")
    ]
    assert len(handlers) == 10
    for handler in handlers:
        body = ast.dump(handler)
        assert "backups_api" in body or handler.name in {
            "backups_api",
            "backups_status_api",
            "backups_restore_review_api",
        }


def test_configuration_failure_is_an_explicit_503(client, as_user, monkeypatch):
    import daylily_tapdb.cli.db_config as db_config

    as_user({"email": "admin@example.com", "role": "admin"})
    monkeypatch.setattr(
        db_config,
        "get_backup_settings",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("no config")),
    )
    response = client.get("/api/admin/backups")
    assert response.status_code == 503
    assert response.json()["detail"]["error"] == "backup_unavailable"
