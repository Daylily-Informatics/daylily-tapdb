"""Route registration, admin gating, and the new permission.

Driven through the FastAPI app rather than the adapter functions, because the
property under test is that every backup route is *reachable only by an admin*
-- and that is a property of the decorators, not of the handlers.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import admin.main as admin_main
from admin.auth import PERMISSIONS, get_user_permissions

BACKUP_ROUTES = [
    ("GET", "/api/backups"),
    ("GET", "/api/backups/status"),
    ("GET", "/api/backups/plan"),
    ("POST", "/api/backups"),
    ("POST", "/api/backups/abc/verify"),
    ("POST", "/api/backups/abc/restore/stage"),
    ("POST", "/api/backups/abc/restore/apply"),
    ("POST", "/api/backups/abc/rehearse"),
]


@pytest.fixture
def client():
    return TestClient(admin_main.app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# registration
# ---------------------------------------------------------------------------


def test_all_eight_documented_routes_are_mounted():
    mounted = {
        (method, route.path)
        for route in admin_main.app.routes
        for method in getattr(route, "methods", set()) - {"HEAD", "OPTIONS"}
    }

    for method, path in [
        ("GET", "/api/backups"),
        ("GET", "/api/backups/status"),
        ("GET", "/api/backups/plan"),
        ("POST", "/api/backups"),
        ("POST", "/api/backups/{ref}/verify"),
        ("POST", "/api/backups/{ref}/restore/stage"),
        ("POST", "/api/backups/{ref}/restore/apply"),
        ("POST", "/api/backups/{ref}/rehearse"),
    ]:
        assert (method, path) in mounted, f"{method} {path} is not mounted"


def test_create_is_declared_201():
    route = next(
        r
        for r in admin_main.app.routes
        if getattr(r, "path", "") == "/api/backups"
        and "POST" in getattr(r, "methods", set())
    )

    assert route.status_code == 201


# ---------------------------------------------------------------------------
# admin gating
# ---------------------------------------------------------------------------


@pytest.fixture
def as_user(monkeypatch):
    """Control who the auth layer believes is calling."""
    import admin.auth as auth_mod

    def _set(user):
        async def _current(request):
            return user

        monkeypatch.setattr(auth_mod, "get_current_user", _current)

    return _set


@pytest.mark.parametrize("method, path", BACKUP_ROUTES)
def test_anonymous_callers_are_redirected_to_login(client, as_user, method, path):
    """Backups can be restored over live data; none of this is public."""
    as_user(None)

    response = client.request(method, path, follow_redirects=False)

    assert response.status_code in (302, 303), (
        f"{method} {path} returned {response.status_code} to an anonymous caller"
    )
    assert "/login" in response.headers.get("location", "")


@pytest.mark.parametrize("method, path", BACKUP_ROUTES)
def test_a_non_admin_user_is_forbidden(client, as_user, method, path):
    as_user({"email": "user@example.com", "role": "user"})

    response = client.request(method, path, follow_redirects=False)

    assert response.status_code == 403, f"{method} {path} allowed a non-admin"


@pytest.mark.parametrize("method, path", BACKUP_ROUTES)
def test_a_user_pending_a_password_change_is_diverted(client, as_user, method, path):
    as_user({"email": "a@b.c", "role": "admin", "require_password_change": True})

    response = client.request(method, path, follow_redirects=False)

    # An admin who has not yet set a password must not reach a restore.
    assert response.status_code in (302, 303)
    assert "change-password" in response.headers.get("location", "")


@pytest.mark.parametrize("method, path", BACKUP_ROUTES)
def test_no_backup_route_ever_answers_an_unauthenticated_caller(client, method, path):
    """Defence in depth, with the auth layer left exactly as deployed.

    Without a resolvable config the auth lookup itself fails, which is an
    error rather than a redirect -- but the response must still never be a
    success.
    """
    response = client.request(method, path, follow_redirects=False)

    assert not 200 <= response.status_code < 300


# ---------------------------------------------------------------------------
# permission
# ---------------------------------------------------------------------------


def test_the_new_permission_exists_for_both_roles():
    assert "can_manage_backups" in PERMISSIONS["admin"]
    assert "can_manage_backups" in PERMISSIONS["user"]


def test_only_admins_may_manage_backups():
    assert get_user_permissions({"role": "admin"})["can_manage_backups"] is True
    assert get_user_permissions({"role": "user"})["can_manage_backups"] is False


def test_an_anonymous_caller_has_no_permissions_at_all():
    assert get_user_permissions(None) == {}


def test_the_permission_matches_the_other_destructive_capabilities():
    # Managing backups is at least as dangerous as deleting an object, so it
    # must not be granted anywhere those are withheld.
    for role, perms in PERMISSIONS.items():
        if not perms.get("can_delete_object"):
            assert not perms.get("can_manage_backups"), role


# ---------------------------------------------------------------------------
# adapter wiring
# ---------------------------------------------------------------------------


def test_routes_delegate_to_the_admin_adapter():
    import ast
    from pathlib import Path

    source = Path("admin/main.py").read_text()
    tree = ast.parse(source)
    handlers = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name.startswith("api_backups_")
    ]

    assert len(handlers) == 9
    for handler in handlers:
        body = ast.dump(handler)
        # Each route body must call into admin/backups.py, not do work itself.
        assert "backups_api" in body, handler.name


def test_a_config_failure_is_a_503_not_a_crash():
    """An unconfigured admin service should say so, not 500.

    503 tells a caller the service cannot serve backups right now, which is
    materially different from the request being wrong.
    """
    from fastapi import HTTPException

    import daylily_tapdb.cli.db_config as db_config_mod

    original = db_config_mod.get_backup_settings
    db_config_mod.get_backup_settings = lambda **kw: (_ for _ in ()).throw(
        RuntimeError("no config")
    )
    try:
        with pytest.raises(HTTPException) as excinfo:
            admin_main._backup_context()
    finally:
        db_config_mod.get_backup_settings = original

    assert excinfo.value.status_code == 503
