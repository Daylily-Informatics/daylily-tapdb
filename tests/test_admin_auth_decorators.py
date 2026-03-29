from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from fastapi.responses import RedirectResponse

import admin.auth as auth


def _request(*, path: str = "/protected", root_path: str = "") -> SimpleNamespace:
    return SimpleNamespace(
        session={},
        scope={"path": path, "root_path": root_path},
        state=SimpleNamespace(),
    )


@pytest.mark.anyio
async def test_get_current_user_returns_disabled_auth_user(
    monkeypatch: pytest.MonkeyPatch,
):
    expected = {"uid": 0, "username": "tapdb-admin@localhost", "role": "admin"}
    monkeypatch.setattr(auth, "_auth_disabled", lambda: True)
    monkeypatch.setattr(auth, "_disabled_auth_user", lambda: expected)

    user = await auth.get_current_user(_request())

    assert user == expected


@pytest.mark.anyio
async def test_get_current_user_prefers_shared_auth(monkeypatch: pytest.MonkeyPatch):
    expected = {"uid": 22, "username": "shared@example.com", "role": "user"}
    monkeypatch.setattr(auth, "_auth_disabled", lambda: False)
    monkeypatch.setattr(auth, "_resolve_shared_auth_user", lambda _request: expected)

    user = await auth.get_current_user(_request())

    assert user == expected


@pytest.mark.anyio
async def test_get_current_user_marks_password_change_requirement(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(auth, "_auth_disabled", lambda: False)
    monkeypatch.setattr(auth, "_resolve_shared_auth_user", lambda _request: None)
    monkeypatch.setattr(
        auth,
        "get_user_by_uid",
        lambda user_uid: {"uid": int(user_uid), "username": "user@example.com"},
    )
    request = _request()
    request.session["user_uid"] = 44
    request.session["cognito_challenge"] = "NEW_PASSWORD_REQUIRED"

    user = await auth.get_current_user(request)

    assert user is not None
    assert user["uid"] == 44
    assert user["require_password_change"] is True


@pytest.mark.anyio
async def test_require_auth_redirects_to_login_when_missing_user(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _no_user(_request):
        return None

    monkeypatch.setattr(auth, "get_current_user", _no_user)

    @auth.require_auth
    async def _handler(request):
        return request

    response = await _handler(_request(root_path="/tapdb"))

    assert isinstance(response, RedirectResponse)
    assert response.headers["location"] == "/tapdb/login"


@pytest.mark.anyio
async def test_require_auth_redirects_to_password_change_when_required(
    monkeypatch: pytest.MonkeyPatch,
):
    async def _password_change_user(_request):
        return {"uid": 1, "require_password_change": True}

    monkeypatch.setattr(auth, "get_current_user", _password_change_user)

    @auth.require_auth
    async def _handler(request):
        return request

    response = await _handler(_request(root_path="/tapdb"))

    assert isinstance(response, RedirectResponse)
    assert response.headers["location"] == "/tapdb/change-password"


@pytest.mark.anyio
async def test_require_auth_injects_user_into_request_state(
    monkeypatch: pytest.MonkeyPatch,
):
    expected = {"uid": 5, "username": "user@example.com", "role": "user"}

    async def _user(_request):
        return expected

    monkeypatch.setattr(auth, "get_current_user", _user)

    @auth.require_auth
    async def _handler(request):
        return request.state.user

    user = await _handler(_request())

    assert user == expected


@pytest.mark.anyio
async def test_require_admin_rejects_non_admin(monkeypatch: pytest.MonkeyPatch):
    async def _non_admin(_request):
        return {"uid": 9, "role": "user", "require_password_change": False}

    monkeypatch.setattr(auth, "get_current_user", _non_admin)

    @auth.require_admin
    async def _handler(request):
        return request

    with pytest.raises(HTTPException, match="Admin access required"):
        await _handler(_request())


@pytest.mark.anyio
async def test_require_admin_allows_admin(monkeypatch: pytest.MonkeyPatch):
    expected = {"uid": 1, "role": "admin", "require_password_change": False}

    async def _admin(_request):
        return expected

    monkeypatch.setattr(auth, "get_current_user", _admin)

    @auth.require_admin
    async def _handler(request):
        return request.state.user

    user = await _handler(_request())

    assert user == expected


def test_get_user_permissions_defaults_to_user_role():
    assert (
        auth.get_user_permissions({"username": "user@example.com"})
        == auth.PERMISSIONS["user"]
    )
