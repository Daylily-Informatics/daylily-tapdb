from __future__ import annotations

import base64
import json
from types import SimpleNamespace

import itsdangerous
import pytest

import admin.auth as auth


def _signed_bloom_cookie(
    *,
    secret: str,
    payload: dict,
) -> str:
    signer = itsdangerous.TimestampSigner(secret)
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8"))
    return signer.sign(encoded).decode("utf-8")


def test_extract_bloom_user_parses_signed_cookie(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("TAPDB_ADMIN_BLOOM_SESSION_SECRET", "secret-a")
    monkeypatch.setenv("TAPDB_ADMIN_BLOOM_SESSION_COOKIE", "session")
    cookie = _signed_bloom_cookie(
        secret="secret-a",
        payload={"user_data": {"email": "ADMIN@EXAMPLE.COM", "role": "admin"}},
    )
    request = SimpleNamespace(cookies={"session": cookie})

    user = auth._extract_bloom_user(request)

    assert user == {"email": "admin@example.com", "role": "admin"}


def test_extract_bloom_user_invalid_cookie_returns_none(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("TAPDB_ADMIN_BLOOM_SESSION_SECRET", "secret-a")
    monkeypatch.setenv("TAPDB_ADMIN_BLOOM_SESSION_COOKIE", "session")
    request = SimpleNamespace(cookies={"session": "not-a-valid-cookie"})

    assert auth._extract_bloom_user(request) is None


def test_extract_bloom_user_invalid_role_defaults_to_user(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("TAPDB_ADMIN_BLOOM_SESSION_SECRET", "secret-a")
    cookie = _signed_bloom_cookie(
        secret="secret-a",
        payload={"user_data": {"email": "user@example.com", "role": "super-admin"}},
    )
    request = SimpleNamespace(cookies={"session": cookie})

    assert auth._extract_bloom_user(request) == {
        "email": "user@example.com",
        "role": "user",
    }


def test_resolve_shared_auth_user_reuses_existing_user(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(auth, "_shared_auth_enabled", lambda: True)
    monkeypatch.setattr(
        auth,
        "_extract_bloom_user",
        lambda _request: {"email": "admin@example.com", "role": "admin"},
    )
    monkeypatch.setattr(
        auth,
        "get_user_by_username",
        lambda username: {
            "uid": 42,
            "username": username,
            "role": "admin",
            "email": username,
        },
    )
    request = SimpleNamespace(
        session={
            "cognito_challenge": "NEW_PASSWORD_REQUIRED",
            "cognito_challenge_session": "challenge-token",
        }
    )

    user = auth._resolve_shared_auth_user(request)

    assert user is not None
    assert user["uid"] == 42
    assert request.session["user_uid"] == 42
    assert request.session["cognito_username"] == "admin@example.com"
    assert "cognito_challenge" not in request.session
    assert "cognito_challenge_session" not in request.session


def test_resolve_shared_auth_user_creates_missing_user(monkeypatch: pytest.MonkeyPatch):
    created = {}

    monkeypatch.setattr(auth, "_shared_auth_enabled", lambda: True)
    monkeypatch.setattr(
        auth,
        "_extract_bloom_user",
        lambda _request: {"email": "new@example.com", "role": "user"},
    )
    monkeypatch.setattr(auth, "get_user_by_username", lambda _username: None)

    def _create(email: str, *, role: str):
        created["email"] = email
        created["role"] = role
        return {"uid": 77, "username": email, "email": email, "role": role}

    monkeypatch.setattr(auth, "get_or_create_user_from_email", _create)
    request = SimpleNamespace(session={})

    user = auth._resolve_shared_auth_user(request)

    assert user is not None
    assert created == {"email": "new@example.com", "role": "user"}
    assert request.session["user_uid"] == 77
    assert request.session["cognito_username"] == "new@example.com"


def test_resolve_shared_auth_user_handles_create_error(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(auth, "_shared_auth_enabled", lambda: True)
    monkeypatch.setattr(
        auth,
        "_extract_bloom_user",
        lambda _request: {"email": "new@example.com", "role": "user"},
    )
    monkeypatch.setattr(auth, "get_user_by_username", lambda _username: None)

    def _raise(*_args, **_kwargs):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(auth, "get_or_create_user_from_email", _raise)
    request = SimpleNamespace(session={})

    assert auth._resolve_shared_auth_user(request) is None
    assert request.session == {}
