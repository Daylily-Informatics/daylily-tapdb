from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

import daylily_tapdb.cli.user as user_mod

runner = CliRunner()


class _FakeConn:
    def __init__(self) -> None:
        self.sessions: list[SimpleNamespace] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    @contextmanager
    def session_scope(self, commit: bool = False):
        session = SimpleNamespace(commit=commit)
        self.sessions.append(session)
        yield session


def test_user_list_shows_no_users(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(user_mod, "list_users", lambda _session, include_inactive=False: [])

    result = runner.invoke(user_mod.user_app, ["list", "dev"])

    assert result.exit_code == 0
    assert "No users found" in result.output


def test_user_list_renders_table(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(
        user_mod,
        "list_users",
        lambda _session, include_inactive=False: [
            SimpleNamespace(
                username="admin@example.com",
                email="admin@example.com",
                display_name="Admin",
                role="admin",
                is_active=True,
                created_dt="2026-03-29T12:00:00+00:00",
                last_login_dt="2026-03-29T14:00:00+00:00",
            )
        ],
    )

    result = runner.invoke(user_mod.user_app, ["list", "dev"])

    assert result.exit_code == 0
    assert "TAPDB Users (dev)" in result.output
    assert "Admin" in result.output
    assert "14:00" in result.output


def test_user_add_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(user_mod, "_hash_password", lambda value: f"hashed:{value}")
    captured: dict[str, object] = {}

    def _fake_create_or_get(session, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(username=kwargs["login_identifier"]), True

    monkeypatch.setattr(user_mod, "create_or_get", _fake_create_or_get)

    result = runner.invoke(
        user_mod.user_app,
        [
            "add",
            "dev",
            "--username",
            "alice@example.com",
            "--role",
            "admin",
            "--email",
            "alice@example.com",
            "--name",
            "Alice",
            "--password",
            "secret",
        ],
    )

    assert result.exit_code == 0
    assert "Created user" in result.output
    assert captured["login_identifier"] == "alice@example.com"
    assert captured["role"] == "admin"
    assert captured["password_hash"] == "hashed:secret"


def test_user_add_duplicate_exits_nonzero(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(
        user_mod,
        "create_or_get",
        lambda session, **kwargs: (SimpleNamespace(username=kwargs["login_identifier"]), False),
    )

    result = runner.invoke(
        user_mod.user_app,
        ["add", "dev", "--username", "alice@example.com"],
    )

    assert result.exit_code == 1
    assert "already exists" in result.output


def test_user_set_role_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(user_mod, "set_role", lambda _session, _username, _role: True)

    result = runner.invoke(
        user_mod.user_app,
        ["set-role", "dev", "alice@example.com", "admin"],
    )

    assert result.exit_code == 0
    assert "role to" in result.output


def test_user_activate_and_deactivate_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    calls: list[bool] = []

    def _fake_set_active(_session, _username, is_active):
        calls.append(is_active)
        return True

    monkeypatch.setattr(user_mod, "set_active", _fake_set_active)

    deactivate = runner.invoke(user_mod.user_app, ["deactivate", "dev", "alice"])
    activate = runner.invoke(user_mod.user_app, ["activate", "dev", "alice"])

    assert deactivate.exit_code == 0
    assert activate.exit_code == 0
    assert calls == [False, True]


def test_user_set_password_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(user_mod, "_hash_password", lambda value: f"hashed:{value}")
    captured: dict[str, object] = {}

    def _fake_set_password_hash(_session, username, password_hash, *, require_password_change=None):
        captured["username"] = username
        captured["password_hash"] = password_hash
        captured["require_password_change"] = require_password_change
        return True

    monkeypatch.setattr(user_mod, "set_password_hash", _fake_set_password_hash)

    result = runner.invoke(
        user_mod.user_app,
        ["set-password", "dev", "alice", "--password", "secret"],
    )

    assert result.exit_code == 0
    assert "Password updated" in result.output
    assert captured == {
        "username": "alice",
        "password_hash": "hashed:secret",
        "require_password_change": None,
    }


def test_user_delete_cancelled_without_force(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(user_mod.typer, "confirm", lambda _msg: False)

    result = runner.invoke(user_mod.user_app, ["delete", "dev", "alice"])

    assert result.exit_code == 0
    assert "Cancelled" in result.output


def test_user_delete_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(user_mod.typer, "confirm", lambda _msg: True)
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(user_mod, "soft_delete", lambda _session, _username: True)

    result = runner.invoke(user_mod.user_app, ["delete", "dev", "alice"])

    assert result.exit_code == 0
    assert "Deleted user" in result.output
