"""Tests for admin DB connection config resolution."""

from __future__ import annotations

import admin.auth as auth_mod
import admin.main as main_mod


def _capture_connection_kwargs(monkeypatch, module):
    captured = {}

    class _FakeConnection:
        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(module, "TAPDBConnection", _FakeConnection)
    return captured


def test_admin_main_get_db_uses_aurora_iam_settings(monkeypatch):
    monkeypatch.setenv("TAPDB_ENV", "dev")
    monkeypatch.setattr(
        main_mod,
        "get_db_config_for_env",
        lambda _env: {
            "engine_type": "aurora",
            "host": "dev.cluster-abc.us-west-2.rds.amazonaws.com",
            "port": "5432",
            "user": "tapdb_admin",
            "password": "",
            "database": "tapdb_dev",
            "region": "us-west-2",
            "iam_auth": "true",
        },
    )
    captured = _capture_connection_kwargs(monkeypatch, main_mod)

    _ = main_mod.get_db()

    assert captured["db_hostname"] == "dev.cluster-abc.us-west-2.rds.amazonaws.com:5432"
    assert captured["db_user"] == "tapdb_admin"
    assert captured["db_pass"] is None
    assert captured["db_name"] == "tapdb_dev"
    assert captured["engine_type"] == "aurora"
    assert captured["region"] == "us-west-2"
    assert captured["iam_auth"] is True


def test_admin_auth_get_db_uses_aurora_password_when_iam_disabled(monkeypatch):
    monkeypatch.setenv("TAPDB_ENV", "dev")
    monkeypatch.setattr(
        auth_mod,
        "get_db_config_for_env",
        lambda _env: {
            "engine_type": "aurora",
            "host": "dev.cluster-abc.us-west-2.rds.amazonaws.com",
            "port": "5432",
            "user": "tapdb_admin",
            "password": "pw123",
            "database": "tapdb_dev",
            "region": "us-west-2",
            "iam_auth": "false",
        },
    )
    captured = _capture_connection_kwargs(monkeypatch, auth_mod)

    _ = auth_mod.get_db()

    assert captured["engine_type"] == "aurora"
    assert captured["iam_auth"] is False
    assert captured["db_pass"] == "pw123"

