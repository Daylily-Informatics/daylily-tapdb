"""Tests for admin DB pooling helpers."""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine as sa_create_engine

import admin.auth as auth_mod
import admin.db_metrics as metrics_mod
import admin.db_pool as pool_mod
import admin.main as main_mod


def test_admin_get_db_reuses_single_engine_bundle(monkeypatch):
    pool_mod._clear_engine_cache_for_tests()
    monkeypatch.setattr(
        pool_mod,
        "_admin_settings",
        lambda _env: {
            "db_pool_size": 5,
            "db_max_overflow": 10,
            "db_pool_timeout": 30,
            "db_pool_recycle": 1800,
        },
    )
    monkeypatch.setattr(
        metrics_mod, "_admin_settings", lambda: {"metrics_enabled": False}
    )
    monkeypatch.setattr(
        pool_mod,
        "get_db_config",
        lambda: {
            "engine_type": "local",
            "host": "localhost",
            "port": "5533",
            "user": "tapdb_admin",
            "password": "",
            "database": "tapdb_dev",
            "schema_name": "tapdb_dev",
        },
    )

    calls = {"count": 0}

    def _fake_create_engine(_url, *, echo_sql, env_name):
        _ = echo_sql, env_name
        calls["count"] += 1
        return sa_create_engine("sqlite:///:memory:")

    monkeypatch.setattr(pool_mod, "_create_engine", _fake_create_engine)

    _ = main_mod.get_db()
    _ = auth_mod.get_db()
    _ = main_mod.get_db()

    assert calls["count"] == 1


def test_admin_get_engine_bundle_requires_schema_name(monkeypatch):
    pool_mod._clear_engine_cache_for_tests()
    monkeypatch.setattr(
        pool_mod,
        "get_db_config",
        lambda: {
            "engine_type": "local",
            "host": "localhost",
            "port": "5533",
            "user": "tapdb_admin",
            "password": "",
            "database": "tapdb_dev",
        },
    )

    with pytest.raises(RuntimeError, match="schema_name"):
        pool_mod.get_engine_bundle()


def test_admin_session_scope_sets_search_path(monkeypatch):
    pool_mod._clear_engine_cache_for_tests()

    class _Trans:
        def commit(self):
            return None

        def rollback(self):
            return None

    class _Session:
        def __init__(self):
            self.bind = type(
                "Bind", (), {"dialect": type("Dialect", (), {"name": "postgresql"})()}
            )()
            self.statements = []

        def begin(self):
            return _Trans()

        def execute(self, stmt, params=None):
            self.statements.append((str(stmt), params or {}))

        def close(self):
            return None

    session = _Session()
    bundle = pool_mod.EngineBundle(
        env_name="dev",
        engine=sa_create_engine("sqlite:///:memory:"),
        SessionFactory=lambda: session,
        cfg={"schema_name": "tapdb_dev"},
        schema_name="tapdb_dev",
    )
    conn = pool_mod.AdminDBConnection(bundle)
    monkeypatch.setattr(
        "admin.db_metrics.db_username_var",
        type(
            "Var",
            (),
            {"set": lambda self, value: value, "reset": lambda self, token: None},
        )(),
    )

    with conn.session_scope(commit=False):
        pass

    assert "set_config('search_path'" in session.statements[0][0]
    assert session.statements[0][1]["schema_name"] == "tapdb_dev"


def test_attach_aurora_password_provider_refreshes_iam_token(monkeypatch):
    engine = sa_create_engine("sqlite:///:memory:")
    monkeypatch.setattr(
        pool_mod.AuroraConnectionBuilder,
        "get_iam_auth_token",
        lambda **_k: "tok123",
    )

    listener = pool_mod._attach_aurora_password_provider(
        engine,
        region="us-west-2",
        host="dev.cluster-abc.us-west-2.rds.amazonaws.com",
        port=5432,
        user="tapdb_admin",
        aws_profile=None,
        iam_auth=True,
        secret_arn=None,
        password="",
    )
    cparams = {}
    listener(None, None, None, cparams)
    assert cparams["password"] == "tok123"


def test_attach_aurora_password_provider_uses_static_password_when_iam_disabled():
    engine = sa_create_engine("sqlite:///:memory:")
    listener = pool_mod._attach_aurora_password_provider(
        engine,
        region="us-west-2",
        host="dev.cluster-abc.us-west-2.rds.amazonaws.com",
        port=5432,
        user="tapdb_admin",
        aws_profile=None,
        iam_auth=False,
        secret_arn=None,
        password="pw123",
    )
    cparams = {}
    listener(None, None, None, cparams)
    assert cparams["password"] == "pw123"


def test_attach_aurora_password_provider_uses_secret_arn(monkeypatch):
    engine = sa_create_engine("sqlite:///:memory:")
    monkeypatch.setattr(
        pool_mod.AuroraConnectionBuilder,
        "get_secret_password",
        lambda **_k: "secret-from-arn",
    )
    listener = pool_mod._attach_aurora_password_provider(
        engine,
        region="us-west-2",
        host="dev.cluster-abc.us-west-2.rds.amazonaws.com",
        port=5432,
        user="tapdb_admin",
        aws_profile=None,
        iam_auth=False,
        secret_arn="arn:aws:secretsmanager:us-west-2:123:secret:db",
        password="",
    )
    cparams = {}
    listener(None, None, None, cparams)
    assert cparams["password"] == "secret-from-arn"
