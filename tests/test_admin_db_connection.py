"""Tests for admin DB pooling helpers."""

from __future__ import annotations

import admin.auth as auth_mod
import admin.db_pool as pool_mod
import admin.main as main_mod
from sqlalchemy import create_engine as sa_create_engine


def test_admin_get_db_reuses_single_engine_bundle(monkeypatch):
    pool_mod._clear_engine_cache_for_tests()
    monkeypatch.setenv("TAPDB_ENV", "dev")
    monkeypatch.setattr(
        pool_mod,
        "get_db_config_for_env",
        lambda _env: {
            "engine_type": "local",
            "host": "localhost",
            "port": "5533",
            "user": "tapdb_admin",
            "password": "",
            "database": "tapdb_dev",
        },
    )

    calls = {"count": 0}

    def _fake_create_engine(_url, *, echo_sql):
        _ = echo_sql
        calls["count"] += 1
        return sa_create_engine("sqlite:///:memory:")

    monkeypatch.setattr(pool_mod, "_create_engine", _fake_create_engine)

    _ = main_mod.get_db()
    _ = auth_mod.get_db()
    _ = main_mod.get_db()

    assert calls["count"] == 1


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
        iam_auth=True,
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
        iam_auth=False,
        password="pw123",
    )
    cparams = {}
    listener(None, None, None, cparams)
    assert cparams["password"] == "pw123"

