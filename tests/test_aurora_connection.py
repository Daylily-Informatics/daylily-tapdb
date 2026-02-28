"""Tests for AuroraConnectionBuilder — all boto3 calls are mocked."""

import json
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_boto3_client(service, region_name=None):
    """Return a mock client that handles rds and secretsmanager."""
    client = MagicMock()
    if service == "rds":
        client.generate_db_auth_token.return_value = "iam-token-abc123"
    elif service == "secretsmanager":
        client.get_secret_value.return_value = {
            "SecretString": json.dumps({"password": "s3cret-from-sm"}),
        }
    return client


@pytest.fixture(autouse=True)
def _mock_boto3(monkeypatch):
    """Patch boto3.client globally for all tests in this module."""
    fake_boto3 = types.ModuleType("boto3")
    fake_boto3.client = _fake_boto3_client
    monkeypatch.setitem(__import__("sys").modules, "boto3", fake_boto3)


@pytest.fixture()
def _ca_bundle(tmp_path, monkeypatch):
    """Point the CA bundle path to a temp file so no download occurs."""
    from daylily_tapdb.aurora import connection as mod

    bundle = tmp_path / "rds-ca-bundle.pem"
    bundle.write_text("--- FAKE CA ---")
    monkeypatch.setattr(mod, "_CA_BUNDLE_PATH", bundle)
    monkeypatch.setattr(mod, "_CA_BUNDLE_DIR", tmp_path)
    return bundle


# ---------------------------------------------------------------------------
# get_iam_auth_token
# ---------------------------------------------------------------------------

def test_get_iam_auth_token():
    from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

    token = AuroraConnectionBuilder.get_iam_auth_token(
        region="us-east-1", host="mydb.cluster-xyz.us-east-1.rds.amazonaws.com",
        port=5432, user="tapdb_admin",
    )
    assert token == "iam-token-abc123"


# ---------------------------------------------------------------------------
# get_secret_password
# ---------------------------------------------------------------------------

def test_get_secret_password_with_explicit_region():
    from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

    pw = AuroraConnectionBuilder.get_secret_password(
        "arn:aws:secretsmanager:us-east-1:123456:secret:mydb", region="us-east-1",
    )
    assert pw == "s3cret-from-sm"


def test_get_secret_password_infers_region_from_arn():
    from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

    pw = AuroraConnectionBuilder.get_secret_password(
        "arn:aws:secretsmanager:eu-west-1:123456:secret:mydb",
    )
    assert pw == "s3cret-from-sm"


# ---------------------------------------------------------------------------
# ensure_ca_bundle
# ---------------------------------------------------------------------------

def test_ensure_ca_bundle_returns_existing(tmp_path, monkeypatch):
    from daylily_tapdb.aurora import connection as mod

    bundle = tmp_path / "rds-ca-bundle.pem"
    bundle.write_text("EXISTING")
    monkeypatch.setattr(mod, "_CA_BUNDLE_PATH", bundle)
    monkeypatch.setattr(mod, "_CA_BUNDLE_DIR", tmp_path)

    result = mod.AuroraConnectionBuilder.ensure_ca_bundle()
    assert result == bundle


def test_ensure_ca_bundle_downloads_when_missing(tmp_path, monkeypatch):
    from daylily_tapdb.aurora import connection as mod

    bundle = tmp_path / "subdir" / "rds-ca-bundle.pem"
    monkeypatch.setattr(mod, "_CA_BUNDLE_PATH", bundle)
    monkeypatch.setattr(mod, "_CA_BUNDLE_DIR", tmp_path / "subdir")

    # Mock urllib.request.urlretrieve
    def fake_urlretrieve(url, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text("DOWNLOADED")

    with patch("urllib.request.urlretrieve", fake_urlretrieve):
        result = mod.AuroraConnectionBuilder.ensure_ca_bundle()

    assert result == bundle
    assert bundle.read_text() == "DOWNLOADED"


# ---------------------------------------------------------------------------
# build_connection_url
# ---------------------------------------------------------------------------

def test_build_connection_url_iam(_ca_bundle):
    from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

    url = AuroraConnectionBuilder.build_connection_url(
        host="mydb.cluster-xyz.us-east-1.rds.amazonaws.com",
        port=5432, database="tapdb_dev", user="tapdb_admin",
        region="us-east-1", iam_auth=True,
    )
    assert url.startswith("postgresql+psycopg2://")
    assert "tapdb_dev" in url
    assert "sslmode=verify-full" in url
    assert "sslrootcert=" in url


def test_build_connection_url_secret_arn(_ca_bundle):
    from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

    url = AuroraConnectionBuilder.build_connection_url(
        host="mydb.cluster-xyz.us-east-1.rds.amazonaws.com",
        port=5432, database="tapdb_dev", user="tapdb_admin",
        region="us-east-1", iam_auth=False,
        secret_arn="arn:aws:secretsmanager:us-east-1:123456:secret:mydb",
    )
    assert "s3cret-from-sm" in url or "s3cret" in url  # URL-encoded
    assert "sslmode=verify-full" in url


def test_build_connection_url_explicit_password(_ca_bundle):
    from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

    url = AuroraConnectionBuilder.build_connection_url(
        host="mydb.cluster-xyz.us-east-1.rds.amazonaws.com",
        port=5432, database="tapdb_dev", user="tapdb_admin",
        region="us-east-1", iam_auth=False, password="plain-pw",
    )
    assert "plain-pw" in url
    assert "sslmode=verify-full" in url


# ---------------------------------------------------------------------------
# TAPDBConnection with engine_type="aurora"
# ---------------------------------------------------------------------------

def test_tapdb_connection_aurora_delegates_to_builder(_ca_bundle, monkeypatch):
    """TAPDBConnection(engine_type='aurora') uses AuroraConnectionBuilder."""
    from daylily_tapdb import connection as m

    class FakeEngine:
        def dispose(self):
            return None

    monkeypatch.setattr(m, "create_engine", lambda url, **kw: FakeEngine())
    monkeypatch.setattr(m, "sessionmaker", lambda bind: (lambda: None))

    conn = m.TAPDBConnection(
        engine_type="aurora",
        db_hostname="mydb.cluster-xyz.us-east-1.rds.amazonaws.com:5432",
        db_user="tapdb_admin",
        db_name="tapdb_dev",
        region="us-east-1",
        iam_auth=True,
    )
    assert "sslmode=verify-full" in conn._db_url
    assert "tapdb_dev" in conn._db_url


def test_tapdb_connection_aurora_requires_hostname(monkeypatch):
    """engine_type='aurora' without db_hostname raises ValueError."""
    from daylily_tapdb import connection as m

    monkeypatch.setattr(m, "create_engine", lambda *a, **k: None)
    monkeypatch.setattr(m, "sessionmaker", lambda bind: (lambda: None))

    with pytest.raises(ValueError, match="db_hostname.*required"):
        m.TAPDBConnection(engine_type="aurora", db_name="tapdb_dev")


def test_tapdb_connection_local_unchanged(monkeypatch):
    """Default (no engine_type) still builds a local URL."""
    from daylily_tapdb import connection as m

    monkeypatch.setenv("PGPORT", "5432")
    monkeypatch.setenv("PGPASSWORD", "pw")
    monkeypatch.setenv("USER", "alice")

    called = {}

    class FakeEngine:
        def dispose(self):
            return None

    def fake_create_engine(url, **kwargs):
        called["url"] = url
        return FakeEngine()

    monkeypatch.setattr(m, "create_engine", fake_create_engine)
    monkeypatch.setattr(m, "sessionmaker", lambda bind: (lambda: None))

    m.TAPDBConnection(db_name="tapdb")
    assert called["url"] == "postgresql://alice:pw@localhost:5432/tapdb"


# ---------------------------------------------------------------------------
# _ensure_boto3 error message
# ---------------------------------------------------------------------------

def test_ensure_boto3_missing_gives_clear_error(monkeypatch):
    """When boto3 is not installed, a clear ImportError is raised."""
    import sys

    monkeypatch.delitem(sys.modules, "boto3", raising=False)

    # Temporarily make boto3 unimportable
    import builtins
    real_import = builtins.__import__

    def no_boto3(name, *args, **kwargs):
        if name == "boto3":
            raise ImportError("No module named 'boto3'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", no_boto3)

    from daylily_tapdb.aurora.connection import _ensure_boto3

    with pytest.raises(ImportError, match="pip install daylily-tapdb"):
        _ensure_boto3()


def test_build_connection_url_no_auth_raises(_ca_bundle):
    from daylily_tapdb.aurora.connection import AuroraConnectionBuilder

    with pytest.raises(ValueError, match="requires iam_auth"):
        AuroraConnectionBuilder.build_connection_url(
            host="mydb.cluster-xyz.us-east-1.rds.amazonaws.com",
            port=5432, database="tapdb_dev", user="tapdb_admin",
            region="us-east-1", iam_auth=False,
        )

