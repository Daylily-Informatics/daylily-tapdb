"""Pytest configuration for daylily-tapdb tests."""

from urllib.parse import quote

import pytest


def pytest_addoption(parser):
    group = parser.getgroup("tapdb")
    group.addoption(
        "--tapdb-config",
        action="store",
        default="",
        help="Explicit TapDB config path for integration tests.",
    )
    group.addoption(
        "--tapdb-env",
        action="store",
        default="",
        help="Explicit TapDB env name for integration tests.",
    )


def resolve_tapdb_test_dsn(pytestconfig) -> str:
    config_path = str(pytestconfig.getoption("--tapdb-config") or "").strip()
    env_name = str(pytestconfig.getoption("--tapdb-env") or "").strip().lower()
    if not config_path or not env_name:
        pytest.skip("Set --tapdb-config and --tapdb-env to run Postgres integration tests")

    from daylily_tapdb.cli.db_config import get_db_config_for_env

    cfg = get_db_config_for_env(env_name, config_path=config_path)
    user = str(cfg.get("user") or "postgres")
    password = str(cfg.get("password") or "")
    host = str(cfg.get("host") or "localhost")
    port = str(cfg.get("port") or "5432")
    database = str(cfg.get("database") or "")
    return (
        "postgresql://"
        f"{quote(user, safe='')}:{quote(password, safe='')}@"
        f"{host}:{port}/{quote(database, safe='')}"
    )


@pytest.fixture
def euid_config():
    """Provide a fresh EUIDConfig for testing."""
    from daylily_tapdb.euid import EUIDConfig

    return EUIDConfig()
