"""Pytest configuration for daylily-tapdb tests."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from pathlib import Path
from urllib.parse import quote

import pytest
import yaml

# ---------------------------------------------------------------------------
# Global domain/owner defaults — every test session uses Z / daylily-tapdb
# ---------------------------------------------------------------------------
os.environ.setdefault("MERIDIAN_DOMAIN_CODE", "Z")
os.environ.setdefault("TAPDB_OWNER_REPO", "daylily-tapdb")

# ---------------------------------------------------------------------------
# Port used by the ephemeral PostgreSQL test instance
# ---------------------------------------------------------------------------
TAPDB_TEST_PG_PORT = 15438


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
        pytest.skip(
            "Set --tapdb-config and --tapdb-env to run Postgres integration tests"
        )

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


# ---------------------------------------------------------------------------
# Ephemeral PostgreSQL fixture (session-scoped)
# ---------------------------------------------------------------------------


def _wait_for_pg(port: int, timeout: float = 15.0) -> bool:
    """Block until pg_isready succeeds or *timeout* seconds elapse."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = subprocess.run(
                ["pg_isready", "-h", "localhost", "-p", str(port)],
                capture_output=True,
                timeout=3,
            )
            if r.returncode == 0:
                return True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            pass
        time.sleep(0.3)
    return False


@pytest.fixture(scope="session")
def pg_instance(tmp_path_factory):
    """Spin up an ephemeral PostgreSQL cluster on port TAPDB_TEST_PG_PORT.

    Yields a dict:
        port       – int
        data_dir   – Path
        config_path – Path  (valid tapdb config YAML)
        user       – str
        database   – str
        dsn        – str  (SQLAlchemy URL)

    Cleanup: pg_ctl stop + rm data dir.
    """
    pg_ctl = shutil.which("pg_ctl")
    initdb_bin = shutil.which("initdb")
    createdb_bin = shutil.which("createdb")
    if not all([pg_ctl, initdb_bin, createdb_bin]):
        pytest.skip("PostgreSQL binaries (pg_ctl, initdb, createdb) not on PATH")

    port = TAPDB_TEST_PG_PORT
    base = tmp_path_factory.mktemp("tapdb_pg")
    data_dir = base / "data"
    # macOS limits Unix socket paths to 104 chars — use /tmp for short path
    socket_dir = Path(f"/tmp/tapdb_test_pg_{os.getpid()}")
    socket_dir.mkdir(exist_ok=True)
    log_file = base / "postgresql.log"

    # --- initdb ---
    subprocess.run(
        [initdb_bin, "-D", str(data_dir), "--no-locale", "-E", "UTF8", "-A", "trust"],
        check=True,
        capture_output=True,
    )

    # --- start ---
    options = (
        f"-p {port} "
        f"-k {socket_dir} "
        f"-c listen_addresses=localhost "
        f"-c unix_socket_directories='{socket_dir}' "
        f"-c logging_collector=off"
    )
    subprocess.run(
        [pg_ctl, "start", "-D", str(data_dir), "-l", str(log_file), "-o", options],
        check=True,
        capture_output=True,
    )

    if not _wait_for_pg(port):
        # dump the log for debugging
        print(log_file.read_text())
        raise RuntimeError(f"PostgreSQL did not start on port {port}")

    user = os.environ.get("USER", "postgres")
    database = "tapdb_test_integ"

    # --- create database ---
    subprocess.run(
        [createdb_bin, "-h", "localhost", "-p", str(port), "-U", user, database],
        check=True,
        capture_output=True,
    )

    # --- write tapdb config ---
    cfg_dir = base / ".config" / "tapdb" / "testclient" / "testdb"
    cfg_dir.mkdir(parents=True)
    cfg_path = cfg_dir / "tapdb-config.yaml"
    domain_registry_path = base / "domain_code_registry.json"
    prefix_registry_path = base / "prefix_ownership_registry.json"
    domain_registry_path.write_text(
        json.dumps(
            {
                "version": "0.4.0",
                "domains": {"Z": {"name": "test-localhost"}},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    prefix_registry_path.write_text(
        json.dumps(
            {
                "version": "0.4.0",
                "ownership": {
                    "Z": {
                        "TPX": {"issuer_app_code": "daylily-tapdb"},
                        "EDG": {"issuer_app_code": "daylily-tapdb"},
                        "ADT": {"issuer_app_code": "daylily-tapdb"},
                        "SYS": {"issuer_app_code": "daylily-tapdb"},
                        "MSG": {"issuer_app_code": "daylily-tapdb"},
                    }
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    cfg_data = {
        "meta": {
            "config_version": 3,
            "client_id": "testclient",
            "database_name": "testdb",
            "owner_repo_name": "daylily-tapdb",
            "domain_registry_path": str(domain_registry_path),
            "prefix_ownership_registry_path": str(prefix_registry_path),
        },
        "environments": {
            "dev": {
                "engine_type": "local",
                "host": "localhost",
                "port": port,
                "ui_port": 18911,
                "domain_code": "Z",
                "user": user,
                "password": "",
                "database": database,
            },
        },
    }
    cfg_path.write_text(yaml.safe_dump(cfg_data), encoding="utf-8")
    os.chmod(cfg_path, 0o600)

    # Construct a postgres_dir layout that matches what pg.py expects
    pg_runtime = cfg_dir / "dev" / "postgres"
    pg_runtime.mkdir(parents=True, exist_ok=True)
    # Symlink data and run dirs so CLI pg commands find them
    (pg_runtime / "data").symlink_to(data_dir)
    run_link = pg_runtime / "run"
    run_link.symlink_to(socket_dir)

    dsn = f"postgresql://{user}:@localhost:{port}/{database}"

    yield {
        "port": port,
        "data_dir": data_dir,
        "socket_dir": socket_dir,
        "config_path": cfg_path,
        "config_dir": cfg_dir,
        "user": user,
        "database": database,
        "dsn": dsn,
        "base": base,
    }

    # --- teardown ---
    subprocess.run(
        [pg_ctl, "stop", "-D", str(data_dir), "-m", "immediate"],
        capture_output=True,
    )
    # Clean up short-path socket dir
    shutil.rmtree(socket_dir, ignore_errors=True)
