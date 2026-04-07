from __future__ import annotations

import json
import runpy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer

import daylily_tapdb.cli as cli_mod
import daylily_tapdb.cli.pg as pg_mod
import daylily_tapdb.cli.user as user_mod
import daylily_tapdb.web.runtime as runtime_mod
from daylily_tapdb.cli.db import Environment


class _FakeConn:
    def __init__(self) -> None:
        self.sessions: list[SimpleNamespace] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def session_scope(self, commit: bool = False):
        session = SimpleNamespace(commit=commit)
        self.sessions.append(session)

        class _Scope:
            def __enter__(self):
                return session

            def __exit__(self, exc_type, exc, tb):
                return False

        return _Scope()


class _FakeTransaction:
    def __init__(self) -> None:
        self.commits = 0
        self.rollbacks = 0

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1


class _FakeSession:
    def __init__(self, *, fail_execute: Exception | None = None) -> None:
        self.fail_execute = fail_execute
        self.closed = 0
        self.execute_calls: list[tuple[object, object]] = []
        self.transactions: list[_FakeTransaction] = []

    def begin(self) -> _FakeTransaction:
        tx = _FakeTransaction()
        self.transactions.append(tx)
        return tx

    def execute(self, stmt, params=None):
        self.execute_calls.append((stmt, params))
        if self.fail_execute is not None:
            raise self.fail_execute
        return None

    def close(self) -> None:
        self.closed += 1


@dataclass
class _FakeBundle:
    config_path: str
    env_name: str
    engine: object
    SessionFactory: object
    cfg: dict[str, str]


def test_cli_module_main_raises_system_exit(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_mod, "main", lambda: 7)

    with pytest.raises(SystemExit) as exc:
        runpy.run_module("daylily_tapdb.cli.__main__", run_name="__main__")

    assert exc.value.code == 7


def test_user_format_date_blank_and_raw_string() -> None:
    assert user_mod._format_date("") == "-"
    assert user_mod._format_date("not-a-date") == "not-a-date"
    assert user_mod._format_date(datetime(2026, 4, 7, 9, 30), include_time=True) == (
        "2026-04-07 09:30"
    )


def test_user_list_error_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        user_mod, "_open_connection", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    with pytest.raises(typer.Exit) as exc:
        user_mod.user_list(Environment.dev, False)

    assert exc.value.exit_code == 1


def test_user_add_invalid_role_and_hash_error(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(typer.Exit) as exc:
        user_mod.user_add(
            Environment.dev,
            username="alice@example.com",
            role="owner",
            email=None,
            display_name=None,
            password=None,
        )
    assert exc.value.exit_code == 1

    monkeypatch.setattr(user_mod, "_hash_password", lambda _value: (_ for _ in ()).throw(RuntimeError("bcrypt missing")))
    with pytest.raises(typer.Exit) as exc:
        user_mod.user_add(
            Environment.dev,
            username="alice@example.com",
            role="admin",
            email=None,
            display_name=None,
            password="secret",
        )
    assert exc.value.exit_code == 1


def test_user_add_connection_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        user_mod, "_open_connection", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("db down"))
    )

    with pytest.raises(typer.Exit) as exc:
        user_mod.user_add(
            Environment.dev,
            username="alice@example.com",
            role="admin",
            email="alice@example.com",
            display_name="Alice",
            password=None,
        )

    assert exc.value.exit_code == 1


def test_user_set_role_error_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    with pytest.raises(typer.Exit) as exc:
        user_mod.user_set_role(Environment.dev, "alice", "owner")
    assert exc.value.exit_code == 1

    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(user_mod, "set_role", lambda *_a, **_k: False)
    with pytest.raises(typer.Exit) as exc:
        user_mod.user_set_role(Environment.dev, "alice", "admin")
    assert exc.value.exit_code == 1

    monkeypatch.setattr(
        user_mod, "_open_connection", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("db down"))
    )
    with pytest.raises(typer.Exit) as exc:
        user_mod.user_set_role(Environment.dev, "alice", "admin")
    assert exc.value.exit_code == 1


@pytest.mark.parametrize(
    ("command", "is_active"),
    [("user_deactivate", False), ("user_activate", True)],
)
def test_user_activate_deactivate_error_paths(
    monkeypatch: pytest.MonkeyPatch, command: str, is_active: bool
) -> None:
    fn = getattr(user_mod, command)
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(user_mod, "set_active", lambda *_a, **_k: False)
    with pytest.raises(typer.Exit) as exc:
        fn(Environment.dev, "alice")
    assert exc.value.exit_code == 1

    monkeypatch.setattr(
        user_mod, "_open_connection", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("db down"))
    )
    with pytest.raises(typer.Exit) as exc:
        fn(Environment.dev, "alice")
    assert exc.value.exit_code == 1


def test_user_set_password_error_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_mod, "_hash_password", lambda _value: (_ for _ in ()).throw(RuntimeError("bcrypt missing")))
    with pytest.raises(typer.Exit) as exc:
        user_mod.user_set_password(Environment.dev, "alice", "secret")
    assert exc.value.exit_code == 1

    monkeypatch.setattr(user_mod, "_hash_password", lambda value: f"hashed:{value}")
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(user_mod, "set_password_hash", lambda *_a, **_k: False)
    with pytest.raises(typer.Exit) as exc:
        user_mod.user_set_password(Environment.dev, "alice", "secret")
    assert exc.value.exit_code == 1

    monkeypatch.setattr(
        user_mod, "_open_connection", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("db down"))
    )
    with pytest.raises(typer.Exit) as exc:
        user_mod.user_set_password(Environment.dev, "alice", "secret")
    assert exc.value.exit_code == 1


def test_user_delete_error_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(user_mod.typer, "confirm", lambda _msg: True)
    monkeypatch.setattr(user_mod, "_open_connection", lambda *_a, **_k: _FakeConn())
    monkeypatch.setattr(user_mod, "soft_delete", lambda *_a, **_k: False)

    with pytest.raises(typer.Exit) as exc:
        user_mod.user_delete(Environment.dev, "alice", True)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(
        user_mod, "_open_connection", lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("db down"))
    )
    with pytest.raises(typer.Exit) as exc:
        user_mod.user_delete(Environment.dev, "alice", True)
    assert exc.value.exit_code == 1


def test_runtime_parse_bool_and_audit_username() -> None:
    assert runtime_mod._parse_bool(None, default=True) is True
    assert runtime_mod._parse_bool("true", default=False) is True
    assert runtime_mod._parse_bool("off", default=True) is False
    assert runtime_mod._parse_bool("maybe", default=False) is False
    assert runtime_mod._audit_username_for_session(None) == "unknown"
    assert runtime_mod._audit_username_for_session(" alice ") == "alice"


def test_runtime_set_audit_username_logs_warning(caplog) -> None:
    session = _FakeSession(fail_execute=RuntimeError("no tx"))

    with caplog.at_level("WARNING"):
        runtime_mod._set_audit_username(session, "alice")

    assert "Could not set session audit username" in caplog.text


def test_runtime_db_connection_context_manager_commit_and_rollback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessions: list[_FakeSession] = []

    def _factory() -> _FakeSession:
        session = _FakeSession()
        sessions.append(session)
        return session

    bundle = runtime_mod.RuntimeBundle(
        config_path="/tmp/tapdb-config.yaml",
        env_name="dev",
        engine=object(),
        SessionFactory=_factory,
        cfg={},
    )
    conn = runtime_mod.RuntimeDBConnection(bundle)
    conn.app_username = "alice"

    with conn as entered:
        assert entered is conn

    with conn.session_scope(commit=True):
        pass
    assert sessions[-1].transactions[0].commits == 1
    assert sessions[-1].transactions[0].rollbacks == 0
    assert sessions[-1].closed == 1

    with conn.session_scope(commit=False):
        pass
    assert sessions[-1].transactions[0].commits == 0
    assert sessions[-1].transactions[0].rollbacks == 1
    assert sessions[-1].closed == 1

    with pytest.raises(RuntimeError, match="boom"):
        with conn.session_scope(commit=True):
            raise RuntimeError("boom")
    assert sessions[-1].transactions[0].rollbacks == 1
    assert sessions[-1].closed == 1


def test_runtime_build_engine_for_cfg_local_and_aurora(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_calls: list[dict[str, object]] = []
    attach_calls: list[dict[str, object]] = []

    def _fake_create_engine(url, *, config_path, env_name, echo_sql):
        create_calls.append(
            {
                "drivername": url.drivername,
                "username": url.username,
                "password": url.password,
                "host": url.host,
                "port": url.port,
                "database": url.database,
                "query": dict(url.query),
                "config_path": config_path,
                "env_name": env_name,
                "echo_sql": echo_sql,
            }
        )
        return f"engine-{len(create_calls)}"

    monkeypatch.setattr(runtime_mod, "_create_engine", _fake_create_engine)
    monkeypatch.setattr(
        runtime_mod.AuroraConnectionBuilder,
        "ensure_ca_bundle",
        staticmethod(lambda: Path("/tmp/aurora-ca.pem")),
    )
    monkeypatch.setattr(
        runtime_mod,
        "_attach_aurora_password_provider",
        lambda engine, **kwargs: attach_calls.append({"engine": engine, **kwargs}),
    )
    monkeypatch.setenv("ECHO_SQL", "true")
    monkeypatch.setenv("AWS_PROFILE", "ambient")

    local_engine = runtime_mod._build_engine_for_cfg(
        {
            "engine_type": "local",
            "host": "localhost",
            "port": "5432",
            "database": "tapdb_dev",
            "user": "tapdb",
            "password": "",
        },
        config_path="/tmp/local.yaml",
        env_name="dev",
    )
    aurora_engine = runtime_mod._build_engine_for_cfg(
        {
            "engine_type": "aurora",
            "host": "aurora.local",
            "port": "6432",
            "database": "tapdb_prod",
            "user": "tapdb",
            "password": "",
            "iam_auth": "yes",
            "region": "us-west-2",
        },
        config_path="/tmp/aurora.yaml",
        env_name="prod",
    )

    assert local_engine == "engine-1"
    assert aurora_engine == "engine-2"
    assert create_calls[0]["password"] is None
    assert create_calls[0]["query"] == {}
    assert create_calls[1]["query"]["sslmode"] == "verify-full"
    assert create_calls[1]["query"]["sslrootcert"] == "/tmp/aurora-ca.pem"
    assert create_calls[1]["echo_sql"] is True
    assert attach_calls == [
        {
            "engine": "engine-2",
            "region": "us-west-2",
            "host": "aurora.local",
            "port": 6432,
            "user": "tapdb",
            "aws_profile": "ambient",
            "iam_auth": True,
            "password": "",
        }
    ]


def test_runtime_get_db_requires_env_name() -> None:
    with pytest.raises(RuntimeError, match="env name is required"):
        runtime_mod.get_db("/tmp/tapdb-config.yaml", "")


def test_runtime_clear_cache_logs_dispose_errors(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    class _Engine:
        def dispose(self) -> None:
            raise RuntimeError("dispose boom")

    bundle = runtime_mod.RuntimeBundle(
        config_path="/tmp/tapdb-config.yaml",
        env_name="dev",
        engine=_Engine(),
        SessionFactory=lambda: None,
        cfg={},
    )
    runtime_mod._bundles[("/tmp/tapdb-config.yaml", "dev")] = bundle

    with caplog.at_level("WARNING"):
        runtime_mod._clear_runtime_cache_for_tests()

    assert "Error disposing DAG runtime engine" in caplog.text
    assert runtime_mod._bundles == {}


def test_pg_socket_dir_prefers_configured_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        pg_mod,
        "get_db_config_for_env",
        lambda _env: {"unix_socket_dir": "~/tapdb-pg-socket"},
    )

    resolved = pg_mod._get_postgres_socket_dir(Environment.dev)

    assert resolved == Path("~/tapdb-pg-socket").expanduser()


def test_pg_active_env_invalid_defaults_to_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pg_mod, "active_env_name", lambda _default="dev": "invalid")
    assert pg_mod._active_env() == Environment.dev


def test_pg_get_pg_service_cmd_linux_and_unknown(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pg_mod.platform, "system", lambda: "Linux")

    def _linux_systemd(path_self) -> bool:
        return str(path_self) in {"/bin/systemctl", "/usr/bin/systemctl"}

    monkeypatch.setattr(pg_mod.Path, "exists", _linux_systemd, raising=False)
    method, start_cmd, stop_cmd, _ = pg_mod._get_pg_service_cmd()
    assert method == "systemd"
    assert start_cmd[-1] == "postgresql"
    assert stop_cmd[-1] == "postgresql"

    monkeypatch.setattr(pg_mod.Path, "exists", lambda _self: False, raising=False)
    method, start_cmd, stop_cmd, log_path = pg_mod._get_pg_service_cmd()
    assert (method, start_cmd, stop_cmd, log_path) == ("unknown", [], [], Path())

    monkeypatch.setattr(pg_mod.platform, "system", lambda: "Darwin")
    method, start_cmd, stop_cmd, log_path = pg_mod._get_pg_service_cmd()
    assert (method, start_cmd, stop_cmd, log_path) == ("unknown", [], [], Path())


def test_pg_is_pg_running_exception_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pg_mod.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    assert pg_mod._is_pg_running() == (False, "boom")


def test_pg_start_unknown_service_and_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(pg_mod, "_is_pg_running", lambda: (False, ""))
    monkeypatch.setattr(pg_mod, "_get_pg_service_cmd", lambda: ("unknown", [], [], Path()))

    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_start()
    assert exc.value.exit_code == 1

    calls = {"polls": 0}

    def _fake_is_running():
        if calls["polls"] == 0:
            calls["polls"] += 1
            return False, ""
        return True, "PostgreSQL 16"

    monkeypatch.setattr(pg_mod, "_is_pg_running", _fake_is_running)
    monkeypatch.setattr(
        pg_mod,
        "_get_pg_service_cmd",
        lambda: ("systemd", ["sudo", "systemctl", "start", "postgresql"], [], Path()),
    )
    monkeypatch.setattr(
        pg_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr="", stdout=""),
    )
    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)
    pg_mod.pg_start()

    monkeypatch.setattr(pg_mod, "_is_pg_running", lambda: (False, ""))
    monkeypatch.setattr(
        pg_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stderr="cannot start", stdout=""),
    )
    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_start()
    assert exc.value.exit_code == 1


def test_pg_stop_status_logs_and_restart(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(pg_mod, "_is_pg_running", lambda: (False, ""))
    pg_mod.pg_stop()

    monkeypatch.setattr(pg_mod, "_is_pg_running", lambda: (True, "16"))
    monkeypatch.setattr(pg_mod, "_get_pg_service_cmd", lambda: ("unknown", [], [], Path()))
    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_stop()
    assert exc.value.exit_code == 1

    state = {"checks": 0}

    def _stop_running():
        state["checks"] += 1
        if state["checks"] < 3:
            return True, "16"
        return False, ""

    monkeypatch.setattr(pg_mod, "_is_pg_running", _stop_running)
    monkeypatch.setattr(
        pg_mod,
        "_get_pg_service_cmd",
        lambda: ("systemd", [], ["sudo", "systemctl", "stop", "postgresql"], Path()),
    )
    monkeypatch.setattr(
        pg_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr="", stdout=""),
    )
    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)
    pg_mod.pg_stop()

    monkeypatch.setattr(pg_mod, "_active_env", lambda: Environment.prod)
    monkeypatch.setattr(pg_mod, "_is_pg_running", lambda: (False, "boom"))
    pg_mod.pg_status()

    monkeypatch.setattr(pg_mod, "_active_env", lambda: Environment.dev)
    monkeypatch.setattr(
        pg_mod,
        "get_db_config_for_env",
        lambda _env: {"host": "localhost", "port": "5533", "user": "tapdb"},
    )
    monkeypatch.setattr(pg_mod, "_get_postgres_data_dir", lambda _env: Path("/tmp/data"))
    monkeypatch.setattr(pg_mod, "_get_postgres_log_file", lambda _env: Path("/tmp/postgresql.log"))
    monkeypatch.setattr(pg_mod, "_get_postgres_socket_dir", lambda _env: Path("/tmp/socket"))
    monkeypatch.setattr(pg_mod, "_get_instance_lock_file", lambda _env: Path("/tmp/instance.lock"))
    monkeypatch.setattr(
        pg_mod,
        "resolve_context",
        lambda **_kwargs: SimpleNamespace(namespace_slug=lambda: "atlas/app"),
    )
    monkeypatch.setattr(
        pg_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stdout="", stderr=""),
    )
    pg_mod.pg_status()

    log_file = tmp_path / "postgresql.log"
    log_file.write_text("line-1\nline-2\nline-3\n", encoding="utf-8")
    monkeypatch.setattr(pg_mod, "_get_pg_service_cmd", lambda: ("unknown", [], [], Path("/missing.log")))
    monkeypatch.setattr(pg_mod, "_active_env", lambda: Environment.dev)
    monkeypatch.setattr(pg_mod, "_get_postgres_log_file", lambda _env: log_file)
    pg_mod.pg_logs(False, 2)

    monkeypatch.setattr(
        pg_mod.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    pg_mod.pg_logs(True, 5)

    events: list[str] = []
    monkeypatch.setattr(pg_mod, "pg_stop", lambda: events.append("stop"))
    monkeypatch.setattr(pg_mod, "pg_start", lambda: events.append("start"))
    monkeypatch.setattr("time.sleep", lambda *_args, **_kwargs: None)
    pg_mod.pg_restart()
    assert events == ["stop", "start"]


def test_pg_init_start_local_and_stop_local_branches(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    data_dir = tmp_path / "data"
    lock_file = tmp_path / "instance.lock"
    log_file = tmp_path / "postgresql.log"
    socket_dir = tmp_path / "socket"

    monkeypatch.setattr(pg_mod, "_get_postgres_data_dir", lambda _env: data_dir)
    monkeypatch.setattr(pg_mod, "_get_instance_lock_file", lambda _env: lock_file)
    monkeypatch.setattr(pg_mod, "_get_postgres_log_file", lambda _env: log_file)
    monkeypatch.setattr(pg_mod, "_get_postgres_socket_dir", lambda _env: socket_dir)
    monkeypatch.setattr(
        pg_mod,
        "get_db_config_for_env",
        lambda _env: {"user": "postgres", "port": "5533"},
    )

    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_init(Environment.prod, False)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(pg_mod.shutil, "which", lambda _name: None)
    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_init(Environment.dev, False)
    assert exc.value.exit_code == 1

    data_dir.mkdir(parents=True, exist_ok=True)
    (data_dir / "PG_VERSION").write_text("16\n", encoding="utf-8")
    monkeypatch.setattr(pg_mod.shutil, "which", lambda _name: "/usr/bin/initdb")
    pg_mod.pg_init(Environment.dev, False)

    monkeypatch.setattr(
        pg_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr="", stdout=""),
    )
    monkeypatch.setattr(pg_mod.shutil, "rmtree", lambda path: None)
    pg_mod.pg_init(Environment.dev, True)

    monkeypatch.setattr(
        pg_mod,
        "get_db_config_for_env",
        lambda _env: {"port": "0", "user": "postgres"},
    )
    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_start_local(Environment.dev, None)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(
        pg_mod,
        "get_db_config_for_env",
        lambda _env: {"port": "5533", "user": "postgres"},
    )
    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_start_local(Environment.dev, 5534)
    assert exc.value.exit_code == 1

    data_dir.mkdir(parents=True, exist_ok=True)
    if (data_dir / "PG_VERSION").exists():
        (data_dir / "PG_VERSION").unlink()
    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_start_local(Environment.dev, None)
    assert exc.value.exit_code == 1

    (data_dir / "PG_VERSION").write_text("16\n", encoding="utf-8")
    monkeypatch.setattr(pg_mod.shutil, "which", lambda _name: None)
    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_start_local(Environment.dev, None)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(pg_mod.shutil, "which", lambda _name: "/usr/bin/pg_ctl")
    monkeypatch.setattr(pg_mod, "_is_port_available", lambda _port: False)
    monkeypatch.setattr(pg_mod, "_port_conflict_details", lambda _port: "conflict")
    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_start_local(Environment.dev, None)
    assert exc.value.exit_code == 1

    monkeypatch.setattr(pg_mod, "_is_port_available", lambda _port: True)
    monkeypatch.setattr(
        pg_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stderr="", stdout=""),
    )
    pg_mod.pg_start_local(Environment.dev, None)
    payload = json.loads(lock_file.read_text(encoding="utf-8"))
    assert payload["port"] == 5533
    assert payload["socket_dir"] == str(socket_dir)

    monkeypatch.setattr(
        pg_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=1, stderr="could not stop", stdout=""),
    )
    pg_mod.pg_stop_local(Environment.dev)

    monkeypatch.setattr(pg_mod.subprocess, "run", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(typer.Exit) as exc:
        pg_mod.pg_stop_local(Environment.dev)
    assert exc.value.exit_code == 1
