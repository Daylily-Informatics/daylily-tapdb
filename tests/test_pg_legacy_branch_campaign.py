from __future__ import annotations

import builtins
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
import typer

from daylily_tapdb.cli import pg


class _OutputRecorder:
    def __init__(self):
        self.messages: list[tuple[str, str]] = []

    def _record(self, level: str, message: object) -> None:
        self.messages.append((level, str(message)))

    def success(self, message: object) -> None:
        self._record("success", message)

    def warning(self, message: object) -> None:
        self._record("warning", message)

    def error(self, message: object) -> None:
        self._record("error", message)

    def print_text(self, message: object) -> None:
        self._record("text", message)

    def contains(self, text: str) -> bool:
        return any(text in message for _, message in self.messages)


@pytest.fixture
def output(monkeypatch: pytest.MonkeyPatch) -> _OutputRecorder:
    recorder = _OutputRecorder()
    monkeypatch.setattr(pg, "ccyo_out", recorder)
    return recorder


def _proc(returncode: int = 0, stdout: str = "", stderr: str = ""):
    return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)


def test_pg_legacy_branch_campaign_runtime_paths_and_options(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    context = SimpleNamespace(
        postgres_dir=lambda: tmp_path / "postgres",
        postgres_socket_dir=lambda: tmp_path / "socket",
        lock_dir=lambda: tmp_path / "locks",
    )
    monkeypatch.setattr(pg, "resolve_context", lambda **_kwargs: context)
    monkeypatch.setattr(pg, "get_db_config", lambda: {"unix_socket_dir": ""})

    assert (
        pg._get_postgres_data_dir(pg.Environment.target) == tmp_path / "postgres/data"
    )
    assert (
        pg._get_postgres_log_file(pg.Environment.target)
        == tmp_path / "postgres/postgresql.log"
    )
    assert pg._get_postgres_socket_dir(pg.Environment.target) == tmp_path / "socket"
    assert (
        pg._get_instance_lock_file(pg.Environment.target)
        == tmp_path / "locks/instance.lock"
    )
    assert pg._build_pg_ctl_options(5544, tmp_path / "socket dir") == (
        f"-p 5544 -k '{tmp_path}/socket dir' -h localhost"
    )


def test_pg_legacy_branch_campaign_conf_rendering_and_platform_rules(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert pg._set_postgresql_conf_value(
        "shared_memory_type = sysv\n", "shared_memory_type", "mmap"
    ) == ("shared_memory_type = mmap\n")
    appended = pg._set_postgresql_conf_value(
        "port = 5432", "shared_memory_type", "mmap"
    )
    assert appended.endswith(
        "# TAPDB local Linux shared memory settings\nshared_memory_type = mmap\n"
    )
    assert pg._set_postgresql_conf_value("", "shared_memory_type", "mmap").startswith(
        "# TAPDB"
    )

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    monkeypatch.setattr(pg.platform, "system", lambda: "Darwin")
    pg._ensure_linux_local_postgres_conf(data_dir)

    monkeypatch.setattr(pg.platform, "system", lambda: "Linux")
    with pytest.raises(RuntimeError, match="postgresql.conf not found"):
        pg._ensure_linux_local_postgres_conf(data_dir)

    conf = data_dir / "postgresql.conf"
    conf.write_text(
        "shared_memory_type = mmap\ndynamic_shared_memory_type = mmap\n",
        encoding="utf-8",
    )
    before = conf.stat().st_mtime_ns
    pg._ensure_linux_local_postgres_conf(data_dir)
    assert conf.stat().st_mtime_ns == before

    conf.write_text("# shared_memory_type = sysv\n", encoding="utf-8")
    pg._ensure_linux_local_postgres_conf(data_dir)
    assert "shared_memory_type = mmap" in conf.read_text(encoding="utf-8")


@pytest.mark.parametrize(
    ("result", "expected"),
    [
        (_proc(returncode=1), "already in use"),
        (_proc(stdout=""), "already in use"),
        (_proc(stdout="COMMAND PID\n"), "already in use"),
        (_proc(stdout="COMMAND PID\npostgres 42\n"), "postgres 42"),
    ],
)
def test_pg_legacy_branch_campaign_port_conflict_details(
    monkeypatch: pytest.MonkeyPatch, result, expected: str
) -> None:
    monkeypatch.setattr(pg.subprocess, "run", lambda *_a, **_k: result)
    assert expected in pg._port_conflict_details(5544)


def test_pg_legacy_branch_campaign_port_checks_tolerate_missing_lsof(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable(*_args, **_kwargs):
        raise FileNotFoundError("lsof")

    monkeypatch.setattr(pg.subprocess, "run", unavailable)
    assert pg._port_conflict_details(5544) == "port 5544 is already in use"
    assert pg._is_port_available(5544) is True

    monkeypatch.setattr(
        pg.subprocess, "run", lambda *_a, **_k: _proc(stdout="postgres")
    )
    assert pg._is_port_available(5544) is False
    monkeypatch.setattr(pg.subprocess, "run", lambda *_a, **_k: _proc(returncode=1))
    assert pg._is_port_available(5544) is True


@pytest.mark.parametrize(
    ("system", "existing", "method"),
    [
        ("Darwin", set(), "unknown"),
        ("Linux", {"/bin/systemctl"}, "systemd"),
        ("Linux", {"/usr/bin/systemctl"}, "systemd"),
        ("Linux", {"/usr/sbin/service"}, "sysvinit"),
        ("Linux", set(), "unknown"),
    ],
)
def test_pg_legacy_branch_campaign_service_discovery(
    monkeypatch: pytest.MonkeyPatch, system: str, existing: set[str], method: str
) -> None:
    monkeypatch.setattr(pg.platform, "system", lambda: system)
    monkeypatch.setattr(Path, "exists", lambda self: str(self) in existing)
    discovered, start, stop, _log = pg._get_pg_service_cmd()
    assert discovered == method
    if method == "unknown":
        assert start == stop == []
    else:
        assert start[-1] == "postgresql" or start[-2:] == ["postgresql", "start"]


@pytest.mark.parametrize(
    ("effects", "expected"),
    [
        ([_proc(returncode=1)], (False, "")),
        (
            [_proc(), _proc(stdout="PostgreSQL 16.4, compiled\n")],
            (True, "PostgreSQL 16.4"),
        ),
        ([_proc(), _proc(returncode=1)], (True, "unknown")),
        ([FileNotFoundError("missing")], (False, "pg_isready not found")),
        ([subprocess.TimeoutExpired("pg_isready", 5)], (False, "timeout")),
        ([RuntimeError("broken")], (False, "broken")),
    ],
)
def test_pg_legacy_branch_campaign_running_probe(
    monkeypatch: pytest.MonkeyPatch, effects, expected
) -> None:
    queue = list(effects)

    def run(*_args, **_kwargs):
        effect = queue.pop(0)
        if isinstance(effect, Exception):
            raise effect
        return effect

    monkeypatch.setattr(pg.subprocess, "run", run)
    assert pg._is_pg_running() == expected


def test_pg_legacy_branch_campaign_system_start_states(
    monkeypatch: pytest.MonkeyPatch, output: _OutputRecorder
) -> None:
    monkeypatch.setattr(pg, "_is_pg_running", lambda: (True, "PostgreSQL 16"))
    pg.pg_start()
    assert output.contains("already running")

    monkeypatch.setattr(pg, "_is_pg_running", lambda: (False, ""))
    monkeypatch.setattr(
        pg,
        "_get_pg_service_cmd",
        lambda: ("unknown", [], [], Path("/definitely/missing/system.log")),
    )
    with pytest.raises(typer.Exit):
        pg.pg_start()
    assert output.contains("No system PostgreSQL service")


@pytest.mark.parametrize(
    ("start_effect", "probes", "expected", "raises"),
    [
        (_proc(), [(True, "PostgreSQL 16")], "PostgreSQL started", False),
        (_proc(), [(False, "")] * 10, "not responding", False),
        (_proc(returncode=1, stderr="denied"), [], "Failed to start", True),
        (subprocess.TimeoutExpired("start", 30), [], "timed out", False),
        (RuntimeError("spawn failed"), [], "spawn failed", True),
    ],
)
def test_pg_legacy_branch_campaign_system_start_process_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    output: _OutputRecorder,
    start_effect,
    probes,
    expected: str,
    raises: bool,
) -> None:
    queue = iter([(False, ""), *probes])
    monkeypatch.setattr(pg, "_is_pg_running", lambda: next(queue))
    monkeypatch.setattr(
        pg, "_get_pg_service_cmd", lambda: ("systemd", ["start"], [], Path())
    )
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    def run(*_args, **_kwargs):
        if isinstance(start_effect, Exception):
            raise start_effect
        return start_effect

    monkeypatch.setattr(pg.subprocess, "run", run)
    if raises:
        with pytest.raises(typer.Exit):
            pg.pg_start()
    else:
        pg.pg_start()
    assert output.contains(expected)


def test_pg_legacy_branch_campaign_system_stop_states(
    monkeypatch: pytest.MonkeyPatch, output: _OutputRecorder
) -> None:
    monkeypatch.setattr(pg, "_is_pg_running", lambda: (False, ""))
    pg.pg_stop()
    assert output.contains("not running")

    monkeypatch.setattr(pg, "_is_pg_running", lambda: (True, ""))
    monkeypatch.setattr(
        pg,
        "_get_pg_service_cmd",
        lambda: ("unknown", [], [], Path("/definitely/missing/system.log")),
    )
    with pytest.raises(typer.Exit):
        pg.pg_stop()
    assert output.contains("No system PostgreSQL service")


@pytest.mark.parametrize(
    ("stop_effect", "probes", "expected", "raises"),
    [
        (_proc(), [(False, "")], "PostgreSQL stopped", False),
        (_proc(), [(True, "")] * 10, "still running", False),
        (_proc(returncode=1, stderr="denied"), [], "Failed to stop", True),
        (RuntimeError("stop failed"), [], "stop failed", True),
    ],
)
def test_pg_legacy_branch_campaign_system_stop_process_outcomes(
    monkeypatch: pytest.MonkeyPatch,
    output: _OutputRecorder,
    stop_effect,
    probes,
    expected: str,
    raises: bool,
) -> None:
    queue = iter([(True, ""), *probes])
    monkeypatch.setattr(pg, "_is_pg_running", lambda: next(queue))
    monkeypatch.setattr(
        pg, "_get_pg_service_cmd", lambda: ("systemd", [], ["stop"], Path())
    )
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    def run(*_args, **_kwargs):
        if isinstance(stop_effect, Exception):
            raise stop_effect
        return stop_effect

    monkeypatch.setattr(pg.subprocess, "run", run)
    if raises:
        with pytest.raises(typer.Exit):
            pg.pg_stop()
    else:
        pg.pg_stop()
    assert output.contains(expected)


@pytest.mark.parametrize("ready_effect", [_proc(), FileNotFoundError("missing")])
def test_pg_legacy_branch_campaign_status_reports_ready_and_missing_client(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output: _OutputRecorder,
    ready_effect,
) -> None:
    paths = {
        "_get_postgres_data_dir": tmp_path / "data",
        "_get_postgres_log_file": tmp_path / "postgres.log",
        "_get_postgres_socket_dir": tmp_path / "socket",
        "_get_instance_lock_file": tmp_path / "instance.lock",
    }
    for name, value in paths.items():
        monkeypatch.setattr(pg, name, lambda _env, value=value: value)
    context = SimpleNamespace(namespace_slug=lambda: "branch-campaign")
    monkeypatch.setattr(pg, "resolve_context", lambda **_kwargs: context)
    monkeypatch.setattr(
        pg,
        "get_db_config",
        lambda: {"host": "localhost", "port": 5544, "user": "tapdb"},
    )

    def run(*_args, **_kwargs):
        if isinstance(ready_effect, Exception):
            raise ready_effect
        return ready_effect

    monkeypatch.setattr(pg.subprocess, "run", run)
    pg.pg_status()
    if isinstance(ready_effect, Exception):
        assert output.contains("not running")
    else:
        assert output.contains("is running")
    assert output.contains("branch-campaign")


def test_pg_legacy_branch_campaign_logs_reads_requested_tail(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: _OutputRecorder
) -> None:
    log = tmp_path / "postgres.log"
    log.write_text("one\ntwo\nthree\n", encoding="utf-8")
    monkeypatch.setattr(pg, "_get_pg_service_cmd", lambda: ("unknown", [], [], Path()))
    monkeypatch.setattr(pg, "_get_postgres_log_file", lambda _env: log)
    pg.pg_logs(follow=False, lines=2)
    assert output.contains("two")
    assert output.contains("three")
    assert not output.contains("one")


def test_pg_legacy_branch_campaign_logs_follow_can_be_interrupted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: _OutputRecorder
) -> None:
    log = tmp_path / "postgres.log"
    log.write_text("line\n", encoding="utf-8")
    monkeypatch.setattr(pg, "_get_pg_service_cmd", lambda: ("unknown", [], [], Path()))
    monkeypatch.setattr(pg, "_get_postgres_log_file", lambda _env: log)
    monkeypatch.setattr(
        pg.subprocess,
        "run",
        lambda *_a, **_k: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    pg.pg_logs(follow=True, lines=3)
    assert output.contains("Stopped")


@pytest.mark.parametrize("follow", [False, True])
def test_pg_legacy_branch_campaign_logs_falls_back_to_journal(
    monkeypatch: pytest.MonkeyPatch,
    output: _OutputRecorder,
    follow: bool,
) -> None:
    monkeypatch.setattr(
        pg,
        "_get_pg_service_cmd",
        lambda: ("unknown", [], [], Path("/definitely/missing/system.log")),
    )
    monkeypatch.setattr(
        pg,
        "_get_postgres_log_file",
        lambda _env: (_ for _ in ()).throw(RuntimeError("no context")),
    )
    monkeypatch.setattr(pg.platform, "system", lambda: "Linux")
    calls = []
    monkeypatch.setattr(pg.subprocess, "run", lambda command: calls.append(command))
    pg.pg_logs(follow=follow, lines=7)
    assert calls[0][:4] == ["sudo", "journalctl", "-u", "postgresql"]
    assert ("-f" in calls[0]) is follow
    assert output.contains("Trying journalctl")


@pytest.mark.parametrize(
    "effect", [KeyboardInterrupt(), RuntimeError("journal failed")]
)
def test_pg_legacy_branch_campaign_journal_failures_are_reported(
    monkeypatch: pytest.MonkeyPatch, output: _OutputRecorder, effect: Exception
) -> None:
    monkeypatch.setattr(
        pg,
        "_get_pg_service_cmd",
        lambda: ("unknown", [], [], Path("/definitely/missing/system.log")),
    )
    monkeypatch.setattr(
        pg, "_get_postgres_log_file", lambda _env: Path("/missing/local.log")
    )
    monkeypatch.setattr(pg.platform, "system", lambda: "Linux")
    monkeypatch.setattr(
        pg.subprocess, "run", lambda *_a, **_k: (_ for _ in ()).throw(effect)
    )
    pg.pg_logs(follow=False, lines=2)
    assert output.contains(
        "Stopped" if isinstance(effect, KeyboardInterrupt) else "journal failed"
    )


def test_pg_legacy_branch_campaign_logs_read_failures_are_reported(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: _OutputRecorder
) -> None:
    log = tmp_path / "postgres.log"
    log.write_text("line", encoding="utf-8")
    monkeypatch.setattr(pg, "_get_pg_service_cmd", lambda: ("unknown", [], [], Path()))
    monkeypatch.setattr(pg, "_get_postgres_log_file", lambda _env: log)

    real_open = builtins.open
    monkeypatch.setattr(
        builtins,
        "open",
        lambda *_a, **_k: (_ for _ in ()).throw(PermissionError("denied")),
    )
    pg.pg_logs(follow=False, lines=2)
    assert output.contains("Permission denied")

    monkeypatch.setattr(
        builtins,
        "open",
        lambda *_a, **_k: (_ for _ in ()).throw(OSError("read failed")),
    )
    pg.pg_logs(follow=False, lines=2)
    assert output.contains("Error reading logs")
    monkeypatch.setattr(builtins, "open", real_open)


def test_pg_legacy_branch_campaign_restart_orders_stop_then_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    monkeypatch.setattr(pg, "pg_stop", lambda: calls.append("stop"))
    monkeypatch.setattr(pg, "pg_start", lambda: calls.append("start"))
    monkeypatch.setattr("time.sleep", lambda seconds: calls.append(f"sleep:{seconds}"))
    pg.pg_restart()
    assert calls == ["stop", "sleep:2", "start"]


def _patch_local_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    paths = SimpleNamespace(
        data=tmp_path / "postgres/data",
        log=tmp_path / "postgres/postgresql.log",
        socket=tmp_path / "socket",
        lock=tmp_path / "locks/instance.lock",
    )
    monkeypatch.setattr(pg, "_get_postgres_data_dir", lambda _env: paths.data)
    monkeypatch.setattr(pg, "_get_postgres_log_file", lambda _env: paths.log)
    monkeypatch.setattr(pg, "_get_postgres_socket_dir", lambda _env: paths.socket)
    monkeypatch.setattr(pg, "_get_instance_lock_file", lambda _env: paths.lock)
    monkeypatch.setattr(
        pg,
        "get_db_config",
        lambda: {
            "user": "tapdb",
            "port": 5544,
            "operator_configured": True,
            "operator_user": "tapdb_operator",
        },
    )
    return paths


def test_pg_legacy_branch_campaign_init_missing_and_existing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: _OutputRecorder
) -> None:
    paths = _patch_local_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(pg.shutil, "which", lambda _name: None)
    with pytest.raises(typer.Exit):
        pg.pg_init(force=False)
    assert output.contains("initdb not found")

    paths.data.mkdir(parents=True)
    (paths.data / "PG_VERSION").write_text("16", encoding="utf-8")
    monkeypatch.setattr(pg.shutil, "which", lambda _name: "/bin/initdb")
    pg.pg_init(force=False)
    assert output.contains("already initialized")


@pytest.mark.parametrize(
    ("effect", "expected", "raises"),
    [
        (_proc(), "initialized", False),
        (_proc(returncode=1, stderr="bad cluster"), "initdb failed", True),
        (subprocess.TimeoutExpired("initdb", 60), "timed out", True),
        (RuntimeError("spawn failed"), "spawn failed", True),
    ],
)
def test_pg_legacy_branch_campaign_init_process_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output: _OutputRecorder,
    effect,
    expected: str,
    raises: bool,
) -> None:
    paths = _patch_local_paths(monkeypatch, tmp_path)
    paths.data.mkdir(parents=True)
    (paths.data / "PG_VERSION").write_text("16", encoding="utf-8")
    monkeypatch.setattr(pg.shutil, "which", lambda _name: "/bin/initdb")

    def run(*_args, **_kwargs):
        if isinstance(effect, Exception):
            raise effect
        return effect

    monkeypatch.setattr(pg.subprocess, "run", run)
    if raises:
        with pytest.raises(typer.Exit):
            pg.pg_init(force=True)
    else:
        pg.pg_init(force=True)
    assert output.contains(expected)
    assert not paths.data.exists()


@pytest.mark.parametrize(
    ("config", "port", "prepare", "message"),
    [
        ({"port": 0}, None, False, "Missing/invalid"),
        ({"port": 5544}, 5545, False, "does not match"),
        ({"port": 5544}, None, False, "not initialized"),
        ({"port": 5544}, None, True, "pg_ctl not found"),
    ],
)
def test_pg_legacy_branch_campaign_start_local_preconditions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output: _OutputRecorder,
    config,
    port,
    prepare: bool,
    message: str,
) -> None:
    paths = _patch_local_paths(monkeypatch, tmp_path)
    monkeypatch.setattr(pg, "get_db_config", lambda: config)
    if prepare:
        paths.data.mkdir(parents=True)
        (paths.data / "PG_VERSION").write_text("16", encoding="utf-8")
    monkeypatch.setattr(pg.shutil, "which", lambda _name: None)
    with pytest.raises(typer.Exit):
        pg.pg_start_local(port=port)
    assert output.contains(message)


def test_pg_legacy_branch_campaign_start_local_pid_and_port_conflict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: _OutputRecorder
) -> None:
    paths = _patch_local_paths(monkeypatch, tmp_path)
    paths.data.mkdir(parents=True)
    (paths.data / "PG_VERSION").write_text("16", encoding="utf-8")
    (paths.data / "postmaster.pid").write_text("42", encoding="utf-8")
    monkeypatch.setattr(pg.shutil, "which", lambda _name: "/bin/pg_ctl")
    pg.pg_start_local(port=None)
    assert output.contains("may already be running")

    (paths.data / "postmaster.pid").unlink()
    monkeypatch.setattr(pg, "_is_port_available", lambda _port: False)
    monkeypatch.setattr(pg, "_port_conflict_details", lambda port: f"conflict:{port}")
    with pytest.raises(typer.Exit):
        pg.pg_start_local(port=5544)
    assert output.contains("conflict:5544")


@pytest.mark.parametrize(
    ("effect", "message"),
    [
        (RuntimeError("bad config"), "Error preparing PostgreSQL config"),
        (_proc(returncode=1, stderr="start denied"), "Failed to start PostgreSQL"),
        (RuntimeError("spawn failed"), "spawn failed"),
    ],
)
def test_pg_legacy_branch_campaign_start_local_failures(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output: _OutputRecorder,
    effect,
    message: str,
) -> None:
    paths = _patch_local_paths(monkeypatch, tmp_path)
    paths.data.mkdir(parents=True)
    (paths.data / "PG_VERSION").write_text("16", encoding="utf-8")
    monkeypatch.setattr(pg.shutil, "which", lambda _name: "/bin/pg_ctl")
    monkeypatch.setattr(pg, "_is_port_available", lambda _port: True)
    if isinstance(effect, RuntimeError) and str(effect) == "bad config":
        monkeypatch.setattr(
            pg,
            "_ensure_linux_local_postgres_conf",
            lambda _path: (_ for _ in ()).throw(effect),
        )
    else:
        monkeypatch.setattr(pg, "_ensure_linux_local_postgres_conf", lambda _path: None)

        def run(*_args, **_kwargs):
            if isinstance(effect, Exception):
                raise effect
            return effect

        monkeypatch.setattr(pg.subprocess, "run", run)
    with pytest.raises(typer.Exit):
        pg.pg_start_local(port=5544)
    assert output.contains(message)


def test_pg_legacy_branch_campaign_start_local_writes_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: _OutputRecorder
) -> None:
    paths = _patch_local_paths(monkeypatch, tmp_path)
    paths.data.mkdir(parents=True)
    (paths.data / "PG_VERSION").write_text("16", encoding="utf-8")
    monkeypatch.setattr(pg.shutil, "which", lambda _name: "/bin/pg_ctl")
    monkeypatch.setattr(pg, "_is_port_available", lambda _port: True)
    monkeypatch.setattr(pg, "_ensure_linux_local_postgres_conf", lambda _path: None)
    commands = []
    monkeypatch.setattr(
        pg.subprocess,
        "run",
        lambda command, **_kwargs: commands.append(command) or _proc(),
    )
    pg.pg_start_local(port=5544)
    receipt = __import__("json").loads(paths.lock.read_text(encoding="utf-8"))
    assert receipt == {
        "target": "explicit",
        "port": 5544,
        "data_dir": str(paths.data),
        "log_file": str(paths.log),
        "socket_dir": str(paths.socket),
    }
    assert commands[0][1] == "start"
    assert output.contains("PostgreSQL started")


def test_pg_legacy_branch_campaign_stop_local_preconditions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, output: _OutputRecorder
) -> None:
    paths = _patch_local_paths(monkeypatch, tmp_path)
    pg.pg_stop_local()
    assert output.contains("doesn't exist")

    paths.data.mkdir(parents=True)
    monkeypatch.setattr(pg.shutil, "which", lambda _name: None)
    with pytest.raises(typer.Exit):
        pg.pg_stop_local()
    assert output.contains("pg_ctl not found")


@pytest.mark.parametrize(
    ("effect", "message", "raises"),
    [
        (_proc(), "PostgreSQL stopped", False),
        (_proc(returncode=1, stderr="not running"), "not running", False),
        (_proc(returncode=1, stderr=""), "may not be running", False),
        (RuntimeError("stop spawn failed"), "stop spawn failed", True),
    ],
)
def test_pg_legacy_branch_campaign_stop_local_process_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    output: _OutputRecorder,
    effect,
    message: str,
    raises: bool,
) -> None:
    paths = _patch_local_paths(monkeypatch, tmp_path)
    paths.data.mkdir(parents=True)
    paths.lock.parent.mkdir(parents=True)
    paths.lock.write_text("lock", encoding="utf-8")
    monkeypatch.setattr(pg.shutil, "which", lambda _name: "/bin/pg_ctl")

    def run(*_args, **_kwargs):
        if isinstance(effect, Exception):
            raise effect
        return effect

    monkeypatch.setattr(pg.subprocess, "run", run)
    if raises:
        with pytest.raises(typer.Exit):
            pg.pg_stop_local()
    else:
        pg.pg_stop_local()
    assert output.contains(message)
    assert paths.lock.exists() is (effect.returncode != 0 if not raises else True)
