from __future__ import annotations

import json
import os
import re
import stat
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.cli.context import (
    clear_cli_context,
    set_cli_context,
)

runner = CliRunner()
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _strip(text: str) -> str:
    return _ANSI_RE.sub("", text)


@pytest.fixture
def cli_namespace(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("HOME", str(tmp_path))
    cfg_path = (
        tmp_path / ".config" / "tapdb" / "testclient" / "testdb" / "tapdb-config.yaml"
    )
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    domain_registry = tmp_path / "domain_code_registry.json"
    prefix_registry = tmp_path / "prefix_ownership_registry.json"
    domain_registry.write_text(
        json.dumps({"version": "0.4.0", "domains": {"Z": {"name": "test-localhost"}}})
        + "\n",
        encoding="utf-8",
    )
    prefix_registry.write_text(
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
            }
        )
        + "\n",
        encoding="utf-8",
    )
    cfg_path.write_text(
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: testclient\n"
        "  database_name: testdb\n"
        "  owner_repo_name: daylily-tapdb\n"
        f"  domain_registry_path: {domain_registry}\n"
        f"  prefix_ownership_registry_path: {prefix_registry}\n"
        "admin:\n"
        "  footer:\n"
        "    repo_url: https://github.com/example/tapdb\n"
        "  session:\n"
        "    secret: session-secret\n"
        "  auth:\n"
        "    mode: tapdb\n"
        "    disabled_user:\n"
        "      email: tapdb-admin@localhost\n"
        "      role: admin\n"
        "    shared_host:\n"
        "      session_secret: shared-secret\n"
        "      session_cookie: session\n"
        "      session_max_age_seconds: 1209600\n"
        "  cors:\n"
        "    allowed_origins: []\n"
        "  ui:\n"
        "    tls:\n"
        "      cert_path: ''\n"
        "      key_path: ''\n"
        "  metrics:\n"
        "    enabled: true\n"
        "    queue_max: 20000\n"
        "    flush_seconds: 1.0\n"
        "  db_pool_size: 5\n"
        "  db_max_overflow: 10\n"
        "  db_pool_timeout: 30\n"
        "  db_pool_recycle: 1800\n"
        "environments:\n"
        "  dev:\n"
        "    engine_type: local\n"
        "    host: localhost\n"
        "    port: 5533\n"
        "    ui_port: 8911\n"
        "    domain_code: Z\n"
        "    user: tapdb\n"
        "    password: ''\n"
        "    database: tapdb_dev\n"
        "    schema_name: tapdb_testdb_dev\n"
        "  prod:\n"
        "    engine_type: local\n"
        "    host: localhost\n"
        "    port: 5535\n"
        "    ui_port: 9443\n"
        "    domain_code: Z\n"
        "    user: tapdb\n"
        "    password: ''\n"
        "    database: tapdb_prod\n"
        "    schema_name: tapdb_testdb_prod\n",
        encoding="utf-8",
    )
    os.chmod(cfg_path, stat.S_IRUSR | stat.S_IWUSR)
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        env_name="dev",
        config_path=cfg_path,
    )
    yield cfg_path
    clear_cli_context()


def test_root_tls_helpers_and_admin_module_lookup(
    cli_namespace: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_admin_settings_for_env",
        lambda _env, config_path=None: {
            "tls_cert_path": "~/admin.crt",
            "tls_key_path": "~/admin.key",
        },
    )
    cert_path, key_path = cli_mod._resolve_tls_paths("dev")
    assert cert_path == Path("~/admin.crt").expanduser()
    assert key_path == Path("~/admin.key").expanduser()

    cert_path, key_path = cli_mod._resolve_tls_paths(
        "dev",
        cert_file=Path("~/explicit.crt"),
        key_file=Path("~/explicit.key"),
    )
    assert cert_path == Path("~/explicit.crt").expanduser()
    assert key_path == Path("~/explicit.key").expanduser()

    cert_file = tmp_path / "certs" / "localhost.crt"
    key_file = tmp_path / "certs" / "localhost.key"
    monkeypatch.setattr(
        cli_mod, "_resolve_tls_paths", lambda *_args, **_kwargs: (cert_file, key_file)
    )
    monkeypatch.setattr(cli_mod.shutil, "which", lambda _name: None)
    with pytest.raises(RuntimeError, match="openssl is required"):
        cli_mod._ensure_tls_certificates("localhost", env_name="dev")

    openssl_calls: list[list[str]] = []

    def _openssl_success(cmd, capture_output=True, text=True):
        openssl_calls.append(list(cmd))
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cli_mod.shutil, "which", lambda _name: "/usr/bin/openssl")
    monkeypatch.setattr(cli_mod.subprocess, "run", _openssl_success)
    cli_mod._ensure_tls_certificates("tapdb.local", env_name="dev")
    assert "subjectAltName=DNS:localhost,DNS:tapdb.local" in " ".join(openssl_calls[0])

    fallback_calls: list[list[str]] = []

    def _openssl_fallback(cmd, capture_output=True, text=True):
        fallback_calls.append(list(cmd))
        return SimpleNamespace(
            returncode=1 if len(fallback_calls) == 1 else 0,
            stdout="",
            stderr="bad addext",
        )

    monkeypatch.setattr(cli_mod.subprocess, "run", _openssl_fallback)
    cli_mod._ensure_tls_certificates("localhost", env_name="dev")
    assert len(fallback_calls) == 2

    monkeypatch.setattr(
        cli_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout="", stderr="still broken"
        ),
    )
    with pytest.raises(RuntimeError, match="Failed to generate TLS certificate"):
        cli_mod._ensure_tls_certificates("localhost", env_name="dev")

    monkeypatch.setattr(cli_mod.subprocess, "run", _openssl_success)
    monkeypatch.setattr(
        cli_mod.os,
        "chmod",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("chmod blocked")),
    )
    cli_mod._ensure_tls_certificates("localhost", env_name="dev")

    monkeypatch.chdir(tmp_path)
    fake_pkg_root = tmp_path / "pkgroot" / "daylily_tapdb" / "cli"
    fake_pkg_root.mkdir(parents=True)
    monkeypatch.setattr(cli_mod, "__file__", str(fake_pkg_root / "__init__.py"))
    with pytest.raises(ValueError, match="Cannot find admin module"):
        cli_mod._find_admin_module()


def test_root_port_details_register_and_main(
    cli_namespace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        cli_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=0,
            stdout="COMMAND\npython 12345 user  TCP *:8911",
            stderr="",
        ),
    )
    assert "python 12345" in cli_mod._port_conflict_details(8911)

    captured: dict[str, object] = {}

    def _fake_run(_spec, argv):
        captured["argv"] = list(argv)
        return 17

    monkeypatch.setattr("cli_core_yo.app.run", _fake_run)
    monkeypatch.setattr(
        "sys.argv",
        [
            "tapdb",
            "--config",
            str(cli_namespace),
            "--env",
            "dev",
            "ui",
            "status",
        ],
    )
    with pytest.raises(SystemExit) as exc:
        cli_mod.main()
    assert exc.value.code == 17
    assert captured["argv"] == [
        "--config",
        str(cli_namespace),
        "--env",
        "dev",
        "ui",
        "status",
    ]


def test_register_supports_registry_without_add_typer_app(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fresh_app = cli_mod.build_app()
    sentinel_policy = object()
    monkeypatch.setattr(cli_mod, "app", fresh_app)
    monkeypatch.setattr(cli_mod, "policy_for_command", lambda *_args: sentinel_policy)

    class _Registry:
        def __init__(self) -> None:
            self.groups: list[tuple[str, str]] = []
            self.commands: list[tuple[str | None, str, str, object]] = []

        def add_group(
            self,
            name,
            *,
            help_text="",
            order=None,
            metadata=None,
        ):
            _ = (order, metadata)
            self.groups.append((name, help_text))

        def add_command(
            self,
            group_path,
            name,
            callback,
            *,
            help_text="",
            policy,
            order=None,
        ):
            _ = (callback, order)
            self.commands.append((group_path, name, help_text, policy))

    registry = _Registry()
    cli_mod.register(registry, object())

    assert any(name == "db-config" for name, _help_text in registry.groups)
    assert any(
        group_path == "ui" and name == "start" and policy is sentinel_policy
        for group_path, name, _help_text, policy in registry.commands
    )


def test_root_callback_and_ui_start_branches(
    cli_namespace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(
        cli_mod,
        "_require_context",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("bad context")),
    )
    result = runner.invoke(
        fresh_app,
        ["--config", str(cli_namespace), "--env", "dev", "info"],
    )
    assert result.exit_code == 1
    assert isinstance(result.exception, RuntimeError)
    assert str(result.exception) == "bad context"

    fake_context = SimpleNamespace(
        ui_dir=lambda _env: cli_namespace.parent / ".tapdb-ui",
        namespace_slug=lambda: "testclient/testdb",
    )
    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(
        cli_mod,
        "_require_context",
        lambda **_kwargs: fake_context,
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda _env: {"ui_port": "8911"},
    )
    result = runner.invoke(
        fresh_app,
        ["ui", "start", "--port", "9000"],
    )
    assert result.exit_code == 1

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(
        cli_mod, "_require_admin_extras", lambda: (_ for _ in ()).throw(SystemExit(1))
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda _env: {"ui_port": "8911"},
    )
    result = runner.invoke(fresh_app, ["ui", "start"])
    assert result.exit_code == 1
    assert "Admin UI dependencies are not installed" in _strip(result.output)

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod, "_require_admin_extras", lambda: None)
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: 123)
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda _env: {"ui_port": "8911"},
    )
    result = runner.invoke(fresh_app, ["ui", "start"])
    assert result.exit_code == 0
    assert "already running" in _strip(result.output)

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod, "_require_admin_extras", lambda: None)
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: None)
    monkeypatch.setattr(cli_mod, "_port_is_available", lambda _host, _port: False)
    monkeypatch.setattr(cli_mod, "_port_conflict_details", lambda _port: "busy")
    monkeypatch.setattr(
        cli_mod,
        "_require_context",
        lambda **_kwargs: fake_context,
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda _env: {"ui_port": "8911"},
    )
    result = runner.invoke(fresh_app, ["ui", "start"])
    assert result.exit_code == 1
    assert "busy" in _strip(result.output)

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod, "_require_admin_extras", lambda: None)
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: None)
    monkeypatch.setattr(cli_mod, "_port_is_available", lambda _host, _port: True)
    monkeypatch.setattr(
        cli_mod,
        "_ensure_tls_certificates",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("tls failed")),
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda _env: {"ui_port": "8911"},
    )
    result = runner.invoke(fresh_app, ["ui", "start"])
    assert result.exit_code == 1
    assert "tls failed" in _strip(result.output)

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod, "_require_admin_extras", lambda: None)
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: None)
    monkeypatch.setattr(cli_mod, "_port_is_available", lambda _host, _port: True)
    monkeypatch.setattr(
        cli_mod,
        "_ensure_tls_certificates",
        lambda *_args, **_kwargs: (
            Path("/tmp/localhost.crt"),
            Path("/tmp/localhost.key"),
        ),
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda _env: {"ui_port": "8911"},
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_config_path",
        lambda: cli_namespace,
    )

    class _PopenFail:
        pid = 222

        def poll(self):
            return 1

    monkeypatch.setattr(
        cli_mod.subprocess,
        "Popen",
        lambda *args, **kwargs: _PopenFail(),
    )
    monkeypatch.setattr(cli_mod.time, "sleep", lambda *_args, **_kwargs: None)
    result = runner.invoke(fresh_app, ["ui", "start"])
    assert result.exit_code == 1
    assert "Server failed to start" in _strip(result.output)

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod, "_require_admin_extras", lambda: None)
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: None)
    monkeypatch.setattr(cli_mod, "_port_is_available", lambda _host, _port: True)
    monkeypatch.setattr(
        cli_mod,
        "_ensure_tls_certificates",
        lambda *_args, **_kwargs: (
            Path("/tmp/localhost.crt"),
            Path("/tmp/localhost.key"),
        ),
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda _env: {"ui_port": "8911"},
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_config_path",
        lambda: cli_namespace,
    )

    class _PopenOk:
        pid = 321

        def poll(self):
            return None

    monkeypatch.setattr(
        cli_mod.subprocess,
        "Popen",
        lambda *args, **kwargs: _PopenOk(),
    )
    monkeypatch.setattr(cli_mod.time, "sleep", lambda *_args, **_kwargs: None)
    result = runner.invoke(fresh_app, ["ui", "start"])
    assert result.exit_code == 0
    assert "UI server started" in _strip(result.output)

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod, "_require_admin_extras", lambda: None)
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: None)
    monkeypatch.setattr(cli_mod, "_port_is_available", lambda _host, _port: True)
    monkeypatch.setattr(
        cli_mod,
        "_ensure_tls_certificates",
        lambda *_args, **_kwargs: (
            Path("/tmp/localhost.crt"),
            Path("/tmp/localhost.key"),
        ),
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda _env: {"ui_port": "8911"},
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_config_path",
        lambda: cli_namespace,
    )
    monkeypatch.setattr(
        cli_mod.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    result = runner.invoke(fresh_app, ["ui", "start", "--foreground"])
    assert result.exit_code == 0


def test_ui_mkcert_stop_logs_restart_and_bootstrap(
    cli_namespace: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod.shutil, "which", lambda _name: None)
    result = runner.invoke(fresh_app, ["ui", "mkcert"])
    assert result.exit_code == 1

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod.shutil, "which", lambda _name: "/usr/bin/mkcert")
    mkcert_calls = {"count": 0}

    def _mkcert_install_fail(cmd, capture_output=True, text=True):
        mkcert_calls["count"] += 1
        return SimpleNamespace(returncode=1, stdout="", stderr="install failed")

    monkeypatch.setattr(cli_mod.subprocess, "run", _mkcert_install_fail)
    result = runner.invoke(fresh_app, ["ui", "mkcert"])
    assert result.exit_code == 1

    fresh_app = cli_mod.build_app()

    def _mkcert_generate_fail(cmd, capture_output=True, text=True):
        return SimpleNamespace(
            returncode=0 if "-install" in cmd else 1,
            stdout="",
            stderr="generate failed",
        )

    monkeypatch.setattr(cli_mod.shutil, "which", lambda _name: "/usr/bin/mkcert")
    monkeypatch.setattr(cli_mod.subprocess, "run", _mkcert_generate_fail)
    result = runner.invoke(fresh_app, ["ui", "mkcert"])
    assert result.exit_code == 1

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod.shutil, "which", lambda _name: "/usr/bin/mkcert")
    monkeypatch.setattr(
        cli_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout="", stderr=""),
    )
    monkeypatch.setattr(
        cli_mod.os,
        "chmod",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("chmod blocked")),
    )
    result = runner.invoke(fresh_app, ["ui", "mkcert"])
    assert result.exit_code == 0

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: 123)
    kill_state = {"phase": 0}

    def _kill_success(pid, sig):
        if sig == 0 and kill_state["phase"] == 0:
            kill_state["phase"] += 1
            return None
        if sig == 0:
            raise ProcessLookupError()
        return None

    monkeypatch.setattr(cli_mod.os, "kill", _kill_success)
    monkeypatch.setattr(cli_mod.time, "sleep", lambda *_args, **_kwargs: None)
    result = runner.invoke(fresh_app, ["ui", "stop"])
    assert result.exit_code == 0

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: 123)
    monkeypatch.setattr(
        cli_mod.os,
        "kill",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(ProcessLookupError()),
    )
    result = runner.invoke(fresh_app, ["ui", "stop"])
    assert result.exit_code == 0
    assert "Server was not running" in _strip(result.output)

    fresh_app = cli_mod.build_app()
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: 123)
    monkeypatch.setattr(
        cli_mod.os,
        "kill",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(PermissionError()),
    )
    result = runner.invoke(fresh_app, ["ui", "stop"])
    assert result.exit_code == 1

    fresh_app = cli_mod.build_app()
    result = runner.invoke(fresh_app, ["ui", "logs"])
    assert result.exit_code == 0
    assert "No log file found" in _strip(result.output)

    fresh_app = cli_mod.build_app()
    cli_mod._require_context(env_name="dev")
    _, log_file, _ = cli_mod._ui_runtime_paths("dev")
    log_file.parent.mkdir(parents=True, exist_ok=True)
    log_file.write_text("line-1\nline-2\n", encoding="utf-8")
    monkeypatch.setattr(
        cli_mod.subprocess,
        "run",
        lambda *args, **kwargs: (_ for _ in ()).throw(KeyboardInterrupt()),
    )
    result = runner.invoke(fresh_app, ["ui", "logs"])
    assert result.exit_code == 0

    fresh_app = cli_mod.build_app()
    log_file.write_text("line-1\nline-2\n", encoding="utf-8")
    monkeypatch.setattr(
        "builtins.open",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("read failed")),
    )
    result = runner.invoke(fresh_app, ["ui", "logs", "--no-follow"])
    assert result.exit_code == 0
    assert "Error reading logs: read failed" in _strip(result.output)

    monkeypatch.setattr(cli_mod.time, "sleep", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli_mod, "_require_admin_extras", lambda: None)
    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: None)
    monkeypatch.setattr(cli_mod, "_port_is_available", lambda _host, _port: True)
    monkeypatch.setattr(
        cli_mod,
        "_ensure_tls_certificates",
        lambda *_args, **_kwargs: (
            Path("/tmp/localhost.crt"),
            Path("/tmp/localhost.key"),
        ),
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda _env: {"ui_port": "8911", "engine_type": "aurora"},
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_config_path",
        lambda: cli_namespace,
    )

    class _PopenOk:
        pid = 444

        def poll(self):
            return None

    monkeypatch.setattr(cli_mod.subprocess, "Popen", lambda *args, **kwargs: _PopenOk())
    fresh_app = cli_mod.build_app()
    result = runner.invoke(fresh_app, ["bootstrap", "local"])
    assert result.exit_code == 1
    assert "use bootstrap aurora" in _strip(result.output)

    monkeypatch.setattr("daylily_tapdb.cli.db.create_database", lambda **_kwargs: None)
    monkeypatch.setattr("daylily_tapdb.cli.db.apply_schema", lambda **_kwargs: None)
    monkeypatch.setattr("daylily_tapdb.cli.db.run_migrations", lambda **_kwargs: None)
    monkeypatch.setattr("daylily_tapdb.cli.db.seed_templates", lambda **_kwargs: None)
    monkeypatch.setattr(
        "daylily_tapdb.cli.db._create_default_admin", lambda **_kwargs: False
    )
    monkeypatch.setattr("daylily_tapdb.cli.pg.pg_init", lambda **_kwargs: None)
    monkeypatch.setattr("daylily_tapdb.cli.pg.pg_start_local", lambda **_kwargs: None)
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda _env: {"engine_type": "local", "ui_port": "9443"},
    )
    monkeypatch.setattr(
        cli_mod, "_require_admin_extras", lambda: (_ for _ in ()).throw(SystemExit(1))
    )
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        env_name="prod",
        config_path=cli_namespace,
    )
    fresh_app = cli_mod.build_app()
    result = runner.invoke(fresh_app, ["bootstrap", "local"])
    assert result.exit_code == 0
    assert "Local bootstrap complete" in _strip(result.output)


def test_config_init_update_and_info_commands(
    cli_namespace: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fresh_app = cli_mod.build_app()
    init_path = tmp_path / "new-config.yaml"
    clear_cli_context()
    set_cli_context(config_path=init_path)
    result = runner.invoke(
        fresh_app,
        [
            "config",
            "init",
            "--client-id",
            "atlas",
            "--database-name",
            "app",
            "--owner-repo-name",
            "lsmc-atlas",
            "--env",
            "dev",
            "--env",
            "test",
            "--domain-code",
            "dev=Z",
            "--domain-code",
            "test=Z",
            "--db-port",
            "dev=5533",
            "--db-port",
            "test=5534",
            "--ui-port",
            "dev=8911",
            "--ui-port",
            "test=8912",
            "--schema-name",
            "dev=tapdb_atlas_unidbtst_dev",
        ],
    )
    assert result.exit_code == 0
    init_payload = yaml.safe_load(init_path.read_text(encoding="utf-8"))
    assert init_payload["meta"]["client_id"] == "atlas"
    assert init_payload["environments"]["dev"]["ui_port"] == "8911"
    assert init_payload["environments"]["dev"]["schema_name"] == "tapdb_atlas_unidbtst_dev"
    assert init_payload["environments"]["test"]["port"] == "5534"
    assert init_payload["environments"]["test"]["schema_name"] == "tapdb_app_test"

    clear_cli_context()
    set_cli_context(config_path=init_path)
    result = runner.invoke(
        fresh_app,
        [
            "config",
            "init",
            "--client-id",
            "other",
            "--database-name",
            "app",
            "--owner-repo-name",
            "other-repo",
            "--domain-code",
            "dev=Z",
            "--db-port",
            "dev=5533",
            "--ui-port",
            "dev=8911",
        ],
    )
    assert result.exit_code != 0

    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        env_name="dev",
        config_path=cli_namespace,
    )
    fresh_app = cli_mod.build_app()
    result = runner.invoke(
        fresh_app,
        ["config", "update", "--env", "dev", "--clear", "invalid"],
    )
    assert result.exit_code != 0

    empty_cfg = tmp_path / "empty.yaml"
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        env_name="dev",
        config_path=empty_cfg,
    )
    fresh_app = cli_mod.build_app()
    result = runner.invoke(
        fresh_app, ["config", "update", "--env", "dev", "--host", "db.local"]
    )
    assert result.exit_code != 0

    bad_meta_cfg = tmp_path / "bad-meta.yaml"
    bad_meta_cfg.write_text("environments: {}\n", encoding="utf-8")
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        env_name="dev",
        config_path=bad_meta_cfg,
    )
    fresh_app = cli_mod.build_app()
    result = runner.invoke(
        fresh_app, ["config", "update", "--env", "dev", "--host", "db.local"]
    )
    assert result.exit_code != 0

    bad_admin_cfg = tmp_path / "bad-admin.yaml"
    bad_admin_cfg.write_text(
        "meta:\n"
        "  client_id: testclient\n"
        "  database_name: testdb\n"
        "admin: not-a-map\n"
        "environments:\n"
        "  dev: {}\n",
        encoding="utf-8",
    )
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        env_name="dev",
        config_path=bad_admin_cfg,
    )
    fresh_app = cli_mod.build_app()
    result = runner.invoke(
        fresh_app, ["config", "update", "--env", "dev", "--host", "db.local"]
    )
    assert result.exit_code != 0

    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        env_name="dev",
        config_path=cli_namespace,
    )
    fresh_app = cli_mod.build_app()
    result = runner.invoke(fresh_app, ["config", "update", "--env", "dev"])
    assert result.exit_code != 0

    result = runner.invoke(
        fresh_app,
        [
            "config",
            "update",
            "--env",
            "dev",
            "--host",
            "db.internal",
            "--schema-name",
            "tapdb_testdb_dev_next",
            "--port",
            "6543",
            "--ui-port",
            "9443",
            "--support-email",
            "support@example.com",
            "--admin-repo-url",
            "https://github.com/example/tapdb-core",
            "--admin-session-secret",
            "new-secret",
            "--admin-auth-mode",
            "shared_host",
            "--admin-disabled-user-email",
            "admin@example.com",
            "--admin-disabled-user-role",
            "admin",
            "--admin-shared-host-session-secret",
            "shared-host-secret",
            "--admin-shared-host-session-cookie",
            "tapdb_session",
            "--admin-shared-host-session-max-age-seconds",
            "3600",
            "--admin-allowed-origin",
            "https://portal.example.com",
            "--admin-tls-cert-path",
            "/tmp/localhost.crt",
            "--admin-tls-key-path",
            "/tmp/localhost.key",
            "--admin-metrics-enabled",
            "--admin-metrics-queue-max",
            "500",
            "--admin-metrics-flush-seconds",
            "2.5",
            "--clear",
            "support_email",
        ],
    )
    assert result.exit_code == 0
    updated = yaml.safe_load(cli_namespace.read_text(encoding="utf-8"))
    assert updated["environments"]["dev"]["host"] == "db.internal"
    assert updated["environments"]["dev"]["schema_name"] == "tapdb_testdb_dev_next"
    assert (
        updated["admin"]["footer"]["repo_url"]
        == "https://github.com/example/tapdb-core"
    )
    assert updated["admin"]["auth"]["mode"] == "shared_host"
    assert updated["admin"]["metrics"]["queue_max"] == 500

    monkeypatch.setattr(cli_mod, "_get_pid", lambda _pid_file: 321)

    def _info_run(cmd, capture_output=True, text=True, env=None, timeout=None):
        if cmd[:2] == ["ps", "-p"]:
            return SimpleNamespace(
                returncode=0,
                stdout="Mon Apr 07 12:34:56 2025\n",
                stderr="",
            )
        if cmd[0] == "psql" and cmd[-1] == "select 1;":
            return SimpleNamespace(returncode=0, stdout="1\n", stderr="")
        if cmd[0] == "psql" and "pg_postmaster_start_time" in cmd[-1]:
            return SimpleNamespace(returncode=0, stdout="00:10:00\n", stderr="")
        raise AssertionError(cmd)

    monkeypatch.setattr("shutil.which", lambda name: name)
    monkeypatch.setattr(cli_mod.subprocess, "run", _info_run)
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_db_config_for_env",
        lambda env_name: {
            "host": "localhost",
            "port": "5533" if env_name == "dev" else "5534",
            "user": "tapdb",
            "password": "secret",
            "database": f"tapdb_{env_name}",
            "schema_name": f"tapdb_testdb_{env_name}",
        },
    )
    result = runner.invoke(fresh_app, ["info", "--check-all-envs", "--json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["ui"]["running"] is True
    assert payload["postgres"]["dev"]["status"] == "ok"

    monkeypatch.setattr(
        cli_mod.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(
            returncode=1, stdout="", stderr="ps failed"
        ),
    )
    result = runner.invoke(fresh_app, ["info"])
    assert result.exit_code == 0
