from __future__ import annotations

import json
import stat
from pathlib import Path

from typer.testing import CliRunner

from daylily_tapdb.cli import framework_app, spec

runner = CliRunner()


def _write_config(path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "meta:\n"
        "  config_version: 3\n"
        "  client_id: alpha\n"
        "  database_name: beta\n"
        "  euid_client_code: A\n"
        "admin:\n"
        "  footer:\n"
        "    repo_url: https://example.com/tapdb\n"
        "  session:\n"
        "    secret: secret123\n"
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
        "    port: '5533'\n"
        "    ui_port: '8911'\n"
        "    user: tapdb\n"
        "    password: ''\n"
        "    database: tapdb_dev\n"
        "    audit_log_euid_prefix: AGX\n",
        encoding="utf-8",
    )
    path.chmod(stat.S_IRUSR | stat.S_IWUSR)
    return path


def test_cli_spec_uses_platform_v2_context_contract() -> None:
    assert spec.policy.profile == "platform-v2"
    assert spec.context is not None
    assert [option.name for option in spec.context.options] == [
        "env_name",
        "client_id",
        "database_name",
    ]


def test_cli_registry_exposes_v2_command_tree_and_policies() -> None:
    assert framework_app is not None
    registry = framework_app._cli_core_yo_registry

    for argv in (
        ["version"],
        ["info"],
        ["config", "path"],
        ["db-config", "init"],
        ["ui", "status"],
        ["db", "schema", "drift-check"],
        ["pg", "status"],
        ["users", "delete"],
        ["cognito", "status"],
        ["aurora", "status"],
    ):
        assert registry.resolve_command_args(argv) is not None

    version_cmd = registry.get_command(("version",))
    info_cmd = registry.get_command(("info",))
    db_config_init_cmd = registry.get_command(("db-config", "init"))
    ui_start_cmd = registry.get_command(("ui", "start"))
    user_delete_cmd = registry.get_command(("users", "delete"))

    assert version_cmd is not None
    assert version_cmd.policy.runtime_guard == "exempt"

    assert info_cmd is not None
    assert info_cmd.policy.runtime_guard == "exempt"
    assert info_cmd.policy.supports_json is True

    assert db_config_init_cmd is not None
    assert db_config_init_cmd.policy.mutates_state is True

    assert ui_start_cmd is not None
    assert ui_start_cmd.policy.long_running is True

    assert user_delete_cmd is not None
    assert user_delete_cmd.policy.interactive is True


def test_root_json_is_global_for_version() -> None:
    assert framework_app is not None

    result = runner.invoke(framework_app, ["--json", "version"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["app"] == "TapDB CLI"


def test_json_rejected_for_non_json_command(tmp_path: Path) -> None:
    assert framework_app is not None
    cfg_path = tmp_path / "tapdb-config.yaml"

    result = runner.invoke(
        framework_app,
        [
            "--json",
            "--config",
            str(cfg_path),
            "db-config",
            "init",
            "--client-id",
            "alpha",
            "--database-name",
            "beta",
            "--euid-client-code",
            "A",
            "--env",
            "dev",
            "--db-port",
            "dev=5533",
            "--ui-port",
            "dev=8911",
        ],
    )

    assert result.exit_code == 2
    payload = json.loads(result.stdout)
    assert payload["error"]["code"] == "contract_violation"
    assert payload["error"]["details"]["command"] == "db-config/init"


def test_framework_invocation_context_reaches_runtime_command(tmp_path: Path) -> None:
    assert framework_app is not None
    cfg_path = _write_config(tmp_path / "tapdb-config.yaml")

    result = runner.invoke(
        framework_app,
        ["--config", str(cfg_path), "--env", "dev", "ui", "status"],
    )

    assert result.exit_code == 0
    assert "not running" in result.stdout.lower()
