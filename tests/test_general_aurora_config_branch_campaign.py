from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from daylily_tapdb import euid, governance
from daylily_tapdb.aurora import cfn_template
from daylily_tapdb.aurora.config import AuroraConfig
from daylily_tapdb.aurora.connection import AuroraConnectionBuilder
from daylily_tapdb.aurora.schema_deployer import AuroraSchemaDeployer
from daylily_tapdb.aurora.stack_manager import (
    AuroraStackManager,
    _cfn_events_summary,
)
from daylily_tapdb.cli import context, db_config


def test_general_campaign_cfn_default_and_explicit_tags_and_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert cfn_template._build_tags(region="eu-west-1")[1] == {
        "Key": "lsmc-project",
        "Value": "tapdb-eu-west-1",
    }
    assert cfn_template._build_tags(project="explicit")[1]["Value"] == "explicit"
    monkeypatch.setattr(cfn_template, "__file__", str(tmp_path / "cfn_template.py"))
    written = cfn_template.save_template()
    assert written == tmp_path / "templates/aurora-postgres.json"
    assert json.loads(written.read_text(encoding="utf-8"))["Resources"]


def test_general_campaign_connection_rejects_non_https_and_uses_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(RuntimeError, match="https URL"):
        __import__(
            "daylily_tapdb.aurora.connection", fromlist=["x"]
        )._require_https_url("http://example.test/file", label="bundle")

    client = MagicMock()
    client.generate_db_auth_token.return_value = "token"
    boto = SimpleNamespace(
        session=SimpleNamespace(
            Session=lambda **kwargs: SimpleNamespace(
                client=lambda *_args, **_kwargs: client
            )
        )
    )
    import daylily_tapdb.aurora.connection as connection

    connection._iam_token_cache.clear()
    monkeypatch.setattr(connection, "_ensure_boto3", lambda: boto)
    assert (
        AuroraConnectionBuilder.get_iam_auth_token(
            "us-west-2", "db.example", 5432, "tapdb", profile="dev"
        )
        == "token"
    )
    assert client.generate_db_auth_token.called


def test_general_campaign_schema_deployer_hostaddr_and_empty_command(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        AuroraConnectionBuilder, "ensure_ca_bundle", lambda: Path("/ca.pem")
    )
    monkeypatch.setattr(
        AuroraConnectionBuilder, "get_iam_auth_token", lambda **_kwargs: "token"
    )
    env = AuroraSchemaDeployer.client_env(
        host="db.example",
        hostaddr="127.0.0.1",
        port=5432,
        user="tapdb",
        region="us-west-2",
    )
    assert env["PGHOSTADDR"] == "127.0.0.1"
    commands = []
    monkeypatch.setattr(
        "daylily_tapdb.aurora.schema_deployer.subprocess.run",
        lambda command, **_kwargs: (
            commands.append(command)
            or SimpleNamespace(returncode=0, stdout=" ok ", stderr="")
        ),
    )
    ok, output = AuroraSchemaDeployer.run_psql(
        host="db.example",
        port=5432,
        user="tapdb",
        database="tapdb",
        region="us-west-2",
    )
    assert (ok, output) == (True, "ok")
    assert "-c" not in commands[0] and "-f" not in commands[0]


def test_general_campaign_stack_event_reason_and_failure() -> None:
    client = MagicMock()
    client.describe_stack_events.return_value = {
        "StackEvents": [
            {
                "Timestamp": "now",
                "LogicalResourceId": "DB",
                "ResourceStatus": "FAILED",
                "ResourceStatusReason": "bad subnet",
            }
        ]
    }
    assert "bad subnet" in _cfn_events_summary(client, "stack")
    client.describe_stack_events.side_effect = RuntimeError("denied")
    assert "unable" in _cfn_events_summary(client, "stack")


def _manager() -> AuroraStackManager:
    return AuroraStackManager(cfn_client=MagicMock(), ec2_client=MagicMock())


def test_general_campaign_stack_update_and_delete_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _manager()
    config = AuroraConfig(cluster_identifier="branch")
    monkeypatch.setattr(
        manager,
        "_stack_request",
        lambda _config: ("tapdb-branch", "{}", [], [], "vpc"),
    )
    manager._cfn.update_stack.side_effect = RuntimeError(
        "No updates are to be performed"
    )
    monkeypatch.setattr(
        manager,
        "get_stack_status",
        lambda _name: {"status": "UPDATE_COMPLETE", "outputs": {}},
    )
    assert manager.update_stack(config)["status"] == "UPDATE_COMPLETE"

    manager._cfn.update_stack.side_effect = RuntimeError("denied")
    with pytest.raises(RuntimeError, match="Stack update failed"):
        manager.update_stack(config)

    manager._cfn.update_stack.side_effect = None
    monkeypatch.setattr(
        manager,
        "wait_for_stack",
        lambda *_a, **_k: {"status": "UPDATE_FAILED", "outputs": {}},
    )
    with pytest.raises(RuntimeError, match="UPDATE_FAILED"):
        manager.update_stack(config)

    with pytest.raises(RuntimeError, match="DELETE_FAILED"):
        monkeypatch.setattr(
            manager,
            "wait_for_stack",
            lambda *_a, **_k: {"status": "DELETE_FAILED"},
        ) or manager.delete_stack("tapdb-branch", retain_networking=False)


def test_general_campaign_stack_detection_and_wait_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manager = _manager()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {
            "StackSummaries": [
                {"StackName": "other"},
                {"StackName": "tapdb-good"},
                {"StackName": "tapdb-bad"},
            ]
        },
        {"StackSummaries": []},
    ]
    manager._cfn.get_paginator.return_value = paginator
    manager._cfn.describe_stacks.return_value = {
        "Stacks": [{"Tags": [{"Key": "lsmc-project", "Value": "tapdb-good"}]}]
    }

    def status(name):
        if name == "tapdb-bad":
            raise RuntimeError("gone")
        return {"status": "CREATE_COMPLETE", "outputs": {}}

    monkeypatch.setattr(manager, "get_stack_status", status)
    assert list(manager.detect_existing_resources()) == ["tapdb-good"]

    states = iter(
        [
            {"status": "CREATE_IN_PROGRESS", "outputs": {}},
            {"status": "CREATE_COMPLETE", "outputs": {"Endpoint": "db"}},
        ]
    )
    monkeypatch.setattr(manager, "get_stack_status", lambda _name: next(states))
    ticks = iter([0.0, 0.0, 1.0])
    monkeypatch.setattr(
        "daylily_tapdb.aurora.stack_manager.time.monotonic", lambda: next(ticks)
    )
    monkeypatch.setattr(
        "daylily_tapdb.aurora.stack_manager.time.sleep", lambda _delay: None
    )
    callbacks = []
    result = manager.wait_for_stack(
        "tapdb-good", "CREATE_COMPLETE", callback=lambda *args: callbacks.append(args)
    )
    assert result["outputs"] == {"Endpoint": "db"}
    assert len(callbacks) == 2

    monkeypatch.setattr(
        manager,
        "get_stack_status",
        lambda _name: (_ for _ in ()).throw(RuntimeError("deleted")),
    )
    monkeypatch.setattr(
        "daylily_tapdb.aurora.stack_manager.time.monotonic", lambda: 0.0
    )
    assert manager.wait_for_stack("gone", "DELETE_COMPLETE") == {
        "status": "DELETE_COMPLETE",
        "outputs": {},
    }


def test_general_campaign_governance_validation_and_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(ValueError, match="explicit registry"):
        governance._resolved_path(None)
    for invalid in ("", "AB", "-"):
        with pytest.raises(ValueError, match="domain code"):
            governance._validate_domain_code(invalid)
    assert governance._validate_domain_code(" z ") == "Z"

    calls = []
    monkeypatch.setattr(
        governance,
        "meridian_assert_registered_domain",
        lambda code, **kwargs: calls.append((code, kwargs)) or code,
    )
    registry_path = tmp_path / "domains.json"
    assert governance.assert_registered_domain("z", path=registry_path) == "Z"
    assert (
        governance.assert_registered_domain(
            "z", registry=frozenset({"Z"}), registry_metadata={"Z": {}}, path=None
        )
        == "Z"
    )
    assert calls[0][1]["path"] == registry_path.resolve()
    assert calls[1][1]["registry"] == frozenset({"Z"})

    registry = {("Z", "TPX"): "daylily-tapdb"}
    assert governance.resolve_prefix_owner_repo_name("z", "tpx", registry=registry) == (
        "daylily-tapdb"
    )
    with pytest.raises(ValueError, match="not registered"):
        governance.resolve_prefix_owner_repo_name("z", "bad", registry=registry)
    with pytest.raises(ValueError, match="is owned by"):
        governance.assert_prefix_owner_repo_name(
            "z", "tpx", "other-repo", registry=registry
        )


def test_general_campaign_governance_context_loads_and_requires_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    domain_path = tmp_path / "domains.json"
    prefix_path = tmp_path / "prefixes.json"
    metadata = {"Z": {"name": "test"}}
    ownership = {("Z", "TPX"): "daylily-tapdb"}
    monkeypatch.setattr(
        governance, "validate_registries_consistent", lambda **_kwargs: None
    )
    monkeypatch.setattr(
        governance, "load_domain_registry_metadata", lambda _path: metadata
    )
    monkeypatch.setattr(
        governance, "load_prefix_ownership_registry", lambda _path: ownership
    )
    monkeypatch.setattr(
        governance, "assert_registered_domain", lambda code, **_kwargs: code.upper()
    )
    loaded = governance.GovernanceContext.load(
        domain_code="z",
        owner_repo_name="daylily-tapdb",
        domain_registry_path=domain_path,
        prefix_ownership_registry_path=prefix_path,
    )
    assert loaded.registered_domains == frozenset({"Z"})
    assert loaded.require_prefix("TPX") == "daylily-tapdb"


def test_general_campaign_euid_rejects_ambiguous_inputs() -> None:
    for bad in (True, 0, -1, "1"):
        with pytest.raises(ValueError, match="positive int"):
            euid._int_to_base32(bad)
    assert euid._canonical_euid_parts(123) is None
    with pytest.raises(ValueError, match="empty string"):
        euid.resolve_runtime_owner_repo_name({"TAPDB_OWNER_REPO": " "})
    with pytest.raises(ValueError, match="empty string"):
        euid.resolve_runtime_domain_code({"MERIDIAN_DOMAIN_CODE": " "})
    with pytest.raises(ValueError, match="prefix is required"):
        euid.format_euid("", 1, domain_code="Z")
    with pytest.raises(ValueError, match="Unsupported"):
        euid.validate_euid("<persisted-euid>", environment="legacy")
    config = euid.EUIDConfig()
    assert config.get_discriminator_for_prefix("") is None
    assert config.is_canonical_prefix("") is False
    assert config.is_canonical_prefix("TPX") is True


@pytest.fixture(autouse=True)
def _clear_context() -> None:
    context.clear_cli_context()
    yield
    context.clear_cli_context()


def test_general_campaign_context_runtime_invocation_branches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(context, "_runtime_context", lambda: None)
    assert context._runtime_invocation_value("client_id") is None
    monkeypatch.setattr(
        context, "_runtime_context", lambda: SimpleNamespace(invocation=[])
    )
    assert context._runtime_invocation_value("client_id") is None
    monkeypatch.setattr(
        context,
        "_runtime_context",
        lambda: SimpleNamespace(invocation={"client_id": None}),
    )
    assert context._runtime_invocation_value("client_id") is None
    monkeypatch.setattr(
        context,
        "_runtime_context",
        lambda: SimpleNamespace(invocation={"client_id": "  alpha  "}),
    )
    assert context._runtime_invocation_value("client_id") == "alpha"
    assert context._normalize_key(" ", field_name="client") is None
    with pytest.raises(RuntimeError, match="Invalid"):
        context._normalize_key("/bad", field_name="client")


def test_general_campaign_context_paths_and_socket_fallback(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    implicit = context.TapdbContext("alpha", "beta")
    monkeypatch.setattr(Path, "home", lambda: tmp_path)
    assert implicit.config_dir() == tmp_path / ".config/tapdb/alpha/beta"
    assert implicit.config_path().name == context.CONFIG_FILENAME
    short = context.TapdbContext("alpha", "beta", Path("/tmp/tapdb-config.yaml"))
    assert short.postgres_socket_dir() == short.postgres_dir() / "run"

    explicit = context.TapdbContext(
        "a" * 80, "b" * 80, tmp_path / ("c" * 80) / "config.yaml"
    )
    monkeypatch.setenv("XDG_STATE_HOME", str(tmp_path / ("s" * 80)))
    assert explicit.postgres_socket_dir().name.startswith("tapdb-pg-")


def test_general_campaign_context_config_metadata_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    missing = tmp_path / "missing.yaml"
    assert context._load_meta_from_config_path(missing) == (None, None)
    malformed = tmp_path / "malformed.yaml"
    malformed.write_text("- item\n", encoding="utf-8")
    assert context._load_meta_from_config_path(malformed) == (None, None)
    no_meta = tmp_path / "no-meta.yaml"
    no_meta.write_text("target: {}\n", encoding="utf-8")
    assert context._load_meta_from_config_path(no_meta) == (None, None)

    monkeypatch.setattr(context, "_runtime_context", lambda: None)
    assert context.active_config_path() is None
    monkeypatch.setattr(
        context,
        "_runtime_context",
        lambda: SimpleNamespace(config_path=""),
    )
    assert context.active_config_path() is None


def test_general_campaign_context_resolution_optional_and_required(
    tmp_path: Path,
) -> None:
    resolved = context.resolve_context(
        require_keys=False, client_id="alpha", database_name="beta"
    )
    assert resolved == context.TapdbContext("alpha", "beta")
    assert context.resolve_context(require_keys=False) is None
    with pytest.raises(RuntimeError, match="config path is required"):
        context.resolve_context(require_keys=True)

    config_path = tmp_path / "config.yaml"
    config_path.write_text("meta: {client_id: alpha}\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="metadata is required"):
        context.resolve_context(require_keys=True, config_path=config_path)


def _db_context(tmp_path: Path) -> db_config.TapdbContext:
    return db_config.TapdbContext("alpha", "beta", tmp_path / "config.yaml")


def _db_root(tmp_path: Path, *, engine: str = "local") -> tuple[dict, dict]:
    target = {
        "engine_type": engine,
        "host": "localhost",
        "port": "5533",
        "ui_port": "8911",
        "user": "tapdb",
        "password": "",
        "database": "tapdb_shared",
        "schema_name": "tapdb_beta",
        "domain_code": "Z",
    }
    root = {
        "meta": {
            "config_version": 4,
            "client_id": "alpha",
            "database_name": "beta",
            "owner_repo_name": "daylily-tapdb",
            "domain_registry_path": str(tmp_path / "domains.json"),
            "prefix_ownership_registry_path": str(tmp_path / "prefixes.json"),
        },
        "target": target,
        "safety": {
            "safety_tier": "shared",
            "destructive_operations": "confirm_required",
        },
    }
    return root, target


def test_general_campaign_db_config_small_helper_branches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with pytest.raises(RuntimeError, match="non-empty"):
        db_config.normalize_postgres_identifier_component("---")
    assert db_config.default_database_name_for_namespace("db", "dev") == "tapdb_db_dev"
    ctx = _db_context(tmp_path)
    monkeypatch.setattr(db_config, "resolve_context", lambda **_kwargs: ctx)
    assert db_config.get_config_paths() == [ctx.config_path()]
    assert db_config.get_config_path() == ctx.config_path()

    with pytest.raises(RuntimeError, match="missing safety"):
        db_config._read_safety({}, tmp_path / "config.yaml")
    for key, value, message in (
        ("safety_tier", "unsafe", "safety_tier"),
        ("destructive_operations", "maybe", "destructive_operations"),
    ):
        safety = {"safety_tier": "shared", "destructive_operations": "blocked"}
        safety[key] = value
        with pytest.raises(RuntimeError, match=message):
            db_config._read_safety({"safety": safety}, tmp_path / "config.yaml")

    with pytest.raises(RuntimeError, match="must be a mapping"):
        db_config._as_mapping([], field_name="field")
    assert db_config._as_mapping(None, field_name="field") == {}
    assert db_config._string(None, default="x") == "x"
    assert db_config._bool(None, default=True) is True
    assert db_config._bool("yes", default=False) is True
    assert db_config._bool("off", default=True) is False
    assert db_config._bool("unknown", default=True) is True
    assert db_config._int("", default=3) == 3
    assert db_config._int("4", default=3) == 4
    assert db_config._int("bad", default=3) == 3
    assert db_config._float("", default=1.5) == 1.5
    assert db_config._float("2.5", default=1.5) == 2.5
    assert db_config._float("bad", default=1.5) == 1.5
    assert db_config._unparseable_number("") is False
    assert db_config._unparseable_number("bad") is True
    assert db_config._unparseable_number(float("nan")) is True
    assert db_config._unparseable_number("3.7", integral=True) is True
    assert db_config._unparseable_number("3", integral=True) is False
    assert db_config._string_list(None) == []
    assert db_config._string_list([" a ", ""]) == ["a"]
    assert db_config._string_list(" ") == []
    assert db_config._string_list("a, b") == ["a", "b"]


def test_general_campaign_db_config_loader_and_metadata_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty = tmp_path / "empty.yaml"
    empty.write_text("", encoding="utf-8")
    assert db_config._load_yaml_or_json(empty) == {}
    sequence = tmp_path / "sequence.yaml"
    sequence.write_text("- one\n", encoding="utf-8")
    with pytest.raises(ValueError, match="mapping"):
        db_config._load_yaml_or_json(sequence)

    ctx = _db_context(tmp_path)
    with pytest.raises(RuntimeError, match="metadata is required"):
        db_config._validate_meta_for_context({}, ctx)
    root, _target = _db_root(tmp_path)
    root["meta"]["config_version"] = 99
    with pytest.raises(RuntimeError, match="Unsupported"):
        db_config._validate_meta_for_context(root, ctx)
    root["meta"]["config_version"] = 4
    root["meta"]["owner_repo_name"] = ""
    with pytest.raises(RuntimeError):
        db_config._validate_meta_for_context(root, ctx)
    root["meta"]["owner_repo_name"] = "daylily-tapdb"
    root["meta"]["client_id"] = "other"
    with pytest.raises(RuntimeError, match="does not match"):
        db_config._validate_meta_for_context(root, ctx)

    monkeypatch.setattr(
        db_config, "get_config_paths", lambda **_kwargs: [tmp_path / "missing"]
    )
    monkeypatch.setattr(db_config, "resolve_context", lambda **_kwargs: ctx)
    with pytest.raises(RuntimeError, match="No TAPDB config"):
        db_config._resolve_common_config()


def test_general_campaign_db_config_target_engine_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        db_config.GovernanceContext,
        "load",
        lambda **kwargs: SimpleNamespace(
            domain_code=str(kwargs["domain_code"]).upper()
        ),
    )
    ctx = _db_context(tmp_path)
    path = tmp_path / "config.yaml"

    root, target = _db_root(tmp_path, engine="invalid")
    with pytest.raises(RuntimeError, match="Unsupported TAPDB engine_type"):
        db_config._build_db_config_from_section(
            ctx=ctx,
            root=root,
            resolved_config_path=path,
            file_cfg=target,
            section_name="target",
            target_name="target",
        )

    root, target = _db_root(tmp_path, engine="local")
    target["hostaddr"] = "127.0.0.1"
    with pytest.raises(RuntimeError, match="hostaddr"):
        db_config._build_db_config_from_section(
            ctx=ctx,
            root=root,
            resolved_config_path=path,
            file_cfg=target,
            section_name="target",
            target_name="target",
        )
    target.pop("hostaddr")
    target["host"] = "remote"
    with pytest.raises(RuntimeError, match="Invalid local host"):
        db_config._build_db_config_from_section(
            ctx=ctx,
            root=root,
            resolved_config_path=path,
            file_cfg=target,
            section_name="target",
            target_name="target",
        )

    root, target = _db_root(tmp_path, engine="aurora")
    target.update(
        region="us-west-2", cluster_identifier="cluster", iam_auth=True, ssl=True
    )
    target["hostaddr"] = "bad"
    with pytest.raises(RuntimeError, match="IP address"):
        db_config._build_db_config_from_section(
            ctx=ctx,
            root=root,
            resolved_config_path=path,
            file_cfg=target,
            section_name="target",
            target_name="target",
        )
    target["hostaddr"] = "127.0.0.1"
    assert (
        db_config._build_db_config_from_section(
            ctx=ctx,
            root=root,
            resolved_config_path=path,
            file_cfg=target,
            section_name="target",
            target_name="target",
        )["hostaddr"]
        == "127.0.0.1"
    )

    root, target = _db_root(tmp_path, engine="compose")
    target["hostaddr"] = "127.0.0.1"
    with pytest.raises(RuntimeError, match="hostaddr"):
        db_config._build_db_config_from_section(
            ctx=ctx,
            root=root,
            resolved_config_path=path,
            file_cfg=target,
            section_name="target",
            target_name="target",
        )


def test_general_campaign_db_config_target_and_settings_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    ctx = _db_context(tmp_path)
    monkeypatch.setattr(
        db_config, "_resolve_common_config", lambda **_kwargs: (ctx, {}, tmp_path / "c")
    )
    with pytest.raises(RuntimeError, match="metadata is required"):
        db_config.get_db_config()

    with pytest.raises(RuntimeError, match="missing target.value"):
        db_config._require_section_str(
            {}, "value", tmp_path / "c", section_name="target"
        )

    monkeypatch.setattr(
        db_config,
        "get_db_config",
        lambda **_kwargs: {"client_id": "a", "database_name": "b"},
    )
    monkeypatch.setattr(
        db_config,
        "_load_config_with_path",
        lambda **_kwargs: ({}, tmp_path / "c", False),
    )
    with pytest.raises(RuntimeError, match="No TAPDB config"):
        db_config.get_backup_settings()
    with pytest.raises(RuntimeError, match="No TAPDB config"):
        db_config.get_admin_settings()

    monkeypatch.setattr(
        db_config,
        "_load_config_with_path",
        lambda **_kwargs: (
            {"backup": {"encryption": {"mode": "aes"}}},
            tmp_path / "c",
            True,
        ),
    )
    with pytest.raises(RuntimeError, match="encryption.mode"):
        db_config.get_backup_settings()
    monkeypatch.setattr(
        db_config,
        "_load_config_with_path",
        lambda **_kwargs: (
            {"backup": {"signing": {"mode": "bad"}}},
            tmp_path / "c",
            True,
        ),
    )
    with pytest.raises(RuntimeError, match="signing.mode"):
        db_config.get_backup_settings()
    monkeypatch.setattr(
        db_config,
        "_load_config_with_path",
        lambda **_kwargs: ({"admin": {"auth": {"mode": "bad"}}}, tmp_path / "c", True),
    )
    with pytest.raises(RuntimeError, match="admin.auth.mode"):
        db_config.get_admin_settings()
