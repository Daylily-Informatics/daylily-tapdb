"""Explicit reserved-domain authorization; all fixtures are local and offline."""

from __future__ import annotations

import json
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from meridian_euid.exceptions import EUIDGovernanceError
from typer.testing import CliRunner

from daylily_tapdb.cli import framework_app
from daylily_tapdb.cli.context import clear_cli_context
from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.governance import (
    GovernanceAuthorization,
    GovernanceContext,
    assert_registered_domain,
)
from daylily_tapdb.templates import loader, repository


DOMAIN_METADATA = {
    "M": {
        "name": "LSMC",
        "status": "reserved",
        "reserved_for": "LSMC",
        "non_production_policy": "triple_approval_required",
    },
    "Z": {"name": "test"},
}
PRODUCTION_AUTHORIZATION = {
    "reserved_for": "LSMC",
    "deployment_environment": "production",
    "approval_tokens": [],
}


@pytest.fixture(autouse=True)
def _isolated_context():
    clear_cli_context()
    yield
    clear_cli_context()


def _registries(tmp_path: Path) -> tuple[Path, Path]:
    domains = tmp_path / "domains.json"
    prefixes = tmp_path / "prefixes.json"
    domains.write_text(json.dumps({"version": "0.4.8", "domains": DOMAIN_METADATA}))
    prefixes.write_text(json.dumps({
        "version": "0.4.8",
        "ownership": {
            "M": {"ASY": {"issuer_app_code": "example-owner"}},
            "Z": {"ASY": {"issuer_app_code": "example-owner"}},
        },
    }))
    return domains, prefixes


def _config(tmp_path: Path, authorization=None) -> Path:
    domains, prefixes = _registries(tmp_path)
    config = tmp_path / "tapdb.yaml"
    root = {
        "meta": {
            "config_version": 4,
            "client_id": "qualification",
            "database_name": "governance",
            "owner_repo_name": "example-owner",
            "domain_registry_path": str(domains),
            "prefix_ownership_registry_path": str(prefixes),
        },
        "target": {
            "engine_type": "local", "host": "localhost", "port": "5533",
            "ui_port": "8911", "domain_code": "M", "user": "runtime",
            "password": "", "database": "qualification", "schema_name": "governance",
        },
        "safety": {"safety_tier": "production", "destructive_operations": "blocked"},
    }
    if authorization is not None:
        root["meta"]["governance_authorization"] = authorization
    config.write_text(yaml.safe_dump(root))
    return config


@pytest.mark.parametrize("authorization, message", [
    (None, "reserved_for is required"),
    ({"reserved_for": "other", "deployment_environment": "production"}, "not 'other'"),
    ({"reserved_for": "LSMC"}, "deployment_environment is required"),
    ({"reserved_for": "LSMC", "deployment_environment": "development"}, "exactly three"),
    ({"reserved_for": "LSMC", "deployment_environment": "development",
      "approval_tokens": ["fixture-approval-one", "fixture-approval-two"]}, "exactly three"),
    ({"reserved_for": "LSMC", "deployment_environment": "development",
      "approval_tokens": ["fixture-approval-one", "fixture-approval-one", "fixture-approval-three"]}, "distinct"),
    ({"reserved_for": "LSMC", "deployment_environment": "development",
      "approval_tokens": ["one", "two", "three", "four"]}, "exactly three"),
])
def test_meridian_policy_rejections_are_preserved(authorization, message):
    with pytest.raises(EUIDGovernanceError, match=message):
        assert_registered_domain(
            "M", registry_metadata=DOMAIN_METADATA,
            governance_authorization=authorization,
        )


@pytest.mark.parametrize("authorization", [
    PRODUCTION_AUTHORIZATION,
    {"reserved_for": "LSMC", "deployment_environment": "development",
     "approval_tokens": ["fixture-approval-one", "fixture-approval-two", "fixture-approval-three"]},
])
def test_reserved_authorization_works_through_context_and_config(tmp_path, authorization):
    config = _config(tmp_path, authorization)
    cfg = get_db_config(config_path=config)
    assert cfg["governance_authorization"] == authorization
    context = GovernanceContext.load(
        domain_code=cfg["domain_code"], owner_repo_name=cfg["owner_repo_name"],
        domain_registry_path=cfg["domain_registry_path"],
        prefix_ownership_registry_path=cfg["prefix_ownership_registry_path"],
        governance_authorization=cfg["governance_authorization"],
    )
    assert context.require_prefix("ASY") == "example-owner"
    assert context.governance_authorization.to_dict() == authorization


def test_no_authorization_is_inferred_from_safety_registry_or_environment(tmp_path, monkeypatch):
    config = _config(tmp_path)
    monkeypatch.setenv("QEO_TAPDB_RESERVED_FOR", "LSMC")
    monkeypatch.setenv("QEO_TAPDB_GOVERNANCE_ENVIRONMENT", "production")
    with pytest.raises(EUIDGovernanceError, match="reserved_for is required"):
        get_db_config(config_path=config)


def test_unrestricted_domain_remains_usable_without_authorization(tmp_path):
    config = _config(tmp_path)
    root = yaml.safe_load(config.read_text())
    root["target"]["domain_code"] = "Z"
    config.write_text(yaml.safe_dump(root))
    cfg = get_db_config(config_path=config)
    assert cfg["domain_code"] == "Z"
    assert cfg["governance_authorization"] is None


@pytest.mark.parametrize("malformed", [
    "LSMC", [], {"owner": "LSMC"}, {"reserved_for": 1},
    {"deployment_environment": " "}, {"approval_tokens": "abc"},
    {"approval_tokens": None}, {"approval_tokens": [1]}, {"approval_tokens": [""]},
])
def test_structured_authorization_rejects_malformed_values(malformed):
    with pytest.raises(ValueError, match="governance_authorization"):
        GovernanceAuthorization.from_value(malformed)


def test_authorization_copies_tokens_and_does_not_show_them_in_repr():
    tokens = ["fixture-approval-one"]
    authorization = GovernanceAuthorization.from_value({"approval_tokens": tokens})
    tokens.append("fixture-approval-two")
    assert authorization.approval_tokens == ("fixture-approval-one",)
    assert "fixture-approval-one" not in repr(authorization)


@pytest.mark.parametrize("authorize_at_init", [False, True])
def test_public_config_controls_can_authorize_an_existing_rejected_config(tmp_path, authorize_at_init):
    domains, prefixes = _registries(tmp_path)
    config = tmp_path / "canonical.yaml"
    runner = CliRunner()
    args = [
        "--config", str(config), "db-config", "init",
        "--client-id", "qualification", "--database-name", "governance",
        "--owner-repo-name", "example-owner", "--domain-code", "M",
        "--domain-registry-path", str(domains),
        "--prefix-ownership-registry-path", str(prefixes),
        "--engine-type", "local", "--host", "localhost", "--user", "runtime",
        "--database", "qualification", "--schema-name", "governance",
    ]
    if authorize_at_init:
        args += ["--governance-authorization-json", json.dumps(PRODUCTION_AUTHORIZATION)]
    result = runner.invoke(framework_app, args)
    assert result.exit_code == 0, result.output
    before = yaml.safe_load(config.read_text())
    if authorize_at_init:
        assert get_db_config(config_path=config)["governance_authorization"] == PRODUCTION_AUTHORIZATION
    else:
        with pytest.raises(EUIDGovernanceError, match="reserved_for is required"):
            get_db_config(config_path=config)
    result = runner.invoke(framework_app, [
        "--config", str(config), "db-config", "update",
        "--governance-authorization-json", json.dumps(PRODUCTION_AUTHORIZATION),
    ])
    assert result.exit_code == 0, result.output
    after = yaml.safe_load(config.read_text())
    assert after["target"] == before["target"]
    assert {key: value for key, value in after["meta"].items() if key != "governance_authorization"} == {
        key: value for key, value in before["meta"].items() if key != "governance_authorization"
    }
    assert get_db_config(config_path=config)["governance_authorization"] == PRODUCTION_AUTHORIZATION
    # Unrelated updates preserve the explicit envelope, and malformed input
    # cannot partially replace the existing canonical config.
    result = runner.invoke(framework_app, ["--config", str(config), "db-config", "update", "--support-email", "support@example.test"])
    assert result.exit_code == 0, result.output
    saved = config.read_bytes()
    result = runner.invoke(framework_app, ["--config", str(config), "db-config", "update", "--governance-authorization-json", '"LSMC"'])
    assert result.exit_code != 0
    assert config.read_bytes() == saved
    assert get_db_config(config_path=config)["governance_authorization"] == PRODUCTION_AUTHORIZATION


@pytest.mark.parametrize("as_json", [False, True])
@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_dedicated_public_update_preserves_operator_config_without_admin(tmp_path, as_json, newline):
    from daylily_tapdb.cli._registry_v2 import policy_for_command

    config = _config(tmp_path)
    root = yaml.safe_load(config.read_text())
    assert "admin" not in root
    if as_json:
        raw = json.dumps(root, indent=2) + "\n"
    else:
        raw = "# Operator config: preserve comments and café text.\n" + config.read_text()
    config.write_bytes(raw.replace("\n", newline).encode("utf-8"))
    config.chmod(0o640)
    before = config.read_bytes()
    with pytest.raises(EUIDGovernanceError, match="reserved_for is required"):
        get_db_config(config_path=config)
    policy = policy_for_command("db-config", "set-governance-authorization")
    assert policy.mutates_state is True
    result = CliRunner().invoke(framework_app, [
        "--config", str(config), "db-config", "set-governance-authorization",
        "--authorization-json", json.dumps(PRODUCTION_AUTHORIZATION),
    ])
    assert result.exit_code == 0, result.output
    after = config.read_bytes()
    # Adding the object must be a single insertion, preserving every old byte.
    offset = next(index for index, pair in enumerate(zip(before, after)) if pair[0] != pair[1])
    added = len(after) - len(before)
    assert added > 0
    assert after[:offset] + after[offset + added:] == before
    assert config.stat().st_mode & 0o777 == 0o640
    updated = yaml.safe_load(after)
    assert "admin" not in updated
    assert updated["target"] == root["target"]
    assert get_db_config(config_path=config)["governance_authorization"] == PRODUCTION_AUTHORIZATION


def test_dedicated_update_replaces_existing_block_and_validates_before_write(tmp_path):
    from daylily_tapdb.cli.governance_config import set_governance_authorization

    config = _config(tmp_path, {"reserved_for": "LSMC"})
    config.write_text(config.read_text().replace(
        "    reserved_for: LSMC\n",
        "    reserved_for: LSMC\n  # Keep this metadata comment verbatim.\n",
    ))
    before = config.read_bytes()
    invalid = {"reserved_for": "LSMC", "deployment_environment": "development"}
    with pytest.raises(EUIDGovernanceError, match="exactly three"):
        set_governance_authorization(config, invalid)
    assert config.read_bytes() == before
    set_governance_authorization(config, PRODUCTION_AUTHORIZATION)
    assert "  # Keep this metadata comment verbatim.\n" in config.read_text()
    updated = yaml.safe_load(config.read_bytes())
    original = yaml.safe_load(before)
    original["meta"]["governance_authorization"] = PRODUCTION_AUTHORIZATION
    assert updated == original
    assert get_db_config(config_path=config)["governance_authorization"] == PRODUCTION_AUTHORIZATION
    root = yaml.safe_load(config.read_text())
    root["target"].pop("host")
    config.write_text(yaml.safe_dump(root))
    before = config.read_bytes()
    with pytest.raises(RuntimeError, match="host"):
        set_governance_authorization(config, PRODUCTION_AUTHORIZATION)
    assert config.read_bytes() == before
    assert not list(tmp_path.glob(".tapdb-governance-*"))


def test_dedicated_update_rejects_ambiguous_metadata_without_rewriting(tmp_path):
    from daylily_tapdb.cli.governance_config import set_governance_authorization

    config = _config(tmp_path)
    raw = config.read_text().replace("meta:\n", "meta: &shared_metadata\n")
    config.write_text(raw)
    with pytest.raises(ValueError, match="anchors or aliases"):
        set_governance_authorization(config, PRODUCTION_AUTHORIZATION)
    assert config.read_text() == raw


def test_gui_meridian_validation_reuses_explicit_config_authorization(tmp_path):
    from daylily_tapdb.gui.router import _meridian_validation_payload

    config = _config(tmp_path, PRODUCTION_AUTHORIZATION)
    result = _meridian_validation_payload(config_path=str(config), euid="", prefix="ASY")
    assert result["domain_code"] == "M"
    assert result["prefix_owner"] == "example-owner"
    assert result["governance"].governance_authorization.to_dict() == PRODUCTION_AUTHORIZATION


def test_direct_template_seed_rejects_missing_authorization_before_session_use(tmp_path):
    domains, prefixes = _registries(tmp_path)
    with pytest.raises(EUIDGovernanceError, match="reserved_for is required"):
        loader.seed_templates(
            object(), [], overwrite=False, core_config_dir=tmp_path / "core",
            domain_code="M", owner_repo_name="example-owner",
            domain_registry_path=domains, prefix_registry_path=prefixes,
        )


def test_seed_authorization_does_not_relax_prefix_ownership(tmp_path):
    domains, prefixes = _registries(tmp_path)
    kwargs = dict(
        domain_code="M", core_config_dir=tmp_path / "core",
        domain_registry_path=domains, prefix_registry_path=prefixes,
        governance_authorization=PRODUCTION_AUTHORIZATION,
    )
    loader._validate_seed_ownership([{"instance_prefix": "ASY"}], owner_repo_name="example-owner", **kwargs)
    with pytest.raises(ValueError, match="claimed by"):
        loader._validate_seed_ownership([{"instance_prefix": "ASY"}], owner_repo_name="other-owner", **kwargs)


@pytest.mark.parametrize("dry_run", [False, True])
def test_repository_import_requires_authorization_and_carries_it_to_seed(tmp_path, monkeypatch, dry_run):
    domains, prefixes = _registries(tmp_path)
    pack = tmp_path / "pack.json"
    payload = {"templates": [{
        "category": "assay", "type": "sequencing", "subtype": "short_read",
        "version": "1.0", "instance_prefix": "ASY",
    }]}
    monkeypatch.setattr(repository, "read_repository_pack", lambda _: (pack, payload, b"fixture"))
    monkeypatch.setattr(repository, "_verified_receipt", lambda *args, **kwargs: None)
    kwargs = dict(
        domain_code="M", owner_repo_name="example-owner",
        domain_registry_path=domains, prefix_registry_path=prefixes, dry_run=dry_run,
    )
    with pytest.raises(EUIDGovernanceError, match="reserved_for is required"):
        repository.import_repository_pack(object(), pack, **kwargs)
    observed = []

    def seed(_session, _templates, **seed_kwargs):
        observed.append(seed_kwargs["governance_authorization"])
        return SimpleNamespace(inserted=1, skipped=0)

    monkeypatch.setattr(repository, "seed_templates", seed)
    session = SimpleNamespace(execute=lambda _: SimpleNamespace(scalars=lambda: []))
    result = repository.import_repository_pack(
        session, pack, governance_authorization=PRODUCTION_AUTHORIZATION, **kwargs,
    )
    assert result.dry_run is dry_run
    assert observed == ([] if dry_run else [PRODUCTION_AUTHORIZATION])


def test_cli_template_import_forwards_canonical_authorization(tmp_path, monkeypatch):
    from daylily_tapdb.cli import templates as templates_cli

    config = _config(tmp_path, PRODUCTION_AUTHORIZATION)
    monkeypatch.setattr(templates_cli, "get_db_config", lambda: get_db_config(config_path=config))
    observed = []

    @contextmanager
    def scope(**kwargs):
        yield object()

    @contextmanager
    def connection(*args, **kwargs):
        yield SimpleNamespace(session_scope=scope)

    def import_pack(*args, **kwargs):
        observed.append(kwargs["governance_authorization"])
        return repository.RepositoryImportResult(True, 0, 0, 0, (), "fixture-checksum")

    monkeypatch.setattr(templates_cli, "_tapdb_connection_for_env", connection)
    monkeypatch.setattr(templates_cli, "import_repository_pack", import_pack)
    monkeypatch.setattr(templates_cli, "_emit", lambda _: None)
    templates_cli.templates_import(repository_pack=tmp_path / "pack.json", apply=False, actor="fixture-actor")
    assert observed == [PRODUCTION_AUTHORIZATION]
