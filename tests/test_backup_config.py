"""The `backup:` config section: seeding, updating, and normalized loading."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db_config import get_backup_settings

runner = CliRunner()


def _write_registries(root: Path) -> tuple[Path, Path]:
    domain_registry = root / "domain_code_registry.json"
    prefix_registry = root / "prefix_ownership_registry.json"
    domain_registry.write_text(
        '{"version":"0.4.0","domains":{"Z":{"name":"test-localhost"}}}\n',
        encoding="utf-8",
    )
    prefix_registry.write_text(
        (
            '{"version":"0.4.0","ownership":{"Z":{'
            '"TPX":{"issuer_app_code":"daylily-tapdb"},'
            '"EDG":{"issuer_app_code":"daylily-tapdb"},'
            '"ADT":{"issuer_app_code":"daylily-tapdb"},'
            '"SYS":{"issuer_app_code":"daylily-tapdb"},'
            '"MSG":{"issuer_app_code":"daylily-tapdb"}}}}\n'
        ),
        encoding="utf-8",
    )
    return domain_registry, prefix_registry


def _init_config(tmp_path: Path) -> Path:
    cfg_path = tmp_path / "tapdb-config.yaml"
    domain_registry, prefix_registry = _write_registries(tmp_path)
    result = runner.invoke(
        app,
        [
            "--config",
            str(cfg_path),
            "config",
            "init",
            "--client-id",
            "alpha",
            "--database-name",
            "beta",
            "--owner-repo-name",
            "daylily-tapdb",
            "--domain-code",
            "Z",
            "--domain-registry-path",
            str(domain_registry),
            "--prefix-ownership-registry-path",
            str(prefix_registry),
            "--engine-type",
            "local",
            "--host",
            "localhost",
            "--port",
            "5533",
            "--ui-port",
            "8911",
            "--user",
            "tapdb",
            "--database",
            "tapdb_shared",
            "--schema-name",
            "tapdb_alpha_beta",
            "--safety-tier",
            "shared",
            "--destructive-operations",
            "confirm_required",
        ],
    )
    assert result.exit_code == 0, result.output
    return cfg_path


def _update(cfg_path: Path, *args: str):
    return runner.invoke(app, ["--config", str(cfg_path), "config", "update", *args])


def _failure_text(result) -> str:
    """Return the message a failed invocation reported.

    Config errors surface as raised RuntimeErrors rather than printed output,
    so the exception is where the message lives.
    """
    return f"{result.output}{result.exception or ''}"


@pytest.fixture(autouse=True)
def _clean_context():
    clear_cli_context()
    yield
    clear_cli_context()


def _raw(cfg_path: Path) -> dict:
    return yaml.safe_load(cfg_path.read_text(encoding="utf-8"))


def test_config_init_seeds_the_backup_section(tmp_path: Path):
    cfg_path = _init_config(tmp_path)

    backup = _raw(cfg_path)["backup"]

    assert backup["storage"] == {"uri": ""}
    assert backup["retention"] == {"keep_last": 30}
    assert backup["encryption"] == {"mode": "none"}
    assert backup["signing"] == {"mode": "none", "kms_key_arn": ""}
    assert backup["provider_snapshots"] == {
        "enabled": False,
        "cluster_identifier": "",
    }
    assert backup["rehearsal"] == {"database_prefix": "tapdb_rehearsal"}
    assert backup["expected_interval_hours"] == 0
    assert backup["receipt_mirror"] == {}


def test_settings_load_with_defaults(tmp_path: Path):
    cfg_path = _init_config(tmp_path)
    set_cli_context(config_path=cfg_path)

    settings = get_backup_settings()

    assert settings["storage_uri"] == ""
    assert settings["keep_last"] == 30
    assert settings["signing_mode"] == "none"
    assert settings["expected_interval_hours"] == 0.0
    assert settings["rehearsal_database_prefix"] == "tapdb_rehearsal"
    assert settings["receipt_mirror"] == {}
    assert settings["config_dir"] == str(cfg_path.parent)


def test_a_config_without_a_backup_section_still_loads(tmp_path: Path):
    # Configs written before this release must keep working -- the backup
    # subsystem is additive for consumers pinned to 9.0.x.
    cfg_path = _init_config(tmp_path)
    root = _raw(cfg_path)
    root.pop("backup")
    cfg_path.write_text(yaml.safe_dump(root), encoding="utf-8")
    cfg_path.chmod(0o600)
    set_cli_context(config_path=cfg_path)

    settings = get_backup_settings()

    assert settings["storage_uri"] == ""
    assert settings["keep_last"] == 30


def test_config_update_sets_storage_and_cadence(tmp_path: Path):
    cfg_path = _init_config(tmp_path)

    result = _update(
        cfg_path,
        "--backup-storage-uri",
        "s3://acme-backups/tapdb",
        "--backup-expected-interval-hours",
        "24",
        "--backup-keep-last",
        "14",
    )
    assert result.exit_code == 0, result.output

    set_cli_context(config_path=cfg_path)
    settings = get_backup_settings()

    assert settings["storage_uri"] == "s3://acme-backups/tapdb"
    assert settings["expected_interval_hours"] == 24.0
    assert settings["keep_last"] == 14


def test_config_update_sets_rehearsal_prefix_and_mirror(tmp_path: Path):
    cfg_path = _init_config(tmp_path)

    result = _update(
        cfg_path,
        "--backup-rehearsal-database-prefix",
        "tapdb_drill",
        "--backup-receipt-mirror-uri",
        "s3://audit/receipts",
    )
    assert result.exit_code == 0, result.output

    set_cli_context(config_path=cfg_path)
    settings = get_backup_settings()

    assert settings["rehearsal_database_prefix"] == "tapdb_drill"
    assert settings["receipt_mirror"] == {"uri": "s3://audit/receipts"}


@pytest.mark.parametrize(
    "flag, value",
    [
        ("--backup-storage-uri", "s3://KEY:SECRET@bucket/prefix"),
        ("--backup-receipt-mirror-uri", "s3://KEY:SECRET@bucket/receipts"),
    ],
)
def test_credential_bearing_uris_are_rejected_at_write_time(
    tmp_path: Path, flag: str, value: str
):
    # Catching this at write time keeps a bad value from sitting in the config
    # until a backup runs -- and manifests carry storage URIs off-box.
    cfg_path = _init_config(tmp_path)

    result = _update(cfg_path, flag, value)

    assert result.exit_code != 0
    assert "credential" in _failure_text(result).lower()
    # The rejected value must not have reached the file.
    assert "SECRET" not in _raw(cfg_path)["backup"]["storage"]["uri"]
    assert _raw(cfg_path)["backup"]["receipt_mirror"] == {}


def test_credential_bearing_uri_in_a_hand_edited_config_is_rejected_on_load(
    tmp_path: Path,
):
    cfg_path = _init_config(tmp_path)
    root = _raw(cfg_path)
    root["backup"]["storage"]["uri"] = "s3://KEY:SECRET@bucket/prefix"
    cfg_path.write_text(yaml.safe_dump(root), encoding="utf-8")
    cfg_path.chmod(0o600)
    set_cli_context(config_path=cfg_path)

    with pytest.raises(RuntimeError, match="credential"):
        get_backup_settings()


def test_negative_cadence_is_rejected(tmp_path: Path):
    cfg_path = _init_config(tmp_path)

    result = _update(cfg_path, "--backup-expected-interval-hours", "-1")

    assert result.exit_code != 0


def test_keep_last_below_one_is_rejected(tmp_path: Path):
    cfg_path = _init_config(tmp_path)

    result = _update(cfg_path, "--backup-keep-last", "0")

    assert result.exit_code != 0


def test_unsupported_signing_mode_is_rejected(tmp_path: Path):
    cfg_path = _init_config(tmp_path)
    root = _raw(cfg_path)
    root["backup"]["signing"]["mode"] = "pgp"
    cfg_path.write_text(yaml.safe_dump(root), encoding="utf-8")
    cfg_path.chmod(0o600)
    set_cli_context(config_path=cfg_path)

    with pytest.raises(RuntimeError, match="signing.mode"):
        get_backup_settings()


def test_reinitializing_preserves_operator_set_backup_values(tmp_path: Path):
    # Re-running `config init` must not silently discard a storage destination.
    cfg_path = _init_config(tmp_path)
    assert _update(cfg_path, "--backup-storage-uri", "s3://acme/tapdb").exit_code == 0

    domain_registry, prefix_registry = _write_registries(tmp_path)
    result = runner.invoke(
        app,
        [
            "--config",
            str(cfg_path),
            "config",
            "init",
            "--client-id",
            "alpha",
            "--database-name",
            "beta",
            "--owner-repo-name",
            "daylily-tapdb",
            "--domain-code",
            "Z",
            "--domain-registry-path",
            str(domain_registry),
            "--prefix-ownership-registry-path",
            str(prefix_registry),
            "--engine-type",
            "local",
            "--host",
            "localhost",
            "--port",
            "5533",
            "--ui-port",
            "8911",
            "--user",
            "tapdb",
            "--database",
            "tapdb_shared",
            "--schema-name",
            "tapdb_alpha_beta",
            "--safety-tier",
            "shared",
            "--destructive-operations",
            "confirm_required",
        ],
    )
    assert result.exit_code == 0, result.output

    assert _raw(cfg_path)["backup"]["storage"]["uri"] == "s3://acme/tapdb"


def test_update_with_no_changes_still_errors(tmp_path: Path):
    cfg_path = _init_config(tmp_path)

    result = _update(cfg_path)

    assert result.exit_code != 0
    assert "No config changes requested" in _failure_text(result)


def test_config_update_sets_the_rehearsal_cadence(tmp_path: Path):
    """Without a flag this would be the only backup setting that is YAML-only.

    Also guards the ``backup_changed`` wiring: an option that parses but is
    never marked as a change fails with "No config changes requested", which
    reads as a user error rather than a missing line in the command.
    """
    cfg_path = _init_config(tmp_path)

    result = _update(cfg_path, "--backup-expected-rehearsal-interval-days", "90")
    assert result.exit_code == 0, result.output

    set_cli_context(config_path=cfg_path)
    settings = get_backup_settings()

    assert settings["expected_rehearsal_interval_days"] == 90.0
    # Top-level under `backup:`, not nested under `backup.rehearsal`.
    raw = yaml.safe_load(Path(cfg_path).read_text())
    assert raw["backup"]["expected_rehearsal_interval_days"] == 90.0
    assert "expected_rehearsal_interval_days" not in raw["backup"].get("rehearsal", {})


def test_a_negative_rehearsal_cadence_is_rejected(tmp_path: Path):
    cfg_path = _init_config(tmp_path)

    result = _update(cfg_path, "--backup-expected-rehearsal-interval-days", "-1")

    assert result.exit_code != 0


@pytest.mark.parametrize(
    "raw, field",
    [
        ("24h", "expected_interval_hours"),
        (".inf", "expected_interval_hours"),
        (".nan", "expected_interval_hours"),
    ],
)
def test_a_broken_cadence_is_reported_not_silently_defaulted(
    tmp_path: Path, raw, field
):
    """`.inf` and `.nan` are the dangerous ones, because they parse.

    `_float` accepts both, so the value survives as a number and
    `cadence_configured` reports the alarm as armed -- while `age_hours > inf`
    is unsatisfiable and every comparison against `nan` is False. The staleness
    detector is silently disarmed by a config that looks deliberate.
    """
    cfg_path = _init_config(tmp_path)
    data = yaml.safe_load(Path(cfg_path).read_text())
    data.setdefault("backup", {})["expected_interval_hours"] = yaml.safe_load(
        f"x: {raw}"
    )["x"]
    Path(cfg_path).write_text(yaml.safe_dump(data))

    set_cli_context(config_path=cfg_path)
    settings = get_backup_settings()

    assert field in settings["invalid_fields"], settings["invalid_fields"]


@pytest.mark.parametrize("field", ["keep_last", "health_verify_max_bytes"])
def test_a_non_integer_byte_or_count_setting_is_reported(tmp_path: Path, field):
    """Both are resolved by `_int`, so both need integral validation.

    A float that `int()` rejects falls back to the default silently -- on
    settings that govern deletion and how much health reads.
    """
    cfg_path = _init_config(tmp_path)
    data = yaml.safe_load(Path(cfg_path).read_text())
    backup = data.setdefault("backup", {})
    if field == "keep_last":
        backup.setdefault("retention", {})["keep_last"] = 3.7
    else:
        backup[field] = 3.7
    Path(cfg_path).write_text(yaml.safe_dump(data))

    set_cli_context(config_path=cfg_path)
    settings = get_backup_settings()

    assert field in settings["invalid_fields"], settings["invalid_fields"]


@pytest.mark.parametrize("raw", ["3.7", "1e3", "thirty"])
def test_a_non_integer_keep_last_is_reported(tmp_path: Path, raw):
    """Validated with the parser that resolves it, not a laxer one.

    `keep_last` is resolved by `_int`, so `3.7` raises and falls back to the
    default of 30 -- a silent substitution on the setting that governs
    deletion. A float-based check would have called `3.7` perfectly valid.
    """
    cfg_path = _init_config(tmp_path)
    data = yaml.safe_load(Path(cfg_path).read_text())
    data.setdefault("backup", {}).setdefault("retention", {})["keep_last"] = (
        yaml.safe_load(f"x: {raw}")["x"]
    )
    Path(cfg_path).write_text(yaml.safe_dump(data))

    set_cli_context(config_path=cfg_path)
    settings = get_backup_settings()

    assert "keep_last" in settings["invalid_fields"]
    assert settings["keep_last"] == 30  # the silent fallback, now surfaced


def test_valid_numbers_are_not_flagged(tmp_path: Path):
    cfg_path = _init_config(tmp_path)

    result = _update(
        cfg_path,
        "--backup-expected-interval-hours",
        "24",
        "--backup-keep-last",
        "7",
    )
    assert result.exit_code == 0, result.output

    set_cli_context(config_path=cfg_path)
    settings = get_backup_settings()

    assert settings["invalid_fields"] == []
