"""Static contracts that keep README command maps aligned with example files."""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_readme_exists_and_points_to_examples() -> None:
    readme = REPO_ROOT / "README.md"
    assert readme.exists(), "README.md should be present at the repo root."

    text = readme.read_text(encoding="utf-8")
    assert "examples/readme/00_smoke.sh" in text
    assert "examples/readme/10_bootstrap_local.sh" in text
    assert "examples/readme/20_python_api.py" in text
    assert "source ./activate" in text
    assert "tapdb --config <path> ..." in text
    assert "--json info" in text
    assert "lsmc-bio/meridian-registry" in text
    assert "meridian-euid domain-check Q" in text


def test_examples_contain_the_canonical_commands() -> None:
    smoke = (REPO_ROOT / "examples" / "readme" / "00_smoke.sh").read_text(
        encoding="utf-8"
    )
    bootstrap = (REPO_ROOT / "examples" / "readme" / "10_bootstrap_local.sh").read_text(
        encoding="utf-8"
    )
    python_api = (REPO_ROOT / "examples" / "readme" / "20_python_api.py").read_text(
        encoding="utf-8"
    )

    assert "tapdb --help" in smoke
    assert "config init" in bootstrap
    assert "--env" not in bootstrap
    assert "bootstrap local --no-gui" in bootstrap
    assert "TAPDBConnection" in python_api
    assert "TemplateManager" in python_api
    assert "InstanceFactory" in python_api


def test_activate_banner_does_not_advertise_legacy_env_selectors() -> None:
    activate = (REPO_ROOT / "activate").read_text(encoding="utf-8")

    assert "--env" not in activate
    assert "dev | test | prod" not in activate
    assert "<env>" not in activate
    assert (
        "tapdb --config ~/.config/tapdb/<client>/<database>/tapdb-config.yaml"
        in activate
    )


def test_embeddable_gui_docs_expose_the_only_web_stack_with_full_parity() -> None:
    integration = (REPO_ROOT / "docs" / "integration-and-embedding.md").read_text(
        encoding="utf-8"
    )
    inclusion = (REPO_ROOT / "docs" / "tapdb_gui_inclusion.md").read_text(
        encoding="utf-8"
    )

    combined = integration + "\n" + inclusion
    assert "create_tapdb_gui_app" in combined
    assert 'app.mount(\n    "/tapdb"' in combined
    assert 'config_path="/abs/path/to/tapdb-config.yaml"' in combined
    assert "Dayhoff-Style Host Example" in integration
    assert "does not require mutating a Dayhoff repo" in integration
    assert "/tapdb/api/create/{template_euid}" in combined
    assert "/tapdb/api/object/{euid}/lineage" in combined
    assert "/tapdb/api/admin/readiness" in combined
    assert "/tapdb/api/admin/backups" in combined
    assert "full former-admin feature parity" in combined.lower()
    assert "rich graph" in combined.lower()
    assert "create_tapdb_web_app(...)` remains available" not in integration
    assert "/tapdb/api/object/{euid}/external-links" not in combined


# ---------------------------------------------------------------------------
# The backup runbook's commands must actually exist
# ---------------------------------------------------------------------------
#
# An operator runbook is read during an incident, when nobody has spare
# attention to debug a typo. The first draft of this one shipped
# `tapdb backup list --json`, which is rejected outright -- `--json` is a
# global flag and has to precede the subcommand. These tests check every
# documented invocation against the real CLI so the runbook cannot drift away
# from the tool it documents.

RUNBOOK = REPO_ROOT / "docs" / "backup-and-recovery.md"


def _documented_commands() -> list[list[str]]:
    """Every `tapdb ...` invocation in the runbook, as argv lists."""
    import re

    commands: list[list[str]] = []
    for raw in RUNBOOK.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line.startswith("tapdb "):
            continue
        line = line.split("#", 1)[0]  # trailing comment
        line = line.split("|", 1)[0]  # shell pipeline
        line = line.rstrip("\\").strip()  # line continuation
        tokens = [t for t in re.split(r"\s+", line) if t][1:]
        if tokens:
            commands.append(tokens)
    return commands


def _root():
    """The click tree for the real `tapdb` entry point.

    `main()` runs from `spec`, and `framework_app` is that same spec built into
    a Typer app -- it is what carries the global `--json`/`--dry-run` flags.
    Introspecting the inner `app` instead would miss them and wrongly report
    every global flag as invalid.
    """
    import typer.main

    from daylily_tapdb.cli import framework_app

    assert framework_app is not None, "CLI framework app failed to build"
    return typer.main.get_command(framework_app)


def _takes_a_value(node, flag: str) -> bool:
    for param in node.params:
        if flag in param.opts:
            return not getattr(param, "is_flag", False)
    return False


def _walk(argv: list[str]):
    """Resolve argv to (bad_flags, missing_command).

    Each flag is checked against the options of the command **in effect where
    it is written**, not against a union of every level. That distinction is
    the whole point: `--json` is a real option, but only on the root group, so
    `tapdb backup list --json` is an error while `tapdb --json backup list` is
    correct. A union-based check calls both of them fine and misses the bug
    this test exists to catch.

    Option *values* are skipped, so `--backup-id <id>` does not make `<id>`
    look like a subcommand.
    """
    node = _root()
    bad_flags: list[str] = []

    index = 0
    while index < len(argv):
        token = argv[index]
        if token.startswith("-"):
            flag = token.split("=", 1)[0]
            here = {opt for param in node.params for opt in param.opts}
            if flag not in here:
                bad_flags.append(flag)
            elif _takes_a_value(node, flag) and "=" not in token:
                index += 1  # skip its value
            index += 1
            continue
        child = node.get_command(None, token) if hasattr(node, "get_command") else None
        if child is None:
            return bad_flags, token
        node = child
        index += 1
    return bad_flags, None


def test_the_runbook_documents_some_commands() -> None:
    # Guards the guard: an empty list would make the checks below vacuous.
    assert len(_documented_commands()) >= 8


def test_every_runbook_command_exists() -> None:
    """Each documented subcommand path resolves to a real CLI command."""
    missing = []
    for argv in _documented_commands():
        _bad, unknown = _walk(argv)
        if unknown is not None:
            missing.append(f"tapdb {' '.join(argv)} (no such command: {unknown})")
    assert missing == [], missing


def test_every_runbook_flag_is_accepted_where_it_is_written() -> None:
    """Flags must be valid *at the position the runbook puts them*.

    This is the check that catches `tapdb backup list --json`: `--json` is
    real, but only on the root group, so writing it after the subcommand is an
    error an operator hits immediately.
    """
    problems: list[str] = []
    for argv in _documented_commands():
        bad, _unknown = _walk(argv)
        for flag in bad:
            problems.append(f"tapdb {' '.join(argv)} -> {flag} not accepted here")
    assert problems == [], problems


# ---------------------------------------------------------------------------
# The runbook's config keys must be the keys the loader reads
# ---------------------------------------------------------------------------
#
# The runbook first documented the *resolved settings* keys (`storage_uri`,
# `keep_last`, ...) as though they were YAML. The loader reads a nested shape
# (`backup.storage.uri`, `backup.retention.keep_last`), and unknown keys are
# silently ignored -- so an operator following the docs to point backups at S3
# got them written to the local config directory, with no warning at all.
#
# `test_every_runbook_command_exists` validates documented *commands*; nothing
# validated documented *config*. This does.


def _documented_backup_yaml_paths() -> list[str]:
    """Every `backup.*` path the runbook's config table documents."""
    import re

    text = RUNBOOK.read_text(encoding="utf-8")
    section = text[text.index("## 10. Configuration") :]
    section = section[: section.index("\n## ")]
    return sorted(set(re.findall(r"`(backup\.[a-z_.]+)`", section)))


def test_the_runbook_documents_backup_config_paths() -> None:
    # Guards the guard.
    assert len(_documented_backup_yaml_paths()) >= 6


def test_every_documented_backup_config_path_actually_takes_effect(tmp_path) -> None:
    """Write the documented YAML, and assert the loader honours each value.

    Round-trip rather than key-comparison: a key can exist in the loader and
    still be read from a different place in the file, which is exactly the
    failure this replaces.
    """
    import yaml

    from daylily_tapdb.cli.db_config import get_backup_settings

    config = tmp_path / "tapdb-config.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "meta": {
                    "config_version": 4,
                    "client_id": "docs",
                    "database_name": "check",
                    "owner_repo_name": "daylily-tapdb",
                    "domain_registry_path": str(
                        REPO_ROOT / "daylily_tapdb/etc/domain_code_registry.json"
                    ),
                    "prefix_ownership_registry_path": str(
                        REPO_ROOT / "daylily_tapdb/etc/prefix_ownership_registry.json"
                    ),
                },
                "target": {
                    "engine_type": "local",
                    "host": "localhost",
                    "port": "5599",
                    "ui_port": "8999",
                    "domain_code": "Z",
                    "user": "nobody",
                    "password": "",
                    "database": "tapdb_docs_check",
                    "schema_name": "tapdb_docs_check",
                },
                "safety": {
                    "safety_tier": "local",
                    "destructive_operations": "confirm_required",
                },
                # Exactly the shape the runbook prints.
                "backup": {
                    "storage": {"uri": "file:///tmp/docs-check-store"},
                    "retention": {"keep_last": 90},
                    "encryption": {"mode": "none"},
                    "signing": {"mode": "none", "kms_key_arn": ""},
                    "provider_snapshots": {
                        "enabled": True,
                        "cluster_identifier": "docs-cluster",
                    },
                    "rehearsal": {"database_prefix": "docs_rehearsal"},
                    "expected_interval_hours": 12,
                    "expected_rehearsal_interval_days": 45,
                    "health_verify_max_bytes": 2048,
                    "receipt_mirror": {"uri": "file:///tmp/docs-check-mirror"},
                },
            }
        ),
        encoding="utf-8",
    )
    config.chmod(0o600)

    settings = get_backup_settings(config_path=str(config))

    # Documented YAML path -> (settings key, expected value).
    #
    # Iterated, not hardcoded. The previous version listed nine asserts and
    # never consulted `_documented_backup_yaml_paths()`, so documenting a new
    # key and mis-wiring it into the loader passed CI in silence -- the exact
    # failure the docstring above claims to prevent. The closure assertion
    # below is what makes that impossible: a newly documented path with no
    # entry here fails, rather than going unchecked.
    expected: dict[str, tuple[str, object]] = {
        "backup.storage.uri": ("storage_uri", "file:///tmp/docs-check-store"),
        "backup.retention.keep_last": ("keep_last", 90),
        "backup.expected_interval_hours": ("expected_interval_hours", 12),
        "backup.expected_rehearsal_interval_days": (
            "expected_rehearsal_interval_days",
            45,
        ),
        "backup.health_verify_max_bytes": ("health_verify_max_bytes", 2048),
        "backup.encryption.mode": ("encryption_mode", "none"),
        "backup.signing.mode": ("signing_mode", "none"),
        "backup.provider_snapshots.enabled": ("provider_snapshots_enabled", True),
        "backup.rehearsal.database_prefix": (
            "rehearsal_database_prefix",
            "docs_rehearsal",
        ),
        "backup.receipt_mirror": ("receipt_mirror", "file:///tmp/docs-check-mirror"),
    }

    documented = set(_documented_backup_yaml_paths())
    assert documented == set(expected), (
        "documented backup config paths and round-trip coverage have diverged; "
        f"undocumented={sorted(set(expected) - documented)} "
        f"unchecked={sorted(documented - set(expected))}"
    )

    for yaml_path, (settings_key, want) in sorted(expected.items()):
        got = settings[settings_key]
        if settings_key == "receipt_mirror":
            got = (got or {}).get("uri")
        assert got == want, f"{yaml_path} did not reach settings[{settings_key!r}]"

    # Also pinned separately, because provider_snapshots documents two values
    # under one backticked path and the regex only captures the first.
    assert settings["provider_snapshots_cluster_identifier"] == "docs-cluster"


def test_a_flat_backup_key_does_not_silently_work(tmp_path) -> None:
    """Pin the trap itself, so the docs can never drift back to the flat shape.

    If a future loader accepted `storage_uri` at the top of `backup:`, this
    fails and the docs table should be updated -- rather than the docs quietly
    describing one shape while the loader reads another.
    """
    import yaml

    from daylily_tapdb.cli.db_config import get_backup_settings

    config = tmp_path / "tapdb-config.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "meta": {
                    "config_version": 4,
                    "client_id": "docs",
                    "database_name": "check",
                    "owner_repo_name": "daylily-tapdb",
                    "domain_registry_path": str(
                        REPO_ROOT / "daylily_tapdb/etc/domain_code_registry.json"
                    ),
                    "prefix_ownership_registry_path": str(
                        REPO_ROOT / "daylily_tapdb/etc/prefix_ownership_registry.json"
                    ),
                },
                "target": {
                    "engine_type": "local",
                    "host": "localhost",
                    "port": "5599",
                    "ui_port": "8999",
                    "domain_code": "Z",
                    "user": "nobody",
                    "password": "",
                    "database": "tapdb_docs_check",
                    "schema_name": "tapdb_docs_check",
                },
                "safety": {
                    "safety_tier": "local",
                    "destructive_operations": "confirm_required",
                },
                "backup": {"storage_uri": "s3://wrong-shape/nope", "keep_last": 90},
            }
        ),
        encoding="utf-8",
    )
    config.chmod(0o600)

    settings = get_backup_settings(config_path=str(config))

    # Pinned to the defaults, not merely "not the flat value". `!=` passes for
    # any value at all, including a correctly-read one -- so it could not tell
    # a rejected flat key from an accepted one.
    assert settings["storage_uri"] == ""
    assert settings["keep_last"] == 30
