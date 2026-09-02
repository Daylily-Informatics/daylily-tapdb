"""Release-level contracts that must hold for TapDB 9.2.0."""

from __future__ import annotations

import hashlib
import importlib.metadata
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
LOCK = ROOT / "uv.lock"
ACTIVE_GUIDES = (
    "README.md",
    "AI_DIRECTIVE.md",
    "docs/README.md",
    "docs/architecture.md",
    "docs/consumer-discoverability-guide.md",
    "docs/dag_spec.md",
    "docs/integration-and-embedding.md",
    "docs/runtime-and-cli.md",
    "docs/tapdb_gui_inclusion.md",
)


def _pyproject() -> dict:
    with PYPROJECT.open("rb") as handle:
        return tomllib.load(handle)


def test_meridian_048_is_exact_and_uses_verified_artifacts() -> None:
    project = _pyproject()
    dependencies = project["project"]["dependencies"]
    assert "meridian-euid==0.4.8" in dependencies
    assert not any(
        dependency.startswith("meridian-euid") and dependency != "meridian-euid==0.4.8"
        for dependency in dependencies
    )

    lock = LOCK.read_text(encoding="utf-8")
    assert 'name = "meridian-euid"\nversion = "0.4.8"' in lock
    assert "c36e96b36c78da427b200bda7f76c23bd7db74dbe32b54a473134e2f1f61b946" in lock
    assert "54a67bf831088f52c395119d2cacac9354a60c3a1b084817d9b1d90f718a4b3d" in lock


def test_every_meridian_api_imported_by_tapdb_is_available_at_048() -> None:
    from meridian_euid import (
        MERIDIAN_REGISTRY_INDEX_URL,
        MERIDIAN_REGISTRY_REPOSITORY,
        MERIDIAN_REGISTRY_VERSION,
        assert_registered_domain,
        compute_check_character,
        load_domain_registry,
        load_domain_registry_metadata,
        load_prefix_ownership_registry,
        validate_issuer_app_code,
        validate_registries_consistent,
    )

    assert importlib.metadata.version("meridian-euid") == "0.4.8"
    assert all(
        isinstance(value, str) and value
        for value in (
            MERIDIAN_REGISTRY_INDEX_URL,
            MERIDIAN_REGISTRY_REPOSITORY,
            MERIDIAN_REGISTRY_VERSION,
        )
    )
    assert all(
        callable(value)
        for value in (
            assert_registered_domain,
            compute_check_character,
            load_domain_registry,
            load_domain_registry_metadata,
            load_prefix_ownership_registry,
            validate_issuer_app_code,
            validate_registries_consistent,
        )
    )


def test_release_quality_configuration_is_strict() -> None:
    project = _pyproject()
    coverage_run = project["tool"]["coverage"]["run"]
    coverage_report = project["tool"]["coverage"]["report"]

    assert coverage_run["branch"] is True
    assert "omit" not in coverage_run
    assert coverage_report["fail_under"] >= 90
    mypy = project["tool"]["mypy"]
    assert mypy["python_version"] == "3.12"
    assert mypy["follow_imports"] == "skip"
    assert len(mypy["files"]) == 12
    assert project["project"]["urls"]["Repository"].endswith(
        "/Daylily-Informatics/daylily-tapdb.git"
    )
    assert "docs/*.md" in project["tool"]["setuptools"]["data-files"]["docs"]


def test_dag_spec_is_preserved_exactly() -> None:
    spec = (
        ROOT / "docs/plans/20260901T100631Z_kahlo_global_dag_tapdb_eligibility_spec.md"
    )
    assert spec.is_file()
    assert (
        hashlib.sha256(spec.read_bytes()).hexdigest()
        == "d5e8593b6bc85256924db76630d3014882b4a65389a5154782ef7a2d087ca8eb"
    )


def test_consumer_guide_and_readme_are_public_safe() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    guide_path = ROOT / "docs/consumer-discoverability-guide.md"
    assert guide_path.is_file()
    guide = guide_path.read_text(encoding="utf-8")

    assert "consumer-discoverability-guide.md" in readme
    assert "<persisted-euid>" in guide
    assert "/Users/" not in readme
    assert "/Users/" not in guide
    assert "meridian-euid==0.4.8" in readme


def test_active_guides_use_the_current_repository_identity() -> None:
    for relative_path in ACTIVE_GUIDES:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "tapdb-core" not in text, relative_path


def test_ci_runs_the_complete_release_matrix() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    for required in (
        "postgres:17",
        "ruff check",
        "ruff format",
        "mypy",
        "bandit",
        "detect-secrets",
        "TAPDB_RUN_DOCS_LOCAL",
        "--cov=daylily_tapdb",
        "--cov=admin",
        "--cov-branch",
        "--cov-report=json:coverage.json",
        "verify_changed_coverage.py",
        "verify_wheel_assets.py",
        "python -m build",
    ):
        assert required in workflow
    assert "--deselect" not in workflow
