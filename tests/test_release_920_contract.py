"""Release-level contracts that must hold for the TapDB 10.1 candidate."""

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
    "docs/backup-and-recovery.md",
    "docs/consumer-discoverability-guide.md",
    "docs/dag_spec.md",
    "docs/external-references-and-federation.md",
    "docs/integration-and-embedding.md",
    "docs/repository-review.md",
    "docs/runtime-and-cli.md",
    "docs/service-readiness.md",
    "docs/tapdb_gui_inclusion.md",
    "docs/template-authoring.md",
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
    assert len(mypy["files"]) == 24
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


def test_consumer_and_service_readiness_guides_are_public_safe() -> None:
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    directive = (ROOT / "AI_DIRECTIVE.md").read_text(encoding="utf-8")
    guide_path = ROOT / "docs/consumer-discoverability-guide.md"
    readiness_path = ROOT / "docs/service-readiness.md"
    assert guide_path.is_file()
    assert readiness_path.is_file()
    guide = guide_path.read_text(encoding="utf-8")
    readiness = readiness_path.read_text(encoding="utf-8")

    assert "consumer-discoverability-guide.md" in readme
    assert "service-readiness.md" in readme
    assert "<persisted-euid>" in guide
    for text in (readme, directive, guide, readiness):
        assert "/Users/" not in text
    assert "meridian-euid==0.4.8" in readme
    for text in (readme, directive):
        normalized = " ".join(text.split())
        assert "TapDB 10.0.0 is the latest verified public release" in normalized
        assert "TapDB 10.1.0" in normalized
        assert "unreleased candidate" in normalized
        assert "PostgreSQL 16.13 and 17.11" in normalized
        assert "Aurora PostgreSQL 16.13" in normalized


def test_service_readiness_requires_complete_explicit_recovery_evidence() -> None:
    readiness = (ROOT / "docs/service-readiness.md").read_text(encoding="utf-8")
    handoff = (
        ROOT / "docs/plans/20260910_tapdb_service_readiness_handoff.md"
    ).read_text(encoding="utf-8")

    for required in (
        "Exact TapDB config",
        "Domain-code and prefix-ownership registries",
        "TLS trust files",
        "IAM policies",
        "Operator and runtime secret references",
        "Service runtime files",
        "External receipt journals",
        "tapdb-recovery-family/v1",
        "journal root and receipt ID",
        "runtime-principal bootstrap",
        "runtime-principal bind",
        "An ordinary recovery apply must name",
        "observed-only",
        "session closure",
    ):
        assert required in readiness

    lifecycle = (
        "Inventory the exact source",
        "Prepare the target principal",
        "Back up and restore with explicit recovery authority",
        "Apply the reviewed schema migration",
        "Perform the service-owned conversion",
        "Verify identities, floors, and runtime binding",
        "Run service acceptance",
    )
    offsets = [readiness.index(label) for label in lifecycle]
    assert offsets == sorted(offsets)
    assert "**Candidate only. Not released. Not independently accepted.**" in handoff
    assert (
        "The release is not complete while any required field is `PENDING`." in handoff
    )
    assert "Production changed: **no**" in handoff


def test_active_guides_use_the_current_repository_identity() -> None:
    for relative_path in ACTIVE_GUIDES:
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        assert "tapdb-core" not in text, relative_path


def test_ci_runs_the_complete_release_matrix() -> None:
    workflow = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
    for required in (
        "fail-fast: false",
        "postgres_label: '16.13'",
        "postgres_image: postgres:16.13",
        "postgres_major: '16'",
        "postgres_label: '17'",
        "postgres:17",
        "postgres_major: '17'",
        "image: ${{ matrix.postgres_image }}",
        '"postgresql-${{ matrix.postgres_major }}"',
        '"postgresql-client-${{ matrix.postgres_major }}"',
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
        "SETUPTOOLS_SCM_PRETEND_VERSION: '10.1.0'",
        "verify_wheel_assets.py --expected-version 10.1.0",
        "python -m build",
        "python -m twine check dist/*",
        '"${wheels[0]}[gui]"',
        "from daylily_tapdb.external_references import ExternalReferenceService",
        "from daylily_tapdb.federation import DagV2FederationClient",
        "from daylily_tapdb.gui import create_tapdb_gui_app",
        "validation external-references --help",
    ):
        assert required in workflow
    assert "--deselect" not in workflow
    assert "PostgreSQL 17 full suite" not in workflow
    assert "postgresql-17 postgresql-client-17" not in workflow
    assert "SETUPTOOLS_SCM_PRETEND_VERSION: '9.2.0'" not in workflow
    assert "SETUPTOOLS_SCM_PRETEND_VERSION: '9.2.1'" not in workflow
    assert "SETUPTOOLS_SCM_PRETEND_VERSION: '9.2.2'" not in workflow
