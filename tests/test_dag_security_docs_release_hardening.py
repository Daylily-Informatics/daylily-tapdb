from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from daylily_tapdb import external_references, runtime_info
from daylily_tapdb.services import graph_payloads
from daylily_tapdb.services.object_search import search_objects
from daylily_tapdb.web import dag_v2


def test_runtime_database_probe_rejects_ambient_libpq_and_tapdb_targeting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name, value in {
        "PGHOSTADDR": "203.0.113.10",
        "PGPORT": "1",
        "PGSERVICE": "hostile",
        "PGOPTIONS": "-c search_path=pg_temp",
        "TAPDB_CONFIG": "/tmp/ambient.yaml",
    }.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(runtime_info.shutil, "which", lambda _name: "/usr/bin/psql")
    captured = {}

    def _run(_command, **kwargs):
        captured.update(kwargs)
        return SimpleNamespace(returncode=0, stdout="17.6\n")

    monkeypatch.setattr(runtime_info.subprocess, "run", _run)
    status = runtime_info._database_probe(
        {
            "host": "db.example.test",
            "hostaddr": "192.0.2.10",
            "port": "5432",
            "user": "tapdb_runtime",
            "password": "explicit-password",
            "database": "tapdb",
            "allow_global_claims": False,
        }
    )
    assert status == ("ok", "17.6")
    env = captured["env"]
    assert env["PGHOSTADDR"] == "192.0.2.10"
    assert env["PGPASSWORD"] == "explicit-password"
    assert env["PGCONNECT_TIMEOUT"] == "3"
    assert "PGPORT" not in env
    assert "PGSERVICE" not in env
    assert "PGOPTIONS" not in env
    assert "TAPDB_CONFIG" not in env


def test_v2_edge_projects_persisted_lineage_as_authoritative_provenance() -> None:
    parent = SimpleNamespace(uid=1, euid="<persisted-parent-euid>")
    child = SimpleNamespace(uid=2, euid="<persisted-child-euid>")
    lineage = SimpleNamespace(
        uid=3,
        euid="<persisted-lineage-euid>",
        parent_instance=parent,
        child_instance=child,
        relationship_type="contains",
        created_dt=datetime(2026, 1, 1, tzinfo=timezone.utc),
        json_addl={"properties": {}},
    )
    edge = graph_payloads._v2_edge(lineage, service_id="atlas")

    assert (
        edge["data"]["presentation"]["assertion_provenance"]
        == "tapdb.lineage:<persisted-lineage-euid>"
    )


@pytest.mark.parametrize("limit", [0, 101, True, "25"])
def test_shared_search_rejects_limits_instead_of_silently_clamping(limit) -> None:
    with pytest.raises(ValueError, match="1 through 100"):
        search_objects(object(), service_name="atlas", limit=limit)


def test_named_adopter_path_uses_exact_v2_manifests_and_typed_xrf_lineage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Model the TapDB-owned half of acceptance #10 with registry-selected IDs."""

    selected_service_ids = ("atlas", "bloom", "ursa", "dewey", "zebra-day")
    manifests = {
        service_id: dag_v2._manifest_for(
            service_id=service_id,
            display_name=service_id.replace("-", " ").title(),
            limits=dag_v2.DagV2Limits(
                max_depth=4,
                max_nodes=100,
                max_search_page_size=100,
            ),
        ).to_dict()
        for service_id in selected_service_ids
    }
    assert all(
        dag_v2.validate_dag_v2_manifest(manifest, expected_service_id=service_id)
        is None
        for service_id, manifest in manifests.items()
    )
    assert (
        dag_v2.validate_dag_v2_manifest(
            manifests["zebra-day"], expected_service_id="zebra_day"
        )
        is dag_v2.DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH
    )

    real_validator = external_references.validate_euid
    monkeypatch.setattr(
        external_references,
        "validate_euid",
        lambda value: (
            (
                isinstance(value, str)
                and value.startswith("<persisted-")
                and value.endswith("-euid>")
            )
            or real_validator(value)
        ),
    )

    def _typed_projection(source_service: str, target_service: str) -> dict:
        target = external_references.TapDBObjectTarget(
            target_service_id=target_service,
            target_object_euid=f"<persisted-{target_service}-object-euid>",
        )
        reference = SimpleNamespace(
            euid=f"<persisted-{source_service}-to-{target_service}-xrf-euid>",
            identity_key=target.identity_key,
            tenant_id=None,
            is_deleted=False,
            category="reference",
            type="external_identifier",
            subtype="tapdb_object",
            version="1.0",
            parent_template=SimpleNamespace(
                **external_references._canonical_template_definition("tapdb_object"),
                domain_code="Z",
                issuer_app_code=source_service,
            ),
            domain_code="Z",
            issuer_app_code=source_service,
            json_addl={
                "properties": {
                    "target_service_id": target_service,
                    "target_object_euid": f"<persisted-{target_service}-object-euid>",
                    "target_tenant_id": None,
                    "target_object_kind": None,
                }
            },
        )
        lineage = SimpleNamespace(
            euid=f"<persisted-{source_service}-to-{target_service}-lineage-euid>",
            relationship_type="references",
            is_deleted=False,
            child_instance=reference,
            json_addl={
                "properties": {
                    "asserted_at": "2026-09-02T00:00:00+00:00",
                    "assertion_provenance": "authenticated registry fixture",
                    "assertion_authority": source_service,
                    "approved_global_link": True,
                    "deactivated_at": None,
                    "deactivation_provenance": None,
                }
            },
        )
        source = SimpleNamespace(
            json_addl={"properties": {}},
            parent_of_lineages=[lineage],
        )
        return external_references._project_outbound_external_references(source)[
            "external_refs"
        ][0]

    path = [
        _typed_projection(source, target)
        for source, target in zip(selected_service_ids, selected_service_ids[1:])
    ]
    assert [edge["target_service_id"] for edge in path] == [
        "bloom",
        "ursa",
        "dewey",
        "zebra-day",
    ]
    assert "zebra_day" not in {edge["target_service_id"] for edge in path}
    assert "owy" not in manifests
    assert "kahlo" not in manifests  # Kahlo remains the hub, not a contributor.


def test_active_docs_state_identity_system_user_and_adopter_hard_cuts() -> None:
    root = Path(__file__).resolve().parents[1]
    readme = (root / "README.md").read_text(encoding="utf-8")
    templates = (root / "docs/template-authoring.md").read_text(encoding="utf-8")
    identity = (root / "docs/identity-and-scoping.md").read_text(encoding="utf-8")
    consumer = (root / "docs/consumer-discoverability-guide.md").read_text(
        encoding="utf-8"
    )
    assert "returns `EXISTING` and the stored winner" in readme
    assert "optional bundled GUI/auth subsystem" in templates
    assert "not a universal business-domain primitive" in templates
    assert "issue #12" in templates
    assert "reference/external_identifier/tapdb_object/1.0" in templates
    assert "reference/external_identifier/opaque/1.0" in templates
    assert "Z-AGX-1AD" not in identity
    assert "meridian-euid==0.4.8" in identity
    assert "`truncated`" in consumer
    assert "`truncation_reason`" in consumer
    assert "Kahlo remains the global visualization layer" in consumer
    assert "DagV2FederationClient" in consumer
    assert "unresolved boundary" in consumer
    assert "alias resolution" in consumer


def test_ci_secret_scan_and_nested_branch_push_are_real_gates() -> None:
    workflow = (
        Path(__file__).resolve().parents[1] / ".github/workflows/ci.yml"
    ).read_text(encoding="utf-8")
    assert "branches: ['**']" in workflow
    assert "> detect-secrets.json" in workflow
    assert 'report.get("results")' in workflow
    assert "verified secrets detected" in workflow
