"""Contracts for the public DAG v2 consumer guide."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
GUIDE = ROOT / "docs" / "consumer-discoverability-guide.md"


def _text() -> str:
    return GUIDE.read_text(encoding="utf-8")


def test_consumer_guide_has_the_complete_discovery_contract() -> None:
    text = _text()
    for required in (
        "tapdb.dag_v2",
        "dag:v2",
        "/api/dag/manifest",
        "/api/dag/v2/search",
        "/api/dag/v2/object/{euid}",
        "/api/dag/v2/data",
        "Search one bounded page",
        "exact lookup",
        "graph_revision",
        "snapshot_at",
        "truncation",
        "ExternalReferenceService",
        "TapDBObjectTarget",
        "ExternalIdentifierTarget",
        "DagV2FederationClient",
        "FederationLimits",
        "generic_instance_lineage",
        "NOSUPERUSER NOBYPASSRLS",
        "claim_instance_by_identity",
        "IdentityScope.GLOBAL",
        "IdentityScope.TENANT",
        "templates inventory",
        "--json info",
        "--preflight-receipt",
        "exactly one owner",
        "unresolved boundary",
        "identity and scoping",
        "backup and recovery",
    ):
        assert required in text

    for removed in (
        "TypedExternalReferenceSpec",
        "V1ProxyPolicy",
        "create_tapdb_dag_router",
        "/api/dag/data",
        "/api/dag/object/",
    ):
        assert removed not in text


def test_consumer_examples_use_only_persisted_euid_placeholders() -> None:
    text = _text()
    assert "<persisted-euid>" in text
    assert "Never replace" in text
    assert "/Users/" not in text
    assert not re.search(r"\bM-[A-Z0-9]{2,}-[A-Z0-9-]+\b", text)


def test_python_examples_are_syntax_valid() -> None:
    blocks = re.findall(r"~~~python\n(.*?)\n~~~", _text(), flags=re.DOTALL)
    assert len(blocks) == 2
    for index, block in enumerate(blocks):
        compile(block, f"consumer-guide-block-{index}.py", "exec")


def test_readme_and_docs_index_link_the_guide() -> None:
    relative = "docs/consumer-discoverability-guide.md"
    assert relative in (ROOT / "README.md").read_text(encoding="utf-8")
    assert "consumer-discoverability-guide.md" in (
        ROOT / "docs" / "README.md"
    ).read_text(encoding="utf-8")
    assert "external-references-and-federation.md" in (
        ROOT / "docs" / "README.md"
    ).read_text(encoding="utf-8")
