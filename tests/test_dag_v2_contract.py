from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.testclient import TestClient

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.services import external_refs as external_refs_module
from daylily_tapdb.services.external_refs import (
    TypedExternalReferenceSpec,
    UntypedExternalReferenceError,
    create_or_reuse_typed_external_reference,
    project_outbound_typed_references,
)
from daylily_tapdb.services.graph_payloads import (
    DagV2GraphContractError,
    build_graph_v2_payload,
)
from daylily_tapdb.web.dag_v2 import (
    DAG_V2_CONTRACT,
    DagV2EligibilityReason,
    DagV2Limits,
    mount_tapdb_dag_surfaces,
    validate_dag_v2_manifest,
)

REMOTE_EUID = "<persisted-remote-object-euid>"
XRF_EUID = "<persisted-external-reference-euid>"
LINEAGE_EUID = "<persisted-lineage-euid>"


def _xrf_template(*, domain: str = "Z", owner: str = "daylily-tapdb"):
    return SimpleNamespace(
        **external_refs_module._canonical_xrf_template_definition(),
        domain_code=domain,
        issuer_app_code=owner,
    )


@pytest.fixture(autouse=True)
def _accept_unit_persisted_euid_placeholders(monkeypatch):
    real_validator = external_refs_module.validate_euid
    monkeypatch.setattr(
        external_refs_module,
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


class _Related:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def filter_by(self, **values):
        return _Related(
            row
            for row in self.rows
            if all(getattr(row, key, None) == value for key, value in values.items())
        )

    def all(self):
        return list(self.rows)

    def __iter__(self):
        return iter(self.rows)


def _instance(
    uid: int,
    euid: str,
    *,
    tenant_id: str | None = "00000000-0000-0000-0000-000000000001",
    typed_xrf: bool = False,
    properties: dict | None = None,
):
    coords = (
        ("reference", "external_identifier", "tapdb_object", "1.0")
        if typed_xrf
        else ("content", "specimen", "sample", "1.0")
    )
    return SimpleNamespace(
        uid=uid,
        euid=euid,
        name=f"Object {uid}",
        category=coords[0],
        type=coords[1],
        subtype=coords[2],
        version=coords[3],
        bstatus="active",
        tenant_id=tenant_id,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
        polymorphic_discriminator="generic_instance",
        parent_template=(
            _xrf_template() if typed_xrf else SimpleNamespace(instance_prefix="SMP")
        ),
        json_addl={"properties": properties or {}},
        created_dt=datetime(2026, 1, 1, tzinfo=timezone.utc),
        modified_dt=datetime(2026, 1, 2, tzinfo=timezone.utc),
        is_deleted=False,
        parent_of_lineages=_Related(),
        child_of_lineages=_Related(),
    )


def _lineage(
    uid: int,
    euid: str,
    parent,
    child,
    *,
    approved_global: bool = False,
):
    row = SimpleNamespace(
        uid=uid,
        euid=euid,
        name=euid,
        parent_instance=parent,
        child_instance=child,
        parent_instance_uid=parent.uid,
        child_instance_uid=child.uid,
        relationship_type="references" if approved_global else "contains",
        tenant_id=parent.tenant_id,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
        json_addl={
            "properties": {
                "asserted_at": "2026-01-01T00:00:00+00:00",
                "assertion_provenance": "unit-test-fixture",
                "approved_global_link": approved_global,
            }
        },
        created_dt=datetime(2026, 1, 1, tzinfo=timezone.utc),
        is_deleted=False,
    )
    parent.parent_of_lineages.rows.append(row)
    child.child_of_lineages.rows.append(row)
    return row


def _config(tmp_path):
    domain_registry = tmp_path / "domain_code_registry.json"
    prefix_registry = tmp_path / "prefix_ownership_registry.json"
    domain_registry.write_text(
        json.dumps({"version": "0.4.0", "domains": {"Z": {"name": "unit-test"}}}),
        encoding="utf-8",
    )
    prefix_registry.write_text(
        json.dumps(
            {
                "version": "0.4.0",
                "ownership": {
                    "Z": {
                        prefix: {"issuer_app_code": "daylily-tapdb"}
                        for prefix in (
                            "ADT",
                            "EDG",
                            "GSE",
                            "GVR",
                            "MSG",
                            "SYS",
                            "TPX",
                            "XRF",
                        )
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    path = tmp_path / "tapdb-config.yaml"
    path.write_text(
        json.dumps(
            {
                "meta": {
                    "config_version": 4,
                    "client_id": "example",
                    "database_name": "graph",
                    "owner_repo_name": "daylily-tapdb",
                    "domain_registry_path": str(domain_registry),
                    "prefix_ownership_registry_path": str(prefix_registry),
                },
                "target": {
                    "engine_type": "local",
                    "host": "localhost",
                    "port": 5432,
                    "ui_port": 8000,
                    "user": "tapdb_runtime",
                    "password": "",
                    "database": "tapdb",
                    "schema_name": "tapdb_unit",
                    "domain_code": "Z",
                },
                "safety": {
                    "safety_tier": "local",
                    "destructive_operations": "confirm_required",
                },
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    return path


def test_mount_is_atomic_explicit_authenticated_and_exact(tmp_path) -> None:
    app = FastAPI()
    limits = DagV2Limits(max_depth=4, max_nodes=100, max_search_page_size=25)

    missing = mount_tapdb_dag_surfaces(
        app,
        config_path=str(tmp_path / "missing.yaml"),
        service_id="example-service",
        display_name="Example Service",
        auth_dependency=lambda _request: {"username": "operator"},
        limits=limits,
    )
    assert missing.mounted is False
    assert missing.reason is DagV2EligibilityReason.MISSING_CONFIG
    assert missing.advertisement is None
    assert not any(route.path == "/api/dag/manifest" for route in app.routes)

    invalid_path = tmp_path / "invalid-config.yaml"
    invalid_path.write_text(
        "meta:\n  config_version: 4\n  client_id: example\n  database_name: graph\n",
        encoding="utf-8",
    )
    invalid_path.chmod(0o600)
    invalid = mount_tapdb_dag_surfaces(
        app,
        config_path=str(invalid_path),
        service_id="example-service",
        display_name="Example Service",
        auth_dependency=lambda _request: {"username": "operator"},
        limits=limits,
    )
    assert invalid.mounted is False
    assert invalid.reason is DagV2EligibilityReason.INVALID_CONFIG
    assert invalid.advertisement is None
    assert not any(route.path == "/api/dag/manifest" for route in app.routes)

    no_auth = mount_tapdb_dag_surfaces(
        app,
        config_path=str(_config(tmp_path)),
        service_id="example-service",
        display_name="Example Service",
        auth_dependency=None,
        limits=limits,
    )
    assert no_auth.reason is DagV2EligibilityReason.AUTH_REQUIRED

    async def authenticated(_request: Request):
        return {"username": "operator"}

    mounted = mount_tapdb_dag_surfaces(
        app,
        config_path=str(_config(tmp_path)),
        service_id="example-service",
        display_name="Example Service",
        auth_dependency=authenticated,
        limits=limits,
    )
    assert mounted.mounted is True
    assert mounted.manifest is not None
    assert mounted.manifest.service_id == "example-service"
    assert mounted.manifest.contract == DAG_V2_CONTRACT
    assert mounted.manifest.features["outbound_fetch"] is False
    mismatch = mount_tapdb_dag_surfaces(
        app,
        config_path=str(_config(tmp_path)),
        service_id="example_service",
        display_name="Example Service",
        auth_dependency=authenticated,
        limits=limits,
    )
    assert mismatch.reason is DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH
    assert mounted.manifest.service_id == "example-service"


def test_manifest_validation_has_no_alias_or_v1_fallback(tmp_path) -> None:
    app = FastAPI()

    async def authenticated(_request: Request):
        return {"username": "operator"}

    result = mount_tapdb_dag_surfaces(
        app,
        config_path=str(_config(tmp_path)),
        service_id="zebra-day",
        display_name="Zebra Day",
        auth_dependency=authenticated,
        limits=DagV2Limits(max_depth=3, max_nodes=50, max_search_page_size=20),
    )
    payload = result.manifest.to_dict()
    assert validate_dag_v2_manifest(payload, expected_service_id="zebra-day") is None
    assert (
        validate_dag_v2_manifest(payload, expected_service_id="zebra_day")
        is DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH
    )
    assert (
        validate_dag_v2_manifest(payload, expected_service_id="zebra-é")
        is DagV2EligibilityReason.SERVICE_IDENTITY_MISMATCH
    )
    v1 = dict(payload, extension="tapdb.dag_v1", contract="dag:v1")
    assert (
        validate_dag_v2_manifest(v1, expected_service_id="zebra-day")
        is DagV2EligibilityReason.VERSION_MISMATCH
    )


def test_typed_xrf_projection_requires_lineage_and_rejects_raw_metadata() -> None:
    root = _instance(1, "persisted-local-object")
    xrf = _instance(
        2,
        XRF_EUID,
        tenant_id=None,
        typed_xrf=True,
        properties={
            "target_service_id": "remote-service",
            "target_object_euid": REMOTE_EUID,
            "target_object_kind": "specimen",
        },
    )
    lineage = _lineage(
        3,
        LINEAGE_EUID,
        root,
        xrf,
        approved_global=True,
    )

    assert project_outbound_typed_references(root) == [
        {
            "target_service_id": "remote-service",
            "target_object_euid": REMOTE_EUID,
            "relationship_type": "references",
            "asserted_at": "2026-01-01T00:00:00+00:00",
            "assertion_provenance": "unit-test-fixture",
            "external_reference_euid": XRF_EUID,
            "lineage_euid": LINEAGE_EUID,
            "target_object_kind": "specimen",
        }
    ]
    root.parent_of_lineages = _Related()
    root.json_addl = {
        "properties": {"external_payload": {"tapdb_graph": {"root_euid": "copied-id"}}}
    }
    with pytest.raises(UntypedExternalReferenceError, match="typed External Object"):
        project_outbound_typed_references(root)
    assert lineage.euid == LINEAGE_EUID


def test_graph_v2_snapshot_presentation_outbound_refs_and_truncation() -> None:
    root = _instance(
        1,
        "persisted-root",
        properties={
            "graph_presentation": {
                "role": "source",
                "collapse_by_default": False,
                "expected_fanout": {
                    "relationship_types": ["contains"],
                    "max_degree": 4,
                    "reason": "bounded source fanout",
                },
            }
        },
    )
    child = _instance(2, "persisted-child")
    _lineage(3, "persisted-edge", root, child)

    payload = build_graph_v2_payload(
        root,
        record_type="instance",
        service_id="example-service",
        depth=0,
        max_nodes=10,
        snapshot_at=datetime(2026, 1, 3, tzinfo=timezone.utc),
    )
    assert payload["meta"]["contract"] == "dag:v2"
    assert payload["meta"]["snapshot_at"] == "2026-01-03T00:00:00+00:00"
    assert len(payload["meta"]["graph_revision"]) == 64
    assert payload["meta"]["truncated"] is True
    assert payload["meta"]["truncation_reason"] == "max_depth"
    assert payload["elements"]["nodes"][0]["data"]["presentation"]["role"] == "source"
    assert payload["elements"]["nodes"][0]["data"]["external_refs"] == []


def test_graph_v2_rejects_self_loop_cycle_and_cross_tenant() -> None:
    root = _instance(1, "persisted-root")
    self_loop = _lineage(2, "persisted-self-loop", root, root)
    with pytest.raises(DagV2GraphContractError, match="Self-loop"):
        build_graph_v2_payload(
            root,
            record_type="instance",
            service_id="example-service",
            depth=2,
            max_nodes=10,
        )
    root.parent_of_lineages.rows.remove(self_loop)
    root.child_of_lineages.rows.remove(self_loop)

    other = _instance(
        3,
        "persisted-other-tenant",
        tenant_id="00000000-0000-0000-0000-000000000002",
    )
    _lineage(4, "persisted-cross-tenant", root, other)
    with pytest.raises(DagV2GraphContractError, match="Cross-tenant"):
        build_graph_v2_payload(
            root,
            record_type="instance",
            service_id="example-service",
            depth=2,
            max_nodes=10,
        )


class _Query:
    def __init__(self, value):
        self.value = value

    def filter_by(self, **_values):
        return self

    def one_or_none(self):
        return self.value


class _XrfSession:
    bind = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))

    def __init__(self, template, lineage=None):
        self.template = template
        self.lineage = lineage
        self.added = []

    def query(self, model):
        return _Query(self.template if model is generic_template else self.lineage)

    def add(self, value):
        self.added.append(value)

    @contextmanager
    def begin_nested(self):
        yield

    def flush(self):
        if self.added:
            self.added[-1].euid = LINEAGE_EUID


def test_xrf_factory_consumes_natural_identity_and_persists_lineage() -> None:
    source = _instance(1, "persisted-source")
    reference = _instance(
        2,
        "persisted-reference",
        tenant_id=None,
        typed_xrf=True,
        properties={
            "target_service_id": "remote-service",
            "target_object_euid": REMOTE_EUID,
            "target_tenant_id": None,
            "target_object_kind": "specimen",
        },
    )
    template = _xrf_template()
    template.uid = 10
    template.is_deleted = False
    session = _XrfSession(template)
    calls = []

    class Factory:
        def claim_instance_by_identity(self, session_arg, **kwargs):
            calls.append((session_arg, kwargs))
            return SimpleNamespace(instance=reference, outcome="created")

    result = create_or_reuse_typed_external_reference(
        session,
        source=source,
        spec=TypedExternalReferenceSpec(
            target_service_id="remote-service",
            target_object_euid=REMOTE_EUID,
            target_object_kind="specimen",
            relationship_type="references",
            asserted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            assertion_provenance="contract-test",
        ),
        instance_factory=Factory(),
    )
    assert result.reference is reference
    assert result.lineage.euid == LINEAGE_EUID
    assert result.created is True
    assert calls[0][1]["identity_key"] == (
        f"tapdb.external-reference/v1:remote-service:{REMOTE_EUID}"
    )
    assert calls[0][1]["properties"] == {
        "target_service_id": "remote-service",
        "target_object_euid": REMOTE_EUID,
        "target_tenant_id": None,
        "target_object_kind": "specimen",
    }
    assert calls[0][1]["claimant_tenant_id"] is None
    assert calls[0][1]["command_evidence"] == {
        "contract": "tapdb.external-reference/v1"
    }
    assert result.lineage.parent_instance_uid == source.uid
    assert result.lineage.child_instance_uid == reference.uid
    assert result.lineage.json_addl["properties"]["asserted_at"] == (
        "2026-01-01T00:00:00+00:00"
    )


def test_xrf_spec_rejects_non_string_relationship_type() -> None:
    with pytest.raises(ValueError, match="relationship_type must be a string"):
        TypedExternalReferenceSpec(
            target_service_id="remote-service",
            target_object_euid=REMOTE_EUID,
            relationship_type=1,  # type: ignore[arg-type]
            asserted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            assertion_provenance="contract-test",
        )


def test_xrf_reuse_keeps_assertion_provenance_on_each_source_lineage() -> None:
    first_source = _instance(1, "persisted-first-source")
    second_source = _instance(2, "persisted-second-source")
    reference = _instance(
        3,
        XRF_EUID,
        tenant_id=None,
        typed_xrf=True,
        properties={
            "target_service_id": "remote-service",
            "target_object_euid": REMOTE_EUID,
            "target_tenant_id": None,
            "target_object_kind": "specimen",
        },
    )
    template = _xrf_template()
    template.uid = 10
    template.is_deleted = False
    lineages = []

    class Query:
        def __init__(self, rows):
            self.rows = list(rows)

        def filter_by(self, **values):
            return Query(
                row
                for row in self.rows
                if all(
                    getattr(row, key, None) == value for key, value in values.items()
                )
            )

        def one_or_none(self):
            assert len(self.rows) <= 1
            return self.rows[0] if self.rows else None

    class Session:
        def query(self, model):
            rows = [template] if model.__name__ == "generic_template" else lineages
            return Query(rows)

        def add(self, value):
            lineages.append(value)

        @contextmanager
        def begin_nested(self):
            yield

        def flush(self):
            for index, lineage in enumerate(lineages, start=1):
                if not getattr(lineage, "euid", None):
                    lineage.euid = f"<persisted-lineage-{index}-euid>"

    class Factory:
        calls = 0

        def claim_instance_by_identity(self, _session, **kwargs):
            assert "asserted_at" not in kwargs["properties"]
            assert "assertion_provenance" not in kwargs["properties"]
            self.calls += 1
            return SimpleNamespace(
                instance=reference,
                outcome="created" if self.calls == 1 else "existing",
            )

    session = Session()
    factory = Factory()
    first = create_or_reuse_typed_external_reference(
        session,
        source=first_source,
        spec=TypedExternalReferenceSpec(
            target_service_id="remote-service",
            target_object_euid=REMOTE_EUID,
            target_object_kind="specimen",
            relationship_type="references",
            asserted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            assertion_provenance="first authenticated ownership lookup",
        ),
        instance_factory=factory,
    )
    second = create_or_reuse_typed_external_reference(
        session,
        source=second_source,
        spec=TypedExternalReferenceSpec(
            target_service_id="remote-service",
            target_object_euid=REMOTE_EUID,
            target_object_kind="specimen",
            relationship_type="references",
            asserted_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
            assertion_provenance="second authenticated ownership lookup",
        ),
        instance_factory=factory,
    )

    assert first.reference is second.reference is reference
    assert first.lineage is not second.lineage
    assert "asserted_at" not in reference.json_addl["properties"]
    assert "assertion_provenance" not in reference.json_addl["properties"]

    first.lineage.__dict__["child_instance"] = reference
    second.lineage.__dict__["child_instance"] = reference
    first.lineage.is_deleted = False
    second.lineage.is_deleted = False
    first_source.parent_of_lineages.rows.append(first.lineage)
    second_source.parent_of_lineages.rows.append(second.lineage)
    assert project_outbound_typed_references(first_source)[0][
        "assertion_provenance"
    ] == ("first authenticated ownership lookup")
    assert project_outbound_typed_references(second_source)[0]["asserted_at"] == (
        "2026-01-02T00:00:00+00:00"
    )


class _ObjectQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def filter_by(self, **values):
        return _ObjectQuery(
            row
            for row in self.rows
            if all(getattr(row, key, None) == value for key, value in values.items())
        )

    def first(self):
        return self.rows[0] if self.rows else None


class _ObjectSession:
    def __init__(self, root):
        self.root = root

    def query(self, model):
        return _ObjectQuery([self.root] if model is generic_instance else [])


class _Connection:
    def __init__(self, root):
        self.session = _ObjectSession(root)
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def session_scope(self):
        yield self.session


def test_v2_routes_exact_lookup_and_bounded_search_are_authenticated(
    monkeypatch, tmp_path
) -> None:
    root = _instance(1, "persisted-root")
    app = FastAPI()

    async def authenticated(request: Request):
        if request.headers.get("authorization") != "Bearer test-token":
            raise HTTPException(status_code=401, detail="auth_required")
        return {"username": "operator"}

    monkeypatch.setattr(
        "daylily_tapdb.web.runtime.get_db", lambda _path: _Connection(root)
    )
    search_calls = []

    def fake_search(_session, **kwargs):
        search_calls.append(kwargs)
        return {
            "items": [],
            "page": {"limit": kwargs["limit"], "returned": 0, "next_cursor": None},
            "filters": {},
        }

    monkeypatch.setattr("daylily_tapdb.web.dag_v2.search_objects", fake_search)
    result = mount_tapdb_dag_surfaces(
        app,
        config_path=str(_config(tmp_path)),
        service_id="example-service",
        display_name="Example Service",
        auth_dependency=authenticated,
        limits=DagV2Limits(max_depth=3, max_nodes=50, max_search_page_size=20),
    )
    assert result.mounted
    client = TestClient(app)
    assert client.get("/api/dag/manifest").status_code == 401
    headers = {"authorization": "Bearer test-token"}
    assert client.get("/api/dag/manifest", headers=headers).status_code == 200
    owned = client.get("/api/dag/v2/object/persisted-root", headers=headers)
    assert owned.status_code == 200
    assert owned.json()["service_id"] == "example-service"
    assert (
        client.get("/api/dag/v2/object/not-owned", headers=headers).status_code == 404
    )
    search = client.get(
        "/api/dag/v2/search",
        headers=headers,
        params={"limit": 20, "cursor": "opaque-cursor"},
    )
    assert search.status_code == 200
    assert search.json()["meta"]["ownership_proof"] is False
    assert search_calls[0]["cursor"] == "opaque-cursor"
    assert (
        client.get(
            "/api/dag/v2/search", headers=headers, params={"limit": 21}
        ).status_code
        == 422
    )
