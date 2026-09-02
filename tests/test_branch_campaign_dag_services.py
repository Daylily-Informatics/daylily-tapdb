from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from sqlalchemy.exc import IntegrityError

from daylily_tapdb.services import external_refs as refs
from daylily_tapdb.services import graph_payloads as graphs

REMOTE_EUID = "<persisted-remote-object-euid>"
XRF_EUID = "<persisted-external-reference-euid>"
LINEAGE_EUID = "<persisted-lineage-euid>"
TENANT_ID = "00000000-0000-4000-8000-000000000203"


def _xrf_template(*, domain: str = "A", owner: str = "owner-a"):
    return SimpleNamespace(
        **refs._canonical_xrf_template_definition(),
        domain_code=domain,
        issuer_app_code=owner,
    )


@pytest.fixture(autouse=True)
def _accept_unit_persisted_euid_placeholders(monkeypatch):
    real_validator = refs.validate_euid
    monkeypatch.setattr(
        refs,
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


def _object(
    uid: int,
    euid: str,
    *,
    tenant: str | None = "tenant-a",
    owner: str = "owner-a",
    domain: str = "A",
    typed: bool = False,
    properties: object | None = None,
):
    coords = (
        ("reference", "external_identifier", "tapdb_object", "1.0")
        if typed
        else ("content", "specimen", "sample", "1.0")
    )
    obj = SimpleNamespace(
        uid=uid,
        euid=euid,
        name=f"Object {uid}",
        category=coords[0],
        type=coords[1],
        subtype=coords[2],
        version=coords[3],
        polymorphic_discriminator="generic_instance",
        parent_template=(
            _xrf_template(domain=domain, owner=owner)
            if typed
            else SimpleNamespace(instance_prefix="SMP")
        ),
        tenant_id=tenant,
        domain_code=domain,
        issuer_app_code=owner,
        created_dt=datetime(2026, 1, 1, tzinfo=timezone.utc),
        modified_dt=None,
        json_addl={"properties": properties or {}},
        parent_of_lineages=_Related(),
        child_of_lineages=_Related(),
    )
    return obj


def _lineage(
    uid: int,
    euid: str,
    parent,
    child,
    *,
    tenant: str | None = "tenant-a",
    owner: str = "owner-a",
    domain: str = "A",
    properties: dict | None = None,
):
    lineage_properties = {
        "asserted_at": "2026-01-02T00:00:00+00:00",
        "assertion_provenance": "unit-test persisted lineage fixture",
    }
    if properties:
        lineage_properties.update(properties)
    lineage = SimpleNamespace(
        uid=uid,
        euid=euid,
        parent_instance=parent,
        child_instance=child,
        relationship_type="references",
        tenant_id=tenant,
        domain_code=domain,
        issuer_app_code=owner,
        is_deleted=False,
        created_dt=datetime(2026, 1, 2, tzinfo=timezone.utc),
        json_addl={"properties": lineage_properties},
    )
    parent.parent_of_lineages.rows.append(lineage)
    child.child_of_lineages.rows.append(lineage)
    return lineage


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        ({"allowed_hosts": frozenset()}, "at least one"),
        (
            {"allowed_hosts": frozenset({"public.example"}), "timeout_seconds": 0},
            "timeout_seconds",
        ),
        (
            {
                "allowed_hosts": frozenset({"public.example"}),
                "max_response_bytes": 0,
            },
            "max_response_bytes",
        ),
    ],
)
def test_branch_campaign_proxy_policy_rejects_invalid_bounds(kwargs, message):
    with pytest.raises(ValueError, match=message):
        refs.V1ProxyPolicy(**kwargs)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"target_service_id": " bad"}, "target_service_id"),
        ({"target_service_id": "bad/service"}, "target_service_id"),
        ({"target_service_id": "service-é"}, "target_service_id"),
        ({"target_object_euid": "remote object"}, "target_object_euid"),
        ({"relationship_type": "bad\nvalue"}, "relationship_type"),
        ({"target_tenant_id": "tenant a"}, "target_tenant_id"),
        ({"target_object_kind": " bad"}, "target_object_kind"),
        ({"assertion_provenance": ""}, "assertion_provenance"),
        ({"asserted_at": datetime(2026, 1, 1)}, "timezone"),
    ],
)
def test_branch_campaign_typed_spec_validates_each_field(changes, message):
    values = {
        "target_service_id": "remote-service",
        "target_object_euid": REMOTE_EUID,
        "relationship_type": "references",
        "asserted_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "assertion_provenance": "branch-campaign",
    }
    values.update(changes)
    with pytest.raises(ValueError, match=message):
        refs.TypedExternalReferenceSpec(**values)


def test_branch_campaign_reference_public_projection_and_metadata_guards():
    ref = refs.ExternalGraphRef(
        label="Remote",
        system="remote",
        root_euid="remote-object",
        tenant_id=None,
        href=None,
        graph_expandable=False,
        reason="not configured",
        base_url=None,
        graph_data_path=None,
        object_detail_path_template=None,
        auth_mode="none",
    )
    assert ref.to_public_dict(ref_index=2)["reason"] == "not configured"

    typed = _object(1, "typed-reference", tenant=None, typed=True)
    refs.validate_no_untyped_federation_metadata(typed)
    ordinary = _object(
        2,
        "ordinary-object",
        properties={
            "target_object_euid": "copied-object",
            "external_payload": {"tapdb_graph": {"root_euid": "copied-object"}},
        },
    )
    with pytest.raises(refs.UntypedExternalReferenceError) as exc_info:
        refs.validate_no_untyped_federation_metadata(ordinary)
    assert "external_payload.tapdb_graph" in str(exc_info.value)
    assert "target_object_euid" in str(exc_info.value)

    typed.parent_template = SimpleNamespace(instance_prefix="SMP")
    assert refs.is_typed_external_reference(typed) is False
    altered = _object(3, "<persisted-altered-xrf-euid>", tenant=None, typed=True)
    altered.parent_template.name = "Client spoof"
    assert refs.is_typed_external_reference(altered) is False


def test_branch_campaign_public_v2_properties_strip_routing_case_variants():
    obj = _object(
        23,
        "<persisted-object-euid>",
        properties={
            "display_value": "kept",
            "BASE_URL": "https://forbidden.example",
            "Callback_URL": "https://forbidden.example/callback",
            "AUTH_MODE": "same_origin",
            "auth_token": "forbidden",
            "Graph_Data_Path": "/api/legacy",
            7: "non-json-key",
        },
    )

    assert graphs._clean_public_properties(obj) == {"display_value": "kept"}


def test_branch_campaign_projected_refs_filter_sort_and_optional_fields():
    root = _object(1, "local-root")
    untyped = _object(2, "ordinary-child")
    _lineage(3, "ordinary-lineage", root, untyped)
    typed = _object(
        4,
        XRF_EUID,
        tenant=None,
        typed=True,
        properties={
            "target_service_id": "z-remote",
            "target_object_euid": REMOTE_EUID,
            "target_tenant_id": TENANT_ID,
            "target_object_kind": "specimen",
        },
    )
    _lineage(
        5,
        LINEAGE_EUID,
        root,
        typed,
        properties={
            "asserted_at": "2026-01-01T00:00:00+00:00",
            "assertion_provenance": "branch-campaign",
        },
    )
    root.parent_of_lineages.rows.insert(
        0, SimpleNamespace(child_instance=None, is_deleted=False)
    )

    projected = refs.project_outbound_typed_references(root)

    assert projected == [
        {
            "target_service_id": "z-remote",
            "target_object_euid": REMOTE_EUID,
            "relationship_type": "references",
            "asserted_at": "2026-01-01T00:00:00+00:00",
            "assertion_provenance": "branch-campaign",
            "external_reference_euid": XRF_EUID,
            "lineage_euid": LINEAGE_EUID,
            "target_tenant_id": TENANT_ID,
            "target_object_kind": "specimen",
        }
    ]
    assert refs._active_lineages(None) == []
    assert refs._properties(SimpleNamespace(json_addl=None)) == {}


class _OneQuery:
    def __init__(self, value):
        self.value = value

    def filter_by(self, **_values):
        return self

    def one_or_none(self):
        return self.value


class _XrfSession:
    def __init__(self, template=None, lineage=None):
        self.template = template if template is not None else _xrf_template()
        self.lineage = lineage
        self.added = []

    def query(self, model):
        value = (
            self.lineage if model is refs.generic_instance_lineage else self.template
        )
        return _OneQuery(value)

    def add(self, value):
        self.added.append(value)

    @contextmanager
    def begin_nested(self):
        yield

    def flush(self):
        if self.added:
            self.added[-1].euid = LINEAGE_EUID


def _valid_spec():
    return refs.TypedExternalReferenceSpec(
        target_service_id="remote-service",
        target_object_euid=REMOTE_EUID,
        relationship_type="references",
        asserted_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        assertion_provenance="branch-campaign",
    )


def test_branch_campaign_xrf_creation_guards_and_claim_shapes():
    source = _object(1, "persisted-source")
    with pytest.raises(ValueError, match="persisted"):
        refs.create_or_reuse_typed_external_reference(
            _XrfSession(),
            source=_object(0, ""),
            spec=_valid_spec(),
            instance_factory=object(),
        )
    with pytest.raises(RuntimeError, match="claim_instance_by_identity"):
        refs.create_or_reuse_typed_external_reference(
            _XrfSession(), source=source, spec=_valid_spec(), instance_factory=object()
        )

    class _Factory:
        def __init__(self, reference):
            self.reference = reference

        def claim_instance_by_identity(self, *_args, **_kwargs):
            return SimpleNamespace(value=self.reference, outcome="CREATED")

    reference = _object(
        2,
        "persisted-reference",
        tenant=None,
        typed=True,
        properties={
            "target_service_id": "remote-service",
            "target_object_euid": REMOTE_EUID,
            "target_tenant_id": None,
            "target_object_kind": None,
            "asserted_at": "2026-01-01T00:00:00+00:00",
            "assertion_provenance": "branch-campaign",
        },
    )
    result = refs.create_or_reuse_typed_external_reference(
        _XrfSession(),
        source=source,
        spec=_valid_spec(),
        instance_factory=_Factory(reference),
    )
    assert result.created is True
    assert result.lineage.euid == LINEAGE_EUID


def test_branch_campaign_xrf_lineage_conflict_recovers_inside_savepoint():
    source = _object(1, "<persisted-source-euid>")
    reference = _object(
        2,
        XRF_EUID,
        tenant=None,
        typed=True,
        properties={
            "target_service_id": "remote-service",
            "target_object_euid": REMOTE_EUID,
            "target_tenant_id": None,
            "target_object_kind": None,
        },
    )
    winner = SimpleNamespace(
        euid=LINEAGE_EUID,
        parent_instance_uid=source.uid,
        child_instance_uid=reference.uid,
        relationship_type="references",
        is_deleted=False,
        json_addl={
            "properties": {
                "asserted_at": "2026-01-01T00:00:00+00:00",
                "assertion_provenance": "branch-campaign",
                "approved_global_link": True,
            }
        },
    )

    class ConflictSession(_XrfSession):
        def __init__(self):
            super().__init__(template=_xrf_template())
            self.lineage_queries = 0
            self.outer_transaction_usable = True

        def query(self, model):
            if model is refs.generic_instance_lineage:
                self.lineage_queries += 1
                return _OneQuery(None if self.lineage_queries == 1 else winner)
            return _OneQuery(self.template)

        def flush(self):
            raise IntegrityError("insert", {}, Exception("unique edge"))

        @contextmanager
        def begin_nested(self):
            try:
                yield
            except IntegrityError:
                self.outer_transaction_usable = True
                raise

    class Factory:
        def claim_instance_by_identity(self, *_args, **_kwargs):
            return SimpleNamespace(instance=reference, outcome="EXISTING")

    session = ConflictSession()
    result = refs.create_or_reuse_typed_external_reference(
        session,
        source=source,
        spec=_valid_spec(),
        instance_factory=Factory(),
    )

    assert result.lineage is winner
    assert session.outer_transaction_usable is True
    assert session.lineage_queries == 2


@pytest.mark.parametrize(
    ("reference", "message"),
    [
        (None, "persisted instance"),
        (
            _object(2, "persisted-reference", typed=True, tenant="tenant-a"),
            "must be global",
        ),
        (
            _object(
                2,
                "persisted-reference",
                typed=True,
                tenant=None,
                properties={"target_service_id": "different"},
            ),
            "divergent",
        ),
    ],
)
def test_branch_campaign_xrf_claim_rejects_invalid_results(reference, message):
    class _Factory:
        def claim_instance_by_identity(self, *_args, **_kwargs):
            return SimpleNamespace(instance=reference, outcome="existing")

    with pytest.raises((RuntimeError, ValueError), match=message):
        refs.create_or_reuse_typed_external_reference(
            _XrfSession(),
            source=_object(1, "persisted-source"),
            spec=_valid_spec(),
            instance_factory=_Factory(),
        )


def test_branch_campaign_legacy_ref_parsing_deduplicates_and_composes_paths():
    assert (
        refs._compose_object_href(
            base_url="https://public.example",
            object_detail_path_template="",
            root_euid="remote-object",
        )
        == ""
    )
    assert (
        refs._compose_object_href(
            base_url="https://public.example",
            object_detail_path_template="objects",
            root_euid="remote-object",
        )
        == "https://public.example/objects/remote-object"
    )

    item = {
        "service_id": "remote",
        "value": "remote-object",
        "base_url": "https://public.example",
        "graph_data_path": "/graph",
        "object_detail_path_template": "/objects/{euid}",
    }
    obj = SimpleNamespace(
        category="external_identifier",
        type="other",
        subtype="other",
        json_addl={
            "properties": {
                "external_payload": {"tapdb_graph": [item, dict(item)]},
                "external_reference": item,
            }
        },
    )
    resolved = refs.resolve_external_graph_refs(obj)
    assert len(resolved) == 1
    assert resolved[0].href.endswith("/objects/remote-object")


def _proxy_ref(**changes):
    values = {
        "label": "Remote",
        "system": "remote",
        "root_euid": "remote-object",
        "tenant_id": None,
        "href": None,
        "graph_expandable": True,
        "reason": None,
        "base_url": "https://public.example",
        "graph_data_path": "/graph",
        "object_detail_path_template": "/objects?euid={euid}",
        "auth_mode": "none",
    }
    values.update(changes)
    return refs.ExternalGraphRef(**values)


def _policy():
    return refs.V1ProxyPolicy(
        allowed_hosts=frozenset({"public.example"}), max_response_bytes=32
    )


def test_branch_campaign_legacy_fetch_preconditions_and_auth():
    with pytest.raises(RuntimeError, match="not expandable"):
        refs.fetch_remote_graph(
            None, _proxy_ref(graph_expandable=False), depth=1, policy=_policy()
        )
    with pytest.raises(RuntimeError, match="not available"):
        refs.fetch_remote_object_detail(
            None,
            _proxy_ref(graph_expandable=False),
            euid="remote-object",
            policy=_policy(),
        )


@pytest.mark.parametrize(
    ("host", "message"),
    [
        (" public.example", "exact DNS"),
        ("127.0.0.1", "IP literals"),
        ("bad..example", "valid DNS"),
    ],
)
def test_branch_campaign_proxy_dns_name_validation(host, message):
    with pytest.raises(ValueError, match=message):
        refs._require_dns_name(host)


def test_branch_campaign_proxy_resolution_and_url_guards(monkeypatch):
    monkeypatch.setattr(refs.socket, "getaddrinfo", lambda *_a, **_k: [])
    with pytest.raises(RuntimeError, match="returned no addresses"):
        refs._require_public_resolution("public.example", 443)

    def _fail(*_args, **_kwargs):
        raise OSError("dns down")

    monkeypatch.setattr(refs.socket, "getaddrinfo", _fail)
    with pytest.raises(RuntimeError, match="resolution failed"):
        refs._require_public_resolution("public.example", 443)

    with pytest.raises(RuntimeError, match="credentials or fragments"):
        refs._require_v1_proxy_url("https://user@public.example/path", policy=_policy())
    with pytest.raises(RuntimeError, match="not explicitly allowed"):
        refs._require_v1_proxy_url("https://other.example/path", policy=_policy())


class _Response:
    def __init__(self, raw: bytes, headers: dict[str, str], *, status: int = 200):
        self.raw = raw
        self.headers = headers
        self.status = status
        self._offset = 0

    def read(self, size):
        chunk = self.raw[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


class _Connection:
    def __init__(self, value):
        self.value = value
        self.closed = False

    def close(self):
        self.closed = True


def _install_fetch(monkeypatch, value):
    target = refs._V1ProxyTarget(
        host="public.example",
        port=443,
        host_header="public.example",
        request_target="/path",
        endpoints=(
            (refs.socket.AF_INET, refs.socket.SOCK_STREAM, 6, ("93.184.216.34", 443)),
        ),
    )
    monkeypatch.setattr(
        refs,
        "_require_v1_proxy_url",
        lambda url, *, policy, resolution_timeout=None: target,
    )

    def _open(*_args, **_kwargs):
        if isinstance(value, Exception):
            raise value
        return _Connection(value), value

    monkeypatch.setattr(refs, "_open_pinned_https", _open)


@pytest.mark.parametrize(
    ("response", "message"),
    [
        (_Response(b"{}", {"Content-Type": "text/plain"}), "application/json"),
        (
            _Response(
                b"{}", {"Content-Type": "application/json", "Content-Length": "x"}
            ),
            "invalid Content-Length",
        ),
        (
            _Response(
                b"{}", {"Content-Type": "application/json", "Content-Length": "33"}
            ),
            "size limit",
        ),
        (_Response(b"x" * 33, {"Content-Type": "application/json"}), "size limit"),
        (_Response(b"not-json", {"Content-Type": "application/json"}), "UTF-8 JSON"),
        (
            _Response(json.dumps([]).encode(), {"Content-Type": "application/json"}),
            "JSON object",
        ),
    ],
)
def test_branch_campaign_fetch_rejects_unsafe_responses(monkeypatch, response, message):
    _install_fetch(monkeypatch, response)
    with pytest.raises(RuntimeError, match=message):
        refs._fetch_v1_json(
            "https://public.example/path", policy=_policy(), label="Remote"
        )


def test_branch_campaign_fetch_rejects_redirect(monkeypatch):
    _install_fetch(
        monkeypatch,
        _Response(b"{}", {"Content-Type": "application/json"}, status=302),
    )
    with pytest.raises(RuntimeError, match="does not follow redirects"):
        refs._fetch_v1_json(
            "https://public.example/path", policy=_policy(), label="Remote"
        )


@pytest.mark.parametrize(
    ("properties", "message"),
    [
        ({"graph_presentation": "bad"}, "must be an object"),
        ({"graph_presentation": {"role": " bad"}}, "role must be exact"),
        ({"graph_presentation": {"collapse_by_default": 1}}, "must be boolean"),
        ({"graph_presentation": {"expected_fanout": []}}, "must be an object"),
        (
            {
                "graph_presentation": {
                    "expected_fanout": {
                        "relationship_types": [],
                        "max_degree": 1,
                        "reason": "why",
                    }
                }
            },
            "non-empty string list",
        ),
        (
            {
                "graph_presentation": {
                    "expected_fanout": {
                        "relationship_types": ["rel"],
                        "max_degree": True,
                        "reason": "why",
                    }
                }
            },
            "positive integer",
        ),
        (
            {
                "graph_presentation": {
                    "expected_fanout": {
                        "relationship_types": ["rel"],
                        "max_degree": 1,
                        "reason": " bad",
                    }
                }
            },
            "reason must be exact",
        ),
        ({"graph_presentation": {"unsupported": True}}, "Unsupported"),
    ],
)
def test_branch_campaign_graph_presentation_rejects_malformed_values(
    properties, message
):
    with pytest.raises(graphs.DagV2GraphContractError, match=message):
        graphs.build_object_detail_v2_payload(
            _object(1, "persisted-object", properties=properties),
            record_type="instance",
            service_id="local-service",
        )


def test_branch_campaign_graph_presentation_accepts_complete_contract():
    payload = graphs.build_object_detail_v2_payload(
        _object(
            1,
            "persisted-object",
            properties={
                "graph_presentation": {
                    "role": "source",
                    "collapse_by_default": False,
                    "expected_fanout": {
                        "relationship_types": ["z", "a", "z"],
                        "max_degree": 2,
                        "reason": "bounded",
                    },
                },
                "base_url": "https://must-not-leak.example",
                "callback_url": "https://must-not-leak.example/callback",
                "public": "visible",
            },
        ),
        record_type="instance",
        service_id="local-service",
    )
    assert payload["presentation"]["expected_fanout"]["relationship_types"] == [
        "a",
        "z",
    ]
    assert payload["properties"]["public"] == "visible"
    assert "base_url" not in payload["properties"]
    assert "callback_url" not in payload["properties"]


@pytest.mark.parametrize(
    ("depth", "max_nodes", "snapshot", "message"),
    [
        (True, 1, None, "depth"),
        (0, False, None, "max_nodes"),
        (0, 1, datetime(2026, 1, 1), "timezone"),
    ],
)
def test_branch_campaign_graph_v2_validates_bounds(depth, max_nodes, snapshot, message):
    with pytest.raises(ValueError, match=message):
        graphs.build_graph_v2_payload(
            _object(1, "persisted-object"),
            record_type="instance",
            service_id="local-service",
            depth=depth,
            max_nodes=max_nodes,
            snapshot_at=snapshot,
        )


def test_branch_campaign_graph_v2_non_instance_and_max_nodes_truncation():
    singleton = graphs.build_graph_v2_payload(
        _object(1, "persisted-template"),
        record_type="template",
        service_id="local-service",
        depth=0,
        max_nodes=1,
    )
    assert singleton["meta"]["truncated"] is False

    root = _object(1, "persisted-root")
    child = _object(2, "persisted-child")
    _lineage(3, "persisted-edge", root, child)
    graph = graphs.build_graph_v2_payload(
        root,
        record_type="instance",
        service_id="local-service",
        depth=2,
        max_nodes=1,
    )
    assert graph["meta"]["truncation_reason"] == "max_nodes"


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda root, child, edge: setattr(root, "domain_code", ""), "root is missing"),
        (
            lambda root, child, edge: setattr(child, "issuer_app_code", "owner-b"),
            "Cross-domain",
        ),
        (
            lambda root, child, edge: setattr(edge, "domain_code", "B"),
            "Lineage row scope",
        ),
        (
            lambda root, child, edge: setattr(edge, "child_instance", None),
            "endpoint could not",
        ),
    ],
)
def test_branch_campaign_graph_v2_rejects_scope_and_endpoint_faults(mutate, message):
    root = _object(1, "persisted-root")
    child = _object(2, "persisted-child")
    edge = _lineage(3, "persisted-edge", root, child)
    mutate(root, child, edge)
    with pytest.raises(graphs.DagV2GraphContractError, match=message):
        graphs.build_graph_v2_payload(
            root,
            record_type="instance",
            service_id="local-service",
            depth=2,
            max_nodes=5,
        )


def test_branch_campaign_graph_v2_allows_approved_tenant_to_global_xrf():
    root = _object(1, "persisted-root")
    xrf = _object(
        2,
        XRF_EUID,
        tenant=None,
        typed=True,
        properties={
            "target_service_id": "remote-service",
            "target_object_euid": REMOTE_EUID,
        },
    )
    _lineage(
        3,
        LINEAGE_EUID,
        root,
        xrf,
        properties={
            "approved_global_link": True,
            "asserted_at": "2026-01-01T00:00:00+00:00",
            "assertion_provenance": "branch-campaign",
        },
    )
    graph = graphs.build_graph_v2_payload(
        root,
        record_type="instance",
        service_id="local-service",
        depth=2,
        max_nodes=5,
    )
    assert len(graph["elements"]["edges"]) == 1


def test_branch_campaign_v2_edge_requires_persisted_endpoint_euids():
    parent = _object(1, "persisted-parent")
    child = _object(2, "")
    edge = _lineage(3, "persisted-edge", parent, child)
    with pytest.raises(graphs.DagV2GraphContractError, match="persisted EUIDs"):
        graphs._v2_edge(edge, service_id="local-service")


def test_branch_campaign_graph_cycle_and_legacy_empty_paths():
    root = _object(1, "persisted-root")
    child = _object(2, "persisted-child")
    _lineage(3, "edge-forward", root, child)
    _lineage(4, "edge-back", child, root)
    with pytest.raises(graphs.DagV2GraphContractError, match="Cycle"):
        graphs.build_graph_v2_payload(
            root,
            record_type="instance",
            service_id="local-service",
            depth=3,
            max_nodes=5,
        )

    orphan_edge = SimpleNamespace(parent_instance=None, child_instance=child)
    assert graphs._lineage_edge_payload(orphan_edge, service_name="local") is None
    no_id = _object(5, "")
    legacy = graphs.build_graph_payload(
        no_id, record_type="instance", service_name="local", depth=1
    )
    assert len(legacy["elements"]["nodes"]) == 1
