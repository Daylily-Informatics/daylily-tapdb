"""Typed external-reference helpers and the separately gated DAG v1 proxy.

``tapdb.dag_v2`` only projects references backed by a persisted External Object
Reference instance and ``generic_instance_lineage``.  The older metadata-driven
resolver remains available solely to an explicitly configured v1 proxy; it is
never consulted by the v2 graph surface.
"""

from __future__ import annotations

import http.client
import ipaddress
import json
import re
import socket
import ssl
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any
from urllib.parse import urlencode, urljoin, urlsplit

from fastapi import Request
from sqlalchemy.exc import IntegrityError

from daylily_tapdb.euid import validate_euid
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template

ALLOWED_AUTH_MODES = {"none", "same_origin"}
EXTERNAL_REFERENCE_TEMPLATE_CODE = "reference/external_identifier/tapdb_object/1.0/"
EXTERNAL_REFERENCE_IDENTITY_NAMESPACE = "tapdb.external-reference/v1"
_EXTERNAL_REFERENCE_COORDS = (
    "reference",
    "external_identifier",
    "tapdb_object",
    "1.0",
)
TYPED_EXTERNAL_IDENTIFIER_MARKERS = {
    "external_identifier",
    "external_id",
    "external_reference",
    "tapdb_external_identifier",
}
_MAX_V1_RESOLVED_ENDPOINTS = 8
_SERVICE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


@dataclass(frozen=True)
class ExternalGraphRef:
    """Normalized external graph reference for UI and proxy routes."""

    label: str
    system: str
    root_euid: str
    tenant_id: str | None
    href: str | None
    graph_expandable: bool
    reason: str | None
    base_url: str | None
    graph_data_path: str | None
    object_detail_path_template: str | None
    auth_mode: str
    relationship_type: str | None = None
    source_field: str | None = None

    def to_public_dict(self, *, ref_index: int) -> dict[str, Any]:
        payload = {
            "label": self.label,
            "system": self.system,
            "root_euid": self.root_euid,
            "tenant_id": self.tenant_id,
            "href": self.href,
            "graph_expandable": self.graph_expandable,
            "ref_index": ref_index,
        }
        if self.relationship_type:
            payload["relationship_type"] = self.relationship_type
        if self.source_field:
            payload["source_field"] = self.source_field
        if self.reason:
            payload["reason"] = self.reason
        return payload


@dataclass(frozen=True)
class V1ProxyPolicy:
    """Explicit network boundary for the legacy, metadata-driven proxy."""

    allowed_hosts: frozenset[str]
    timeout_seconds: float = 5.0
    max_response_bytes: int = 1_048_576

    def __post_init__(self) -> None:
        normalized = frozenset(_require_dns_name(item) for item in self.allowed_hosts)
        if not normalized:
            raise ValueError("v1 proxy requires at least one explicitly allowed host")
        if not (0 < float(self.timeout_seconds) <= 10):
            raise ValueError("v1 proxy timeout_seconds must be > 0 and <= 10")
        if not (0 < int(self.max_response_bytes) <= 5_242_880):
            raise ValueError("v1 proxy max_response_bytes must be > 0 and <= 5242880")
        object.__setattr__(self, "allowed_hosts", normalized)


@dataclass(frozen=True)
class _V1ProxyTarget:
    """One validated URL bound to the exact public socket endpoints resolved."""

    host: str
    port: int
    host_header: str
    request_target: str
    endpoints: tuple[tuple[int, int, int, tuple[Any, ...]], ...]


@dataclass(frozen=True)
class TypedExternalReferenceSpec:
    """Caller-supplied descriptor for one persisted foreign object."""

    target_service_id: str
    target_object_euid: str
    relationship_type: str
    asserted_at: datetime
    assertion_provenance: str
    target_tenant_id: str | None = None
    target_object_kind: str | None = None

    def __post_init__(self) -> None:
        _require_service_id(self.target_service_id)
        _require_persisted_euid(self.target_object_euid, "target_object_euid")
        _require_token(self.relationship_type, "relationship_type", max_length=128)
        if self.target_tenant_id is not None:
            _require_tenant_id(self.target_tenant_id)
        if self.target_object_kind is not None:
            _require_token(
                self.target_object_kind, "target_object_kind", max_length=128
            )
        _require_token(
            self.assertion_provenance, "assertion_provenance", max_length=512
        )
        _normalize_asserted_at(self.asserted_at)

    @property
    def identity_key(self) -> str:
        return (
            f"{EXTERNAL_REFERENCE_IDENTITY_NAMESPACE}:"
            f"{self.target_service_id}:{self.target_object_euid}"
        )


@dataclass(frozen=True)
class TypedExternalReferenceResult:
    """Persisted XRF and authoritative local-to-XRF lineage result."""

    reference: generic_instance
    lineage: generic_instance_lineage
    created: bool


class UntypedExternalReferenceError(ValueError):
    """Raised when v2 input attempts to federate untyped metadata."""


def _require_token(value: Any, field: str, *, max_length: int) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    text = value
    if text != text.strip() or not text or len(text) > max_length:
        raise ValueError(f"{field} must be non-empty, exact, and <= {max_length} chars")
    if any(ord(char) < 32 or ord(char) == 127 for char in text):
        raise ValueError(f"{field} must not contain control characters")
    return text


def _require_service_id(value: Any) -> str:
    text = _require_token(value, "target_service_id", max_length=128)
    if _SERVICE_ID_RE.fullmatch(text) is None:
        raise ValueError(
            "target_service_id must start with an alphanumeric character and "
            "contain only alphanumerics, '.', '_' or '-'"
        )
    return text


def _require_external_identifier(
    value: Any, field: str, *, max_length: int = 255
) -> str:
    text = _require_token(value, field, max_length=max_length)
    if any(char.isspace() for char in text):
        raise ValueError(f"{field} must not contain whitespace")
    return text


def _require_persisted_euid(value: Any, field: str) -> str:
    text = _require_external_identifier(value, field)
    if not validate_euid(text):
        raise ValueError(f"{field} must be a canonical Meridian EUID")
    return text


def _require_tenant_id(value: Any) -> str:
    text = _require_external_identifier(value, "target_tenant_id", max_length=36)
    try:
        canonical = str(uuid.UUID(text))
    except ValueError as exc:
        raise ValueError("target_tenant_id must be a canonical UUID") from exc
    if text != canonical:
        raise ValueError("target_tenant_id must be a canonical UUID")
    return text


def _normalize_asserted_at(value: Any) -> str:
    """Return one timezone-aware ISO-8601 assertion timestamp."""

    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = _require_token(value, "asserted_at", max_length=128)
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError("asserted_at must be a valid ISO-8601 timestamp") from exc
    else:
        raise ValueError("asserted_at must be a datetime or ISO-8601 string")
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError("asserted_at must include a timezone")
    return parsed.isoformat()


def _properties(obj: Any) -> dict[str, Any]:
    json_addl = getattr(obj, "json_addl", None)
    if not isinstance(json_addl, dict):
        return {}
    properties = json_addl.get("properties")
    return dict(properties) if isinstance(properties, dict) else {}


def _active_lineages(value: Any) -> list[Any]:
    if value is None:
        return []
    query = value.filter_by(is_deleted=False) if hasattr(value, "filter_by") else value
    return list(query.all() if hasattr(query, "all") else query)


def is_typed_external_reference(obj: Any) -> bool:
    """Return whether an instance has the exact reserved XRF coordinates."""

    coordinates_match = (
        getattr(obj, "category", None),
        getattr(obj, "type", None),
        getattr(obj, "subtype", None),
        getattr(obj, "version", None),
    ) == _EXTERNAL_REFERENCE_COORDS
    template = getattr(obj, "parent_template", None)
    if not coordinates_match or template is None:
        return False
    if getattr(template, "domain_code", None) != getattr(obj, "domain_code", None):
        return False
    if getattr(template, "issuer_app_code", None) != getattr(
        obj, "issuer_app_code", None
    ):
        return False
    return _is_exact_xrf_template(template)


@lru_cache(maxsize=1)
def _canonical_xrf_template_definition() -> dict[str, Any]:
    from daylily_tapdb.templates.loader import (
        find_tapdb_core_config_dir,
        load_template_configs,
    )

    for template in load_template_configs(find_tapdb_core_config_dir()):
        if (
            template.get("category"),
            template.get("type"),
            template.get("subtype"),
            template.get("version"),
            template.get("instance_prefix"),
        ) == (*_EXTERNAL_REFERENCE_COORDS, "XRF"):
            return dict(template)
    raise RuntimeError("Installed TapDB core inventory has no exact XRF template")


def _is_exact_xrf_template(template: Any) -> bool:
    canonical = _canonical_xrf_template_definition()
    for field in (
        "name",
        "polymorphic_discriminator",
        "category",
        "type",
        "subtype",
        "version",
        "instance_prefix",
        "is_singleton",
        "bstatus",
        "json_addl",
    ):
        if getattr(template, field, None) != canonical.get(field):
            return False
    return True


def validate_no_untyped_federation_metadata(obj: Any) -> None:
    """Fail when an ordinary v2 node carries metadata pretending to be an edge."""

    if is_typed_external_reference(obj):
        return
    properties = _properties(obj)
    external_payload = properties.get("external_payload")
    has_raw_graph = isinstance(external_payload, dict) and bool(
        external_payload.get("tapdb_graph")
    )
    copied_identifier_fields = {
        key
        for key in properties
        if key in {"object_euid", "target_object_euid"} or key.endswith("_object_euid")
    }
    if has_raw_graph or copied_identifier_fields:
        fields = sorted(copied_identifier_fields)
        if has_raw_graph:
            fields.append("external_payload.tapdb_graph")
        raise UntypedExternalReferenceError(
            "tapdb.dag_v2 requires a typed External Object Reference connected "
            "by generic_instance_lineage; non-authoritative field(s): "
            + ", ".join(fields)
        )


def project_outbound_typed_references(obj: Any) -> list[dict[str, Any]]:
    """Project only authoritative local-object -> typed-XRF lineage relations."""

    validate_no_untyped_federation_metadata(obj)
    projected: list[dict[str, Any]] = []
    for lineage in _active_lineages(getattr(obj, "parent_of_lineages", None)):
        reference = getattr(lineage, "child_instance", None)
        if reference is None or not is_typed_external_reference(reference):
            continue
        properties = _properties(reference)
        target_service_id = _require_service_id(properties.get("target_service_id"))
        target_object_euid = _require_persisted_euid(
            properties.get("target_object_euid"), "target_object_euid"
        )
        relationship_type = _require_token(
            str(getattr(lineage, "relationship_type", "") or ""),
            "relationship_type",
            max_length=128,
        )
        lineage_properties = _properties(lineage)
        projection = {
            "target_service_id": target_service_id,
            "target_object_euid": target_object_euid,
            "relationship_type": relationship_type,
            "asserted_at": _normalize_asserted_at(
                lineage_properties.get("asserted_at")
            ),
            "assertion_provenance": _require_token(
                lineage_properties.get("assertion_provenance"),
                "assertion_provenance",
                max_length=512,
            ),
            "external_reference_euid": _require_persisted_euid(
                getattr(reference, "euid", None), "external_reference_euid"
            ),
            "lineage_euid": _require_persisted_euid(
                getattr(lineage, "euid", None), "lineage_euid"
            ),
        }
        if properties.get("target_tenant_id"):
            projection["target_tenant_id"] = _require_tenant_id(
                properties["target_tenant_id"]
            )
        if properties.get("target_object_kind"):
            projection["target_object_kind"] = _require_token(
                properties["target_object_kind"],
                "target_object_kind",
                max_length=128,
            )
        projected.append(projection)
    projected.sort(
        key=lambda item: (
            str(item["target_service_id"]),
            str(item["target_object_euid"]),
            str(item["relationship_type"]),
        )
    )
    return projected


def _find_external_reference_template(session: Any) -> generic_template:
    template = (
        session.query(generic_template)
        .filter_by(
            category=_EXTERNAL_REFERENCE_COORDS[0],
            type=_EXTERNAL_REFERENCE_COORDS[1],
            subtype=_EXTERNAL_REFERENCE_COORDS[2],
            version=_EXTERNAL_REFERENCE_COORDS[3],
            instance_prefix="XRF",
            is_deleted=False,
        )
        .one_or_none()
    )
    if template is None or not _is_exact_xrf_template(template):
        raise RuntimeError(
            "The exact bundled External Object Reference template is not seeded"
        )
    return template


def create_or_reuse_typed_external_reference(
    session: Any,
    *,
    source: generic_instance,
    spec: TypedExternalReferenceSpec,
    instance_factory: Any,
) -> TypedExternalReferenceResult:
    """Claim one typed XRF and ensure one authoritative outbound lineage.

    The factory's #93 natural-identity API allocates the XRF.  This function
    never fabricates or rewrites an EUID; both returned identifiers must have
    been assigned by the database before this function returns.
    """

    if getattr(source, "uid", None) is None or not getattr(source, "euid", None):
        raise ValueError("source must be a persisted TapDB instance")
    if not hasattr(instance_factory, "claim_instance_by_identity"):
        raise RuntimeError(
            "InstanceFactory.claim_instance_by_identity is required for typed XRFs"
        )
    _find_external_reference_template(session)
    source_tenant = getattr(source, "tenant_id", None)
    asserted_at = _normalize_asserted_at(spec.asserted_at)
    properties = {
        "target_service_id": spec.target_service_id,
        "target_object_euid": spec.target_object_euid,
        "target_tenant_id": spec.target_tenant_id,
        "target_object_kind": spec.target_object_kind,
    }
    claim = instance_factory.claim_instance_by_identity(
        session,
        template_code=EXTERNAL_REFERENCE_TEMPLATE_CODE,
        identity_key=spec.identity_key,
        name=f"External reference to {spec.target_service_id}",
        properties=properties,
        claimant_tenant_id=None,
        command_evidence={"contract": EXTERNAL_REFERENCE_IDENTITY_NAMESPACE},
        create_children=False,
    )
    reference = getattr(claim, "instance", None)
    if reference is None:
        reference = getattr(claim, "value", None)
    if reference is None or not getattr(reference, "euid", None):
        raise RuntimeError("Natural-identity claim did not return a persisted instance")
    if getattr(reference, "tenant_id", None) is not None:
        raise RuntimeError("Typed external-reference identity must be global")
    existing_properties = _properties(reference)
    if any(existing_properties.get(key) != value for key, value in properties.items()):
        raise ValueError(
            "Existing typed external-reference identity has divergent target metadata"
        )

    lineage = (
        session.query(generic_instance_lineage)
        .filter_by(
            parent_instance_uid=source.uid,
            child_instance_uid=reference.uid,
            relationship_type=spec.relationship_type,
            is_deleted=False,
        )
        .one_or_none()
    )
    if lineage is None:
        candidate = generic_instance_lineage(
            name=f"{source.euid}->{reference.euid}",
            tenant_id=source_tenant,
            polymorphic_discriminator="generic_instance_lineage",
            category="generic",
            type="lineage",
            subtype="external_reference",
            version="1.0.0",
            bstatus="active",
            parent_instance_uid=source.uid,
            child_instance_uid=reference.uid,
            relationship_type=spec.relationship_type,
            parent_type=source.polymorphic_discriminator,
            child_type=reference.polymorphic_discriminator,
            json_addl={
                "properties": {
                    "asserted_at": asserted_at,
                    "assertion_provenance": spec.assertion_provenance,
                    "approved_global_link": True,
                }
            },
        )
        try:
            with session.begin_nested():
                session.add(candidate)
                session.flush()
            lineage = candidate
        except IntegrityError:
            lineage = (
                session.query(generic_instance_lineage)
                .filter_by(
                    parent_instance_uid=source.uid,
                    child_instance_uid=reference.uid,
                    relationship_type=spec.relationship_type,
                    is_deleted=False,
                )
                .one_or_none()
            )
            if lineage is None:
                raise RuntimeError(
                    "Concurrent typed external-reference lineage was not visible"
                )
    else:
        existing_assertion = _properties(lineage)
        expected_assertion = {
            "asserted_at": asserted_at,
            "assertion_provenance": spec.assertion_provenance,
        }
        if any(
            existing_assertion.get(key) != value
            for key, value in expected_assertion.items()
        ):
            raise ValueError(
                "Existing typed external-reference lineage has divergent assertion metadata"
            )
    if not getattr(lineage, "euid", None):
        raise RuntimeError("Typed external-reference lineage was not persisted")
    outcome = str(getattr(claim, "outcome", "")).lower()
    return TypedExternalReferenceResult(
        reference=reference,
        lineage=lineage,
        created=outcome.endswith("created"),
    )


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _compose_object_href(
    *,
    base_url: str,
    object_detail_path_template: str,
    root_euid: str,
) -> str:
    template = _clean(object_detail_path_template)
    if not template:
        return ""
    if "{euid}" in template:
        relative = template.replace("{euid}", root_euid)
    else:
        relative = template.rstrip("/") + f"/{root_euid}"
    return urljoin(base_url.rstrip("/") + "/", relative.lstrip("/"))


def _external_ref_from_item(raw: Any) -> ExternalGraphRef:
    item = _as_dict(raw)
    system = _clean(
        item.get("system")
        or item.get("target_system")
        or item.get("service")
        or item.get("service_id")
    )
    root_euid = _clean(
        item.get("root_euid")
        or item.get("target_euid")
        or item.get("remote_euid")
        or item.get("value")
        or item.get("euid")
    )
    tenant_id = _clean(item.get("tenant_id")) or None
    base_url = _clean(item.get("base_url")) or None
    graph_data_path = _clean(item.get("graph_data_path")) or None
    object_detail_path_template = (
        _clean(item.get("object_detail_path_template")) or None
    )
    auth_mode = _clean(item.get("auth_mode")) or "none"
    relationship_type = _clean(item.get("relationship_type")) or None
    source_field = _clean(item.get("source_field")) or None
    href = _clean(item.get("href")) or None
    if not href and base_url and object_detail_path_template and root_euid:
        href = _compose_object_href(
            base_url=base_url,
            object_detail_path_template=object_detail_path_template,
            root_euid=root_euid,
        )

    graph_expandable = True
    reason: str | None = None
    missing: list[str] = []
    if not system:
        missing.append("system")
    if not root_euid:
        missing.append("root_euid")
    if not base_url:
        missing.append("base_url")
    if not graph_data_path:
        missing.append("graph_data_path")
    if not object_detail_path_template:
        missing.append("object_detail_path_template")
    if auth_mode not in ALLOWED_AUTH_MODES:
        missing.append("auth_mode")
    if missing:
        graph_expandable = False
        reason = "Missing required graph metadata: " + ", ".join(missing)

    label = _clean(item.get("label")) or (
        f"{system}:{root_euid}" if system and root_euid else "external reference"
    )
    return ExternalGraphRef(
        label=label,
        system=system or "external",
        root_euid=root_euid,
        tenant_id=tenant_id,
        href=href,
        graph_expandable=graph_expandable,
        reason=reason,
        base_url=base_url,
        graph_data_path=graph_data_path,
        object_detail_path_template=object_detail_path_template,
        auth_mode=auth_mode,
        relationship_type=relationship_type,
        source_field=source_field,
    )


def _typed_external_identifier_items(obj: Any, properties: dict[str, Any]) -> list[Any]:
    raw = (
        properties.get("tapdb_external_identifier")
        or properties.get("external_identifier")
        or properties.get("external_reference")
    )
    if isinstance(raw, list):
        return list(raw)
    if isinstance(raw, dict):
        return [raw]
    markers = {
        _clean(getattr(obj, "category", None)).lower(),
        _clean(getattr(obj, "type", None)).lower(),
        _clean(getattr(obj, "subtype", None)).lower(),
    }
    if markers & TYPED_EXTERNAL_IDENTIFIER_MARKERS:
        return [properties]
    return []


def resolve_external_graph_refs(obj: Any) -> list[ExternalGraphRef]:
    """Parse explicit external graph refs and typed external identifier objects."""

    json_addl = _as_dict(getattr(obj, "json_addl", None))
    properties = _as_dict(json_addl.get("properties"))
    external_payload = _as_dict(properties.get("external_payload"))
    tapdb_graph = external_payload.get("tapdb_graph")
    refs_raw: list[Any]
    if isinstance(tapdb_graph, list):
        refs_raw = list(tapdb_graph)
    elif isinstance(tapdb_graph, dict):
        refs_raw = [tapdb_graph]
    else:
        refs_raw = []
    refs_raw.extend(_typed_external_identifier_items(obj, properties))

    refs: list[ExternalGraphRef] = []
    seen: set[tuple[str, str, str | None]] = set()
    for raw in refs_raw:
        ref = _external_ref_from_item(raw)
        key = (ref.system, ref.root_euid, ref.tenant_id)
        if key in seen:
            continue
        seen.add(key)
        refs.append(ref)

    refs.sort(
        key=lambda ref: (ref.system, ref.label, ref.root_euid, ref.tenant_id or "")
    )
    return refs


def external_ref_payloads(obj: Any) -> list[dict[str, Any]]:
    """Return public payload dictionaries for all explicit external refs."""

    return [
        ref.to_public_dict(ref_index=index)
        for index, ref in enumerate(resolve_external_graph_refs(obj))
    ]


def get_external_ref_by_index(obj: Any, ref_index: int) -> ExternalGraphRef:
    refs = resolve_external_graph_refs(obj)
    if ref_index < 0 or ref_index >= len(refs):
        raise IndexError("External reference not found")
    return refs[ref_index]


def fetch_remote_graph(
    request: Request,
    ref: ExternalGraphRef,
    *,
    depth: int,
    policy: V1ProxyPolicy,
) -> dict[str, Any]:
    """Fetch a v1 remote graph under an explicit, no-credential proxy policy."""

    del request

    if not ref.graph_expandable or not ref.base_url or not ref.graph_data_path:
        raise RuntimeError(ref.reason or "External graph is not expandable")

    params = {"start_euid": ref.root_euid, "depth": int(depth)}
    if ref.tenant_id:
        params["tenant_id"] = ref.tenant_id

    url = urljoin(ref.base_url.rstrip("/") + "/", ref.graph_data_path.lstrip("/"))
    url = f"{url}?{urlencode(params)}"
    return _fetch_v1_json(url, policy=policy, label="Remote graph")


def fetch_remote_object_detail(
    request: Request,
    ref: ExternalGraphRef,
    *,
    euid: str,
    policy: V1ProxyPolicy,
) -> dict[str, Any]:
    """Fetch v1 object detail under an explicit, no-credential proxy policy."""

    del request

    if (
        not ref.graph_expandable
        or not ref.base_url
        or not ref.object_detail_path_template
    ):
        raise RuntimeError(ref.reason or "External object detail is not available")

    url = _compose_object_href(
        base_url=ref.base_url,
        object_detail_path_template=ref.object_detail_path_template,
        root_euid=euid,
    )
    if ref.tenant_id:
        joiner = "&" if "?" in url else "?"
        url = f"{url}{joiner}{urlencode({'tenant_id': ref.tenant_id})}"
    return _fetch_v1_json(url, policy=policy, label="Remote object")


def namespace_external_graph(
    payload: dict[str, Any],
    *,
    ref: ExternalGraphRef,
    ref_index: int,
    source_euid: str,
) -> dict[str, Any]:
    """Namespace remote graph elements so they can be merged safely."""

    elements = _as_dict(payload.get("elements"))
    nodes = _as_list(elements.get("nodes"))
    edges = _as_list(elements.get("edges"))
    namespace = f"ext::{ref.system}::{ref.tenant_id or 'global'}"

    def namespaced_id(raw_id: Any) -> str:
        return f"{namespace}::{_clean(raw_id)}"

    namespaced_nodes: list[dict[str, Any]] = []
    for node in nodes:
        data = _as_dict(_as_dict(node).get("data"))
        remote_euid = _clean(data.get("euid") or data.get("id"))
        if not remote_euid:
            continue
        node_data = dict(data)
        node_data["id"] = namespaced_id(remote_euid)
        node_data["remote_euid"] = remote_euid
        node_data["is_external"] = True
        node_data["external_system"] = ref.system
        node_data["external_tenant_id"] = ref.tenant_id
        node_data["source_ref_index"] = ref_index
        node_data["external_source_euid"] = source_euid
        namespaced_nodes.append({"data": node_data})

    namespaced_edges: list[dict[str, Any]] = []
    for edge in edges:
        data = _as_dict(_as_dict(edge).get("data"))
        remote_edge_id = _clean(data.get("id"))
        source_id = _clean(data.get("source"))
        target_id = _clean(data.get("target"))
        if not remote_edge_id or not source_id or not target_id:
            continue
        edge_data = dict(data)
        edge_data["id"] = namespaced_id(remote_edge_id)
        edge_data["source"] = namespaced_id(source_id)
        edge_data["target"] = namespaced_id(target_id)
        edge_data["remote_euid"] = remote_edge_id
        edge_data["is_external"] = True
        edge_data["external_system"] = ref.system
        edge_data["external_tenant_id"] = ref.tenant_id
        edge_data["source_ref_index"] = ref_index
        edge_data["external_source_euid"] = source_euid
        namespaced_edges.append({"data": edge_data})

    bridge_id = f"bridge::{source_euid}::{ref.system}::{ref.tenant_id or 'global'}::{ref.root_euid}"
    bridge_data = {
        "id": bridge_id,
        "source": source_euid,
        "target": namespaced_id(ref.root_euid),
        "relationship_type": ref.relationship_type or "external_reference",
        "is_external_bridge": True,
        "external_system": ref.system,
        "external_tenant_id": ref.tenant_id,
        "source_ref_index": ref_index,
        "external_source_euid": source_euid,
    }
    if ref.source_field:
        bridge_data["source_field"] = ref.source_field
    namespaced_edges.append({"data": bridge_data})

    return {
        "elements": {"nodes": namespaced_nodes, "edges": namespaced_edges},
        "meta": {
            "source_euid": source_euid,
            "root_euid": ref.root_euid,
            "system": ref.system,
            "tenant_id": ref.tenant_id,
            "ref_index": ref_index,
            "node_count": len(namespaced_nodes),
            "edge_count": len(namespaced_edges),
        },
    }


def _require_dns_name(value: str) -> str:
    host = str(value or "")
    if host != host.strip() or not host or len(host) > 253:
        raise ValueError("allowed v1 proxy hosts must be exact DNS names")
    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        raise ValueError("allowed v1 proxy hosts must not be IP literals")
    labels = host.lower().split(".")
    if any(
        not label
        or len(label) > 63
        or label[0] == "-"
        or label[-1] == "-"
        or any(
            not (char.isascii() and (char.isalnum() or char == "-")) for char in label
        )
        for label in labels
    ):
        raise ValueError("allowed v1 proxy hosts must be valid DNS names")
    return host.lower()


def _require_public_resolution(
    host: str, port: int, *, timeout_seconds: float = 5.0
) -> tuple[tuple[int, int, int, tuple[Any, ...]], ...]:
    executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="tapdb-v1-dns")
    try:
        future = executor.submit(socket.getaddrinfo, host, port, 0, socket.SOCK_STREAM)
        infos = future.result(timeout=timeout_seconds)
    except FutureTimeoutError as exc:
        raise RuntimeError(f"v1 proxy host resolution timed out: {host}") from exc
    except OSError as exc:
        raise RuntimeError(f"v1 proxy host resolution failed: {host}") from exc
    finally:
        executor.shutdown(wait=False, cancel_futures=True)
    if not infos:
        raise RuntimeError(f"v1 proxy host resolution returned no addresses: {host}")
    for _family, _socktype, _protocol, _canonical_name, sockaddr in infos:
        raw = sockaddr[0]
        address = ipaddress.ip_address(raw)
        if not address.is_global:
            raise RuntimeError(
                f"v1 proxy host resolved to a non-public address: {host}"
            )
    endpoints: list[tuple[int, int, int, tuple[Any, ...]]] = []
    seen: set[tuple[int, int, int, tuple[Any, ...]]] = set()
    for family, socktype, protocol, _canonical_name, sockaddr in infos:
        endpoint = (family, socktype, protocol, tuple(sockaddr))
        if endpoint not in seen:
            seen.add(endpoint)
            endpoints.append(endpoint)
    if len(endpoints) > _MAX_V1_RESOLVED_ENDPOINTS:
        raise RuntimeError(
            f"v1 proxy host resolved to more than {_MAX_V1_RESOLVED_ENDPOINTS} endpoints"
        )
    return tuple(endpoints)


def _require_v1_proxy_url(
    url: str, *, policy: V1ProxyPolicy, resolution_timeout: float | None = None
) -> _V1ProxyTarget:
    parts = urlsplit(url)
    if parts.scheme != "https" or not parts.netloc:
        raise RuntimeError("TapDB v1 proxy requires an absolute https URL")
    if parts.username or parts.password or parts.fragment:
        raise RuntimeError(
            "TapDB v1 proxy URL must not contain credentials or fragments"
        )
    host = (parts.hostname or "").lower()
    if host not in policy.allowed_hosts:
        raise RuntimeError("TapDB v1 proxy target host is not explicitly allowed")
    try:
        port = parts.port or 443
    except ValueError as exc:
        raise RuntimeError("TapDB v1 proxy URL has an invalid port") from exc
    endpoints = _require_public_resolution(
        host,
        port,
        timeout_seconds=(
            float(policy.timeout_seconds)
            if resolution_timeout is None
            else resolution_timeout
        ),
    )
    request_target = parts.path or "/"
    if parts.query:
        request_target += f"?{parts.query}"
    return _V1ProxyTarget(
        host=host,
        port=port,
        host_header=parts.netloc,
        request_target=request_target,
        endpoints=endpoints,
    )


class _PinnedHTTPSConnection(http.client.HTTPSConnection):
    """HTTPS connection that never resolves the validated hostname again."""

    def __init__(
        self,
        target: _V1ProxyTarget,
        endpoint: tuple[int, int, int, tuple[Any, ...]],
        *,
        timeout: float,
    ) -> None:
        self._tapdb_ssl_context = ssl.create_default_context()
        super().__init__(
            target.host,
            target.port,
            timeout=timeout,
            context=self._tapdb_ssl_context,
        )
        self._tapdb_endpoint = endpoint

    def connect(self) -> None:
        family, socktype, protocol, sockaddr = self._tapdb_endpoint
        raw_socket = socket.socket(family, socktype, protocol)
        try:
            raw_socket.settimeout(self.timeout)
            raw_socket.connect(sockaddr)
            raw_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.sock = self._tapdb_ssl_context.wrap_socket(
                raw_socket,
                server_hostname=self.host,
            )
        except Exception:
            raw_socket.close()
            raise


def _open_pinned_https(
    target: _V1ProxyTarget,
    endpoint: tuple[int, int, int, tuple[Any, ...]],
    *,
    timeout: float,
) -> tuple[_PinnedHTTPSConnection, http.client.HTTPResponse]:
    connection = _PinnedHTTPSConnection(target, endpoint, timeout=timeout)
    try:
        connection.request(
            "GET",
            target.request_target,
            headers={"Accept": "application/json", "Host": target.host_header},
        )
        return connection, connection.getresponse()
    except Exception:
        connection.close()
        raise


def _fetch_v1_json(
    url: str,
    *,
    policy: V1ProxyPolicy,
    label: str,
) -> dict[str, Any]:
    deadline = time.monotonic() + float(policy.timeout_seconds)
    target = _require_v1_proxy_url(
        url,
        policy=policy,
        resolution_timeout=max(0.001, deadline - time.monotonic()),
    )
    last_connection_error: Exception | None = None
    raw: bytes | None = None
    for endpoint in target.endpoints:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            last_connection_error = TimeoutError("v1 proxy deadline expired")
            break
        connection: _PinnedHTTPSConnection | None = None
        try:
            connection, response = _open_pinned_https(
                target,
                endpoint,
                timeout=remaining,
            )
            status = int(response.status)
            if 300 <= status < 400:
                raise RuntimeError("TapDB v1 proxy does not follow redirects")
            if status < 200 or status >= 300:
                raise RuntimeError(f"{label} returned HTTP {status}")
            content_type = str(response.headers.get("Content-Type") or "").lower()
            if content_type.split(";", 1)[0].strip() != "application/json":
                raise RuntimeError(f"{label} response must use application/json")
            content_length = str(response.headers.get("Content-Length") or "").strip()
            if content_length:
                try:
                    declared_length = int(content_length)
                except ValueError as exc:
                    raise RuntimeError(
                        f"{label} response has an invalid Content-Length"
                    ) from exc
                if declared_length < 0:
                    raise RuntimeError(
                        f"{label} response has an invalid Content-Length"
                    )
                if declared_length > policy.max_response_bytes:
                    raise RuntimeError(
                        f"{label} response exceeds the configured size limit"
                    )
            chunks: list[bytes] = []
            bytes_read = 0
            while bytes_read <= policy.max_response_bytes:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError("v1 proxy deadline expired")
                sock = getattr(connection, "sock", None)
                if sock is not None:
                    sock.settimeout(remaining)
                chunk = response.read(
                    min(65_536, policy.max_response_bytes + 1 - bytes_read)
                )
                if not chunk:
                    break
                chunks.append(chunk)
                bytes_read += len(chunk)
            raw = b"".join(chunks)
            break
        except (OSError, ssl.SSLError, TimeoutError, http.client.HTTPException) as exc:
            last_connection_error = exc
        finally:
            if connection is not None:
                connection.close()
    if raw is None:
        raise RuntimeError(f"{label} connection failed") from last_connection_error
    if len(raw) > policy.max_response_bytes:
        raise RuntimeError(f"{label} response exceeds the configured size limit")
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"{label} response must be valid UTF-8 JSON") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"{label} response must be a JSON object")
    return payload
