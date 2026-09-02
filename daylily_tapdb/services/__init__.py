"""Shared service helpers for TAPDB reusable API and admin surfaces."""

from daylily_tapdb.services.external_refs import (
    ALLOWED_AUTH_MODES,
    ExternalGraphRef,
    TypedExternalReferenceResult,
    TypedExternalReferenceSpec,
    UntypedExternalReferenceError,
    V1ProxyPolicy,
    create_or_reuse_typed_external_reference,
    external_ref_payloads,
    fetch_remote_graph,
    fetch_remote_object_detail,
    get_external_ref_by_index,
    namespace_external_graph,
    project_outbound_typed_references,
    resolve_external_graph_refs,
)
from daylily_tapdb.services.graph_payloads import (
    DagV2GraphContractError,
    build_graph_payload,
    build_graph_v2_payload,
    build_object_detail_payload,
    build_object_detail_v2_payload,
)
from daylily_tapdb.services.object_lookup import find_object_by_euid
from daylily_tapdb.services.object_search import search_objects

__all__ = [
    "ALLOWED_AUTH_MODES",
    "ExternalGraphRef",
    "TypedExternalReferenceResult",
    "TypedExternalReferenceSpec",
    "UntypedExternalReferenceError",
    "V1ProxyPolicy",
    "DagV2GraphContractError",
    "build_graph_payload",
    "build_graph_v2_payload",
    "build_object_detail_payload",
    "build_object_detail_v2_payload",
    "create_or_reuse_typed_external_reference",
    "external_ref_payloads",
    "fetch_remote_graph",
    "fetch_remote_object_detail",
    "find_object_by_euid",
    "get_external_ref_by_index",
    "namespace_external_graph",
    "project_outbound_typed_references",
    "resolve_external_graph_refs",
    "search_objects",
]
