"""Reusable DAG payload builders."""

from __future__ import annotations

import hashlib
import json
from collections import deque
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import or_
from sqlalchemy.orm import joinedload, load_only, selectinload

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.services.graph_errors import (
    add_issue, prune_stopped_branches, relationship_error,
    retain_acyclic_edges, stop_node,
)

from daylily_tapdb.external_references import (
    ExternalReferenceContractError,
    OPAQUE_IDENTIFIER_TEMPLATE_CODE,
    TAPDB_OBJECT_TEMPLATE_CODE,
    _is_xrf_coordinates,
    _project_outbound_external_references,
    _target_from_reference,
)


def _isoformat_attr(obj: Any, attr: str) -> str | None:
    value = getattr(obj, attr, None)
    return value.isoformat() if value is not None else None


class DagV2GraphContractError(ValueError):
    """Raised when persisted lineage cannot be represented as a safe DAG v2."""


def _unsupported_reference_boundary(obj: Any) -> dict[str, str] | None:
    """Recognize a typed reference family without interpreting its old payload."""
    coordinates = tuple(str(getattr(obj, field, "") or "") for field in (
        "category", "type", "subtype", "version",
    ))
    for code in (TAPDB_OBJECT_TEMPLATE_CODE, OPAQUE_IDENTIFIER_TEMPLATE_CODE):
        current = tuple(code.rstrip("/").split("/"))
        if coordinates[:3] == current[:3] and coordinates[3] != current[3]:
            return {
                "code": "unsupported_external_reference",
                "status": "not_traversed",
                "reference_version": coordinates[3],
                "supported_version": current[3],
                "message": (
                    f"External reference version {coordinates[3]} is unsupported; "
                    f"not traversed. Canonical version {current[3]} is required."
                ),
            }
    return None


def _boundary_warnings(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"service_id": node["data"]["service_id"], "euid": node["data"]["euid"],
         **node["data"]["traversal_boundary"]}
        for node in nodes if node["data"].get("traversal_boundary")
    ] + [
        {"service_id": node["data"]["service_id"], "euid": node["data"]["euid"], **issue}
        for node in nodes for issue in node["data"].get("graph_issues", [])
    ]


def _boundary_meta(nodes: list[dict[str, Any]]) -> dict[str, Any]:
    codes = [n["data"]["traversal_boundary"]["code"] for n in nodes
             if n["data"].get("traversal_boundary")]
    issues = sum(len(n["data"].get("graph_issues", [])) for n in nodes)
    return {"partial": bool(codes or issues),
            "unsupported_external_references": codes.count("unsupported_external_reference"),
            "node_errors": sum(code != "unsupported_external_reference" for code in codes),
            "blocked_relationships": issues}


class GraphLineageReader:
    """Request-local adjacency batches through the caller's scoped ORM session.

    Endpoint identity and scope are loaded under the same RLS context. Large
    properties are loaded only for the retained graph and its typed references.
    No graph or authorization data survives the request.
    """

    batch_size = 128

    def __init__(self, session: Any):
        self.session = session
        self.outgoing: dict[int, list[Any]] = {}
        self.incoming: dict[int, list[Any]] = {}

    def prefetch(self, objects: list[Any]) -> None:
        pending = sorted({int(obj.uid) for obj in objects} - self.outgoing.keys())
        for offset in range(0, len(pending), self.batch_size):
            uids = pending[offset:offset + self.batch_size]
            for uid in uids:
                self.outgoing[uid] = []
                self.incoming[uid] = []
            model = generic_instance_lineage
            endpoint = generic_instance
            endpoint_fields = [getattr(endpoint, name) for name in (
                "uid", "euid", "domain_code", "issuer_app_code", "tenant_id",
                "is_deleted", "category", "type", "subtype", "version",
            )]
            rows = (
                self.session.query(model)
                .filter(model.is_deleted.is_(False), or_(
                    model.parent_instance_uid.in_(uids),
                    model.child_instance_uid.in_(uids),
                ))
                .options(
                    load_only(model.uid, model.euid, model.parent_instance_uid,
                              model.child_instance_uid, model.domain_code,
                              model.issuer_app_code, model.tenant_id, model.is_deleted),
                    joinedload(model.parent_instance).load_only(*endpoint_fields),
                    joinedload(model.child_instance).load_only(*endpoint_fields),
                )
                .order_by(model.euid)
                .all()
            )
            selected = set(uids)
            for row in rows:
                if row.parent_instance_uid in selected:
                    self.outgoing[row.parent_instance_uid].append(row)
                if row.child_instance_uid in selected:
                    self.incoming[row.child_instance_uid].append(row)

    def materialize(self, nodes: list[Any], edges: list[Any]) -> None:
        """Hydrate retained payloads without decoding every boundary neighbor.

        Outbound typed references remain part of each node's native contract,
        including those beyond the requested traversal depth. Their canonical
        templates and assertion payloads must therefore also be materialized.
        Every batch uses the caller's RLS session and fails on missing rows.
        """
        selected_nodes = {int(node.uid): node for node in nodes}
        selected_edges = {int(edge.uid): edge for edge in edges}
        for node in nodes:
            for edge in self.outgoing.get(int(node.uid), []):
                reference = edge.child_instance
                if reference is not None and _is_xrf_coordinates(reference):
                    selected_nodes[int(reference.uid)] = reference
                    selected_edges[int(edge.uid)] = edge
        for model, selected in (
            (generic_instance, selected_nodes),
            (generic_instance_lineage, selected_edges),
        ):
            uids = sorted(selected)
            for offset in range(0, len(uids), self.batch_size):
                batch = uids[offset:offset + self.batch_size]
                query = self.session.query(model).filter(
                    model.uid.in_(batch), model.is_deleted.is_(False)
                )
                if model is generic_instance:
                    query = query.options(selectinload(model.parent_template))
                rows = query.all()
                if {int(row.uid) for row in rows} != set(batch):
                    raise DagV2GraphContractError(
                        "Graph payload rows changed or became inaccessible during projection"
                    )


def _dict_attr(obj: Any, attr: str) -> dict[str, Any]:
    value = getattr(obj, attr, None)
    return dict(value) if isinstance(value, dict) else {}


def _properties(obj: Any) -> dict[str, Any]:
    value = _dict_attr(obj, "json_addl").get("properties")
    return dict(value) if isinstance(value, dict) else {}


def _active_related(value: Any) -> list[Any]:
    if value is None:
        return []
    query = value.filter_by(is_deleted=False) if hasattr(value, "filter_by") else value
    rows = query.all() if hasattr(query, "all") else list(query)
    return sorted(
        (row for row in rows if not bool(getattr(row, "is_deleted", False))),
        key=lambda row: str(getattr(row, "euid", "") or ""),
    )


def _clean_public_properties(obj: Any) -> dict[str, Any]:
    properties = _properties(obj)
    public: dict[str, Any] = {}
    for key, value in properties.items():
        if not isinstance(key, str):
            continue
        normalized_key = key.casefold()
        if normalized_key in {
            "external_payload",
            "base_url",
            "auth",
            "auth_mode",
            "graph_data_path",
            "graph_presentation",
            "object_detail_path_template",
        }:
            continue
        if normalized_key.startswith("auth_") or normalized_key.endswith("_url"):
            continue
        public[key] = value
    return public


def _v2_node_presentation(obj: Any) -> dict[str, Any]:
    raw = _properties(obj).get("graph_presentation")
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise DagV2GraphContractError("graph_presentation must be an object")
    presentation: dict[str, Any] = {}
    if "role" in raw:
        role = str(raw["role"] or "")
        if role != role.strip() or not role or len(role) > 64:
            raise DagV2GraphContractError(
                "graph_presentation.role must be exact and 1-64 characters"
            )
        presentation["role"] = role
    if "collapse_by_default" in raw:
        if not isinstance(raw["collapse_by_default"], bool):
            raise DagV2GraphContractError(
                "graph_presentation.collapse_by_default must be boolean"
            )
        presentation["collapse_by_default"] = raw["collapse_by_default"]
    if "expected_fanout" in raw:
        fanout = raw["expected_fanout"]
        if not isinstance(fanout, dict):
            raise DagV2GraphContractError(
                "graph_presentation.expected_fanout must be an object"
            )
        relationship_types = fanout.get("relationship_types")
        max_degree = fanout.get("max_degree")
        reason = str(fanout.get("reason") or "")
        if (
            not isinstance(relationship_types, list)
            or not relationship_types
            or any(
                not isinstance(item, str) or not item or item != item.strip()
                for item in relationship_types
            )
        ):
            raise DagV2GraphContractError(
                "expected_fanout.relationship_types must be a non-empty string list"
            )
        if (
            not isinstance(max_degree, int)
            or isinstance(max_degree, bool)
            or max_degree < 1
        ):
            raise DagV2GraphContractError(
                "expected_fanout.max_degree must be a positive integer"
            )
        if reason != reason.strip() or not reason or len(reason) > 512:
            raise DagV2GraphContractError(
                "expected_fanout.reason must be exact and 1-512 characters"
            )
        presentation["expected_fanout"] = {
            "relationship_types": sorted(set(relationship_types)),
            "max_degree": max_degree,
            "reason": reason,
        }
    unsupported = set(raw) - {"role", "collapse_by_default", "expected_fanout"}
    if unsupported:
        raise DagV2GraphContractError(
            "Unsupported graph_presentation field(s): " + ", ".join(sorted(unsupported))
        )
    return presentation


def _v2_node(
    obj: Any, *, record_type: str, service_id: str,
    outbound_lineages: list[Any] | None = None,
) -> dict[str, Any]:
    boundary = _unsupported_reference_boundary(obj) if record_type == "instance" else None
    euid = getattr(obj, "euid", None)
    data = {
        "id": euid,
        "euid": euid,
        "display_label": getattr(obj, "name", None) or euid,
        "service_id": service_id,
        "record_type": record_type,
        "category": getattr(obj, "category", None),
        "type": getattr(obj, "type", None),
        "subtype": getattr(obj, "subtype", None),
        "version": getattr(obj, "version", None),
        "tenant_id": str(getattr(obj, "tenant_id", "") or "") or None,
        "created_dt": _isoformat_attr(obj, "created_dt"),
        "modified_dt": _isoformat_attr(obj, "modified_dt"),
        "properties": {}, "presentation": {},
        "external_refs": [], "external_identifiers": [],
    }
    if boundary:
        data["traversal_boundary"] = boundary
        data["expandable"] = False
        return {"data": data}
    # Only documented, attributable data errors become a boundary. Database,
    # authorization and programming failures are not caught here.
    raw = getattr(obj, "json_addl", None)
    if raw is not None and (not isinstance(raw, dict) or
            (raw.get("properties") is not None and not isinstance(raw["properties"], dict))):
        return {"data": stop_node(data, "invalid_node_contract")}
    try:
        if record_type == "instance" and _is_xrf_coordinates(obj):
            _target_from_reference(obj)
        _project_outbound_external_references(obj, lineages=[])
    except ExternalReferenceContractError:
        return {"data": stop_node(data, "invalid_external_reference")}
    try:
        data["presentation"] = _v2_node_presentation(obj)
    except DagV2GraphContractError:
        return {"data": stop_node(data, "invalid_node_presentation")}
    data["properties"] = _clean_public_properties(obj)
    lineages = outbound_lineages if outbound_lineages is not None else _active_related(
        getattr(obj, "parent_of_lineages", None))
    for lineage in lineages:
        reference = getattr(lineage, "child_instance", None)
        if reference is None or not _is_xrf_coordinates(reference):
            continue
        try:
            # A malformed reference is isolated at its own node; it must not
            # suppress the source's other canonical reference assertions.
            _target_from_reference(reference)
            external = _project_outbound_external_references(obj, lineages=[lineage])
        except ExternalReferenceContractError:
            add_issue(data, relationship_error("invalid_relationship", lineage.euid))
            continue
        data["external_refs"].extend(external["external_refs"])
        data["external_identifiers"].extend(external["external_identifiers"])
    return {"data": data}


def build_object_detail_v2_payload(
    obj: Any,
    *,
    record_type: str,
    service_id: str,
    outbound_lineages: list[Any] | None = None,
) -> dict[str, Any]:
    """Return safe v2 object detail without legacy routing metadata."""

    return dict(_v2_node(obj, record_type=record_type, service_id=service_id,
                         outbound_lineages=outbound_lineages)["data"])


def _scope_tuple(obj: Any) -> tuple[str, str, str | None]:
    tenant = str(getattr(obj, "tenant_id", "") or "") or None
    return (
        str(getattr(obj, "domain_code", "") or ""),
        str(getattr(obj, "issuer_app_code", "") or ""),
        tenant,
    )


def _validate_lineage_scope(
    root: Any,
    lineage: Any,
    neighbor: Any,
    tenant_scope: frozenset[str | None] | None = None,
) -> None:
    root_domain, root_owner, root_tenant = _scope_tuple(root)
    neighbor_domain, neighbor_owner, neighbor_tenant = _scope_tuple(neighbor)
    lineage_domain, lineage_owner, lineage_tenant = _scope_tuple(lineage)
    if not root_domain or not root_owner:
        raise DagV2GraphContractError("DAG v2 root is missing domain/owner scope")
    if (neighbor_domain, neighbor_owner) != (root_domain, root_owner):
        raise DagV2GraphContractError(
            "Cross-domain or cross-owner lineage is forbidden"
        )
    if (lineage_domain, lineage_owner) != (root_domain, root_owner):
        raise DagV2GraphContractError("Lineage row scope differs from its endpoints")
    if tenant_scope is not None:
        if not {root_tenant, neighbor_tenant, lineage_tenant}.issubset(tenant_scope):
            raise DagV2GraphContractError(
                "Lineage lies outside the configured service tenant scope"
            )
        return
    if neighbor_tenant == root_tenant and lineage_tenant == root_tenant:
        return
    properties = _properties(lineage)
    if (
        bool(properties.get("approved_global_link"))
        and neighbor_tenant is None
        and lineage_tenant == root_tenant
        and _is_xrf_coordinates(neighbor)
    ):
        try:
            _target_from_reference(neighbor)
        except ExternalReferenceContractError as exc:
            raise DagV2GraphContractError(str(exc)) from exc
        return
    raise DagV2GraphContractError(
        "Cross-tenant lineage is forbidden unless it is an approved typed global link"
    )


def _v2_edge(lineage: Any, *, service_id: str) -> dict[str, Any]:
    lineage_euid = getattr(lineage, "euid", None)
    if not isinstance(lineage_euid, str) or not lineage_euid:
        raise DagV2GraphContractError("Lineage must have a persisted EUID")
    parent = getattr(lineage, "parent_instance", None)
    child = getattr(lineage, "child_instance", None)
    if parent is None or child is None:
        raise DagV2GraphContractError("Lineage endpoint could not be resolved")
    parent_euid = str(getattr(parent, "euid", "") or "")
    child_euid = str(getattr(child, "euid", "") or "")
    if not parent_euid or not child_euid:
        raise DagV2GraphContractError("Lineage endpoints must have persisted EUIDs")
    if parent_euid == child_euid or getattr(parent, "uid", None) == getattr(
        child, "uid", None
    ):
        raise DagV2GraphContractError("Self-loop lineage is forbidden")
    properties = _properties(lineage)
    asserted_at = properties.get("asserted_at") or _isoformat_attr(
        lineage, "created_dt"
    )
    provenance = properties.get("assertion_provenance")
    if provenance is None:
        lineage_euid = str(getattr(lineage, "euid", "") or "")
        if not lineage_euid:
            raise DagV2GraphContractError(
                "Lineage must have a persisted EUID for authoritative provenance"
            )
        provenance = f"tapdb.lineage:{lineage_euid}"
    elif (
        not isinstance(provenance, str)
        or not provenance
        or provenance != provenance.strip()
        or len(provenance) > 512
    ):
        raise DagV2GraphContractError(
            "Lineage assertion_provenance must be an exact 1-512 character string"
        )
    return {
        "data": {
            "id": getattr(lineage, "euid", None),
            "euid": getattr(lineage, "euid", None),
            "source": parent_euid,
            "target": child_euid,
            "service_id": service_id,
            "relationship_type": getattr(lineage, "relationship_type", None),
            "presentation": {
                "semantics": getattr(lineage, "relationship_type", None),
                "asserted_at": asserted_at,
                "assertion_provenance": provenance,
                "evidence_refs": properties.get("evidence_refs") or [],
            },
        }
    }


def _assert_acyclic(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> None:
    node_ids = {str(node["data"]["id"]) for node in nodes}
    adjacency: dict[str, list[str]] = {node_id: [] for node_id in node_ids}
    for edge in edges:
        source = str(edge["data"]["source"])
        target = str(edge["data"]["target"])
        if source in adjacency and target in node_ids:
            adjacency[source].append(target)
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visiting:
            raise DagV2GraphContractError("Cycle detected in persisted lineage")
        if node_id in visited:
            return
        visiting.add(node_id)
        for target in sorted(adjacency[node_id]):
            visit(target)
        visiting.remove(node_id)
        visited.add(node_id)

    for node_id in sorted(node_ids):
        visit(node_id)


def _project_graph_edges(nodes, lineages, service_id):
    by_id = {node["data"]["euid"]: node["data"] for node in nodes}
    edges = []
    for lineage in sorted(lineages, key=lambda row: str(row.euid)):
        parent, child = getattr(lineage, "parent_instance", None), getattr(lineage, "child_instance", None)
        source = by_id.get(str(getattr(parent, "euid", "") or ""))
        target = by_id.get(str(getattr(child, "euid", "") or ""))
        if parent is None or child is None:
            visible = source if source is not None else target
            if visible is not None:
                add_issue(visible, relationship_error("unavailable_endpoint", lineage.euid))
            continue
        if source is None or target is None or source.get("traversal_boundary"):
            continue
        # A valid local edge may still display an invalid reference as a leaf.
        if not target.get("traversal_boundary") and any(
            issue.get("lineage_euid") == str(lineage.euid) and issue["code"] == "invalid_relationship"
            for issue in source.get("graph_issues", [])
        ):
            continue
        try:
            edges.append(_v2_edge(lineage, service_id=service_id))
        except DagV2GraphContractError:
            add_issue(source, relationship_error("invalid_relationship", lineage.euid))
    return edges


def build_graph_v2_payload(
    obj: Any,
    *,
    record_type: str,
    service_id: str,
    depth: int,
    max_nodes: int,
    max_edges: int | None = None,
    snapshot_at: datetime | None = None,
    tenant_scope: frozenset[str | None] | None = None,
    lineage_reader: GraphLineageReader | None = None,
) -> dict[str, Any]:
    """Build one bounded, validated, lineage-only DAG v2 snapshot."""

    if not isinstance(depth, int) or isinstance(depth, bool) or depth < 0:
        raise ValueError("depth must be a non-negative integer")
    if not isinstance(max_nodes, int) or isinstance(max_nodes, bool) or max_nodes < 1:
        raise ValueError("max_nodes must be a positive integer")
    if max_edges is not None and (
        not isinstance(max_edges, int) or isinstance(max_edges, bool) or max_edges < 1
    ):
        raise ValueError("max_edges must be a positive integer or None")
    snapshot = snapshot_at or datetime.now(timezone.utc)
    if snapshot.tzinfo is None:
        raise ValueError("snapshot_at must include a timezone")

    if record_type != "instance":
        nodes = [_v2_node(obj, record_type=record_type, service_id=service_id)]
        edges: list[dict[str, Any]] = []
        truncated = False
        truncation_reason = None
    else:
        nodes = []
        edges = []
        root_scope = _scope_tuple(obj)
        queue: deque[tuple[Any, int]] = deque([(obj, 0)])
        queued = {str(getattr(obj, "euid", "") or "")}
        visited: dict[str, Any] = {}
        included_edges: dict[str, Any] = {}
        traversal_issues: dict[str, list[dict[str, str]]] = {}
        truncated = False
        reasons: set[str] = set()
        while queue:
            current, current_depth = queue.popleft()
            current_euid = str(getattr(current, "euid", "") or "")
            queued.discard(current_euid)
            if not current_euid or current_euid in visited:
                continue
            if len(visited) >= max_nodes:
                truncated = True
                reasons.add("max_nodes")
                continue
            if _scope_tuple(current)[:2] != root_scope[:2]:
                raise DagV2GraphContractError(
                    "Cross-domain or cross-owner graph node is forbidden"
                )
            visited[current_euid] = current
            # Retain this exact persisted record as a visible leaf. Never read
            # its old target fields or use it to reach another local/remote node.
            if _unsupported_reference_boundary(current):
                continue
            relations: list[tuple[Any, Any]] = []
            if lineage_reader is not None:
                lineage_reader.prefetch([
                    current,
                    *(queued for queued, _ in list(queue)[:lineage_reader.batch_size - 1]
                      if not _unsupported_reference_boundary(queued)),
                ])
                outgoing = lineage_reader.outgoing[int(current.uid)]
                incoming = lineage_reader.incoming[int(current.uid)]
            else:
                outgoing = _active_related(getattr(current, "parent_of_lineages", None))
                incoming = _active_related(getattr(current, "child_of_lineages", None))
            for lineage in outgoing:
                relations.append((lineage, getattr(lineage, "child_instance", None)))
            for lineage in incoming:
                relations.append((lineage, getattr(lineage, "parent_instance", None)))
            for lineage, neighbor in sorted(
                relations,
                key=lambda pair: str(getattr(pair[0], "euid", "") or ""),
            ):
                if neighbor is None:
                    # Do not expose the identity of an inaccessible endpoint.
                    traversal_issues.setdefault(current_euid, []).append(
                        relationship_error("unavailable_endpoint", lineage.euid))
                    continue
                if bool(getattr(neighbor, "is_deleted", False)):
                    continue
                _validate_lineage_scope(obj, lineage, neighbor, tenant_scope)
                if _unsupported_reference_boundary(lineage.parent_instance):
                    continue
                if current_depth >= depth:
                    neighbor_euid = str(getattr(neighbor, "euid", "") or "")
                    if neighbor_euid not in visited:
                        truncated = True
                        reasons.add("max_depth")
                    continue
                neighbor_euid = str(getattr(neighbor, "euid", "") or "")
                if (
                    neighbor_euid not in visited
                    and neighbor_euid not in queued
                    and len(visited) + len(queue) >= max_nodes
                ):
                    truncated = True
                    reasons.add("max_nodes")
                    continue
                edge_euid = str(getattr(lineage, "euid", "") or "")
                if not edge_euid:
                    traversal_issues.setdefault(current_euid, []).append(
                        relationship_error("invalid_relationship"))
                    continue
                if (
                    edge_euid
                    and edge_euid not in included_edges
                    and max_edges is not None
                    and len(included_edges) >= max_edges
                ):
                    truncated = True
                    reasons.add("max_edges")
                    continue
                if edge_euid:
                    included_edges[edge_euid] = lineage
                if neighbor_euid not in visited and neighbor_euid not in queued:
                    queue.append((neighbor, current_depth + 1))
                    queued.add(neighbor_euid)
        if lineage_reader is not None:
            lineage_reader.materialize(list(visited.values()), list(included_edges.values()))
        nodes = [
            _v2_node(
                node, record_type="instance", service_id=service_id,
                outbound_lineages=(lineage_reader.outgoing.get(int(node.uid), [])
                                   if lineage_reader is not None else None),
            )
            for _euid, node in sorted(visited.items())
        ]
        by_id = {node["data"]["euid"]: node["data"] for node in nodes}
        for euid, issues in traversal_issues.items():
            for issue in issues:
                add_issue(by_id[euid], issue)
        edges = _project_graph_edges(nodes, list(included_edges.values()), service_id)
        nodes, edges = prune_stopped_branches(nodes, edges, str(obj.euid))
        edges = retain_acyclic_edges(nodes, edges)
        _assert_acyclic(nodes, edges)
        truncation_reason = "+".join(sorted(reasons)) if reasons else None

    boundary_warnings = _boundary_warnings(nodes)
    revision_input = {"nodes": nodes, "edges": edges}
    graph_revision = hashlib.sha256(
        json.dumps(
            revision_input, sort_keys=True, separators=(",", ":"), default=str
        ).encode("utf-8")
    ).hexdigest()
    return {
        "elements": {"nodes": nodes, "edges": edges},
        "warnings": boundary_warnings,
        "meta": {
            "contract": "dag:v2",
            **_boundary_meta(nodes),
            "service_id": service_id,
            "graph_revision": graph_revision,
            "snapshot_at": snapshot.astimezone(timezone.utc).isoformat(),
            "truncated": truncated,
            "truncation_reason": truncation_reason,
            "effective_limits": {
                "max_depth": depth,
                "max_nodes": max_nodes,
                "max_edges": max_edges,
            },
        },
    }


def build_visible_graph_v2_payload(
    instances: list[Any],
    lineages: list[Any],
    *,
    service_id: str,
    max_nodes: int,
    max_edges: int,
    snapshot_at: datetime | None = None,
    tenant_scope: frozenset[str | None] | None = None,
) -> dict[str, Any]:
    """Build a bounded DAG-v2 view of every instance visible to one session.

    The caller supplies rows already constrained by PostgreSQL/RLS. This builder
    never infers edges from metadata: only persisted lineage whose two endpoints
    are in the visible node set is emitted.
    """

    if not isinstance(max_nodes, int) or isinstance(max_nodes, bool) or max_nodes < 1:
        raise ValueError("max_nodes must be a positive integer")
    if not isinstance(max_edges, int) or isinstance(max_edges, bool) or max_edges < 1:
        raise ValueError("max_edges must be a positive integer")
    snapshot = snapshot_at or datetime.now(timezone.utc)
    if snapshot.tzinfo is None:
        raise ValueError("snapshot_at must include a timezone")

    ordered_instances = sorted(
        (row for row in instances if not bool(getattr(row, "is_deleted", False))),
        key=lambda row: (int(getattr(row, "uid", 0) or 0), str(row.euid or "")),
    )
    selected_instances = ordered_instances[:max_nodes]
    truncated_reasons: set[str] = set()
    if len(ordered_instances) > max_nodes:
        truncated_reasons.add("max_nodes")

    visible: dict[str, Any] = {}
    for instance in selected_instances:
        euid = str(getattr(instance, "euid", "") or "")
        if not euid:
            raise DagV2GraphContractError(
                "Visible graph instance is missing a persisted EUID"
            )
        if euid in visible:
            raise DagV2GraphContractError(
                f"Visible graph contains duplicate instance EUID: {euid}"
            )
        visible[euid] = instance

    included_lineages: list[Any] = []
    ordered_lineages = sorted(
        (row for row in lineages if not bool(getattr(row, "is_deleted", False))),
        key=lambda row: (int(getattr(row, "uid", 0) or 0), str(row.euid or "")),
    )
    for lineage in ordered_lineages:
        parent = getattr(lineage, "parent_instance", None)
        child = getattr(lineage, "child_instance", None)
        if parent is None or child is None:
            if len(included_lineages) >= max_edges:
                truncated_reasons.add("max_edges")
                break
            included_lineages.append(lineage)
            continue
        parent_euid = str(getattr(parent, "euid", "") or "")
        child_euid = str(getattr(child, "euid", "") or "")
        if parent_euid not in visible or child_euid not in visible:
            continue
        _validate_lineage_scope(parent, lineage, child, tenant_scope)
        if _unsupported_reference_boundary(parent):
            continue
        if len(included_lineages) >= max_edges:
            truncated_reasons.add("max_edges")
            break
        included_lineages.append(lineage)

    nodes = [
        _v2_node(instance, record_type="instance", service_id=service_id)
        for _euid, instance in sorted(visible.items())
    ]
    edges = _project_graph_edges(nodes, included_lineages, service_id)
    edges = retain_acyclic_edges(nodes, edges)
    _assert_acyclic(nodes, edges)
    boundary_warnings = _boundary_warnings(nodes)
    revision_input = {"nodes": nodes, "edges": edges}
    graph_revision = hashlib.sha256(
        json.dumps(
            revision_input, sort_keys=True, separators=(",", ":"), default=str
        ).encode("utf-8")
    ).hexdigest()
    return {
        "elements": {"nodes": nodes, "edges": edges},
        "warnings": boundary_warnings,
        "meta": {
            "contract": "dag:v2",
            **_boundary_meta(nodes),
            "service_id": service_id,
            "query_mode": "visible_scope",
            "graph_revision": graph_revision,
            "snapshot_at": snapshot.astimezone(timezone.utc).isoformat(),
            "truncated": bool(truncated_reasons),
            "truncation_reason": (
                "+".join(sorted(truncated_reasons)) if truncated_reasons else None
            ),
            "effective_limits": {
                "max_depth": None,
                "max_nodes": max_nodes,
                "max_edges": max_edges,
            },
        },
    }
