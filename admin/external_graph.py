"""Helpers for explicit external graph references in TAPDB admin."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urljoin, urlsplit
from urllib.request import Request as UrlRequest
from urllib.request import urlopen

from fastapi import Request

ALLOWED_AUTH_MODES = {"none", "same_origin"}


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
        if self.reason:
            payload["reason"] = self.reason
        return payload


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


def resolve_external_graph_refs(obj: Any) -> list[ExternalGraphRef]:
    """Parse explicit `external_payload.tapdb_graph` refs from object JSON."""
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

    refs: list[ExternalGraphRef] = []
    for raw in refs_raw:
        item = _as_dict(raw)
        system = _clean(item.get("system"))
        root_euid = _clean(item.get("root_euid"))
        tenant_id = _clean(item.get("tenant_id")) or None
        base_url = _clean(item.get("base_url")) or None
        graph_data_path = _clean(item.get("graph_data_path")) or None
        object_detail_path_template = (
            _clean(item.get("object_detail_path_template")) or None
        )
        auth_mode = _clean(item.get("auth_mode")) or "none"
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
        refs.append(
            ExternalGraphRef(
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
            )
        )

    refs.sort(
        key=lambda ref: (ref.system, ref.label, ref.root_euid, ref.tenant_id or "")
    )
    return refs


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
) -> dict[str, Any]:
    """Fetch a remote graph payload via the configured resolver metadata."""
    if not ref.graph_expandable or not ref.base_url or not ref.graph_data_path:
        raise RuntimeError(ref.reason or "External graph is not expandable")

    params = {"start_euid": ref.root_euid, "depth": int(depth)}
    if ref.tenant_id:
        params["tenant_id"] = ref.tenant_id

    url = urljoin(ref.base_url.rstrip("/") + "/", ref.graph_data_path.lstrip("/"))
    url = f"{url}?{urlencode(params)}"
    url = _require_http_url(url)
    headers = {"Accept": "application/json"}
    _apply_forwarded_auth(request, ref, headers)
    with urlopen(UrlRequest(url, headers=headers), timeout=20) as response:  # nosec B310
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Remote graph response must be a JSON object")
    return payload


def fetch_remote_object_detail(
    request: Request,
    ref: ExternalGraphRef,
    *,
    euid: str,
) -> dict[str, Any]:
    """Fetch remote object detail via the configured resolver metadata."""
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
    url = _require_http_url(url)
    headers = {"Accept": "application/json"}
    _apply_forwarded_auth(request, ref, headers)
    with urlopen(UrlRequest(url, headers=headers), timeout=20) as response:  # nosec B310
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("Remote object response must be a JSON object")
    return payload


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
    namespaced_edges.append(
        {
            "data": {
                "id": bridge_id,
                "source": source_euid,
                "target": namespaced_id(ref.root_euid),
                "relationship_type": "external_reference",
                "is_external_bridge": True,
                "external_system": ref.system,
                "external_tenant_id": ref.tenant_id,
                "source_ref_index": ref_index,
                "external_source_euid": source_euid,
            }
        }
    )

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


def _apply_forwarded_auth(
    request: Request,
    ref: ExternalGraphRef,
    headers: dict[str, str],
) -> None:
    if ref.auth_mode == "none":
        return
    if ref.auth_mode != "same_origin":
        raise RuntimeError(f"Unsupported auth mode: {ref.auth_mode}")

    if not ref.base_url:
        raise RuntimeError("same_origin auth requires base_url")

    incoming_origin = f"{request.url.scheme}://{request.url.netloc}"
    remote_parts = urlsplit(ref.base_url)
    remote_origin = f"{remote_parts.scheme}://{remote_parts.netloc}"
    if incoming_origin != remote_origin:
        raise RuntimeError(
            "same_origin auth requires matching request origin and base_url"
        )

    cookie = request.headers.get("cookie")
    authorization = request.headers.get("authorization")
    if cookie:
        headers["Cookie"] = cookie
    if authorization:
        headers["Authorization"] = authorization


def _require_http_url(url: str) -> str:
    parts = urlsplit(url)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        raise RuntimeError("External graph fetch requires an absolute http(s) URL")
    return url
