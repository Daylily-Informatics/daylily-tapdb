"""Public, bounded graph diagnostics; never serialize exception text or payloads."""
from __future__ import annotations

from collections import deque
from typing import Any

NODE_ERROR_MESSAGES = {
    "invalid_node_contract": "Node data violates the graph contract; not traversed.",
    "invalid_external_reference": "External reference data is invalid; not traversed.",
    "invalid_node_presentation": "Node presentation metadata is invalid; not traversed.",
    "invalid_external_projection": "Node graph projection is malformed; not traversed.",
    "conflicting_node_response": "The owner returned conflicting data for this node; not traversed.",
}
RELATIONSHIP_ERROR_MESSAGES = {
    "invalid_relationship": "An invalid relationship was omitted; other valid relationships remain visible.",
    "unavailable_endpoint": "A relationship endpoint is unavailable; that relationship was not traversed.",
    "local_cycle_blocked": "A relationship that closes a local cycle was not traversed.",
    "conflicting_relationship": "Conflicting owner data for a relationship was omitted.",
}


def node_error(code: str) -> dict[str, str]:
    return {"code": code, "status": "not_traversed", "message": NODE_ERROR_MESSAGES[code]}


def stop_node(data: dict[str, Any], code: str) -> dict[str, Any]:
    """Retain only established identity/display coordinates for a failed node."""
    safe = {key: data[key] for key in (
        "id", "euid", "global_id", "service_id", "record_type", "category", "type",
        "subtype", "version", "tenant_id",
    ) if key in data and (data[key] is None or isinstance(data[key], str))}
    return {**safe, "display_label": data.get("euid") or data.get("id"),
            "properties": {}, "presentation": {}, "external_refs": [],
            "external_identifiers": [], "expandable": False,
            "traversal_boundary": node_error(code)}


def relationship_error(code: str, lineage_euid: str | None = None) -> dict[str, str]:
    issue = {"code": code, "status": "not_traversed",
             "message": RELATIONSHIP_ERROR_MESSAGES[code]}
    if lineage_euid:
        issue["lineage_euid"] = lineage_euid
    return issue


def add_issue(data: dict[str, Any], issue: dict[str, str]) -> None:
    issues = data.setdefault("graph_issues", [])
    if issue not in issues:
        issues.append(issue)


def retain_acyclic_edges(nodes: list[dict[str, Any]], edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop only DFS back edges, in stable order, and annotate their sources."""
    by_id = {node["data"]["id"]: node["data"] for node in nodes}
    adjacency: dict[str, list[dict[str, Any]]] = {key: [] for key in by_id}
    for edge in edges:
        data = edge["data"]
        if data["source"] in by_id and data["target"] in by_id:
            adjacency[data["source"]].append(edge)
    colors: dict[str, int] = {}
    blocked: set[str] = set()
    for start in sorted(by_id):
        if start in colors:
            continue
        colors[start] = 1
        stack = [(start, iter(sorted(adjacency[start], key=lambda e: e["data"]["id"])))]
        while stack:
            source, children = stack[-1]
            edge = next(children, None)
            if edge is None:
                colors[source] = 2
                stack.pop()
                continue
            target = edge["data"]["target"]
            if colors.get(target) == 1:
                blocked.add(edge["data"]["id"])
                add_issue(by_id[source], relationship_error("local_cycle_blocked", edge["data"].get("euid")))
            elif target not in colors:
                colors[target] = 1
                stack.append((target, iter(sorted(adjacency[target], key=lambda e: e["data"]["id"]))))
    return [edge for edge in edges if edge["data"]["id"] not in blocked]


def prune_stopped_branches(nodes: list[dict[str, Any]], edges: list[dict[str, Any]], start_id: str):
    """Keep the root-connected neighborhood without expanding stopped nodes."""
    by_id = {node["data"]["id"]: node["data"] for node in nodes}
    stopped = {key for key, data in by_id.items() if data.get("traversal_boundary")}
    edges = [edge for edge in edges if edge["data"]["source"] not in stopped]
    adjacency: dict[str, set[str]] = {key: set() for key in by_id}
    for edge in edges:
        a, b = edge["data"]["source"], edge["data"]["target"]
        if a in by_id and b in by_id:
            adjacency[a].add(b)
            adjacency[b].add(a)
    reached, queue = {start_id}, deque([start_id])
    while queue:
        key = queue.popleft()
        if key in stopped:
            continue
        for neighbor in adjacency.get(key, set()) - reached:
            reached.add(neighbor)
            queue.append(neighbor)
    return ([node for node in nodes if node["data"]["id"] in reached],
            [edge for edge in edges if edge["data"]["source"] in reached and edge["data"]["target"] in reached])
