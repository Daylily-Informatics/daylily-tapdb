"""Bounded DAG-v2 federation without credentials, discovery, or HTTP policy.

Applications provide an authenticated transport and an exact fleet inventory.
TapDB validates every service contract and composes neutral graph/search payloads;
it never discovers services, forwards credentials, retries alternate endpoints, or
falls back to an older graph contract.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable
from urllib.parse import urlsplit

from daylily_tapdb.euid import validate_euid
from daylily_tapdb.web.dag_v2 import validate_dag_v2_manifest

_SERVICE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SEARCH_FILTERS = {
    "q",
    "euid",
    "record_type",
    "category",
    "type",
    "subtype",
    "tenant_id",
    "relationship_type",
    "external_service_id",
    "external_object_euid",
    "external_namespace",
    "external_kind",
    "external_value",
    "external_relationship_type",
    "cursor",
}


class FederationContractError(ValueError):
    """A fleet target or remote DAG-v2 payload violated the exact contract."""


class FederationOperationError(RuntimeError):
    """A federated operation could not produce a trustworthy result."""


def _service_id(value: Any) -> str:
    if not isinstance(value, str) or value != value.strip() or not value:
        raise ValueError("service_id must be a non-empty exact string")
    if len(value) > 128 or _SERVICE_ID_RE.fullmatch(value) is None:
        raise ValueError(
            "service_id must start with an alphanumeric character and contain "
            "only alphanumerics, '.', '_' or '-'"
        )
    return value


def _persisted_euid(value: Any) -> str:
    if (
        not isinstance(value, str)
        or value != value.strip()
        or not value
        or any(character.isspace() for character in value)
        or not validate_euid(value)
    ):
        raise ValueError("euid must be a canonical persisted Meridian EUID")
    return value


@dataclass(frozen=True)
class DagServiceTarget:
    """One exact, caller-admitted DAG-v2 service endpoint."""

    service_id: str
    base_url: str

    def __post_init__(self) -> None:
        _service_id(self.service_id)
        parsed = urlsplit(self.base_url)
        if (
            self.base_url != self.base_url.strip()
            or parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
        ):
            raise ValueError(
                "base_url must be an exact credential-free HTTPS service origin"
            )


@dataclass(frozen=True, order=True)
class FederatedObjectKey:
    """Collision-free identity for one persisted object in one service."""

    service_id: str
    euid: str

    def __post_init__(self) -> None:
        _service_id(self.service_id)
        _persisted_euid(self.euid)

    @property
    def global_id(self) -> str:
        return f"{self.service_id}::{self.euid}"


@dataclass(frozen=True)
class FederationLimits:
    """Global operation bounds; callers may only choose equal or lower values."""

    max_services: int = 32
    concurrency: int = 8
    max_external_jumps: int = 32
    max_nodes: int = 5_000
    search_limit: int = 100
    deadline_seconds: float = 30.0

    def __post_init__(self) -> None:
        integer_limits = {
            "max_services": (self.max_services, 32),
            "concurrency": (self.concurrency, 8),
            "max_external_jumps": (self.max_external_jumps, 32),
            "max_nodes": (self.max_nodes, 5_000),
            "search_limit": (self.search_limit, 100),
        }
        for name, (value, ceiling) in integer_limits.items():
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 1
                or value > ceiling
            ):
                raise ValueError(f"{name} must be an integer from 1 through {ceiling}")
        if (
            not isinstance(self.deadline_seconds, (int, float))
            or isinstance(self.deadline_seconds, bool)
            or self.deadline_seconds <= 0
            or self.deadline_seconds > 30
        ):
            raise ValueError("deadline_seconds must be greater than 0 and at most 30")


@runtime_checkable
class DagV2Transport(Protocol):
    """Application-owned authenticated transport for exact DAG-v2 operations."""

    async def manifest(self, target: DagServiceTarget) -> Mapping[str, Any]: ...

    async def search(
        self, target: DagServiceTarget, *, params: Mapping[str, Any]
    ) -> Mapping[str, Any]: ...

    async def object(
        self, target: DagServiceTarget, *, euid: str
    ) -> Mapping[str, Any] | None: ...

    async def graph(
        self,
        target: DagServiceTarget,
        *,
        euid: str,
        depth: int,
        max_nodes: int,
    ) -> Mapping[str, Any]: ...


def _failure(service_id: str, operation: str, exc: BaseException) -> dict[str, Any]:
    return {
        "service_id": service_id,
        "operation": operation,
        "status": "failed",
        "error_type": type(exc).__name__,
        "message": str(exc)[:512],
    }


def _success(service_id: str, operation: str, **detail: Any) -> dict[str, Any]:
    return {
        "service_id": service_id,
        "operation": operation,
        "status": "success",
        **detail,
    }


def _assert_global_acyclic(
    nodes: Mapping[str, Mapping[str, Any]],
    edges: Mapping[str, Mapping[str, Any]],
) -> None:
    adjacency: dict[str, list[str]] = {node_id: [] for node_id in nodes}
    for edge in edges.values():
        data = edge.get("data")
        if not isinstance(data, Mapping):
            raise FederationContractError("federated edge data is malformed")
        source = str(data.get("source") or "")
        target = str(data.get("target") or "")
        if source in adjacency and target in adjacency:
            adjacency[source].append(target)
    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(node_id: str) -> None:
        if node_id in visiting:
            raise FederationContractError("cycle detected in federated DAG")
        if node_id in visited:
            return
        visiting.add(node_id)
        for target in sorted(adjacency[node_id]):
            visit(target)
        visiting.remove(node_id)
        visited.add(node_id)

    for node_id in sorted(nodes):
        visit(node_id)


class DagV2FederationClient:
    """Validate and compose an exact fleet's DAG-v2 data under hard bounds."""

    def __init__(
        self,
        targets: Sequence[DagServiceTarget],
        transport: DagV2Transport,
        *,
        limits: FederationLimits | None = None,
    ) -> None:
        self.limits = limits or FederationLimits()
        if not isinstance(targets, Sequence) or isinstance(targets, (str, bytes)):
            raise ValueError("targets must be a sequence of DagServiceTarget values")
        if not targets:
            raise ValueError("at least one exact service target is required")
        if len(targets) > self.limits.max_services:
            raise ValueError("target count exceeds max_services")
        indexed: dict[str, DagServiceTarget] = {}
        for target in targets:
            if not isinstance(target, DagServiceTarget):
                raise ValueError("targets must contain only DagServiceTarget values")
            if target.service_id in indexed:
                raise ValueError(f"duplicate service_id: {target.service_id}")
            indexed[target.service_id] = target
        required = ("manifest", "search", "object", "graph")
        if any(not callable(getattr(transport, name, None)) for name in required):
            raise ValueError("transport must implement the DagV2Transport protocol")
        self.targets = dict(sorted(indexed.items()))
        self.transport = transport

    def _target(self, service_id: str) -> DagServiceTarget:
        try:
            return self.targets[_service_id(service_id)]
        except KeyError as exc:
            raise FederationContractError(
                f"service is not admitted to this operation: {service_id}"
            ) from exc

    async def _validated_manifest(self, target: DagServiceTarget) -> dict[str, Any]:
        payload = dict(await self.transport.manifest(target))
        reason = validate_dag_v2_manifest(
            payload, expected_service_id=target.service_id
        )
        if reason is not None:
            raise FederationContractError(
                f"{target.service_id} manifest rejected: {reason.value}"
            )
        return payload

    @staticmethod
    def _validate_search_payload(
        payload: Mapping[str, Any], *, service_id: str
    ) -> list[dict[str, Any]]:
        meta = payload.get("meta")
        if (
            not isinstance(meta, Mapping)
            or meta.get("contract") != "dag:v2"
            or meta.get("service_id") != service_id
        ):
            raise FederationContractError("search payload has invalid DAG-v2 metadata")
        raw_items = payload.get("items")
        if not isinstance(raw_items, list):
            raise FederationContractError("search payload items must be a list")
        items: list[dict[str, Any]] = []
        for raw in raw_items:
            if not isinstance(raw, Mapping):
                raise FederationContractError("search item must be an object")
            item = dict(raw)
            if item.get("service_id") != service_id:
                raise FederationContractError("search item service_id mismatch")
            _persisted_euid(item.get("euid"))
            items.append(item)
        return items

    async def search(
        self,
        *,
        filters: Mapping[str, str] | None = None,
        limit: int | None = None,
    ) -> dict[str, Any]:
        """Search every admitted service in parallel with explicit receipts."""

        effective_limit = self.limits.search_limit if limit is None else limit
        if (
            not isinstance(effective_limit, int)
            or isinstance(effective_limit, bool)
            or not 1 <= effective_limit <= self.limits.search_limit
        ):
            raise ValueError("limit exceeds the federation search limit")
        params: dict[str, Any] = {}
        for key, value in dict(filters or {}).items():
            if key not in _SEARCH_FILTERS:
                raise ValueError(f"unsupported DAG-v2 search filter: {key}")
            if not isinstance(value, str):
                raise ValueError(f"search filter {key} must be a string")
            params[key] = value
        params["limit"] = effective_limit
        semaphore = asyncio.Semaphore(self.limits.concurrency)

        async def one(
            target: DagServiceTarget,
        ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
            async with semaphore:
                try:
                    manifest = await self._validated_manifest(target)
                    service_limit = int(manifest["limits"]["max_search_page_size"])
                    service_params = dict(params)
                    service_params["limit"] = min(effective_limit, service_limit)
                    payload = await self.transport.search(target, params=service_params)
                    items = self._validate_search_payload(
                        payload, service_id=target.service_id
                    )
                    return items, _success(
                        target.service_id, "search", item_count=len(items)
                    )
                except Exception as exc:
                    return [], _failure(target.service_id, "search", exc)

        try:
            async with asyncio.timeout(self.limits.deadline_seconds):
                outcomes = await asyncio.gather(
                    *(one(target) for target in self.targets.values())
                )
        except TimeoutError as exc:
            raise FederationOperationError(
                "federated search deadline exceeded"
            ) from exc

        successful = [
            receipt for _items, receipt in outcomes if receipt["status"] == "success"
        ]
        if not successful:
            raise FederationOperationError("every admitted service failed search")
        deduplicated: dict[str, dict[str, Any]] = {}
        for items, _receipt in outcomes:
            for item in items:
                global_id = f"{item['service_id']}::{item['euid']}"
                candidate = dict(item, global_id=global_id)
                prior = deduplicated.get(global_id)
                if prior is not None and prior != candidate:
                    raise FederationContractError(
                        f"conflicting duplicate search result: {global_id}"
                    )
                deduplicated[global_id] = candidate
        ordered = [deduplicated[key] for key in sorted(deduplicated)][:effective_limit]
        receipts = [receipt for _items, receipt in outcomes]
        failures = [item for item in receipts if item["status"] == "failed"]
        return {
            "items": ordered,
            "receipts": receipts,
            "warnings": failures,
            "meta": {
                "contract": "tapdb.federation:v1",
                "partial": bool(failures),
                "service_count": len(self.targets),
                "result_count": len(ordered),
                "limits": asdict(self.limits),
            },
        }

    async def resolve_owner(self, euid: str) -> FederatedObjectKey:
        """Resolve one owner by parallel exact lookup; never infer via search."""

        exact_euid = _persisted_euid(euid)
        semaphore = asyncio.Semaphore(self.limits.concurrency)

        async def one(
            target: DagServiceTarget,
        ) -> tuple[FederatedObjectKey | None, dict[str, Any]]:
            async with semaphore:
                try:
                    await self._validated_manifest(target)
                    payload = await self.transport.object(target, euid=exact_euid)
                    if payload is None:
                        return None, _success(
                            target.service_id, "exact_lookup", found=False
                        )
                    self._validate_object(payload, target.service_id, exact_euid)
                    return FederatedObjectKey(target.service_id, exact_euid), _success(
                        target.service_id, "exact_lookup", found=True
                    )
                except Exception as exc:
                    return None, _failure(target.service_id, "exact_lookup", exc)

        try:
            async with asyncio.timeout(self.limits.deadline_seconds):
                outcomes = await asyncio.gather(
                    *(one(target) for target in self.targets.values())
                )
        except TimeoutError as exc:
            raise FederationOperationError(
                "owner-resolution deadline exceeded"
            ) from exc
        failures = [
            receipt for _key, receipt in outcomes if receipt["status"] == "failed"
        ]
        if failures:
            failed_services = ", ".join(item["service_id"] for item in failures)
            raise FederationOperationError(
                "owner resolution requires every exact lookup to succeed; "
                f"failed services: {failed_services}"
            )
        winners = [key for key, _receipt in outcomes if key is not None]
        if len(winners) > 1:
            owners = ", ".join(key.service_id for key in winners)
            raise FederationContractError(
                f"object is claimed by multiple services: {owners}"
            )
        if not winners:
            raise LookupError("no admitted service owns the exact EUID")
        return winners[0]

    @staticmethod
    def _validate_object(
        payload: Mapping[str, Any], service_id: str, euid: str
    ) -> dict[str, Any]:
        item = dict(payload)
        if item.get("service_id") != service_id or item.get("euid") != euid:
            raise FederationContractError(
                "exact object identity does not match request"
            )
        if not isinstance(item.get("external_refs", []), list) or not isinstance(
            item.get("external_identifiers", []), list
        ):
            raise FederationContractError("object external projections must be lists")
        return item

    @staticmethod
    def _validate_graph(
        payload: Mapping[str, Any], *, service_id: str
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        meta = payload.get("meta")
        elements = payload.get("elements")
        if (
            not isinstance(meta, Mapping)
            or meta.get("contract") != "dag:v2"
            or meta.get("service_id") != service_id
            or not isinstance(elements, Mapping)
            or not isinstance(elements.get("nodes"), list)
            or not isinstance(elements.get("edges"), list)
        ):
            raise FederationContractError("graph payload has invalid DAG-v2 envelope")
        nodes: list[dict[str, Any]] = []
        for raw in elements["nodes"]:
            if not isinstance(raw, Mapping) or not isinstance(raw.get("data"), Mapping):
                raise FederationContractError("graph node must contain data")
            data = dict(raw["data"])
            if data.get("service_id") != service_id:
                raise FederationContractError("graph node service_id mismatch")
            euid = _persisted_euid(data.get("euid"))
            if data.get("id") != euid:
                raise FederationContractError("graph node id must equal its EUID")
            if not isinstance(data.get("external_refs", []), list) or not isinstance(
                data.get("external_identifiers", []), list
            ):
                raise FederationContractError(
                    "graph external projections must be lists"
                )
            nodes.append({"data": data})
        node_ids = {node["data"]["euid"] for node in nodes}
        edges: list[dict[str, Any]] = []
        for raw in elements["edges"]:
            if not isinstance(raw, Mapping) or not isinstance(raw.get("data"), Mapping):
                raise FederationContractError("graph edge must contain data")
            data = dict(raw["data"])
            if (
                data.get("service_id") != service_id
                or data.get("source") not in node_ids
                or data.get("target") not in node_ids
            ):
                raise FederationContractError(
                    "graph edge has invalid service or endpoint"
                )
            _persisted_euid(data.get("euid"))
            edges.append({"data": data})
        return nodes, edges

    async def graph(
        self,
        start: FederatedObjectKey,
        *,
        local_depth: int = 8,
    ) -> dict[str, Any]:
        """Compose a bounded global graph from exact canonical references."""

        if not isinstance(start, FederatedObjectKey):
            raise ValueError("start must be a FederatedObjectKey")
        if (
            not isinstance(local_depth, int)
            or isinstance(local_depth, bool)
            or local_depth < 0
        ):
            raise ValueError("local_depth must be a non-negative integer")
        self._target(start.service_id)
        semaphore = asyncio.Semaphore(self.limits.concurrency)
        manifest_cache: dict[str, dict[str, Any]] = {}
        nodes: dict[str, dict[str, Any]] = {}
        edges: dict[str, dict[str, Any]] = {}
        receipts: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        pending: list[FederatedObjectKey] = [start]
        pending_context: dict[
            FederatedObjectKey, list[tuple[str, Mapping[str, Any]]]
        ] = {}
        visited: set[FederatedObjectKey] = set()
        jumps = 0
        truncated_reasons: set[str] = set()

        async def manifest(target: DagServiceTarget) -> dict[str, Any]:
            cached = manifest_cache.get(target.service_id)
            if cached is None:
                cached = await self._validated_manifest(target)
                manifest_cache[target.service_id] = cached
            # Revalidate the exact cached contract before every external jump.
            reason = validate_dag_v2_manifest(
                cached, expected_service_id=target.service_id
            )
            if reason is not None:
                raise FederationContractError(
                    f"{target.service_id} manifest rejected: {reason.value}"
                )
            return cached

        async def fetch(key: FederatedObjectKey):
            target = self._target(key.service_id)
            async with semaphore:
                await manifest(target)
                payload = await self.transport.graph(
                    target,
                    euid=key.euid,
                    depth=local_depth,
                    max_nodes=self.limits.max_nodes,
                )
                return self._validate_graph(payload, service_id=key.service_id)

        def unresolved(
            source_global_id: str,
            ref: Mapping[str, Any],
            *,
            reason: str,
        ) -> None:
            target_service = str(ref.get("target_service_id") or "")
            target_euid = str(ref.get("target_object_euid") or "")
            digest = hashlib.sha256(
                json.dumps(
                    [source_global_id, target_service, target_euid, reason],
                    separators=(",", ":"),
                ).encode("utf-8")
            ).hexdigest()[:24]
            boundary_id = f"unresolved::{digest}"
            if boundary_id not in nodes and len(nodes) >= self.limits.max_nodes:
                truncated_reasons.add("max_nodes")
                return
            nodes.setdefault(
                boundary_id,
                {
                    "data": {
                        "id": boundary_id,
                        "global_id": boundary_id,
                        "record_type": "unresolved_external_boundary",
                        "target_service_id": target_service,
                        "target_object_euid": target_euid,
                        "reason": reason,
                    }
                },
            )
            lineage_euid = str(ref.get("lineage_euid") or "")
            bridge_id = f"bridge::{source_global_id}::{lineage_euid}"
            edges[bridge_id] = {
                "data": {
                    "id": bridge_id,
                    "source": source_global_id,
                    "target": boundary_id,
                    "edge_kind": "external_reference_unresolved",
                    "relationship_type": ref.get("relationship_type"),
                    "external_reference_euid": ref.get("external_reference_euid"),
                    "lineage_euid": lineage_euid,
                    "assertion_authority": ref.get("assertion_authority"),
                    "asserted_at": ref.get("asserted_at"),
                    "assertion_provenance": ref.get("assertion_provenance"),
                }
            }

        try:
            async with asyncio.timeout(self.limits.deadline_seconds):
                while pending:
                    batch = sorted(set(pending) - visited)
                    pending = []
                    if not batch:
                        break
                    outcomes = await asyncio.gather(
                        *(fetch(key) for key in batch), return_exceptions=True
                    )
                    for key, outcome in zip(batch, outcomes, strict=True):
                        if isinstance(outcome, BaseException):
                            receipt = _failure(key.service_id, "graph", outcome)
                            receipts.append(receipt)
                            if key == start:
                                raise FederationOperationError(
                                    f"starting service failed: {receipt['message']}"
                                ) from outcome
                            warnings.append(receipt)
                            for source_global_id, ref in pending_context.get(key, []):
                                unresolved(
                                    source_global_id,
                                    ref,
                                    reason=f"remote branch failed: {receipt['message']}",
                                )
                            continue
                        visited.add(key)
                        remote_nodes, remote_edges = outcome
                        receipts.append(
                            _success(
                                key.service_id,
                                "graph",
                                root_euid=key.euid,
                                node_count=len(remote_nodes),
                                edge_count=len(remote_edges),
                            )
                        )
                        for remote in remote_nodes:
                            data = dict(remote["data"])
                            global_id = f"{key.service_id}::{data['euid']}"
                            namespaced = {
                                "data": {
                                    **data,
                                    "id": global_id,
                                    "global_id": global_id,
                                }
                            }
                            prior = nodes.get(global_id)
                            if prior is not None and prior != namespaced:
                                raise FederationContractError(
                                    f"conflicting duplicate graph node: {global_id}"
                                )
                            if len(nodes) >= self.limits.max_nodes and prior is None:
                                truncated_reasons.add("max_nodes")
                                continue
                            nodes[global_id] = namespaced

                        for remote in remote_edges:
                            data = dict(remote["data"])
                            edge_id = f"{key.service_id}::{data['euid']}"
                            namespaced = {
                                "data": {
                                    **data,
                                    "id": edge_id,
                                    "global_id": edge_id,
                                    "source": f"{key.service_id}::{data['source']}",
                                    "target": f"{key.service_id}::{data['target']}",
                                }
                            }
                            prior = edges.get(edge_id)
                            if prior is not None and prior != namespaced:
                                raise FederationContractError(
                                    f"conflicting duplicate graph edge: {edge_id}"
                                )
                            edges[edge_id] = namespaced

                        for remote in remote_nodes:
                            data = remote["data"]
                            source_global_id = f"{key.service_id}::{data['euid']}"
                            for identifier in data.get("external_identifiers", []):
                                if not isinstance(identifier, Mapping):
                                    raise FederationContractError(
                                        "opaque identifier projection must be an object"
                                    )
                                digest = hashlib.sha256(
                                    json.dumps(
                                        [
                                            source_global_id,
                                            identifier.get("namespace"),
                                            identifier.get("kind"),
                                            identifier.get("value"),
                                            identifier.get("lineage_euid"),
                                        ],
                                        sort_keys=False,
                                        separators=(",", ":"),
                                    ).encode("utf-8")
                                ).hexdigest()[:24]
                                opaque_id = f"opaque::{digest}"
                                if len(nodes) < self.limits.max_nodes:
                                    nodes.setdefault(
                                        opaque_id,
                                        {
                                            "data": {
                                                "id": opaque_id,
                                                "global_id": opaque_id,
                                                "record_type": "external_identifier",
                                                **dict(identifier),
                                                "expandable": False,
                                            }
                                        },
                                    )
                                    edge_id = f"opaque-bridge::{source_global_id}::{identifier.get('lineage_euid')}"
                                    edges[edge_id] = {
                                        "data": {
                                            "id": edge_id,
                                            "source": source_global_id,
                                            "target": opaque_id,
                                            "edge_kind": "external_identifier",
                                            "relationship_type": identifier.get(
                                                "relationship_type"
                                            ),
                                            "lineage_euid": identifier.get(
                                                "lineage_euid"
                                            ),
                                            "assertion_provenance": identifier.get(
                                                "assertion_provenance"
                                            ),
                                        }
                                    }
                                else:
                                    truncated_reasons.add("max_nodes")

                            for ref in data.get("external_refs", []):
                                if not isinstance(ref, Mapping):
                                    raise FederationContractError(
                                        "external reference projection must be an object"
                                    )
                                target_service = str(ref.get("target_service_id") or "")
                                target_euid = str(ref.get("target_object_euid") or "")
                                try:
                                    target_key = FederatedObjectKey(
                                        target_service, target_euid
                                    )
                                    target = self._target(target_service)
                                    await manifest(target)
                                except Exception as exc:
                                    unresolved(
                                        source_global_id,
                                        ref,
                                        reason=str(exc)[:256],
                                    )
                                    warnings.append(
                                        _failure(
                                            target_service or "unknown",
                                            "external_jump",
                                            exc,
                                        )
                                    )
                                    continue
                                target_global_id = target_key.global_id
                                edge_id = f"bridge::{source_global_id}::{ref.get('lineage_euid')}"
                                edges[edge_id] = {
                                    "data": {
                                        "id": edge_id,
                                        "source": source_global_id,
                                        "target": target_global_id,
                                        "edge_kind": "external_reference",
                                        "source_service_id": key.service_id,
                                        "target_service_id": target_service,
                                        "target_object_euid": target_euid,
                                        "relationship_type": ref.get(
                                            "relationship_type"
                                        ),
                                        "external_reference_euid": ref.get(
                                            "external_reference_euid"
                                        ),
                                        "lineage_euid": ref.get("lineage_euid"),
                                        "assertion_authority": ref.get(
                                            "assertion_authority"
                                        ),
                                        "asserted_at": ref.get("asserted_at"),
                                        "assertion_provenance": ref.get(
                                            "assertion_provenance"
                                        ),
                                    }
                                }
                                if target_key in visited or target_key in pending:
                                    pending_context.setdefault(target_key, []).append(
                                        (source_global_id, ref)
                                    )
                                    continue
                                if jumps >= self.limits.max_external_jumps:
                                    truncated_reasons.add("max_external_jumps")
                                    unresolved(
                                        source_global_id,
                                        ref,
                                        reason="max_external_jumps",
                                    )
                                    continue
                                jumps += 1
                                pending.append(target_key)
                                pending_context.setdefault(target_key, []).append(
                                    (source_global_id, ref)
                                )
        except TimeoutError as exc:
            raise FederationOperationError("federated graph deadline exceeded") from exc

        complete_edges = {
            edge_id: edge
            for edge_id, edge in edges.items()
            if edge["data"]["source"] in nodes and edge["data"]["target"] in nodes
        }
        if len(complete_edges) != len(edges):
            truncated_reasons.add("max_nodes")
        _assert_global_acyclic(nodes, complete_edges)
        return {
            "elements": {
                "nodes": [nodes[key] for key in sorted(nodes)],
                "edges": [complete_edges[key] for key in sorted(complete_edges)],
            },
            "receipts": receipts,
            "warnings": warnings,
            "meta": {
                "contract": "tapdb.federation:v1",
                "start": start.global_id,
                "external_jumps": jumps,
                "truncated": bool(truncated_reasons),
                "truncation_reason": "+".join(sorted(truncated_reasons)) or None,
                "limits": asdict(self.limits),
            },
        }


__all__ = [
    "DagServiceTarget",
    "DagV2FederationClient",
    "DagV2Transport",
    "FederatedObjectKey",
    "FederationContractError",
    "FederationLimits",
    "FederationOperationError",
]
