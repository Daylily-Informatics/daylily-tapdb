from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from daylily_tapdb.services import external_refs as eg


def _ref(
    *,
    auth_mode: str = "none",
    base_url: str = "https://atlas.local",
    graph_expandable: bool = True,
    graph_data_path: str = "/api/graph/data",
    object_detail_path_template: str = "/api/object/{euid}",
    tenant_id: str | None = "tenant-1",
) -> eg.ExternalGraphRef:
    return eg.ExternalGraphRef(
        label="atlas:AT-1",
        system="atlas",
        root_euid="AT-1",
        tenant_id=tenant_id,
        href="https://atlas.local/api/object/AT-1",
        graph_expandable=graph_expandable,
        reason=None,
        base_url=base_url,
        graph_data_path=graph_data_path,
        object_detail_path_template=object_detail_path_template,
        auth_mode=auth_mode,
    )


def _request(
    *,
    scheme: str = "https",
    netloc: str = "atlas.local",
    headers: dict[str, str] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        url=SimpleNamespace(scheme=scheme, netloc=netloc),
        headers=headers or {},
    )


class _FakeResponse:
    def __init__(self, payload: object):
        self._payload = payload

    def read(self) -> bytes:
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_resolve_external_graph_refs_builds_href_and_sorts():
    obj = SimpleNamespace(
        json_addl={
            "properties": {
                "external_payload": {
                    "tapdb_graph": [
                        {
                            "system": "zeta",
                            "root_euid": "Z-2",
                            "base_url": "https://zeta.local",
                            "graph_data_path": "/api/graph/data",
                            "object_detail_path_template": "/api/object/{euid}",
                            "auth_mode": "none",
                        },
                        {
                            "system": "atlas",
                            "root_euid": "A-1",
                            "base_url": "https://atlas.local",
                            "graph_data_path": "/api/graph/data",
                            "object_detail_path_template": "/api/object/{euid}",
                            "auth_mode": "none",
                        },
                    ]
                }
            }
        }
    )

    refs = eg.resolve_external_graph_refs(obj)

    assert [r.system for r in refs] == ["atlas", "zeta"]
    assert refs[0].href == "https://atlas.local/api/object/A-1"
    assert refs[0].graph_expandable is True
    assert refs[1].href == "https://zeta.local/api/object/Z-2"


def test_external_ref_payloads_exposes_public_dicts():
    obj = SimpleNamespace(
        json_addl={
            "properties": {
                "external_payload": {
                    "tapdb_graph": {
                        "system": "atlas",
                        "root_euid": "A-1",
                        "base_url": "https://atlas.local",
                        "graph_data_path": "/api/graph/data",
                        "object_detail_path_template": "/api/object/{euid}",
                        "auth_mode": "none",
                    }
                }
            }
        }
    )

    assert eg.external_ref_payloads(obj) == [
        {
            "label": "atlas:A-1",
            "system": "atlas",
            "root_euid": "A-1",
            "tenant_id": None,
            "href": "https://atlas.local/api/object/A-1",
            "graph_expandable": True,
            "ref_index": 0,
        }
    ]


def test_typed_external_identifier_object_exposes_public_ref():
    obj = SimpleNamespace(
        category="external_identifier",
        type="tapdb",
        subtype="object",
        json_addl={
            "properties": {
                "external_identifier": {
                    "system": "bloom",
                    "target_euid": "BL-1",
                    "tenant_id": "tenant-2",
                }
            }
        },
    )

    assert eg.external_ref_payloads(obj) == [
        {
            "label": "bloom:BL-1",
            "system": "bloom",
            "root_euid": "BL-1",
            "tenant_id": "tenant-2",
            "href": None,
            "graph_expandable": False,
            "ref_index": 0,
            "reason": (
                "Missing required graph metadata: base_url, graph_data_path, "
                "object_detail_path_template"
            ),
        }
    ]


def test_resolve_external_graph_refs_marks_missing_metadata():
    obj = SimpleNamespace(
        json_addl={
            "properties": {
                "external_payload": {
                    "tapdb_graph": {
                        "system": "atlas",
                        "root_euid": "A-1",
                        "auth_mode": "bad-mode",
                    }
                }
            }
        }
    )

    refs = eg.resolve_external_graph_refs(obj)

    assert len(refs) == 1
    assert refs[0].graph_expandable is False
    assert refs[0].reason is not None
    assert "graph_data_path" in refs[0].reason
    assert "object_detail_path_template" in refs[0].reason
    assert "auth_mode" in refs[0].reason


def test_get_external_ref_by_index_raises_for_out_of_range():
    obj = SimpleNamespace(json_addl={"properties": {"external_payload": {}}})
    with pytest.raises(IndexError, match="External reference not found"):
        eg.get_external_ref_by_index(obj, 0)


def test_fetch_remote_graph_builds_expected_url_and_headers(monkeypatch):
    captured = {}

    def _fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["headers"] = dict(request.header_items())
        captured["timeout"] = timeout
        return _FakeResponse({"elements": {"nodes": [], "edges": []}})

    monkeypatch.setattr(eg, "urlopen", _fake_urlopen)
    request = _request(
        headers={"cookie": "sid=abc", "authorization": "Bearer xyz"},
    )

    payload = eg.fetch_remote_graph(request, _ref(auth_mode="same_origin"), depth=3)

    assert payload == {"elements": {"nodes": [], "edges": []}}
    assert captured["timeout"] == 20
    assert captured["url"] == (
        "https://atlas.local/api/graph/data?start_euid=AT-1&depth=3&tenant_id=tenant-1"
    )
    assert captured["headers"]["Cookie"] == "sid=abc"
    assert captured["headers"]["Authorization"] == "Bearer xyz"


def test_fetch_remote_graph_requires_absolute_http_url():
    request = _request()
    ref = _ref(base_url="atlas.local")
    with pytest.raises(RuntimeError, match="absolute http\\(s\\) URL"):
        eg.fetch_remote_graph(request, ref, depth=1)


def test_fetch_remote_graph_rejects_non_object_json(monkeypatch):
    monkeypatch.setattr(eg, "urlopen", lambda *_a, **_k: _FakeResponse(["bad"]))
    request = _request()
    with pytest.raises(RuntimeError, match="JSON object"):
        eg.fetch_remote_graph(request, _ref(), depth=1)


def test_fetch_remote_object_detail_passes_tenant_id(monkeypatch):
    captured = {}

    def _fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["timeout"] = timeout
        return _FakeResponse({"uid": 5, "euid": "AT-9"})

    monkeypatch.setattr(eg, "urlopen", _fake_urlopen)
    request = _request()

    payload = eg.fetch_remote_object_detail(request, _ref(), euid="AT-9")

    assert payload == {"uid": 5, "euid": "AT-9"}
    assert captured["timeout"] == 20
    assert captured["url"] == "https://atlas.local/api/object/AT-9?tenant_id=tenant-1"


def test_apply_forwarded_auth_same_origin_mismatch_raises():
    request = _request(netloc="local-admin:8911")
    headers: dict[str, str] = {}
    with pytest.raises(RuntimeError, match="matching request origin"):
        eg._apply_forwarded_auth(
            request,
            _ref(auth_mode="same_origin", base_url="https://atlas.local"),
            headers,
        )


def test_namespace_external_graph_namespaces_nodes_edges_and_bridge():
    payload = {
        "elements": {
            "nodes": [{"data": {"id": "R-1", "name": "Remote Root"}}],
            "edges": [
                {"data": {"id": "E-1", "source": "R-1", "target": "R-2"}},
                {"data": {"id": "", "source": "R-1", "target": "R-2"}},
            ],
        }
    }
    ref = _ref(tenant_id="tenant-1")

    out = eg.namespace_external_graph(
        payload,
        ref=ref,
        ref_index=2,
        source_euid="TGX-10",
    )

    nodes = out["elements"]["nodes"]
    edges = out["elements"]["edges"]
    assert len(nodes) == 1
    assert nodes[0]["data"]["id"] == "ext::atlas::tenant-1::R-1"
    assert nodes[0]["data"]["remote_euid"] == "R-1"
    assert len(edges) == 2
    assert edges[0]["data"]["id"] == "ext::atlas::tenant-1::E-1"
    assert edges[0]["data"]["source"] == "ext::atlas::tenant-1::R-1"
    assert edges[0]["data"]["target"] == "ext::atlas::tenant-1::R-2"
    assert edges[1]["data"]["is_external_bridge"] is True
    assert edges[1]["data"]["source"] == "TGX-10"
    assert out["meta"]["node_count"] == 1
    assert out["meta"]["edge_count"] == 2
