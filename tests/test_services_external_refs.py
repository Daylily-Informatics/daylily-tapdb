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
    relationship_type: str | None = None,
    source_field: str | None = None,
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
        relationship_type=relationship_type,
        source_field=source_field,
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
    def __init__(
        self,
        payload: object,
        *,
        content_type: str = "application/json",
        status: int = 200,
    ):
        self._raw = json.dumps(payload).encode("utf-8")
        self._offset = 0
        self.headers = {"Content-Type": content_type}
        self.status = status

    def read(self, size: int = -1) -> bytes:
        if size < 0:
            chunk = self._raw[self._offset :]
        else:
            chunk = self._raw[self._offset : self._offset + size]
        self._offset += len(chunk)
        return chunk


class _FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class _FakeTransport:
    def __init__(self, response: _FakeResponse):
        self.response = response
        self.requests: list[tuple[eg._V1ProxyTarget, object, float]] = []
        self.connections: list[_FakeConnection] = []

    def open(self, target, endpoint, *, timeout):
        connection = _FakeConnection()
        self.connections.append(connection)
        self.requests.append((target, endpoint, timeout))
        return connection, self.response


def _policy() -> eg.V1ProxyPolicy:
    return eg.V1ProxyPolicy(
        allowed_hosts=frozenset({"atlas.example"}),
        timeout_seconds=3,
    )


def _install_public_proxy(monkeypatch, payload: object) -> _FakeTransport:
    transport = _FakeTransport(_FakeResponse(payload))
    monkeypatch.setattr(eg, "_open_pinned_https", transport.open)
    monkeypatch.setattr(
        eg.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            (eg.socket.AF_INET, eg.socket.SOCK_STREAM, 6, "", ("93.184.216.34", 443))
        ],
    )
    return transport


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
                    "tapdb_graph": [
                        {
                            "system": "atlas",
                            "root_euid": "A-1",
                            "base_url": "https://atlas.local",
                            "graph_data_path": "/api/graph/data",
                            "object_detail_path_template": "/api/object/{euid}",
                            "auth_mode": "none",
                            "label": "Atlas source object",
                            "relationship_type": "derived_from",
                            "source_field": "properties.atlas_euid",
                        }
                    ]
                }
            }
        }
    )

    assert eg.external_ref_payloads(obj) == [
        {
            "label": "Atlas source object",
            "system": "atlas",
            "root_euid": "A-1",
            "tenant_id": None,
            "href": "https://atlas.local/api/object/A-1",
            "graph_expandable": True,
            "ref_index": 0,
            "relationship_type": "derived_from",
            "source_field": "properties.atlas_euid",
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
    transport = _install_public_proxy(
        monkeypatch, {"elements": {"nodes": [], "edges": []}}
    )
    request = _request(
        headers={"cookie": "sid=abc", "authorization": "Bearer xyz"},
    )

    payload = eg.fetch_remote_graph(
        request,
        _ref(base_url="https://atlas.example"),
        depth=3,
        policy=_policy(),
    )

    assert payload == {"elements": {"nodes": [], "edges": []}}
    target, endpoint, timeout = transport.requests[0]
    assert 0 < timeout <= 3
    assert target.request_target == (
        "/api/graph/data?start_euid=AT-1&depth=3&tenant_id=tenant-1"
    )
    assert target.host == "atlas.example"
    assert endpoint[3] == ("93.184.216.34", 443)
    assert transport.connections[0].closed is True


def test_fetch_remote_graph_requires_absolute_http_url():
    request = _request()
    ref = _ref(base_url="atlas.local")
    with pytest.raises(RuntimeError, match="absolute https URL"):
        eg.fetch_remote_graph(request, ref, depth=1, policy=_policy())


def test_fetch_remote_graph_rejects_non_object_json(monkeypatch):
    _install_public_proxy(monkeypatch, ["bad"])
    request = _request()
    with pytest.raises(RuntimeError, match="JSON object"):
        eg.fetch_remote_graph(
            request,
            _ref(base_url="https://atlas.example"),
            depth=1,
            policy=_policy(),
        )


def test_fetch_remote_object_detail_passes_tenant_id(monkeypatch):
    transport = _install_public_proxy(monkeypatch, {"uid": 5, "euid": "AT-9"})
    request = _request()

    payload = eg.fetch_remote_object_detail(
        request,
        _ref(base_url="https://atlas.example"),
        euid="AT-9",
        policy=_policy(),
    )

    assert payload == {"uid": 5, "euid": "AT-9"}
    target, _endpoint, timeout = transport.requests[0]
    assert 0 < timeout <= 3
    assert target.request_target == ("/api/object/AT-9?tenant_id=tenant-1")


def test_v1_proxy_resolves_once_and_uses_the_validated_endpoint(monkeypatch):
    calls = 0

    def _resolve(*_args, **_kwargs):
        nonlocal calls
        calls += 1
        address = "93.184.216.34" if calls == 1 else "127.0.0.1"
        return [(eg.socket.AF_INET, eg.socket.SOCK_STREAM, 6, "", (address, 443))]

    transport = _FakeTransport(_FakeResponse({"ok": True}))
    monkeypatch.setattr(eg.socket, "getaddrinfo", _resolve)
    monkeypatch.setattr(eg, "_open_pinned_https", transport.open)

    assert eg._fetch_v1_json(
        "https://atlas.example/path", policy=_policy(), label="Remote"
    ) == {"ok": True}
    assert calls == 1
    assert transport.requests[0][1][3] == ("93.184.216.34", 443)


def test_pinned_https_connects_validated_socket_with_original_tls_name(monkeypatch):
    calls: dict[str, object] = {}

    class _Socket:
        def settimeout(self, value):
            calls["timeout"] = value

        def connect(self, value):
            calls["connect"] = value

        def setsockopt(self, *value):
            calls["setsockopt"] = value

        def close(self):
            calls["closed"] = True

    class _Context:
        def wrap_socket(self, value, *, server_hostname):
            calls["wrapped"] = value
            calls["server_hostname"] = server_hostname
            return "tls-socket"

    raw_socket = _Socket()
    monkeypatch.setattr(eg.socket, "socket", lambda *_args: raw_socket)
    monkeypatch.setattr(eg.ssl, "create_default_context", lambda: _Context())
    target = eg._V1ProxyTarget(
        host="atlas.example",
        port=443,
        host_header="atlas.example",
        request_target="/path",
        endpoints=(),
    )
    endpoint = (
        eg.socket.AF_INET,
        eg.socket.SOCK_STREAM,
        6,
        ("93.184.216.34", 443),
    )

    connection = eg._PinnedHTTPSConnection(target, endpoint, timeout=3)
    connection.connect()

    assert calls["timeout"] == 3
    assert calls["connect"] == ("93.184.216.34", 443)
    assert calls["wrapped"] is raw_socket
    assert calls["server_hostname"] == "atlas.example"
    assert connection.sock == "tls-socket"


def test_v1_proxy_rejects_private_dns_resolution(monkeypatch):
    monkeypatch.setattr(
        eg.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            (eg.socket.AF_INET, eg.socket.SOCK_STREAM, 6, "", ("127.0.0.1", 443))
        ],
    )
    with pytest.raises(RuntimeError, match="non-public address"):
        eg.fetch_remote_graph(
            _request(),
            _ref(base_url="https://atlas.example"),
            depth=1,
            policy=_policy(),
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
    ref = _ref(
        relationship_type="derived_from",
        source_field="properties.atlas_euid",
        tenant_id="tenant-1",
    )

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
    assert edges[1]["data"]["relationship_type"] == "derived_from"
    assert edges[1]["data"]["source_field"] == "properties.atlas_euid"
    assert out["meta"]["node_count"] == 1
    assert out["meta"]["edge_count"] == 2
