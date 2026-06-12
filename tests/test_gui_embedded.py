from __future__ import annotations

import json
import re
import tomllib
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace

from fastapi import FastAPI
from fastapi.testclient import TestClient

from daylily_tapdb.gui import create_tapdb_gui_app
from daylily_tapdb.models.audit import audit_log
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.web.bridge import TapdbHostBridge, TapdbHostNavLink


class _Related:
    def __init__(self, rows):
        self._rows = list(rows)

    def filter_by(self, **kwargs):
        return _Related(
            [
                row
                for row in self._rows
                if all(
                    getattr(row, key, None) == value for key, value in kwargs.items()
                )
            ]
        )

    def all(self):
        return list(self._rows)

    def __iter__(self):
        return iter(self._rows)


class _Query:
    def __init__(self, rows):
        self._rows = list(rows)

    def filter_by(self, **kwargs):
        return _Query(
            [
                row
                for row in self._rows
                if all(
                    getattr(row, key, None) == value for key, value in kwargs.items()
                )
            ]
        )

    def filter(self, *args, **kwargs):
        del args, kwargs
        return self

    def order_by(self, *args, **kwargs):
        del args, kwargs
        return self

    def limit(self, value):
        return _Query(self._rows[: int(value)])

    def first(self):
        return self._rows[0] if self._rows else None

    def all(self):
        return list(self._rows)


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.added = []

    def query(self, model):
        return _Query(self.rows.get(model, []))

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        for index, obj in enumerate(self.added, start=100):
            if getattr(obj, "uid", None) is None:
                obj.uid = index
            if getattr(obj, "euid", None) is None:
                obj.euid = f"Z-XRF-{index}Q"


class _Conn:
    def __init__(self, session):
        self.session = session
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    @contextmanager
    def session_scope(self, commit=False):
        del commit
        yield self.session


def _instance(euid, name, *, category="SMP", type_name="sample", subtype="tube"):
    obj = SimpleNamespace(
        uid=len(euid),
        euid=euid,
        name=name,
        category=category,
        type=type_name,
        subtype=subtype,
        version="1.0",
        bstatus="active",
        is_deleted=False,
        json_addl={"properties": {"color": "blue"}},
        polymorphic_discriminator="generic_instance",
        created_dt=None,
        modified_dt=None,
    )
    obj.parent_of_lineages = _Related([])
    obj.child_of_lineages = _Related([])
    return obj


def _template(
    euid="Z-XRF-1Q",
    *,
    name="External Object Reference",
    category="reference",
    type_name="external_identifier",
    subtype="tapdb_object",
    prefix="XRF",
    json_addl=None,
):
    return SimpleNamespace(
        uid=10,
        euid=euid,
        name=name,
        category=category,
        type=type_name,
        subtype=subtype,
        version="1.0",
        instance_prefix=prefix,
        bstatus="active",
        is_deleted=False,
        json_addl=json_addl or {"properties": {"external_identifier": {}}},
        polymorphic_discriminator="generic_template",
        instance_polymorphic_identity="generic_instance",
        created_dt=None,
        modified_dt=None,
    )


def _client(monkeypatch, *, role="admin", session=None, nav_links=()):
    if session is None:
        session = _Session(
            {
                generic_template: [_template()],
                generic_instance: [_instance("Z-SMP-1Q", "Sample 1")],
                generic_instance_lineage: [],
                audit_log: [],
            }
        )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.get_db",
        lambda _config_path: _Conn(session),
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.get_db_config",
        lambda config_path: {
            "client_id": "testclient",
            "domain_code": "Z",
            "owner_repo_name": "daylily-tapdb",
            "domain_registry_path": "daylily_tapdb/etc/domain_code_registry.json",
            "prefix_ownership_registry_path": "daylily_tapdb/etc/prefix_ownership_registry.json",
        },
    )
    bridge = TapdbHostBridge(
        auth_mode="host_session",
        login_url="/login",
        extra_stylesheets=("/static/host.css",),
        nav_links=tuple(nav_links),
        resolve_user=lambda _request: {
            "username": f"{role}@example.com",
            "email": f"{role}@example.com",
            "role": role,
        },
    )
    return TestClient(
        create_tapdb_gui_app(config_path="/tmp/tapdb-config.yaml", host_bridge=bridge),
        base_url="https://localhost",
    )


def test_gui_mount_redirects_unauthenticated_html_and_blocks_api():
    bridge = TapdbHostBridge(
        auth_mode="host_session",
        login_url="/login",
        resolve_user=lambda _request: None,
    )
    client = TestClient(
        create_tapdb_gui_app(config_path="/tmp/tapdb-config.yaml", host_bridge=bridge),
        base_url="https://localhost",
    )

    assert client.get("/", follow_redirects=False).status_code == 302
    response = client.get("/api/search", follow_redirects=False)
    assert response.status_code == 401
    assert response.json()["detail"] == "host_session_required"


def test_gui_mounted_api_blocks_unauthenticated_with_json_401():
    bridge = TapdbHostBridge(
        auth_mode="host_session",
        login_url="/login",
        resolve_user=lambda _request: None,
    )
    host = FastAPI()
    host.mount(
        "/tapdb",
        create_tapdb_gui_app(config_path="/tmp/tapdb-config.yaml", host_bridge=bridge),
    )
    client = TestClient(host, base_url="https://localhost")

    html_response = client.get("/tapdb/search", follow_redirects=False)
    api_response = client.get("/tapdb/api/search", follow_redirects=False)

    assert html_response.status_code == 302
    assert html_response.headers["location"] == "/login"
    assert api_response.status_code == 401
    assert api_response.json()["detail"] == "host_session_required"


def test_gui_search_page_uses_host_css_and_root_safe_links(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/search?q=sample")

    assert response.status_code == 200
    assert "/static/host.css" in response.text
    assert "/object/Z-SMP-1Q" in response.text
    assert "Sample 1" in response.text


def test_gui_shell_deduplicates_host_and_builtin_nav_links(monkeypatch):
    client = _client(
        monkeypatch,
        nav_links=(
            TapdbHostNavLink("Search", "/search"),
            TapdbHostNavLink("Templates", "/templates"),
            TapdbHostNavLink("Support", "/support"),
            TapdbHostNavLink("Meridian", "/admin/meridian"),
            TapdbHostNavLink("Metrics", "/admin/metrics"),
        ),
    )

    response = client.get("/search")

    assert response.status_code == 200
    nav = re.search(r"<nav>(.*?)</nav>", response.text, re.DOTALL)
    assert nav is not None
    nav_html = nav.group(1)
    assert nav_html.count(">Search</a>") == 1
    assert nav_html.count(">Templates</a>") == 1
    assert nav_html.count(">Meridian</a>") == 1
    assert nav_html.count(">Metrics</a>") == 1
    assert 'href="/support">Support</a>' in nav_html
    assert 'href="/admin/readiness">Readiness</a>' in nav_html


def test_gui_graph_page_includes_visual_viewer(monkeypatch):
    root = _instance("Z-SMP-1Q", "Sample 1")
    child = _instance("Z-CHD-2Q", "Child 1")

    monkeypatch.setattr(
        "daylily_tapdb.gui.router.find_object_by_euid",
        lambda session, euid: (root if euid == root.euid else None, "instance"),
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.build_graph_payload",
        lambda obj, record_type, service_name, depth: {
            "elements": {
                "nodes": [
                    {
                        "data": {
                            "id": root.euid,
                            "euid": root.euid,
                            "display_label": root.name,
                            "name": root.name,
                            "category": root.category,
                            "type": root.type,
                            "subtype": root.subtype,
                            "bstatus": root.bstatus,
                        }
                    },
                    {
                        "data": {
                            "id": child.euid,
                            "euid": child.euid,
                            "display_label": child.name,
                            "name": child.name,
                            "category": child.category,
                            "type": child.type,
                            "subtype": child.subtype,
                            "bstatus": child.bstatus,
                        }
                    },
                ],
                "edges": [
                    {
                        "data": {
                            "id": "Z-LIN-3Q",
                            "euid": "Z-LIN-3Q",
                            "source": child.euid,
                            "target": root.euid,
                            "relationship_type": "contains",
                        }
                    }
                ],
            }
        },
    )
    client = _client(monkeypatch)

    response = client.get("/object/Z-SMP-1Q/graph")

    assert response.status_code == 200
    assert 'data-testid="tapdb-graph"' in response.text
    assert "cytoscape@3.28.1" in response.text
    assert 'id="tapdb-graph-payload"' in response.text
    assert "<summary>Payload JSON</summary>" in response.text
    assert "No selection" in response.text


def test_gui_search_rejects_invalid_record_type(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/api/search?record_type=bad")

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid record_type: bad"


def test_gui_object_api_returns_detail_relationships_audit_and_refs(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/api/object/Z-SMP-1Q")

    assert response.status_code == 200
    payload = response.json()
    assert payload["obj"]["euid"] == "Z-SMP-1Q"
    assert payload["record_type"] == "instance"
    assert payload["relationships"] == {"parent_of": [], "child_of": []}
    assert payload["audit_rows"] == []
    assert payload["external_refs"] == []


def test_gui_object_page_links_visible_euids_to_canonical_details(monkeypatch):
    source = _instance("Z-SMP-1Q", "Sample 1")
    child = _instance("Z-CHD-2Q", "Child 1")
    lineage = SimpleNamespace(
        euid="Z-LIN-3Q",
        relationship_type="contains",
        is_deleted=False,
        child_instance=child,
        parent_instance=source,
    )
    source.parent_of_lineages = _Related([lineage])
    child.child_of_lineages = _Related([lineage])
    session = _Session(
        {
            generic_template: [_template()],
            generic_instance: [source, child],
            generic_instance_lineage: [lineage],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.get("/object/Z-SMP-1Q")

    assert response.status_code == 200
    assert 'href="/object/Z-SMP-1Q"' in response.text
    assert 'href="/object/Z-LIN-3Q"' in response.text
    assert 'href="/object/Z-CHD-2Q"' in response.text


def test_gui_admin_pages_require_admin(monkeypatch):
    client = _client(monkeypatch, role="user")

    response = client.get("/admin/meridian")

    assert response.status_code == 403
    assert response.json()["detail"] == "tapdb_gui_admin_required"


def test_gui_create_routes_require_admin(monkeypatch):
    client = _client(monkeypatch, role="user")

    page = client.get("/create/Z-XRF-1Q")
    post_page = client.post(
        "/create/Z-XRF-1Q",
        data={"name": "Link", "properties_json": "{}"},
    )
    post_api = client.post(
        "/api/create/Z-XRF-1Q",
        json={"name": "Link", "properties": {}},
    )

    assert page.status_code == 403
    assert post_page.status_code == 403
    assert post_api.status_code == 403


def test_gui_template_validation_api_reports_valid_level2_template(monkeypatch):
    client = _client(monkeypatch)

    response = client.post(
        "/api/templates/validate",
        json={
            "templates": [
                {
                    "name": "Plate Template",
                    "polymorphic_discriminator": "generic_template",
                    "category": "container",
                    "type": "plate",
                    "subtype": "96well-generic",
                    "version": "1.0",
                    "instance_prefix": "PAT",
                    "instance_polymorphic_identity": "generic_instance",
                    "json_addl": {
                        "properties": {},
                        "instantiation_layouts": [
                            {
                                "relationship_type": "contains",
                                "name_pattern": "{parent_name}_{index}",
                                "child_templates": [
                                    {
                                        "template_code": "container/well/generic/1.0",
                                        "count": 96,
                                    }
                                ],
                            }
                        ],
                    },
                }
            ]
        },
    )

    assert response.status_code == 200
    assert response.json() == {"valid": True, "issues": []}


def test_gui_template_editor_includes_simple_builder(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/templates/new")

    assert response.status_code == 200
    assert 'data-testid="template-builder"' in response.text
    assert 'id="builder-generate-json"' in response.text
    assert 'value="container/well/generic/1.0"' in response.text
    assert "Child Instantiation" in response.text
    assert "data-tapdb-json-editor" in response.text
    assert 'data-json-editor-label="Template pack JSON"' in response.text


def test_gui_template_editor_can_seed_builder_from_template_euid(monkeypatch):
    seeded_template = _template(
        euid="Z-TPX-SEED",
        name="Seeded Plate",
        category="container",
        type_name="plate",
        subtype="seeded_plate",
        prefix="PAT",
        json_addl={
            "properties": {
                "display_name": "",
                "dimensions": {"rows": 8, "columns": 12},
            },
            "instantiation_layouts": [
                {
                    "relationship_type": "contains",
                    "child_templates": [
                        {
                            "template_code": "container/well/seeded/1.0",
                            "count": 96,
                        }
                    ],
                }
            ],
        },
    )
    session = _Session(
        {
            generic_template: [seeded_template],
            generic_instance: [],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.get("/templates/new?seed_euid=Z-TPX-SEED")

    assert response.status_code == 200
    assert "Seeded Plate" in response.text
    assert 'id="builder-category" value="container"' in response.text
    assert 'id="builder-subtype" value="seeded_plate"' in response.text
    assert 'data-builder-property-key value="dimensions"' in response.text
    assert "{&#34;columns&#34;: 12, &#34;rows&#34;: 8}" in response.text
    assert 'value="container/well/seeded/1.0"' in response.text
    assert 'type="number" min="1" value="96"' in response.text
    assert "Z-TPX-SEED" in response.text


def test_gui_example_template_pack_is_self_contained():
    from daylily_tapdb.gui.router import (
        _example_template_pack,
        _validate_template_payload,
    )

    payload = _example_template_pack()
    issues = _validate_template_payload(payload)
    keys = {
        (
            item["category"],
            item["type"],
            item["subtype"],
            item["version"],
        )
        for item in payload["templates"]
    }
    prefixes = {item["instance_prefix"] for item in payload["templates"]}
    registry = json.loads(
        Path("daylily_tapdb/etc/prefix_ownership_registry.json").read_text(
            encoding="utf-8"
        )
    )

    assert issues == []
    assert ("actor", "person", "example_actor", "1.0") in keys
    assert ("container", "well", "generic", "1.0") in keys
    assert ("container", "plate", "96well-generic", "1.0") in keys
    assert prefixes <= set(registry["ownership"]["Z"])


def test_gui_json_editor_asset_is_served_and_base_loads_it(monkeypatch):
    client = _client(monkeypatch)

    asset = client.get("/static/tapdb-json-editor.js")
    page = client.get("/templates/new")

    assert asset.status_code == 200
    assert "tapdb-json-editor" in asset.text
    assert "/static/tapdb-json-editor.js" in page.text
    assert "jsoneditor@10.4.3/dist/jsoneditor.min.js" in page.text
    assert "jsoneditor@10.4.3/dist/jsoneditor.min.css" in page.text


def test_gui_template_validate_get_renders_explicit_editor(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/templates/validate")

    assert response.status_code == 200
    assert "Use Validate after editing the template pack JSON." in response.text
    assert 'data-testid="template-builder"' in response.text


def test_gui_templates_page_renders_template_rows(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/templates")

    assert response.status_code == 200
    assert "External Object Reference" in response.text
    assert "Z-XRF-1Q" in response.text
    assert "/object/Z-XRF-1Q" in response.text
    assert "/create/Z-XRF-1Q" in response.text
    assert "New Template Pack" not in response.text
    assert "Build New Template" in response.text
    assert "/templates/new?seed_euid=Z-XRF-1Q" in response.text


def test_gui_template_seed_requires_existing_template(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/templates/new?seed_euid=Z-NONE-1Q")

    assert response.status_code == 404
    assert response.json()["detail"] == "Template seed not found: Z-NONE-1Q"


def test_gui_template_save_renders_seed_validation_error(monkeypatch):
    client = _client(monkeypatch)

    def fail_seed(*_args, **_kwargs):
        raise ValueError("prefix ZZZ is not claimed by atlas")

    monkeypatch.setattr("daylily_tapdb.gui.router.seed_templates", fail_seed)

    response = client.post(
        "/templates/save",
        data={
            "template_json": (
                '{"templates":[{"name":"Bad","polymorphic_discriminator":"generic_template",'
                '"category":"ZZZ","type":"container","subtype":"bad","version":"1.0",'
                '"instance_prefix":"ZZZ","instance_polymorphic_identity":"generic_instance",'
                '"json_addl":{}}]}'
            )
        },
    )

    assert response.status_code == 200
    assert "prefix ZZZ is not claimed by atlas" in response.text


def test_gui_template_save_renders_config_registry_error(monkeypatch):
    client = _client(monkeypatch)

    def fail_config(*_args, **_kwargs):
        raise ValueError("prefix_ownership_registry.json is invalid JSON")

    monkeypatch.setattr("daylily_tapdb.gui.router.get_db_config", fail_config)

    response = client.post(
        "/templates/save",
        data={
            "template_json": (
                '{"templates":[{"name":"Good","polymorphic_discriminator":"generic_template",'
                '"category":"GUD","type":"container","subtype":"thing","version":"1.0",'
                '"instance_prefix":"GUD","instance_polymorphic_identity":"generic_instance",'
                '"json_addl":{}}]}'
            )
        },
    )

    assert response.status_code == 200
    assert "Template save failed: prefix_ownership_registry.json is invalid JSON" in response.text


def test_gui_template_save_renders_success(monkeypatch):
    client = _client(monkeypatch)

    monkeypatch.setattr(
        "daylily_tapdb.gui.router.seed_templates",
        lambda *_args, **_kwargs: SimpleNamespace(inserted=1, skipped=0),
    )

    response = client.post(
        "/templates/save",
        data={
            "template_json": (
                '{"templates":[{"name":"Good","polymorphic_discriminator":"generic_template",'
                '"category":"GUD","type":"container","subtype":"thing","version":"1.0",'
                '"instance_prefix":"GUD","instance_polymorphic_identity":"generic_instance",'
                '"json_addl":{}}]}'
            )
        },
    )

    assert response.status_code == 200
    assert "Saved 1 template(s); skipped 0." in response.text


def test_gui_create_form_renders_factory_validation_error(monkeypatch):
    session = _Session(
        {
            generic_template: [
                _template(
                    euid="Z-SYS-1Q",
                    name="System User",
                    category="actor",
                    type_name="user",
                    subtype="system",
                    prefix="USR",
                )
            ],
            generic_instance: [],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )

    class _Factory:
        def __init__(self, *_args, **_kwargs):
            pass

        def create_instance(self, *_args, **_kwargs):
            raise ValueError(
                "system_user requires a non-empty login_identifier "
                "(or email/cognito_username)."
            )

    monkeypatch.setattr("daylily_tapdb.gui.router.InstanceFactory", _Factory)
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/create/Z-SYS-1Q",
        data={"name": "No Login", "properties_json": "{}", "create_children": "true"},
    )

    assert response.status_code == 200
    assert "system_user requires a non-empty login_identifier" in response.text
    assert 'value="No Login"' in response.text


def test_gui_home_uses_concrete_search_defaults(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/")

    assert response.status_code == 200
    assert "Search" in response.text
    assert "Z-XRF-1Q" in response.text


def test_gui_create_from_template_passes_child_instantiation_flag(monkeypatch):
    template = _template(
        "Z-PAT-T1Q",
        name="Plate Template",
        category="container",
        type_name="plate",
        subtype="96well-generic",
        prefix="PAT",
        json_addl={
            "properties": {},
            "instantiation_layouts": [
                {
                    "relationship_type": "contains",
                    "child_templates": [
                        {"template_code": "container/well/generic/1.0", "count": 96}
                    ],
                }
            ],
        },
    )
    session = _Session(
        {
            generic_template: [template],
            generic_instance: [],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    created = SimpleNamespace(euid="Z-PAT-2Q")
    calls = []

    class _Factory:
        def __init__(self, template_manager, *, domain_code):
            self.domain_code = domain_code

        def create_instance(
            self, session, template_code, name, properties, create_children
        ):
            calls.append(
                {
                    "template_code": template_code,
                    "name": name,
                    "properties": properties,
                    "create_children": create_children,
                }
            )
            return created

    monkeypatch.setattr("daylily_tapdb.gui.router.InstanceFactory", _Factory)
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/create/Z-PAT-T1Q",
        data={
            "name": "Plate 1",
            "properties_json": '{"plate_type": "96-well"}',
            "create_children": "true",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/object/Z-PAT-2Q?notice=instance_created"
    assert calls == [
        {
            "template_code": "container/plate/96well-generic/1.0/",
            "name": "Plate 1",
            "properties": {"plate_type": "96-well"},
            "create_children": True,
        }
    ]


def test_gui_create_non_template_euid_returns_clear_404(monkeypatch):
    session = _Session(
        {
            generic_template: [],
            generic_instance: [_instance("Z-AGX-2N", "Instance not template")],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.get("/create/Z-AGX-2N")

    assert response.status_code == 404
    assert response.json()["detail"] == "Template not found: Z-AGX-2N"


def test_gui_create_api_passes_child_instantiation_flag(monkeypatch):
    template = _template(
        "Z-PAT-T1Q",
        name="Plate Template",
        category="container",
        type_name="plate",
        subtype="96well-generic",
        prefix="PAT",
    )
    session = _Session(
        {
            generic_template: [template],
            generic_instance: [],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    created = SimpleNamespace(euid="Z-PAT-2Q")

    class _Factory:
        def __init__(self, template_manager, *, domain_code):
            self.domain_code = domain_code

        def create_instance(
            self, session, template_code, name, properties, create_children
        ):
            assert template_code == "container/plate/96well-generic/1.0/"
            assert name == "Plate API"
            assert properties == {"plate_type": "96-well"}
            assert create_children is True
            return created

    monkeypatch.setattr("daylily_tapdb.gui.router.InstanceFactory", _Factory)
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/api/create/Z-PAT-T1Q",
        json={
            "name": "Plate API",
            "properties": {"plate_type": "96-well"},
            "create_children": True,
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "template_euid": "Z-PAT-T1Q",
        "template_code": "container/plate/96well-generic/1.0/",
        "instance_euid": "Z-PAT-2Q",
        "create_children": True,
    }


def test_gui_metrics_page_reuses_metrics_context(monkeypatch):
    calls = {}

    def fake_metrics_context(target, *, limit, config_path):
        calls["target"] = target
        calls["limit"] = limit
        calls["config_path"] = config_path
        return SimpleNamespace(
            metrics_message="",
            metrics_enabled=True,
            metrics_file="tapdb-metrics.tsv",
            dropped_count=0,
            summary=SimpleNamespace(
                by_path=[
                    SimpleNamespace(
                        path="/tapdb/search",
                        method="GET",
                        count=4,
                        total_seconds=0.05,
                    )
                ]
            ),
        )

    monkeypatch.setattr(
        "daylily_tapdb.gui.router.build_metrics_page_context",
        fake_metrics_context,
    )
    client = _client(monkeypatch)

    response = client.get("/admin/metrics")

    assert response.status_code == 200
    assert "DB Metrics" in response.text
    assert "/tapdb/search" in response.text
    assert calls == {
        "target": "target",
        "limit": 5000,
        "config_path": "/tmp/tapdb-config.yaml",
    }


def test_gui_metrics_api_reuses_metrics_context(monkeypatch):
    calls = {}

    def fake_metrics_context(target, *, limit, config_path):
        calls["target"] = target
        calls["limit"] = limit
        calls["config_path"] = config_path
        return {
            "metrics_enabled": True,
            "metrics_file": "tapdb-metrics.tsv",
            "dropped_count": 0,
            "summary": {
                "by_path": [
                    {
                        "path": "/tapdb/search",
                        "method": "GET",
                        "count": 4,
                        "total_seconds": 0.05,
                    }
                ]
            },
        }

    monkeypatch.setattr(
        "daylily_tapdb.gui.router.build_metrics_page_context",
        fake_metrics_context,
    )
    client = _client(monkeypatch)

    response = client.get("/api/admin/metrics?limit=100")

    assert response.status_code == 200
    assert response.json()["summary"]["by_path"][0]["path"] == "/tapdb/search"
    assert calls == {
        "target": "target",
        "limit": 100,
        "config_path": "/tmp/tapdb-config.yaml",
    }


def test_gui_readiness_page_and_api_report_seeded_external_template(monkeypatch):
    client = _client(monkeypatch)

    page = client.get("/admin/readiness")
    api = client.get("/api/admin/readiness")

    assert page.status_code == 200
    assert "TapDB GUI ready: True" in page.text
    assert "external_link_template" in page.text
    assert api.status_code == 200
    payload = api.json()
    assert payload["ready"] is True
    assert payload["domain_code"] == "Z"
    assert payload["public_domain_registry"]["repository"].endswith(
        "lsmc-bio/meridian-registry"
    )
    assert payload["public_domain_registry"]["version"] == "0.1.1"
    checks = {check["name"]: check for check in payload["checks"]}
    assert checks["governance"]["ok"] is True
    assert "public registry 0.1.1" in checks["governance"]["detail"]
    assert {
        "name": "external_link_template",
        "ok": True,
        "detail": "reference/external_identifier/tapdb_object/1.0/",
    } in payload["checks"]
    assert "meridian-registry" in page.text


def test_gui_meridian_validation_api_reports_prefix(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/api/admin/meridian/validate?prefix=XRF")

    assert response.status_code == 200
    payload = response.json()
    assert payload["domain_code"] == "Z"
    assert payload["prefix"] == "XRF"
    assert payload["prefix_owner"] == "daylily-tapdb"
    assert payload["public_domain_registry"]["version"] == "0.1.1"


def test_gui_status_redirect_adds_success_notice(monkeypatch):
    session = _Session(
        {
            generic_template: [_template()],
            generic_instance: [_instance("Z-SMP-1Q", "Sample 1")],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/object/Z-SMP-1Q/status",
        data={"bstatus": "paused"},
        follow_redirects=False,
    )
    notice_page = client.get("/object/Z-SMP-1Q?notice=status_updated")

    assert response.status_code == 303
    assert response.headers["location"] == "/object/Z-SMP-1Q?notice=status_updated"
    assert session.rows[generic_instance][0].bstatus == "paused"
    assert "Status updated." in notice_page.text


def test_gui_name_redirect_adds_success_notice(monkeypatch):
    session = _Session(
        {
            generic_template: [_template()],
            generic_instance: [_instance("Z-SMP-1Q", "Sample 1")],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/object/Z-SMP-1Q/name",
        data={"name": "Updated Sample"},
        follow_redirects=False,
    )
    notice_page = client.get("/object/Z-SMP-1Q?notice=name_updated")

    assert response.status_code == 303
    assert response.headers["location"] == "/object/Z-SMP-1Q?notice=name_updated"
    assert session.rows[generic_instance][0].name == "Updated Sample"
    assert "Name updated." in notice_page.text


def test_gui_object_mutation_apis_update_json_status_and_lineage(monkeypatch):
    source = _instance("Z-SMP-1Q", "Sample 1")
    parent = _instance("Z-PAR-22Q", "Parent 1")
    session = _Session(
        {
            generic_template: [_template()],
            generic_instance: [source, parent],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    json_response = client.post(
        "/api/object/Z-SMP-1Q/edit-json",
        json={"properties": {"color": "red"}},
    )
    name_response = client.post(
        "/api/object/Z-SMP-1Q/name",
        json={"name": "Sample 1 renamed"},
    )
    status_response = client.post(
        "/api/object/Z-SMP-1Q/status",
        json={"bstatus": "paused"},
    )
    lineage_response = client.post(
        "/api/object/Z-SMP-1Q/lineage",
        json={
            "related_euid": "Z-PAR-22Q",
            "direction": "parent",
            "relationship_type": "contains",
        },
    )

    assert json_response.status_code == 200
    assert json_response.json()["json_addl"] == {"properties": {"color": "red"}}
    assert source.json_addl == {"properties": {"color": "red"}}
    assert name_response.status_code == 200
    assert name_response.json() == {"euid": "Z-SMP-1Q", "name": "Sample 1 renamed"}
    assert source.name == "Sample 1 renamed"
    assert status_response.status_code == 200
    assert status_response.json() == {"euid": "Z-SMP-1Q", "bstatus": "paused"}
    assert source.bstatus == "paused"
    assert lineage_response.status_code == 200
    assert lineage_response.json()["parent_euid"] == "Z-PAR-22Q"
    assert lineage_response.json()["child_euid"] == "Z-SMP-1Q"
    assert lineage_response.json()["relationship_type"] == "contains"


def test_gui_object_mutation_api_rejects_immutable_fields(monkeypatch):
    session = _Session(
        {
            generic_template: [_template()],
            generic_instance: [_instance("Z-SMP-1Q", "Sample 1")],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/api/object/Z-SMP-1Q/name",
        json={"name": "New", "uid": 999, "template_euid": "Z-XRF-1Q"},
    )

    assert response.status_code == 400
    assert "Immutable object field(s)" in response.json()["detail"]
    assert "uid" in response.json()["detail"]
    assert "template_euid" in response.json()["detail"]
    assert session.rows[generic_instance][0].name == "Sample 1"


def test_gui_external_link_creates_typed_object_and_lineage(monkeypatch):
    session = _Session(
        {
            generic_template: [_template()],
            generic_instance: [_instance("Z-SMP-1Q", "Sample 1")],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    created = SimpleNamespace(
        uid=201,
        euid="Z-XRF-2Q",
        polymorphic_discriminator="generic_instance",
    )

    class _Factory:
        def __init__(self, template_manager, *, domain_code):
            self.domain_code = domain_code

        def create_instance(
            self, session, template_code, name, properties, create_children
        ):
            assert template_code == "reference/external_identifier/tapdb_object/1.0/"
            assert name == "bloom:M-123"
            assert properties["foreign_uid"] == "M-123"
            assert create_children is False
            return created

    monkeypatch.setattr("daylily_tapdb.gui.router.InstanceFactory", _Factory)
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/object/Z-SMP-1Q/external-links/new",
        data={
            "system": "bloom",
            "foreign_uid": "M-123",
            "relationship_type": "external_ref",
            "auth_mode": "none",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert (
        response.headers["location"] == "/object/Z-XRF-2Q?notice=external_link_created"
    )
    assert len(session.added) == 1
    assert session.added[0].parent_instance_uid == len("Z-SMP-1Q")
    assert session.added[0].child_instance_uid == 201


def test_gui_external_link_api_creates_typed_object_and_lineage(monkeypatch):
    session = _Session(
        {
            generic_template: [_template()],
            generic_instance: [_instance("Z-SMP-1Q", "Sample 1")],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    created = SimpleNamespace(
        uid=201,
        euid="Z-XRF-2Q",
        polymorphic_discriminator="generic_instance",
    )

    class _Factory:
        def __init__(self, template_manager, *, domain_code):
            self.domain_code = domain_code

        def create_instance(
            self, session, template_code, name, properties, create_children
        ):
            assert template_code == "reference/external_identifier/tapdb_object/1.0/"
            assert name == "dewey:M-456"
            assert properties["external_identifier"]["target_euid"] == "M-456"
            assert (
                properties["external_identifier"]["base_url"] == "https://dewey.example"
            )
            assert create_children is False
            return created

    monkeypatch.setattr("daylily_tapdb.gui.router.InstanceFactory", _Factory)
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/api/object/Z-SMP-1Q/external-links",
        json={
            "system": "dewey",
            "foreign_uid": "M-456",
            "relationship_type": "external_ref",
            "graph_base_url": "https://dewey.example",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["source_euid"] == "Z-SMP-1Q"
    assert payload["link_euid"] == "Z-XRF-2Q"
    assert payload["lineage_euid"]
    assert payload["relationship_type"] == "external_ref"
    assert len(session.added) == 1
    assert session.added[0].parent_instance_uid == len("Z-SMP-1Q")
    assert session.added[0].child_instance_uid == 201


def test_gui_external_link_creation_rejects_legacy_template_shape(monkeypatch):
    session = _Session(
        {
            generic_template: [
                _template(
                    category="external_identifier",
                    type_name="tapdb",
                    subtype="object",
                    prefix="XID",
                )
            ],
            generic_instance: [_instance("Z-SMP-1Q", "Sample 1")],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/api/object/Z-SMP-1Q/external-links",
        json={
            "system": "dewey",
            "foreign_uid": "M-456",
            "relationship_type": "external_ref",
        },
    )

    assert response.status_code == 422
    assert (
        response.json()["detail"]
        == "No reference/external_identifier/tapdb_object external link template is seeded."
    )
    assert session.added == []


def test_gui_exports_are_available_from_web_package():
    from daylily_tapdb.web import create_tapdb_gui_app, create_tapdb_gui_router

    assert callable(create_tapdb_gui_app)
    assert callable(create_tapdb_gui_router)


def test_gui_extra_and_package_data_contracts_are_declared():
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    optional = pyproject["project"]["optional-dependencies"]
    assert set(optional["gui"]) >= {"fastapi", "jinja2"}

    package_data = set(pyproject["tool"]["setuptools"]["package-data"]["daylily_tapdb"])
    assert "gui/static/css/*.css" in package_data
    assert "gui/static/js/*.js" in package_data
    assert "gui/templates/*.html" in package_data
