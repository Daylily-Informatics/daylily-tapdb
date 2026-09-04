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

    def count(self):
        return len(self._rows)

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

    def count(self):
        return len(self._rows)


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.added = []

    def query(self, model):
        return _Query(self.rows.get(model, []))

    def add(self, obj):
        self.added.append(obj)

    def execute(self, statement, params=None):
        del params
        if not hasattr(statement, "column_descriptions"):
            return SimpleNamespace(
                mappings=lambda: SimpleNamespace(all=lambda: []),
            )
        entity = statement.column_descriptions[0]["entity"]
        params = statement.compile().params
        value = next(iter(params.values()))
        predicate = str(statement.whereclause)
        if ".machine_uuid " in predicate:
            field = "machine_uuid"
            value = str(value)
        elif ".uid " in predicate:
            field = "uid"
        else:
            field = "euid"
        matches = [
            row
            for row in self.rows.get(entity, [])
            if str(getattr(row, field, None)) == str(value)
        ]
        return SimpleNamespace(
            scalar_one_or_none=lambda: matches[0] if matches else None
        )

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
        template_uid=10,
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
    euid="persisted-template-euid",
    *,
    name="External Object Reference",
    category="reference",
    type_name="external_identifier",
    subtype="tapdb_object",
    prefix="XRF",
    json_addl=None,
    uid=10,
    validator_ref="UNIVERSAL_PASS@1",
):
    return SimpleNamespace(
        uid=uid,
        euid=euid,
        name=name,
        domain_code="Z",
        category=category,
        type=type_name,
        subtype=subtype,
        version="1.0",
        instance_prefix=prefix,
        validator_ref=validator_ref,
        bstatus="active",
        is_deleted=False,
        json_addl=json_addl or {"properties": {"external_identifier": {}}},
        polymorphic_discriminator="generic_template",
        instance_polymorphic_identity="generic_instance",
        created_dt=None,
        modified_dt=None,
    )


def _repair_template():
    return _template(
        "Z-TPX-RPR1",
        uid=99,
        name="Repair Record",
        category="evidence",
        type_name="repair",
        subtype="record",
        prefix="GVR",
        json_addl={"properties": {}},
    )


def _patch_gui_app_startup(monkeypatch):
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.set_cli_context",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.get_admin_settings",
        lambda **_kwargs: {
            "target_name": "test",
            "production_like": False,
            "auth_mode": "disabled",
            "session_secret": "test-session-secret",
            "allowed_origins": [],
        },
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.get_db_config",
        lambda **_kwargs: {
            "client_id": "testclient",
            "domain_code": "Z",
            "owner_repo_name": "daylily-tapdb",
            "domain_registry_path": "daylily_tapdb/etc/domain_code_registry.json",
            "prefix_ownership_registry_path": (
                "daylily_tapdb/etc/prefix_ownership_registry.json"
            ),
        },
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.mount_tapdb_dag_surfaces",
        lambda *_args, **_kwargs: SimpleNamespace(mounted=True, diagnostic=None),
    )


def _client(monkeypatch, *, role="admin", session=None, nav_links=()):
    _patch_gui_app_startup(monkeypatch)
    if session is None:
        session = _Session(
            {
                generic_template: [
                    _template(),
                    _template(
                        "persisted-opaque-template-euid",
                        uid=11,
                        name="Opaque External Identifier",
                        subtype="opaque",
                    ),
                ],
                generic_instance: [_instance("persisted-sample-euid", "Sample 1")],
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


def test_gui_mount_redirects_unauthenticated_html_and_blocks_api(monkeypatch):
    _patch_gui_app_startup(monkeypatch)
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


def test_gui_mounted_api_blocks_unauthenticated_with_json_401(monkeypatch):
    _patch_gui_app_startup(monkeypatch)
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
    assert "/object/persisted-sample-euid" in response.text
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
    root = _instance("persisted-sample-euid", "Sample 1")
    child = _instance("Z-CHD-2Q", "Child 1")

    monkeypatch.setattr(
        "daylily_tapdb.gui.router.find_object_by_euid",
        lambda session, euid: (root if euid == root.euid else None, "instance"),
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.build_graph_v2_payload",
        lambda obj, record_type, service_id, depth, max_nodes, max_edges=None: {
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

    response = client.get("/object/persisted-sample-euid/graph")

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

    response = client.get("/api/object/persisted-sample-euid")

    assert response.status_code == 200
    payload = response.json()
    assert payload["obj"]["euid"] == "persisted-sample-euid"
    assert payload["record_type"] == "instance"
    assert payload["relationships"] == {"parent_of": [], "child_of": []}
    assert payload["audit_rows"] == []
    assert payload["external_refs"] == []


def test_gui_object_page_links_visible_euids_to_canonical_details(monkeypatch):
    source = _instance("persisted-sample-euid", "Sample 1")
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

    response = client.get("/object/persisted-sample-euid")

    assert response.status_code == 200
    assert 'href="/object/persisted-sample-euid"' in response.text
    assert 'href="/object/Z-LIN-3Q"' in response.text
    assert 'href="/object/Z-CHD-2Q"' in response.text


def test_gui_admin_pages_require_admin(monkeypatch):
    client = _client(monkeypatch, role="user")

    response = client.get("/admin/meridian")

    assert response.status_code == 403
    assert response.json()["detail"] == "tapdb_gui_admin_required"


def test_gui_create_routes_require_admin(monkeypatch):
    client = _client(monkeypatch, role="user")

    page = client.get("/create/persisted-template-euid")
    post_page = client.post(
        "/create/persisted-template-euid",
        data={"name": "Link", "properties_json": "{}"},
    )
    post_api = client.post(
        "/api/create/persisted-template-euid",
        json={"name": "Link", "properties": {}},
    )

    assert page.status_code == 403
    assert post_page.status_code == 403
    assert post_api.status_code == 403


def test_gui_create_form_prefills_template_properties(monkeypatch):
    template = _template(
        euid="Z-SYS-1Q",
        name="System User",
        category="actor",
        type_name="user",
        subtype="system",
        prefix="SYS",
        json_addl={
            "properties": {
                "login_identifier": "",
                "email": "",
                "role": "user",
            }
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
    client = _client(monkeypatch, session=session)

    response = client.get("/create/Z-SYS-1Q")

    assert response.status_code == 200
    assert "actor/user/system/1.0" in response.text
    assert "login_identifier" in response.text
    assert "email" in response.text
    assert "role" in response.text
    assert "user" in response.text


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
    assert "persisted-template-euid" in response.text
    assert "/object/persisted-template-euid" in response.text
    assert "/create/persisted-template-euid" in response.text
    assert "New Template Pack" not in response.text
    assert "Build New Template" in response.text
    assert "/templates/new?seed_euid=persisted-template-euid" in response.text


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
    assert (
        "Template save failed: prefix_ownership_registry.json is invalid JSON"
        in response.text
    )


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


def test_gui_home_is_the_operator_overview(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/")

    assert response.status_code == 200
    assert "Search" in response.text
    assert "Overview" in response.text
    assert "active templates" in response.text


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
        "config_path": str(Path("/tmp/tapdb-config.yaml").resolve()),
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
        "config_path": str(Path("/tmp/tapdb-config.yaml").resolve()),
    }


def test_gui_readiness_page_and_api_report_seeded_external_template(monkeypatch):
    client = _client(monkeypatch)

    page = client.get("/admin/readiness")
    api = client.get("/api/admin/readiness")

    assert page.status_code == 200
    assert "TapDB GUI ready: True" in page.text
    assert "canonical_external_reference_templates" in page.text
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
    assert checks["canonical_external_reference_templates"] == {
        "name": "canonical_external_reference_templates",
        "ok": True,
        "detail": (
            "reference/external_identifier/opaque/1.0/, "
            "reference/external_identifier/tapdb_object/1.0/"
        ),
    }
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
            generic_instance: [_instance("persisted-sample-euid", "Sample 1")],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/object/persisted-sample-euid/status",
        data={"bstatus": "paused", "apply": "true"},
        follow_redirects=False,
    )
    notice_page = client.get("/object/persisted-sample-euid?notice=status_updated")

    assert response.status_code == 303
    assert (
        response.headers["location"]
        == "/object/persisted-sample-euid?notice=status_updated"
    )
    assert session.rows[generic_instance][0].bstatus == "paused"
    assert "Status updated." in notice_page.text


def test_gui_name_redirect_adds_success_notice(monkeypatch):
    session = _Session(
        {
            generic_template: [_template()],
            generic_instance: [_instance("persisted-sample-euid", "Sample 1")],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/object/persisted-sample-euid/name",
        data={"name": "Updated Sample", "apply": "true"},
        follow_redirects=False,
    )
    notice_page = client.get("/object/persisted-sample-euid?notice=name_updated")

    assert response.status_code == 303
    assert (
        response.headers["location"]
        == "/object/persisted-sample-euid?notice=name_updated"
    )
    assert session.rows[generic_instance][0].name == "Updated Sample"
    assert "Name updated." in notice_page.text


def test_gui_object_mutation_apis_preview_then_apply_with_receipts(monkeypatch):
    source = _instance("persisted-sample-euid", "Sample 1")
    parent = _instance("Z-PAR-22Q", "Parent 1")
    session = _Session(
        {
            generic_template: [_template(), _repair_template()],
            generic_instance: [source, parent],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    identity_before = (source.uid, source.euid, source.template_uid, source.created_dt)
    json_preview = client.post(
        "/api/object/persisted-sample-euid/edit-json",
        json={"json_addl": {"properties": {"color": "red"}}},
    )
    name_preview = client.post(
        "/api/object/persisted-sample-euid/name",
        json={"name": "Sample 1 renamed"},
    )
    status_preview = client.post(
        "/api/object/persisted-sample-euid/status",
        json={"bstatus": "paused"},
    )
    assert json_preview.status_code == 200
    assert json_preview.json()["operation"] == "repair"
    assert json_preview.json()["dry_run"] is True
    assert json_preview.json()["applied"] is False
    assert json_preview.json()["changes"]["subject_mutated"] is False
    assert name_preview.status_code == 200
    assert name_preview.json()["dry_run"] is True
    assert status_preview.status_code == 200
    assert status_preview.json()["dry_run"] is True
    assert source.name == "Sample 1"
    assert source.bstatus == "active"
    assert source.json_addl == {"properties": {"color": "blue"}}
    assert session.added == []

    json_response = client.post(
        "/api/object/persisted-sample-euid/edit-json",
        json={
            "json_addl": {"properties": {"color": "red"}},
            "apply": True,
        },
    )
    name_response = client.post(
        "/api/object/persisted-sample-euid/name",
        json={"name": "Sample 1 renamed", "apply": True},
    )
    status_response = client.post(
        "/api/object/persisted-sample-euid/status",
        json={"bstatus": "paused", "apply": True},
    )
    lineage_response = client.post(
        "/api/object/persisted-sample-euid/lineage",
        json={
            "related_euid": "Z-PAR-22Q",
            "direction": "parent",
            "relationship_type": "contains",
        },
    )

    assert json_response.status_code == 200
    assert json_response.json()["format"] == "tapdb.object-operation-receipt/v1"
    assert json_response.json()["euid"] == "persisted-sample-euid"
    assert json_response.json()["applied"] is True
    assert json_response.json()["changes"]["subject_mutated"] is False
    assert json_response.json()["changes"]["repair_payload"] == {
        "properties": {"color": "red"}
    }
    assert source.json_addl == {"properties": {"color": "blue"}}
    assert name_response.status_code == 200
    assert name_response.json()["operation"] == "update"
    assert name_response.json()["applied"] is True
    assert name_response.json()["changes"]["name"] == {
        "old": "Sample 1",
        "new": "Sample 1 renamed",
    }
    assert source.name == "Sample 1 renamed"
    assert status_response.status_code == 200
    assert status_response.json()["operation"] == "update"
    assert status_response.json()["applied"] is True
    assert status_response.json()["changes"]["bstatus"] == {
        "old": "active",
        "new": "paused",
    }
    assert source.bstatus == "paused"
    assert (source.uid, source.euid, source.template_uid, source.created_dt) == (
        identity_before
    )
    assert lineage_response.status_code == 200
    assert lineage_response.json()["parent_euid"] == "Z-PAR-22Q"
    assert lineage_response.json()["child_euid"] == "persisted-sample-euid"
    assert lineage_response.json()["relationship_type"] == "contains"
    assert lineage_response.json()["v0_edge"]["compliance_status"] == "generic"
    assert lineage_response.json()["assessment"]["state"] == "valid_current"
    assert lineage_response.json()["assessment"]["subject_mutated"] is False
    assert session.added[0].json_addl["properties"]["subject_mutated"] is False


def test_gui_object_editor_data_assessment_and_revalidation_are_ephemeral(monkeypatch):
    source = _instance("persisted-sample-euid", "Sample 1")
    session = _Session(
        {
            generic_template: [_template(validator_ref="CUSTOM_VALIDATOR@1")],
            generic_instance: [source],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    page = client.get("/object/persisted-sample-euid")
    editor = client.get("/api/object/persisted-sample-euid/editor-data")
    assessment = client.post("/api/object/persisted-sample-euid/assess")
    revalidation = client.post("/api/object/persisted-sample-euid/revalidate")
    recommendations = client.get(
        "/api/object/persisted-sample-euid/repair-recommendations"
    )

    assert page.status_code == 200
    assert "Ephemeral assessment; repair evidence is explicit." in page.text
    assert "Create repair (Apply)" in page.text
    assert 'name="apply" value="true">Set Name (Apply)' in page.text
    assert 'name="apply" value="true">Set Status (Apply)' in page.text
    assert editor.status_code == 200
    assert editor.json()["validator_ref"] == "CUSTOM_VALIDATOR@1"
    assert editor.json()["assessment"]["subject_mutated"] is False
    assert assessment.status_code == 200
    assert assessment.json()["state"] == "not_evaluated_current"
    assert revalidation.status_code == 200
    assert revalidation.json()["revalidated"] is True
    assert recommendations.status_code == 200
    assert recommendations.json()["subject_mutated"] is False
    assert source.json_addl == {"properties": {"color": "blue"}}


def test_gui_repair_api_previews_then_applies_without_mutating_subject(monkeypatch):
    source = _instance("persisted-sample-euid", "Sample 1")
    session = _Session(
        {
            generic_template: [_template(), _repair_template()],
            generic_instance: [source],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    preview = client.post(
        "/api/object/persisted-sample-euid/repairs",
        json={
            "reason": "correct color",
            "repair_payload": {"properties": {"color": "green"}},
        },
    )

    assert preview.status_code == 200, preview.text
    assert preview.json()["operation"] == "repair"
    assert preview.json()["dry_run"] is True
    assert preview.json()["applied"] is False
    assert preview.json()["changes"]["subject_mutated"] is False
    assert session.added == []
    assert source.json_addl == {"properties": {"color": "blue"}}

    response = client.post(
        "/api/object/persisted-sample-euid/repairs",
        json={
            "reason": "correct color",
            "repair_payload": {"properties": {"color": "green"}},
            "apply": True,
        },
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["format"] == "tapdb.object-operation-receipt/v1"
    assert payload["euid"] == "persisted-sample-euid"
    assert payload["dry_run"] is False
    assert payload["applied"] is True
    assert payload["changes"]["subject_mutated"] is False
    assert payload["changes"]["repair_record"]["template_code"] == (
        "evidence/repair/record/1.0/"
    )
    assert session.added[0].json_addl["properties"]["repair_payload"] == {
        "properties": {"color": "green"}
    }
    assert source.json_addl == {"properties": {"color": "blue"}}


def test_gui_repair_form_redirects_with_notice(monkeypatch):
    source = _instance("persisted-sample-euid", "Sample 1")
    session = _Session(
        {
            generic_template: [_template(), _repair_template()],
            generic_instance: [source],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/object/persisted-sample-euid/repairs",
        data={
            "reason": "correct payload",
            "repair_payload": '{"properties":{"color":"yellow"}}',
            "apply": "true",
        },
        follow_redirects=False,
    )
    notice_page = client.get("/object/persisted-sample-euid?notice=repair_created")

    assert response.status_code == 303
    assert (
        response.headers["location"]
        == "/object/persisted-sample-euid?notice=repair_created"
    )
    assert "Repair evidence created." in notice_page.text
    assert source.json_addl == {"properties": {"color": "blue"}}


def test_gui_mutation_forms_require_clicked_apply_and_do_not_mutate(monkeypatch):
    source = _instance("persisted-sample-euid", "Sample 1")
    session = _Session(
        {
            generic_template: [_template(), _repair_template()],
            generic_instance: [source],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    responses = [
        client.post("/object/persisted-sample-euid/name", data={"name": "Not applied"}),
        client.post("/object/persisted-sample-euid/status", data={"bstatus": "paused"}),
        client.post(
            "/object/persisted-sample-euid/repairs",
            data={"reason": "not applied", "repair_payload": "{}"},
        ),
        client.post(
            "/object/persisted-sample-euid/edit-json",
            data={"json_addl": '{"properties":{"color":"red"}}'},
        ),
    ]

    assert [response.status_code for response in responses] == [400, 400, 400, 400]
    assert all(
        "clicked Apply button" in response.json()["detail"] for response in responses
    )
    assert source.name == "Sample 1"
    assert source.bstatus == "active"
    assert source.json_addl == {"properties": {"color": "blue"}}
    assert session.added == []


def test_gui_legacy_edit_json_form_applies_repair_evidence_only(monkeypatch):
    source = _instance("persisted-sample-euid", "Sample 1")
    identity_before = (source.uid, source.euid, source.template_uid, source.created_dt)
    session = _Session(
        {
            generic_template: [_template(), _repair_template()],
            generic_instance: [source],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/object/persisted-sample-euid/edit-json",
        data={
            "json_addl": '{"properties":{"color":"red"}}',
            "apply": "true",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert (
        response.headers["location"]
        == "/object/persisted-sample-euid?notice=repair_created"
    )
    assert source.json_addl == {"properties": {"color": "blue"}}
    assert (source.uid, source.euid, source.template_uid, source.created_dt) == (
        identity_before
    )
    assert session.added[0].json_addl["properties"]["subject_mutated"] is False


def test_gui_mutation_apis_reject_templates_even_when_apply_is_explicit(monkeypatch):
    template = _template()
    session = _Session(
        {
            generic_template: [template, _repair_template()],
            generic_instance: [],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    name = client.post(
        f"/api/object/{template.euid}/name",
        json={"name": "Forbidden", "apply": True},
    )
    repair = client.post(
        f"/api/object/{template.euid}/repairs",
        json={"reason": "Forbidden", "repair_payload": {}, "apply": True},
    )

    assert name.status_code == 403
    assert repair.status_code == 403
    assert template.name == "External Object Reference"
    assert session.added == []


def test_gui_lineage_api_requires_v0_metadata_for_canonical_edges(monkeypatch):
    source = _instance("persisted-sample-euid", "Sample 1")
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

    response = client.post(
        "/api/object/persisted-sample-euid/lineage",
        json={
            "related_euid": "Z-PAR-22Q",
            "direction": "parent",
            "relationship_type": "HOLDS_MATERIAL",
        },
    )

    assert response.status_code == 400
    assert "Canonical LSMC v0 edge writes require v0_edge metadata" in response.text


def test_gui_lineage_api_creates_v0_edge_with_evidence(monkeypatch):
    source = _instance("persisted-sample-euid", "Sample 1")
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

    response = client.post(
        "/api/object/persisted-sample-euid/lineage",
        json={
            "related_euid": "Z-PAR-22Q",
            "direction": "parent",
            "relationship_type": "HOLDS_MATERIAL",
            "v0_edge": {
                "asserted_by_system": "tapdb-test",
                "evidence_refs": [{"euid": "Z-EVD-1Q"}],
                "correlation_id": "corr-1",
                "causation_id": "cause-1",
                "edge_state": "active",
            },
        },
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["v0_edge"]["compliance_status"] == "canonical"
    assert payload["v0_edge"]["edge_type"] == "HOLDS_MATERIAL"
    assert payload["v0_edge"]["semantic_source"]["euid"] == "Z-PAR-22Q"
    assert payload["v0_edge"]["semantic_target"]["euid"] == "persisted-sample-euid"
    assert session.added[0].json_addl["properties"]["v0_edge"]["evidence_refs"] == [
        {"euid": "Z-EVD-1Q"}
    ]


def test_gui_object_mutation_api_rejects_immutable_fields(monkeypatch):
    session = _Session(
        {
            generic_template: [_template()],
            generic_instance: [_instance("persisted-sample-euid", "Sample 1")],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/api/object/persisted-sample-euid/name",
        json={"name": "New", "uid": 999, "template_euid": "persisted-template-euid"},
    )

    assert response.status_code == 400
    assert "Immutable object field(s)" in response.json()["detail"]
    assert "uid" in response.json()["detail"]
    assert "template_euid" in response.json()["detail"]
    assert session.rows[generic_instance][0].name == "Sample 1"


def test_gui_legacy_external_link_writers_are_removed(monkeypatch):
    client = _client(monkeypatch)

    html = client.post(
        "/object/persisted-sample-euid/external-links/new",
        data={"system": "legacy", "foreign_uid": "copied-id"},
    )
    api = client.post(
        "/api/object/persisted-sample-euid/external-links",
        json={"system": "legacy", "foreign_uid": "copied-id"},
    )

    assert html.status_code == 404
    assert api.status_code == 404


def test_gui_exports_are_available_from_web_package():
    from daylily_tapdb.web import create_tapdb_gui_app, create_tapdb_gui_router

    assert callable(create_tapdb_gui_app)
    assert callable(create_tapdb_gui_router)


def test_gui_runtime_surfaces_share_the_sanitized_payload(monkeypatch):
    payload = {
        "format": "tapdb.runtime-info/v1",
        "package": {"version": "9.2.0-test"},
        "python": {},
        "meridian": {},
        "git": {},
        "config": {},
        "database": {},
        "scope": {},
        "storage": {},
        "ui": {},
        "dag": {},
    }
    monkeypatch.setattr(
        "daylily_tapdb.runtime_info.build_runtime_info",
        lambda **_kwargs: payload,
    )
    client = _client(monkeypatch)

    response = client.get("/api/admin/runtime")
    assert response.status_code == 200
    assert response.json() == payload
    page = client.get("/admin/runtime")
    assert page.status_code == 200
    assert "9.2.0-test" in page.text


def test_gui_template_repository_api_and_visible_status(monkeypatch, tmp_path):
    pack = tmp_path / "repository-templates.json"
    inventory = {
        "format": "tapdb.repository-template-inventory/v1",
        "status": "ok",
        "items": [{"stored_euid": "persisted-template-euid", "status": "backed-up"}],
        "counts": {"pending": 0, "backed-up": 1, "failed": 0},
    }
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.repository_inventory",
        lambda *_args, **_kwargs: inventory,
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.export_repository_pack",
        lambda *_args, **kwargs: {
            "format": "tapdb.repository-template-receipt/v1",
            "repository_pack": str(kwargs.get("pack_path") or pack),
        },
    )
    client = _client(monkeypatch)

    response = client.get(
        "/api/templates/repository/status",
        params={"repository_pack": str(pack)},
    )
    assert response.status_code == 200
    assert response.json() == inventory
    page = client.get("/templates", params={"repository_pack": str(pack)})
    assert page.status_code == 200
    assert "backed-up" in page.text
    exported = client.post(
        "/api/templates/repository/export",
        json={"repository_pack": str(pack)},
    )
    assert exported.status_code == 200
    assert exported.json()["format"] == "tapdb.repository-template-receipt/v1"


def test_gui_extra_and_package_data_contracts_are_declared():
    pyproject = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))

    optional = pyproject["project"]["optional-dependencies"]
    assert set(optional["gui"]) >= {"fastapi", "jinja2"}

    package_data = set(pyproject["tool"]["setuptools"]["package-data"]["daylily_tapdb"])
    assert "gui/static/css/*.css" in package_data
    assert "gui/static/js/*.js" in package_data
    assert "gui/templates/*.html" in package_data
