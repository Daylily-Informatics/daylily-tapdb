from __future__ import annotations

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
from daylily_tapdb.web.bridge import TapdbHostBridge


class _Related:
    def __init__(self, rows):
        self._rows = list(rows)

    def filter_by(self, **kwargs):
        return _Related(
            [
                row
                for row in self._rows
                if all(getattr(row, key, None) == value for key, value in kwargs.items())
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
                if all(getattr(row, key, None) == value for key, value in kwargs.items())
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
    category="XRF",
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


def _client(monkeypatch, *, role="admin", session=None):
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
                    "category": "PLT",
                    "type": "container",
                    "subtype": "example_plate",
                    "version": "1.0",
                    "instance_prefix": "PLT",
                    "instance_polymorphic_identity": "generic_instance",
                    "json_addl": {
                        "properties": {},
                        "instantiation_layouts": [
                            {
                                "relationship_type": "contains",
                                "name_pattern": "{parent_name}_{index}",
                                "child_templates": [
                                    {
                                        "template_code": "WEL/container/example_well/1.0",
                                        "count": 2,
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
    assert 'value="WEL/container/example_well/1.0"' in response.text
    assert "Child Instantiation" in response.text


def test_gui_create_from_template_passes_child_instantiation_flag(monkeypatch):
    template = _template(
        "Z-PLT-T1Q",
        name="Plate Template",
        category="PLT",
        type_name="container",
        subtype="example_plate",
        prefix="PLT",
        json_addl={
            "properties": {},
            "instantiation_layouts": [
                {
                    "relationship_type": "contains",
                    "child_templates": [
                        {"template_code": "WEL/container/example_well/1.0", "count": 2}
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
    created = SimpleNamespace(euid="Z-PLT-2Q")
    calls = []

    class _Factory:
        def __init__(self, template_manager, *, domain_code):
            self.domain_code = domain_code

        def create_instance(self, session, template_code, name, properties, create_children):
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
        "/create/Z-PLT-T1Q",
        data={
            "name": "Plate 1",
            "properties_json": '{"plate_type": "96-well"}',
            "create_children": "true",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    assert response.headers["location"] == "/object/Z-PLT-2Q?notice=instance_created"
    assert calls == [
        {
            "template_code": "PLT/container/example_plate/1.0/",
            "name": "Plate 1",
            "properties": {"plate_type": "96-well"},
            "create_children": True,
        }
    ]


def test_gui_create_api_passes_child_instantiation_flag(monkeypatch):
    template = _template(
        "Z-PLT-T1Q",
        name="Plate Template",
        category="PLT",
        type_name="container",
        subtype="example_plate",
        prefix="PLT",
    )
    session = _Session(
        {
            generic_template: [template],
            generic_instance: [],
            generic_instance_lineage: [],
            audit_log: [],
        }
    )
    created = SimpleNamespace(euid="Z-PLT-2Q")

    class _Factory:
        def __init__(self, template_manager, *, domain_code):
            self.domain_code = domain_code

        def create_instance(self, session, template_code, name, properties, create_children):
            assert template_code == "PLT/container/example_plate/1.0/"
            assert name == "Plate API"
            assert properties == {"plate_type": "96-well"}
            assert create_children is True
            return created

    monkeypatch.setattr("daylily_tapdb.gui.router.InstanceFactory", _Factory)
    client = _client(monkeypatch, session=session)

    response = client.post(
        "/api/create/Z-PLT-T1Q",
        json={
            "name": "Plate API",
            "properties": {"plate_type": "96-well"},
            "create_children": True,
        },
    )

    assert response.status_code == 200
    assert response.json() == {
        "template_euid": "Z-PLT-T1Q",
        "template_code": "PLT/container/example_plate/1.0/",
        "instance_euid": "Z-PLT-2Q",
        "create_children": True,
    }


def test_gui_metrics_page_reuses_metrics_context(monkeypatch):
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.build_metrics_page_context",
        lambda target, limit: SimpleNamespace(
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
        ),
    )
    client = _client(monkeypatch)

    response = client.get("/admin/metrics")

    assert response.status_code == 200
    assert "DB Metrics" in response.text
    assert "/tapdb/search" in response.text


def test_gui_metrics_api_reuses_metrics_context(monkeypatch):
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.build_metrics_page_context",
        lambda target, limit: {
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
        },
    )
    client = _client(monkeypatch)

    response = client.get("/api/admin/metrics?limit=100")

    assert response.status_code == 200
    assert response.json()["summary"]["by_path"][0]["path"] == "/tapdb/search"


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
    assert {
        "name": "external_link_template",
        "ok": True,
        "detail": "XRF/external_identifier/tapdb_object/1.0/",
    } in payload["checks"]


def test_gui_meridian_validation_api_reports_prefix(monkeypatch):
    client = _client(monkeypatch)

    response = client.get("/api/admin/meridian/validate?prefix=XRF")

    assert response.status_code == 200
    payload = response.json()
    assert payload["domain_code"] == "Z"
    assert payload["prefix"] == "XRF"
    assert payload["prefix_owner"] == "daylily-tapdb"


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
    assert status_response.status_code == 200
    assert status_response.json() == {"euid": "Z-SMP-1Q", "bstatus": "paused"}
    assert source.bstatus == "paused"
    assert lineage_response.status_code == 200
    assert lineage_response.json()["parent_euid"] == "Z-PAR-22Q"
    assert lineage_response.json()["child_euid"] == "Z-SMP-1Q"
    assert lineage_response.json()["relationship_type"] == "contains"


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

        def create_instance(self, session, template_code, name, properties, create_children):
            assert template_code == "XRF/external_identifier/tapdb_object/1.0/"
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
        response.headers["location"]
        == "/object/Z-XRF-2Q?notice=external_link_created"
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

        def create_instance(self, session, template_code, name, properties, create_children):
            assert template_code == "XRF/external_identifier/tapdb_object/1.0/"
            assert name == "dewey:M-456"
            assert properties["external_identifier"]["target_euid"] == "M-456"
            assert properties["external_identifier"]["base_url"] == "https://dewey.example"
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
    assert response.json() == {
        "source_euid": "Z-SMP-1Q",
        "link_euid": "Z-XRF-2Q",
        "lineage_euid": None,
        "relationship_type": "external_ref",
    }
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
        == "No XRF/external_identifier/tapdb_object external link template is seeded."
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
