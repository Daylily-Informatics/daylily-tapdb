from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import uvicorn
from fastapi import FastAPI
from fastapi.responses import RedirectResponse

from daylily_tapdb.gui import create_tapdb_gui_app
from daylily_tapdb.models.audit import audit_log
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.web import TapdbHostBridge, TapdbHostNavLink

sync_api = pytest.importorskip("playwright.sync_api")


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
    def __init__(self, state):
        self.state = state

    def query(self, model):
        rows = {
            generic_template: self.state.templates,
            generic_instance: self.state.instances,
            generic_instance_lineage: self.state.lineages,
            audit_log: self.state.audit_rows,
        }.get(model, [])
        return _Query(rows)

    def add(self, obj):
        if isinstance(obj, generic_instance_lineage):
            self.state.lineages.append(obj)

    def flush(self):
        return None


class _Conn:
    def __init__(self, state):
        self.state = state
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    @contextmanager
    def session_scope(self, commit=False):
        del commit
        yield _Session(self.state)


class _GuiState:
    def __init__(self):
        self.template_counter = 3
        self.instance_counter = 2
        self.lineage_counter = 1
        self.templates = [
            self.template(
                "Z-ACT-T1",
                "Actor Template",
                "actor",
                "person",
                "browser_actor",
                "ACT",
            ),
            self.template(
                "Z-PAT-T1",
                "Plate Template",
                "container",
                "plate",
                "browser_plate",
                "PAT",
                json_addl={
                    "properties": {"plate_type": "browser"},
                    "instantiation_layouts": [
                        {
                            "relationship_type": "contains",
                            "name_pattern": "{parent_name}_well_{index}",
                            "child_templates": [
                                {
                                    "template_code": "container/well/browser_well/1.0",
                                    "count": 2,
                                }
                            ],
                        }
                    ],
                },
            ),
            self.template(
                "Z-WEN-T1",
                "Well Template",
                "container",
                "well",
                "browser_well",
                "WEN",
            ),
        ]
        self.instances = [
            self.instance("Z-SMP-1Q", "Sample 1", "SMP", "sample", "tube"),
            self.instance("Z-SMP-2Q", "Sample 2", "SMP", "sample", "tube"),
        ]
        self.lineages = []
        self.audit_rows = [
            SimpleNamespace(
                changed_at="2026-06-10T00:00:00Z",
                changed_by="tapdb_admin",
                operation="INSERT",
                column="json_addl",
                new_value='{"properties": {}}',
            )
        ]

    def template(
        self,
        euid,
        name,
        category,
        type_name,
        subtype,
        prefix,
        *,
        json_addl=None,
    ):
        return SimpleNamespace(
            uid=len(self.templates) + 1 if hasattr(self, "templates") else 1,
            euid=euid,
            name=name,
            domain_code="Z",
            category=category,
            type=type_name,
            subtype=subtype,
            version="1.0",
            instance_prefix=prefix,
            validator_ref="UNIVERSAL_PASS@1",
            bstatus="active",
            is_deleted=False,
            json_addl=json_addl
            or {"properties": {"display_name": ""}, "instantiation_layouts": []},
            polymorphic_discriminator="generic_template",
            instance_polymorphic_identity="generic_instance",
            created_dt=None,
            modified_dt=None,
        )

    def instance(self, euid, name, category, type_name, subtype, *, json_addl=None):
        return SimpleNamespace(
            uid=len(self.instances) + 10 if hasattr(self, "instances") else 10,
            euid=euid,
            name=name,
            template_uid=1,
            category=category,
            type=type_name,
            subtype=subtype,
            version="1.0",
            bstatus="active",
            is_deleted=False,
            json_addl=json_addl or {"properties": {"display_name": name}},
            polymorphic_discriminator="generic_instance",
            created_dt=None,
            modified_dt=None,
        )

    def lineage(self, parent_euid, child_euid, relationship_type):
        self.lineage_counter += 1
        row = SimpleNamespace(
            uid=100 + self.lineage_counter,
            euid=f"Z-EDG-{self.lineage_counter}",
            parent_euid=parent_euid,
            child_euid=child_euid,
            parent_instance_uid=0,
            child_instance_uid=0,
            relationship_type=relationship_type,
            category="EDG",
            type="lineage",
            subtype="generic",
            version="1.0",
            bstatus="active",
            is_deleted=False,
            json_addl={},
            polymorphic_discriminator="generic_instance_lineage",
            created_dt=None,
            modified_dt=None,
        )
        self.lineages.append(row)
        return row

    def lookup(self, euid):
        for collection, record_type in (
            (self.templates, "template"),
            (self.instances, "instance"),
            (self.lineages, "lineage"),
        ):
            for item in collection:
                if item.euid == euid:
                    return item, record_type
        return None, None

    def next_instance_euid(self, prefix):
        self.instance_counter += 1
        return f"Z-{prefix}-{self.instance_counter}Q"


def _record_to_item(obj, record_type):
    return {
        "id": obj.euid,
        "euid": obj.euid,
        "record_type": record_type,
        "name": obj.name,
        "display_label": obj.name,
        "category": obj.category,
        "type": obj.type,
        "subtype": obj.subtype,
        "version": obj.version,
        "bstatus": obj.bstatus,
    }


def _object_context(state, euid):
    obj, record_type = state.lookup(euid)
    if obj is None:
        from fastapi import HTTPException

        raise HTTPException(status_code=404, detail=f"Object not found: {euid}")
    row = _record_to_item(obj, record_type)
    row.update(
        {
            "uid": obj.uid,
            "kind": record_type,
            "json_addl": getattr(obj, "json_addl", {}),
            "created_dt": None,
            "modified_dt": None,
        }
    )
    parent_of = []
    child_of = []
    for lineage in state.lineages:
        if lineage.parent_euid == euid:
            child, _ = state.lookup(lineage.child_euid)
            parent_of.append(
                {
                    "lineage_euid": lineage.euid,
                    "related_euid": lineage.child_euid,
                    "related_name": getattr(child, "name", ""),
                    "relationship_type": lineage.relationship_type,
                    "v0_edge": {"edge_type": None, "compliance_status": "missing"},
                }
            )
        if lineage.child_euid == euid:
            parent, _ = state.lookup(lineage.parent_euid)
            child_of.append(
                {
                    "lineage_euid": lineage.euid,
                    "related_euid": lineage.parent_euid,
                    "related_name": getattr(parent, "name", ""),
                    "relationship_type": lineage.relationship_type,
                    "v0_edge": {"edge_type": None, "compliance_status": "missing"},
                }
            )
    return {
        "obj": row,
        "relationships": {"parent_of": parent_of, "child_of": child_of},
        "audit_rows": state.audit_rows,
        "external_refs": [],
        "editor": {
            "validator_ref": "UNIVERSAL_PASS@1",
            "assessment": {
                "state": "valid_current",
                "subject_mutated": False,
            },
        },
    }


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def browser():
    with sync_api.sync_playwright() as playwright:
        try:
            browser = playwright.chromium.launch(headless=True)
        except sync_api.Error as exc:
            pytest.skip(f"Playwright Chromium is not installed or launchable: {exc}")
        yield browser
        browser.close()


@pytest.fixture()
def gui_server(monkeypatch):
    import daylily_tapdb.gui.router as router_mod

    state = _GuiState()
    cfg = {
        "client_id": "browser",
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
        "domain_registry_path": "daylily_tapdb/etc/domain_code_registry.json",
        "prefix_ownership_registry_path": "daylily_tapdb/etc/prefix_ownership_registry.json",
    }

    monkeypatch.setattr(router_mod, "get_db_config", lambda config_path: cfg)
    monkeypatch.setattr(router_mod, "get_db", lambda config_path: _Conn(state))
    monkeypatch.setattr(
        router_mod,
        "search_objects",
        lambda session, service_name, q, record_type, category, type_name, subtype, limit: {
            "items": [
                _record_to_item(obj, kind)
                for obj, kind in [
                    *[(item, "template") for item in state.templates],
                    *[(item, "instance") for item in state.instances],
                    *[(item, "lineage") for item in state.lineages],
                ]
                if (record_type == "all" or kind == record_type)
                and (not q or q.lower() in f"{obj.euid} {obj.name}".lower())
                and (not category or obj.category.lower() == category.lower())
            ][:limit],
            "page": {"limit": limit, "total": 1, "next_cursor": None},
            "filters": {},
        },
    )
    monkeypatch.setattr(
        router_mod,
        "_object_detail_context",
        lambda session, euid: _object_context(state, euid),
    )
    monkeypatch.setattr(
        router_mod,
        "find_object_by_euid",
        lambda session, euid: state.lookup(euid),
    )
    monkeypatch.setattr(
        router_mod,
        "build_graph_payload",
        lambda obj, record_type, service_name, depth: {
            "elements": {
                "nodes": [
                    {"data": _record_to_item(item, "instance")}
                    for item in state.instances
                ],
                "edges": [
                    {
                        "data": {
                            "euid": item.euid,
                            "source": item.child_euid,
                            "target": item.parent_euid,
                            "relationship_type": item.relationship_type,
                        }
                    }
                    for item in state.lineages
                ],
            }
        },
    )

    def seed_templates(session, templates, **kwargs):
        del session, kwargs
        inserted = 0
        for item in templates:
            state.template_counter += 1
            state.templates.append(
                state.template(
                    f"Z-TPX-{state.template_counter}Q",
                    item["name"],
                    item["category"],
                    item["type"],
                    item["subtype"],
                    item["instance_prefix"],
                    json_addl=item.get("json_addl") or {},
                )
            )
            inserted += 1
        return SimpleNamespace(inserted=inserted, skipped=0)

    def create_instance(session, cfg, template_euid, name, properties, create_children):
        del session, cfg
        template, _ = state.lookup(template_euid)
        instance = state.instance(
            state.next_instance_euid(template.instance_prefix),
            name,
            template.category,
            template.type,
            template.subtype,
            json_addl={"properties": properties},
        )
        state.instances.append(instance)
        if (
            create_children
            and template.category == "container"
            and template.type == "plate"
        ):
            for index in range(1, 3):
                child = state.instance(
                    state.next_instance_euid("WEN"),
                    f"{name}_well_{index}",
                    "container",
                    "well",
                    "browser_well",
                )
                state.instances.append(child)
                state.lineage(instance.euid, child.euid, "contains")
        return {
            "template_euid": template_euid,
            "template_code": f"{template.category}/{template.type}/{template.subtype}/{template.version}/",
            "instance_euid": instance.euid,
            "create_children": create_children,
        }

    def create_repair(session, *, cfg, euid, actor, reason, repair_payload):
        del session, cfg, actor, reason
        repair = state.instance(
            state.next_instance_euid("GVR"),
            f"Repair record for {euid}",
            "evidence",
            "repair",
            "record",
            json_addl={
                "properties": {
                    "subject_euid": euid,
                    "repair_payload": repair_payload,
                    "subject_mutated": False,
                }
            },
        )
        state.instances.append(repair)
        return {
            "repair_euid": repair.euid,
            "subject_euid": euid,
            "subject_mutated": False,
            "template_code": "evidence/repair/record/1.0/",
            "properties": repair.json_addl["properties"],
        }

    def update_name(session, *, euid, name):
        del session
        obj, record_type = state.lookup(euid)
        obj.name = name
        return _record_to_item(obj, record_type)

    def update_status(session, *, euid, bstatus):
        del session
        obj, record_type = state.lookup(euid)
        obj.bstatus = bstatus
        return _record_to_item(obj, record_type)

    def add_lineage(
        session,
        *,
        euid,
        related_euid,
        direction,
        relationship_type,
        v0_edge=None,
    ):
        del v0_edge
        del session
        if direction == "child":
            row = state.lineage(euid, related_euid, relationship_type)
        else:
            row = state.lineage(related_euid, euid, relationship_type)
        return {"lineage_euid": row.euid, "relationship_type": relationship_type}

    def create_external_link(
        session,
        cfg,
        source_euid,
        system,
        foreign_uid,
        relationship_type,
        display_url,
        graph_base_url,
        graph_data_path,
        object_detail_path_template,
        auth_mode,
    ):
        del session, cfg, display_url, graph_base_url, graph_data_path
        del object_detail_path_template, auth_mode
        link = state.instance(
            state.next_instance_euid("XRF"),
            f"{system}:{foreign_uid}",
            "reference",
            "external_identifier",
            "tapdb_object",
            json_addl={
                "external_identifier": {"system": system, "foreign_uid": foreign_uid}
            },
        )
        state.instances.append(link)
        row = state.lineage(source_euid, link.euid, relationship_type)
        return {
            "source_euid": source_euid,
            "link_euid": link.euid,
            "lineage_euid": row.euid,
            "relationship_type": relationship_type,
        }

    monkeypatch.setattr(router_mod, "seed_templates", seed_templates)
    monkeypatch.setattr(router_mod, "_create_instance_from_template", create_instance)
    monkeypatch.setattr(router_mod, "_create_object_repair", create_repair)
    monkeypatch.setattr(router_mod, "_update_object_name", update_name)
    monkeypatch.setattr(router_mod, "_update_object_status", update_status)
    monkeypatch.setattr(router_mod, "_add_object_lineage", add_lineage)
    monkeypatch.setattr(router_mod, "_create_external_link", create_external_link)
    monkeypatch.setattr(
        router_mod,
        "_readiness_payload",
        lambda config_path: {
            "ready": True,
            "config_path": config_path,
            "client_id": "browser",
            "domain_code": "Z",
            "owner_repo_name": "daylily-tapdb",
            "public_domain_registry": {"repository": "local", "version": "test"},
            "checks": [{"name": "external_template", "ok": True, "detail": "seeded"}],
        },
    )
    monkeypatch.setattr(
        router_mod,
        "_meridian_validation_payload",
        lambda config_path, euid="", prefix="": {
            "config": cfg,
            "governance": SimpleNamespace(
                domain_code="Z",
                owner_repo_name="daylily-tapdb",
                domain_registry_path="domain.json",
                prefix_ownership_registry_path="prefix.json",
                public_domain_registry_repository="local",
                public_domain_registry_version="test",
            ),
            "euid": euid,
            "euid_valid": bool(euid.startswith("Z-")) if euid else None,
            "prefix": prefix,
            "prefix_owner": "daylily-tapdb" if prefix == "ACT" else "",
            "prefix_error": "" if not prefix or prefix == "ACT" else "unknown prefix",
        },
    )
    monkeypatch.setattr(
        router_mod,
        "build_metrics_page_context",
        lambda target, limit, config_path: {
            "metrics_enabled": True,
            "metrics_file": "memory",
            "metrics_message": "",
            "dropped_count": 0,
            "summary": {
                "by_path": [
                    {
                        "path": "/tapdb/search",
                        "method": "GET",
                        "count": 3,
                        "total_seconds": 0.1,
                    }
                ]
            },
        },
    )

    bridge = TapdbHostBridge(
        auth_mode="host_session",
        app_name="TapDB Browser Test",
        login_url="/tapdb/login",
        nav_links=(
            TapdbHostNavLink("Search", "/tapdb/search"),
            TapdbHostNavLink("Templates", "/tapdb/templates"),
        ),
        resolve_user=lambda request: {
            "username": "browser@example.com",
            "email": "browser@example.com",
            "role": request.cookies.get("tapdb_role", "admin"),
            "display_name": "Browser Tester",
            "is_active": True,
        },
    )
    host = FastAPI(title="TapDB Playwright Host")
    host.add_api_route("/", lambda: RedirectResponse("/tapdb/"), methods=["GET"])
    host.mount(
        "/tapdb",
        create_tapdb_gui_app(config_path="/tmp/tapdb.yaml", host_bridge=bridge),
    )

    port = _free_port()
    server = uvicorn.Server(
        uvicorn.Config(host, host="127.0.0.1", port=port, log_level="warning")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    deadline = time.time() + 10
    while not server.started and time.time() < deadline:
        time.sleep(0.05)
    if not server.started:
        raise RuntimeError("Playwright TapDB test server did not start")
    yield SimpleNamespace(base_url=f"http://127.0.0.1:{port}/tapdb", state=state)
    server.should_exit = True
    thread.join(timeout=5)


def _page(browser):
    page = browser.new_page()
    page.set_default_timeout(15000)
    return page


def _set_json_editor(page, selector, payload):
    page.evaluate(
        """([selector, payload]) => {
            const textarea = document.querySelector(selector);
            window.TapdbJsonEditor.setValue(textarea, JSON.stringify(payload, null, 2));
        }""",
        [selector, payload],
    )


def test_playwright_template_editor_keeps_focus_and_saves(browser, gui_server):
    page = _page(browser)
    page.goto(f"{gui_server.base_url}/templates/new", wait_until="networkidle")
    sync_api.expect(page.locator("[data-testid='tapdb-json-editor']")).to_be_visible()

    editor = page.locator("[data-testid='tapdb-json-editor'] textarea").first
    editor.click()
    editor.press("Meta+A")
    editor.type('{"templates": []}', delay=2)

    assert page.evaluate(
        "document.activeElement === document.querySelector(\"[data-testid='tapdb-json-editor'] textarea\")"
    )
    assert page.locator("#template-json").input_value() == '{"templates": []}'

    _set_json_editor(
        page,
        "#template-json",
        {
            "templates": [
                {
                    "name": "Browser Actor",
                    "polymorphic_discriminator": "generic_template",
                    "category": "actor",
                    "type": "person",
                    "subtype": "browser_actor_saved",
                    "version": "1.0",
                    "instance_prefix": "ACT",
                    "instance_polymorphic_identity": "generic_instance",
                    "json_addl": {"properties": {"display_name": ""}},
                }
            ]
        },
    )
    page.get_by_role("button", name="Validate").click()
    sync_api.expect(page.get_by_role("heading", name="Template Pack")).to_be_visible()
    page.get_by_role("button", name="Save").click()
    sync_api.expect(page.get_by_text("Saved 1 template(s); skipped 0.")).to_be_visible()
    page.close()


def test_playwright_create_detail_graph_and_object_mutations(browser, gui_server):
    page = _page(browser)
    page.goto(f"{gui_server.base_url}/search", wait_until="networkidle")
    page.get_by_placeholder("EUID, name, type, status").fill("Sample 1")
    page.get_by_role("button", name="Search").click()
    sync_api.expect(page.get_by_text("Z-SMP-1Q")).to_be_visible()

    page.goto(f"{gui_server.base_url}/templates", wait_until="networkidle")
    page.get_by_role("link", name="Create").nth(1).click()
    sync_api.expect(page.get_by_text("Plate Template")).to_be_visible()
    page.get_by_label("Name").fill("Browser Plate")
    _set_json_editor(page, "textarea[name='properties_json']", {"plate_type": "2-well"})
    page.get_by_role("button", name="Create").click()
    sync_api.expect(page.get_by_text("Instance created.")).to_be_visible()
    sync_api.expect(page.get_by_text("Browser Plate_well_1")).to_be_visible()
    plate_url = page.url

    page.get_by_role("link", name="Graph").click()
    sync_api.expect(page.get_by_text("Graph:")).to_be_visible()
    sync_api.expect(page.locator("[data-testid='tapdb-graph']")).to_be_visible()
    page.wait_for_selector("#tapdb-graph canvas", state="visible", timeout=10000)
    page.locator("summary", has_text="Nodes and edges").click()
    sync_api.expect(page.get_by_text("contains").first).to_be_visible()

    page.goto(plate_url, wait_until="networkidle")
    page.locator("form[action$='/name'] input[name='name']").fill(
        "Browser Plate Renamed"
    )
    page.get_by_role("button", name="Set Name").click()
    sync_api.expect(page.get_by_text("Name updated.")).to_be_visible()
    sync_api.expect(page.get_by_text("Browser Plate Renamed")).to_be_visible()

    page.locator("form[action$='/status'] input[name='bstatus']").fill("reviewed")
    page.get_by_role("button", name="Set Status").click()
    sync_api.expect(page.get_by_text("Status updated.")).to_be_visible()

    _set_json_editor(
        page,
        "textarea[name='repair_payload']",
        {"properties": {"edited": True, "plate_type": "2-well"}},
    )
    page.locator("form[action$='/repairs'] input[name='reason']").fill("browser edit")
    page.get_by_role("button", name="Create repair").click()
    sync_api.expect(page.get_by_text("Repair evidence created.")).to_be_visible()

    page.locator("form[action$='/lineage'] input[name='related_euid']").fill("Z-SMP-1Q")
    page.locator("form[action$='/lineage'] input[name='relationship_type']").fill(
        "derived_from"
    )
    page.get_by_role("button", name="Add Lineage").click()
    sync_api.expect(page.get_by_text("Lineage added.")).to_be_visible()
    sync_api.expect(page.get_by_text("derived_from")).to_be_visible()

    page.get_by_role("link", name="External Link").click()
    page.get_by_label("System").fill("dewey")
    page.get_by_label("Foreign UID").fill("M-BROWSER-1")
    page.get_by_label("Display URL").fill("https://dewey.example/M-BROWSER-1")
    page.get_by_role("button", name="Create Link").click()
    sync_api.expect(page.get_by_text("External link created.")).to_be_visible()
    sync_api.expect(page.get_by_text("dewey:M-BROWSER-1")).to_be_visible()
    page.close()


def test_playwright_admin_pages_and_admin_gates(browser, gui_server):
    page = _page(browser)
    page.goto(f"{gui_server.base_url}/admin/readiness", wait_until="networkidle")
    sync_api.expect(page.get_by_text("TapDB GUI ready: True")).to_be_visible()

    page.goto(f"{gui_server.base_url}/admin/meridian?euid=Z-SMP-1Q&prefix=ACT")
    sync_api.expect(page.get_by_text("Prefix owner: daylily-tapdb")).to_be_visible()
    sync_api.expect(page.get_by_text("EUID valid for Z: True")).to_be_visible()

    page.goto(f"{gui_server.base_url}/admin/metrics", wait_until="networkidle")
    sync_api.expect(page.get_by_text("DB Metrics")).to_be_visible()
    sync_api.expect(page.get_by_text("/tapdb/search")).to_be_visible()

    page.context.add_cookies(
        [
            {
                "name": "tapdb_role",
                "value": "user",
                "url": gui_server.base_url,
            }
        ]
    )
    page.goto(f"{gui_server.base_url}/admin/metrics", wait_until="networkidle")
    sync_api.expect(page.get_by_text("tapdb_gui_admin_required")).to_be_visible()
    page.goto(f"{gui_server.base_url}/templates/new", wait_until="networkidle")
    sync_api.expect(page.get_by_text("tapdb_gui_admin_required")).to_be_visible()
    page.close()
