from __future__ import annotations

import asyncio
from contextlib import contextmanager
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from fastapi import HTTPException, Request
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from daylily_tapdb.gui import router
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.web.bridge import TapdbHostBridge


class _Related:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def filter_by(self, **values):
        return _Related(
            row
            for row in self.rows
            if all(getattr(row, key, None) == value for key, value in values.items())
        )

    def all(self):
        return list(self.rows)


class _Query:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def filter_by(self, **values):
        return _Query(
            row
            for row in self.rows
            if all(getattr(row, key, None) == value for key, value in values.items())
        )

    def filter(self, *_args, **_kwargs):
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def limit(self, value):
        return _Query(self.rows[: int(value)])

    def first(self):
        return self.rows[0] if self.rows else None

    def all(self):
        return list(self.rows)


class _Session:
    def __init__(self, rows=None):
        self.rows = rows or {}
        self.added = []
        self.flush_error = None

    def query(self, model):
        return _Query(self.rows.get(model, []))

    def add(self, value):
        self.added.append(value)

    def flush(self):
        if self.flush_error is not None:
            raise self.flush_error


class _Conn:
    def __init__(self, session):
        self.session = session
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def session_scope(self, commit=False):
        self.commit = commit
        yield self.session


def _instance(uid=1, euid="persisted-object", *, category="content"):
    obj = SimpleNamespace(
        uid=uid,
        euid=euid,
        name="Object",
        category=category,
        type="specimen",
        subtype="sample",
        version="1.0",
        bstatus="active",
        json_addl={"properties": {}},
        is_deleted=False,
        polymorphic_discriminator="generic_instance",
        parent_of_lineages=_Related(),
        child_of_lineages=_Related(),
        created_dt=None,
        modified_dt=None,
    )
    return obj


def _template(euid="persisted-template"):
    return SimpleNamespace(
        uid=10,
        euid=euid,
        name="Template",
        category="reference",
        type="external_identifier",
        subtype="tapdb_object",
        version="1.0",
        instance_prefix="XRF",
        validator_ref=None,
        bstatus="active",
        json_addl={"properties": {}},
        is_deleted=False,
        polymorphic_discriminator="generic_template",
        instance_polymorphic_identity="generic_instance",
        created_dt=None,
        modified_dt=None,
    )


def _request(*, body=b"", content_type="", root_path="/tapdb", host_user=None):
    headers = []
    if content_type:
        headers.append((b"content-type", content_type.encode()))
    scope = {
        "type": "http",
        "method": "POST",
        "path": "/",
        "root_path": root_path,
        "headers": headers,
        "state": {},
    }
    if host_user is not None:
        scope["tapdb_host_user"] = host_user
    sent = False

    async def receive():
        nonlocal sent
        if sent:
            return {"type": "http.request", "body": b"", "more_body": False}
        sent = True
        return {"type": "http.request", "body": body, "more_body": False}

    return Request(scope, receive)


def test_branch_campaign_template_loader_and_navigation_filter_invalid_entries(
    tmp_path,
):
    existing = tmp_path / "templates"
    existing.mkdir()
    bridge = TapdbHostBridge(
        template_override_dirs=(str(existing), str(tmp_path / "missing")),
    )
    environment = router._build_templates(bridge)
    assert environment.loader is not None

    request = _request(root_path="/tapdb/")
    assert router.gui_url_with_query(request, "/search", q="") == "/tapdb/search"
    links = router.gui_nav_links(
        request,
        {
            "nav_links": [
                {"label": "", "href": "/invalid"},
                {"label": "Search", "href": "/elsewhere"},
                {"label": "Other", "href": "/tapdb/search/"},
            ]
        },
    )
    assert links[0] == {"label": "Search", "href": "/elsewhere"}
    assert sum(item["href"].rstrip("/") == "/tapdb/search" for item in links) == 1


def test_branch_campaign_gui_user_fallback_and_admin_guards(monkeypatch):
    from admin import auth

    async def _missing(_request):
        return None

    monkeypatch.setattr(auth, "get_current_user", _missing)
    with pytest.raises(HTTPException) as exc_info:
        asyncio.run(router.require_tapdb_gui_user(_request()))
    assert exc_info.value.status_code == 401

    async def _found(_request):
        return {"username": "operator", "role": "admin"}

    monkeypatch.setattr(auth, "get_current_user", _found)
    request = _request()
    assert asyncio.run(router.require_tapdb_gui_user(request))["username"] == "operator"
    assert request.state.user["role"] == "admin"
    with pytest.raises(HTTPException) as admin_error:
        asyncio.run(
            router.require_tapdb_gui_admin({"username": "reader", "role": "user"})
        )
    assert admin_error.value.status_code == 403


def test_branch_campaign_relationship_and_external_link_projection(monkeypatch):
    parent = _instance(1, "persisted-parent")
    child = _instance(2, "persisted-child", category="external_identifier")
    lineage = SimpleNamespace(
        euid="persisted-lineage",
        relationship_type="contains",
        parent_instance=parent,
        child_instance=child,
        is_deleted=False,
    )
    parent.parent_of_lineages.rows.append(lineage)
    child.child_of_lineages.rows.append(lineage)
    monkeypatch.setattr(
        router, "describe_lineage_contract", lambda _row: {"edge": "ok"}
    )
    relationships = router._object_relationships(parent, "instance")
    assert relationships["parent_of"][0]["related_euid"] == "persisted-child"
    assert router._object_relationships(parent, "template") == {
        "parent_of": [],
        "child_of": [],
    }
    assert (
        router._object_relationships(child, "instance")["child_of"][0]["related_euid"]
        == "persisted-parent"
    )

    parent.parent_of_lineages.rows.insert(
        0, SimpleNamespace(child_instance=None, is_deleted=False)
    )
    monkeypatch.setattr(
        router,
        "external_ref_payloads",
        lambda _obj: [{"system": "remote", "root_euid": "remote-object"}],
    )
    projected = router._lineage_external_refs(parent, "instance")
    assert projected[0]["lineage_euid"] == "persisted-lineage"
    assert router._lineage_external_refs(parent, "template") == []


def test_branch_campaign_object_and_json_input_guards(monkeypatch):
    monkeypatch.setattr(router, "find_object_by_euid", lambda *_args: (None, None))
    with pytest.raises(HTTPException) as missing:
        router._object_detail_context(object(), "missing-object")
    assert missing.value.status_code == 404

    with pytest.raises(HTTPException, match="invalid JSON"):
        router._parse_json_object("{", label="payload")
    with pytest.raises(HTTPException, match="JSON object"):
        router._parse_json_object("[]", label="payload")
    assert router._parse_evidence_refs("") == []
    assert router._parse_evidence_refs("one,two\nthree") == [
        {"euid": "one"},
        {"euid": "two"},
        {"euid": "three"},
    ]
    assert router._parse_evidence_refs('[{"euid":"evidence"}, "other"]') == [
        {"euid": "evidence"},
        {"euid": "other"},
    ]
    with pytest.raises(HTTPException, match="must be JSON list"):
        router._parse_evidence_refs('{"euid":"bad"}')


def test_branch_campaign_request_body_readers_reject_wrong_shapes():
    with pytest.raises(HTTPException) as wrong_media:
        asyncio.run(
            router._read_urlencoded_form(
                _request(body=b"name=value", content_type="application/json")
            )
        )
    assert wrong_media.value.status_code == 415
    assert asyncio.run(router._read_optional_json_object(_request(body=b"  "))) == {}
    with pytest.raises(HTTPException, match="invalid"):
        asyncio.run(router._read_optional_json_object(_request(body=b"{")))
    with pytest.raises(HTTPException, match="must be an object"):
        asyncio.run(router._read_optional_json_object(_request(body=b"[]")))


def test_branch_campaign_template_builder_and_validator_cover_malformed_layouts(
    monkeypatch,
):
    assert router._template_properties_form_json({"json_addl": None}) == "{}"
    assert (
        router._template_properties_form_json({"json_addl": {"properties": []}}) == "{}"
    )
    assert router._builder_value(None) == ""
    assert router._builder_value("plain") == "plain"

    seed = router._builder_seed_from_template(
        {
            "name": "Template",
            "json_addl": {
                "properties": {"count": 2},
                "instantiation_layouts": [
                    "bad",
                    {"child_templates": "bad"},
                    {
                        "child_templates": [
                            "bad",
                            {"template_code": "child", "count": 0},
                        ]
                    },
                ],
            },
        },
        seed_euid="persisted-template",
    )
    assert seed["properties"] == [{"key": "count", "value": "2"}]
    assert seed["children"][0]["count"] == 1
    assert router._template_editor_context({}, use_default_builder=False)[
        "builder_seed"
    ]["name"]

    payload = {"templates": ["bad", {}, {}, {"json_addl": []}]}
    issues = router._validate_template_payload(payload)
    messages = [item.message for item in issues]
    assert any("must be a JSON object" in item for item in messages)
    assert any("missing required field" in item for item in messages)
    assert any("duplicate template key" in item for item in messages)
    assert any("json_addl must be" in item for item in messages)

    monkeypatch.setattr(
        router,
        "validate_instantiation_layouts",
        lambda _value: (_ for _ in ()).throw(
            router.ValidationError.from_exception_data("Layout", [])
        ),
    )
    invalid_layout = router._validate_template_payload(
        {
            "templates": [
                {
                    "name": "Template",
                    "polymorphic_discriminator": "generic_template",
                    "category": "content",
                    "type": "specimen",
                    "subtype": "sample",
                    "version": "1.0",
                    "instance_prefix": "SMP",
                    "json_addl": {"instantiation_layouts": [{}]},
                }
            ]
        }
    )
    assert invalid_layout[0].message.startswith("Invalid instantiation_layouts")


def test_branch_campaign_create_helpers_reject_missing_objects(monkeypatch):
    session = _Session({generic_instance: [], generic_template: []})
    with pytest.raises(HTTPException) as missing_instance:
        router._resolve_instance(session, "missing", label="Object")
    assert missing_instance.value.status_code == 404
    with pytest.raises(HTTPException, match="Missing required external link"):
        router._create_external_link(
            session,
            cfg={"domain_code": "A"},
            source_euid="missing",
            system="",
            foreign_uid="",
            relationship_type="",
        )

    source = _instance()
    session.rows[generic_instance] = [source]
    with pytest.raises(HTTPException, match="No reference"):
        router._create_external_link(
            session,
            cfg={"domain_code": "A"},
            source_euid=source.euid,
            system="remote",
            foreign_uid="remote-object",
            relationship_type="references",
        )
    with pytest.raises(HTTPException, match="Template not found"):
        router._create_instance_from_template(
            session,
            cfg={"domain_code": "A"},
            template_euid="missing-template",
            name="Object",
            properties={},
            create_children=False,
        )


@pytest.mark.parametrize(
    ("error", "status"),
    [
        (LookupError("Object not found: missing"), 404),
        (LookupError("repair template missing"), 422),
        (ValueError("bad repair"), 400),
    ],
)
def test_branch_campaign_repair_errors_translate_to_http(monkeypatch, error, status):
    monkeypatch.setattr(
        router, "repair_object", lambda *_a, **_k: (_ for _ in ()).throw(error)
    )
    with pytest.raises(HTTPException) as exc_info:
        router._create_object_repair(
            object(),
            cfg={},
            euid="persisted-object",
            actor="operator",
            reason="repair",
            repair_payload={},
        )
    assert exc_info.value.status_code == status


@pytest.mark.parametrize(
    ("helper", "field", "value", "record", "status"),
    [
        (router._update_object_name, "name", "", (_instance(), "instance"), 400),
        (router._update_object_name, "name", "updated", (None, None), 404),
        (router._update_object_name, "name", "updated", (_template(), "template"), 403),
        (router._update_object_status, "bstatus", "", (_instance(), "instance"), 400),
        (router._update_object_status, "bstatus", "ready", (None, None), 404),
        (
            router._update_object_status,
            "bstatus",
            "ready",
            (_template(), "template"),
            403,
        ),
    ],
)
def test_branch_campaign_simple_object_updates_fail_closed(
    monkeypatch, helper, field, value, record, status
):
    monkeypatch.setattr(router, "find_object_by_euid", lambda *_args: record)
    with pytest.raises(HTTPException) as exc_info:
        helper(object(), euid="persisted-object", **{field: value})
    assert exc_info.value.status_code == status


def test_branch_campaign_lineage_direction_and_integrity_error(monkeypatch):
    current = _instance(1, "persisted-current")
    related = _instance(2, "persisted-related")
    monkeypatch.setattr(
        router,
        "_resolve_instance",
        lambda _session, euid, **_kwargs: current if euid == current.euid else related,
    )
    monkeypatch.setattr(router, "is_strict_canonical_edge_type", lambda _value: False)
    session = _Session()
    session.flush_error = IntegrityError("insert", {}, RuntimeError("duplicate"))
    with pytest.raises(HTTPException) as exc_info:
        router._add_object_lineage(
            session,
            euid=current.euid,
            related_euid=related.euid,
            direction="child",
            relationship_type="related",
        )
    assert exc_info.value.status_code == 409
    lineage = session.added[0]
    assert lineage.parent_instance_uid == current.uid
    assert lineage.child_instance_uid == related.uid


def test_branch_campaign_meridian_payload_reports_prefix_error(monkeypatch):
    governance = SimpleNamespace(
        domain_code="A",
        public_domain_registry_repository="registry",
        public_domain_registry_version="1",
        public_domain_registry_index_url="https://registry.example/index.json",
        require_prefix=lambda _prefix: (_ for _ in ()).throw(
            ValueError("unowned prefix")
        ),
    )
    monkeypatch.setattr(
        router,
        "get_db_config",
        lambda **_kwargs: {
            "domain_code": "A",
            "owner_repo_name": "owner",
            "domain_registry_path": "/registry.json",
            "prefix_ownership_registry_path": "/prefixes.json",
        },
    )
    monkeypatch.setattr(router.GovernanceContext, "load", lambda **_kwargs: governance)
    monkeypatch.setattr(router, "validate_euid", lambda *_args, **_kwargs: True)
    payload = router._meridian_validation_payload(
        config_path="/config.yaml",
        euid="persisted-object",
        prefix="ABC",
    )
    assert payload["euid_valid"] is True
    assert payload["prefix_error"] == "unowned prefix"


@dataclass
class _ImportResult:
    imported: int = 1


def _client(monkeypatch, session):
    conn = _Conn(session)
    monkeypatch.setattr(router, "get_db", lambda _path: conn)
    monkeypatch.setattr(
        router,
        "get_db_config",
        lambda **_kwargs: {
            "client_id": "client",
            "domain_code": "A",
            "owner_repo_name": "owner-a",
            "domain_registry_path": "/registry.json",
            "prefix_ownership_registry_path": "/prefixes.json",
        },
    )
    monkeypatch.setattr(
        router,
        "search_objects",
        lambda *_a, **kwargs: {"items": [], "page": {"limit": kwargs["limit"]}},
    )
    bridge = TapdbHostBridge(
        auth_mode="host_session",
        login_url="/login",
        resolve_user=lambda _request: {
            "username": "operator",
            "email": "operator@example.test",
            "role": "admin",
        },
    )
    return TestClient(
        router.create_tapdb_gui_app(config_path="/config.yaml", host_bridge=bridge),
        base_url="https://localhost",
    )


def test_branch_campaign_gui_repository_routes_translate_results_and_errors(
    monkeypatch,
):
    session = _Session({generic_template: [_template()]})
    client = _client(monkeypatch, session)
    assert client.get("/api/search", params={"limit": 2}).status_code == 200

    monkeypatch.setattr(
        router,
        "repository_inventory",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("inventory failed")),
    )
    page = client.get(
        "/templates", params={"category": "reference", "repository_pack": "pack.json"}
    )
    assert page.status_code == 200
    status = client.get(
        "/api/templates/repository/status", params={"repository_pack": "pack.json"}
    )
    assert status.status_code == 400

    monkeypatch.setattr(
        router, "export_repository_pack", lambda *_a, **_k: {"exported": 1}
    )
    exported = client.post(
        "/templates/repository/export",
        content="repository_pack=pack.json",
        headers={"content-type": "application/x-www-form-urlencoded"},
        follow_redirects=False,
    )
    assert exported.status_code == 303
    monkeypatch.setattr(
        router,
        "export_repository_pack",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("export failed")),
    )
    failed_page = client.post(
        "/templates/repository/export",
        content="repository_pack=pack.json",
        headers={"content-type": "application/x-www-form-urlencoded"},
        follow_redirects=False,
    )
    assert "repository_error=export+failed" in failed_page.headers["location"]

    monkeypatch.setattr(
        router,
        "export_repository_pack",
        lambda *_a, **_k: (_ for _ in ()).throw(FileExistsError("exists")),
    )
    assert (
        client.post(
            "/api/templates/repository/export", json={"repository_pack": "pack.json"}
        ).status_code
        == 409
    )
    monkeypatch.setattr(
        router,
        "export_repository_pack",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("bad export")),
    )
    assert (
        client.post(
            "/api/templates/repository/export", json={"repository_pack": "pack.json"}
        ).status_code
        == 400
    )

    monkeypatch.setattr(
        router, "import_repository_pack", lambda *_a, **_k: _ImportResult()
    )
    imported = client.post(
        "/api/templates/repository/import",
        json={"repository_pack": "pack.json", "apply": True},
    )
    assert imported.json() == {"imported": 1}
    monkeypatch.setattr(
        router,
        "import_repository_pack",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("bad import")),
    )
    assert (
        client.post(
            "/api/templates/repository/import", json={"repository_pack": "pack.json"}
        ).status_code
        == 400
    )


def test_branch_campaign_gui_template_validation_routes_reject_bad_json(monkeypatch):
    client = _client(monkeypatch, _Session({generic_template: [_template()]}))
    invalid = client.post(
        "/api/templates/validate",
        content="{",
        headers={"content-type": "application/json"},
    )
    assert invalid.status_code == 400
    wrong_shape = client.post("/api/templates/validate", json=[])
    assert wrong_shape.status_code == 400


def test_branch_campaign_gui_app_requires_config_and_wraps_host_session():
    with pytest.raises(ValueError, match="config_path is required"):
        router.create_tapdb_gui_router(config_path="")
    bridge = TapdbHostBridge(
        auth_mode="host_session", resolve_user=lambda _request: None
    )
    app = router.create_tapdb_gui_app(config_path="/config.yaml", host_bridge=bridge)
    assert app.__class__.__name__ == "TapdbHostBridgeMount"
