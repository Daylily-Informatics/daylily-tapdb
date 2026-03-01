"""Smoke coverage for admin FastAPI routes.

These tests intentionally use lightweight fakes to execute each route handler
at least once without requiring a real database or Cognito runtime.
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import admin.auth as auth_mod
import admin.main as admin_main


class _FakeTemplateRender:
    def __init__(self, name: str):
        self.name = name

    def render(self, **_kwargs):
        return f"TEMPLATE:{self.name}"


class _FakeRelatedQuery:
    def __init__(self, items):
        self._items = list(items)

    def filter_by(self, **kwargs):
        items = [
            it
            for it in self._items
            if all(getattr(it, k, None) == v for k, v in kwargs.items())
        ]
        return _FakeRelatedQuery(items)

    def all(self):
        return list(self._items)

    def __iter__(self):
        return iter(self._items)


class _FakeQuery:
    def __init__(self, items):
        self._base = list(items)
        self._filtered = list(items)
        self._offset = 0
        self._limit = None

    def filter_by(self, **kwargs):
        self._filtered = [
            it
            for it in self._filtered
            if all(getattr(it, k, None) == v for k, v in kwargs.items())
        ]
        return self

    def order_by(self, *_args, **_kwargs):
        return self

    def distinct(self):
        seen = []
        uniq = []
        for row in self._filtered:
            if row not in seen:
                seen.append(row)
                uniq.append(row)
        self._filtered = uniq
        return self

    def count(self):
        return len(self._filtered)

    def offset(self, value: int):
        self._offset = value
        return self

    def limit(self, value: int):
        self._limit = value
        return self

    def _slice(self):
        rows = self._filtered[self._offset :]
        if self._limit is not None:
            rows = rows[: self._limit]
        return rows

    def all(self):
        return list(self._slice())

    def first(self):
        rows = self._slice()
        return rows[0] if rows else None


class _FakeSession:
    def __init__(self, state):
        self._state = state

    def query(self, model):
        if model is admin_main.generic_template:
            return _FakeQuery(self._state["templates"])
        if model is admin_main.generic_instance:
            return _FakeQuery(self._state["instances"])
        if model is admin_main.generic_instance_lineage:
            return _FakeQuery(self._state["lineages"])
        if model is admin_main.generic_template.category:
            rows = [(t.category,) for t in self._state["templates"]]
            return _FakeQuery(rows)
        if model is admin_main.generic_instance.category:
            rows = [(i.category,) for i in self._state["instances"]]
            return _FakeQuery(rows)
        raise AssertionError(f"Unexpected query model: {model!r}")

    def add(self, obj):
        if getattr(obj, "euid", None) is None:
            obj.euid = f"GN{len(self._state['lineages']) + 200}"
        if getattr(obj, "uuid", None) is None:
            obj.uuid = len(self._state["lineages"]) + 200
        obj.is_deleted = False
        obj.created_dt = datetime.now(timezone.utc)
        parent = next(
            (i for i in self._state["instances"] if i.uuid == obj.parent_instance_uuid),
            None,
        )
        child = next(
            (i for i in self._state["instances"] if i.uuid == obj.child_instance_uuid),
            None,
        )
        obj.parent_instance = parent
        obj.child_instance = child
        if parent:
            parent.parent_of_lineages = _FakeRelatedQuery(
                list(parent.parent_of_lineages.all()) + [obj]
            )
        if child:
            child.child_of_lineages = _FakeRelatedQuery(
                list(child.child_of_lineages.all()) + [obj]
            )
        self._state["lineages"].append(obj)

    def flush(self):
        return None


class _FakeConn:
    def __init__(self, state):
        self._state = state
        self.app_username = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    @contextmanager
    def session_scope(self, commit=False):
        _ = commit
        yield _FakeSession(self._state)


@pytest.fixture
def route_client(monkeypatch: pytest.MonkeyPatch):
    now = datetime.now(timezone.utc)
    parent = SimpleNamespace(
        uuid=11,
        euid="GX11",
        name="Parent",
        category="generic",
        type="generic",
        subtype="generic",
        version="1.0",
        bstatus="active",
        json_addl={},
        created_dt=now,
        is_deleted=False,
        polymorphic_discriminator="generic_instance",
        parent_of_lineages=_FakeRelatedQuery([]),
        child_of_lineages=_FakeRelatedQuery([]),
    )
    child = SimpleNamespace(
        uuid=12,
        euid="GX12",
        name="Child",
        category="generic",
        type="generic",
        subtype="generic",
        version="1.0",
        bstatus="active",
        json_addl={},
        created_dt=now,
        is_deleted=False,
        polymorphic_discriminator="generic_instance",
        parent_of_lineages=_FakeRelatedQuery([]),
        child_of_lineages=_FakeRelatedQuery([]),
    )
    lineage = SimpleNamespace(
        uuid=21,
        euid="GN21",
        name="Parent->Child:contains",
        category="lineage",
        type="lineage",
        subtype="generic",
        version="1.0",
        bstatus="active",
        json_addl={},
        created_dt=now,
        is_deleted=False,
        relationship_type="contains",
        parent_instance_uuid=parent.uuid,
        child_instance_uuid=child.uuid,
        parent_type="generic_instance",
        child_type="generic_instance",
        parent_instance=parent,
        child_instance=child,
    )
    parent.parent_of_lineages = _FakeRelatedQuery([lineage])
    child.child_of_lineages = _FakeRelatedQuery([lineage])

    template = SimpleNamespace(
        uuid=1,
        euid="GT1",
        name="Generic Template",
        category="generic",
        type="generic",
        subtype="generic",
        version="1.0",
        bstatus="active",
        json_addl={"properties": {"x": 1}},
        created_dt=now,
        is_deleted=False,
    )

    state = {
        "templates": [template],
        "instances": [parent, child],
        "lineages": [lineage],
    }

    monkeypatch.setattr(
        admin_main.templates,
        "get_template",
        lambda name: _FakeTemplateRender(name),
    )
    monkeypatch.setattr(admin_main, "get_style", lambda: {"skin_css": "x.css"})
    monkeypatch.setattr(admin_main, "get_db", lambda: _FakeConn(state))
    monkeypatch.setattr(admin_main, "get_user_permissions", lambda _u: {"ok": True})
    monkeypatch.setattr(admin_main, "get_user_by_username", lambda _u: None)
    monkeypatch.setattr(admin_main, "update_last_login", lambda _u: None)
    monkeypatch.setattr(admin_main, "authenticate_with_cognito", lambda *_: {"access_token": "tok"})
    monkeypatch.setattr(admin_main, "create_cognito_user_account", lambda *_a, **_k: None)
    monkeypatch.setattr(
        admin_main,
        "get_or_create_user_from_email",
        lambda email, **_k: {
            "uuid": 777,
            "username": email,
            "email": email,
            "role": "user",
            "require_password_change": False,
        },
    )
    monkeypatch.setattr(admin_main, "respond_to_new_password_challenge", lambda *_: {"access_token": "tok"})
    monkeypatch.setattr(admin_main, "change_cognito_password", lambda *_: None)

    return TestClient(admin_main.app), state


def _admin_user():
    return {
        "uuid": 1,
        "username": "admin",
        "email": "admin@example.com",
        "role": "admin",
        "require_password_change": False,
    }


def test_auth_and_public_routes(route_client, monkeypatch: pytest.MonkeyPatch):
    client, _state = route_client

    # Login page (not logged in)
    async def _anon_main(_request):
        return None

    monkeypatch.setattr(admin_main, "get_current_user", _anon_main)
    resp = client.get("/login")
    assert resp.status_code == 200

    # Login submit error path
    monkeypatch.setattr(admin_main, "authenticate_with_cognito", lambda *_: (_ for _ in ()).throw(ValueError("bad")))
    resp = client.post("/login", data={"username": "u", "password": "p"})
    assert resp.status_code == 200
    monkeypatch.setattr(admin_main, "authenticate_with_cognito", lambda *_: {"access_token": "tok"})

    # Signup page + early validation path
    resp = client.get("/signup")
    assert resp.status_code == 200
    resp = client.post(
        "/signup",
        data={
            "email": "bad-email",
            "display_name": "X",
            "password": "Password1!",
            "confirm_password": "Password1!",
        },
    )
    assert resp.status_code == 200

    # Logout and change-password unauthenticated path
    resp = client.get("/logout", follow_redirects=False)
    assert resp.status_code == 302
    resp = client.get("/change-password", follow_redirects=False)
    assert resp.status_code == 302
    resp = client.post(
        "/change-password",
        data={"current_password": "x", "new_password": "Password1!", "confirm_password": "Password1!"},
        follow_redirects=False,
    )
    assert resp.status_code == 302


def test_protected_html_and_api_routes(route_client, monkeypatch: pytest.MonkeyPatch):
    client, state = route_client

    async def _admin_auth_user(_request):
        return _admin_user()

    monkeypatch.setattr(auth_mod, "get_current_user", _admin_auth_user)

    # Protected HTML routes
    assert client.get("/").status_code == 200
    assert client.get("/templates").status_code == 200
    assert client.get("/instances").status_code == 200
    assert client.get("/lineages").status_code == 200
    assert client.get("/object/GX12").status_code == 200
    assert client.get("/graph").status_code == 200
    assert client.get("/create-instance/GT1").status_code == 200
    assert (
        client.post(
            "/create-instance/GT1",
            data={
                "instance_name": "",
                "create_children": "false",
                "parent_euids": "",
                "child_euids": "",
                "relationship_type": "contains",
            },
        ).status_code
        == 200
    )

    # API routes (public)
    assert client.get("/api/graph/data").status_code == 200
    assert client.get("/api/templates").status_code == 200
    assert client.get("/api/instances").status_code == 200
    assert client.get("/api/object/GT1").status_code == 200

    # API routes (admin protected)
    # Cover handler logic with missing required keys.
    resp = client.post("/api/lineage", json={})
    assert resp.status_code == 400

    # Soft-delete an existing object.
    resp = client.delete("/api/object/GT1")
    assert resp.status_code == 200
    assert state["templates"][0].is_deleted is True
