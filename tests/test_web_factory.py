from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from jinja2 import Environment, FileSystemLoader

from daylily_tapdb.web.bridge import TapdbHostBridge, TapdbHostNavLink
from daylily_tapdb.web.factory import (
    TapdbHostBridgeMount,
    _configure_template_environment,
)


def test_configure_template_environment_prefers_host_overrides(tmp_path: Path) -> None:
    default_dir = tmp_path / "default"
    override_dir = tmp_path / "override"
    default_dir.mkdir()
    override_dir.mkdir()
    (default_dir / "hello.html").write_text("default", encoding="utf-8")
    (override_dir / "hello.html").write_text("override", encoding="utf-8")

    admin_main = type(
        "AdminMain",
        (),
        {
            "TEMPLATES_DIR": default_dir,
            "templates": Environment(loader=FileSystemLoader(str(default_dir))),
        },
    )()
    bridge = TapdbHostBridge(
        auth_mode="host_session",
        app_name="Dewey",
        shell_title="Dewey Console",
        nav_links=(TapdbHostNavLink(label="Dashboard", href="/ui"),),
        extra_stylesheets=("/static/console.css",),
        template_override_dirs=(str(override_dir),),
        extra_context=lambda _request: {"deployment": {"name": "local"}},
    )

    _configure_template_environment(admin_main, bridge)

    app = FastAPI()
    app.state.tapdb_host_bridge = bridge

    @app.get("/render")
    async def render(request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "html": admin_main.templates.get_template("hello.html").render(),
                "shell": admin_main.templates.globals["tapdb_host_shell"](request),
                "context": admin_main.templates.globals["tapdb_host_context"](request),
            }
        )

    client = TestClient(app)
    response = client.get("/render")
    assert response.status_code == 200
    body = response.json()
    assert body["html"] == "override"
    assert body["shell"]["app_name"] == "Dewey"
    assert body["shell"]["extra_stylesheets"] == ["/static/console.css"]
    assert body["context"]["deployment"]["name"] == "local"


def test_host_bridge_mount_redirects_html_and_blocks_api_without_user() -> None:
    downstream = FastAPI()

    @downstream.get("/")
    async def home(request: Request) -> JSONResponse:
        return JSONResponse({"user": request.scope.get("tapdb_host_user")})

    @downstream.get("/api/dag/object/GX1")
    async def api_detail(request: Request) -> JSONResponse:
        return JSONResponse({"user": request.scope.get("tapdb_host_user")})

    bridge = TapdbHostBridge(
        auth_mode="host_session",
        login_url="/login?next=/tapdb",
        resolve_user=lambda _request: None,
    )
    client = TestClient(
        TapdbHostBridgeMount(downstream, bridge), base_url="https://localhost"
    )

    html_response = client.get("/", follow_redirects=False)
    assert html_response.status_code == 302
    assert html_response.headers["location"] == "/login?next=/tapdb"

    api_response = client.get("/api/dag/object/GX1", follow_redirects=False)
    assert api_response.status_code == 401
    assert api_response.json()["detail"] == "host_session_required"


def test_host_bridge_mount_injects_user_into_scope() -> None:
    downstream = FastAPI()

    @downstream.get("/")
    async def home(request: Request) -> JSONResponse:
        return JSONResponse(
            {
                "user": request.scope.get("tapdb_host_user"),
                "requested_path": request.scope.get("tapdb_requested_path"),
            }
        )

    bridge = TapdbHostBridge(
        auth_mode="host_session",
        resolve_user=lambda _request: {
            "email": "operator@example.com",
            "sub": "sub-1",
            "role": "admin",
        },
    )
    client = TestClient(
        TapdbHostBridgeMount(downstream, bridge), base_url="https://localhost"
    )

    response = client.get("/?foo=bar")
    assert response.status_code == 200
    assert response.json() == {
        "user": {
            "uid": "operator@example.com",
            "username": "operator@example.com",
            "email": "operator@example.com",
            "display_name": "operator@example.com",
            "role": "admin",
            "is_active": True,
            "require_password_change": False,
        },
        "requested_path": "/?foo=bar",
    }
