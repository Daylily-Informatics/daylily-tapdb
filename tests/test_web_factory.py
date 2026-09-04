from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from daylily_tapdb.web.bridge import TapdbHostBridge, TapdbHostBridgeMount


def test_host_bridge_mount_redirects_html_and_blocks_api_without_user() -> None:
    downstream = FastAPI()

    @downstream.get("/")
    async def home(request: Request) -> JSONResponse:
        return JSONResponse({"user": request.scope.get("tapdb_host_user")})

    @downstream.get("/api/dag/v2/object/GX1")
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

    api_response = client.get("/api/dag/v2/object/GX1", follow_redirects=False)
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
