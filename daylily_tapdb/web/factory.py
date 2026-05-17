"""Reusable TapDB web factory and host-bridge mount support."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import Depends, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from jinja2 import ChoiceLoader, FileSystemLoader

from daylily_tapdb.web.bridge import (
    TapdbHostBridge,
    normalize_host_user,
    resolve_bridge_url,
    resolve_host_context,
    resolve_host_shell,
)
from daylily_tapdb.web.dag import create_tapdb_dag_router


async def require_tapdb_api_user(request: Request) -> dict[str, Any]:
    """Require a TapDB-authenticated browser or host-injected user for API access."""

    from admin.auth import get_current_user

    user = await get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="tapdb_auth_required")
    request.state.user = user
    return user


def _configure_template_environment(admin_main, bridge: TapdbHostBridge | None) -> None:
    override_dirs = []
    if bridge is not None:
        override_dirs = [
            str(Path(item).expanduser().resolve())
            for item in bridge.template_override_dirs
            if str(item).strip() and Path(item).expanduser().exists()
        ]

    loaders = [FileSystemLoader(path) for path in override_dirs]
    loaders.append(FileSystemLoader(str(admin_main.TEMPLATES_DIR)))
    admin_main.templates.loader = ChoiceLoader(loaders)
    admin_main.templates.globals["tapdb_host_shell"] = lambda request: (
        resolve_host_shell(bridge, request)
    )
    admin_main.templates.globals["tapdb_host_context"] = lambda request: (
        resolve_host_context(bridge, request)
    )


def _attach_canonical_dag_router(
    app,
    *,
    config_path: str,
    service_name: str | None,
) -> None:
    if getattr(app.state, "tapdb_dag_router_attached", False):
        return
    router = create_tapdb_dag_router(
        config_path=config_path,
        service_name=service_name,
    )
    app.include_router(router, dependencies=[Depends(require_tapdb_api_user)])
    app.state.tapdb_dag_router_attached = True


def _requested_path(request: Request) -> str:
    root_path = str(request.scope.get("root_path") or "").rstrip("/")
    path = str(request.scope.get("path") or "")
    target = f"{root_path}{path}" or "/"
    query_string = request.scope.get("query_string") or b""
    if query_string:
        target = f"{target}?{query_string.decode('utf-8')}"
    return target


class TapdbHostBridgeMount:
    """ASGI wrapper that gates mounted TapDB UIs through host auth."""

    def __init__(self, app, bridge: TapdbHostBridge) -> None:
        self.app = app
        self.bridge = bridge

    async def __call__(self, scope, receive, send) -> None:
        if scope.get("type") != "http" or self.bridge.auth_mode != "host_session":
            await self.app(scope, receive, send)
            return

        request = Request(scope, receive)
        user = normalize_host_user(
            self.bridge.resolve_user(request)
            if self.bridge.resolve_user is not None
            else None
        )
        if user is None:
            path = str(scope.get("path") or "")
            if path.startswith("/api/"):
                await JSONResponse(
                    status_code=401,
                    content={"detail": "host_session_required"},
                )(scope, receive, send)
                return

            login_url = resolve_bridge_url(self.bridge.login_url, request) or "/login"
            await RedirectResponse(login_url, status_code=302)(scope, receive, send)
            return

        scoped = dict(scope)
        scoped["tapdb_host_user"] = user
        scoped["tapdb_requested_path"] = _requested_path(request)
        await self.app(scoped, receive, send)


def create_tapdb_web_app(
    *,
    config_path: str,
    host_bridge: TapdbHostBridge | None = None,
):
    """Build the reusable TapDB web surface for standalone or embedded use."""

    from daylily_tapdb.cli.admin_server import load_admin_app

    app = load_admin_app(config_path=config_path)
    app.state.tapdb_host_bridge = host_bridge
    _configure_template_environment(app.state.tapdb_admin_module, host_bridge)
    _attach_canonical_dag_router(
        app,
        config_path=config_path,
        service_name=(host_bridge.service_name if host_bridge is not None else None),
    )
    if host_bridge is not None and host_bridge.auth_mode == "host_session":
        return TapdbHostBridgeMount(app, host_bridge)
    return app
