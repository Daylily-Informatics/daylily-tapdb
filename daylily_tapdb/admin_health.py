"""Health and readiness endpoints for the standalone TapDB admin runtime."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi.responses import JSONResponse
from sqlalchemy import text

from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.web.runtime import get_db as get_runtime_db

_HEALTH_ROUTE_ATTR = "tapdb_admin_health_routes_attached"


def _public_config_path(config_path: str) -> str:
    return str(Path(config_path).expanduser().resolve())


def install_tapdb_admin_health_routes(app: Any, *, config_path: str) -> None:
    """Attach unauthenticated liveness and readiness endpoints to the admin app."""

    if getattr(app.state, _HEALTH_ROUTE_ATTR, False):
        return

    resolved_config_path = _public_config_path(config_path)

    async def healthz() -> dict[str, object]:
        return {
            "status": "ok",
            "service": "tapdb-admin",
            "config_path": resolved_config_path,
        }

    async def readyz():
        try:
            cfg = get_db_config(config_path=resolved_config_path)
            with get_runtime_db(resolved_config_path) as conn:
                conn.app_username = "system"
                with conn.session_scope() as session:
                    session.execute(text("SELECT 1")).scalar()
        except Exception as exc:
            return JSONResponse(
                status_code=503,
                content={
                    "status": "not_ready",
                    "service": "tapdb-admin",
                    "config_path": resolved_config_path,
                    "error": str(exc),
                },
            )

        return {
            "status": "ready",
            "service": "tapdb-admin",
            "config_path": resolved_config_path,
            "engine_type": str(cfg.get("engine_type") or ""),
            "database": str(cfg.get("database") or ""),
        }

    app.add_api_route("/healthz", healthz, methods=["GET"], include_in_schema=False)
    app.add_api_route("/readyz", readyz, methods=["GET"], include_in_schema=False)
    setattr(app.state, _HEALTH_ROUTE_ATTR, True)
