"""Reusable TapDB web + DAG integration surfaces."""

from daylily_tapdb.web.bridge import TapdbHostBridge, TapdbHostNavLink
from daylily_tapdb.web.dag import (
    CONTRACT_VERSION,
    build_dag_capability_advertisement,
    create_tapdb_dag_router,
)
from daylily_tapdb.web.factory import create_tapdb_web_app, require_tapdb_api_user

__all__ = [
    "CONTRACT_VERSION",
    "TapdbHostBridge",
    "TapdbHostNavLink",
    "build_dag_capability_advertisement",
    "create_tapdb_dag_router",
    "create_tapdb_web_app",
    "require_tapdb_api_user",
]
