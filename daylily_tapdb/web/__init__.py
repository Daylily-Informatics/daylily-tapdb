"""Reusable TapDB web + DAG integration surfaces."""

from daylily_tapdb.web.bridge import TapdbHostBridge, TapdbHostNavLink
from daylily_tapdb.web.dag import (
    CONTRACT_VERSION,
    build_dag_capability_advertisement,
    create_tapdb_dag_router,
)
from daylily_tapdb.web.dag_v2 import (
    DAG_V2_CONTRACT,
    DAG_V2_EXTENSION,
    DagV2EligibilityReason,
    DagV2Limits,
    DagV2Manifest,
    DagV2MountResult,
    mount_tapdb_dag_surfaces,
    validate_dag_v2_manifest,
)
from daylily_tapdb.web.factory import create_tapdb_web_app, require_tapdb_api_user


def __getattr__(name: str):
    if name in {"create_tapdb_gui_app", "create_tapdb_gui_router"}:
        from daylily_tapdb.gui import create_tapdb_gui_app, create_tapdb_gui_router

        return {
            "create_tapdb_gui_app": create_tapdb_gui_app,
            "create_tapdb_gui_router": create_tapdb_gui_router,
        }[name]
    raise AttributeError(name)


__all__ = [
    "CONTRACT_VERSION",
    "DAG_V2_CONTRACT",
    "DAG_V2_EXTENSION",
    "DagV2EligibilityReason",
    "DagV2Limits",
    "DagV2Manifest",
    "DagV2MountResult",
    "TapdbHostBridge",
    "TapdbHostNavLink",
    "build_dag_capability_advertisement",
    "create_tapdb_dag_router",
    "create_tapdb_gui_app",
    "create_tapdb_gui_router",
    "create_tapdb_web_app",
    "mount_tapdb_dag_surfaces",
    "require_tapdb_api_user",
    "validate_dag_v2_manifest",
]
