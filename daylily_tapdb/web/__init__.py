"""Reusable TapDB web + DAG integration surfaces.

Load optional web dependencies only when a web export is requested. Core CLI
commands also use ``web.runtime`` and must not import FastAPI merely to load
their database helpers. Exported objects still come from their owning modules;
missing web extras fail normally when those surfaces are requested.
"""

from importlib import import_module

_EXPORT_MODULES = {
    "TapdbHostBridge": "daylily_tapdb.web.bridge",
    "TapdbHostNavLink": "daylily_tapdb.web.bridge",
    "DAG_V2_CONTRACT": "daylily_tapdb.web.dag_v2",
    "DAG_V2_EXTENSION": "daylily_tapdb.web.dag_v2",
    "DagV2EligibilityReason": "daylily_tapdb.web.dag_v2",
    "DagV2Limits": "daylily_tapdb.web.dag_v2",
    "DagV2Manifest": "daylily_tapdb.web.dag_v2",
    "DagV2MountResult": "daylily_tapdb.web.dag_v2",
    "mount_tapdb_dag_surfaces": "daylily_tapdb.web.dag_v2",
    "validate_dag_v2_manifest": "daylily_tapdb.web.dag_v2",
    "create_tapdb_gui_app": "daylily_tapdb.gui",
    "create_tapdb_gui_router": "daylily_tapdb.gui",
}


def __getattr__(name: str):
    module = _EXPORT_MODULES.get(name)
    if module is None:
        raise AttributeError(name)
    return getattr(import_module(module), name)


__all__ = [
    "DAG_V2_CONTRACT",
    "DAG_V2_EXTENSION",
    "DagV2EligibilityReason",
    "DagV2Limits",
    "DagV2Manifest",
    "DagV2MountResult",
    "TapdbHostBridge",
    "TapdbHostNavLink",
    "create_tapdb_gui_app",
    "create_tapdb_gui_router",
    "mount_tapdb_dag_surfaces",
    "validate_dag_v2_manifest",
]
