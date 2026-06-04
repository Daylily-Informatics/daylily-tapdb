"""Embeddable TapDB GUI surfaces."""

from daylily_tapdb.gui.router import (
    create_tapdb_gui_app,
    create_tapdb_gui_router,
    require_tapdb_gui_admin,
    require_tapdb_gui_user,
)

__all__ = [
    "create_tapdb_gui_app",
    "create_tapdb_gui_router",
    "require_tapdb_gui_admin",
    "require_tapdb_gui_user",
]
