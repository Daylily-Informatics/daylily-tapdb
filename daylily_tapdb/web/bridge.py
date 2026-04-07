"""Host bridge types for embedded TapDB web surfaces."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Mapping

from fastapi import Request

TapdbUrlResolver = str | Callable[[Request], str]
TapdbUserResolver = Callable[[Request], Mapping[str, Any] | None]
TapdbContextResolver = Callable[[Request], Mapping[str, Any]]


@dataclass(frozen=True)
class TapdbHostNavLink:
    """Single host-shell navigation link exposed to TapDB templates."""

    label: str
    href: str


@dataclass(frozen=True)
class TapdbHostBridge:
    """Declarative host integration contract for TapDB web surfaces."""

    auth_mode: str = "tapdb"
    service_name: str = ""
    app_name: str = "TAPDB"
    shell_title: str = ""
    shell_subtitle: str = ""
    home_url: TapdbUrlResolver = "/"
    login_url: TapdbUrlResolver = "/login"
    logout_url: TapdbUrlResolver | None = "/logout"
    change_password_url: TapdbUrlResolver | None = "/change-password"
    resolve_user: TapdbUserResolver | None = None
    extra_context: TapdbContextResolver | None = None
    nav_links: tuple[TapdbHostNavLink, ...] = field(default_factory=tuple)
    extra_stylesheets: tuple[str, ...] = field(default_factory=tuple)
    template_override_dirs: tuple[str, ...] = field(default_factory=tuple)


def resolve_bridge_url(value: TapdbUrlResolver | None, request: Request) -> str:
    """Resolve a bridge URL or callable into a request-aware string."""

    if value is None:
        return ""
    if callable(value):
        return str(value(request) or "").strip()
    return str(value or "").strip()


def normalize_host_user(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Normalize host-authenticated user payloads into TapDB's session shape."""

    if not isinstance(payload, Mapping):
        return None

    email = str(payload.get("email") or payload.get("username") or "").strip().lower()
    username = str(payload.get("username") or email).strip().lower()
    if not email and not username:
        return None

    role = str(payload.get("role") or "user").strip().lower()
    if role not in {"admin", "user"}:
        role = "user"

    display_name = str(
        payload.get("display_name")
        or payload.get("name")
        or payload.get("full_name")
        or email
        or username
    ).strip()

    return {
        "uid": payload.get("uid") or username or email,
        "username": username or email,
        "email": email or username,
        "display_name": display_name,
        "role": role,
        "is_active": bool(payload.get("is_active", True)),
        "require_password_change": bool(payload.get("require_password_change", False)),
    }


def resolve_host_shell(bridge: TapdbHostBridge | None, request: Request) -> dict[str, Any]:
    """Build request-local shell context consumed by TapDB templates."""

    if bridge is None:
        return {
            "active": False,
            "app_name": "TAPDB",
            "shell_title": "",
            "shell_subtitle": "",
            "home_url": "",
            "login_url": "",
            "logout_url": "",
            "change_password_url": "",
            "nav_links": [],
            "extra_stylesheets": [],
        }

    return {
        "active": True,
        "app_name": str(bridge.app_name or "TAPDB").strip() or "TAPDB",
        "shell_title": str(bridge.shell_title or "").strip(),
        "shell_subtitle": str(bridge.shell_subtitle or "").strip(),
        "home_url": resolve_bridge_url(bridge.home_url, request),
        "login_url": resolve_bridge_url(bridge.login_url, request),
        "logout_url": resolve_bridge_url(bridge.logout_url, request),
        "change_password_url": resolve_bridge_url(bridge.change_password_url, request),
        "nav_links": [
            {"label": str(item.label).strip(), "href": str(item.href).strip()}
            for item in bridge.nav_links
            if str(item.label).strip() and str(item.href).strip()
        ],
        "extra_stylesheets": [
            str(item).strip() for item in bridge.extra_stylesheets if str(item).strip()
        ],
    }


def resolve_host_context(bridge: TapdbHostBridge | None, request: Request) -> dict[str, Any]:
    """Resolve optional host-supplied template context."""

    if bridge is None or bridge.extra_context is None:
        return {}
    payload = bridge.extra_context(request)
    if not isinstance(payload, Mapping):
        return {}
    return dict(payload)
