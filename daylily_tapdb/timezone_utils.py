"""Shared timezone utilities for TAPDB-backed services."""

from __future__ import annotations

from datetime import UTC, datetime
from zoneinfo import ZoneInfo

DEFAULT_DISPLAY_TIMEZONE = "UTC"
_UTC_ALIASES = {"UTC", "GMT", "GMT+00:00", "GMT0", "Z"}


def normalize_display_timezone(
    value: str | None,
    *,
    default: str = DEFAULT_DISPLAY_TIMEZONE,
) -> str:
    """Normalize a display timezone to an IANA key, defaulting to UTC."""
    candidate = str(value or "").strip()
    if not candidate:
        return default

    upper = candidate.upper()
    if upper in _UTC_ALIASES:
        return DEFAULT_DISPLAY_TIMEZONE

    try:
        return ZoneInfo(candidate).key
    except Exception:
        return default


def is_valid_display_timezone(value: str | None) -> bool:
    """Return True if value can be resolved as a supported display timezone."""
    candidate = str(value or "").strip()
    if not candidate:
        return False
    if candidate.upper() in _UTC_ALIASES:
        return True
    try:
        ZoneInfo(candidate)
        return True
    except Exception:
        return False


def utc_now() -> datetime:
    """Return an aware UTC datetime."""
    return datetime.now(UTC)


def utc_now_iso(*, z_suffix: bool = False) -> str:
    """Return the current UTC timestamp as ISO8601 text."""
    value = utc_now().isoformat()
    if z_suffix:
        return value.replace("+00:00", "Z")
    return value
