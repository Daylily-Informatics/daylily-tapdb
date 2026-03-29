from __future__ import annotations

from datetime import UTC, datetime

from daylily_tapdb import timezone_utils as m


def test_normalize_display_timezone_defaults_for_empty_and_invalid():
    assert m.normalize_display_timezone("") == "UTC"
    assert (
        m.normalize_display_timezone(
            "Not/A-Real-TZ",
            default="America/New_York",
        )
        == "America/New_York"
    )


def test_normalize_display_timezone_handles_aliases_and_valid_zone():
    assert m.normalize_display_timezone("GMT") == "UTC"
    assert m.normalize_display_timezone("America/Los_Angeles") == "America/Los_Angeles"


def test_is_valid_display_timezone_checks_aliases_and_invalid_values():
    assert m.is_valid_display_timezone("UTC") is True
    assert m.is_valid_display_timezone("GMT+00:00") is True
    assert m.is_valid_display_timezone("America/Chicago") is True
    assert m.is_valid_display_timezone(None) is False
    assert m.is_valid_display_timezone("bad/value") is False


def test_utc_now_iso_emits_plus00_or_z_suffix(monkeypatch):
    fixed = datetime(2026, 3, 29, 12, 0, 5, tzinfo=UTC)
    monkeypatch.setattr(m, "utc_now", lambda: fixed)

    assert m.utc_now_iso() == "2026-03-29T12:00:05+00:00"
    assert m.utc_now_iso(z_suffix=True) == "2026-03-29T12:00:05Z"
