"""Accessibility checks for TAPDB admin color tokens."""

from __future__ import annotations

import re
from pathlib import Path


CSS_PATH = Path(__file__).resolve().parents[1] / "admin" / "static" / "css" / "style.css"


def _parse_css_vars() -> dict[str, str]:
    text = CSS_PATH.read_text(encoding="utf-8")
    var_pairs = re.findall(r"--([a-zA-Z0-9\-]+)\s*:\s*([^;]+);", text)
    return {k: v.strip() for k, v in var_pairs}


def _resolve_var(name: str, vars_map: dict[str, str]) -> str:
    value = vars_map[name]
    seen = {name}
    while value.startswith("var("):
        inner = value.removeprefix("var(").removesuffix(")").strip()
        inner_name = inner.removeprefix("--")
        if inner_name in seen:
            raise AssertionError(f"Cyclic var reference for {name}")
        seen.add(inner_name)
        value = vars_map[inner_name]
    return value


def _hex_to_rgb(value: str) -> tuple[float, float, float]:
    value = value.strip().lstrip("#")
    if len(value) == 3:
        value = "".join(ch * 2 for ch in value)
    if len(value) != 6:
        raise AssertionError(f"Expected hex color, got: {value!r}")
    return tuple(int(value[i : i + 2], 16) / 255 for i in (0, 2, 4))


def _luminance(rgb: tuple[float, float, float]) -> float:
    def norm(ch: float) -> float:
        return ch / 12.92 if ch <= 0.03928 else ((ch + 0.055) / 1.055) ** 2.4

    r, g, b = rgb
    return 0.2126 * norm(r) + 0.7152 * norm(g) + 0.0722 * norm(b)


def _contrast_ratio(a: str, b: str) -> float:
    lum_a = _luminance(_hex_to_rgb(a))
    lum_b = _luminance(_hex_to_rgb(b))
    high, low = (lum_a, lum_b) if lum_a >= lum_b else (lum_b, lum_a)
    return (high + 0.05) / (low + 0.05)


def test_accessibility_tokens_exist() -> None:
    vars_map = _parse_css_vars()
    required = {
        "color-bg",
        "color-surface",
        "color-text",
        "color-muted",
        "color-link",
        "color-border",
        "color-info",
        "color-success",
        "color-warning",
        "color-danger",
        "focus-ring",
        "btn-primary-bg",
        "btn-primary-fg",
        "btn-danger-bg",
        "btn-danger-fg",
        "btn-success-bg",
        "btn-success-fg",
    }
    missing = sorted(required - vars_map.keys())
    assert not missing, f"Missing accessibility tokens: {missing}"


def test_wcag_aa_contrast_for_core_tokens() -> None:
    vars_map = _parse_css_vars()

    pairs = [
        ("color-text", "color-bg", 4.5),
        ("color-muted", "color-bg", 4.5),
        ("btn-primary-fg", "btn-primary-bg", 4.5),
        ("btn-danger-fg", "btn-danger-bg", 4.5),
        ("btn-success-fg", "btn-success-bg", 4.5),
        ("focus-ring", "color-bg", 3.0),
        ("color-link", "color-bg", 4.5),
    ]

    failures: list[str] = []
    for fg_name, bg_name, minimum in pairs:
        fg = _resolve_var(fg_name, vars_map)
        bg = _resolve_var(bg_name, vars_map)
        ratio = _contrast_ratio(fg, bg)
        if ratio < minimum:
            failures.append(
                f"{fg_name} on {bg_name} contrast {ratio:.2f} < {minimum:.2f}"
            )

    assert not failures, "\n".join(failures)
