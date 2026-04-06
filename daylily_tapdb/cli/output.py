"""CLI output helpers for Rich renderables."""

from __future__ import annotations

from io import StringIO
from typing import Any

from cli_core_yo import ccyo_out
from rich.console import Console


def print_renderable(renderable: Any) -> None:
    """Render a Rich object to plain text before emitting it."""

    buffer = StringIO()
    console = Console(
        file=buffer,
        force_terminal=False,
        color_system=None,
        width=120,
        legacy_windows=False,
    )
    console.print(renderable)
    ccyo_out.print_text(buffer.getvalue().rstrip("\n"))
