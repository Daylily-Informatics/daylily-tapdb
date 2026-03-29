"""Template mutation guardrails.

TapDB template definitions must be loaded from JSON packs through TapDB-owned
loader code. Direct ORM writes from client packages are rejected unless the
current execution context explicitly opts into template mutation.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator


class TemplateMutationGuardError(RuntimeError):
    """Raised when client code attempts to mutate template definitions directly."""


_TEMPLATE_MUTATIONS_ALLOWED: ContextVar[bool] = ContextVar(
    "tapdb_template_mutations_allowed",
    default=False,
)


def template_mutations_allowed() -> bool:
    """Return whether template writes are allowed in the current context."""
    return _TEMPLATE_MUTATIONS_ALLOWED.get()


@contextmanager
def allow_template_mutations() -> Iterator[None]:
    """Allow template mutations for the current call context."""
    token = _TEMPLATE_MUTATIONS_ALLOWED.set(True)
    try:
        yield
    finally:
        _TEMPLATE_MUTATIONS_ALLOWED.reset(token)


def template_mutation_error_message(template_code: str | None = None) -> str:
    """Return the operator-facing error for blocked template writes."""
    target = f" ({template_code})" if template_code else ""
    return (
        "TapDB template mutation is blocked outside the TapDB JSON loader"
        f"{target}. Define templates in a JSON pack and load them with "
        "`tapdb db data seed --config <dir>` or the TapDB template loader API."
    )
