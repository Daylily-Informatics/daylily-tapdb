"""Helpers for requiring JSON-seeded templates at runtime."""

from __future__ import annotations

from collections.abc import Iterable

from sqlalchemy.orm import Session

from daylily_tapdb.models.template import generic_template
from daylily_tapdb.templates.manager import TemplateManager


class MissingSeededTemplateError(RuntimeError):
    """Raised when client runtime code references a template that was not seeded."""


def _runtime_hint(*, app_name: str | None) -> str:
    if app_name:
        return (
            f"Seed the {app_name} TapDB JSON template pack through TapDB before "
            "running this operation."
        )
    return "Seed the required TapDB JSON template pack before running this operation."


def require_seeded_template(
    session: Session,
    template_code: str,
    *,
    domain_code: str | None = None,
    expected_prefix: str | None = None,
    app_name: str | None = None,
    template_manager: TemplateManager | None = None,
) -> generic_template:
    """Return a seeded template or raise a clear runtime error."""

    manager = template_manager or TemplateManager()
    template = manager.get_template(session, template_code, domain_code=domain_code)
    if template is None:
        raise MissingSeededTemplateError(
            f"Missing seeded TapDB template {template_code!r}. {_runtime_hint(app_name=app_name)}"
        )

    if expected_prefix:
        expected = str(expected_prefix).strip().upper()
        actual = str(template.instance_prefix or "").strip().upper()
        if actual != expected:
            raise MissingSeededTemplateError(
                f"Template {template_code!r} is seeded with instance_prefix {actual!r}, "
                f"expected {expected!r}. {_runtime_hint(app_name=app_name)}"
            )

    return template


def require_seeded_templates(
    session: Session,
    requirements: Iterable[tuple[str, str | None]],
    *,
    domain_code: str | None = None,
    app_name: str | None = None,
    template_manager: TemplateManager | None = None,
) -> list[generic_template]:
    """Require a set of seeded templates and return them in order."""

    manager = template_manager or TemplateManager()
    return [
        require_seeded_template(
            session,
            template_code,
            domain_code=domain_code,
            expected_prefix=expected_prefix,
            app_name=app_name,
            template_manager=manager,
        )
        for template_code, expected_prefix in requirements
    ]
