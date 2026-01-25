"""Structured validation for template `json_addl.instantiation_layouts`.

Per REFACTOR_TAPDB.md Phase 2:
  - Canonical structure: `instantiation_layouts` is a list of objects
    with `child_templates` list
  - Add JSON schema validation (Pydantic preferred)

This module is intentionally narrow: it validates and normalizes the shape of
instantiation layouts, and provides a shared function usable by both the core
InstanceFactory and CLI validation.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, RootModel, TypeAdapter, ValidationError, field_validator


def normalize_template_code_str(code: str) -> str:
    """Normalize template code strings.

    Accepts both `category/type/subtype/version` and `.../` forms and returns
    a canonical *no trailing slash* representation.
    """

    s = str(code).strip()
    if s.endswith("/"):
        s = s[:-1]
    return s


def validate_template_code_str(code: str) -> str:
    """Validate a template-code-like string.

    Expected format: `{category}/{type}/{subtype}/{version}` (optional trailing `/`).
    Returns the normalized (no trailing slash) string.
    """

    s = normalize_template_code_str(code)
    parts = [p for p in s.split("/") if p]
    if len(parts) != 4:
        raise ValueError(
            "invalid template_code format (expected {category}/{type}/{subtype}/{version})"
        )
    return s


class ChildTemplateObject(BaseModel):
    """Structured child template entry."""

    model_config = ConfigDict(extra="allow")

    template_code: str
    count: int = 1
    name_pattern: str | None = None

    @field_validator("template_code")
    @classmethod
    def _validate_template_code(cls, v: str) -> str:
        return validate_template_code_str(v)

    @field_validator("count")
    @classmethod
    def _validate_count(cls, v: int) -> int:
        if v < 1:
            raise ValueError("count must be >= 1")
        return v


class ChildTemplate(RootModel[ChildTemplateObject | str]):
    """Child template entry.

    Canonical config allows either:
      - a string (template_code)
      - an object: {template_code, count?, name_pattern?}
    """

    @field_validator("root")
    @classmethod
    def _validate_root(cls, v: ChildTemplateObject | str):
        if isinstance(v, str):
            return validate_template_code_str(v)
        return v


class InstantiationLayout(BaseModel):
    """One instantiation layout definition."""

    model_config = ConfigDict(extra="allow")

    relationship_type: str = "contains"
    name_pattern: str | None = None
    child_templates: list[ChildTemplate]

    @field_validator("relationship_type")
    @classmethod
    def _validate_relationship_type(cls, v: str) -> str:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("relationship_type must be a non-empty string")
        return v


_layouts_adapter: TypeAdapter[list[InstantiationLayout]] = TypeAdapter(list[InstantiationLayout])


def validate_instantiation_layouts(value: Any) -> list[InstantiationLayout]:
    """Validate and parse `instantiation_layouts`.

    Returns a list of `InstantiationLayout` objects.
    Treats `None`, `[]`, and `{}` as empty.
    """

    if value in (None, [], {}):
        return []
    return _layouts_adapter.validate_python(value)


def format_validation_error(e: ValidationError) -> str:
    """Return a compact, human-friendly error summary."""

    parts: list[str] = []
    for err in e.errors():
        loc = ".".join(str(p) for p in err.get("loc", []) if p is not None)
        msg = err.get("msg", "invalid")
        if loc:
            parts.append(f"{loc}: {msg}")
        else:
            parts.append(str(msg))
    return "; ".join(parts) if parts else str(e)
