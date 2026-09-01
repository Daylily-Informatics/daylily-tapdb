"""Backup class (a): export live templates as a re-seedable template pack.

TAPDB could load template packs but never write one. This closes the loop, so
the definitions a database is running can be captured, reviewed, and re-seeded
elsewhere without a full logical restore -- the "template export" arm of the
decision table in the operator runbook.

The output conforms to ``templates/schema/template-pack.schema.json`` and is
shaped to round-trip through ``templates/loader.py::seed_templates``. The keys
the loader requires (``name``, ``polymorphic_discriminator``, ``category``,
``type``, ``subtype``, ``version``, ``instance_prefix``) are always emitted;
optional ones are omitted when empty so the pack stays diff-friendly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import text

from daylily_tapdb.backup.introspect import quote_ident

#: Keys ``seed_templates`` treats as mandatory on every entry.
REQUIRED_TEMPLATE_KEYS: tuple[str, ...] = (
    "name",
    "polymorphic_discriminator",
    "category",
    "type",
    "subtype",
    "version",
    "instance_prefix",
)

#: Optional keys carried through when the row has a value.
OPTIONAL_TEMPLATE_KEYS: tuple[str, ...] = (
    "instance_polymorphic_identity",
    "validator_ref",
    "bstatus",
    "json_addl",
    "json_addl_schema",
    "is_singleton",
)

#: Columns that must never appear in an exported pack. These are identity and
#: bookkeeping owned by the destination database -- carrying them would either
#: be ignored on re-seed or, worse, collide with EUIDs the target has issued.
EXCLUDED_COLUMNS: frozenset[str] = frozenset(
    {
        "uid",
        "euid",
        "euid_prefix",
        "euid_seq",
        "tenant_id",
        "domain_code",
        "issuer_app_code",
        "created_dt",
        "modified_dt",
        "is_deleted",
    }
)

TEMPLATE_PACK_ARTIFACT = "template-pack.json"


def pack_schema_path() -> Path:
    """Return the bundled template-pack JSON Schema."""
    return (
        Path(__file__).resolve().parent.parent
        / "templates"
        / "schema"
        / "template-pack.schema.json"
    )


def export_templates(
    session: Any,
    schema_name: str,
    *,
    include_deleted: bool = False,
) -> list[dict[str, Any]]:
    """Read live template rows into re-seedable dictionaries.

    Soft-deleted templates are excluded by default: a pack is a statement of
    what the database currently defines, and re-seeding tombstones would
    resurrect definitions someone deliberately retired.
    """
    where = "" if include_deleted else " WHERE is_deleted = FALSE"
    rows = session.execute(
        text(
            f"SELECT * FROM {quote_ident(schema_name)}.generic_template{where} "
            "ORDER BY category, type, subtype, version, name"
        )
    ).mappings()

    templates: list[dict[str, Any]] = []
    for row in rows:
        record = dict(row)
        entry: dict[str, Any] = {}
        for key in REQUIRED_TEMPLATE_KEYS:
            entry[key] = "" if record.get(key) is None else str(record[key])
        for key in OPTIONAL_TEMPLATE_KEYS:
            value = record.get(key)
            if value is None:
                continue
            if key == "is_singleton":
                entry[key] = bool(value)
            elif key in {"json_addl", "json_addl_schema"}:
                if value not in ({}, None):
                    entry[key] = value
            else:
                text_value = str(value).strip()
                if text_value:
                    entry[key] = text_value
        templates.append(entry)
    return templates


def build_template_pack(
    session: Any,
    schema_name: str,
    *,
    include_deleted: bool = False,
    note: Optional[str] = None,
) -> dict[str, Any]:
    """Build the full pack document.

    ``$comment`` is the only place provenance can live -- the schema defines
    exactly one other top-level key -- so the note goes there rather than in an
    ad-hoc field that would fail validation.
    """
    templates = export_templates(session, schema_name, include_deleted=include_deleted)
    pack: dict[str, Any] = {"templates": templates}
    comment = f"Exported from TAPDB schema {schema_name}"
    if note:
        comment = f"{comment} -- {note}"
    pack["$comment"] = comment
    return pack


def validate_template_pack(pack: dict[str, Any]) -> list[str]:
    """Validate a pack, returning human-readable problems.

    Falls back to structural checks when ``jsonschema`` is unavailable so an
    export is never silently unvalidated.
    """
    problems: list[str] = []

    if not isinstance(pack.get("templates"), list):
        return ["pack is missing a 'templates' array"]

    for index, entry in enumerate(pack["templates"]):
        if not isinstance(entry, dict):
            problems.append(f"templates[{index}] is not an object")
            continue
        for key in REQUIRED_TEMPLATE_KEYS:
            if not str(entry.get(key, "")).strip():
                problems.append(f"templates[{index}] is missing required '{key}'")
        for key in EXCLUDED_COLUMNS:
            if key in entry:
                problems.append(
                    f"templates[{index}] carries destination-owned column '{key}'"
                )

    try:
        import jsonschema  # noqa: PLC0415 - optional at runtime
    except ImportError:
        return problems

    schema_file = pack_schema_path()
    if not schema_file.is_file():
        return problems
    try:
        schema = json.loads(schema_file.read_text())
        jsonschema.validate(instance=pack, schema=schema)
    except jsonschema.ValidationError as exc:  # pragma: no cover - shape varies
        problems.append(f"schema validation failed: {exc.message}")
    except (ValueError, OSError) as exc:  # pragma: no cover - unreadable schema
        problems.append(f"could not read template-pack schema: {exc}")

    return problems


def pack_summary(pack: dict[str, Any]) -> dict[str, Any]:
    """Summarize a pack for the manifest and CLI output."""
    templates = pack.get("templates") or []
    categories: dict[str, int] = {}
    prefixes: set[str] = set()
    for entry in templates:
        if not isinstance(entry, dict):
            continue
        category = str(entry.get("category") or "")
        categories[category] = categories.get(category, 0) + 1
        prefix = str(entry.get("instance_prefix") or "").strip()
        if prefix:
            prefixes.add(prefix)
    return {
        "template_count": len(templates),
        "categories": dict(sorted(categories.items())),
        "instance_prefixes": sorted(prefixes),
    }


__all__ = [
    "EXCLUDED_COLUMNS",
    "OPTIONAL_TEMPLATE_KEYS",
    "REQUIRED_TEMPLATE_KEYS",
    "TEMPLATE_PACK_ARTIFACT",
    "build_template_pack",
    "export_templates",
    "pack_schema_path",
    "pack_summary",
    "validate_template_pack",
]
