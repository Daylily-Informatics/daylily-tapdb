"""Shared JSON template-pack loading and validation."""

from __future__ import annotations

import importlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.orm import Session

from daylily_tapdb.models.template import generic_template
from daylily_tapdb.sequences import (
    _normalize_instance_prefix,
    ensure_instance_prefix_sequence,
)
from daylily_tapdb.templates.manager import _resolve_template_scope
from daylily_tapdb.templates.mutation import allow_template_mutations
from daylily_tapdb.validation.instantiation_layouts import (
    format_validation_error,
    validate_instantiation_layouts,
)

try:
    from jsonschema import Draft202012Validator
except (
    ModuleNotFoundError
):  # pragma: no cover - dependency failure is surfaced at runtime.
    Draft202012Validator = None  # type: ignore[assignment]


@dataclass(frozen=True)
class ConfigIssue:
    """Validation issue for a JSON template pack."""

    level: str
    message: str
    source_file: str | None = None
    template_code: str | None = None


@dataclass(frozen=True)
class SeedSummary:
    """Result of seeding validated templates into a TapDB session."""

    templates_loaded: int
    inserted: int
    updated: int
    skipped: int
    prefixes_ensured: int


TEMPLATE_MODEL_BY_DISCRIMINATOR = {
    "generic_template": generic_template,
}


def _get_project_root() -> Path:
    current = Path(__file__).resolve()
    for parent in current.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return Path.cwd()


def _is_source_under_dir(source_file: str | None, directory: Path) -> bool:
    if not source_file:
        return False
    try:
        Path(source_file).resolve().relative_to(directory.resolve())
        return True
    except ValueError:
        return False


def find_config_dir() -> Path:
    """Return the canonical built-in TapDB core template config directory."""
    return find_tapdb_core_config_dir()


def find_tapdb_core_config_dir() -> Path:
    """Find TAPDB's built-in core template config directory."""
    candidates: list[Path] = []

    try:
        tapdb_pkg = importlib.import_module("daylily_tapdb")
        pkg_file = Path(tapdb_pkg.__file__).resolve()
        candidates.extend(
            [
                pkg_file.parent / "core_config",
            ]
        )
    except Exception:
        pass

    current = Path(__file__).resolve()
    candidates.extend(
        [
            current.parent.parent / "core_config",
        ]
    )

    for candidate in candidates:
        if not candidate.exists() or not candidate.is_dir():
            continue
        if (candidate / "actor" / "actor.json").exists() and (
            candidate / "generic" / "generic.json"
        ).exists():
            return candidate

    raise FileNotFoundError(
        "Cannot find TAPDB core config directory with actor/generic templates."
    )


def resolve_seed_config_dirs(config_path: Path | None) -> list[Path]:
    """Resolve ordered template config directories for seeding."""
    core_dir = find_tapdb_core_config_dir().resolve()
    dirs: list[Path] = [core_dir]
    client_dir = config_path.resolve() if config_path is not None else None
    if client_dir is not None and client_dir != core_dir:
        dirs.append(client_dir)
    return dirs


def normalize_config_dirs(config_dirs: Path | list[Path]) -> list[Path]:
    """Normalize config directory input into a de-duplicated ordered list."""
    dirs = [config_dirs] if isinstance(config_dirs, Path) else list(config_dirs)
    seen_dirs: set[Path] = set()
    unique_dirs: list[Path] = []
    for directory in dirs:
        resolved = directory.resolve()
        if resolved in seen_dirs:
            continue
        seen_dirs.add(resolved)
        unique_dirs.append(resolved)
    return unique_dirs


def _template_code(template: dict[str, Any]) -> str:
    return (
        f"{template.get('category')}/"
        f"{template.get('type')}/"
        f"{template.get('subtype')}/"
        f"{template.get('version')}/"
    )


def _template_key(template: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(template.get("category", "")),
        str(template.get("type", "")),
        str(template.get("subtype", "")),
        str(template.get("version", "")),
    )


def _normalize_template_code_str(code: Any) -> str:
    s = str(code).strip()
    if s.endswith("/"):
        s = s[:-1]
    return s


def _is_template_code_str(code: Any) -> bool:
    s = _normalize_template_code_str(code)
    parts = [p for p in s.split("/") if p]
    return len(parts) == 4


def _extract_template_refs(obj: Any) -> list[str]:
    refs: list[str] = []

    def _maybe_add(val: Any):
        if isinstance(val, str):
            refs.append(val)

    def _walk(container: Any):
        if not isinstance(container, dict):
            return
        action_imports = container.get("action_imports")
        if isinstance(action_imports, dict):
            for group in action_imports.values():
                if isinstance(group, dict):
                    actions = group.get("actions")
                    if isinstance(actions, dict):
                        for key in actions:
                            _maybe_add(key)
                else:
                    _maybe_add(group)

        for key in ["expected_inputs", "expected_outputs"]:
            values = container.get(key)
            if isinstance(values, list):
                for value in values:
                    _maybe_add(value)

        layouts = container.get("instantiation_layouts")
        if isinstance(layouts, list):
            for layout in layouts:
                if not isinstance(layout, dict):
                    continue
                children = layout.get("child_templates")
                if isinstance(children, list):
                    for child in children:
                        if isinstance(child, str):
                            _maybe_add(child)
                        elif isinstance(child, dict):
                            _maybe_add(child.get("template_code"))

    if isinstance(obj, dict):
        _walk(obj)
        json_addl = obj.get("json_addl")
        if isinstance(json_addl, dict):
            _walk(json_addl)

    return refs


def _load_template_pack_schema() -> dict[str, Any]:
    schema_path = (
        Path(__file__).resolve().parent / "schema" / "template-pack.schema.json"
    )
    return json.loads(schema_path.read_text(encoding="utf-8"))


def _validate_json_schema(
    payload: dict[str, Any],
    *,
    source_file: str,
    issues: list[ConfigIssue],
) -> None:
    if Draft202012Validator is None:
        issues.append(
            ConfigIssue(
                level="error",
                source_file=source_file,
                message="jsonschema is required for TapDB template-pack validation",
            )
        )
        return

    validator = Draft202012Validator(_load_template_pack_schema())
    for error in validator.iter_errors(payload):
        location = "".join(
            f"[{part!r}]" if isinstance(part, str) else f"[{part}]"
            for part in error.path
        )
        issues.append(
            ConfigIssue(
                level="error",
                source_file=source_file,
                message=f"JSON schema validation failed at {location or '<root>'}: {error.message}",
            )
        )


def load_template_configs(config_dirs: Path | list[Path]) -> list[dict[str, Any]]:
    """Load template configs from one or more config directories."""
    templates: list[dict[str, Any]] = []
    unique_dirs = normalize_config_dirs(config_dirs)

    for config_dir in unique_dirs:
        if not config_dir.exists() or not config_dir.is_dir():
            continue

        for category_dir in sorted(config_dir.iterdir()):
            if not category_dir.is_dir() or category_dir.name.startswith("_"):
                continue

            for json_file in sorted(category_dir.glob("*.json")):
                try:
                    data = json.loads(json_file.read_text(encoding="utf-8"))
                except Exception:
                    continue

                if isinstance(data, dict) and isinstance(data.get("templates"), list):
                    for template in data["templates"]:
                        if not isinstance(template, dict):
                            continue
                        payload = dict(template)
                        payload["_source_file"] = str(json_file)
                        templates.append(payload)

    return templates


def find_duplicate_template_keys(
    templates: list[dict[str, Any]],
) -> dict[tuple[str, str, str, str], list[str]]:
    """Return duplicate template keys with source files for hard-fail checks."""
    key_sources: dict[tuple[str, str, str, str], list[str]] = {}
    for template in templates:
        key = _template_key(template)
        source = str(template.get("_source_file") or "(unknown)")
        key_sources.setdefault(key, []).append(source)
    return {key: sources for key, sources in key_sources.items() if len(sources) > 1}


def validate_template_configs(
    config_dirs: Path | list[Path], *, strict: bool
) -> tuple[list[dict[str, Any]], list[ConfigIssue]]:
    """Load and validate template config JSON files."""
    issues: list[ConfigIssue] = []
    templates: list[dict[str, Any]] = []

    unique_dirs = normalize_config_dirs(config_dirs)
    if not unique_dirs:
        return [], [
            ConfigIssue(level="error", message="No config directories provided")
        ]

    for config_dir in unique_dirs:
        if not config_dir.exists() or not config_dir.is_dir():
            issues.append(
                ConfigIssue(
                    level="error",
                    message=f"Config directory not found: {config_dir}",
                )
            )
            continue

        for category_dir in sorted(config_dir.iterdir()):
            if not category_dir.is_dir() or category_dir.name.startswith("_"):
                continue

            for json_file in sorted(category_dir.glob("*.json")):
                source_file = str(json_file)
                try:
                    payload = json.loads(json_file.read_text(encoding="utf-8"))
                except json.JSONDecodeError as exc:
                    issues.append(
                        ConfigIssue(
                            level="error",
                            source_file=source_file,
                            message=f"Invalid JSON: {exc}",
                        )
                    )
                    continue
                except Exception as exc:
                    issues.append(
                        ConfigIssue(
                            level="error",
                            source_file=source_file,
                            message=f"Error reading file: {exc}",
                        )
                    )
                    continue

                if not isinstance(payload, dict):
                    issues.append(
                        ConfigIssue(
                            level="error",
                            source_file=source_file,
                            message="Config root must be an object/dict",
                        )
                    )
                    continue

                _validate_json_schema(payload, source_file=source_file, issues=issues)
                template_list = payload.get("templates")
                if not isinstance(template_list, list):
                    continue

                for index, template in enumerate(template_list):
                    if not isinstance(template, dict):
                        issues.append(
                            ConfigIssue(
                                level="error",
                                source_file=source_file,
                                message=(
                                    f"Template[{index}] must be an object/dict, "
                                    f"got {type(template).__name__}"
                                ),
                            )
                        )
                        continue
                    item = dict(template)
                    item["_source_file"] = source_file
                    templates.append(item)

    if not templates:
        issues.append(
            ConfigIssue(
                level="error",
                message="No templates found under configured directories",
            )
        )

    required_str = [
        "name",
        "polymorphic_discriminator",
        "category",
        "type",
        "subtype",
        "version",
        "instance_prefix",
    ]
    keys_seen: dict[tuple[str, str, str, str], str] = {}
    codes: set[str] = set()
    refs: list[tuple[str, str, str]] = []
    core_config_dir = find_tapdb_core_config_dir().resolve()

    def _validate_ref_container(
        container: Any, *, source_file: str | None, template_code: str
    ) -> None:
        if not isinstance(container, dict):
            return
        action_imports = container.get("action_imports")
        if action_imports is not None and not isinstance(action_imports, dict):
            issues.append(
                ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=template_code,
                    message=(
                        "Field 'action_imports' must be an object/dict "
                        f"(got {type(action_imports).__name__})"
                    ),
                )
            )

        for key in ["expected_inputs", "expected_outputs"]:
            value = container.get(key)
            if value is not None and not isinstance(value, list):
                issues.append(
                    ConfigIssue(
                        level="error",
                        source_file=source_file,
                        template_code=template_code,
                        message=(
                            f"Field '{key}' must be an array/list "
                            f"(got {type(value).__name__})"
                        ),
                    )
                )

        if container.get("instantiation_layouts") is not None:
            try:
                validate_instantiation_layouts(container.get("instantiation_layouts"))
            except ValidationError as exc:
                issues.append(
                    ConfigIssue(
                        level="error",
                        source_file=source_file,
                        template_code=template_code,
                        message=(
                            "Invalid instantiation_layouts: "
                            f"{format_validation_error(exc)}"
                        ),
                    )
                )

    for template in templates:
        source_file = str(template.get("_source_file") or "") or None
        for key in required_str:
            value = template.get(key)
            if not isinstance(value, str) or not value.strip():
                issues.append(
                    ConfigIssue(
                        level="error",
                        source_file=source_file,
                        message=(
                            f"Missing/invalid required field '{key}' "
                            "(must be non-empty string)"
                        ),
                    )
                )

        code = _normalize_template_code_str(_template_code(template))
        codes.add(code)

        key = _template_key(template)
        if key in keys_seen:
            issues.append(
                ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=code,
                    message=f"Duplicate template key {key} also defined in {keys_seen[key]}",
                )
            )
        else:
            keys_seen[key] = source_file or "(unknown)"

        if template.get("json_addl") is not None and not isinstance(
            template.get("json_addl"), dict
        ):
            issues.append(
                ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=code,
                    message=(
                        "Field 'json_addl' must be an object/dict "
                        f"(got {type(template.get('json_addl')).__name__})"
                    ),
                )
            )

        if template.get("json_addl_schema") is not None and not isinstance(
            template.get("json_addl_schema"), dict
        ):
            issues.append(
                ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=code,
                    message=(
                        "Field 'json_addl_schema' must be an object/dict "
                        f"(got {type(template.get('json_addl_schema')).__name__})"
                    ),
                )
            )

        if "is_singleton" in template and not isinstance(
            template.get("is_singleton"), bool
        ):
            issues.append(
                ConfigIssue(
                    level="error",
                    source_file=source_file,
                    template_code=code,
                    message=(
                        "Field 'is_singleton' must be boolean "
                        f"(got {type(template.get('is_singleton')).__name__})"
                    ),
                )
            )

        instance_prefix = str(template.get("instance_prefix") or "").strip()
        if instance_prefix:
            try:
                normalized_instance_prefix = _normalize_instance_prefix(instance_prefix)
            except ValueError as exc:
                issues.append(
                    ConfigIssue(
                        level="error",
                        source_file=source_file,
                        template_code=code,
                        message=str(exc),
                    )
                )
                normalized_instance_prefix = ""
            if normalized_instance_prefix:
                is_core_template = _is_source_under_dir(source_file, core_config_dir)
                if is_core_template and normalized_instance_prefix not in {"GX", "MSG"}:
                    issues.append(
                        ConfigIssue(
                            level="error",
                            source_file=source_file,
                            template_code=code,
                            message=(
                                "TapDB bundled core templates must use placeholder "
                                "instance_prefix 'GX' or reserved system message "
                                "prefix 'MSG'."
                            ),
                        )
                    )
                if not is_core_template and normalized_instance_prefix in {
                    "GX",
                    "TGX",
                    "MSG",
                }:
                    issues.append(
                        ConfigIssue(
                            level="error",
                            source_file=source_file,
                            template_code=code,
                            message=(
                                f"Client templates cannot persist reserved "
                                f"TapDB core instance_prefix {normalized_instance_prefix!r}."
                            ),
                        )
                    )

        _validate_ref_container(template, source_file=source_file, template_code=code)
        if isinstance(template.get("json_addl"), dict):
            _validate_ref_container(
                template["json_addl"], source_file=source_file, template_code=code
            )

        for ref in _extract_template_refs(template):
            refs.append((source_file or "(unknown)", code, ref))
            if not _is_template_code_str(ref):
                issues.append(
                    ConfigIssue(
                        level="error",
                        source_file=source_file,
                        template_code=code,
                        message=(
                            "Invalid template reference (expected 'category/type/"
                            f"subtype/version'): {ref!r}"
                        ),
                    )
                )

    for source_file, owner_code, ref in refs:
        if not _is_template_code_str(ref):
            continue
        normalized_ref = _normalize_template_code_str(ref)
        if "*" in normalized_ref:
            continue
        if normalized_ref not in codes:
            issues.append(
                ConfigIssue(
                    level="error" if strict else "warning",
                    source_file=source_file,
                    template_code=owner_code,
                    message=f"Referenced template not found in config set: {normalized_ref}",
                )
            )

    return templates, issues


def _template_model_for_discriminator(discriminator: str):
    return TEMPLATE_MODEL_BY_DISCRIMINATOR.get(discriminator, generic_template)


def _upsert_template(
    session: Session,
    template: dict[str, Any],
    *,
    overwrite: bool,
) -> tuple[str, generic_template]:
    resolved_domain_code, resolved_issuer_app_code = _resolve_template_scope(session)
    category, type_name, subtype, version = _template_key(template)
    stmt = (
        select(generic_template)
        .where(
            generic_template.domain_code == resolved_domain_code,
            generic_template.issuer_app_code == resolved_issuer_app_code,
            generic_template.category == category,
            generic_template.type == type_name,
            generic_template.subtype == subtype,
            generic_template.version == version,
        )
        .limit(1)
    )
    existing = session.execute(stmt).scalar_one_or_none()

    if existing is None:
        model_cls = _template_model_for_discriminator(
            str(template.get("polymorphic_discriminator") or "").strip()
        )
        created = model_cls(
            name=str(template.get("name") or ""),
            polymorphic_discriminator=str(
                template.get("polymorphic_discriminator") or "generic_template"
            ),
            domain_code=resolved_domain_code,
            issuer_app_code=resolved_issuer_app_code,
            category=category,
            type=type_name,
            subtype=subtype,
            version=version,
            instance_prefix=str(template.get("instance_prefix") or "").strip().upper(),
            instance_polymorphic_identity=str(
                template.get("instance_polymorphic_identity") or ""
            )
            or None,
            json_addl=dict(template.get("json_addl") or {}),
            json_addl_schema=template.get("json_addl_schema"),
            bstatus=str(template.get("bstatus") or "active"),
            is_singleton=bool(template.get("is_singleton", False)),
            is_deleted=False,
        )
        session.add(created)
        session.flush()
        return "inserted", created

    if not overwrite:
        return "skipped", existing

    changed = False
    target_values = {
        "name": str(template.get("name") or ""),
        "polymorphic_discriminator": str(
            template.get("polymorphic_discriminator") or "generic_template"
        ),
        "instance_prefix": str(template.get("instance_prefix") or "").strip().upper(),
        "instance_polymorphic_identity": str(
            template.get("instance_polymorphic_identity") or ""
        )
        or None,
        "json_addl": dict(template.get("json_addl") or {}),
        "json_addl_schema": template.get("json_addl_schema"),
        "bstatus": str(template.get("bstatus") or "active"),
        "is_singleton": bool(template.get("is_singleton", False)),
        "is_deleted": False,
    }
    for key, value in target_values.items():
        if getattr(existing, key) != value:
            setattr(existing, key, value)
            changed = True

    if changed:
        session.flush()
        return "updated", existing
    return "skipped", existing


def _prepare_seed_templates(
    templates: list[dict[str, Any]],
    *,
    core_config_dir: Path,
    core_instance_prefix: str,
) -> list[dict[str, Any]]:
    prepared: list[dict[str, Any]] = []
    normalized_core_prefix = _normalize_instance_prefix(core_instance_prefix)

    for template in templates:
        source_file = str(template.get("_source_file") or "") or None
        item = dict(template)
        instance_prefix = str(item.get("instance_prefix") or "").strip().upper()
        is_core_template = _is_source_under_dir(source_file, core_config_dir)

        if is_core_template:
            if instance_prefix == "GX":
                item["instance_prefix"] = normalized_core_prefix
            elif instance_prefix == "MSG":
                item["instance_prefix"] = "MSG"
            else:
                raise ValueError(
                    f"TapDB bundled core template {_template_code(item)!r} must "
                    "use placeholder instance_prefix 'GX' or reserved system "
                    "message prefix 'MSG'."
                )
        elif instance_prefix in {"GX", "TGX", "MSG"}:
            raise ValueError(
                f"Client template {_template_code(item)!r} cannot persist reserved "
                f"TapDB core instance_prefix {instance_prefix!r}."
            )

        prepared.append(item)

    return prepared


def seed_templates(
    session: Session,
    templates: list[dict[str, Any]],
    *,
    overwrite: bool,
    core_config_dir: Path,
    core_instance_prefix: str,
) -> SeedSummary:
    """Seed validated template definitions into a TapDB session."""
    prepared_templates = _prepare_seed_templates(
        templates,
        core_config_dir=core_config_dir,
        core_instance_prefix=core_instance_prefix,
    )
    prefixes = sorted(
        {
            str(template.get("instance_prefix") or "").strip().upper()
            for template in prepared_templates
            if str(template.get("instance_prefix") or "").strip()
        }
    )

    inserted = 0
    updated = 0
    skipped = 0

    with allow_template_mutations():
        for prefix in prefixes:
            ensure_instance_prefix_sequence(session, prefix)

        for template in prepared_templates:
            outcome, _ = _upsert_template(session, template, overwrite=overwrite)
            if outcome == "inserted":
                inserted += 1
            elif outcome == "updated":
                updated += 1
            else:
                skipped += 1

    return SeedSummary(
        templates_loaded=len(prepared_templates),
        inserted=inserted,
        updated=updated,
        skipped=skipped,
        prefixes_ensured=len(prefixes),
    )
