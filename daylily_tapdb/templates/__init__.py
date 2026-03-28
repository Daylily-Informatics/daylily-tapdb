"""Lazy public exports for TapDB template helpers."""

from __future__ import annotations


__all__ = [
    "ConfigIssue",
    "MissingSeededTemplateError",
    "SeedSummary",
    "TemplateManager",
    "TemplateMutationGuardError",
    "allow_template_mutations",
    "find_config_dir",
    "find_duplicate_template_keys",
    "find_tapdb_core_config_dir",
    "load_template_configs",
    "normalize_config_dirs",
    "require_seeded_template",
    "require_seeded_templates",
    "resolve_seed_config_dirs",
    "seed_templates",
    "validate_template_configs",
]


def __getattr__(name: str):
    if name == "TemplateManager":
        from daylily_tapdb.templates.manager import TemplateManager

        return TemplateManager
    if name in {"TemplateMutationGuardError", "allow_template_mutations"}:
        from daylily_tapdb.templates.mutation import (
            TemplateMutationGuardError,
            allow_template_mutations,
        )

        return {
            "TemplateMutationGuardError": TemplateMutationGuardError,
            "allow_template_mutations": allow_template_mutations,
        }[name]
    if name in {"MissingSeededTemplateError", "require_seeded_template", "require_seeded_templates"}:
        from daylily_tapdb.templates.requirements import (
            MissingSeededTemplateError,
            require_seeded_template,
            require_seeded_templates,
        )

        return {
            "MissingSeededTemplateError": MissingSeededTemplateError,
            "require_seeded_template": require_seeded_template,
            "require_seeded_templates": require_seeded_templates,
        }[name]
    if name in {
        "ConfigIssue",
        "SeedSummary",
        "find_config_dir",
        "find_duplicate_template_keys",
        "find_tapdb_core_config_dir",
        "load_template_configs",
        "normalize_config_dirs",
        "resolve_seed_config_dirs",
        "seed_templates",
        "validate_template_configs",
    }:
        from daylily_tapdb.templates.loader import (
            ConfigIssue,
            SeedSummary,
            find_config_dir,
            find_duplicate_template_keys,
            find_tapdb_core_config_dir,
            load_template_configs,
            normalize_config_dirs,
            resolve_seed_config_dirs,
            seed_templates,
            validate_template_configs,
        )

        return {
            "ConfigIssue": ConfigIssue,
            "SeedSummary": SeedSummary,
            "find_config_dir": find_config_dir,
            "find_duplicate_template_keys": find_duplicate_template_keys,
            "find_tapdb_core_config_dir": find_tapdb_core_config_dir,
            "load_template_configs": load_template_configs,
            "normalize_config_dirs": normalize_config_dirs,
            "resolve_seed_config_dirs": resolve_seed_config_dirs,
            "seed_templates": seed_templates,
            "validate_template_configs": validate_template_configs,
        }[name]
    raise AttributeError(name)
