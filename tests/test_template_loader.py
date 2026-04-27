from __future__ import annotations

import json
from pathlib import Path

import pytest


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _fixture_dir() -> Path:
    return Path(__file__).resolve().parents[1] / "daylily_tapdb" / "etc"


def test_core_bundle_only_seeds_operational_templates():
    from daylily_tapdb.templates.loader import (
        find_tapdb_core_config_dir,
        load_template_configs,
    )

    core_dir = find_tapdb_core_config_dir()
    templates = load_template_configs(core_dir)
    codes = {
        f"{template['category']}/{template['type']}/{template['subtype']}/{template['version']}"
        for template in templates
    }

    assert codes == {
        "SYS/actor/system_user/1.0",
        "MSG/message/webhook_event/1.0",
    }


def test_packaged_registry_fixtures_match_core_prefix_ownership():
    from daylily_tapdb.euid import EUIDConfig

    fixture_dir = _fixture_dir()
    domain_registry = json.loads(
        (fixture_dir / "domain_code_registry.json").read_text(encoding="utf-8")
    )
    prefix_registry = json.loads(
        (fixture_dir / "prefix_ownership_registry.json").read_text(encoding="utf-8")
    )

    assert set(domain_registry["domains"]) == {"Z"}
    assert set(prefix_registry["ownership"]) == {"Z"}
    assert {
        prefix
        for prefix, claim in prefix_registry["ownership"]["Z"].items()
        if claim.get("issuer_app_code") == "daylily-tapdb"
    } == set(EUIDConfig().get_all_prefixes().values())


def test_prepare_seed_templates_rejects_gx_placeholder():
    from daylily_tapdb.templates.loader import _prepare_seed_templates

    with pytest.raises(ValueError, match="same Meridian prefix"):
        _prepare_seed_templates(
            [
                {
                    "_source_file": "/tmp/core/actor/actor.json",
                    "name": "Bad Template",
                    "polymorphic_discriminator": "actor_template",
                    "category": "generic",
                    "type": "actor",
                    "subtype": "system_user",
                    "version": "1.0",
                    "instance_prefix": "GX",
                }
            ],
            core_config_dir=Path("/tmp/core"),
        )


def test_validate_seed_ownership_requires_registered_domain_and_claim(tmp_path):
    from daylily_tapdb.templates.loader import _validate_seed_ownership

    templates = [
        {
            "name": "System User Actor",
            "polymorphic_discriminator": "actor_template",
            "category": "SYS",
            "type": "actor",
            "subtype": "system_user",
            "version": "1.0",
            "instance_prefix": "SYS",
        }
    ]

    domain_registry = tmp_path / "domain_code_registry.json"
    prefix_registry = tmp_path / "prefix_ownership_registry.json"

    _write_json(
        domain_registry,
        {
            "version": "0.4.0",
            "domains": {},
        },
    )
    _write_json(
        prefix_registry,
        {
            "version": "0.4.0",
            "ownership": {
                "T": {
                    "SYS": {"issuer_app_code": "daylily-tapdb"},
                }
            },
        },
    )

    with pytest.raises(ValueError, match="Domain 'T' is not registered"):
        _validate_seed_ownership(
            templates,
            domain_code="T",
            owner_repo_name="daylily-tapdb",
            domain_registry_path=domain_registry,
            prefix_registry_path=prefix_registry,
        )

    _write_json(
        domain_registry,
        {
            "version": "0.4.0",
            "domains": {
                "T": {"label": "localhost"},
            },
        },
    )
    _write_json(
        prefix_registry,
        {
            "version": "0.4.0",
            "ownership": {
                "T": {},
            },
        },
    )

    with pytest.raises(ValueError, match="is not registered for domain"):
        _validate_seed_ownership(
            templates,
            domain_code="T",
            owner_repo_name="daylily-tapdb",
            domain_registry_path=domain_registry,
            prefix_registry_path=prefix_registry,
        )

    _write_json(
        prefix_registry,
        {
            "version": "0.4.0",
            "ownership": {
                "T": {
                    "SYS": {"issuer_app_code": "other-repo"},
                }
            },
        },
    )

    with pytest.raises(ValueError, match="claimed by 'other-repo'"):
        _validate_seed_ownership(
            templates,
            domain_code="T",
            owner_repo_name="daylily-tapdb",
            domain_registry_path=domain_registry,
            prefix_registry_path=prefix_registry,
        )


def test_validate_seed_ownership_accepts_packaged_combined_registry():
    from daylily_tapdb.templates.loader import (
        _validate_seed_ownership,
        load_template_configs,
    )

    fixture_dir = _fixture_dir()
    templates = load_template_configs(
        Path(__file__).resolve().parents[1] / "daylily_tapdb" / "core_config"
    )

    _validate_seed_ownership(
        templates,
        domain_code="Z",
        owner_repo_name="daylily-tapdb",
        domain_registry_path=fixture_dir / "domain_code_registry.json",
        prefix_registry_path=fixture_dir / "prefix_ownership_registry.json",
    )
