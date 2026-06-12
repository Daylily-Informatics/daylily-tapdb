from __future__ import annotations

import json
from pathlib import Path

import pytest

from daylily_tapdb.templates import loader


def _write_pack(path: Path, payload) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _template(**overrides):
    payload = {
        "name": "Sample",
        "polymorphic_discriminator": "generic_template",
        "category": "SMP",
        "type": "sample",
        "subtype": "tube",
        "version": "1.0",
        "instance_prefix": "SMP",
        "instance_polymorphic_identity": "generic_instance",
        "json_addl": {},
    }
    payload.update(overrides)
    return payload


def test_registry_loaders_reject_malformed_files(tmp_path: Path):
    not_object = tmp_path / "not-object.json"
    not_object.write_text("[]", encoding="utf-8")
    bad_domains = tmp_path / "domains.json"
    bad_domains.write_text('{"domains": []}', encoding="utf-8")
    bad_prefixes = tmp_path / "prefixes.json"
    bad_prefixes.write_text('{"ownership": []}', encoding="utf-8")

    with pytest.raises(ValueError, match="JSON object"):
        loader._load_json_file(not_object)
    with pytest.raises(ValueError, match="domains"):
        loader._load_domain_registry(bad_domains)
    with pytest.raises(ValueError, match="ownership"):
        loader._load_prefix_ownership_registry(bad_prefixes)


def test_domain_and_prefix_ownership_validation_errors():
    with pytest.raises(ValueError, match="not registered"):
        loader._assert_registered_domain("Z", {"domains": {}}, source="domains.json")
    with pytest.raises(ValueError, match="No prefix claims"):
        loader._assert_prefix_claimed(
            domain_code="Z",
            prefix="SMP",
            owner_repo_name="owner",
            prefix_registry={"ownership": {}},
            source="prefixes.json",
        )
    with pytest.raises(ValueError, match="not registered"):
        loader._assert_prefix_claimed(
            domain_code="Z",
            prefix="SMP",
            owner_repo_name="owner",
            prefix_registry={"ownership": {"Z": {}}},
            source="prefixes.json",
        )
    with pytest.raises(ValueError, match="missing an owner"):
        loader._assert_prefix_claimed(
            domain_code="Z",
            prefix="SMP",
            owner_repo_name="owner",
            prefix_registry={"ownership": {"Z": {"SMP": {}}}},
            source="prefixes.json",
        )
    with pytest.raises(ValueError, match="claimed by"):
        loader._assert_prefix_claimed(
            domain_code="Z",
            prefix="SMP",
            owner_repo_name="owner",
            prefix_registry={"ownership": {"Z": {"SMP": {"issuer_app_code": "other"}}}},
            source="prefixes.json",
        )


def test_template_ref_extraction_and_duplicate_keys():
    payload = _template(
        json_addl={
            "action_imports": {
                "group": {"actions": {"ACT/action/do/1.0": {}}},
                "single": "ACT/action/single/1.0",
            },
            "expected_inputs": ["SMP/sample/input/1.0"],
            "expected_outputs": ["SMP/sample/output/1.0"],
            "instantiation_layouts": [
                {
                    "relationship_type": "contains",
                    "child_templates": [
                        "WEL/container/well/1.0",
                        {"template_code": "WEL/container/well2/1.0"},
                    ],
                }
            ],
        }
    )

    refs = set(loader._extract_template_refs(payload))
    duplicates = loader.find_duplicate_template_keys(
        [
            {**payload, "_source_file": "a.json"},
            {**payload, "_source_file": "b.json"},
        ]
    )

    assert "ACT/action/do/1.0" in refs
    assert "WEL/container/well2/1.0" in refs
    assert duplicates[("SMP", "sample", "tube", "1.0")] == ["a.json", "b.json"]


def test_load_and_validate_template_configs_collects_errors(
    tmp_path: Path, monkeypatch
):
    config = tmp_path / "config"
    _write_pack(config / "sample" / "valid.json", {"templates": [_template()]})
    _write_pack(config / "sample" / "bad-root.json", [])
    _write_pack(config / "sample" / "bad-template.json", {"templates": ["bad"]})
    (config / "sample" / "invalid-json.json").write_text("{", encoding="utf-8")
    _write_pack(
        config / "sample" / "bad-fields.json",
        {
            "templates": [
                _template(
                    name="",
                    json_addl=[],
                    json_addl_schema=[],
                    is_singleton="no",
                    instance_prefix="SYS",
                    expected_inputs="not-list",
                )
            ]
        },
    )
    monkeypatch.setattr(loader, "find_tapdb_core_config_dir", lambda: tmp_path / "core")

    loaded = loader.load_template_configs([config, config, tmp_path / "missing"])
    templates, issues = loader.validate_template_configs([config], strict=True)

    messages = "\n".join(issue.message for issue in issues)
    assert len(loaded) >= 2
    assert templates
    assert "Config root must be an object/dict" in messages
    assert "Invalid JSON" in messages
    assert "Template[0] must be an object/dict" in messages
    assert "Missing/invalid required field 'name'" in messages
    assert "Field 'json_addl' must be an object/dict" in messages
    assert "Field 'json_addl_schema' must be an object/dict" in messages
    assert "Field 'is_singleton' must be boolean" in messages
    assert (
        "Client templates cannot persist reserved TapDB operational prefix" in messages
    )


def test_validate_template_configs_missing_dirs_and_empty_input(tmp_path: Path):
    no_templates, no_dirs_issues = loader.validate_template_configs([], strict=True)
    missing_templates, missing_issues = loader.validate_template_configs(
        [tmp_path / "missing"],
        strict=False,
    )

    assert no_templates == []
    assert no_dirs_issues[0].message == "No config directories provided"
    assert missing_templates == []
    assert any(
        "Config directory not found" in issue.message for issue in missing_issues
    )
    assert any("No templates found" in issue.message for issue in missing_issues)


def test_prepare_seed_templates_rejects_bad_prefixes(tmp_path: Path):
    with pytest.raises(ValueError, match="must declare an instance_prefix"):
        loader._prepare_seed_templates(
            [_template(instance_prefix="")], core_config_dir=tmp_path
        )

    prepared = loader._prepare_seed_templates(
        [_template(category="container", type="tube", instance_prefix="SMP")],
        core_config_dir=tmp_path,
    )
    assert prepared[0]["category"] == "container"
    assert prepared[0]["instance_prefix"] == "SMP"

    with pytest.raises(ValueError, match="Invalid TAPDB instance prefix"):
        loader._prepare_seed_templates(
            [_template(category="container", instance_prefix="ABCDE")],
            core_config_dir=tmp_path,
        )
    with pytest.raises(ValueError, match="reserved TapDB operational prefix"):
        loader._prepare_seed_templates(
            [_template(category="actor", type="user", subtype="system", instance_prefix="SYS")],
            core_config_dir=tmp_path,
        )


def test_validate_seed_ownership_rejects_missing_prefix(tmp_path: Path):
    domain_registry = tmp_path / "domain.json"
    prefix_registry = tmp_path / "prefix.json"
    domain_registry.write_text(
        '{"domains": {"Z": {"name": "test"}}}',
        encoding="utf-8",
    )
    prefix_registry.write_text(
        '{"ownership": {"Z": {"SMP": {"issuer_app_code": "daylily-tapdb"}}}}',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="missing an instance_prefix"):
        loader._validate_seed_ownership(
            [_template(instance_prefix="")],
            domain_code="Z",
            owner_repo_name="daylily-tapdb",
            domain_registry_path=domain_registry,
            prefix_registry_path=prefix_registry,
        )


def test_seed_templates_rejects_missing_domain_and_owner(tmp_path: Path):
    with pytest.raises(ValueError, match="domain_code is required"):
        loader.seed_templates(
            object(),
            [],
            overwrite=False,
            core_config_dir=tmp_path,
            domain_code="",
            owner_repo_name="daylily-tapdb",
            domain_registry_path=tmp_path / "domain.json",
            prefix_registry_path=tmp_path / "prefix.json",
        )
    with pytest.raises(ValueError, match="owner_repo_name is required"):
        loader.seed_templates(
            object(),
            [],
            overwrite=False,
            core_config_dir=tmp_path,
            domain_code="Z",
            owner_repo_name="",
            domain_registry_path=tmp_path / "domain.json",
            prefix_registry_path=tmp_path / "prefix.json",
        )
