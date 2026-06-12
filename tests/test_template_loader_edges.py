from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

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


def test_loader_helpers_cover_project_root_and_ignored_reference_shapes(tmp_path: Path, monkeypatch):
    assert (loader._get_project_root() / "pyproject.toml").exists()
    assert loader._extract_template_refs(["not-a-dict"]) == []

    payload = _template(
        json_addl={
            "instantiation_layouts": [
                "not-a-dict-layout",
                {"child_templates": ["container/*/*/1.0"]},
            ]
        }
    )
    monkeypatch.setattr(loader, "find_tapdb_core_config_dir", lambda: tmp_path / "core")
    _templates, issues = loader.validate_template_configs([_write_pack(tmp_path / "cfg" / "sample" / "pack.json", {"templates": [payload]}).parents[1]], strict=True)

    assert loader._extract_template_refs(payload) == ["container/*/*/1.0"]
    assert not any("Referenced template not found" in issue.message for issue in issues)


def test_load_and_validate_template_configs_collects_errors(
    tmp_path: Path, monkeypatch
):
    config = tmp_path / "config"
    _write_pack(config / "sample" / "valid.json", {"templates": [_template()]})
    _write_pack(config / "sample" / "bad-root.json", [])
    _write_pack(config / "sample" / "bad-template-list.json", {"templates": {}})
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


def test_validate_template_configs_collects_nested_reference_and_validator_errors(
    tmp_path: Path, monkeypatch
):
    config = tmp_path / "config"
    core = tmp_path / "core"
    monkeypatch.setattr(loader, "find_tapdb_core_config_dir", lambda: core)

    _write_pack(
        config / "sample" / "bad-refs.json",
        {
            "templates": [
                _template(
                    validator_ref=" ",
                    instance_prefix="BAD!",
                    action_imports=[],
                    expected_inputs="not-list",
                    json_addl={
                        "expected_outputs": "not-list",
                        "instantiation_layouts": [
                            {
                                "relationship_type": "contains",
                                "child_templates": [
                                    {"template_code": "container/tube/missing/1.0", "count": 0}
                                ],
                            }
                        ],
                    },
                ),
                _template(
                    name="Sample Duplicate",
                    action_imports={"single": "bad-ref"},
                ),
            ]
        },
    )

    templates, issues = loader.validate_template_configs([config], strict=False)

    messages = "\n".join(issue.message for issue in issues)
    assert len(templates) == 2
    assert "Field 'validator_ref' must be a non-empty string" in messages
    assert "Invalid TAPDB instance prefix" in messages
    assert "Field 'action_imports' must be an object/dict" in messages
    assert "Field 'expected_inputs' must be an array/list" in messages
    assert "Field 'expected_outputs' must be an array/list" in messages
    assert "Invalid instantiation_layouts" in messages
    assert "Invalid template reference" in messages
    assert "Duplicate template key" in messages


def test_validate_template_configs_flags_core_prefix_violations(
    tmp_path: Path, monkeypatch
):
    core = tmp_path / "core"
    monkeypatch.setattr(loader, "find_tapdb_core_config_dir", lambda: core)
    _write_pack(
        core / "container" / "container.json",
        {"templates": [_template(category="container", type="tube", instance_prefix="SMP")]},
    )

    _templates, issues = loader.validate_template_configs([core], strict=True)

    assert any("TapDB bundled core templates must use reserved" in issue.message for issue in issues)


def test_validate_json_schema_reports_missing_dependency(monkeypatch):
    issues = []
    monkeypatch.setattr(loader, "Draft202012Validator", None)

    loader._validate_json_schema({}, source_file="pack.json", issues=issues)

    assert issues[0].message == "jsonschema is required for TapDB template-pack validation"


def test_apply_seed_session_scope_sets_postgres_identity():
    calls = []

    class Session:
        bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        def execute(self, stmt, params):
            del stmt
            calls.append(params)

    loader._apply_seed_session_scope(
        Session(), domain_code="Z", owner_repo_name="daylily-tapdb"
    )

    assert calls == [{"code": "Z"}, {"owner": "daylily-tapdb"}]


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


def test_upsert_template_inserts_updates_and_skips(monkeypatch):
    class Result:
        def __init__(self, value):
            self.value = value

        def scalar_one_or_none(self):
            return self.value

    class Session:
        def __init__(self, existing):
            self.existing = list(existing)
            self.added = []
            self.flushed = 0

        def execute(self, stmt):
            del stmt
            return Result(self.existing.pop(0))

        def add(self, obj):
            self.added.append(obj)

        def flush(self):
            self.flushed += 1

    base = _template(
        category="container",
        type="tube",
        subtype="1.5ml-eppi",
        version="1.0",
        instance_prefix="SMP",
        validator_ref="CUSTOM@1",
        bstatus="active",
        is_singleton=True,
    )
    changed_existing = SimpleNamespace(
        name="Old",
        polymorphic_discriminator="generic_template",
        domain_code="Z",
        instance_prefix="OLD",
        instance_polymorphic_identity=None,
        json_addl={},
        validator_ref="OLD@1",
        json_addl_schema=None,
        bstatus="old",
        is_singleton=False,
        is_deleted=True,
    )
    same_existing = SimpleNamespace(
        name=base["name"],
        polymorphic_discriminator=base["polymorphic_discriminator"],
        domain_code="Z",
        instance_prefix=base["instance_prefix"],
        instance_polymorphic_identity=base["instance_polymorphic_identity"],
        json_addl=base["json_addl"],
        validator_ref=base["validator_ref"],
        json_addl_schema=None,
        bstatus=base["bstatus"],
        is_singleton=base["is_singleton"],
        is_deleted=False,
    )
    session = Session([None, changed_existing, same_existing, same_existing])

    assert loader._template_model_for_discriminator("unknown") is loader.generic_template
    inserted, created = loader._upsert_template(
        session, base, domain_code="Z", overwrite=True
    )
    updated, updated_obj = loader._upsert_template(
        session, base, domain_code="Z", overwrite=True
    )
    skipped, skipped_obj = loader._upsert_template(
        session, base, domain_code="Z", overwrite=True
    )
    skipped_no_overwrite, _ = loader._upsert_template(
        session, base, domain_code="Z", overwrite=False
    )

    assert inserted == "inserted"
    assert created in session.added
    assert updated == "updated"
    assert updated_obj.name == base["name"]
    assert skipped == "skipped"
    assert skipped_obj is same_existing
    assert skipped_no_overwrite == "skipped"
    assert session.flushed >= 2


def test_seed_templates_counts_outcomes_and_governance_hook(tmp_path: Path, monkeypatch):
    outcomes = iter(["inserted", "updated", "skipped"])
    ensured_prefixes = []
    governance_calls = []

    monkeypatch.setattr(loader, "_validate_seed_ownership", lambda *args, **kwargs: None)
    monkeypatch.setattr(loader, "ensure_instance_prefix_sequence", lambda session, prefix: ensured_prefixes.append(prefix))
    monkeypatch.setattr(
        loader,
        "_upsert_template",
        lambda session, template, *, domain_code, overwrite: (
            next(outcomes),
            SimpleNamespace(),
        ),
    )
    monkeypatch.setattr(
        loader,
        "ensure_core_governance_objects",
        lambda session, *, domain_code: governance_calls.append(domain_code),
    )

    summary = loader.seed_templates(
        SimpleNamespace(bind=SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))),
        [
            _template(
                _source_file=str(tmp_path / "core" / "governance" / "governance.json"),
                category="governance",
                type="validator",
                subtype="definition",
                instance_prefix="GVR",
            ),
            _template(category="container", type="tube", subtype="small", instance_prefix="SMP"),
            _template(category="container", type="tube", subtype="large", instance_prefix="SMP"),
        ],
        overwrite=True,
        core_config_dir=tmp_path / "core",
        domain_code="z",
        owner_repo_name="daylily-tapdb",
        domain_registry_path=tmp_path / "domain.json",
        prefix_registry_path=tmp_path / "prefix.json",
    )

    assert summary.templates_loaded == 3
    assert summary.inserted == 1
    assert summary.updated == 1
    assert summary.skipped == 1
    assert summary.prefixes_ensured == 2
    assert ensured_prefixes == ["GVR", "SMP"]
    assert governance_calls == ["Z"]
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
