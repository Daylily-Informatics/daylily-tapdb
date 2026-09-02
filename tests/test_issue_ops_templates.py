"""Focused acceptance coverage for ISS-090, ISS-072, ISS-048, TEST-OPS-001."""

from __future__ import annotations

import ast
import base64
import json
import os
import shutil
import stat
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from click import unstyle
from typer.testing import CliRunner

import daylily_tapdb.runtime_info as runtime_info
import daylily_tapdb.templates.repository as repository
from daylily_tapdb.cli import app
from daylily_tapdb.cli import objects as objects_cli
from daylily_tapdb.cli import templates as templates_cli
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.runtime_info import build_runtime_info
from daylily_tapdb.services.object_operations import (
    ObjectSelector,
    get_object,
    object_payload,
    repair_object,
    resolve_object,
    soft_delete_object,
    update_object,
)
from daylily_tapdb.services.object_search import search_objects
from daylily_tapdb.templates.repository import (
    FORBIDDEN_FIELDS,
    export_repository_pack,
    import_repository_pack,
    read_repository_pack,
    repository_inventory,
    serialize_template,
    validate_repository_pack,
)


class _Scalars:
    def __init__(self, rows):
        self.rows = list(rows)

    def scalars(self):
        return self

    def __iter__(self):
        return iter(self.rows)

    def scalar_one_or_none(self):
        if len(self.rows) > 1:
            raise AssertionError("fixture returned more than one row")
        return self.rows[0] if self.rows else None


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.flush_count = 0

    def execute(self, statement):
        entity = statement.column_descriptions[0].get("entity")
        return _Scalars(self.rows.get(entity, []))

    def flush(self):
        self.flush_count += 1


class _CliConn:
    def __init__(self, session):
        self.session = session

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    class _Scope:
        def __init__(self, session):
            self.session = session

        def __enter__(self):
            return self.session

        def __exit__(self, exc_type, exc, tb):
            return False

    def session_scope(self, commit=False):
        del commit
        return self._Scope(self.session)


def _template(euid="stored-template-one"):
    return SimpleNamespace(
        uid=7,
        euid=euid,
        euid_prefix="TPX",
        euid_seq=91,
        domain_code="Z",
        issuer_app_code="example-owner",
        tenant_id=None,
        name="Assay Template",
        polymorphic_discriminator="generic_template",
        category="assay",
        type="sequencing",
        subtype="short_read",
        version="1.0",
        instance_prefix="ASY",
        instance_polymorphic_identity="generic_instance",
        validator_ref="UNIVERSAL_PASS@1",
        bstatus="active",
        json_addl={"properties": {"platform": ""}},
        json_addl_schema=None,
        is_singleton=False,
        is_deleted=False,
        created_dt=None,
        modified_dt=None,
    )


def _instance():
    return SimpleNamespace(
        uid=11,
        euid="stored-instance-one",
        machine_uuid=None,
        domain_code="Z",
        issuer_app_code="example-owner",
        tenant_id=None,
        name="Run one",
        category="assay",
        type="sequencing",
        subtype="short_read",
        version="1.0",
        bstatus="active",
        json_addl={"lane": 1},
        is_deleted=False,
        created_dt=None,
        modified_dt=None,
        template_uid=7,
    )


def _registries(tmp_path: Path) -> tuple[Path, Path]:
    domain = tmp_path / "domains.json"
    prefix = tmp_path / "prefixes.json"
    domain.write_text(
        json.dumps({"version": "0.4.8", "domains": {"Z": {"name": "test"}}}),
        encoding="utf-8",
    )
    prefix.write_text(
        json.dumps(
            {
                "version": "0.4.8",
                "ownership": {"Z": {"ASY": {"issuer_app_code": "example-owner"}}},
            }
        ),
        encoding="utf-8",
    )
    return domain, prefix


def test_repository_pack_is_deterministic_seedable_and_identity_free(tmp_path: Path):
    _domain, prefix = _registries(tmp_path)
    row = _template()
    session = _Session({generic_template: [row]})
    first = tmp_path / "first.json"
    second = tmp_path / "second.json"

    receipt = export_repository_pack(
        session,
        first,
        domain_code="Z",
        issuer_app_code="example-owner",
        prefix_registry_path=prefix,
        actor="operator@example.test",
    )
    export_repository_pack(
        session,
        second,
        domain_code="Z",
        issuer_app_code="example-owner",
        prefix_registry_path=prefix,
        actor="operator@example.test",
    )

    assert first.read_bytes() == second.read_bytes()
    _path, pack, _raw = read_repository_pack(first)
    assert pack["templates"] == [serialize_template(row)]
    assert not FORBIDDEN_FIELDS.intersection(pack["templates"][0])
    assert receipt["templates"][0]["stored_euid"] == row.euid
    assert receipt["repository_pack"] == first.name
    assert str(tmp_path) not in json.dumps(receipt)
    assert receipt["prefix_registry"]["claims"]["ASY"]["issuer_app_code"] == (
        "example-owner"
    )
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        export_repository_pack(
            session,
            first,
            domain_code="Z",
            issuer_app_code="example-owner",
            prefix_registry_path=prefix,
            actor="operator@example.test",
        )

    secret_row = _template("stored-template-two")
    secret_row.json_addl = {"api_token": "must-not-leak"}
    with pytest.raises(ValueError, match="sensitive key"):
        serialize_template(secret_row)


def test_repository_inventory_and_import_round_trip_revalidates_claims(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    domain, prefix = _registries(tmp_path)
    row = _template()
    source = _Session({generic_template: [row]})
    pack_path = tmp_path / "templates.json"
    export_repository_pack(
        source,
        pack_path,
        domain_code="Z",
        issuer_app_code="example-owner",
        prefix_registry_path=prefix,
        actor="operator@example.test",
    )

    inventory = repository_inventory(
        source,
        pack_path,
        domain_code="Z",
        issuer_app_code="example-owner",
    )
    assert inventory["counts"] == {"pending": 0, "backed-up": 1, "failed": 0}

    target = _Session({generic_template: []})
    result = import_repository_pack(
        target,
        pack_path,
        domain_code="Z",
        owner_repo_name="example-owner",
        domain_registry_path=domain,
        prefix_registry_path=prefix,
        dry_run=True,
    )
    assert result.dry_run is True
    assert result.template_count == 1
    assert result.prefixes_validated == ("ASY",)
    assert target.flush_count == 0

    seeded: list[dict] = []

    def _seed_templates(_session, templates, **_kwargs):
        seeded.extend(templates)
        return SimpleNamespace(inserted=len(templates), skipped=0)

    monkeypatch.setattr(
        "daylily_tapdb.templates.repository.seed_templates", _seed_templates
    )
    applied = import_repository_pack(
        target,
        pack_path,
        domain_code="Z",
        owner_repo_name="example-owner",
        domain_registry_path=domain,
        prefix_registry_path=prefix,
        dry_run=False,
    )
    assert applied.inserted == 1
    assert seeded == [serialize_template(row)]

    pack_path.write_text(
        pack_path.read_text(encoding="utf-8").replace("Assay Template", "Tampered"),
        encoding="utf-8",
    )
    failed = repository_inventory(
        source,
        pack_path,
        domain_code="Z",
        issuer_app_code="example-owner",
    )
    assert failed["status"] == "failed"
    assert failed["counts"]["failed"] == 1
    with pytest.raises(ValueError, match="checksum"):
        import_repository_pack(
            target,
            pack_path,
            domain_code="Z",
            owner_repo_name="example-owner",
            domain_registry_path=domain,
            prefix_registry_path=prefix,
            dry_run=True,
        )


def test_repository_pack_receipt_is_portable_across_checkout_paths(
    tmp_path: Path,
) -> None:
    domain, prefix = _registries(tmp_path)
    source_dir = tmp_path / "checkout-a"
    target_dir = tmp_path / "checkout-b"
    source_dir.mkdir()
    target_dir.mkdir()
    source_path = source_dir / "templates.json"
    target_path = target_dir / source_path.name
    session = _Session({generic_template: [_template()]})

    receipt = export_repository_pack(
        session,
        source_path,
        domain_code="Z",
        issuer_app_code="example-owner",
        prefix_registry_path=prefix,
        actor="operator@example.test",
    )
    assert stat.S_IMODE(source_path.stat().st_mode) == 0o644
    assert stat.S_IMODE(repository.receipt_path(source_path).stat().st_mode) == 0o444
    shutil.copy2(source_path, target_path)
    shutil.copy2(
        repository.receipt_path(source_path), repository.receipt_path(target_path)
    )

    result = import_repository_pack(
        _Session({generic_template: []}),
        target_path,
        domain_code="Z",
        owner_repo_name="example-owner",
        domain_registry_path=domain,
        prefix_registry_path=prefix,
        dry_run=True,
    )

    assert receipt["repository_pack"] == "templates.json"
    assert result.template_count == 1
    assert (
        repository_inventory(
            session,
            target_path,
            domain_code="Z",
            issuer_app_code="example-owner",
        )["counts"]["backed-up"]
        == 1
    )


def test_repository_atomic_publication_sets_mode_before_link_and_fsyncs_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    target = tmp_path / "receipt.json"
    observations: list[tuple[bool, int, bool]] = []
    real_fsync = os.fsync

    def recording_fsync(descriptor: int) -> None:
        mode = os.fstat(descriptor).st_mode
        observations.append((stat.S_ISDIR(mode), stat.S_IMODE(mode), target.exists()))
        real_fsync(descriptor)

    monkeypatch.setattr(repository.os, "fsync", recording_fsync)
    repository._atomic_write_new(target, b"{}\n", mode=0o444)

    assert stat.S_IMODE(target.stat().st_mode) == 0o444
    assert any(
        not is_directory and mode == 0o444 and not published
        for is_directory, mode, published in observations
    )
    assert any(
        is_directory and published for is_directory, _mode, published in observations
    )


def test_repository_export_refuses_a_dangling_symlink_collision(tmp_path: Path) -> None:
    _domain, prefix = _registries(tmp_path)
    pack_path = tmp_path / "templates.json"
    missing_target = tmp_path / "must-not-be-created.json"
    pack_path.symlink_to(missing_target)

    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        export_repository_pack(
            _Session({generic_template: [_template()]}),
            pack_path,
            domain_code="Z",
            issuer_app_code="example-owner",
            prefix_registry_path=prefix,
            actor="operator@example.test",
        )

    assert pack_path.is_symlink()
    assert not missing_target.exists()
    assert not repository.receipt_path(pack_path).exists()


def test_repository_export_rejects_sensitive_prefix_claims(tmp_path: Path) -> None:
    _domain, prefix = _registries(tmp_path)
    payload = json.loads(prefix.read_text(encoding="utf-8"))
    payload["ownership"]["Z"]["ASY"]["api_token"] = "synthetic-secret"
    prefix.write_text(json.dumps(payload), encoding="utf-8")
    pack_path = tmp_path / "templates.json"

    with pytest.raises(ValueError, match="prefix_registry.claims.*api_token"):
        export_repository_pack(
            _Session({generic_template: [_template()]}),
            pack_path,
            domain_code="Z",
            issuer_app_code="example-owner",
            prefix_registry_path=prefix,
            actor="operator@example.test",
        )

    assert not pack_path.exists()
    assert not repository.receipt_path(pack_path).exists()


def test_repository_pack_validation_and_path_failures_are_explicit(tmp_path: Path):
    valid = repository.build_repository_pack([_template()])
    cases = [
        ({"format": "wrong", "templates": valid["templates"]}, "format must"),
        ({"format": repository.REPOSITORY_PACK_FORMAT, "templates": []}, "non-empty"),
        (
            {
                "format": repository.REPOSITORY_PACK_FORMAT,
                "templates": [{**valid["templates"][0], "uid": 1}],
            },
            "forbidden field",
        ),
        (
            {
                "format": repository.REPOSITORY_PACK_FORMAT,
                "templates": [
                    {
                        key: value
                        for key, value in valid["templates"][0].items()
                        if key != "name"
                    }
                ],
            },
            "missing required",
        ),
        (
            {
                "format": repository.REPOSITORY_PACK_FORMAT,
                "templates": [{**valid["templates"][0], "unsupported": True}],
            },
            "unsupported field",
        ),
        (
            {
                "format": repository.REPOSITORY_PACK_FORMAT,
                "templates": valid["templates"] * 2,
            },
            "duplicate template",
        ),
    ]
    for payload, message in cases:
        with pytest.raises(ValueError, match=message):
            validate_repository_pack(payload)
    with pytest.raises(ValueError, match="sensitive key"):
        validate_repository_pack(
            {
                "format": repository.REPOSITORY_PACK_FORMAT,
                "templates": [
                    {
                        **valid["templates"][0],
                        "json_addl_schema": {"access_token": {"type": "string"}},
                    }
                ],
            }
        )
    with pytest.raises(ValueError, match="absolute"):
        repository.read_repository_pack(Path("relative.json"))
    with pytest.raises(ValueError, match="absolute"):
        repository.read_repository_pack(Path("~/repository-pack.json"))
    with pytest.raises(FileNotFoundError):
        repository.read_repository_pack(tmp_path / "missing-dir" / "pack.json")
    with pytest.raises(ValueError, match=".json"):
        repository.read_repository_pack(tmp_path / "pack.txt")


def test_repository_export_and_inventory_failure_paths(tmp_path: Path):
    _domain, prefix = _registries(tmp_path)
    empty = _Session({generic_template: []})
    with pytest.raises(LookupError, match="no active owned"):
        export_repository_pack(
            empty,
            tmp_path / "empty.json",
            domain_code="Z",
            issuer_app_code="example-owner",
            prefix_registry_path=prefix,
            actor="operator@example.test",
        )
    with pytest.raises(LookupError, match="template not found"):
        export_repository_pack(
            empty,
            tmp_path / "missing.json",
            domain_code="Z",
            issuer_app_code="example-owner",
            prefix_registry_path=prefix,
            actor="operator@example.test",
            template_euid="missing-template",
        )

    source = _Session({generic_template: [_template()]})
    pending_path = tmp_path / "pending.json"
    pending = repository_inventory(
        source,
        pending_path,
        domain_code="Z",
        issuer_app_code="example-owner",
    )
    assert pending["counts"]["pending"] == 1
    repository.receipt_path(pending_path).write_text("{}", encoding="utf-8")
    orphan = repository_inventory(
        source,
        pending_path,
        domain_code="Z",
        issuer_app_code="example-owner",
    )
    assert orphan["status"] == "failed"

    collision_path = tmp_path / "collision.json"
    repository.receipt_path(collision_path).write_text("{}", encoding="utf-8")
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        export_repository_pack(
            source,
            collision_path,
            domain_code="Z",
            issuer_app_code="example-owner",
            prefix_registry_path=prefix,
            actor="operator@example.test",
        )


def test_repository_import_detects_scope_registry_and_semantic_conflicts(
    tmp_path: Path,
):
    domain, prefix = _registries(tmp_path)
    source_row = _template()
    source = _Session({generic_template: [source_row]})
    pack_path = tmp_path / "templates.json"
    export_repository_pack(
        source,
        pack_path,
        domain_code="Z",
        issuer_app_code="example-owner",
        prefix_registry_path=prefix,
        actor="operator@example.test",
    )
    same = import_repository_pack(
        source,
        pack_path,
        domain_code="Z",
        owner_repo_name="example-owner",
        domain_registry_path=domain,
        prefix_registry_path=prefix,
    )
    assert same.skipped == 1

    conflicting = _template()
    conflicting.name = "Conflicting stored name"
    with pytest.raises(ValueError, match="conflicts with stored identity"):
        import_repository_pack(
            _Session({generic_template: [conflicting]}),
            pack_path,
            domain_code="Z",
            owner_repo_name="example-owner",
            domain_registry_path=domain,
            prefix_registry_path=prefix,
        )
    with pytest.raises(ValueError, match="receipt owner"):
        import_repository_pack(
            _Session({generic_template: []}),
            pack_path,
            domain_code="Z",
            owner_repo_name="different-owner",
            domain_registry_path=domain,
            prefix_registry_path=prefix,
        )

    wrong_domain = tmp_path / "wrong-domain.json"
    wrong_domain.write_text(json.dumps({"version": "0.4.8", "domains": {}}))
    with pytest.raises(ValueError, match="domain registry"):
        import_repository_pack(
            _Session({generic_template: []}),
            pack_path,
            domain_code="Z",
            owner_repo_name="example-owner",
            domain_registry_path=wrong_domain,
            prefix_registry_path=prefix,
        )


def test_repository_receipt_and_registry_validation_is_fail_closed(tmp_path: Path):
    row = _template()
    row.json_addl = {"values": (1, SimpleNamespace(label="display-only"))}
    assert serialize_template(row)["json_addl"]["values"] == [
        1,
        "namespace(label='display-only')",
    ]

    missing_ownership = tmp_path / "missing-ownership.json"
    missing_ownership.write_text("{}", encoding="utf-8")
    with pytest.raises(ValueError, match="missing an ownership"):
        repository._load_registry_claims(
            missing_ownership, domain_code="Z", prefixes=["ASY"]
        )
    missing_domain = tmp_path / "missing-domain.json"
    missing_domain.write_text(json.dumps({"ownership": {}}), encoding="utf-8")
    with pytest.raises(ValueError, match="no claims for domain"):
        repository._load_registry_claims(
            missing_domain, domain_code="Z", prefixes=["ASY"]
        )
    missing_prefix = tmp_path / "missing-prefix.json"
    missing_prefix.write_text(json.dumps({"ownership": {"Z": {}}}), encoding="utf-8")
    with pytest.raises(ValueError, match="ownership claim"):
        repository._load_registry_claims(
            missing_prefix, domain_code="Z", prefixes=["ASY"]
        )

    pack_path = tmp_path / "pack.json"
    raw = repository.canonical_json_bytes(
        repository.build_repository_pack([_template()])
    )
    pack_path.write_bytes(raw)
    with pytest.raises(FileNotFoundError, match="receipt not found"):
        repository._verified_receipt(
            pack_path,
            raw,
            domain_code="Z",
            issuer_app_code="example-owner",
        )
    sidecar = repository.receipt_path(pack_path)
    sidecar.write_text("not-json", encoding="utf-8")
    with pytest.raises(ValueError, match="unreadable"):
        repository._verified_receipt(
            pack_path,
            raw,
            domain_code="Z",
            issuer_app_code="example-owner",
        )

    valid_receipt = {
        "format": repository.RECEIPT_FORMAT,
        "repository_pack": pack_path.name,
        "content_sha256": repository.hashlib.sha256(raw).hexdigest(),
        "domain_code": "Z",
        "issuer_app_code": "example-owner",
    }
    for replacement, message in (
        ({"format": "wrong"}, "receipt format"),
        ({"repository_pack": "other.json"}, "pack path"),
        ({"domain_code": "Y"}, "receipt domain"),
    ):
        sidecar.write_text(
            json.dumps({**valid_receipt, **replacement}), encoding="utf-8"
        )
        with pytest.raises(ValueError, match=message):
            repository._verified_receipt(
                pack_path,
                raw,
                domain_code="Z",
                issuer_app_code="example-owner",
            )

    non_object = tmp_path / "non-object.json"
    non_object.write_text("[]", encoding="utf-8")
    with pytest.raises(ValueError, match="root must be a JSON object"):
        read_repository_pack(non_object)


def test_exact_selector_mutations_are_allowlisted_audited_and_dry_run_first():
    instance = _instance()
    session = _Session(
        {
            generic_template: [],
            generic_instance: [instance],
            generic_instance_lineage: [],
        }
    )
    selector = ObjectSelector(euid=instance.euid)

    preview = update_object(
        session,
        selector,
        {"name": "Run two"},
        actor="operator@example.test",
        dry_run=True,
    )
    assert preview["dry_run"] is True
    assert preview["changes"]["name"] == {"old": "Run one", "new": "Run two"}
    assert instance.name == "Run one"

    applied = update_object(
        session,
        selector,
        {"name": "Run two", "json_addl": {"lane": 2}},
        actor="operator@example.test",
        dry_run=False,
    )
    assert applied["applied"] is True
    assert instance.name == "Run two"
    assert session.flush_count == 1
    with pytest.raises(ValueError, match="not allowed"):
        update_object(
            session,
            selector,
            {"euid": "invented"},
            actor="operator@example.test",
        )

    preview_delete = soft_delete_object(
        session, selector, actor="operator@example.test", dry_run=True
    )
    assert preview_delete["operation"] == "soft-delete"
    assert instance.is_deleted is False
    soft_delete_object(session, selector, actor="operator@example.test", dry_run=False)
    assert instance.is_deleted is True


def test_lineage_relationship_identity_is_not_operator_updateable() -> None:
    lineage = SimpleNamespace(
        uid=21,
        euid="stored-lineage-one",
        relationship_type="contains",
        name="contains",
        bstatus="active",
        json_addl={},
        is_deleted=False,
    )
    session = _Session({generic_instance_lineage: [lineage]})

    with pytest.raises(ValueError, match="relationship_type"):
        update_object(
            session,
            ObjectSelector(euid=lineage.euid, record_type="lineage"),
            {"relationship_type": "references"},
            actor="operator@example.test",
            dry_run=True,
        )


def test_exact_selectors_and_payloads_cover_each_governed_object_kind():
    instance = _instance()
    instance.machine_uuid = uuid4()
    template = _template()
    lineage = SimpleNamespace(
        uid=21,
        euid="stored-lineage-one",
        name="contains",
        domain_code="Z",
        issuer_app_code="example-owner",
        tenant_id=None,
        category="lineage",
        type="lineage",
        subtype="generic",
        version="1.0",
        bstatus="active",
        json_addl={},
        is_deleted=False,
        created_dt=datetime.now(UTC),
        modified_dt=None,
        parent_instance_uid=11,
        child_instance_uid=12,
        relationship_type="contains",
    )
    assert (
        get_object(
            _Session({generic_instance: [instance]}),
            ObjectSelector(machine_uuid=instance.machine_uuid),
        )["record_type"]
        == "instance"
    )
    assert (
        get_object(
            _Session({generic_template: [template]}),
            ObjectSelector(uid=7, record_type="template"),
        )["validator_ref"]
        == "UNIVERSAL_PASS@1"
    )
    lineage_payload = object_payload(lineage, "lineage")
    assert lineage_payload["relationship_type"] == "contains"
    assert lineage_payload["created_dt"]
    assert resolve_object(
        _Session(
            {
                generic_template: [],
                generic_instance: [],
                generic_instance_lineage: [lineage],
            }
        ),
        ObjectSelector(euid=lineage.euid),
    ) == (lineage, "lineage")

    instance.is_deleted = True
    with pytest.raises(LookupError, match="soft-deleted"):
        get_object(
            _Session({generic_instance: [instance]}),
            ObjectSelector(machine_uuid=instance.machine_uuid),
        )
    assert (
        get_object(
            _Session({generic_instance: [instance]}),
            ObjectSelector(machine_uuid=instance.machine_uuid),
            include_deleted=True,
        )["is_deleted"]
        is True
    )


@pytest.mark.parametrize(
    ("selector", "message"),
    [
        (ObjectSelector(), "exactly one"),
        (ObjectSelector(euid="one", uid=1, record_type="instance"), "exactly one"),
        (ObjectSelector(uid=1), "record_type is required"),
        (ObjectSelector(uid=0, record_type="instance"), "positive integer"),
        (ObjectSelector(euid="one", record_type="other"), "record_type must"),
        (
            ObjectSelector(machine_uuid=str(uuid4()), record_type="template"),
            "machine_uuid selects only",
        ),
        (ObjectSelector(machine_uuid="not-a-uuid"), "badly formed"),
    ],
)
def test_selector_validation_fails_loudly(selector, message):
    with pytest.raises((ValueError, AttributeError), match=message):
        selector.validated()


def test_object_resolution_and_mutation_error_paths(monkeypatch):
    template = _template()
    instance = _instance()
    with pytest.raises(RuntimeError, match="ambiguous"):
        resolve_object(
            _Session(
                {
                    generic_template: [template],
                    generic_instance: [instance],
                    generic_instance_lineage: [],
                }
            ),
            ObjectSelector(euid="same-selector"),
        )
    with pytest.raises(LookupError, match="not found"):
        resolve_object(
            _Session(
                {
                    generic_template: [],
                    generic_instance: [],
                    generic_instance_lineage: [],
                }
            ),
            ObjectSelector(euid="missing-object"),
        )
    with pytest.raises(PermissionError, match="read-only"):
        update_object(
            _Session({generic_template: [template]}),
            ObjectSelector(euid=template.euid, record_type="template"),
            {"name": "No"},
            actor="operator@example.test",
        )

    session = _Session({generic_instance: [instance]})
    selector = ObjectSelector(euid=instance.euid, record_type="instance")
    for changes, message in (
        ({}, "at least one"),
        ({"name": ""}, "non-empty"),
        ({"json_addl": []}, "JSON object"),
    ):
        with pytest.raises(ValueError, match=message):
            update_object(session, selector, changes, actor="operator@example.test")
    instance.is_deleted = True
    with pytest.raises(ValueError, match="soft-deleted"):
        update_object(
            session,
            selector,
            {"name": "No"},
            actor="operator@example.test",
        )
    with pytest.raises(ValueError, match="already soft-deleted"):
        soft_delete_object(session, selector, actor="operator@example.test")

    instance.is_deleted = False
    repair_calls = []
    monkeypatch.setattr(
        "daylily_tapdb.services.object_operations.create_repair_record",
        lambda *_args, **kwargs: repair_calls.append(kwargs) or {"evidence": "created"},
    )
    preview = repair_object(
        session,
        selector,
        domain_code="Z",
        actor="operator@example.test",
        reason="repair evidence",
        repair_payload={"field": "name"},
        dry_run=True,
    )
    assert preview["changes"]["subject_mutated"] is False
    assert repair_calls == []
    applied = repair_object(
        session,
        selector,
        domain_code="Z",
        actor="operator@example.test",
        reason="repair evidence",
        repair_payload={"field": "name"},
        dry_run=False,
    )
    assert applied["changes"]["repair_record"] == {"evidence": "created"}
    assert repair_calls[0]["subject_euid"] == instance.euid
    with pytest.raises(ValueError, match="reason is required"):
        repair_object(
            session,
            selector,
            domain_code="Z",
            actor="operator@example.test",
            reason="",
            repair_payload={},
        )
    with pytest.raises(ValueError, match="JSON object"):
        repair_object(
            session,
            selector,
            domain_code="Z",
            actor="operator@example.test",
            reason="reason",
            repair_payload=[],
        )


class _Query:
    def __init__(self, rows, calls):
        self.rows = list(rows)
        self.calls = calls

    def filter(self, *criteria):
        self.calls.extend(criteria)
        return self

    def order_by(self, *criteria):
        self.calls.extend(criteria)
        return self

    def limit(self, limit):
        return _Query(self.rows[: int(limit)], self.calls)

    def all(self):
        return self.rows


class _SearchSession:
    def __init__(self, mapping):
        self.mapping = mapping
        self.calls = []

    def query(self, model):
        return _Query(self.mapping.get(model, []), self.calls)


def test_search_uses_sql_predicates_and_opaque_keyset_cursor():
    rows = [_instance(), SimpleNamespace(**{**_instance().__dict__, "uid": 12})]
    session = _SearchSession({generic_instance: rows})
    page = search_objects(
        session,
        service_name="example-service",
        record_type="instance",
        category="assay",
        q="run",
        limit=1,
    )
    assert len(session.calls) >= 4
    assert page["page"]["returned"] == 1
    assert page["page"]["next_cursor"]
    assert "uid" not in page["page"]["next_cursor"]
    with pytest.raises(ValueError, match="cursor is malformed"):
        search_objects(session, service_name="example-service", cursor="not-json")


def test_search_covers_all_sql_filters_record_types_and_cursor_guards():
    template = _template()
    template.uid = 1
    template.modified_dt = datetime.now(UTC)
    instance = _instance()
    instance.uid = 2
    lineage = SimpleNamespace(
        uid=3,
        euid="stored-lineage-one",
        name="contains",
        category="lineage",
        type="lineage",
        subtype="generic",
        version="1.0",
        bstatus="active",
        tenant_id=None,
        relationship_type="contains",
        is_deleted=False,
        created_dt=None,
        modified_dt=None,
    )
    session = _SearchSession(
        {
            generic_template: [template],
            generic_instance: [instance],
            generic_instance_lineage: [lineage],
        }
    )
    page = search_objects(
        session,
        service_name="example-service",
        q="record",
        euid="exact-object",
        category="assay",
        type_name="sequencing",
        subtype="short_read",
        tenant_id="tenant-one",
        relationship_type="contains",
        limit=10,
    )
    assert {item["record_type"] for item in page["items"]} == {
        "template",
        "instance",
        "lineage",
    }
    assert page["items"][0]["modified_dt"]
    assert page["items"][2]["relationship_type"] == "contains"
    assert len(session.calls) >= 25

    template_cursor = search_objects(
        _SearchSession(
            {generic_template: [template, _template("stored-template-two")]}
        ),
        service_name="example-service",
        record_type="template",
        limit=1,
    )["page"]["next_cursor"]
    with pytest.raises(ValueError, match="selected record_type"):
        search_objects(
            session,
            service_name="example-service",
            record_type="instance",
            cursor=template_cursor,
        )
    invalid_kind = base64.urlsafe_b64encode(
        json.dumps({"kind": "unknown", "uid": 1}).encode()
    ).decode()
    with pytest.raises(ValueError, match="cursor is malformed"):
        search_objects(
            session,
            service_name="example-service",
            cursor=invalid_kind,
        )
    with pytest.raises(ValueError, match="record_type must"):
        search_objects(
            session,
            service_name="example-service",
            record_type="unknown",
        )


def test_runtime_payload_is_shared_shape_and_never_exposes_secrets(tmp_path: Path):
    config = tmp_path / "tapdb-config.yaml"
    config.write_text(
        "meta:\n"
        "  config_version: 4\n"
        "  client_id: example-service\n"
        "  database_name: example-db\n"
        "backup:\n"
        "  storage:\n"
        "    uri: s3://user:hunter2@example-bucket/prefix?token=secret-token\n",
        encoding="utf-8",
    )
    cfg = {
        "client_id": "example-service",
        "database_name": "example-db",
        "engine_type": "local",
        "host": "localhost",
        "port": "5432",
        "user": "tapdb",
        "password": "hunter2",
        "database": "tapdb_example",
        "schema_name": "tapdb_example",
        "domain_code": "Z",
        "owner_repo_name": "example-owner",
        "ui_port": "8911",
        "aws_profile": "example-profile",
        "region": "us-west-2",
    }
    payload = build_runtime_info(
        config_path=config,
        probe_database=False,
        resolved_config=cfg,
    )
    rendered = json.dumps(payload, sort_keys=True)
    assert payload["format"] == "tapdb.runtime-info/v1"
    assert payload["package"]["version"]
    assert payload["python"]["version"]
    assert payload["meridian"]["version"] == "0.4.8"
    assert set(payload) == {
        "format",
        "package",
        "python",
        "meridian",
        "git",
        "config",
        "database",
        "scope",
        "storage",
        "ui",
        "dag",
    }
    assert "hunter2" not in rendered
    assert "secret-token" not in rendered


def test_runtime_helpers_report_unknowns_and_strip_sensitive_uri_parts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    assert runtime_info._safe_uri("") is None
    assert runtime_info._safe_uri("relative/path") is None
    assert (
        runtime_info._safe_uri("s3://user:password@bucket/prefix?token=value")
        == "s3://bucket/prefix"
    )
    assert runtime_info._bucket("s3://bucket/prefix") == "bucket"
    assert runtime_info._bucket("file:///tmp/data") is None

    pid_file = tmp_path / "ui.pid"
    pid_file.write_text("not-a-pid", encoding="utf-8")
    assert runtime_info._ui_pid(pid_file) == (None, False)
    pid_file.write_text("123", encoding="utf-8")
    monkeypatch.setattr(runtime_info.os, "kill", lambda *_args: None)
    assert runtime_info._ui_pid(pid_file) == (123, True)

    monkeypatch.setattr(runtime_info.shutil, "which", lambda _name: None)
    assert runtime_info._database_probe({}) == ("unknown", "psql is not installed")

    class _Result:
        returncode = 0
        stdout = "17.6\n"

    monkeypatch.setattr(runtime_info.shutil, "which", lambda _name: "/usr/bin/psql")
    monkeypatch.setattr(
        runtime_info.subprocess, "run", lambda *_args, **_kwargs: _Result()
    )
    cfg = {
        "host": "localhost",
        "port": "5432",
        "user": "tapdb",
        "database": "tapdb_test",
        "password": "not-rendered",
    }
    assert runtime_info._database_probe(cfg) == ("ok", "17.6")
    _Result.returncode = 2
    assert runtime_info._database_probe(cfg) == ("error", "psql exit 2")
    monkeypatch.setattr(
        runtime_info.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(TimeoutError()),
    )
    assert runtime_info._database_probe(cfg) == ("error", "TimeoutError")


def test_runtime_payload_reports_configured_status_and_clean_git_checkout(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    config = tmp_path / "tapdb-config.yaml"
    config.write_text(
        "meta:\n"
        "  config_version: 4\n"
        "backup:\n"
        "  storage:\n"
        "    uri: s3://bucket/backup\n"
        "    receipt_uri: s3://other/receipts?sig=hidden\n"
        "dag_v2:\n"
        "  service_id: example-service\n",
        encoding="utf-8",
    )

    def _git(_repo, *args):
        values = {
            ("describe", "--tags", "--exact-match", "HEAD"): None,
            ("rev-parse", "HEAD"): "abc123",
            ("branch", "--show-current"): "test-branch",
            ("status", "--porcelain"): "",
        }
        return values[args]

    monkeypatch.setattr(runtime_info, "_git", _git)
    monkeypatch.setattr(runtime_info, "_package_version", lambda _name: None)
    monkeypatch.setattr(runtime_info, "_database_probe", lambda _cfg: ("ok", "17.6"))
    monkeypatch.setattr(runtime_info, "_ui_pid", lambda _path: (321, True))
    payload = build_runtime_info(
        config_path=config,
        resolved_config={
            "client_id": "example-service",
            "database_name": "example-db",
            "host": "localhost",
            "port": "5432",
            "user": "tapdb",
            "database": "tapdb_test",
            "schema_name": "tapdb_test",
            "domain_code": "Z",
            "owner_repo_name": "example-owner",
        },
    )
    assert payload["git"]["dirty"] is False
    assert payload["database"] == {
        "engine_type": None,
        "host": "localhost",
        "port": "5432",
        "database": "tapdb_test",
        "schema_name": "tapdb_test",
        "status": "ok",
        "server_version": "17.6",
    }
    assert payload["storage"]["s3_buckets"] == ["bucket", "other"]
    assert payload["ui"]["status"] == "running"
    assert payload["dag"] == {
        "status": "configured",
        "service_id": "example-service",
        "eligible": True,
    }

    invalid = tmp_path / "invalid.yaml"
    invalid.write_text("- not\n- a\n- mapping\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="root must be a mapping"):
        build_runtime_info(
            config_path=invalid,
            probe_database=False,
            resolved_config={},
        )


def test_cli_groups_and_legacy_read_routes_are_hard_wired_to_authentication():
    assert app is not None
    group_names = {group.name for group in app.registered_groups}
    assert {"templates", "objects"}.issubset(group_names)

    tree = ast.parse(Path("admin/main.py").read_text(encoding="utf-8"))
    expected = {
        "get_graph_data",
        "api_list_templates",
        "api_list_instances",
        "api_get_object",
    }
    found = {}
    for node in tree.body:
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name in expected
        ):
            decorators = {
                decorator.id
                for decorator in node.decorator_list
                if isinstance(decorator, ast.Name)
            }
            found[node.name] = decorators
    assert found.keys() == expected
    assert all("require_auth" in decorators for decorators in found.values())


def test_template_cli_commands_share_repository_services(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    emitted = []
    calls = []
    cfg = {
        "domain_code": "Z",
        "owner_repo_name": "example-owner",
        "domain_registry_path": str(tmp_path / "domains.json"),
        "prefix_ownership_registry_path": str(tmp_path / "prefixes.json"),
    }
    conn = _CliConn(object())
    monkeypatch.setattr(templates_cli, "get_db_config", lambda: cfg)
    monkeypatch.setattr(templates_cli, "_dry_run_requested", lambda: False)
    monkeypatch.setattr(
        templates_cli,
        "_tapdb_connection_for_env",
        lambda _env, app_username: calls.append(("actor", app_username)) or conn,
    )
    monkeypatch.setattr(templates_cli, "_emit", emitted.append)
    monkeypatch.setattr(
        templates_cli,
        "export_repository_pack",
        lambda *_args, **kwargs: (
            calls.append(("export", kwargs)) or {"operation": "export"}
        ),
    )
    monkeypatch.setattr(
        templates_cli,
        "import_repository_pack",
        lambda *_args, **kwargs: (
            calls.append(("import", kwargs))
            or repository.RepositoryImportResult(
                dry_run=kwargs["dry_run"],
                template_count=1,
                inserted=1,
                skipped=0,
                prefixes_validated=("ASY",),
                checksum_sha256="checksum",
            )
        ),
    )
    monkeypatch.setattr(
        templates_cli,
        "repository_inventory",
        lambda *_args, **kwargs: (
            calls.append(("inventory", kwargs)) or {"status": "ok"}
        ),
    )
    pack = tmp_path / "pack.json"
    templates_cli.templates_export(pack, euid="stored-template-one", actor="operator")
    templates_cli.templates_import(pack, apply=True, actor="operator")
    templates_cli.templates_inventory(pack, actor="operator")
    assert emitted == [
        {"operation": "export"},
        {
            "dry_run": False,
            "template_count": 1,
            "inserted": 1,
            "skipped": 0,
            "prefixes_validated": ("ASY",),
            "checksum_sha256": "checksum",
        },
        {"status": "ok"},
    ]
    assert next(value for name, value in calls if name == "import")["dry_run"] is False


def test_template_import_cli_accepts_explicit_dry_run_and_documents_both_modes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[dict] = []
    cfg = {
        "domain_code": "Z",
        "owner_repo_name": "example-owner",
        "domain_registry_path": str(tmp_path / "domains.json"),
        "prefix_ownership_registry_path": str(tmp_path / "prefixes.json"),
    }
    monkeypatch.setattr(templates_cli, "get_db_config", lambda: cfg)
    monkeypatch.setattr(
        templates_cli,
        "_tapdb_connection_for_env",
        lambda _env, app_username: _CliConn(object()),
    )
    monkeypatch.setattr(
        templates_cli,
        "import_repository_pack",
        lambda *_args, **kwargs: (
            calls.append(kwargs)
            or repository.RepositoryImportResult(
                dry_run=kwargs["dry_run"],
                template_count=1,
                inserted=0,
                skipped=1,
                prefixes_validated=("ASY",),
                checksum_sha256="checksum",
            )
        ),
    )
    runner = CliRunner()
    pack = tmp_path / "pack.json"

    result = runner.invoke(
        app,
        [
            "templates",
            "import",
            "--repository-pack",
            str(pack),
            "--dry-run",
            "--actor",
            "operator",
        ],
    )
    help_result = runner.invoke(app, ["templates", "import", "--help"])

    assert result.exit_code == 0, result.output
    assert calls[0]["dry_run"] is True
    assert json.loads(result.output)["dry_run"] is True
    assert help_result.exit_code == 0
    help_text = unstyle(help_result.output)
    assert "--apply" in help_text
    assert "--dry-run" in help_text


def test_object_cli_commands_use_exact_selectors_and_dry_run_defaults(monkeypatch):
    emitted = []
    calls = []
    conn = _CliConn(object())
    monkeypatch.setattr(objects_cli, "_emit", emitted.append)
    monkeypatch.setattr(objects_cli, "_dry_run_requested", lambda: False)
    monkeypatch.setattr(objects_cli, "_connection", lambda _actor: conn)
    monkeypatch.setattr(
        objects_cli,
        "get_db_config",
        lambda: {"client_id": "example-service", "domain_code": "Z"},
    )

    def _record(name):
        def _call(*_args, **kwargs):
            calls.append((name, kwargs))
            return {"operation": name, "dry_run": kwargs.get("dry_run")}

        return _call

    monkeypatch.setattr(objects_cli, "search_objects", _record("search"))
    monkeypatch.setattr(objects_cli, "get_object", _record("get"))
    monkeypatch.setattr(objects_cli, "update_object", _record("update"))
    monkeypatch.setattr(objects_cli, "repair_object", _record("repair"))
    monkeypatch.setattr(objects_cli, "soft_delete_object", _record("delete"))

    objects_cli.objects_search(
        q="run",
        euid="",
        record_type="instance",
        category="assay",
        type_name="sequencing",
        subtype="short_read",
        tenant_id="",
        relationship_type="",
        limit=10,
        cursor="",
        actor="operator",
    )
    selector_args = {
        "euid": "stored-instance-one",
        "machine_uuid": "",
        "uid": None,
        "record_type": "instance",
        "actor": "operator",
    }
    objects_cli.objects_get(**selector_args, include_deleted=True)
    objects_cli.objects_update(
        **selector_args,
        set_values=["name=Run two", 'json_addl={"lane":2}'],
        apply=False,
    )
    objects_cli.objects_repair(
        **selector_args,
        reason="repair evidence",
        repair_json='{"field":"name"}',
        apply=True,
    )
    objects_cli.objects_delete(**selector_args, apply=False)
    assert [item["operation"] for item in emitted] == [
        "search",
        "get",
        "update",
        "repair",
        "delete",
    ]
    assert dict(calls)["update"]["dry_run"] is True
    assert dict(calls)["repair"]["dry_run"] is False
    assert dict(calls)["delete"]["dry_run"] is True
    assert objects_cli._changes(["name=Run two", "bstatus=active"]) == {
        "name": "Run two",
        "bstatus": "active",
    }
    with pytest.raises(Exception, match="FIELD=JSON_VALUE"):
        objects_cli._changes(["missing-separator"])
    with pytest.raises(Exception, match="field may not be blank"):
        objects_cli._changes([" =value"])
    with pytest.raises(Exception, match="valid JSON"):
        objects_cli.objects_repair(
            **selector_args,
            reason="repair",
            repair_json="{",
            apply=False,
        )
    with pytest.raises(Exception, match="JSON object"):
        objects_cli.objects_repair(
            **selector_args,
            reason="repair",
            repair_json="[]",
            apply=False,
        )
