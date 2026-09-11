"""Bounded historical-template preservation and native versioned-XRF proof."""

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from sqlalchemy import text
from sqlalchemy.orm import Session

from daylily_tapdb import (
    InstanceFactory,
    TemplateManager,
    generic_instance,
    generic_template,
)
from daylily_tapdb.external_references import (
    ExternalLinkSpec,
    ExternalReferenceContractError,
    ExternalReferenceService,
    TapDBObjectTarget,
    _target_from_reference,
)
from daylily_tapdb.runtime_principal import operator_connection
from daylily_tapdb.security_context import apply_transaction_context
from daylily_tapdb.services.graph_payloads import build_graph_v2_payload
from daylily_tapdb.services.object_search import search_external_reference_sources
from daylily_tapdb.templates import (
    find_tapdb_core_config_dir,
    seed_templates,
    validate_template_configs,
)
from daylily_tapdb.templates.mutation import allow_template_mutations
from tests.test_runtime_principal_pg import _context, _install_schema

pytest_plugins = ["tests.test_runtime_principal_pg"]


@pytest.mark.parametrize("deleted", [False, True])
def test_additive_xrf_version_preserves_history_and_public_paths(
    principal_database, pg_instance, deleted
):
    cfg = principal_database
    _install_schema(cfg)
    meta = yaml.safe_load(pg_instance["config_path"].read_text())["meta"]
    core = find_tapdb_core_config_dir()
    templates, issues = validate_template_configs([core], strict=True)
    assert not [issue for issue in issues if issue.level == "error"]
    templates = [item for item in templates if item["category"] == "reference"]
    historical = json.loads(
        (Path(__file__).parent / "fixtures/external_reference_9_0_10.json").read_text()
    )["templates"][0]
    context = replace(_context(cfg), tenant_id=None, allow_global_rows=True)
    with operator_connection(cfg, isolation_level="REPEATABLE READ") as connection:
        apply_transaction_context(connection, context, assert_runtime_role=False)
        connection.execute(text("SET LOCAL TIME ZONE 'UTC'"))
        with Session(bind=connection) as session:
            # Fixture only: reproduce the persisted 9.0.10 definition and binding.
            # Every UID/EUID is minted by the installed native allocators.
            connection.execute(text("CREATE SEQUENCE xrf_instance_seq START 10000"))
            with allow_template_mutations():
                old_template = generic_template(**historical, is_deleted=False)
                session.add(old_template)
                session.flush()
            old_instance = generic_instance(
                name="historical reference",
                template_uid=old_template.uid,
                category=historical["category"],
                type=historical["type"],
                subtype=historical["subtype"],
                version=historical["version"],
                bstatus="active",
                is_singleton=False,
                is_deleted=deleted,
                json_addl=historical["json_addl"],
            )
            session.add(old_instance)
            session.flush()
            if deleted:
                with allow_template_mutations():
                    old_template.is_deleted = True
                    old_template.bstatus = "retired"
                    session.flush()

            def originals():
                return [
                    connection.execute(
                        text(
                            "SELECT to_jsonb(t) FROM generic_template t WHERE uid=:uid"
                        ),
                        {"uid": old_template.uid},
                    ).scalar_one(),
                    connection.execute(
                        text(
                            "SELECT to_jsonb(t) FROM generic_instance t WHERE uid=:uid"
                        ),
                        {"uid": old_instance.uid},
                    ).scalar_one(),
                    connection.execute(
                        text(
                            "SELECT to_jsonb(t) FROM audit_log t WHERE rel_table_name IN "
                            "('generic_template', 'generic_instance') AND "
                            "((rel_table_name='generic_template' AND rel_table_uid_fk=:template) "
                            "OR (rel_table_name='generic_instance' AND rel_table_uid_fk=:instance)) "
                            "ORDER BY uid"
                        ),
                        {"template": old_template.uid, "instance": old_instance.uid},
                    )
                    .scalars()
                    .all(),
                ]

            before = originals()
            floor = connection.execute(
                text("SELECT last_value FROM xrf_instance_seq")
            ).scalar_one()
            seeded = seed_templates(
                session,
                templates,
                overwrite=False,
                core_config_dir=core,
                domain_code=cfg["domain_code"],
                owner_repo_name=cfg["owner_repo_name"],
                domain_registry_path=meta["domain_registry_path"],
                prefix_registry_path=meta["prefix_ownership_registry_path"],
            )
            assert seeded.inserted == 2 and seeded.updated == 0
            assert originals() == before
            with pytest.raises(ExternalReferenceContractError, match="not a core XRF"):
                _target_from_reference(old_instance)

            apply_transaction_context(
                connection,
                replace(_context(cfg), allow_global_rows=True),
                assert_runtime_role=False,
            )
            factory = InstanceFactory(TemplateManager(), domain_code=cfg["domain_code"])
            source = factory.create_instance(
                session, "set/scope-0/generic/1.0/", "new source", create_children=False
            )
            remote = factory.create_instance(
                session,
                "set/scope-0/generic/1.0/",
                "persisted target",
                create_children=False,
            )
            target = TapDBObjectTarget("remote-catalog", remote.euid)
            spec = ExternalLinkSpec(
                target,
                "references",
                "test.versioning",
                datetime.now(UTC),
                "test:versioned-xrf",
            )
            service = ExternalReferenceService(session)
            created = service.attach(source, spec)
            declared_version = next(
                t["version"] for t in templates if t["subtype"] == "tapdb_object"
            )
            assert (
                created.status == "created"
                and created.reference.version == declared_version
            )
            assert created.reference.parent_template.version == declared_version
            assert declared_version != historical["version"]
            assert created.reference.euid_seq > floor
            assert _target_from_reference(created.reference) == target
            replay = service.attach(source, spec)
            assert (
                replay.status == "existing"
                and replay.reference.uid == created.reference.uid
            )
            assert replay.lineage.uid == created.lineage.uid
            assert service.list_for_source(source)["page"]["returned"] == 1
            assert (
                search_external_reference_sources(
                    session,
                    service_name="test",
                    external_service_id="remote-catalog",
                    external_object_euid=remote.euid,
                )["page"]["returned"]
                == 1
            )
            graph = build_graph_v2_payload(
                source, record_type="instance", service_id="test", depth=1, max_nodes=10
            )
            assert (
                len(graph["elements"]["nodes"]) == 2
                and len(graph["elements"]["edges"]) == 1
            )
            assert originals() == before


def test_loader_refuses_semantic_overwrite_and_preserves_retirement():
    from types import SimpleNamespace
    from unittest.mock import Mock

    from daylily_tapdb.templates.loader import _upsert_template

    definition = {
        "name": "Immutable definition",
        "polymorphic_discriminator": "generic_template",
        "category": "content",
        "type": "record",
        "subtype": "test",
        "version": "1.0",
        "instance_prefix": "TST",
        "is_singleton": False,
        "json_addl": {"properties": {}},
    }
    stored = SimpleNamespace(
        **definition,
        domain_code="Z",
        instance_polymorphic_identity=None,
        validator_ref="UNIVERSAL_PASS@1",
        json_addl_schema=None,
        bstatus="retired",
        is_deleted=True,
    )
    session = Mock()
    session.execute.return_value.scalar_one_or_none.return_value = stored
    outcome, same = _upsert_template(
        session, definition, domain_code="Z", owner_repo_name="test", overwrite=True
    )
    assert (
        outcome == "skipped"
        and same is stored
        and stored.is_deleted
        and stored.bstatus == "retired"
    )
    with pytest.raises(ValueError, match="publish a new template version"):
        _upsert_template(
            session,
            {**definition, "json_addl": {"properties": {"new": 1}}},
            domain_code="Z",
            owner_repo_name="test",
            overwrite=True,
        )
    assert stored.json_addl == definition["json_addl"]
    session.flush.assert_not_called()


def test_template_version_is_only_a_declared_identity_coordinate(tmp_path, monkeypatch):
    import daylily_tapdb.external_references as refs

    directory = tmp_path / "core_config" / "system"
    directory.mkdir(parents=True)
    path = directory / "external_reference.json"
    path.write_text(
        json.dumps(
            {
                "templates": [
                    {
                        "category": "reference",
                        "type": "external_identifier",
                        "subtype": "tapdb_object",
                        "version": 7.25,
                    }
                ]
            }
        )
    )
    monkeypatch.setattr(refs, "files", lambda package: tmp_path)
    assert refs._packaged_reference_coordinates(
        "external_reference.json", "tapdb_object"
    ) == ("reference", "external_identifier", "tapdb_object", "7.25")
