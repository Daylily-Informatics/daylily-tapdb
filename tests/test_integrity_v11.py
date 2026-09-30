"""Focused TapDB 11 qualification; isolated native PostgreSQL, never production."""

from __future__ import annotations

from uuid import uuid4
import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from typer.testing import CliRunner

from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import set_cli_context, clear_cli_context
from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.security_context import Attribution
from daylily_tapdb.audit import query_audit_trail
from daylily_tapdb.factory import InstanceFactory
from daylily_tapdb.templates.manager import TemplateManager


def context(subject="person-not-a-uuid"):
    return Attribution(
        "human", "qualification", subject, "tapdb-test", str(uuid4()), str(uuid4())
    )


def connection(pg, attribution=True):
    return TAPDBConnection(
        db_url=pg["dsn"],
        db_user=pg["user"],
        app_username="display-only",
        domain_code="Z",
        owner_repo_name="daylily-tapdb",
        schema_name=pg["schema_name"],
        engine_type="local",
        allow_global_rows=True,
        config_identity=str(pg["config_path"]),
        attribution=context() if attribution is True else attribution or None,
    )


@pytest.fixture(scope="module")
def db(pg_instance):
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        config_path=pg_instance["config_path"],
    )
    runner = CliRunner()
    for args in [["db", "schema", "apply"], ["db", "data", "seed"]]:
        result = runner.invoke(app, args)
        assert result.exit_code == 0, result.output + repr(result.exception)
    yield pg_instance
    from admin.db_metrics import stop_all_writers

    stop_all_writers()
    clear_cli_context()


def create(session, name="qualification record"):
    return InstanceFactory(TemplateManager(), domain_code="Z").create_instance(
        session,
        template_code="message/webhook/event/1.0/",
        name=name,
    )


def test_full_revision_and_attribution(db):
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        obj = create(s)
        s.flush()
        s.refresh(obj)
        assert obj.record_revision == 1
        entries = query_audit_trail(s, euid=obj.euid)
        assert len(entries) == 1
        assert entries[0].euid == obj.euid != entries[0].audit_euid
        assert entries[0].record_table == "generic_instance"
        assert entries[0].before_state is None
        assert entries[0].after_state["name"] == obj.name
        assert entries[0].attribution["actor_subject"] == "person-not-a-uuid"
        assert entries[0].attribution["database_principal"] == db["user"]
        obj.name = "changed"
        s.flush()
        s.refresh(obj)
        assert obj.record_revision == 2
        rows = query_audit_trail(s, euid=obj.euid, order="asc")
        assert [r.name for r in rows] == ["qualification record", "changed"]
        assert all(r.current_context["name"] == "changed" for r in rows)
        assert all(r.current_context["euid"] == obj.euid for r in rows)


@pytest.mark.parametrize(
    "command",
    [
        "UPDATE audit_log SET is_deleted=true",
        "DELETE FROM audit_log",
        "TRUNCATE audit_log",
        "INSERT INTO audit_log (rel_table_name) VALUES ('generic_instance')",
    ],
)
def test_runtime_cannot_write_audit(db, command):
    with connection(db) as conn:
        with pytest.raises(DBAPIError):
            with conn.session_scope(commit=True) as s:
                s.execute(text(command))


def test_missing_attribution_fails_domain_write(db):
    with connection(db, attribution=False) as conn:
        with pytest.raises(DBAPIError, match="contract mismatch"):
            with conn.session_scope(commit=True) as s:
                create(s)
                s.flush()


def test_history_boundaries_and_rollback(db):
    from daylily_tapdb.history import HistoryService
    from daylily_tapdb.models.instance import generic_instance

    with connection(db) as conn, conn.session_scope(commit=True) as s:
        obj = create(s, "before boundary")
        identity = obj.euid
    with connection(db) as conn, conn.session_scope() as s:
        boundary = HistoryService(s).capture_boundary()
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        obj = s.query(generic_instance).filter_by(euid=identity).one()
        obj.name = "after boundary"
    with connection(db) as conn, conn.session_scope() as s:
        history = HistoryService(s)
        assert (
            history.object_at(identity, record_type="instance", boundary=boundary)[
                "state"
            ]["name"]
            == "before boundary"
        )
        assert (
            history.object_at(identity, record_type="instance", revision=2)["state"][
                "name"
            ]
            == "after boundary"
        )
        obj = s.query(generic_instance).filter_by(euid=identity).one()
        obj.name = "will rollback"
        s.flush()
    with connection(db) as conn, conn.session_scope() as s:
        assert len(query_audit_trail(s, euid=identity)) == 2


def test_stale_correction_and_durable_retry(db):
    from dataclasses import replace
    from daylily_tapdb.history import HistoryService, HistoryError
    from daylily_tapdb.models.instance import generic_instance

    attr = replace(context(), correction_reason="Qualification correction")
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        obj = create(s, "original")
        identity = obj.euid
    with connection(db) as conn, conn.session_scope() as s:
        plan = HistoryService(s).plan_correction(
            [
                {
                    "euid": identity,
                    "record_type": "instance",
                    "fields": {"name": "corrected"},
                }
            ],
            operation_id=attr.operation_id,
            reason=attr.correction_reason,
        )
    with connection(db, attr) as conn:
        with pytest.raises(HistoryError, match="owner validator"):
            with conn.session_scope(commit=True) as s:
                HistoryService(s).apply_correction(plan)
    validators = {"daylily-tapdb": lambda session, obj, fields: None}
    with connection(db, attr) as conn, conn.session_scope(commit=True) as s:
        receipt = HistoryService(s, owner_validators=validators).apply_correction(plan)
        assert receipt["replayed"] is False
    with connection(db, attr) as conn, conn.session_scope(commit=True) as s:
        replay = HistoryService(s, owner_validators=validators).apply_correction(plan)
        assert replay["euid"] == receipt["euid"] and replay["replayed"] is True
        assert len(query_audit_trail(s, euid=identity)) == 2


def test_independent_reference_and_annotation(db):
    from daylily_tapdb.external_references import (
        ExternalReferenceService,
        ExternalIdentifierTarget,
    )

    target = ExternalIdentifierTarget(
        namespace="qualification",
        kind="tube",
        value=str(uuid4()),
        scope="public_global",
    )
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        service = ExternalReferenceService(s)
        assert service.resolve(target) is None
        ref = service.register(target)
        assert service.register(target).euid == ref.euid
        assert service.resolve(target).euid == ref.euid
        annotation = service.annotate(
            ref, authority="tapdb-test", key="display", data={"name": "external tube"}
        )
        s.refresh(annotation)
        revision = annotation.record_revision
        assert annotation.euid != ref.euid
        same = service.annotate(
            ref,
            authority="tapdb-test",
            key="display",
            data={"name": "renamed"},
            expected_revision=revision,
        )
        assert same.euid == annotation.euid
        assert service.resolve(target).euid == ref.euid


def test_boundary_excludes_later_commit_even_older_timestamp(db):
    from daylily_tapdb.history import HistoryService
    from daylily_tapdb.models.instance import generic_instance

    with connection(db) as conn, conn.session_scope(commit=True) as s:
        identity = create(s, "committed first").euid
    with connection(db) as first, first.session_scope(commit=True) as pending:
        obj = pending.query(generic_instance).filter_by(euid=identity).one()
        obj.name = "pending at boundary"
        pending.flush()
        with connection(db) as second, second.session_scope() as s:
            token = HistoryService(s).capture_boundary()
    with connection(db) as conn, conn.session_scope() as s:
        assert (
            HistoryService(s).object_at(
                identity, record_type="instance", boundary=token
            )["state"]["name"]
            == "committed first"
        )


def test_savepoint_abort_and_pooled_context(db):
    with connection(db) as conn:
        with conn.session_scope(commit=True) as s:
            kept = create(s, "kept")
            with pytest.raises(RuntimeError):
                with s.begin_nested():
                    discarded = create(s, "discarded")
                    lost_id = discarded.euid
                    raise RuntimeError("abort savepoint")
            assert query_audit_trail(s, euid=lost_id) == []
            assert len(query_audit_trail(s, euid=kept.euid)) == 1
        conn.attribution = None
        with pytest.raises(DBAPIError, match="contract mismatch"):
            with conn.session_scope(commit=True) as s:
                create(s, "must not inherit prior actor")


def test_soft_delete_reactivation_and_noop(db):
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        obj = create(s)
        s.execute(
            text("UPDATE generic_instance SET name=name WHERE uid=:uid"),
            {"uid": obj.uid},
        )
        assert len(query_audit_trail(s, euid=obj.euid)) == 1
        s.execute(text("DELETE FROM generic_instance WHERE uid=:uid"), {"uid": obj.uid})
        s.refresh(obj)
        assert obj.is_deleted and obj.record_revision == 2
        obj.is_deleted = False
        s.flush()
        rows = query_audit_trail(s, euid=obj.euid, order="asc")
        assert [r.after_state["is_deleted"] for r in rows] == [False, True, False]


def test_uid_collisions_resolve_correct_table(db):
    with connection(db) as conn, conn.session_scope() as s:
        pair = s.execute(
            text(
                "SELECT t.euid,i.euid FROM generic_template t JOIN generic_instance i ON i.uid=t.uid LIMIT 1"
            )
        ).one()
        for euid, kind in zip(
            pair, ["generic_template", "generic_instance"], strict=True
        ):
            rows = query_audit_trail(s, euid=euid)
            assert rows and all(
                r.record_table == kind and r.current_context["euid"] == euid
                for r in rows
            )


def test_api_cli_python_read_parity(db):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from daylily_tapdb.gui.integrity import integrity_router
    from daylily_tapdb.web.runtime import _bundles
    from daylily_tapdb.external_references import (
        ExternalReferenceService,
        ExternalIdentifierTarget,
    )

    value = str(uuid4())
    payload = {
        "target_type": "opaque",
        "namespace": "qualification",
        "kind": "tube",
        "value": value,
        "scope": "public_global",
    }
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        ref = ExternalReferenceService(s).register(
            ExternalIdentifierTarget(
                namespace="qualification",
                kind="tube",
                value=value,
                scope="public_global",
            )
        )
        identity = ref.euid
    app_http = FastAPI()

    async def authenticated():
        return {"username": "qualification", "uid": 1, "role": "admin"}

    app_http.include_router(
        integrity_router(
            config_path=str(db["config_path"]),
            require_user=authenticated,
            require_admin=authenticated,
        )
    )
    with TestClient(app_http) as client:
        result = client.post("/api/integrity/read/resolve", json=payload)
        assert result.status_code == 200, result.text
        assert result.json()["reference"]["euid"] == identity
        denied = client.post("/api/integrity/read/register", json=payload)
        assert denied.status_code == 404
    for bundle in _bundles.values():
        bundle.engine.dispose()
    path = db["config_path"].parent / "resolve-reference.json"
    path.write_text(__import__("json").dumps(payload))
    result = CliRunner().invoke(app, ["references", "resolve", "--payload", str(path)])
    assert result.exit_code == 0, result.output
    assert __import__("json").loads(result.output)["reference"]["euid"] == identity


def test_repeated_adoption_is_rejected(db):
    from daylily_tapdb.cli.db_config import get_db_config
    from daylily_tapdb.runtime_principal import operator_session
    from daylily_tapdb.integrity_lifecycle import plan_adoption

    cfg = get_db_config(config_path=str(db["config_path"]))
    with operator_session(cfg) as conn:
        with pytest.raises(ValueError, match="repeated adoption"):
            with conn.begin():
                plan_adoption(conn, cfg)


def test_reference_claims_serialize_and_edges_reject_stale(db):
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime, UTC
    from daylily_tapdb.external_references import (
        ExternalReferenceService,
        ExternalIdentifierTarget,
        ExternalLinkSpec,
    )
    from daylily_tapdb.models.instance import generic_instance
    from daylily_tapdb.revisions import RevisionConflict

    target = ExternalIdentifierTarget(
        namespace="qualification",
        kind="tube",
        value=str(uuid4()),
        scope="public_global",
    )

    def claim(_):
        with connection(db) as conn, conn.session_scope(commit=True) as s:
            return ExternalReferenceService(s).register(target).euid

    with ThreadPoolExecutor(max_workers=2) as pool:
        identities = list(pool.map(claim, [1, 2]))
    assert len(set(identities)) == 1
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        source = create(s)
        source_id = source.euid
        s.refresh(source)
        source_rev = source.record_revision
        spec = ExternalLinkSpec(
            target, "participates", "tapdb-test", datetime.now(UTC), "qualified"
        )
        result = ExternalReferenceService(s).attach(
            source, spec, expected_source_revision=source_rev
        )
        s.refresh(result.lineage)
        lineage_rev = result.lineage.record_revision
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        source = s.query(generic_instance).filter_by(euid=source_id).one()
        result = ExternalReferenceService(s).detach(
            source,
            target,
            expected_source_revision=source_rev,
            expected_lineage_revision=lineage_rev,
            relationship_type="participates",
            assertion_authority="tapdb-test",
            deactivated_at=datetime.now(UTC),
            deactivation_provenance="qualification",
        )
        assert result.status == "deactivated"
    with connection(db) as conn:
        with pytest.raises(RevisionConflict):
            with conn.session_scope(commit=True) as s:
                source = s.query(generic_instance).filter_by(euid=source_id).one()
                ExternalReferenceService(s).attach(
                    source,
                    spec,
                    expected_source_revision=source_rev,
                    expected_lineage_revision=lineage_rev,
                )


def test_historical_graph_keeps_cycle_and_is_local(db):
    from daylily_tapdb.history import HistoryService
    from daylily_tapdb.models.lineage import generic_instance_lineage

    with connection(db) as conn, conn.session_scope(commit=True) as s:
        a = create(s, "cycle a")
        b = create(s, "cycle b")
        identity = a.euid
        for parent, child in [(a, b), (b, a)]:
            s.add(
                generic_instance_lineage(
                    name="cycle",
                    parent_instance_uid=parent.uid,
                    child_instance_uid=child.uid,
                    polymorphic_discriminator="generic_instance_lineage",
                    category="lineage",
                    type="lineage",
                    subtype="generic",
                    version="1.0",
                    bstatus="active",
                    relationship_type="related",
                    is_singleton=False,
                    json_addl={},
                )
            )
    with connection(db) as conn, conn.session_scope() as s:
        history = HistoryService(s)
        token = history.capture_boundary()
        graph = history.graph_at([identity], boundary=token)
        assert len(graph["nodes"]) == 2 and len(graph["edges"]) == 2
        assert graph["complete"] and not graph["external_expansion"]
        small = history.graph_at([identity], boundary=token, max_depth=0)
        assert len(small["nodes"]) == 1 and small["complete"] is False


def test_stale_object_write_fails_without_lost_change(db):
    from daylily_tapdb.services.object_operations import update_object, ObjectSelector
    from daylily_tapdb.revisions import RevisionConflict

    with connection(db) as conn, conn.session_scope(commit=True) as s:
        obj = create(s)
        identity = obj.euid
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        update_object(
            s,
            ObjectSelector(euid=identity),
            {"name": "first"},
            actor="display",
            dry_run=False,
            expected_revision=1,
        )
    with connection(db) as conn:
        with pytest.raises(RevisionConflict):
            with conn.session_scope(commit=True) as s:
                update_object(
                    s,
                    ObjectSelector(euid=identity),
                    {"name": "stale overwrite"},
                    actor="display",
                    dry_run=False,
                    expected_revision=1,
                )
        with conn.session_scope() as s:
            assert query_audit_trail(s, euid=identity)[0].after_state["name"] == "first"


def test_real_backup_restore_preserves_history_and_changes_epoch(db, tmp_path):
    from daylily_tapdb.backup import service, verify
    from daylily_tapdb.cli.db_config import get_db_config, get_backup_settings
    from daylily_tapdb.runtime_principal import operator_session

    cfg = dict(get_db_config(config_path=str(db["config_path"])))
    settings = dict(get_backup_settings())
    settings.update(
        config_dir=str(tmp_path), storage_uri=f'file://{tmp_path / "store"}'
    )
    from daylily_tapdb.history import HistoryService, HistoryError

    with connection(db) as runtime, runtime.session_scope(commit=True) as session:
        history_identity = create(session, "restored historical object").euid
    with connection(db) as runtime, runtime.session_scope() as session:
        boundary = HistoryService(session).capture_boundary()
    with operator_session(cfg) as conn, conn.begin():
        conn.execute(
            text("SELECT set_config('search_path',:schema,true)"),
            {"schema": cfg["schema_name"]},
        )
        old_epoch = conn.execute(
            text("SELECT epoch::text FROM tapdb_history_epoch WHERE active")
        ).scalar_one()
        old_audit = conn.execute(text("SELECT count(*) FROM audit_log")).scalar_one()
    backup = service.create_backup(cfg, settings)
    result = verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=verify.RestoreOptions(
            target_database="integrity_restore_" + uuid4().hex[:12]
        ),
        recovery_source={"purpose": "isolated_rehearsal"},
    )
    assert not any(c.failed for c in result.checks), [
        c.to_payload() for c in result.checks if c.failed
    ]
    restored = dict(cfg, database=result.target_database)
    with operator_session(restored) as conn, conn.begin():
        conn.execute(
            text("SELECT set_config('search_path',:schema,true)"),
            {"schema": cfg["schema_name"]},
        )
        new_epoch = conn.execute(
            text("SELECT epoch::text FROM tapdb_history_epoch WHERE active")
        ).scalar_one()
        assert new_epoch != old_epoch
        from sqlalchemy.orm import Session
        from daylily_tapdb.integrity_lifecycle import _context

        _context(conn, restored, "inspect-restored-history")
        with Session(bind=conn) as lookup:
            assert (
                HistoryService(lookup).object_at(
                    history_identity,
                    record_type="instance",
                    revision=1,
                    epoch=old_epoch,
                )["state"]["name"]
                == "restored historical object"
            )
            with pytest.raises(HistoryError, match="epoch"):
                HistoryService(lookup).object_at(
                    history_identity, record_type="instance", boundary=boundary
                )
        assert (
            conn.execute(text("SELECT count(*) FROM audit_log")).scalar_one()
            == old_audit
        )
        assert (
            conn.execute(
                text(
                    "SELECT count(*) FROM tapdb_history_epoch WHERE epoch=CAST(:epoch AS uuid)"
                ),
                {"epoch": old_epoch},
            ).scalar_one()
            == 1
        )
        assert (
            conn.execute(
                text(
                    "SELECT count(*) FROM tapdb_history_baseline WHERE epoch=CAST(:epoch AS uuid)"
                ),
                {"epoch": new_epoch},
            ).scalar_one()
            > 0
        )


def test_exact_10111_adoption_preserves_original_evidence(db, tmp_path):
    import json, hashlib
    from pathlib import Path
    from sqlalchemy import create_engine
    from sqlalchemy.pool import NullPool
    from daylily_tapdb.cli.db_config import get_db_config
    from daylily_tapdb.runtime_principal import operator_session
    from daylily_tapdb.integrity_lifecycle import (
        plan_adoption,
        adopt_integrity,
        _context,
    )

    assets = Path(__file__).parent / "fixtures" / "integrity_10111"
    manifest = json.loads((assets / "manifest.json").read_text())
    assert manifest["commit"] == "8fb344ecac341dcf29fc84faf9d1e0bd0af2bd4c"
    cfg = dict(get_db_config(config_path=str(db["config_path"])))
    cfg["database"] = "integrity_old_" + uuid4().hex[:12]
    control = dict(cfg, database="postgres")
    engine = create_engine(
        db["operator_dsn"], isolation_level="AUTOCOMMIT", poolclass=NullPool
    )
    with engine.connect() as conn:
        conn.exec_driver_sql('CREATE DATABASE "' + cfg["database"] + '"')
    engine.dispose()
    with operator_session(cfg) as conn, conn.begin():
        conn.exec_driver_sql('CREATE SCHEMA "' + cfg["schema_name"] + '"')
        conn.execute(
            text("SELECT set_config('search_path',:schema,true)"),
            {"schema": cfg["schema_name"]},
        )
        for name in (
            "tapdb_schema.sql",
            "rls.sql",
            "runtime_identity_authorization.sql",
        ):
            data = (assets / name).read_bytes()
            assert hashlib.sha256(data).hexdigest() == manifest["sha256"][name]
            conn.exec_driver_sql(data.decode().replace("%", "%%"))
        _context(conn, cfg, "legacy-fixture")
        conn.exec_driver_sql(
            "CREATE SEQUENCE tpx_instance_seq; CREATE SEQUENCE edg_instance_seq; CREATE SEQUENCE adt_instance_seq"
        )
        conn.execute(
            text(
                "INSERT INTO tapdb_identity_prefix_config(entity,domain_code,issuer_app_code,prefix) VALUES ('generic_template',:domain,:owner,'TPX'),('generic_instance_lineage',:domain,:owner,'EDG'),('audit_log',:domain,:owner,'ADT')"
            ),
            {"domain": cfg["domain_code"], "owner": cfg["owner_repo_name"]},
        )
        template = conn.execute(
            text(
                "INSERT INTO generic_template(name,polymorphic_discriminator,category,type,subtype,version,instance_prefix,bstatus,is_singleton) VALUES ('Historical message','generic_template','message','webhook','event','1.0','MSG','active',false) RETURNING uid"
            )
        ).scalar_one()
        identity = conn.execute(
            text(
                "INSERT INTO generic_instance(name,polymorphic_discriminator,category,type,subtype,version,template_uid,bstatus) VALUES ('Legacy name','generic_instance','message','webhook','event','1.0',:template,'active') RETURNING euid"
            ),
            {"template": template},
        ).scalar_one()
        conn.execute(
            text(
                "UPDATE generic_instance SET name='Retained legacy name' WHERE euid=:euid"
            ),
            {"euid": identity},
        )
    # Retain the existing exact-source restore contract; restoration is not adoption.
    from daylily_tapdb.backup import service, verify
    from daylily_tapdb.backup.source_contract import capture_source_contract
    from daylily_tapdb.cli.db_config import get_backup_settings

    settings = dict(get_backup_settings())
    settings.update(
        config_dir=str(tmp_path), storage_uri=f'file://{tmp_path / "old-store"}'
    )
    with operator_session(cfg, isolation_level="REPEATABLE READ") as conn, conn.begin():
        _context(conn, cfg, "capture-legacy-source")
        contract = capture_source_contract(
            conn,
            schema_name=cfg["schema_name"],
            target=service.inventory_target(cfg),
            source_version="10.1.11",
        )
    backup = service.create_backup(cfg, settings, source_contract=contract)
    restored = verify.restore_backup(
        cfg,
        settings,
        backup_id=backup.backup_id,
        options=verify.RestoreOptions(
            target_database="legacy_restore_" + uuid4().hex[:12]
        ),
        recovery_source={"purpose": "isolated_rehearsal"},
    )
    assert restored.ok, restored.to_payload()
    assert restored.principal_binding_required
    assert any(
        c.id == "history.adoption_required" and c.status == "warn"
        for c in restored.checks
    )

    with operator_session(cfg, isolation_level="REPEATABLE READ") as conn, conn.begin():
        plan = plan_adoption(conn, cfg)
    result = adopt_integrity(
        cfg, control, plan=plan, receipts_dir=tmp_path / "legacy-adoption"
    )
    assert result["fence_released"]
    with operator_session(cfg) as conn, conn.begin():
        _context(conn, cfg, "inspect-adopted")
        state = conn.execute(
            text(
                "SELECT state FROM tapdb_history_baseline WHERE rel_table_euid_fk=:euid"
            ),
            {"euid": identity},
        ).scalar_one()
        assert state["name"] == "Retained legacy name" and state["record_revision"] == 0
        assert result["templates"]["inserted"] == 2
        assert (
            conn.execute(
                text(
                    "SELECT count(*) FROM audit_log WHERE rel_table_euid_fk=:euid AND json_addl->>'format'='tapdb.revision/v1'"
                ),
                {"euid": identity},
            ).scalar_one()
            == 0
        )
        conn.execute(
            text(
                "UPDATE generic_instance SET name='First attributed revision' WHERE euid=:euid"
            ),
            {"euid": identity},
        )
        revision = conn.execute(
            text("SELECT record_revision FROM generic_instance WHERE euid=:euid"),
            {"euid": identity},
        ).scalar_one()
        assert revision == 1
        assert (
            conn.execute(
                text(
                    "SELECT count(*) FROM audit_log WHERE rel_table_euid_fk=:euid AND json_addl->>'format'='tapdb.revision/v1'"
                ),
                {"euid": identity},
            ).scalar_one()
            == 1
        )


def test_inherited_audit_privileges_and_role_assumption_are_rejected(db):
    from daylily_tapdb.cli.db_config import get_db_config
    from daylily_tapdb.runtime_principal import operator_session
    from daylily_tapdb.audit_storage import audit_writer_role, enforce_audit_read_grants

    cfg = get_db_config(config_path=str(db["config_path"]))
    schema = cfg["schema_name"]
    inherited = "qualification_audit_" + uuid4().hex[:12]
    writer = audit_writer_role(cfg["database"], schema)
    with connection(db) as conn:
        with pytest.raises(DBAPIError):
            with conn.session_scope() as s:
                s.execute(text('SET ROLE "' + writer + '"'))
        with pytest.raises(DBAPIError):
            with conn.session_scope() as s:
                s.execute(text("SELECT record_insert()"))
    with operator_session(cfg) as conn, conn.begin():
        conn.exec_driver_sql('CREATE ROLE "' + inherited + '" NOLOGIN')
        conn.exec_driver_sql(f'GRANT UPDATE ON "{schema}".audit_log TO "{inherited}"')
        conn.exec_driver_sql(f'GRANT "{inherited}" TO "{db["user"]}"')
    try:
        with operator_session(cfg) as conn:
            with pytest.raises(ValueError, match="inherited"):
                with conn.begin():
                    enforce_audit_read_grants(
                        conn, database=cfg["database"], schema=schema
                    )
    finally:
        with operator_session(cfg) as conn, conn.begin():
            conn.exec_driver_sql(f'REVOKE "{inherited}" FROM "{db["user"]}"')
    # A stale direct column grant is actually removed, not merely hidden by RLS.
    with operator_session(cfg) as conn, conn.begin():
        conn.exec_driver_sql(
            f'GRANT UPDATE (is_deleted) ON "{schema}".audit_log TO "{db["user"]}"'
        )
        enforce_audit_read_grants(conn, database=cfg["database"], schema=schema)
        assert not conn.execute(
            text("SELECT has_any_column_privilege(:role,:table,'UPDATE')"),
            {"role": db["user"], "table": f"{schema}.audit_log"},
        ).scalar_one()


def test_mandatory_audit_failure_rolls_back_domain_change(db):
    from daylily_tapdb.cli.db_config import get_db_config
    from daylily_tapdb.runtime_principal import operator_session
    from daylily_tapdb.audit_storage import audit_writer_role
    from daylily_tapdb.models.instance import generic_instance

    cfg = get_db_config(config_path=str(db["config_path"]))
    schema = cfg["schema_name"]
    writer = audit_writer_role(cfg["database"], schema)
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        identity = create(s, "must survive failed audit").euid
    with operator_session(cfg) as conn, conn.begin():
        conn.exec_driver_sql(f'REVOKE INSERT ON "{schema}".audit_log FROM "{writer}"')
    try:
        with connection(db) as conn:
            with pytest.raises(DBAPIError):
                with conn.session_scope(commit=True) as s:
                    obj = s.query(generic_instance).filter_by(euid=identity).one()
                    obj.name = "must rollback"
    finally:
        with operator_session(cfg) as conn, conn.begin():
            conn.exec_driver_sql(f'GRANT INSERT ON "{schema}".audit_log TO "{writer}"')
    with connection(db) as conn, conn.session_scope() as s:
        obj = s.query(generic_instance).filter_by(euid=identity).one()
        assert obj.name == "must survive failed audit" and obj.record_revision == 1
        assert len(query_audit_trail(s, euid=identity)) == 1


def test_correction_stale_plan_and_unrelated_work(db):
    from dataclasses import replace
    from daylily_tapdb.history import HistoryService
    from daylily_tapdb.models.instance import generic_instance
    from daylily_tapdb.revisions import RevisionConflict

    attr = replace(context(), correction_reason="repair selected name")
    validators = {"daylily-tapdb": lambda session, obj, fields: None}
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        identity = create(s, "original").euid
    with connection(db) as conn, conn.session_scope() as s:
        plan = HistoryService(s).plan_correction(
            [
                {
                    "euid": identity,
                    "record_type": "instance",
                    "fields": {"name": "corrected"},
                }
            ],
            operation_id=attr.operation_id,
            reason=attr.correction_reason,
        )
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        obj = s.query(generic_instance).filter_by(euid=identity).one()
        obj.bstatus = "retain unrelated work"
    with connection(db, attr) as conn:
        with pytest.raises(RevisionConflict):
            with conn.session_scope(commit=True) as s:
                HistoryService(s, owner_validators=validators).apply_correction(plan)
        with conn.session_scope() as s:
            fresh = HistoryService(s).plan_correction(
                [
                    {
                        "euid": identity,
                        "record_type": "instance",
                        "fields": {"name": "corrected"},
                    }
                ],
                operation_id=attr.operation_id,
                reason=attr.correction_reason,
            )
        with conn.session_scope(commit=True) as s:
            HistoryService(s, owner_validators=validators).apply_correction(fresh)
        with conn.session_scope() as s:
            obj = s.query(generic_instance).filter_by(euid=identity).one()
            assert obj.name == "corrected" and obj.bstatus == "retain unrelated work"


def test_tenant_references_history_and_scope_cannot_leak(db, tmp_path):
    from daylily_tapdb.cli.db_config import get_db_config
    from daylily_tapdb.runtime_principal import operator_session, bind_runtime_principal
    from daylily_tapdb.external_references import (
        ExternalReferenceService,
        ExternalIdentifierTarget,
    )
    from daylily_tapdb.history import HistoryService

    configs = []
    for i in range(2):
        cfg = dict(
            get_db_config(config_path=str(db["config_path"])),
            user="integrity_tenant_" + uuid4().hex[:12],
            tenant_id=str(uuid4()),
        )
        with operator_session(cfg) as conn, conn.begin():
            conn.exec_driver_sql(
                f'CREATE ROLE "{cfg["user"]}" LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS'
            )
            conn.exec_driver_sql(
                f'GRANT CONNECT ON DATABASE "{cfg["database"]}" TO "{cfg["user"]}"'
            )
        receipt = tmp_path / f"tenant-{i}.json"
        bind_runtime_principal(cfg, receipt_path=receipt)
        bind_runtime_principal(cfg, receipt_path=receipt, apply=True)
        configs.append(cfg)

    def tenant_connection(cfg):
        return TAPDBConnection(
            db_url=f'postgresql://{cfg["user"]}:@localhost:{db["port"]}/{db["database"]}',
            db_user=cfg["user"],
            app_username="tenant qualification",
            domain_code="Z",
            owner_repo_name="daylily-tapdb",
            schema_name=db["schema_name"],
            engine_type="local",
            allow_global_rows=True,
            tenant_id=cfg["tenant_id"],
            config_identity=str(db["config_path"]),
            attribution=context(),
        )

    from uuid import UUID

    target = ExternalIdentifierTarget(
        namespace="qualification",
        kind="tube",
        value=str(uuid4()),
        scope="tenant",
        tenant_id=UUID(configs[0]["tenant_id"]),
    )
    with tenant_connection(configs[0]) as conn, conn.session_scope(commit=True) as s:
        identity = ExternalReferenceService(s).register(target).euid
    with tenant_connection(configs[1]) as conn:
        with conn.session_scope() as s:
            assert ExternalReferenceService(s).resolve(target) is None
            assert query_audit_trail(s, euid=identity) == []
            boundary = HistoryService(s).capture_boundary()
            assert (
                HistoryService(s).object_at(
                    identity, record_type="instance", boundary=boundary
                )["state"]
                is None
            )
        with pytest.raises(ValueError, match="current tenant"):
            with conn.session_scope(commit=True) as s:
                ExternalReferenceService(s).register(target)
        with pytest.raises(DBAPIError):
            with conn.session_scope(commit=True) as s:
                s.execute(
                    text("SELECT set_config('session.current_tenant_id',:tenant,true)"),
                    {"tenant": configs[0]["tenant_id"]},
                )
                create(s, "scope escalation rejected")


def test_competing_owner_bindings_are_serialized(db):
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime, UTC
    from daylily_tapdb.external_references import (
        ExternalReferenceService,
        ExternalIdentifierTarget,
        TapDBObjectTarget,
        ExternalLinkSpec,
    )
    from daylily_tapdb.models.instance import generic_instance
    from daylily_tapdb.models.lineage import generic_instance_lineage

    source_target = ExternalIdentifierTarget(
        namespace="qualification",
        kind="tube",
        value=str(uuid4()),
        scope="public_global",
    )
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        source = ExternalReferenceService(s).register(source_target)
        identity = source.euid
        revision = source.record_revision
        # These are actual persisted identities from the isolated owning store.
        targets = [create(s, "owner assertion target").euid for _ in range(2)]

    def bind(target_id):
        try:
            with connection(db) as conn, conn.session_scope(commit=True) as s:
                source = s.query(generic_instance).filter_by(euid=identity).one()

                def one_binding(session, subject, reference, spec):
                    count = (
                        session.query(generic_instance_lineage)
                        .filter_by(
                            parent_instance_uid=subject.uid,
                            relationship_type="verified_same_object",
                            is_deleted=False,
                        )
                        .count()
                    )
                    if count:
                        raise ValueError("consumer permits only one binding")

                spec = ExternalLinkSpec(
                    TapDBObjectTarget("qualification-owner", target_id),
                    "verified_same_object",
                    "tapdb-test",
                    datetime.now(UTC),
                    "isolated owner fixture",
                )
                ExternalReferenceService(s).attach(
                    source,
                    spec,
                    expected_source_revision=revision,
                    owner_validator=one_binding,
                )
                return "bound"
        except ValueError as exc:
            assert "one binding" in str(exc)
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(bind, targets)) == ["bound", "rejected"]
    with connection(db) as conn, conn.session_scope() as s:
        assert ExternalReferenceService(s).resolve(source_target).euid == identity


def test_cli_api_writes_and_read_only_counts(db, tmp_path):
    import json
    from dataclasses import asdict
    from fastapi import FastAPI, HTTPException
    from fastapi.testclient import TestClient
    from daylily_tapdb.gui.integrity import integrity_router
    from daylily_tapdb.web.runtime import _bundles

    target = {
        "target_type": "opaque",
        "namespace": "qualification",
        "kind": "tube",
        "value": str(uuid4()),
        "scope": "public_global",
    }

    async def authenticated():
        return {
            "username": "qualification",
            "actor_issuer": "qualification",
            "actor_subject": "human-api",
            "role": "admin",
        }

    api = FastAPI()
    api.include_router(
        integrity_router(
            config_path=str(db["config_path"]),
            require_user=authenticated,
            require_admin=authenticated,
        )
    )
    with TestClient(api) as client:
        response = client.post("/api/integrity/write/register", json=target)
        assert response.status_code == 200, response.text
        identity = response.json()["reference"]["euid"]
        with connection(db) as conn, conn.session_scope() as s:
            before = s.execute(
                text(
                    "SELECT (SELECT count(*) FROM generic_instance),(SELECT count(*) FROM generic_instance_lineage),(SELECT count(*) FROM audit_log)"
                )
            ).one()
        boundary = client.post("/api/integrity/read/boundary", json={}).json()[
            "boundary"
        ]
        result = client.post(
            "/api/integrity/read/graph-at",
            json={"seeds": [identity], "boundary": boundary},
        )
        assert result.status_code == 200, result.text
        assert result.json()["nodes"][0]["euid"] == identity
        with connection(db) as conn, conn.session_scope() as s:
            after = s.execute(
                text(
                    "SELECT (SELECT count(*) FROM generic_instance),(SELECT count(*) FROM generic_instance_lineage),(SELECT count(*) FROM audit_log)"
                )
            ).one()
            assert before == after
            audit = query_audit_trail(s, euid=identity)[0]
            assert audit.attribution["actor_subject"] == "human-api"
    payload = tmp_path / "payload.json"
    payload.write_text(json.dumps(target))
    attr = tmp_path / "attribution.json"
    attr.write_text(json.dumps(asdict(context())))
    import subprocess, sys

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "daylily_tapdb.cli",
            "--config",
            str(db["config_path"]),
            "--attribution",
            str(attr),
            "references",
            "register",
            "--payload",
            str(payload),
            "--apply",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout)["reference"]["euid"] == identity

    async def no_admin():
        raise HTTPException(403, "Admin required")

    denied = FastAPI()
    denied.include_router(
        integrity_router(
            config_path=str(db["config_path"]),
            require_user=authenticated,
            require_admin=no_admin,
        )
    )
    with TestClient(denied) as client:
        assert (
            client.post("/api/integrity/write/register", json=target).status_code == 403
        )
    for bundle in _bundles.values():
        bundle.engine.dispose()


def test_runtime_cannot_disable_protections_or_truncate_domain(db):
    for command in [
        "ALTER TABLE audit_log DISABLE TRIGGER ALL",
        "TRUNCATE generic_instance CASCADE",
    ]:
        with connection(db) as conn:
            with pytest.raises(DBAPIError):
                with conn.session_scope(commit=True) as s:
                    s.execute(text(command))


def test_catalog_drift_rejects_disabled_audit_trigger(db, tmp_path):
    from daylily_tapdb.cli.db_config import get_db_config
    from daylily_tapdb.runtime_principal import (
        operator_session,
        build_runtime_principal_binding_plan,
    )

    cfg = get_db_config(config_path=str(db["config_path"]))
    # Deliberate catalog corruption stays inside an uncommitted fixture transaction.
    with operator_session(cfg) as conn:
        with pytest.raises(Exception, match="trigger|Trigger"):
            with conn.begin():
                conn.exec_driver_sql(
                    f'ALTER TABLE "{cfg["schema_name"]}".audit_log DISABLE TRIGGER tapdb_audit_no_mutation'
                )
                build_runtime_principal_binding_plan(conn, cfg)


def test_attribution_envelope_and_malformed_boundary_fail_explicitly(db):
    from dataclasses import asdict
    from daylily_tapdb.history import HistoryService, HistoryError
    import base64, json

    for fields in [
        {"version": True},
        {"actor_subject": 123},
        {"service_identity": " x "},
        {"actor_kind": "unknown"},
    ]:
        with pytest.raises(ValueError):
            Attribution(**{**asdict(context()), **fields})
    with connection(db) as conn, conn.session_scope() as s:
        with pytest.raises(HistoryError):
            HistoryService(s)._boundary(
                base64.urlsafe_b64encode(json.dumps([]).encode()).decode()
            )


def test_correction_receipts_cannot_be_rewritten(db):
    from daylily_tapdb.models.instance import generic_instance

    with connection(db) as conn:
        with pytest.raises(DBAPIError, match="immutable"):
            with conn.session_scope(commit=True) as s:
                receipt = (
                    s.query(generic_instance)
                    .filter_by(category="governance", type="correction_receipt")
                    .first()
                )
                assert receipt is not None
                receipt.name = "attempted receipt rewrite"


def test_read_only_audit_pagination_and_attribution_filter(db):
    attr = context("page actor")
    with connection(db, attr) as conn, conn.session_scope(commit=True) as s:
        identity = create(s, "page one").euid
        for i in range(3):
            obj = (
                s.query(
                    __import__(
                        "daylily_tapdb.models.instance", fromlist=["generic_instance"]
                    ).generic_instance
                )
                .filter_by(euid=identity)
                .one()
            )
            obj.name = "revision " + str(i)
            s.flush()
    with connection(db) as conn, conn.session_scope() as s:
        first = query_audit_trail(
            s,
            euid=identity,
            actor_issuer=attr.actor_issuer,
            actor_subject=attr.actor_subject,
            order="asc",
            limit=2,
        )
        second = query_audit_trail(
            s,
            euid=identity,
            order="asc",
            cursor=(first[-1].changed_at, first[-1].audit_uid),
            limit=2,
        )
        assert [r.revision for r in first + second] == [1, 2, 3, 4]
        assert len(query_audit_trail(s, euid=identity, revision=2)) == 1


def test_host_identity_is_not_inferred_from_foreign_uid(db):
    from types import SimpleNamespace
    from daylily_tapdb.gui.integrity import authenticated_attribution
    from daylily_tapdb.web.bridge import normalize_host_user
    from daylily_tapdb.cli.db_config import get_db_config

    conn = SimpleNamespace(
        _bundle=SimpleNamespace(cfg=get_db_config(config_path=str(db["config_path"])))
    )
    user = normalize_host_user(
        {"uid": 17, "username": "external human", "role": "admin"}
    )
    with pytest.raises(ValueError, match="explicit actor"):
        authenticated_attribution(user, conn)
    user = normalize_host_user(
        {
            "uid": 17,
            "username": "external human",
            "role": "admin",
            "actor_issuer": "trusted-host",
            "actor_subject": "not-a-uuid",
            "actor_kind": "human",
        }
    )
    attr = authenticated_attribution(user, conn)
    assert attr.actor_subject == "not-a-uuid" and attr.actor_issuer == "trusted-host"


def test_reference_reconcile_retains_identity_and_authority(db):
    from datetime import datetime, UTC
    from daylily_tapdb.external_references import (
        ExternalReferenceService,
        ExternalIdentifierTarget,
        ExternalLinkSpec,
    )
    from daylily_tapdb.revisions import RevisionConflict

    targets = [
        ExternalIdentifierTarget(
            namespace="qualification",
            kind="tube",
            value=str(uuid4()),
            scope="public_global",
        )
        for _ in range(2)
    ]
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        source = create(s)
        s.refresh(source)
        svc = ExternalReferenceService(s)
        spec = ExternalLinkSpec(
            targets[0], "contains", "tapdb-test", datetime.now(UTC), "fixture"
        )
        first = svc.attach(
            source, spec, expected_source_revision=source.record_revision
        )
        s.refresh(first.lineage)
        old_id = first.lineage.euid
        old_ref = first.reference.euid
        desired = ExternalLinkSpec(
            targets[1], "contains", "tapdb-test", datetime.now(UTC), "fixture"
        )
        outcomes = svc.reconcile(
            source,
            "tapdb-test",
            [desired],
            expected_source_revision=source.record_revision,
            expected_lineage_revisions={old_id: first.lineage.record_revision},
            deactivated_at=datetime.now(UTC),
            deactivation_provenance="replace association",
        )
        assert sorted(o.status for o in outcomes) == ["created", "deactivated"]
        assert svc.resolve(targets[0]).euid == old_ref
        assert first.lineage.is_deleted
        with pytest.raises(RevisionConflict):
            svc.reconcile(
                source,
                "tapdb-test",
                [],
                expected_source_revision=source.record_revision,
                expected_lineage_revisions={},
                deactivated_at=datetime.now(UTC),
                deactivation_provenance="stale set",
            )


def test_annotation_authority_and_revision_are_enforced(db):
    from dataclasses import replace
    from daylily_tapdb.external_references import (
        ExternalReferenceService,
        ExternalIdentifierTarget,
    )
    from daylily_tapdb.revisions import RevisionConflict
    from daylily_tapdb.models.instance import generic_instance

    target = ExternalIdentifierTarget(
        namespace="qualification",
        kind="tube",
        value=str(uuid4()),
        scope="public_global",
    )
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        svc = ExternalReferenceService(s)
        ref = svc.register(target)
        ann = svc.annotate(
            ref, authority="tapdb-test", key="display", data={"label": "a"}
        )
        s.refresh(ann)
        rev = ann.record_revision
        ann_id = ann.euid
        svc.annotate(
            ref,
            authority="tapdb-test",
            key="display",
            data={"label": "b"},
            expected_revision=rev,
        )
        with pytest.raises(RevisionConflict):
            svc.annotate(
                ref,
                authority="tapdb-test",
                key="display",
                data={"label": "c"},
                expected_revision=rev,
            )
    with connection(
        db, replace(context(), service_identity="different-service")
    ) as conn:
        with pytest.raises(DBAPIError, match="owning authority"):
            with conn.session_scope(commit=True) as s:
                ann = s.query(generic_instance).filter_by(euid=ann_id).one()
                ann.json_addl = {
                    **ann.json_addl,
                    "properties": {
                        **ann.json_addl["properties"],
                        "data": {"label": "unauthorized"},
                    },
                }


def test_historical_missing_seed_is_not_claimed_complete(db):
    from daylily_tapdb.history import HistoryService

    with connection(db) as conn, conn.session_scope() as s:
        svc = HistoryService(s)
        token = svc.capture_boundary()
        result = svc.graph_at(["unpersisted-illustrative-alias"], boundary=token)
        assert not result["complete"] and result["missing_seeds"] == [
            "unpersisted-illustrative-alias"
        ]


def test_authenticated_api_correction_and_conflict_parity(db):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from daylily_tapdb.gui.integrity import integrity_router
    from daylily_tapdb.web.runtime import _bundles

    async def actor():
        return {
            "username": "correction-human",
            "actor_issuer": "qualification",
            "actor_subject": "human-corrector",
            "role": "admin",
        }

    validators = {"daylily-tapdb": lambda session, obj, fields: None}
    api = FastAPI()
    api.include_router(
        integrity_router(
            config_path=str(db["config_path"]),
            require_user=actor,
            require_admin=actor,
            owner_validators=validators,
        )
    )
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        identity = create(s, "before API correction").euid
    with TestClient(api) as client:
        response = client.post(
            "/api/integrity/read/plan-correction",
            json={
                "changes": [
                    {
                        "euid": identity,
                        "record_type": "instance",
                        "fields": {"name": "after API correction"},
                    }
                ],
                "operation_id": str(uuid4()),
                "reason": "qualified correction",
            },
        )
        assert response.status_code == 200, response.text
        plan = response.json()
        first = client.post("/api/integrity/write/apply-correction", json=plan)
        assert first.status_code == 200, first.text
        again = client.post("/api/integrity/write/apply-correction", json=plan)
        assert again.status_code == 200, again.text
        assert first.json()["euid"] == again.json()["euid"] and again.json()["replayed"]
        with connection(db) as conn, conn.session_scope() as s:
            rows = query_audit_trail(s, euid=identity, order="asc")
            assert (
                len(rows) == 2
                and rows[-1].attribution["actor_subject"] == "human-corrector"
            )
    for bundle in _bundles.values():
        bundle.engine.dispose()


def test_offline_reads_do_not_load_correction_plugins(db, monkeypatch):
    from daylily_tapdb import owner_validation
    from daylily_tapdb.history import HistoryService
    from daylily_tapdb.services.integrity_operations import execute_integrity_operation

    def unavailable(*args, **kwargs):
        raise RuntimeError("unrelated owner plugin must not be loaded")

    monkeypatch.setattr(owner_validation, "load_owner_validators", unavailable)
    with connection(db) as conn, conn.session_scope() as s:
        svc = HistoryService(s)
        token = svc.capture_boundary()
        assert not svc.graph_at(["illustrative-alias"], boundary=token)["complete"]
        assert execute_integrity_operation(
            s,
            "resolve",
            {
                "target_type": "opaque",
                "namespace": "qualification",
                "kind": "tube",
                "value": str(uuid4()),
                "scope": "public_global",
            },
        ) == {"reference": None}


def test_lineage_correction_rejects_intervening_endpoint_change(db):
    from daylily_tapdb.models.instance import generic_instance
    from dataclasses import replace
    from datetime import datetime, UTC
    from daylily_tapdb.history import HistoryService
    from daylily_tapdb.revisions import RevisionConflict
    from daylily_tapdb.external_references import (
        ExternalReferenceService,
        ExternalIdentifierTarget,
        ExternalLinkSpec,
    )

    target = ExternalIdentifierTarget(
        namespace="qualification",
        kind="tube",
        value=str(uuid4()),
        scope="public_global",
    )
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        source = create(s)
        source_id = source.euid
        result = ExternalReferenceService(s).attach(
            source,
            ExternalLinkSpec(
                target, "participates", "tapdb-test", datetime.now(UTC), "qualified"
            ),
            expected_source_revision=1,
        )
        edge_id = result.lineage.euid
    attr = replace(context(), correction_reason="endpoint concurrency qualification")
    with connection(db) as conn, conn.session_scope() as s:
        plan = HistoryService(s).plan_correction(
            [
                {
                    "euid": edge_id,
                    "record_type": "lineage",
                    "fields": {"name": "corrected display"},
                }
            ],
            operation_id=attr.operation_id,
            reason=attr.correction_reason,
        )
        assert len(plan["dependencies"]) == 2
    with connection(db) as conn, conn.session_scope(commit=True) as s:
        s.query(generic_instance).filter_by(euid=source_id).one().name = (
            "intervening change"
        )
    with connection(db, attr) as conn:
        with pytest.raises(RevisionConflict):
            with conn.session_scope(commit=True) as s:
                HistoryService(
                    s, owner_validators={"daylily-tapdb": lambda *args: None}
                ).apply_correction(plan)
