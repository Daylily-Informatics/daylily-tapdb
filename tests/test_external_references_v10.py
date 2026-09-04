from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from typer.testing import CliRunner

import daylily_tapdb.external_references as external_references
from daylily_tapdb.cli import app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.connection import TAPDBConnection
from daylily_tapdb.external_references import (
    ExternalIdentifierTarget,
    ExternalLinkSpec,
    ExternalReferenceContractError,
    ExternalReferenceService,
    TapDBObjectTarget,
)
from daylily_tapdb.factory import InstanceFactory
from daylily_tapdb.services.object_operations import (
    ObjectSelector,
    soft_delete_object,
    update_object,
)
from daylily_tapdb.services.object_search import search_external_reference_sources
from daylily_tapdb.templates.manager import TemplateManager

runner = CliRunner()


def _connection(pg_instance, username: str) -> TAPDBConnection:
    runtime_user = pg_instance["runtime_user"]
    return TAPDBConnection(
        db_url=(
            f"postgresql://{runtime_user}:@localhost:{pg_instance['port']}/"
            f"{pg_instance['database']}"
        ),
        db_user=runtime_user,
        app_username=username,
        domain_code="Z",
        owner_repo_name="daylily-tapdb",
        schema_name=pg_instance["schema_name"],
        engine_type="local",
        allow_global_rows=True,
        config_identity=str(pg_instance["config_path"]),
    )


@pytest.fixture(scope="module", autouse=True)
def _schema_and_templates(pg_instance):
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        config_path=pg_instance["config_path"],
    )
    applied = runner.invoke(app, ["db", "schema", "apply"])
    assert applied.exit_code == 0, applied.output
    seeded = runner.invoke(app, ["db", "data", "seed", "--overwrite"])
    assert seeded.exit_code == 0, seeded.output
    pg_instance["runtime_user"] = pg_instance["user"]
    yield
    clear_cli_context()


def _create_instance(session, name: str, *, tenant_id: uuid.UUID | None = None):
    return InstanceFactory(TemplateManager(), domain_code="Z").create_instance(
        session,
        "message/webhook/event/1.0/",
        name,
        create_children=False,
        tenant_id=tenant_id,
    )


def _spec(target, *, authority: str = "tests.lifecycle", offset: int = 0):
    return ExternalLinkSpec(
        target=target,
        relationship_type="references",
        assertion_authority=authority,
        asserted_at=datetime(2026, 9, 4, 6, tzinfo=UTC) + timedelta(minutes=offset),
        assertion_provenance=f"pytest receipt {offset}",
    )


def test_public_module_exports_one_canonical_surface():
    assert external_references.__all__ == [
        "TapDBObjectTarget",
        "ExternalIdentifierTarget",
        "ExternalLinkSpec",
        "ExternalLinkOutcome",
        "ExternalReferenceService",
    ]


def test_opaque_identifier_validation_and_identity_are_exact():
    tenant_id = uuid.uuid4()
    first = ExternalIdentifierTarget(
        namespace="doi",
        kind="article",
        value="10.1000/Exact.Case",
        scope="tenant",
        tenant_id=tenant_id,
        canonical_uri="https://doi.org/10.1000/Exact.Case",
    )
    second = ExternalIdentifierTarget(
        namespace="doi",
        kind="article",
        value="10.1000/Exact.Case",
        scope="public_global",
    )
    assert first.identity_key == second.identity_key
    assert "10.1000" not in first.identity_key
    assert first.value == "10.1000/Exact.Case"

    with pytest.raises(ValueError, match="lowercase canonical token"):
        ExternalIdentifierTarget("DOI", "article", "value", "public_global")
    with pytest.raises(ValueError, match="exact"):
        ExternalIdentifierTarget("doi", "article", " value", "public_global")
    with pytest.raises(ValueError, match="credentials"):
        ExternalIdentifierTarget(
            "doi",
            "article",
            "value",
            "public_global",
            canonical_uri="https://user:secret@example.invalid/item",
        )
    with pytest.raises(ValueError, match="fragment"):
        ExternalIdentifierTarget(
            "doi",
            "article",
            "value",
            "public_global",
            canonical_uri="https://example.invalid/item#fragment",
        )
    with pytest.raises(ValueError, match="require a tenant UUID"):
        ExternalIdentifierTarget("doi", "article", "value", "tenant")
    with pytest.raises(ValueError, match="forbid tenant_id"):
        ExternalIdentifierTarget(
            "doi", "article", "value", "public_global", tenant_id=tenant_id
        )


def test_tapdb_target_rejects_nonpersisted_identifier():
    with pytest.raises(ValueError, match="canonical persisted Meridian EUID"):
        TapDBObjectTarget("atlas", "not-a-persisted-euid")


def test_attach_detach_reactivate_reverse_lookup_and_guards(pg_instance):
    with _connection(pg_instance, "pytest:external-lifecycle") as connection:
        with connection.session_scope(commit=True) as session:
            source = _create_instance(session, "External lifecycle source")
            remote_receipt = _create_instance(session, "Persisted target receipt")
            target = TapDBObjectTarget("remote-catalog", remote_receipt.euid)
            service = ExternalReferenceService(session)

            created = service.attach(source, _spec(target))
            assert created.status == "created"
            assert created.reference is not None
            assert created.lineage is not None
            lineage_euid = created.lineage.euid
            reference_uid = created.reference.uid

            replay = service.attach(source, _spec(target))
            assert replay.status == "existing"
            assert replay.reference.uid == reference_uid
            assert replay.lineage.euid == lineage_euid

            listed = service.list_for_source(source)
            assert listed["page"]["returned"] == 1
            assert listed["items"][0]["target_service_id"] == "remote-catalog"
            reverse = service.find_sources(target)
            assert reverse["page"]["returned"] == 1
            assert reverse["items"][0]["source"]["euid"] == source.euid

            with pytest.raises(PermissionError, match="ExternalReferenceService"):
                InstanceFactory(TemplateManager(), domain_code="Z").create_instance(
                    session,
                    external_references.TAPDB_OBJECT_TEMPLATE_CODE,
                    "Forbidden generic XRF",
                    create_children=False,
                )
            selector = ObjectSelector(euid=created.reference.euid)
            with pytest.raises(PermissionError, match="ExternalReferenceService"):
                update_object(
                    session,
                    selector,
                    {"name": "Forbidden update"},
                    actor="pytest",
                    dry_run=False,
                )
            with pytest.raises(PermissionError, match="ExternalReferenceService"):
                soft_delete_object(
                    session,
                    selector,
                    actor="pytest",
                    dry_run=False,
                )

            deactivated = service.detach(
                source,
                target,
                relationship_type="references",
                assertion_authority="tests.lifecycle",
                deactivated_at=datetime(2026, 9, 4, 7, tzinfo=UTC),
                deactivation_provenance="pytest deactivation",
            )
            assert deactivated.status == "deactivated"
            assert deactivated.lineage.euid == lineage_euid
            assert service.list_for_source(source)["items"] == []
            assert service.find_sources(target)["items"] == []

            repeated = service.detach(
                source,
                target,
                relationship_type="references",
                assertion_authority="tests.lifecycle",
                deactivated_at=datetime(2026, 9, 4, 8, tzinfo=UTC),
                deactivation_provenance="pytest repeated deactivation",
            )
            assert repeated.status == "already_inactive"

            reactivated = service.attach(source, _spec(target, offset=180))
            assert reactivated.status == "reactivated"
            assert reactivated.lineage.euid == lineage_euid
            assert reactivated.reference.uid == reference_uid


def test_shared_target_authority_conflict_and_reconcile_isolation(pg_instance):
    with _connection(pg_instance, "pytest:external-reconcile") as connection:
        with connection.session_scope(commit=True) as session:
            first_source = _create_instance(session, "First shared source")
            second_source = _create_instance(session, "Second shared source")
            first_remote = _create_instance(session, "First persisted target")
            second_remote = _create_instance(session, "Second persisted target")
            first_target = TapDBObjectTarget("remote-catalog", first_remote.euid)
            second_target = TapDBObjectTarget("remote-catalog", second_remote.euid)
            service = ExternalReferenceService(session)

            first = service.attach(
                first_source, _spec(first_target, authority="tests.owner-a")
            )
            shared = service.attach(
                second_source, _spec(first_target, authority="tests.owner-a")
            )
            assert first.reference.uid == shared.reference.uid
            assert first.lineage.uid != shared.lineage.uid

            with pytest.raises(
                ExternalReferenceContractError, match="different assertion authority"
            ):
                service.attach(
                    first_source,
                    _spec(first_target, authority="tests.owner-b"),
                )

            service.attach(
                first_source,
                _spec(second_target, authority="tests.owner-b"),
            )
            outcomes = service.reconcile(
                first_source,
                "tests.owner-a",
                [],
                deactivated_at=datetime(2026, 9, 4, 9, tzinfo=UTC),
                deactivation_provenance="pytest reconcile",
            )
            assert [outcome.status for outcome in outcomes] == ["deactivated"]
            active = service.list_for_source(first_source)
            assert active["page"]["returned"] == 1
            assert active["items"][0]["assertion_authority"] == "tests.owner-b"
            historical = service.list_for_source(first_source, include_inactive=True)
            assert historical["page"]["returned"] == 2


def test_public_opaque_projection_is_visible_and_non_federated(pg_instance):
    with _connection(pg_instance, "pytest:opaque-reference") as connection:
        with connection.session_scope(commit=True) as session:
            source = _create_instance(session, "Opaque identifier source")
            target = ExternalIdentifierTarget(
                namespace="pmid",
                kind="article",
                value="12345678",
                scope="public_global",
                canonical_uri="https://pubmed.ncbi.nlm.nih.gov/12345678/",
            )
            service = ExternalReferenceService(session)
            result = service.attach(source, _spec(target, authority="tests.public-id"))
            assert result.status == "created"
            projected = external_references._project_outbound_external_references(
                source
            )
            assert projected["external_refs"] == []
            assert projected["external_identifiers"][0]["namespace"] == "pmid"
            assert "target_service_id" not in projected["external_identifiers"][0]


def test_outer_transaction_rollback_removes_link_and_xrf(pg_instance):
    with _connection(pg_instance, "pytest:external-rollback") as connection:
        with connection.session_scope(commit=False) as session:
            source = _create_instance(session, "Rollback source")
            remote_receipt = _create_instance(session, "Rollback target receipt")
            target = TapDBObjectTarget("rollback-remote", remote_receipt.euid)
            result = ExternalReferenceService(session).attach(source, _spec(target))
            identity_key = result.reference.identity_key
            assert result.status == "created"

    with _connection(pg_instance, "pytest:external-rollback-proof") as connection:
        with connection.session_scope(commit=False) as session:
            assert (
                session.execute(
                    text(
                        "SELECT count(*) FROM generic_instance "
                        "WHERE identity_key = :identity_key"
                    ),
                    {"identity_key": identity_key},
                ).scalar_one()
                == 0
            )


def test_exact_reverse_search_paginates_active_sources_without_gaps(pg_instance):
    with _connection(pg_instance, "pytest:external-search-page") as connection:
        with connection.session_scope(commit=True) as session:
            remote_receipt = _create_instance(session, "Search page target receipt")
            target = TapDBObjectTarget("search-page-remote", remote_receipt.euid)
            sources = [
                _create_instance(session, f"Search page source {index}")
                for index in range(3)
            ]
            service = ExternalReferenceService(session)
            for source in sources:
                service.attach(
                    source,
                    _spec(target, authority="tests.search-page"),
                )

            first = search_external_reference_sources(
                session,
                service_name="tapdb",
                external_service_id="search-page-remote",
                external_object_euid=remote_receipt.euid,
                limit=2,
            )
            assert [item["euid"] for item in first["items"]] == [
                sources[0].euid,
                sources[1].euid,
            ]
            assert first["page"]["next_cursor"]

            second = search_external_reference_sources(
                session,
                service_name="tapdb",
                external_service_id="search-page-remote",
                external_object_euid=remote_receipt.euid,
                limit=2,
                cursor=first["page"]["next_cursor"],
            )
            assert [item["euid"] for item in second["items"]] == [sources[2].euid]
            assert second["page"]["next_cursor"] is None


class _ExternalSearchQuery:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self.criteria = []

    def join(self, *_args):
        return self

    def filter(self, *criteria):
        self.criteria.extend(criteria)
        return self

    def order_by(self, *_args):
        return self

    def limit(self, value):
        self.rows = self.rows[: int(value)]
        return self

    def all(self):
        return self.rows


class _ExternalSearchSession:
    def __init__(self, rows=()):
        self.query_state = _ExternalSearchQuery(rows)

    def query(self, *_args):
        return self.query_state


@pytest.mark.parametrize(
    ("kwargs", "message"),
    [
        (
            {
                "external_service_id": "remote",
                "external_object_euid": "<persisted-target-euid>",
                "external_namespace": "doi",
                "external_kind": "article",
                "external_value": "10.1/example",
            },
            "mutually exclusive",
        ),
        ({"external_service_id": "remote"}, "required together"),
        ({"external_namespace": "doi"}, "required together"),
        ({}, "one complete external target filter group"),
        (
            {
                "external_service_id": "remote",
                "external_object_euid": "<persisted-target-euid>",
                "limit": True,
            },
            "integer from 1 through 100",
        ),
        (
            {
                "external_service_id": "remote",
                "external_object_euid": "<persisted-target-euid>",
                "external_relationship_type": " references ",
            },
            "must be exact",
        ),
    ],
)
def test_exact_reverse_search_rejects_ambiguous_or_incomplete_filters(
    monkeypatch, kwargs, message
):
    monkeypatch.setattr(external_references, "validate_euid", lambda _value: True)
    with pytest.raises(ValueError, match=message):
        search_external_reference_sources(
            _ExternalSearchSession(),
            service_name="tapdb",
            **kwargs,
        )


def test_exact_reverse_search_builds_typed_opaque_query_and_relationship_filter():
    response = search_external_reference_sources(
        _ExternalSearchSession(),
        service_name="tapdb",
        external_namespace="doi",
        external_kind="article",
        external_value="10.1/example",
        external_relationship_type="references",
        limit=7,
    )
    assert response["items"] == []
    assert response["page"] == {"limit": 7, "returned": 0, "next_cursor": None}
    assert response["filters"] == {
        "external_service_id": "",
        "external_object_euid": "",
        "external_namespace": "doi",
        "external_kind": "article",
        "external_value": "10.1/example",
        "external_relationship_type": "references",
        "record_type": "instance",
        "limit": 7,
    }


def _core_template(subtype: str):
    definition = external_references._canonical_template_definition(subtype)
    return SimpleNamespace(
        **definition,
        uid=1,
        euid=f"persisted-{subtype}-template",
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
    )


def _reference_row(target, *, uid=31, euid="persisted-reference"):
    template = _core_template(
        "tapdb_object" if isinstance(target, TapDBObjectTarget) else "opaque"
    )
    properties = external_references._target_properties(target)
    return SimpleNamespace(
        uid=uid,
        euid=euid,
        category="reference",
        type="external_identifier",
        subtype=template.subtype,
        version="1.0",
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
        tenant_id=getattr(target, "tenant_id", None),
        identity_key=target.identity_key,
        json_addl={"properties": properties},
        parent_template=template,
        is_deleted=False,
    )


def _lineage_row(reference, *, uid=41, deleted=False):
    return SimpleNamespace(
        uid=uid,
        euid=f"persisted-lineage-{uid}",
        relationship_type="references",
        is_deleted=deleted,
        child_instance=reference,
        json_addl={
            "properties": {
                "assertion_authority": "tests.contract",
                "asserted_at": "2026-09-04T06:00:00+00:00",
                "assertion_provenance": "pytest receipt",
                "approved_global_link": reference.tenant_id is None,
                "deactivated_at": ("2026-09-04T07:00:00+00:00" if deleted else None),
                "deactivation_provenance": "pytest detach" if deleted else None,
            }
        },
    )


def test_external_target_value_contracts_cover_all_failure_classes(monkeypatch):
    monkeypatch.setattr(external_references, "validate_euid", lambda value: bool(value))
    tenant_id = uuid.uuid4()
    with pytest.raises(ValueError, match="service_id"):
        TapDBObjectTarget("bad service!", "persisted-target")
    with pytest.raises(ValueError, match="target_tenant_id"):
        TapDBObjectTarget("atlas", "persisted-target", target_tenant_id="not-uuid")
    with pytest.raises(ValueError, match="target_object_kind"):
        TapDBObjectTarget("atlas", "persisted-target", target_object_kind=" bad ")
    with pytest.raises(ValueError, match="scope"):
        ExternalIdentifierTarget("doi", "article", "value", "global")
    with pytest.raises(ValueError, match="must be a string"):
        ExternalIdentifierTarget("doi", "article", 1, "public_global")
    with pytest.raises(ValueError, match="control characters"):
        ExternalIdentifierTarget("doi", "article", "bad\nvalue", "public_global")
    with pytest.raises(ValueError, match="absolute"):
        ExternalIdentifierTarget(
            "doi", "article", "value", "public_global", canonical_uri="relative"
        )
    with pytest.raises(ValueError, match="require a host"):
        ExternalIdentifierTarget(
            "doi", "article", "value", "public_global", canonical_uri="https:item"
        )

    with pytest.raises(ValueError, match="canonical external target"):
        ExternalLinkSpec(
            target=object(),
            relationship_type="references",
            assertion_authority="tests.contract",
            asserted_at=datetime.now(UTC),
            assertion_provenance="receipt",
        )
    valid_target = ExternalIdentifierTarget(
        "account", "accession", "value", "tenant", tenant_id=tenant_id
    )
    with pytest.raises(ValueError, match="timezone"):
        ExternalLinkSpec(
            target=valid_target,
            relationship_type="references",
            assertion_authority="tests.contract",
            asserted_at=datetime(2026, 9, 4),
            assertion_provenance="receipt",
        )
    with pytest.raises(ValueError, match="unsupported external-link"):
        external_references.ExternalLinkOutcome("unknown", None, None)


def test_external_reference_parsers_and_cursor_contracts():
    aware = datetime(2026, 9, 4, tzinfo=UTC)
    assert external_references._parse_timestamp(aware, "stamp") == aware.isoformat()
    with pytest.raises(ExternalReferenceContractError, match="ISO-8601 string"):
        external_references._parse_timestamp(1, "stamp")
    with pytest.raises(ExternalReferenceContractError, match="ISO-8601 timestamp"):
        external_references._parse_timestamp("not-a-time", "stamp")
    with pytest.raises(ValueError, match="timezone"):
        external_references._parse_timestamp("2026-09-04T06:00:00", "stamp")
    with pytest.raises(ExternalReferenceContractError, match="json_addl"):
        external_references._json_properties(SimpleNamespace(json_addl=None))
    with pytest.raises(ExternalReferenceContractError, match="properties"):
        external_references._json_properties(SimpleNamespace(json_addl={}))

    encoded = external_references._encode_cursor(9)
    assert external_references._decode_cursor(encoded) == 9
    assert external_references._decode_cursor(None) == 0
    for malformed in (" bad ", "not-base64", external_references._encode_cursor(0)):
        with pytest.raises(ValueError, match="cursor"):
            external_references._decode_cursor(malformed)
    for invalid_limit in (0, 501, True, "10"):
        with pytest.raises(ValueError, match="limit"):
            external_references._validate_page(invalid_limit, None)


def test_persisted_reference_contract_rejects_drift_and_projects_descriptors(
    monkeypatch,
):
    monkeypatch.setattr(external_references, "validate_euid", lambda value: bool(value))
    tenant_id = uuid.uuid4()
    tapdb_target = TapDBObjectTarget(
        "atlas",
        "persisted-target",
        target_tenant_id=tenant_id,
        target_object_kind="analysis",
    )
    tapdb_reference = _reference_row(tapdb_target)
    assert external_references._target_from_reference(tapdb_reference) == tapdb_target
    kind, projected = external_references._projection(
        _lineage_row(tapdb_reference), tapdb_reference
    )
    assert kind == "tapdb_object"
    assert projected["target_tenant_id"] == str(tenant_id)
    assert projected["target_object_kind"] == "analysis"

    opaque_target = ExternalIdentifierTarget(
        "doi",
        "article",
        "exact-value",
        "public_global",
        canonical_uri="https://doi.org/exact-value",
    )
    opaque_reference = _reference_row(opaque_target, uid=32)
    kind, projected = external_references._projection(
        _lineage_row(opaque_reference, uid=42), opaque_reference
    )
    assert kind == "opaque"
    assert projected["canonical_uri"] == "https://doi.org/exact-value"

    wrong_coordinates = SimpleNamespace(
        category="content", type="sample", subtype="tube", version="1.0"
    )
    with pytest.raises(ExternalReferenceContractError, match="not a core XRF"):
        external_references._reference_kind(wrong_coordinates)
    no_template = _reference_row(tapdb_target)
    no_template.parent_template = None
    with pytest.raises(ExternalReferenceContractError, match="exact seeded"):
        external_references._reference_kind(no_template)
    wrong_owner = _reference_row(tapdb_target)
    wrong_owner.domain_code = "OTHER"
    with pytest.raises(ExternalReferenceContractError, match="ownership"):
        external_references._reference_kind(wrong_owner)
    wrong_template = _core_template("tapdb_object")
    wrong_template.name = "Changed"
    assert (
        external_references._is_exact_xrf_template(wrong_template, "tapdb_object")
        is False
    )
    with pytest.raises(RuntimeError, match="no exact missing"):
        external_references._canonical_template_definition("missing")

    extra_field = _reference_row(tapdb_target)
    extra_field.json_addl["properties"]["legacy"] = True
    with pytest.raises(ExternalReferenceContractError, match="canonical fields"):
        external_references._target_from_reference(extra_field)
    bad_uuid = _reference_row(tapdb_target)
    bad_uuid.json_addl["properties"]["target_tenant_id"] = "not-a-uuid"
    with pytest.raises(ExternalReferenceContractError, match="canonical UUID"):
        external_references._target_from_reference(bad_uuid)
    wrong_identity = _reference_row(tapdb_target)
    wrong_identity.identity_key = "wrong"
    with pytest.raises(ExternalReferenceContractError, match="identity_key"):
        external_references._target_from_reference(wrong_identity)
    tenant_row = _reference_row(tapdb_target)
    tenant_row.tenant_id = tenant_id
    with pytest.raises(ExternalReferenceContractError, match="must be global"):
        external_references._target_from_reference(tenant_row)

    opaque_extra = _reference_row(opaque_target)
    opaque_extra.json_addl["properties"]["legacy"] = True
    with pytest.raises(ExternalReferenceContractError, match="canonical fields"):
        external_references._target_from_reference(opaque_extra)
    opaque_scope = _reference_row(opaque_target)
    opaque_scope.json_addl["properties"]["scope"] = "global"
    with pytest.raises(ExternalReferenceContractError, match="scope"):
        external_references._target_from_reference(opaque_scope)


def test_lineage_and_metadata_projection_fail_closed(monkeypatch):
    monkeypatch.setattr(external_references, "validate_euid", lambda value: bool(value))
    target = TapDBObjectTarget("atlas", "persisted-target")
    reference = _reference_row(target)
    lineage = _lineage_row(reference)
    malformed = _lineage_row(reference)
    malformed.json_addl["properties"]["legacy"] = True
    with pytest.raises(ExternalReferenceContractError, match="canonical fields"):
        external_references._lineage_assertion(malformed)

    clean = SimpleNamespace(json_addl={"properties": {}})
    external_references._reject_metadata_pseudo_edges(clean)
    legacy = SimpleNamespace(
        json_addl={
            "properties": {
                "copied_object_euid": "not-authoritative",
                "external_payload": {"tapdb_graph": {"legacy": True}},
            }
        }
    )
    with pytest.raises(ExternalReferenceContractError, match="non-authoritative"):
        external_references._reject_metadata_pseudo_edges(legacy)
    assert external_references._project_outbound_external_references(clean) == {
        "external_refs": [],
        "external_identifiers": [],
    }
    source = SimpleNamespace(
        json_addl={"properties": {}},
        parent_of_lineages=[
            SimpleNamespace(child_instance=None),
            SimpleNamespace(child_instance=clean),
            lineage,
        ],
    )
    projected = external_references._project_outbound_external_references(source)
    assert projected["external_refs"][0]["target_service_id"] == "atlas"


def test_lifecycle_input_guards_do_not_require_a_database(monkeypatch):
    monkeypatch.setattr(external_references, "validate_euid", lambda value: bool(value))
    service = ExternalReferenceService(None)
    with pytest.raises(ValueError, match="TapDB instance"):
        service._require_source(object())
    incomplete = external_references.generic_instance()
    with pytest.raises(ValueError, match="persisted active"):
        service._require_source(incomplete)

    target = TapDBObjectTarget("atlas", "persisted-target")
    spec = _spec(target)
    with pytest.raises(ValueError, match="ExternalLinkSpec"):
        service.attach(incomplete, object())
    with pytest.raises(ValueError, match="sequence"):
        service.reconcile(
            incomplete,
            "tests.contract",
            "not-a-sequence",
            deactivated_at=datetime.now(UTC),
            deactivation_provenance="receipt",
        )
    with pytest.raises(ValueError, match="at most 500"):
        service.reconcile(
            incomplete,
            "tests.contract",
            [spec] * 501,
            deactivated_at=datetime.now(UTC),
            deactivation_provenance="receipt",
        )

    monkeypatch.setattr(service, "_lock_source", lambda source: source)
    with pytest.raises(ValueError, match="only ExternalLinkSpec"):
        service.reconcile(
            incomplete,
            "tests.contract",
            [object()],
            deactivated_at=datetime.now(UTC),
            deactivation_provenance="receipt",
        )
    other_authority = _spec(target, authority="tests.other")
    with pytest.raises(ValueError, match="assertion_authority"):
        service.reconcile(
            incomplete,
            "tests.contract",
            [other_authority],
            deactivated_at=datetime.now(UTC),
            deactivation_provenance="receipt",
        )
    monkeypatch.setattr(
        service,
        "_attach_locked",
        lambda *_args: external_references.ExternalLinkOutcome("existing", None, None),
    )
    with pytest.raises(ValueError, match="duplicate"):
        service.reconcile(
            incomplete,
            "tests.lifecycle",
            [spec, spec],
            deactivated_at=datetime.now(UTC),
            deactivation_provenance="receipt",
        )
