from __future__ import annotations

import json
import uuid
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
import typer

import daylily_tapdb.external_references as refs
from daylily_tapdb.cli import validation as validation_cli
from daylily_tapdb.external_reference_audit import audit_external_references
from daylily_tapdb.external_references import (
    ExternalIdentifierTarget,
    TapDBObjectTarget,
    _canonical_template_definition,
)
from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template


class Query:
    def __init__(self, rows):
        self.rows = list(rows)

    def filter_by(self, **_values):
        return self

    def filter(self, *_expressions):
        return self

    def order_by(self, *_expressions):
        return self

    def yield_per(self, _size):
        return iter(self.rows)

    def all(self):
        return list(self.rows)


class Session:
    def __init__(self, templates, instances, lineages):
        self.templates = templates
        self.instances = instances
        self.lineages = lineages
        self.mutations = 0

    def query(self, *entities):
        if len(entities) == 1 and entities[0] is generic_template:
            return Query(self.templates)
        if len(entities) == 1 and entities[0] is generic_instance:
            return Query(
                item
                for item in self.instances
                if getattr(item, "category", None) == "reference"
                and getattr(item, "type", None) == "external_identifier"
                and getattr(item, "subtype", None) in {"tapdb_object", "opaque"}
            )
        if len(entities) == 1 and entities[0] is generic_instance_lineage:
            return Query(self.lineages)
        if (
            len(entities) == 2
            and entities[0] is generic_instance.euid
            and entities[1] is generic_instance.json_addl
        ):
            return Query((item.euid, item.json_addl) for item in self.instances)
        raise AssertionError(f"unexpected query: {entities!r}")

    def add(self, _value):
        self.mutations += 1

    def flush(self):
        self.mutations += 1


def _template(subtype: str):
    definition = _canonical_template_definition(subtype)
    return SimpleNamespace(
        **definition,
        uid=1 if subtype == "tapdb_object" else 2,
        euid=f"persisted-{subtype}-template",
        is_deleted=False,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
    )


def _reference(target, template, *, uid, euid, extra=None):
    properties = (
        {
            "target_service_id": target.target_service_id,
            "target_object_euid": target.target_object_euid,
            "target_tenant_id": None,
            "target_object_kind": None,
        }
        if isinstance(target, TapDBObjectTarget)
        else {
            "namespace": target.namespace,
            "kind": target.kind,
            "value": target.value,
            "scope": target.scope,
            "canonical_uri": target.canonical_uri,
        }
    )
    properties.update(extra or {})
    return SimpleNamespace(
        uid=uid,
        euid=euid,
        category="reference",
        type="external_identifier",
        subtype=template.subtype,
        version="1.0",
        parent_template=template,
        domain_code="Z",
        issuer_app_code="daylily-tapdb",
        tenant_id=getattr(target, "tenant_id", None),
        identity_key=target.identity_key,
        json_addl={"properties": properties},
        is_deleted=False,
    )


@pytest.fixture(autouse=True)
def _persisted_euid_validator(monkeypatch):
    monkeypatch.setattr(refs, "validate_euid", lambda value: bool(value))


def test_audit_accepts_canonical_state_without_mutating_or_leaking_values():
    tapdb_template = _template("tapdb_object")
    opaque_template = _template("opaque")
    tapdb_target = TapDBObjectTarget("atlas", "persisted-remote-object")
    opaque_target = ExternalIdentifierTarget(
        "doi", "publication", "secret-external-value", "public_global"
    )
    source = SimpleNamespace(
        uid=20,
        euid="persisted-source",
        json_addl={"properties": {}},
    )
    reference_a = _reference(
        tapdb_target, tapdb_template, uid=30, euid="persisted-tapdb-reference"
    )
    reference_b = _reference(
        opaque_target, opaque_template, uid=31, euid="persisted-opaque-reference"
    )
    session = Session(
        [tapdb_template, opaque_template],
        [source, reference_a, reference_b],
        [],
    )

    payload = audit_external_references(session)

    assert payload["ok"] is True
    assert payload["read_only"] is True
    assert payload["canonical_references_by_scope"] == {
        "tapdb_global": 1,
        "opaque_tenant": 0,
        "opaque_public_global": 1,
        "deleted": 0,
    }
    assert session.mutations == 0
    assert "secret-external-value" not in json.dumps(payload)


def test_audit_reports_malformed_legacy_and_duplicate_state_with_euids_only():
    tapdb_template = _template("tapdb_object")
    opaque_template = _template("opaque")
    target = TapDBObjectTarget("atlas", "persisted-remote-object")
    source = SimpleNamespace(
        uid=20,
        euid="persisted-source",
        json_addl={
            "properties": {
                "external_payload": {"tapdb_graph": {"url": "secret-url"}},
                "copied_object_euid": "secret-copied-value",
            }
        },
    )
    malformed = _reference(
        target,
        tapdb_template,
        uid=30,
        euid="persisted-malformed-reference",
        extra={"legacy_url": "https://secret.example"},
    )
    lineages = [
        SimpleNamespace(
            uid=index,
            euid=f"persisted-lineage-{index}",
            parent_instance_uid=source.uid,
            child_instance_uid=malformed.uid,
            parent_instance=source,
            child_instance=malformed,
            relationship_type="references",
        )
        for index in (40, 41)
    ]
    session = Session([tapdb_template, opaque_template], [source, malformed], lineages)

    payload = audit_external_references(session, sample_limit=1)
    rendered = json.dumps(payload)

    assert payload["ok"] is False
    assert payload["violations"]["malformed_xrfs"]["count"] == 1
    assert payload["violations"]["raw_graph_metadata"]["count"] == 1
    assert payload["violations"]["copied_pseudo_edge_fields"]["count"] == 1
    assert payload["violations"]["duplicate_historical_links"]["count"] == 1
    assert "secret-url" not in rendered
    assert "secret-copied-value" not in rendered
    assert "https://secret.example" not in rendered
    assert session.mutations == 0


def test_tenant_scope_must_match_the_reference_row():
    tenant = uuid.uuid4()
    template = _template("opaque")
    target = ExternalIdentifierTarget(
        "account", "customer", "external-value", "tenant", tenant_id=tenant
    )
    reference = _reference(target, template, uid=30, euid="persisted-tenant-reference")
    reference.tenant_id = None
    session = Session([_template("tapdb_object"), template], [reference], [])
    assert audit_external_references(session)["ok"] is False


def test_cli_prints_once_and_exits_nonzero_only_for_violations(monkeypatch):
    captured = []

    class Connection:
        app_username = None

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        @contextmanager
        def session_scope(self, *, commit=False):
            assert commit is False
            yield object()

    monkeypatch.setattr(validation_cli, "get_config_path", lambda: "/tmp/config")
    monkeypatch.setattr(validation_cli, "get_db", lambda _path: Connection())
    monkeypatch.setattr(validation_cli, "_print_payload", captured.append)
    monkeypatch.setattr(
        "daylily_tapdb.external_reference_audit.audit_external_references",
        lambda _session, sample_limit: {"ok": True, "sample_limit": sample_limit},
    )
    validation_cli.external_references(7)
    assert captured == [{"ok": True, "sample_limit": 7}]

    monkeypatch.setattr(
        "daylily_tapdb.external_reference_audit.audit_external_references",
        lambda _session, sample_limit: {"ok": False, "sample_limit": sample_limit},
    )
    with pytest.raises(typer.Exit) as raised:
        validation_cli.external_references(3)
    assert raised.value.exit_code == 1


@pytest.mark.parametrize("sample_limit", [0, 101, True, "25"])
def test_audit_rejects_unbounded_or_noninteger_sample_limits(sample_limit):
    with pytest.raises(ValueError, match="sample_limit"):
        audit_external_references(Session([], [], []), sample_limit=sample_limit)


def test_audit_reports_missing_and_malformed_core_template_state():
    broken = _template("tapdb_object")
    broken.json_addl_schema = None
    broken.instance_prefix = "BAD"
    broken.is_deleted = True
    broken.bstatus = "inactive"

    payload = audit_external_references(Session([broken], [], []))

    assert payload["ok"] is False
    states = {item["subtype"]: item for item in payload["seed_state"]}
    assert states["tapdb_object"]["diagnostics"] == [
        "template schema is not the exact canonical shape",
        "template instance_prefix is not XRF",
        "template is not active",
    ]
    assert states["opaque"]["diagnostics"] == ["expected one template, found 0"]


def test_audit_counts_tenant_and_deleted_state_and_bounds_samples():
    tenant_id = uuid.uuid4()
    tapdb_template = _template("tapdb_object")
    opaque_template = _template("opaque")
    tenant_target = ExternalIdentifierTarget(
        "account",
        "accession",
        "private-external-value",
        "tenant",
        tenant_id=tenant_id,
    )
    tenant_reference = _reference(
        tenant_target,
        opaque_template,
        uid=30,
        euid="persisted-tenant-reference",
    )
    tenant_reference.is_deleted = True
    source_rows = [
        SimpleNamespace(
            uid=index,
            euid=f"persisted-source-{index}",
            json_addl={
                "properties": {
                    "external_payload": {"tapdb_graph": {"legacy": True}},
                    "target_object_euid": "redacted-by-audit",
                }
            },
        )
        for index in (40, 41)
    ]
    no_properties = SimpleNamespace(
        uid=42,
        euid="persisted-source-without-properties",
        json_addl={"not_properties": {}},
    )
    ignored_lineage = SimpleNamespace(
        uid=50,
        euid="persisted-ignored-lineage",
        parent_instance_uid=source_rows[0].uid,
        child_instance_uid=tenant_reference.uid,
        parent_instance=source_rows[0],
        child_instance=None,
        relationship_type="references",
    )
    session = Session(
        [tapdb_template, opaque_template],
        [tenant_reference, *source_rows, no_properties],
        [ignored_lineage],
    )

    payload = audit_external_references(session, sample_limit=1)

    assert payload["canonical_references_by_scope"]["opaque_tenant"] == 1
    assert payload["canonical_references_by_scope"]["deleted"] == 1
    assert payload["violations"]["raw_graph_metadata"]["count"] == 2
    assert len(payload["violations"]["raw_graph_metadata"]["samples"]) == 1
    assert payload["violations"]["copied_pseudo_edge_fields"]["count"] == 2
    assert len(payload["violations"]["copied_pseudo_edge_fields"]["samples"]) == 1
    assert "private-external-value" not in json.dumps(payload)
    assert "redacted-by-audit" not in json.dumps(payload)
