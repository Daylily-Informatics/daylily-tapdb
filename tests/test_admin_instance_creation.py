"""Unit tests for admin instance creation lineage handling."""

from types import SimpleNamespace

import pytest

from admin.main import _new_graph_lineage, _resolve_lineage_targets_or_raise


class _FakeQuery:
    def __init__(self, objects):
        self._objects = objects
        self._filters = {}

    def filter_by(self, **kwargs):
        self._filters = kwargs
        return self

    def first(self):
        euid = self._filters.get("euid")
        is_deleted = self._filters.get("is_deleted")
        for obj in self._objects:
            if obj.euid == euid and obj.is_deleted == is_deleted:
                return obj
        return None


class _FakeSession:
    def __init__(self, objects):
        self._objects = objects

    def query(self, _model):
        return _FakeQuery(self._objects)


def test_resolve_lineage_targets_or_raise_returns_resolved_instances():
    session = _FakeSession(
        [
            SimpleNamespace(euid="GX1", is_deleted=False),
            SimpleNamespace(euid="GX2", is_deleted=False),
        ]
    )

    parents, children = _resolve_lineage_targets_or_raise(
        session, parent_euids=["GX1", "GX1"], child_euids=["GX2"]
    )

    assert [obj.euid for obj in parents] == ["GX1"]
    assert [obj.euid for obj in children] == ["GX2"]


def test_resolve_lineage_targets_or_raise_missing_parent_raises():
    session = _FakeSession([SimpleNamespace(euid="GX2", is_deleted=False)])

    with pytest.raises(ValueError, match=r"missing parent EUID\(s\): GX404"):
        _resolve_lineage_targets_or_raise(
            session, parent_euids=["GX404"], child_euids=["GX2"]
        )


def test_resolve_lineage_targets_or_raise_missing_child_raises():
    session = _FakeSession([SimpleNamespace(euid="GX1", is_deleted=False)])

    with pytest.raises(ValueError, match=r"missing child EUID\(s\): GX405"):
        _resolve_lineage_targets_or_raise(
            session, parent_euids=["GX1"], child_euids=["GX405"]
        )


def test_resolve_lineage_targets_or_raise_missing_parent_and_child_raises():
    session = _FakeSession([])

    with pytest.raises(
        ValueError,
        match=r"missing parent EUID\(s\): GX404; missing child EUID\(s\): GX405",
    ):
        _resolve_lineage_targets_or_raise(
            session, parent_euids=["GX404"], child_euids=["GX405"]
        )


def test_new_graph_lineage_populates_required_non_null_fields():
    parent = SimpleNamespace(
        uuid=101,
        euid="ACT-P",
        polymorphic_discriminator="actor_instance",
    )
    child = SimpleNamespace(
        uuid=202,
        euid="ACT-C",
        polymorphic_discriminator="actor_instance",
    )

    lineage = _new_graph_lineage(
        parent=parent,
        child=child,
        relationship_type="depends_on",
    )

    assert lineage.name == "ACT-P->ACT-C:depends_on"
    assert lineage.polymorphic_discriminator == "generic_instance_lineage"
    assert lineage.category == "lineage"
    assert lineage.type == "lineage"
    assert lineage.subtype == "generic"
    assert lineage.version == "1.0"
    assert lineage.bstatus == "active"
    assert lineage.relationship_type == "depends_on"
    assert lineage.parent_type == "actor_instance"
    assert lineage.child_type == "actor_instance"
    assert lineage.parent_instance_uuid == 101
    assert lineage.child_instance_uuid == 202
    assert lineage.json_addl == {}


def test_new_graph_lineage_defaults_blank_relationship_to_generic():
    parent = SimpleNamespace(
        uuid=101,
        euid="ACT-P",
        polymorphic_discriminator="actor_instance",
    )
    child = SimpleNamespace(
        uuid=202,
        euid="ACT-C",
        polymorphic_discriminator="actor_instance",
    )

    lineage = _new_graph_lineage(
        parent=parent,
        child=child,
        relationship_type="   ",
    )

    assert lineage.relationship_type == "generic"
    assert lineage.name == "ACT-P->ACT-C:generic"
