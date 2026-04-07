from __future__ import annotations

from types import SimpleNamespace

from daylily_tapdb.models.instance import generic_instance
from daylily_tapdb.models.lineage import generic_instance_lineage
from daylily_tapdb.models.template import generic_template
from daylily_tapdb.services.object_lookup import find_object_by_euid


class _FakeQuery:
    def __init__(self, items):
        self._items = list(items)

    def filter_by(self, **kwargs):
        rows = [
            item
            for item in self._items
            if all(getattr(item, key, None) == value for key, value in kwargs.items())
        ]
        return _FakeQuery(rows)

    def first(self):
        return self._items[0] if self._items else None


class _FakeSession:
    def __init__(self, mapping):
        self.mapping = mapping

    def query(self, model):
        return _FakeQuery(self.mapping.get(model, []))


def test_find_object_by_euid_prefers_template_then_instance_then_lineage() -> None:
    template = SimpleNamespace(euid="GX1", is_deleted=False)
    instance = SimpleNamespace(euid="GX1", is_deleted=False)
    lineage = SimpleNamespace(euid="GX1", is_deleted=False)
    session = _FakeSession(
        {
            generic_template: [template],
            generic_instance: [instance],
            generic_instance_lineage: [lineage],
        }
    )

    obj, record_type = find_object_by_euid(session, "GX1")

    assert obj is template
    assert record_type == "template"


def test_find_object_by_euid_returns_none_for_missing_or_blank_euid() -> None:
    session = _FakeSession({})

    assert find_object_by_euid(session, "") == (None, None)
    assert find_object_by_euid(session, "NOPE") == (None, None)
