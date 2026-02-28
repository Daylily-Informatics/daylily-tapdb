import uuid
from types import SimpleNamespace

import pytest


class _FakeSession:
    def __init__(self):
        self.added = []
        self.flushed = 0

    def add(self, obj):
        self.added.append(obj)

    def flush(self):
        self.flushed += 1


def test_materialize_actions_skips_missing_action_templates():
    from daylily_tapdb.factory.instance import materialize_actions

    sess = _FakeSession()
    tmpl = SimpleNamespace(
        json_addl={
            "action_imports": {
                "a": "action/core/create-note/1.0",
                "b": "action/core/missing/1.0",
            }
        }
    )

    action_tmpl = SimpleNamespace(
        uuid=uuid.uuid4(),
        euid="XX1",
        type="core",
        json_addl={"action_definition": {"foo": "bar"}},
    )

    class TM:
        def get_template(self, session, template_code):
            if template_code.endswith("missing/1.0"):
                return None
            return action_tmpl

    groups = materialize_actions(sess, tmpl, TM())

    assert "core_actions" in groups
    assert "a" in groups["core_actions"]
    assert "b" not in groups["core_actions"]
    assert groups["core_actions"]["a"]["action_template_euid"] == "XX1"


def test_create_instance_errors_depth_cycle_and_missing_template():
    from daylily_tapdb.factory.instance import InstanceFactory

    sess = _FakeSession()

    class TMNone:
        def get_template(self, session, template_code):
            return None

    f = InstanceFactory(TMNone())

    with pytest.raises(ValueError, match="Maximum instantiation depth"):
        f.create_instance(sess, "a/b/c/1.0", "n", _depth=f.MAX_INSTANTIATION_DEPTH + 1)

    with pytest.raises(ValueError, match="Cycle detected"):
        f.create_instance(sess, "a/b/c/1.0/", "n", _visited={"a/b/c/1.0"})

    with pytest.raises(ValueError, match="Template not found"):
        f.create_instance(sess, "a/b/c/1.0", "n")


def test_create_instance_builds_json_addl_and_merges_properties():
    from daylily_tapdb.factory.instance import InstanceFactory

    sess = _FakeSession()
    t_uuid = uuid.uuid4()

    tmpl = SimpleNamespace(
        uuid=t_uuid,
        is_singleton=False,
        instance_polymorphic_identity=None,
        polymorphic_discriminator="generic_template",
        category="generic",
        type="generic",
        subtype="generic",
        version="1.0",
        json_addl={"properties": {"a": 1}, "action_imports": {}},
    )

    class TM:
        def get_template(self, session, template_code):
            return tmpl

    f = InstanceFactory(TM())
    inst = f.create_instance(
        sess,
        template_code="generic/generic/generic/1.0",
        name="x",
        properties={"b": 2},
        create_children=False,
    )

    assert inst.name == "x"
    assert inst.template_uuid == t_uuid
    assert inst.json_addl["properties"] == {"a": 1, "b": 2}
    assert "action_groups" in inst.json_addl
    assert sess.flushed >= 1


def test_create_instance_invalid_instantiation_layouts_raises_value_error():
    from daylily_tapdb.factory.instance import InstanceFactory

    sess = _FakeSession()
    tmpl = SimpleNamespace(
        uuid=uuid.uuid4(),
        is_singleton=False,
        instance_polymorphic_identity=None,
        polymorphic_discriminator="generic_template",
        category="generic",
        type="generic",
        subtype="generic",
        version="1.0",
        # invalid: count must be >= 1
        json_addl={
            "properties": {},
            "instantiation_layouts": [
                {
                    "relationship_type": "contains",
                    "child_templates": [{"template_code": "a/b/c/1.0", "count": 0}],
                }
            ],
        },
    )

    class TM:
        def get_template(self, session, template_code):
            return tmpl

    f = InstanceFactory(TM())
    with pytest.raises(ValueError, match="Invalid instantiation_layouts"):
        f.create_instance(
            sess, "generic/generic/generic/1.0", "x", create_children=True
        )


def test_create_children_handles_string_and_object_child_templates_and_creates_lineages(
    monkeypatch,
):
    from daylily_tapdb.factory.instance import InstanceFactory
    from daylily_tapdb.models.lineage import generic_instance_lineage

    sess = _FakeSession()

    parent = SimpleNamespace(
        uuid=uuid.uuid4(),
        euid="GX1",
        name="parent",
        polymorphic_discriminator="generic_instance",
    )

    tmpl = SimpleNamespace(
        json_addl={
            "instantiation_layouts": [
                {
                    "relationship_type": "contains",
                    "name_pattern": "{parent_name}_{child_subtype}_{index}",
                    "child_templates": [
                        "generic/generic/child1/1.0",
                        {
                            "template_code": "generic/generic/child2/1.0",
                            "count": 2,
                            "name_pattern": "{parent_euid}:{child_subtype}:{index}",
                        },
                    ],
                }
            ]
        }
    )

    created = []

    def fake_create_instance(*, session, template_code, name, **kwargs):
        created.append((template_code, name))
        return SimpleNamespace(
            uuid=uuid.uuid4(),
            euid=f"GX_child_{len(created)}",
            polymorphic_discriminator="generic_instance",
        )

    f = InstanceFactory(
        template_manager=SimpleNamespace(get_template=lambda *a, **k: None)
    )
    monkeypatch.setattr(f, "create_instance", fake_create_instance)

    f._create_children(sess, parent=parent, template=tmpl, depth=0, visited=set())

    assert len(created) == 3
    assert created[0][0] == "generic/generic/child1/1.0"
    assert created[0][1] == "parent_child1_1"
    assert created[1][1] == "GX1:child2:1"
    assert created[2][1] == "GX1:child2:2"

    lineages = [o for o in sess.added if isinstance(o, generic_instance_lineage)]
    assert len(lineages) == 3


def test_get_or_create_singleton_instance_existing_is_returned_and_filters_is_deleted():
    from daylily_tapdb.factory.instance import InstanceFactory

    existing = SimpleNamespace(uuid=uuid.uuid4(), is_deleted=False)

    class Q:
        def __init__(self, result):
            self.result = result
            self.filters = []

        def filter(self, *conds):
            self.filters.extend(conds)
            return self

        def order_by(self, *a, **k):
            return self

        def first(self):
            return self.result

    class Sess:
        def __init__(self):
            self.q = Q(existing)

        def query(self, *a, **k):
            return self.q

    tmpl = SimpleNamespace(uuid=uuid.uuid4(), is_singleton=True)

    class TM:
        def get_template(self, session, template_code):
            return tmpl

    f = InstanceFactory(TM())
    s = Sess()
    got = f.get_or_create_singleton_instance(s, "generic/generic/single/1.0", "n")
    assert got is existing
    assert any("is_deleted" in str(c) for c in s.q.filters)
