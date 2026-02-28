"""Unit tests for lineage helper methods on generic_instance.

These methods are pure-python sorting/filtering utilities; we use lightweight
fake objects to avoid requiring a DB session.
"""

from types import SimpleNamespace


def test_get_sorted_parent_of_lineages_prioritizes_and_sorts_by_child_euid():
    from daylily_tapdb.models.instance import generic_instance

    # Priority discriminator is workflow_step_instance
    child_a = SimpleNamespace(
        polymorphic_discriminator="workflow_step_instance", euid="B", json_addl={}
    )
    child_b = SimpleNamespace(
        polymorphic_discriminator="workflow_step_instance", euid="A", json_addl={}
    )
    child_c = SimpleNamespace(
        polymorphic_discriminator="container_instance", euid="C", json_addl={}
    )

    lin1 = SimpleNamespace(child_instance=child_a)
    lin2 = SimpleNamespace(child_instance=child_b)
    lin3 = SimpleNamespace(child_instance=child_c)

    fake_self = SimpleNamespace(parent_of_lineages=[lin1, lin2, lin3])

    got = generic_instance.get_sorted_parent_of_lineages(fake_self)
    assert got == [lin2, lin1, lin3]


def test_get_sorted_child_of_lineages_prioritizes_and_sorts_by_parent_euid():
    from daylily_tapdb.models.instance import generic_instance

    parent_a = SimpleNamespace(
        polymorphic_discriminator="workflow_step_instance", euid="B", json_addl={}
    )
    parent_b = SimpleNamespace(
        polymorphic_discriminator="workflow_step_instance", euid="A", json_addl={}
    )
    parent_c = SimpleNamespace(
        polymorphic_discriminator="workflow_instance", euid="C", json_addl={}
    )

    lin1 = SimpleNamespace(parent_instance=parent_a)
    lin2 = SimpleNamespace(parent_instance=parent_b)
    lin3 = SimpleNamespace(parent_instance=parent_c)

    fake_self = SimpleNamespace(child_of_lineages=[lin1, lin2, lin3])

    got = generic_instance.get_sorted_child_of_lineages(fake_self)
    assert got == [lin2, lin1, lin3]


def test_filter_lineage_members_validates_and_matches_attrs_or_json_addl():
    import pytest

    from daylily_tapdb.models.instance import generic_instance

    inst1 = SimpleNamespace(category="container", json_addl={"k": "v"})
    inst2 = SimpleNamespace(category="content", json_addl={"k": "v"})
    lin1 = SimpleNamespace(child_instance=inst1)
    lin2 = SimpleNamespace(child_instance=inst2)

    fake_self = SimpleNamespace(parent_of_lineages=[lin1, lin2])

    with pytest.raises(ValueError):
        generic_instance.filter_lineage_members(
            fake_self, "bad", "child_instance", {"category": "container"}
        )
    with pytest.raises(ValueError):
        generic_instance.filter_lineage_members(
            fake_self, "parent_of_lineages", "bad", {"category": "container"}
        )
    with pytest.raises(ValueError):
        generic_instance.filter_lineage_members(
            fake_self, "parent_of_lineages", "child_instance", {}
        )

    # Match by normal attribute
    got = generic_instance.filter_lineage_members(
        fake_self,
        of_lineage_type="parent_of_lineages",
        lineage_member_type="child_instance",
        filter_criteria={"category": "container"},
    )
    assert got == [lin1]

    # Match by json_addl (no attribute named 'k')
    got2 = generic_instance.filter_lineage_members(
        fake_self,
        of_lineage_type="parent_of_lineages",
        lineage_member_type="child_instance",
        filter_criteria={"k": "v"},
    )
    assert got2 == [lin1, lin2]
