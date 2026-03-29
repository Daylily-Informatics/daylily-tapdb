from __future__ import annotations

from types import SimpleNamespace

import pytest

import daylily_tapdb.templates.requirements as m


class _FakeManager:
    def __init__(self, mapping: dict[str, object]):
        self._mapping = mapping
        self.calls: list[tuple[object, str]] = []

    def get_template(self, session, template_code: str):
        self.calls.append((session, template_code))
        return self._mapping.get(template_code)


def test_runtime_hint_without_app_name():
    assert (
        m._runtime_hint(app_name=None)
        == "Seed the required TapDB JSON template pack before running this operation."
    )


def test_runtime_hint_with_app_name():
    assert "atlas" in m._runtime_hint(app_name="atlas").lower()


def test_require_seeded_template_raises_when_missing():
    manager = _FakeManager({})
    session = object()

    with pytest.raises(m.MissingSeededTemplateError, match="Missing seeded TapDB"):
        m.require_seeded_template(
            session,
            "generic/actor/system_user/1.0",
            app_name="atlas",
            template_manager=manager,
        )

    assert manager.calls == [(session, "generic/actor/system_user/1.0")]


def test_require_seeded_template_raises_on_prefix_mismatch():
    template = SimpleNamespace(instance_prefix="GX")
    manager = _FakeManager({"generic/item/foo/1.0": template})

    with pytest.raises(
        m.MissingSeededTemplateError,
        match="expected 'WX'",
    ):
        m.require_seeded_template(
            object(),
            "generic/item/foo/1.0",
            expected_prefix="wx",
            template_manager=manager,
        )


def test_require_seeded_template_returns_template_on_success():
    template = SimpleNamespace(instance_prefix="GX")
    session = object()
    manager = _FakeManager({"generic/item/foo/1.0": template})

    resolved = m.require_seeded_template(
        session,
        "generic/item/foo/1.0",
        expected_prefix="gx",
        template_manager=manager,
    )

    assert resolved is template
    assert manager.calls == [(session, "generic/item/foo/1.0")]


def test_require_seeded_template_blank_expected_prefix_raises_mismatch():
    template = SimpleNamespace(instance_prefix="GX")
    manager = _FakeManager({"generic/item/foo/1.0": template})

    with pytest.raises(
        m.MissingSeededTemplateError,
        match="expected ''",
    ):
        m.require_seeded_template(
            object(),
            "generic/item/foo/1.0",
            expected_prefix="   ",
            template_manager=manager,
        )


def test_require_seeded_templates_resolves_all_in_order():
    session = object()
    first = SimpleNamespace(instance_prefix="GX")
    second = SimpleNamespace(instance_prefix="WX")
    manager = _FakeManager(
        {
            "generic/a/one/1.0": first,
            "generic/b/two/1.0": second,
        }
    )

    resolved = m.require_seeded_templates(
        session,
        [
            ("generic/a/one/1.0", "GX"),
            ("generic/b/two/1.0", "WX"),
        ],
        app_name="atlas",
        template_manager=manager,
    )

    assert resolved == [first, second]
    assert manager.calls == [
        (session, "generic/a/one/1.0"),
        (session, "generic/b/two/1.0"),
    ]
