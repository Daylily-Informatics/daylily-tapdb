from __future__ import annotations

import pytest

from daylily_tapdb.models.template import (
    _block_direct_template_mutations,
    generic_template,
)
from daylily_tapdb.templates.mutation import (
    TemplateMutationGuardError,
    allow_template_mutations,
)


class _FakeSession:
    def __init__(
        self,
        *,
        new: list[object] | None = None,
        dirty: list[object] | None = None,
        deleted: list[object] | None = None,
        modified: set[int] | None = None,
    ) -> None:
        self.new = list(new or [])
        self.dirty = list(dirty or [])
        self.deleted = list(deleted or [])
        self._modified = set(modified or set())

    def is_modified(self, obj: object, include_collections: bool = False) -> bool:
        del include_collections
        return id(obj) in self._modified


def _template(template_code: str = "dewey/data/artifact/1.0/") -> generic_template:
    category, type_name, subtype, version = template_code.strip("/").split("/")
    return generic_template(
        name="Test Template",
        polymorphic_discriminator="generic_template",
        category=category,
        type=type_name,
        subtype=subtype,
        version=version,
        instance_prefix="DAT",
        instance_polymorphic_identity="generic_instance",
        json_addl={},
        bstatus="active",
        is_singleton=False,
        is_deleted=False,
    )


def test_template_guard_blocks_direct_inserts() -> None:
    session = _FakeSession(new=[_template()])

    with pytest.raises(
        TemplateMutationGuardError,
        match="dewey/data/artifact/1.0/",
    ):
        _block_direct_template_mutations(session, None, None)


def test_template_guard_blocks_direct_updates() -> None:
    template = _template("atlas/core/user-profile/1.0/")
    template.name = "Updated Template"
    session = _FakeSession(dirty=[template], modified={id(template)})

    with pytest.raises(
        TemplateMutationGuardError,
        match="atlas/core/user-profile/1.0/",
    ):
        _block_direct_template_mutations(session, None, None)


def test_template_guard_allows_loader_context() -> None:
    session = _FakeSession(new=[_template("ursa/analysis/result/1.0/")])

    with allow_template_mutations():
        _block_direct_template_mutations(session, None, None)
