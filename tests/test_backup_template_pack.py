"""Template-pack validation and summarization (no database required).

These guard the checks that stop a broken pack from being shipped as a backup:
a pack that fails validation is refused at capture time, so the failure surfaces
while the source database is still there to re-export from.
"""

from __future__ import annotations

import builtins
from pathlib import Path

import pytest

from daylily_tapdb.backup.template_pack import (
    EXCLUDED_COLUMNS,
    REQUIRED_TEMPLATE_KEYS,
    build_template_pack,
    pack_schema_path,
    pack_summary,
    validate_template_pack,
)


def _entry(**overrides) -> dict:
    entry = {
        "name": "user",
        "polymorphic_discriminator": "actor_template",
        "category": "actor",
        "type": "user",
        "subtype": "system",
        "version": "1.0",
        "instance_prefix": "SYS",
    }
    entry.update(overrides)
    return entry


class FakeSession:
    """Minimal stand-in returning fixed template rows."""

    def __init__(self, rows):
        self._rows = rows
        self.statements: list[str] = []

    def execute(self, statement, *args, **kwargs):
        self.statements.append(str(statement))
        rows = self._rows

        class _Result:
            @staticmethod
            def mappings():
                return rows

        return _Result()


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------


def test_a_well_formed_pack_has_no_problems():
    assert validate_template_pack({"templates": [_entry()]}) == []


def test_a_pack_without_a_templates_array_is_rejected():
    problems = validate_template_pack({"$comment": "nope"})

    assert problems == ["pack is missing a 'templates' array"]


def test_a_templates_value_of_the_wrong_type_is_rejected():
    assert validate_template_pack({"templates": "not-a-list"})


def test_a_non_object_entry_is_reported_with_its_index():
    problems = validate_template_pack({"templates": [_entry(), "oops"]})

    assert any("templates[1] is not an object" in p for p in problems)


@pytest.mark.parametrize("key", REQUIRED_TEMPLATE_KEYS)
def test_each_required_key_is_enforced(key):
    problems = validate_template_pack({"templates": [_entry(**{key: ""})]})

    assert any(f"missing required '{key}'" in p for p in problems)


def test_whitespace_only_values_do_not_satisfy_a_required_key():
    problems = validate_template_pack({"templates": [_entry(name="   ")]})

    assert any("missing required 'name'" in p for p in problems)


@pytest.mark.parametrize("column", sorted(EXCLUDED_COLUMNS))
def test_destination_owned_columns_are_rejected(column):
    # Re-seeding an exported pack must never carry the source database's
    # identity: EUIDs belong to whichever database issued them.
    problems = validate_template_pack({"templates": [_entry(**{column: "x"})]})

    assert any(f"destination-owned column '{column}'" in p for p in problems)


def test_validation_degrades_gracefully_without_jsonschema(monkeypatch):
    real_import = builtins.__import__

    def _no_jsonschema(name, *args, **kwargs):
        if name == "jsonschema":
            raise ImportError("no jsonschema")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_jsonschema)

    # Structural checks still run, so an export is never silently unvalidated.
    assert validate_template_pack({"templates": [_entry(name="")]})
    assert validate_template_pack({"templates": [_entry()]}) == []


def test_validation_survives_a_missing_schema_file(monkeypatch):
    import daylily_tapdb.backup.template_pack as tp

    monkeypatch.setattr(tp, "pack_schema_path", lambda: Path("/nonexistent.json"))

    assert validate_template_pack({"templates": [_entry()]}) == []


def test_the_bundled_schema_file_exists():
    assert pack_schema_path().is_file()


# ---------------------------------------------------------------------------
# summary
# ---------------------------------------------------------------------------


def test_summary_counts_categories_and_prefixes():
    pack = {
        "templates": [
            _entry(category="actor", instance_prefix="SYS"),
            _entry(category="actor", instance_prefix="SYS"),
            _entry(category="governance", instance_prefix="GVR"),
        ]
    }

    summary = pack_summary(pack)

    assert summary["template_count"] == 3
    assert summary["categories"] == {"actor": 2, "governance": 1}
    assert summary["instance_prefixes"] == ["GVR", "SYS"]


def test_summary_of_an_empty_pack():
    assert pack_summary({"templates": []}) == {
        "template_count": 0,
        "categories": {},
        "instance_prefixes": [],
    }


def test_summary_skips_malformed_entries():
    summary = pack_summary({"templates": [_entry(), "junk"]})

    assert summary["template_count"] == 2
    assert summary["categories"] == {"actor": 1}


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------


def test_build_records_provenance_in_the_only_permitted_field():
    # The schema allows exactly two top-level keys, so a note has nowhere else
    # to go without failing validation.
    session = FakeSession([])

    pack = build_template_pack(session, "tapdb_prod", note="pre-migration")

    assert set(pack) <= {"templates", "$comment"}
    assert "tapdb_prod" in pack["$comment"]
    assert "pre-migration" in pack["$comment"]
    assert validate_template_pack(pack) == []


def test_build_excludes_soft_deleted_templates_by_default():
    session = FakeSession([])

    build_template_pack(session, "tapdb_prod")

    assert "is_deleted = FALSE" in session.statements[0]


def test_build_can_include_soft_deleted_templates():
    session = FakeSession([])

    build_template_pack(session, "tapdb_prod", include_deleted=True)

    assert "is_deleted" not in session.statements[0]
