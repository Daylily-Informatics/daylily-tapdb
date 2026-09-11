"""Source-backed allocator declarations for explicit historical test fixtures."""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from sqlalchemy import text


def verified_source_sequence_mappings(
    connection, *, schema_name: str, source_schema_path: Path
):
    """Read explicit prefix comments in the exact SQL source and verify live DDL.

    This is a test evidence producer, not runtime name discovery. Every returned
    mapping retains the source hash, complete source statement and live catalog
    definition. An absent or changed declared generator is a fixture failure.
    """
    if not source_schema_path.is_absolute() or not source_schema_path.is_file():
        raise ValueError("An absolute exact source schema asset is required")
    source = source_schema_path.read_text(encoding="utf-8")
    source_sha256 = hashlib.sha256(source.encode("utf-8")).hexdigest()
    pattern = re.compile(
        r"^(CREATE SEQUENCE IF NOT EXISTS ([a-z0-9_]+);)\s*--\s*([0-9A-HJ-KMNP-TV-Z]{1,4})\s*\([^\n]+\)$",
        re.M,
    )
    statements = pattern.findall(source)
    if not statements:
        raise ValueError(
            "Source schema contains no explicit allocator prefix declarations"
        )
    result = {}
    for statement, name, prefix in statements:
        if name != prefix.lower() + "_instance_seq":
            raise ValueError("Source prefix declaration and generator name disagree")
        definition = (
            connection.execute(
                text(
                    "SELECT s.seqincrement,s.seqmin,s.seqmax,s.seqstart,s.seqcache,s.seqcycle "
                    "FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid "
                    "JOIN pg_namespace n ON n.oid=c.relnamespace "
                    "WHERE n.nspname=:schema AND c.relname=:name"
                ),
                {"schema": schema_name, "name": name},
            )
            .mappings()
            .one()
        )
        # These exact CREATE statements define an untouched bigint generator.
        if dict(definition) != {
            "seqincrement": 1,
            "seqmin": 1,
            "seqmax": 9223372036854775807,
            "seqstart": 1,
            "seqcache": 1,
            "seqcycle": False,
        }:
            raise ValueError(
                "Live allocator definition differs from its exact source declaration"
            )
        result[name] = {
            "kind": "prefix",
            "prefix": prefix,
            "evidence": json.dumps(
                {
                    "source_sha256": source_sha256,
                    "statement": statement,
                    "declared_prefix": prefix,
                    "catalog_definition": dict(definition),
                },
                sort_keys=True,
                separators=(",", ":"),
            ),
        }
    return result
