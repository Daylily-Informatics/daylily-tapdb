"""Helpers for TAPDB schema inventory, drift detection, and reporting."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional, Sequence

from sqlalchemy import text
from sqlalchemy.orm import Session

_NON_SYSTEM_SCHEMA_SQL = """
    nspname NOT IN ('pg_catalog', 'information_schema')
    AND nspname NOT LIKE 'pg_toast%'
    AND nspname NOT LIKE 'pg_temp_%'
    AND nspname NOT LIKE 'pg_toast_temp_%'
"""

_TABLE_BLOCK_RE = re.compile(
    r"CREATE TABLE(?: IF NOT EXISTS)?\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s*\((.*?)\);",
    re.IGNORECASE | re.DOTALL,
)
_ALTER_ADD_COLUMN_RE = re.compile(
    r"ALTER TABLE\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s+ADD COLUMN(?: IF NOT EXISTS)?\s+"
    r"\"?([A-Za-z_][A-Za-z0-9_]*)\"?",
    re.IGNORECASE,
)
_CREATE_SEQUENCE_RE = re.compile(
    r"CREATE SEQUENCE(?: IF NOT EXISTS)?\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?",
    re.IGNORECASE,
)
_FUNCTION_RE = re.compile(
    r"CREATE OR REPLACE FUNCTION\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s*\((.*?)\)\s+RETURNS\b",
    re.IGNORECASE | re.DOTALL,
)
_TRIGGER_RE = re.compile(
    r"CREATE TRIGGER\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s+.*?\s+ON\s+"
    r"\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s+(?:FOR EACH|EXECUTE)\b",
    re.IGNORECASE | re.DOTALL,
)
_INDEX_RE = re.compile(
    r"CREATE(?: UNIQUE)? INDEX(?: IF NOT EXISTS)?\s+\"?([A-Za-z_][A-Za-z0-9_]*)\"?\s+ON\s+"
    r"\"?([A-Za-z_][A-Za-z0-9_]*)\"?",
    re.IGNORECASE,
)

_COLUMN_SKIP_TOKENS = {
    "CONSTRAINT",
    "PRIMARY",
    "UNIQUE",
    "CHECK",
    "FOREIGN",
    "REFERENCES",
}
_UNEXPECTED_FUNCTION_PREFIXES = (
    "tapdb_",
    "meridian_",
    "set_generic_",
    "set_audit_",
    "record_",
    "crockford_",
)
_UNEXPECTED_FUNCTION_NAMES = {"soft_delete_row", "update_modified_dt"}


class SchemaDriftOperationalError(RuntimeError):
    """Raised when a drift check cannot be completed safely."""


@dataclass
class TapdbSchemaInventory:
    """Normalized inventory of TAPDB-owned schema objects."""

    schema_name: Optional[str]
    tables: set[str] = field(default_factory=set)
    columns: dict[str, set[str]] = field(default_factory=dict)
    sequences: set[str] = field(default_factory=set)
    functions: set[str] = field(default_factory=set)
    triggers: dict[str, set[str]] = field(default_factory=dict)
    indexes: dict[str, set[str]] = field(default_factory=dict)

    def add_table(self, table_name: str) -> None:
        table_name = _normalize_identifier(table_name)
        self.tables.add(table_name)
        self.columns.setdefault(table_name, set())
        self.triggers.setdefault(table_name, set())
        self.indexes.setdefault(table_name, set())

    def add_column(self, table_name: str, column_name: str) -> None:
        table_name = _normalize_identifier(table_name)
        column_name = _normalize_identifier(column_name)
        self.add_table(table_name)
        self.columns[table_name].add(column_name)

    def add_sequence(self, sequence_name: str) -> None:
        self.sequences.add(_normalize_identifier(sequence_name))

    def add_function(self, signature: str) -> None:
        self.functions.add(_normalize_function_signature(signature))

    def add_trigger(self, table_name: str, trigger_name: str) -> None:
        table_name = _normalize_identifier(table_name)
        trigger_name = _normalize_identifier(trigger_name)
        self.add_table(table_name)
        self.triggers[table_name].add(trigger_name)

    def add_index(self, table_name: str, index_name: str) -> None:
        table_name = _normalize_identifier(table_name)
        index_name = _normalize_identifier(index_name)
        self.add_table(table_name)
        self.indexes[table_name].add(index_name)

    def counts(self) -> dict[str, int]:
        return {
            "tables": len(self.tables),
            "columns": sum(len(columns) for columns in self.columns.values()),
            "sequences": len(self.sequences),
            "functions": len(self.functions),
            "triggers": sum(len(triggers) for triggers in self.triggers.values()),
            "indexes": sum(len(indexes) for indexes in self.indexes.values()),
        }


@dataclass(frozen=True)
class SchemaDriftReport:
    """Rendered result of a TAPDB schema drift comparison."""

    env: str
    database: str
    schema_name: Optional[str]
    strict: bool
    expected_asset_paths: list[str]
    expected: TapdbSchemaInventory
    live: TapdbSchemaInventory
    missing: dict[str, list[str]]
    unexpected: dict[str, list[str]]

    @property
    def has_drift(self) -> bool:
        return any(self.missing.values()) or any(self.unexpected.values())

    @property
    def status(self) -> str:
        return "drift" if self.has_drift else "clean"

    def counts(self) -> dict[str, dict[str, int]]:
        return {
            "expected": self.expected.counts(),
            "live": self.live.counts(),
            "missing": {key: len(values) for key, values in self.missing.items()},
            "unexpected": {key: len(values) for key, values in self.unexpected.items()},
        }

    def to_payload(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "env": self.env,
            "database": self.database,
            "schema_name": self.schema_name,
            "strict": self.strict,
            "expected_asset_paths": self.expected_asset_paths,
            "counts": self.counts(),
            "missing": self.missing,
            "unexpected": self.unexpected,
        }


def load_expected_schema_inventory(
    schema_paths: Sequence[Path], *, dynamic_sequence_name: str
) -> TapdbSchemaInventory:
    """Build expected TAPDB inventory from canonical schema assets."""

    inventory = TapdbSchemaInventory(schema_name=None)
    for path in schema_paths:
        _parse_schema_file(path, inventory)
    inventory.add_sequence(dynamic_sequence_name)
    return inventory


def discover_tapdb_schema_name(session: Session) -> Optional[str]:
    """Return the single non-system schema containing TAPDB core tables."""

    rows = session.execute(
        text(
            f"""
            SELECT DISTINCT ns.nspname AS schema_name
            FROM pg_class cls
            JOIN pg_namespace ns ON ns.oid = cls.relnamespace
            WHERE cls.relname = 'generic_template'
              AND cls.relkind IN ('r', 'p')
              AND {_NON_SYSTEM_SCHEMA_SQL}
            ORDER BY ns.nspname
            """
        )
    ).mappings()
    schema_names = [str(row["schema_name"]) for row in rows]
    if not schema_names:
        return None
    if len(schema_names) > 1:
        joined = ", ".join(schema_names)
        raise SchemaDriftOperationalError(
            f"Multiple TAPDB schemas detected ({joined}); drift-check cannot choose safely."
        )
    return schema_names[0]


def load_live_schema_inventory(
    session: Session, *, schema_name: Optional[str] = None
) -> TapdbSchemaInventory:
    """Inspect the deployed TAPDB schema from PostgreSQL catalogs."""

    resolved_schema = schema_name or discover_tapdb_schema_name(session)
    inventory = TapdbSchemaInventory(schema_name=resolved_schema)
    if not resolved_schema:
        return inventory

    for row in session.execute(
        text(
            """
            SELECT tablename AS table_name
            FROM pg_tables
            WHERE schemaname = :schema_name
            ORDER BY tablename
            """
        ),
        {"schema_name": resolved_schema},
    ).mappings():
        inventory.add_table(str(row["table_name"]))

    for row in session.execute(
        text(
            """
            SELECT table_name, column_name
            FROM information_schema.columns
            WHERE table_schema = :schema_name
            ORDER BY table_name, ordinal_position
            """
        ),
        {"schema_name": resolved_schema},
    ).mappings():
        inventory.add_column(str(row["table_name"]), str(row["column_name"]))

    for row in session.execute(
        text(
            """
            SELECT sequencename AS sequence_name
            FROM pg_sequences
            WHERE schemaname = :schema_name
            ORDER BY sequencename
            """
        ),
        {"schema_name": resolved_schema},
    ).mappings():
        inventory.add_sequence(str(row["sequence_name"]))

    for row in session.execute(
        text(
            """
            SELECT
                p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')'
                    AS function_signature
            FROM pg_proc p
            JOIN pg_namespace ns ON ns.oid = p.pronamespace
            WHERE ns.nspname = :schema_name
            ORDER BY function_signature
            """
        ),
        {"schema_name": resolved_schema},
    ).mappings():
        inventory.add_function(str(row["function_signature"]))

    for row in session.execute(
        text(
            """
            SELECT cls.relname AS table_name, tg.tgname AS trigger_name
            FROM pg_trigger tg
            JOIN pg_class cls ON cls.oid = tg.tgrelid
            JOIN pg_namespace ns ON ns.oid = cls.relnamespace
            WHERE ns.nspname = :schema_name
              AND NOT tg.tgisinternal
            ORDER BY cls.relname, tg.tgname
            """
        ),
        {"schema_name": resolved_schema},
    ).mappings():
        inventory.add_trigger(str(row["table_name"]), str(row["trigger_name"]))

    for row in session.execute(
        text(
            """
            SELECT tablename AS table_name, indexname AS index_name
            FROM pg_indexes
            WHERE schemaname = :schema_name
            ORDER BY tablename, indexname
            """
        ),
        {"schema_name": resolved_schema},
    ).mappings():
        inventory.add_index(str(row["table_name"]), str(row["index_name"]))

    return inventory


def diff_schema_inventory(
    expected: TapdbSchemaInventory,
    live: TapdbSchemaInventory,
    *,
    env: str = "",
    database: str = "",
    strict: bool,
    expected_asset_paths: Sequence[str] = (),
) -> SchemaDriftReport:
    """Diff normalized expected and live TAPDB inventories."""

    shared_tables = expected.tables & live.tables
    missing = _empty_change_map()
    unexpected = _empty_change_map()

    missing["tables"] = sorted(expected.tables - live.tables)
    missing["columns"] = sorted(
        f"{table_name}.{column_name}"
        for table_name in sorted(shared_tables)
        for column_name in sorted(
            expected.columns.get(table_name, set())
            - live.columns.get(table_name, set())
        )
    )
    missing["sequences"] = sorted(expected.sequences - live.sequences)
    missing["functions"] = sorted(expected.functions - live.functions)
    missing["triggers"] = sorted(
        f"{table_name}.{trigger_name}"
        for table_name in sorted(shared_tables)
        for trigger_name in sorted(
            expected.triggers.get(table_name, set())
            - live.triggers.get(table_name, set())
        )
    )
    missing["indexes"] = sorted(
        f"{table_name}.{index_name}"
        for table_name in sorted(shared_tables)
        for index_name in sorted(
            expected.indexes.get(table_name, set())
            - live.indexes.get(table_name, set())
        )
    )

    if strict:
        unexpected["tables"] = sorted(
            table_name
            for table_name in live.tables - expected.tables
            if _should_flag_unexpected_table(table_name)
        )
        unexpected["columns"] = sorted(
            f"{table_name}.{column_name}"
            for table_name in sorted(shared_tables)
            for column_name in sorted(
                live.columns.get(table_name, set())
                - expected.columns.get(table_name, set())
            )
        )
        unexpected["triggers"] = sorted(
            f"{table_name}.{trigger_name}"
            for table_name in sorted(shared_tables)
            for trigger_name in sorted(
                live.triggers.get(table_name, set())
                - expected.triggers.get(table_name, set())
            )
        )
        unexpected["indexes"] = sorted(
            f"{table_name}.{index_name}"
            for table_name in sorted(shared_tables)
            for index_name in sorted(
                live.indexes.get(table_name, set())
                - expected.indexes.get(table_name, set())
            )
        )
        unexpected["functions"] = sorted(
            signature
            for signature in live.functions - expected.functions
            if _should_flag_unexpected_function(signature)
        )
        unexpected["sequences"] = sorted(
            sequence_name
            for sequence_name in live.sequences - expected.sequences
            if _should_flag_unexpected_sequence(sequence_name)
        )

    return SchemaDriftReport(
        env=env,
        database=database,
        schema_name=live.schema_name,
        strict=strict,
        expected_asset_paths=list(expected_asset_paths),
        expected=expected,
        live=live,
        missing=missing,
        unexpected=unexpected,
    )


def schema_asset_files(schema_root: Path) -> list[Path]:
    """Return schema asset files from a resolved schema root."""
    asset_paths = [schema_root / "tapdb_schema.sql"]
    migrations_dir = schema_root / "migrations"
    if migrations_dir.exists():
        asset_paths.extend(sorted(migrations_dir.glob("*.sql")))
    return asset_paths


def build_expected_schema_inventory(
    schema_paths: Sequence[Path], *, dynamic_sequence_name: str
) -> TapdbSchemaInventory:
    """Compatibility wrapper for expected inventory construction."""
    return load_expected_schema_inventory(
        schema_paths,
        dynamic_sequence_name=dynamic_sequence_name,
    )


def inventory_counts(inventory: TapdbSchemaInventory) -> dict[str, int]:
    """Compatibility helper returning inventory category counts."""
    return inventory.counts()


def drift_entry_counts(entries: dict[str, list[str]]) -> dict[str, int]:
    """Return category counts for missing or unexpected drift entries."""
    return {key: len(values) for key, values in entries.items()}


def _parse_schema_file(path: Path, inventory: TapdbSchemaInventory) -> None:
    sql = _strip_sql_comments(path.read_text(encoding="utf-8"))

    for match in _TABLE_BLOCK_RE.finditer(sql):
        table_name = match.group(1)
        inventory.add_table(table_name)
        for column_name in _parse_table_columns(match.group(2)):
            inventory.add_column(table_name, column_name)

    for match in _ALTER_ADD_COLUMN_RE.finditer(sql):
        inventory.add_column(match.group(1), match.group(2))

    for match in _CREATE_SEQUENCE_RE.finditer(sql):
        inventory.add_sequence(match.group(1))

    for match in _FUNCTION_RE.finditer(sql):
        inventory.add_function(f"{match.group(1)}({match.group(2)})")

    for match in _TRIGGER_RE.finditer(sql):
        inventory.add_trigger(match.group(2), match.group(1))

    for match in _INDEX_RE.finditer(sql):
        inventory.add_index(match.group(2), match.group(1))


def _parse_table_columns(block: str) -> list[str]:
    columns: list[str] = []
    for raw_line in block.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line:
            continue
        token = line.split(None, 1)[0].strip('"')
        if token.upper() in _COLUMN_SKIP_TOKENS:
            continue
        if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", token):
            continue
        columns.append(token)
    return columns


def _strip_sql_comments(sql: str) -> str:
    return "\n".join(line.split("--", 1)[0] for line in sql.splitlines())


def _normalize_identifier(value: str) -> str:
    return str(value or "").strip().strip('"').lower()


def _normalize_function_signature(signature: str) -> str:
    text_value = str(signature or "").strip().lower()
    text_value = re.sub(r"\s+", " ", text_value)
    text_value = re.sub(r"\(\s*", "(", text_value)
    text_value = re.sub(r"\s*\)", ")", text_value)
    text_value = re.sub(r"\s*,\s*", ", ", text_value)
    return text_value


def _should_flag_unexpected_sequence(sequence_name: str) -> bool:
    normalized = _normalize_identifier(sequence_name)
    if normalized.endswith("_instance_seq"):
        return False
    return normalized.endswith("_audit_seq") or normalized in {
        "generic_template_seq",
        "generic_instance_lineage_seq",
        "tgx_core_seq",
    }


def _should_flag_unexpected_function(signature: str) -> bool:
    function_name = signature.split("(", 1)[0].strip().lower()
    return function_name.startswith(_UNEXPECTED_FUNCTION_PREFIXES) or (
        function_name in _UNEXPECTED_FUNCTION_NAMES
    )


def _should_flag_unexpected_table(table_name: str) -> bool:
    normalized = _normalize_identifier(table_name)
    return (
        normalized.startswith("generic_")
        or normalized.startswith("tapdb_")
        or normalized.startswith("_tapdb_")
        or normalized in {"audit_log", "outbox_event"}
    )


def _empty_change_map() -> dict[str, list[str]]:
    return {
        "tables": [],
        "columns": [],
        "sequences": [],
        "functions": [],
        "triggers": [],
        "indexes": [],
    }
