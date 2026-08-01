"""Live reads of the state a manifest records.

Split out from ``service.py`` deliberately: orchestration and SQL have very
different testing needs -- these functions are the ones that must be exercised
against a real PostgreSQL, while the service around them can be driven with
fakes. (The plan's module list does not name this file; it is a cohesion split,
not new behaviour.)

Nothing here hardcodes a table, sequence, or column list. Tables are enumerated
from ``information_schema``, sequences from ``pg_sequences``, and EUID-bearing
tables are discovered by their ``euid_prefix``/``euid_seq`` column signature --
so a table added a year from now is captured without touching this file.
"""

from __future__ import annotations

import ipaddress
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Optional

from sqlalchemy import text

from daylily_tapdb.backup.manifest import SequenceState


def quote_ident(value: str) -> str:
    """Quote a PostgreSQL identifier for interpolation into DDL/DML."""
    return '"' + str(value).replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    """Quote a string *literal* for interpolation into SQL.

    Identifier quoting is not interchangeable with literal quoting: a value
    landing inside ``'...'`` needs single-quote doubling, and using
    ``quote_ident`` there would produce a syntactically valid but wrong query.
    """
    return "'" + str(value).replace("'", "''") + "'"


def server_version(session: Any) -> dict[str, Any]:
    """Return the target's version, both numeric and human-readable."""
    num = session.execute(text("SHOW server_version_num")).scalar()
    label = session.execute(text("SHOW server_version")).scalar()
    return {
        "server_version_num": int(num) if num is not None else None,
        "server_version": str(label) if label is not None else None,
    }


def list_tables(session: Any, schema_name: str) -> list[str]:
    """Return every base table in the schema, enumerated live."""
    rows = session.execute(
        text(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        ),
        {"schema": schema_name},
    )
    return [str(row[0]) for row in rows]


def capture_row_counts(session: Any, schema_name: str) -> dict[str, int]:
    """Count every table in the schema.

    Call inside the snapshot-exporting transaction so the numbers describe the
    same instant as the dump.
    """
    counts: dict[str, int] = {}
    for table in list_tables(session, schema_name):
        statement = text(
            f"SELECT count(*) FROM {quote_ident(schema_name)}.{quote_ident(table)}"
        )
        counts[table] = int(session.execute(statement).scalar() or 0)
    return counts


def capture_sequences(session: Any, schema_name: str) -> list[SequenceState]:
    """Capture every sequence's position, precisely enough to prevent reuse.

    Read from each sequence relation rather than from ``pg_sequences``. That
    view reports ``last_value`` as NULL whenever ``is_called`` is false --
    including after ``setval(s, 5, false)``, which TapDB's own prefix-sequence
    reconciliation performs. A sequence poised to issue ``5`` was therefore
    recorded identically to a fresh one poised to issue ``1``: the value was
    lost, the high-water check skipped it as unknown, and an in-place restore
    reissued an EUID that had already been handed to a consumer.

    The relation exposes ``last_value`` and ``is_called`` separately, which
    together determine the next value -- see ``SequenceState.next_value``.

    Sequence values are non-transactional, so this is read *after* the dump
    completes. The recorded position is therefore a lower bound on the live
    one, and verification asserts ``>=``. That direction is what guarantees a
    restore never reissues an EUID.
    """
    names = [
        str(row[0])
        for row in session.execute(
            text(
                """
                SELECT sequencename
                FROM pg_sequences
                WHERE schemaname = :schema
                ORDER BY sequencename
                """
            ),
            {"schema": schema_name},
        )
    ]
    if not names:
        return []

    # One round trip. Sequence names come from the catalogue, never from a
    # caller, and are quoted regardless.
    union = " UNION ALL ".join(
        f"SELECT {quote_literal(name)} AS name, last_value, is_called "
        f"FROM {quote_ident(schema_name)}.{quote_ident(name)}"
        for name in names
    )
    rows = session.execute(text(f"SELECT * FROM ({union}) AS s ORDER BY name"))
    return [
        SequenceState(
            name=str(row[0]),
            last_value=None if row[1] is None else int(row[1]),
            is_called=bool(row[2]),
        )
        for row in rows
    ]


def euid_bearing_tables(session: Any, schema_name: str) -> list[str]:
    """Find tables that carry EUIDs, by column signature rather than by name.

    Any table with both ``euid_prefix`` and ``euid_seq`` participates in EUID
    identity. Discovering them this way means a new EUID-bearing table is
    covered by sampling and uniqueness checks the day it is created.
    """
    rows = session.execute(
        text(
            """
            SELECT table_name
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND column_name IN ('euid_prefix', 'euid_seq')
            GROUP BY table_name
            HAVING count(DISTINCT column_name) = 2
            ORDER BY table_name
            """
        ),
        {"schema": schema_name},
    )
    return [str(row[0]) for row in rows]


def capture_representative_objects(
    session: Any,
    schema_name: str,
    *,
    per_table: int = 3,
) -> list[dict[str, Any]]:
    """Sample EUIDs so a restore can be proven to resolve real objects.

    Sampling the lowest and highest ``euid_seq`` per table is deliberate: the
    highest is the row most likely to be lost if a restore silently truncates,
    and the lowest proves the oldest history survived.
    """
    samples: list[dict[str, Any]] = []
    for table in euid_bearing_tables(session, schema_name):
        qualified = f"{quote_ident(schema_name)}.{quote_ident(table)}"
        rows = session.execute(
            text(
                f"""
                (SELECT euid, euid_prefix, euid_seq FROM {qualified}
                 ORDER BY euid_seq ASC LIMIT :n)
                UNION
                (SELECT euid, euid_prefix, euid_seq FROM {qualified}
                 ORDER BY euid_seq DESC LIMIT :n)
                """
            ),
            {"n": per_table},
        )
        for row in rows:
            samples.append(
                {
                    "table": table,
                    "euid": str(row[0]),
                    "euid_prefix": str(row[1]),
                    "euid_seq": int(row[2]),
                }
            )
    samples.sort(key=lambda item: (item["table"], item["euid_seq"]))
    return samples


def capture_migrations(session: Any, schema_name: str) -> list[dict[str, Any]]:
    """Return applied migration rows, or an empty list if the table is absent.

    A missing ``_tapdb_migrations`` is a legitimate state (a schema applied
    before the migration runner existed), so it is recorded as "none applied"
    rather than failing the backup.
    """
    if "_tapdb_migrations" not in list_tables(session, schema_name):
        return []
    rows = session.execute(
        text(f"SELECT * FROM {quote_ident(schema_name)}._tapdb_migrations ORDER BY 1")
    ).mappings()
    applied: list[dict[str, Any]] = []
    for row in rows:
        applied.append({key: _jsonable(value) for key, value in dict(row).items()})
    return applied


@contextmanager
def snapshot_transaction(engine: Any) -> Iterator[tuple[Any, Optional[str]]]:
    """Open a repeatable-read transaction and export its snapshot.

    Yields ``(connection, snapshot_name)``. The connection stays open for the
    caller's whole capture, because a snapshot is only valid while the
    exporting transaction lives -- ``pg_dump --snapshot`` reads it from another
    session concurrently.

    Isolation is set through ``execution_options`` rather than a ``SET
    TRANSACTION`` statement: PostgreSQL only accepts that statement before any
    query has run, and ``TAPDBConnection.session_scope`` issues four ``SET
    LOCAL`` statements (timezone, search_path, domain code, username) before
    yielding. So a snapshot can never be exported on a scoped session -- hence
    the dedicated connection here.

    ``snapshot_name`` is None when the server will not export one (a read
    replica, or a pooler in the way); the caller then records
    ``consistency: best_effort`` rather than claiming a guarantee it lacks.
    Every read this yields is schema-qualified, so the absence of the scoped
    session's ``search_path`` does not matter.
    """
    connection = engine.connect().execution_options(isolation_level="REPEATABLE READ")
    transaction = connection.begin()
    snapshot: Optional[str] = None
    try:
        try:
            value = connection.execute(text("SELECT pg_export_snapshot()")).scalar()
            snapshot = str(value) if value else None
        except Exception:
            # Degrade to best effort without losing the connection.
            transaction.rollback()
            transaction = connection.begin()
            snapshot = None
        yield connection, snapshot
    finally:
        # Read-only throughout: rollback is the correct close.
        try:
            transaction.rollback()
        finally:
            connection.close()


def resolved_backend_address(session: Any) -> dict[str, Any]:
    """Return the address of the backend actually serving this session.

    ``pg_dump --snapshot`` is only valid against the same backend that exported
    the snapshot, and an Aurora cluster's reader and writer endpoints resolve
    to different hosts. Recording what this session reached lets the dump be
    pinned to it.
    """
    try:
        row = session.execute(
            text("SELECT inet_server_addr()::text, inet_server_port()")
        ).first()
    except Exception:
        return {"address": None, "port": None}
    if row is None:
        return {"address": None, "port": None}
    # `inet_server_addr()::text` renders as a CIDR value (`::1/128`), which
    # PGHOSTADDR rejects outright -- so strip the prefix length and keep only
    # what is actually a literal address.
    raw = None if row[0] is None else str(row[0]).split("/", 1)[0].strip()
    address = None
    if raw:
        try:
            ipaddress.ip_address(raw)
            address = raw
        except ValueError:
            address = None
    return {
        "address": address,
        "port": None if row[1] is None else int(row[1]),
    }


def _jsonable(value: Any) -> Any:
    """Coerce a database value into something JSON can hold."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


__all__ = [
    "capture_migrations",
    "capture_representative_objects",
    "capture_row_counts",
    "capture_sequences",
    "euid_bearing_tables",
    "list_tables",
    "quote_ident",
    "resolved_backend_address",
    "server_version",
    "snapshot_transaction",
]
