"""Sequence helpers for TapDB-backed EUID issuance.

TapDB uses shared per-prefix PostgreSQL sequences (for example:
``agx_instance_seq``) across every table that emits the same prefix. This
module centralizes the logic for creating and safely initializing these
sequences.
"""

from __future__ import annotations

import re

from sqlalchemy import text
from sqlalchemy.orm import Session

_INSTANCE_PREFIX_RE = re.compile(r"[0-9A-HJ-KMNP-TV-Z]{1,4}")


def _normalize_instance_prefix(prefix: str) -> str:
    normalized = prefix.strip().upper()
    if not _INSTANCE_PREFIX_RE.fullmatch(normalized):
        raise ValueError(f"Invalid TAPDB instance prefix: {prefix!r}")
    return normalized


def _build_ensure_instance_prefix_sequence_sql(seq_name: str) -> str:
    # `seq_name` must already be safe for identifier interpolation (derived from
    # validated prefix). We still quote identifiers for defense-in-depth.
    return f"""
    WITH
      desired AS (
        SELECT
          COALESCE(
            (
              SELECT max(euid_seq)
              FROM (
                SELECT euid_seq FROM generic_template WHERE euid_prefix = :prefix
                UNION ALL
                SELECT euid_seq FROM generic_instance WHERE euid_prefix = :prefix
                UNION ALL
                SELECT euid_seq FROM generic_instance_lineage WHERE euid_prefix = :prefix
                UNION ALL
                SELECT euid_seq FROM audit_log WHERE euid_prefix = :prefix
              ) all_euid_rows
            ),
            0
          ) + 1 AS next_val
      ),
      seq_state AS (
        SELECT
          last_value,
          is_called,
          seqincrement AS increment,
          seqmax AS maximum_value,
          seqcycle AS cycle,
          seqcache AS cache_size
        FROM "{seq_name}"
        CROSS JOIN pg_sequence
        WHERE seqrelid = '"{seq_name}"'::regclass
      ),
      seq_next AS (
        SELECT
          CASE WHEN is_called THEN last_value + increment ELSE last_value END
            AS next_val,
          increment,
          maximum_value,
          cycle,
          cache_size
        FROM seq_state
      )
    SELECT
      desired.next_val AS desired_next,
      seq_next.next_val AS current_next,
      seq_next.increment,
      seq_next.maximum_value,
      seq_next.cycle,
      seq_next.cache_size
    FROM desired CROSS JOIN seq_next
    """


def ensure_instance_prefix_sequence(session: Session, prefix: str) -> None:
    """Create + initialize the per-prefix instance sequence.

    Sequence init algorithm:
    - desired nextval() should yield `max(euid_seq) + 1` scoped by `euid_prefix`
    - never move the sequence backwards (avoid reusing previously-issued EUIs)
    """

    normalized = _normalize_instance_prefix(prefix)
    seq_name = f"{normalized.lower()}_instance_seq"

    session.execute(
        text(f'CREATE SEQUENCE IF NOT EXISTS "{seq_name}" CACHE 1 NO CYCLE')
    )
    # Block ordinary identity inserts while the row high-water mark and sequence
    # state are compared. The lock is held until the caller's transaction ends.
    session.execute(
        text(
            "LOCK TABLE generic_template, generic_instance, "
            "generic_instance_lineage, audit_log IN ACCESS EXCLUSIVE MODE"
        )
    )
    state = session.execute(
        text(_build_ensure_instance_prefix_sequence_sql(seq_name)),
        {"prefix": normalized},
    ).one()
    desired_next = int(state.desired_next)
    current_next = int(state.current_next)
    increment = int(state.increment)
    if increment != 1 or bool(state.cycle) or int(state.cache_size) != 1:
        raise ValueError(
            f"Sequence {seq_name!r} has ambiguous issuance settings; "
            "expected INCREMENT 1, NO CYCLE, CACHE 1"
        )
    if current_next >= desired_next:
        return
    if desired_next > int(state.maximum_value):
        raise ValueError(f"Sequence {seq_name!r} cannot advance without wrapping")
    # ALTER SEQUENCE RESTART is transactional in PostgreSQL. Unlike setval(), a
    # later failure in template seeding restores the exact prior sequence state.
    session.execute(text(f'ALTER SEQUENCE "{seq_name}" RESTART WITH {desired_next}'))
