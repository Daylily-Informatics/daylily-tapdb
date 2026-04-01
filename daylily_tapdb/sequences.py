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

_INSTANCE_PREFIX_RE = re.compile(r"[A-HJ-KMNP-TV-Z]{2,3}")


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
        SELECT last_value, is_called FROM "{seq_name}"
      ),
      seq_next AS (
        SELECT CASE WHEN is_called THEN last_value + 1 ELSE last_value END AS next_val
        FROM seq_state
      ),
      final_next AS (
        SELECT GREATEST(
          (SELECT next_val FROM desired),
          (SELECT next_val FROM seq_next)
        ) AS next_val
      )
    SELECT setval('"{seq_name}"', (SELECT next_val FROM final_next), false)
    """


def ensure_instance_prefix_sequence(session: Session, prefix: str) -> None:
    """Create + initialize the per-prefix instance sequence.

    Sequence init algorithm:
    - desired nextval() should yield `max(euid_seq) + 1` scoped by `euid_prefix`
    - never move the sequence backwards (avoid reusing previously-issued EUIs)
    """

    normalized = _normalize_instance_prefix(prefix)
    seq_name = f"{normalized.lower()}_instance_seq"

    session.execute(text(f'CREATE SEQUENCE IF NOT EXISTS "{seq_name}"'))
    session.execute(
        text(_build_ensure_instance_prefix_sequence_sql(seq_name)),
        {"prefix": normalized},
    )
