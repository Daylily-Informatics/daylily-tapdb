"""Post-restore verification: proving a restored schema is actually usable.

Row counts alone do not prove a recovery. These checks answer the questions an
operator would otherwise have to answer by hand at the worst possible moment:
are the references intact, is the audit trail continuous, can the objects be
looked up, and -- most importantly -- will the next insert reuse an identifier
that has already been handed out.

Split from ``verify.py`` for the same reason ``introspect.py`` was: preflight
and execution are orchestration, whereas these are SQL-heavy assertions with
very different testing needs. (Plan section 3.1 lists them under verify.py; this
is a cohesion split, not a change in behaviour.)

Two kinds of check live here, and they age differently:

* **Structural** checks are signature-driven, so they never go stale. EUID
  tables are discovered by their ``euid_prefix``/``euid_seq`` columns, row
  counts come from the manifest, and schema expectations come from the schema
  assets -- a table added next year is covered without editing this file.
* **Semantic** checks necessarily name the tables whose invariants they encode
  (lineage edges must reference instances; the audit trail must be
  continuous). Plan section 3.7 anticipates this: a new table carrying a new
  invariant needs a human to write that invariant down. Each guards with
  ``_table_exists`` and skips rather than failing when its table is absent, so
  a schema that legitimately lacks one is not reported as broken.
"""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy import text

from daylily_tapdb.backup import introspect
from daylily_tapdb.backup.introspect import quote_ident, quote_literal
from daylily_tapdb.backup.manifest import BackupManifest
from daylily_tapdb.backup.service import (
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_SKIP,
    STATUS_WARN,
    CheckResult,
)

#: How many sampled EUIDs to validate and resolve. Sampling rather than
#: exhaustive checking keeps verification bounded on large databases; the
#: structural checks below are the ones that must be exhaustive.
SAMPLE_LIMIT = 25


def _table_exists(session: Any, schema: str, table: str) -> bool:
    return (
        session.execute(
            text(
                "SELECT 1 FROM information_schema.tables "
                "WHERE table_schema = :s AND table_name = :t"
            ),
            {"s": schema, "t": table},
        ).first()
        is not None
    )


# ---------------------------------------------------------------------------
# 1. row counts
# ---------------------------------------------------------------------------


def check_rowcounts(session: Any, manifest: BackupManifest, schema: str) -> CheckResult:
    """Every table the manifest recorded must have exactly its row count."""
    live = introspect.capture_row_counts(session, schema)
    expected = manifest.row_counts
    mismatched = {
        table: {"expected": count, "live": live.get(table)}
        for table, count in expected.items()
        if live.get(table) != count
    }
    return CheckResult(
        id="rowcounts.exact",
        status=STATUS_PASS if not mismatched else STATUS_FAIL,
        detail=(
            f"{len(expected)} table(s) match the manifest"
            if not mismatched
            else f"{len(mismatched)} table(s) differ from the manifest"
        ),
        data=mismatched,
    )


# ---------------------------------------------------------------------------
# 2. template/instance references
# ---------------------------------------------------------------------------


def check_template_references(session: Any, schema: str) -> CheckResult:
    """No instance may point at a template that did not survive the restore."""
    if not _table_exists(session, schema, "generic_instance"):
        return CheckResult(
            id="refs.template_instance",
            status=STATUS_SKIP,
            detail="generic_instance is absent",
        )
    orphans = session.execute(
        text(
            f"""
            SELECT count(*)
            FROM {quote_ident(schema)}.generic_instance AS i
            LEFT JOIN {quote_ident(schema)}.generic_template AS t
                   ON t.uid = i.template_uid
            WHERE i.template_uid IS NOT NULL AND t.uid IS NULL
            """
        )
    ).scalar()
    count = int(orphans or 0)
    return CheckResult(
        id="refs.template_instance",
        status=STATUS_PASS if count == 0 else STATUS_FAIL,
        detail=(
            "every instance resolves to a template"
            if count == 0
            else f"{count} instance(s) reference a missing template"
        ),
        data={"orphans": count},
    )


# ---------------------------------------------------------------------------
# 3. lineage integrity
# ---------------------------------------------------------------------------


def check_lineage_integrity(session: Any, schema: str) -> CheckResult:
    """Lineage edges must reference instances that exist.

    Checked exhaustively rather than sampled: a dangling edge is a structural
    defect, and the count is cheap compared with the cost of discovering one
    later through a broken graph walk.
    """
    if not _table_exists(session, schema, "generic_instance_lineage"):
        return CheckResult(
            id="lineage.integrity",
            status=STATUS_SKIP,
            detail="generic_instance_lineage is absent",
        )
    row = session.execute(
        text(
            f"""
            SELECT
                count(*) FILTER (
                    WHERE l.parent_instance_uid IS NOT NULL AND p.uid IS NULL
                ) AS orphan_parents,
                count(*) FILTER (
                    WHERE l.child_instance_uid IS NOT NULL AND c.uid IS NULL
                ) AS orphan_children,
                count(*) AS edges
            FROM {quote_ident(schema)}.generic_instance_lineage AS l
            LEFT JOIN {quote_ident(schema)}.generic_instance AS p
                   ON p.uid = l.parent_instance_uid
            LEFT JOIN {quote_ident(schema)}.generic_instance AS c
                   ON c.uid = l.child_instance_uid
            """
        )
    ).first()
    orphan_parents = int(row[0] or 0)
    orphan_children = int(row[1] or 0)
    edges = int(row[2] or 0)
    broken = orphan_parents + orphan_children
    return CheckResult(
        id="lineage.integrity",
        status=STATUS_PASS if broken == 0 else STATUS_FAIL,
        detail=(
            f"{edges} lineage edge(s) all resolve"
            if broken == 0
            else f"{broken} lineage edge(s) reference missing instances"
        ),
        data={
            "edges": edges,
            "orphan_parents": orphan_parents,
            "orphan_children": orphan_children,
        },
    )


# ---------------------------------------------------------------------------
# 4. audit continuity
# ---------------------------------------------------------------------------


def check_audit_continuity(
    session: Any, manifest: BackupManifest, schema: str
) -> CheckResult:
    """The audit trail must be complete and its identity sequence ahead of it.

    A sequence behind ``max(uid)`` would make the next audit write collide --
    the same class of failure as EUID reuse, in the table whose whole purpose
    is to be trustworthy.
    """
    if not _table_exists(session, schema, "audit_log"):
        return CheckResult(
            id="audit.continuity", status=STATUS_SKIP, detail="audit_log is absent"
        )

    expected = manifest.row_counts.get("audit_log")
    row = session.execute(
        text(
            f"SELECT count(*), coalesce(max(uid), 0) "
            f"FROM {quote_ident(schema)}.audit_log"
        )
    ).first()
    live_count = int(row[0] or 0)
    max_uid = int(row[1] or 0)

    problems: dict[str, Any] = {}
    if expected is not None and live_count != expected:
        problems["count"] = {"expected": expected, "live": live_count}

    sequences = {seq.name: seq for seq in introspect.capture_sequences(session, schema)}
    identity = sequences.get("audit_log_uid_seq")
    # Compare the value the sequence will *issue next*, not its last_value.
    # `setval(s, 100, false)` and `setval(s, 100, true)` share a last_value but
    # issue 100 and 101 respectively -- so with max(uid)=100 the first collides
    # on the primary key while `last_value < max_uid` reports healthy. This is
    # the same class of bug already found and fixed in
    # `check_sequence_high_water`; it was left in place here.
    if identity is not None and identity.next_value is not None:
        if identity.next_value <= max_uid:
            problems["identity_sequence"] = {
                "last_value": identity.last_value,
                "is_called": identity.is_called,
                "next_value": identity.next_value,
                "max_uid": max_uid,
            }

    return CheckResult(
        id="audit.continuity",
        status=STATUS_PASS if not problems else STATUS_FAIL,
        detail=(
            f"{live_count} audit row(s); identity sequence ahead of max(uid)"
            if not problems
            else "audit trail is incomplete or its identity sequence lags"
        ),
        data=problems or {"rows": live_count, "max_uid": max_uid},
    )


# ---------------------------------------------------------------------------
# 5. EUID uniqueness
# ---------------------------------------------------------------------------


def check_euid_uniqueness(session: Any, schema: str) -> CheckResult:
    """No EUID may appear twice, anywhere.

    Tables are discovered by their ``euid_prefix``/``euid_seq`` column
    signature rather than named, so a new EUID-bearing table is covered the
    day it is created. A duplicate here means two objects share an identity --
    the failure the whole sequence-preservation design exists to prevent.
    """
    tables = introspect.euid_bearing_tables(session, schema)
    if not tables:
        return CheckResult(
            id="euid.uniqueness",
            status=STATUS_SKIP,
            detail="no EUID-bearing tables found",
        )

    union = " UNION ALL ".join(
        f"SELECT euid FROM {quote_ident(schema)}.{quote_ident(table)} "
        "WHERE euid IS NOT NULL"
        for table in tables
    )
    duplicates = session.execute(
        text(
            f"SELECT count(*) FROM ("
            f"  SELECT euid FROM ({union}) AS all_euids"
            f"  GROUP BY euid HAVING count(*) > 1"
            f") AS dupes"
        )
    ).scalar()
    count = int(duplicates or 0)

    return CheckResult(
        id="euid.uniqueness",
        status=STATUS_PASS if count == 0 else STATUS_FAIL,
        detail=(
            f"no duplicate EUIDs across {len(tables)} table(s)"
            if count == 0
            else f"{count} EUID(s) appear more than once"
        ),
        data={"tables": tables, "duplicates": count},
    )


def check_euid_format(session: Any, schema: str) -> CheckResult:
    """Sampled EUIDs must still parse and checksum correctly."""
    from daylily_tapdb.euid import validate_euid

    tables = introspect.euid_bearing_tables(session, schema)
    if not tables:
        return CheckResult(
            id="euid.format", status=STATUS_SKIP, detail="no EUID-bearing tables"
        )

    invalid: list[str] = []
    sampled = 0
    for table in tables:
        rows = session.execute(
            text(
                f"SELECT euid FROM {quote_ident(schema)}.{quote_ident(table)} "
                f"WHERE euid IS NOT NULL ORDER BY euid_seq DESC LIMIT :n"
            ),
            {"n": SAMPLE_LIMIT},
        ).scalars()
        for euid in rows:
            sampled += 1
            try:
                if not validate_euid(str(euid)):
                    invalid.append(str(euid))
            except Exception:
                invalid.append(str(euid))

    return CheckResult(
        id="euid.format",
        status=STATUS_PASS if not invalid else STATUS_FAIL,
        detail=(
            f"{sampled} sampled EUID(s) valid"
            if not invalid
            else f"{len(invalid)} sampled EUID(s) failed validation"
        ),
        data={"sampled": sampled, "invalid": invalid[:10]},
    )


# ---------------------------------------------------------------------------
# 6. sequence high-water -- the no-EUID-reuse guarantee
# ---------------------------------------------------------------------------


def check_sequence_high_water(
    session: Any, manifest: BackupManifest, schema: str
) -> CheckResult:
    """No sequence may be poised to reissue an identifier already handed out.

    Sequence values are captured after the dump, so the recorded position is a
    lower bound on what had been issued. A live sequence behind it would hand
    out identifiers that already exist -- silently, and only detectably later
    as a duplicate-key error or, worse, a wrong reference.

    The comparison is on ``next_value``, not ``last_value``. Those differ:
    ``setval(s, 5, false)`` and ``setval(s, 5, true)`` share a ``last_value``
    but issue ``5`` and ``6`` respectively. Comparing ``last_value`` called a
    sequence about to reissue ``5`` equal to one that had already issued it,
    which is exactly how an in-place restore reissued a live EUID while this
    check reported everything at or above its recorded value.

    A recorded ``next_value`` of ``None`` means the manifest predates that
    capture and genuinely cannot be compared; those are counted and reported
    rather than silently passed, so an old backup does not read as verified.
    """
    live = {seq.name: seq for seq in introspect.capture_sequences(session, schema)}
    regressed: dict[str, Any] = {}
    uncomparable: list[str] = []
    for recorded in manifest.sequences:
        current = live.get(recorded.name)
        if current is None:
            regressed[recorded.name] = "missing from the restored schema"
            continue
        if recorded.next_value is None:
            uncomparable.append(recorded.name)
            continue
        if (current.next_value or 0) < recorded.next_value:
            regressed[recorded.name] = {
                "recorded_next": recorded.next_value,
                "live_next": current.next_value,
                "recorded": {
                    "last_value": recorded.last_value,
                    "is_called": recorded.is_called,
                },
                "live": {
                    "last_value": current.last_value,
                    "is_called": current.is_called,
                },
            }
    comparable = len(manifest.sequences) - len(uncomparable)
    if regressed:
        detail = "a sequence would reissue already-used identifiers"
    elif uncomparable:
        detail = (
            f"{comparable} sequence(s) verified; {len(uncomparable)} not "
            "comparable (manifest predates exact sequence capture)"
        )
    else:
        detail = f"all {comparable} sequence(s) at or beyond their recorded position"

    return CheckResult(
        id="sequences.high_water",
        status=(
            STATUS_FAIL if regressed else (STATUS_WARN if uncomparable else STATUS_PASS)
        ),
        detail=detail,
        data=(
            regressed
            if regressed
            else ({"uncomparable": uncomparable} if uncomparable else {})
        ),
    )


def check_prefix_sequences_ahead(session: Any, schema: str) -> CheckResult:
    """Report per-prefix sequences sitting below the highest EUID in the data.

    **Advisory, not blocking, because it cannot cause EUID reuse.** Two
    independent things prevent that, and it is worth being precise about which
    applies when:

    * an insert through TAPDB calls ``sequences.ensure_instance_prefix_sequence``
      first, which moves the sequence to ``GREATEST(max(euid_seq) + 1,
      current)`` and never backwards -- so the write simply succeeds with a
      fresh identifier;
    * an insert that bypasses that path (raw SQL, another client) gets a
      duplicate value from the lagging sequence and is rejected by the
      ``euid`` unique constraint.

    So the outcome is either a correct EUID or a loud failure, never a reused
    identifier. Verified adversarially in
    ``test_backup_pg_lifecycle.py::test_a_rewound_sequence_cannot_reuse_an_euid``.

    This state also arises normally -- re-running ``db schema apply`` on a
    populated database leaves prefix sequences un-called while rows remain.

    Failing on it would block recovery for a condition TAPDB fixes itself,
    which is the opposite of what a restore check is for. The binding
    guarantee is ``sequences.high_water``: the restored sequence must not sit
    below what the backup recorded.
    """
    tables = introspect.euid_bearing_tables(session, schema)
    if not tables:
        return CheckResult(
            id="sequences.prefix_projection",
            status=STATUS_SKIP,
            detail="no EUID-bearing tables",
        )

    union = " UNION ALL ".join(
        f"SELECT euid_prefix, euid_seq FROM {quote_ident(schema)}."
        f"{quote_ident(table)} WHERE euid_prefix IS NOT NULL"
        for table in tables
    )
    highest = session.execute(
        text(
            f"SELECT lower(euid_prefix), max(euid_seq) FROM ({union}) AS e "
            f"GROUP BY lower(euid_prefix)"
        )
    ).all()

    live = {seq.name: seq for seq in introspect.capture_sequences(session, schema)}
    behind: dict[str, Any] = {}
    checked = 0
    for prefix, max_seq in highest:
        sequence = live.get(f"{prefix}_instance_seq")
        if sequence is None:
            continue
        checked += 1
        # Next-value again: a sequence poised to reissue max(euid_seq) is
        # behind its data even though last_value equals it.
        if (sequence.next_value or 0) <= int(max_seq or 0):
            behind[str(prefix)] = {
                "sequence_last_value": sequence.last_value,
                "sequence_next_value": sequence.next_value,
                "max_euid_seq": int(max_seq or 0),
            }

    return CheckResult(
        id="sequences.prefix_projection",
        status=STATUS_PASS if not behind else STATUS_WARN,
        detail=(
            f"{checked} prefix sequence(s) ahead of their highest issued EUID"
            if not behind
            else f"{len(behind)} prefix sequence(s) behind their data; "
            "reconciled automatically before the next EUID is issued"
        ),
        data=behind,
    )


# ---------------------------------------------------------------------------
# 7. schema drift
# ---------------------------------------------------------------------------


def check_schema_drift(session: Any, cfg: dict[str, Any], schema: str) -> CheckResult:
    """The restored schema must match the schema assets."""
    from pathlib import Path

    from daylily_tapdb.euid import GENERIC_TEMPLATE_PREFIX
    from daylily_tapdb.schema_inventory import (
        diff_schema_inventory,
        drift_entry_counts,
        find_schema_root,
        load_expected_schema_inventory,
        load_live_schema_inventory,
        schema_asset_files,
    )

    try:
        schema_root = find_schema_root(Path("tapdb_schema.sql"))
    except FileNotFoundError as exc:
        return CheckResult(id="schema.drift", status=STATUS_WARN, detail=str(exc))

    asset_paths = schema_asset_files(schema_root)
    expected = load_expected_schema_inventory(
        asset_paths,
        dynamic_sequence_name=f"{GENERIC_TEMPLATE_PREFIX.lower()}_instance_seq",
    )
    live = load_live_schema_inventory(session, schema_name=schema)
    report = diff_schema_inventory(
        expected,
        live,
        env="restore-target",
        database=str(cfg.get("database") or ""),
        # Strict so that an object present in the restored schema but absent
        # from the schema assets is reported. A restore that quietly gained
        # objects is as wrong as one that lost them.
        strict=True,
        expected_asset_paths=[str(path.resolve()) for path in asset_paths],
    )
    return CheckResult(
        id="schema.drift",
        status=STATUS_PASS if not report.has_drift else STATUS_FAIL,
        detail=(
            "restored schema matches the schema assets"
            if not report.has_drift
            else "restored schema differs from the schema assets"
        ),
        data={
            "missing": drift_entry_counts(report.missing),
            "unexpected": drift_entry_counts(report.unexpected),
        },
    )


# ---------------------------------------------------------------------------
# 8. representative objects
# ---------------------------------------------------------------------------


def _object_addressable_tables() -> set[str]:
    """Tables that ``find_object_by_euid`` can actually resolve.

    Derived from the same model classes the lookup queries rather than written
    out as strings, so the two cannot drift apart silently.

    This matters because not every EUID-bearing table is object-addressable:
    ``audit_log`` carries EUIDs but records events, not objects, and asking the
    object lookup to find one would fail for a reason that says nothing about
    the restore.
    """
    from daylily_tapdb.models.instance import generic_instance
    from daylily_tapdb.models.lineage import generic_instance_lineage
    from daylily_tapdb.models.template import generic_template

    return {
        model.__tablename__
        for model in (generic_template, generic_instance, generic_instance_lineage)
    }


def check_representative_objects(session: Any, manifest: BackupManifest) -> CheckResult:
    """Each sampled object must resolve through the normal lookup path.

    Going through ``find_object_by_euid`` rather than raw SQL is the point: it
    proves the restored data is reachable the way the application reaches it,
    including the soft-delete filter and polymorphic dispatch.

    Only samples from object-addressable tables are required to resolve; the
    rest are counted and reported, not failed.
    """
    from daylily_tapdb.services.object_lookup import find_object_by_euid

    samples = manifest.representative_objects[:SAMPLE_LIMIT]
    if not samples:
        return CheckResult(
            id="objects.representative",
            status=STATUS_SKIP,
            detail="manifest recorded no representative objects",
        )

    addressable = _object_addressable_tables()
    unresolved: list[str] = []
    checked = 0
    not_addressable = 0
    # Swallowing the exception and reporting "does not resolve" is true but
    # useless: when every sample fails it is almost never the data, it is the
    # lookup path erroring identically each time. Against a real restore this
    # reported "12 of 12 do not resolve" when the actual cause was
    # `column generic_template.validator_ref does not exist` -- a schema older
    # than the code. The operator needs the cause, not the symptom.
    first_error: Optional[str] = None

    for sample in samples:
        euid = str(sample.get("euid") or "")
        if not euid:
            continue
        if str(sample.get("table") or "") not in addressable:
            not_addressable += 1
            continue
        checked += 1
        try:
            found, _ = find_object_by_euid(session, euid)
        except Exception as exc:  # noqa: BLE001 - reported, not swallowed
            found = None
            if first_error is None:
                first_error = f"{type(exc).__name__}: {exc}"
        if found is None:
            unresolved.append(euid)

    if not checked:
        return CheckResult(
            id="objects.representative",
            status=STATUS_SKIP,
            detail="no sampled objects are addressable by EUID lookup",
            data={"not_addressable": not_addressable},
        )

    return CheckResult(
        id="objects.representative",
        status=STATUS_PASS if not unresolved else STATUS_FAIL,
        detail=(
            f"all {checked} addressable object(s) resolve"
            if not unresolved
            else (
                f"{len(unresolved)} of {checked} object(s) do not resolve"
                + (f" -- {first_error}" if first_error else "")
            )
        ),
        data={
            "checked": checked,
            "not_addressable": not_addressable,
            "unresolved": unresolved[:10],
            "lookup_error": first_error,
        },
    )


# ---------------------------------------------------------------------------
# Suite
# ---------------------------------------------------------------------------


def run_all(
    session: Any,
    cfg: dict[str, Any],
    manifest: BackupManifest,
    *,
    schema: Optional[str] = None,
) -> list[CheckResult]:
    """Run the full post-restore suite against one restored schema."""
    target = schema or str(cfg["schema_name"])
    return [
        check_rowcounts(session, manifest, target),
        check_template_references(session, target),
        check_lineage_integrity(session, target),
        check_audit_continuity(session, manifest, target),
        check_euid_uniqueness(session, target),
        check_euid_format(session, target),
        check_sequence_high_water(session, manifest, target),
        check_prefix_sequences_ahead(session, target),
        check_schema_drift(session, cfg, target),
        check_representative_objects(session, manifest),
    ]


def reconcile_sequences_to_floor(
    session: Any,
    schema: str,
    *,
    floor: list[Any],
) -> dict[str, Any]:
    """Advance sequences so none can reissue an identifier already handed out.

    **Why an in-place restore needs this.** Restoring rewinds rows *and*
    sequences to backup state. Any EUID issued between the backup and the
    restore is then re-issuable: the rows that carried those identifiers are
    gone, so ``max(euid_seq)`` drops back, and the sequence is restored from
    the archive. Nothing in the archive knows those identifiers ever existed --
    by construction, the backup predates them.

    ``sequences.high_water`` cannot cover this. It compares against what *the
    backup* recorded, and the backup is precisely the thing that is out of
    date. Observed: a marker row minted ``Z-GVR-5R``, an in-place restore
    rolled it back, and the next insert was issued ``Z-GVR-5R`` again -- a
    consumer holding that EUID would silently resolve to a different object.

    The pre-restore **safety backup** is the missing input. It is taken
    immediately before anything is touched, so its sequence positions cover
    everything ever issued on this target. Passing its sequences as ``floor``
    closes the gap.

    ``setval(seq, n, false)`` makes ``nextval()`` return exactly ``n``, which
    is why the floor is applied directly rather than as ``n - 1`` with
    ``is_called`` -- the latter breaks when ``n`` is the sequence minimum.

    Returns what was moved, for the receipt. Sequences already at or beyond
    their floor are left alone, so this is idempotent.
    """
    live = {seq.name: seq for seq in introspect.capture_sequences(session, schema)}
    advanced: dict[str, Any] = {}

    for recorded in floor:
        required = recorded.next_value
        if required is None:
            # Manifest predates exact capture; nothing to enforce.
            continue
        current = live.get(recorded.name)
        if current is None:
            continue
        if (current.next_value or 0) >= required:
            continue
        session.execute(
            text(
                # The regclass argument is a string *literal*, so the
                # qualified name is built with quote_ident and then escaped
                # as a literal. Using quote_ident for the outer quoting broke
                # on any sequence whose name contains a single quote -- in the
                # function that enforces the no-EUID-reuse guarantee.
                f"SELECT setval("
                f"{quote_literal(f'{quote_ident(schema)}.{quote_ident(recorded.name)}')}"
                f"::regclass, :value, false)"
            ),
            {"value": required},
        )
        advanced[recorded.name] = {
            "from_next": current.next_value,
            "to_next": required,
        }
    return advanced


__all__ = [
    "SAMPLE_LIMIT",
    "check_audit_continuity",
    "check_euid_format",
    "check_euid_uniqueness",
    "check_lineage_integrity",
    "check_prefix_sequences_ahead",
    "check_representative_objects",
    "check_rowcounts",
    "check_schema_drift",
    "check_sequence_high_water",
    "check_template_references",
    "reconcile_sequences_to_floor",
    "run_all",
]
