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

from typing import Any, Mapping, Optional

from sqlalchemy import text

from daylily_tapdb.backup import introspect
from daylily_tapdb.backup.errors import BackupVerificationError
from daylily_tapdb.backup.introspect import quote_ident
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
    session: Any,
    manifest: BackupManifest,
    schema: str,
    *,
    target: Optional[dict[str, Any]] = None,
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

    sequences = {
        seq.name: seq
        for seq in introspect.capture_sequences(
            session, schema, target=target or manifest.sequence_inventory["target"]
        )
    }
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
    session: Any,
    manifest: BackupManifest,
    schema: str,
    *,
    target: Optional[dict[str, Any]] = None,
) -> CheckResult:
    """Verify strictly ahead of all captured issued and assigned floors."""
    from daylily_tapdb.backup.recovery import (
        inventory_floors,
        require_retained_definitions,
    )
    from daylily_tapdb.sequences import (
        capture_sequence_inventory,
        verify_sequence_floors,
    )

    try:
        if not manifest.sequence_inventory or target is None:
            raise ValueError(
                "complete manifest sequence inventory and explicit target are required"
            )
        current = capture_sequence_inventory(session, schema_name=schema, target=target)
        require_retained_definitions(current, [manifest.sequence_inventory])
        result = verify_sequence_floors(
            current,
            floors=inventory_floors(manifest.sequence_inventory, source="backup"),
        )
        return CheckResult(
            id="sequences.high_water",
            status=STATUS_PASS if result["ok"] else STATUS_FAIL,
            detail="every generator is strictly ahead of all recorded floors",
            data=result,
        )
    except (ValueError, RuntimeError, BackupVerificationError) as exc:
        return CheckResult(
            id="sequences.high_water", status=STATUS_FAIL, detail=str(exc)
        )


def check_prefix_sequences_ahead(
    session: Any, schema: str, *, target: Optional[dict[str, Any]] = None
) -> CheckResult:
    """Missing, unmapped or lagging generators are blocking failures."""
    from daylily_tapdb.sequences import (
        capture_sequence_inventory,
        verify_sequence_floors,
    )

    try:
        if target is None:
            raise ValueError(
                "explicit target is required for complete generator verification"
            )
        current = capture_sequence_inventory(session, schema_name=schema, target=target)
        result = verify_sequence_floors(current, floors=[])
        return CheckResult(
            id="sequences.prefix_projection",
            status=STATUS_PASS if result["ok"] else STATUS_FAIL,
            detail="all mapped generators are ahead of assigned identities",
            data=result,
        )
    except (ValueError, RuntimeError, BackupVerificationError) as exc:
        return CheckResult(
            id="sequences.prefix_projection", status=STATUS_FAIL, detail=str(exc)
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


def _current_source_assets_match(manifest: BackupManifest) -> bool:
    """Version labels never select current-runtime semantic verification."""
    from pathlib import Path

    from daylily_tapdb.backup.inventory import schema_asset_checksums
    from daylily_tapdb.schema_inventory import find_schema_root, schema_asset_files

    if manifest.schema_drift.get("has_drift") is not False:
        return False
    current = schema_asset_checksums(
        schema_asset_files(find_schema_root(Path("tapdb_schema.sql")))
    )
    return (
        bool(current)
        and all(item["sha256"] for item in current)
        and (manifest.migrations.get("asset_checksums") == current)
    )


def _current_representative_lookup(
    session: Any, manifest: BackupManifest, schema: str
) -> CheckResult:
    """Use normal ORM lookups inside the retained, already fenced connection."""
    from sqlalchemy.orm import Session

    previous = session.execute(
        text("SELECT current_setting('search_path')")
    ).scalar_one()
    session.execute(
        text("SELECT set_config('search_path', :schema, true)"),
        {"schema": quote_ident(schema) + ", pg_catalog"},
    )
    try:
        with Session(bind=session, autoflush=False) as lookup:
            return check_representative_objects(lookup, manifest)
    finally:
        session.execute(
            text("SELECT set_config('search_path', :previous, true)"),
            {"previous": previous},
        )


def run_all(
    session: Any,
    cfg: dict[str, Any],
    manifest: BackupManifest,
    *,
    schema: Optional[str] = None,
    identity_inventory: Optional[dict[str, Any]] = None,
) -> list[CheckResult]:
    """Verify the restored historical source, before migration or principal bind."""
    from daylily_tapdb.backup.service import inventory_target
    from daylily_tapdb.identity_inventory import (
        capture_identity_inventory,
        verify_identity_inventory,
        with_inventory_limits,
    )

    target_schema = schema or str(cfg["schema_name"])
    target = inventory_target(cfg)
    checks = [
        check_rowcounts(session, manifest, target_schema),
        check_sequence_high_water(session, manifest, target_schema, target=target),
        check_prefix_sequences_ahead(session, target_schema, target=target),
    ]
    if identity_inventory is None:
        checks.append(
            CheckResult(
                id="identity.preservation",
                status=STATUS_FAIL,
                detail="backup lacks the required exhaustive physical identity asset",
            )
        )
        return checks
    actual = capture_identity_inventory(
        session,
        schema_name=target_schema,
        target=with_inventory_limits(target, identity_inventory),
    )
    result = verify_identity_inventory(
        identity_inventory,
        actual,
        conversion_manifest={
            "schema_version": "tapdb-identity-conversion/v1",
            "target": actual["target"],
            "tables": {},
            "added_tables": [],
        },
    )
    checks.append(
        CheckResult(
            id="identity.preservation",
            status=STATUS_PASS if result["ok"] else STATUS_FAIL,
            detail="exhaustive original rows, identities and schema compared",
            data=result,
        )
    )
    if result["ok"] and _current_source_assets_match(manifest):
        drift = check_schema_drift(session, cfg, target_schema)
        checks.append(drift)
        if drift.status == STATUS_PASS:
            checks.extend(
                [
                    check_template_references(session, target_schema),
                    check_lineage_integrity(session, target_schema),
                    check_audit_continuity(
                        session, manifest, target_schema, target=target
                    ),
                    check_euid_uniqueness(session, target_schema),
                    check_euid_format(session, target_schema),
                    _current_representative_lookup(session, manifest, target_schema),
                ]
            )
    return checks


def reconcile_sequences_to_floor(
    session: Any,
    schema: str,
    *,
    floor: list[Any],
    target: dict[str, Any],
    writer_fence: dict[str, Any],
    receipts_dir: Any,
    recovery_family: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    """Delegate every advance to the shared transactional allocator."""
    from daylily_tapdb.sequences import (
        apply_sequence_advance_plan,
        build_sequence_advance_plan,
        capture_sequence_inventory,
    )

    current = capture_sequence_inventory(session, schema_name=schema, target=target)
    floors = []
    for record in floor:
        if isinstance(record, dict) and set(("name", "value", "source")).issubset(
            record
        ):
            floors.append(record)
            continue
        if record.next_value is None:
            raise ValueError(
                f"generator {record.name} lacks complete retained metadata"
            )
        for field in ("allocated_floor", "assigned_floor"):
            value = getattr(record, field)
            if value is not None:
                floors.append(
                    {"name": record.name, "value": value, "source": f"retained:{field}"}
                )
    plan = build_sequence_advance_plan(
        current, floors=floors, recovery_family=recovery_family
    )
    return apply_sequence_advance_plan(
        session, plan, writer_fence=writer_fence, receipts_dir=receipts_dir
    )


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
