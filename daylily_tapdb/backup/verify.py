"""Restore preflight, staging, and execution.

The organising rule: **every check runs before anything is mutated.** A restore
that is going to fail should fail while the target is still untouched, so the
corruption gate, version gate, identity gate, and capacity gate all complete
before the first `CREATE DATABASE` or `ALTER SCHEMA`.

Two restore shapes:

* ``isolated`` (default) restores into a separate database, leaving live data
  alone entirely. Failure drops what it created.
* ``in_place`` replaces the configured schema, and is the only path that can
  destroy data. It never does so directly: the live schema is renamed aside,
  the replacement is restored and verified beside it, and only then is the
  previous schema dropped. Every failure path renames the original back.

Staging is stateless. ``plan_restore`` returns a fingerprint over everything
that would affect the outcome; apply re-plans and compares, so an operator can
never confirm one thing and have another happen -- and no server-side session
state is needed, which keeps it correct under multiple workers.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import text

from daylily_tapdb.backup import engine, introspect, service
from daylily_tapdb.backup.errors import (
    BackupPolicyBlockedError,
    BackupVerificationError,
    RestoreConfirmationError,
    RestoreStageStaleError,
)
from daylily_tapdb.backup.introspect import quote_ident, quote_literal
from daylily_tapdb.backup.manifest import (
    BACKUP_CLASS_FULL,
    PROVENANCE_RESTORE,
    BackupManifest,
    canonical_bytes,
    sha256_hex,
)
from daylily_tapdb.backup.receipts import (
    OPERATION_REHEARSE,
    OPERATION_RESTORE,
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    SURFACE_CLI,
    Actor,
    write_receipt,
)
from daylily_tapdb.backup.service import (
    STATUS_FAIL,
    STATUS_PASS,
    STATUS_SKIP,
    STATUS_WARN,
    CheckResult,
)

MODE_ISOLATED = "isolated"
MODE_IN_PLACE = "in_place"
RESTORE_MODES = (MODE_ISOLATED, MODE_IN_PLACE)

POLICY_BLOCKED = "blocked"
POLICY_ALLOWED = "allowed"
POLICY_CONFIRM_REQUIRED = "confirm_required"

#: PostgreSQL truncates identifiers past this silently, which would discard
#: the digest that makes an isolated target unique per backup.
_PG_IDENTIFIER_LIMIT = 63

#: Headroom over the artifact size before a local restore is attempted.
DISK_HEADROOM_FACTOR = 1.2


class _PreflightSessionError(RuntimeError):
    """Target could not be inspected during preflight.

    A private wrapper so the surrounding staging ``try`` only converts genuine
    session failures into a check, rather than swallowing bugs in the checks
    themselves.
    """


@dataclass(frozen=True)
class RestoreOptions:
    """Everything an operator can vary about a restore."""

    mode: str = MODE_ISOLATED
    target_database: Optional[str] = None
    target_schema: Optional[str] = None
    allow_identity_mismatch: bool = False
    allow_unknown_migrations: bool = False
    #: Proceed even when the backup carries EUID prefixes this target's
    #: governance context cannot claim. Off by default: restoring one client's
    #: identifiers into another client's target is a real hazard. But every
    #: TapDB schema carries the base `TPX` prefix *plus* client prefixes, and
    #: those live in different registries -- so without an override the gate
    #: blocked every restore in a real deployment, with no way through.
    allow_unclaimable_prefixes: bool = False
    keep_superseded: bool = False

    def normalized_mode(self) -> str:
        mode = str(self.mode or MODE_ISOLATED).strip().lower().replace("-", "_")
        if mode not in RESTORE_MODES:
            raise ValueError(
                f"Unknown restore mode {self.mode!r}; expected one of: "
                + ", ".join(RESTORE_MODES)
            )
        return mode

    def validated_target_database(self) -> Optional[str]:
        """Return the requested database name, rejecting anything unsafe.

        These names reach DDL and a catalog lookup, so they are validated as
        PostgreSQL identifiers rather than trusted. Quoting at the call sites
        is the second layer; this is the first.
        """
        if not self.target_database:
            return None
        from daylily_tapdb.cli.db_config import validate_postgres_identifier_component

        return validate_postgres_identifier_component(
            self.target_database, field_name="restore.target_database"
        )

    def validated_target_schema(self) -> Optional[str]:
        """Return the requested schema name, rejecting anything unsafe."""
        if not self.target_schema:
            return None
        from daylily_tapdb.cli.db_config import validate_postgres_identifier_component

        return validate_postgres_identifier_component(
            self.target_schema, field_name="restore.target_schema"
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "mode": self.normalized_mode(),
            "target_database": self.target_database,
            "target_schema": self.target_schema,
            "allow_identity_mismatch": self.allow_identity_mismatch,
            "allow_unknown_migrations": self.allow_unknown_migrations,
            "allow_unclaimable_prefixes": self.allow_unclaimable_prefixes,
            "keep_superseded": self.keep_superseded,
        }


@dataclass(frozen=True)
class RestorePlan:
    """A staged restore: what would happen, and the token that pins it."""

    backup_id: str
    mode: str
    target_database: str
    target_schema: str
    source_label: Optional[str]
    required_confirm_target: str
    plan_fingerprint: str
    #: The schema name baked into the archive. A custom-format dump recreates
    #: the schema it was captured from, so restoring under a different name is
    #: a rename *after* the restore -- which means the executor has to know
    #: both names. Without this the rename could not happen at all, and
    #: ``--target-schema`` silently restored under the original name while
    #: verification ran against a schema that did not exist.
    source_schema: str = ""
    #: Whether apply will actually check ``required_confirm_target``. Surfaces
    #: render the typed-label control from this rather than deciding for
    #: themselves, so no surface can imply a check the service does not make.
    confirmation_required: bool = True
    steps: list[str] = field(default_factory=list)
    checks: list[CheckResult] = field(default_factory=list)
    options: dict[str, Any] = field(default_factory=dict)

    @property
    def blocking(self) -> list[CheckResult]:
        return [check for check in self.checks if check.failed]

    @property
    def ok(self) -> bool:
        return not self.blocking

    def to_payload(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "mode": self.mode,
            "target_database": self.target_database,
            "target_schema": self.target_schema,
            "source_schema": self.source_schema,
            "source_label": self.source_label,
            "required_confirm_target": self.required_confirm_target,
            "confirmation_required": self.confirmation_required,
            "plan_fingerprint": self.plan_fingerprint,
            "ok": self.ok,
            "steps": self.steps,
            "checks": [check.to_payload() for check in self.checks],
            "options": self.options,
        }


@dataclass(frozen=True)
class RestoreResult:
    """Outcome of an applied restore."""

    backup_id: str
    mode: str
    target_database: str
    target_schema: str
    checks: list[CheckResult] = field(default_factory=list)
    receipt_id: Optional[str] = None
    safety_backup_id: Optional[str] = None
    superseded_schema: Optional[str] = None
    #: Sequences pushed forward past what the target had already issued, so an
    #: in-place restore cannot reissue an EUID a consumer already holds.
    #: ``{name: {"from_next": int, "to_next": int}}``.
    sequences_advanced: dict[str, Any] = field(default_factory=dict)
    quarantined: bool = False
    dry_run: bool = False
    plan: Optional[RestorePlan] = None

    @property
    def ok(self) -> bool:
        return not self.quarantined and not any(c.failed for c in self.checks)

    def to_payload(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "mode": self.mode,
            "target_database": self.target_database,
            "target_schema": self.target_schema,
            "ok": self.ok,
            "quarantined": self.quarantined,
            "dry_run": self.dry_run,
            "receipt_id": self.receipt_id,
            "safety_backup_id": self.safety_backup_id,
            "superseded_schema": self.superseded_schema,
            "sequences_advanced": self.sequences_advanced,
            "checks": [check.to_payload() for check in self.checks],
            "plan": self.plan.to_payload() if self.plan else None,
        }


# ---------------------------------------------------------------------------
# Naming
# ---------------------------------------------------------------------------


def _stamp(now: Optional[datetime] = None) -> str:
    return (now or datetime.now(UTC)).astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")


def isolated_database_name(cfg: dict[str, Any], *, backup_id: str) -> str:
    """Name for the throwaway database an isolated restore creates.

    Derived from the backup id, **not** from the clock. The name is part of
    the plan fingerprint, so a timestamp here would make every re-stage
    produce a different fingerprint: staging and applying a second apart would
    then fail as ``stale_stage``, which is exactly what a human using the
    review page would hit every time.

    Re-staging the same backup always yields the same target, so the plan
    tells the truth about what apply will create.

    The database prefix is truncated so the digest always survives. Only the
    backup id was hashed before, and the docstring claimed the result stayed
    within PostgreSQL's 63-character identifier limit -- it did not. For a
    database name of 43 characters or more, PostgreSQL's silent truncation cut
    the digest off entirely and *every* backup's isolated restore collided on
    one target name.
    """
    digest = sha256_hex(str(backup_id).encode("utf-8"))[:12]
    suffix = f"_restore_{digest}"
    database = str(cfg["database"])
    return f"{database[: _PG_IDENTIFIER_LIMIT - len(suffix)]}{suffix}"


def superseded_schema_name(schema: str, *, now: Optional[datetime] = None) -> str:
    """Name the live schema is renamed to before an in-place restore."""
    return f"{schema}_superseded_{_stamp(now)}"


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------


def _check_artifact_integrity(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    backup_id: str,
) -> list[CheckResult]:
    """Corruption gate: full deep verification before anything is touched."""
    report = service.verify_backup(
        cfg,
        settings,
        backup_id=backup_id,
        level=service.VERIFY_DEEP,
        record_receipt=False,
    )
    return list(report.checks)


def _check_version_compatibility(
    manifest: BackupManifest,
    target_versions: dict[str, Any],
) -> CheckResult:
    """PostgreSQL only restores forward; catch a backward restore early."""
    source = manifest.postgres.get("server_version")
    target = target_versions.get("server_version")
    try:
        engine.assert_restore_target_is_new_enough(
            source_server_version=source,
            target_server_version=target,
        )
    except Exception as exc:
        return CheckResult(
            id="version.compatible",
            status=STATUS_FAIL,
            detail=str(exc),
            data={"source": source, "target": target},
        )
    return CheckResult(
        id="version.compatible",
        status=STATUS_PASS,
        detail=f"source {source} -> target {target}",
    )


def _check_restore_client_version(manifest: BackupManifest) -> CheckResult:
    """The local ``pg_restore`` must be at least as new as the dump.

    An older client cannot read a newer archive format, and finds out only
    after it has begun writing to the target.
    """
    client = engine.client_version("pg_restore")
    dumped_by = manifest.content_inventory.get("dumped_by_version")
    client_major = engine.parse_version_major(client)
    dump_major = engine.parse_version_major(dumped_by)

    if client is None:
        return CheckResult(
            id="client.pg_restore",
            status=STATUS_FAIL,
            detail="pg_restore not found on PATH",
        )
    if client_major is None or dump_major is None:
        return CheckResult(id="client.pg_restore", status=STATUS_PASS, detail=client)
    return CheckResult(
        id="client.pg_restore",
        status=STATUS_PASS if client_major >= dump_major else STATUS_FAIL,
        detail=(
            f"pg_restore {client_major} can read an archive written by "
            f"pg_dump {dump_major}"
            if client_major >= dump_major
            else f"pg_restore {client_major} is older than the pg_dump "
            f"{dump_major} that wrote this archive"
        ),
        data={"client_major": client_major, "dump_major": dump_major},
    )


def _check_prefix_claimability(
    cfg: dict[str, Any],
    manifest: BackupManifest,
    *,
    allow_unclaimable: bool = False,
) -> CheckResult:
    """Every EUID prefix in the backup must be claimable by this target.

    Restoring rows whose prefixes belong to a different owner repo would put
    identifiers into a database with no right to issue them -- the governance
    equivalent of an identity mismatch, and not something a row count would
    ever reveal.
    """
    from daylily_tapdb.governance import resolve_prefix_owner_repo_name

    prefixes = sorted(
        {
            str(sample.get("euid_prefix"))
            for sample in manifest.representative_objects
            if sample.get("euid_prefix")
        }
    )
    if not prefixes:
        return CheckResult(
            id="governance.prefixes",
            status=STATUS_SKIP,
            detail="backup records no EUID prefixes",
        )

    registry_path = cfg.get("prefix_ownership_registry_path")
    if not registry_path:
        return CheckResult(
            id="governance.prefixes",
            status=STATUS_WARN,
            detail="no prefix ownership registry configured",
        )

    domain_code = str(cfg.get("domain_code") or "")
    owner = str(cfg.get("owner_repo_name") or "")
    unclaimable: dict[str, str] = {}
    for prefix in prefixes:
        try:
            resolved = resolve_prefix_owner_repo_name(
                domain_code, prefix, path=registry_path
            )
        except Exception as exc:
            unclaimable[prefix] = str(exc)
            continue
        if owner and resolved and resolved != owner:
            unclaimable[prefix] = f"owned by {resolved}, target is {owner}"

    return CheckResult(
        id="governance.prefixes",
        status=(
            STATUS_PASS
            if not unclaimable
            else (STATUS_WARN if allow_unclaimable else STATUS_FAIL)
        ),
        detail=(
            f"all {len(prefixes)} prefix(es) claimable by {domain_code}/{owner}"
            if not unclaimable
            else f"{len(unclaimable)} prefix(es) not claimable by this target"
        ),
        data={"prefixes": prefixes, "unclaimable": unclaimable},
    )


def _check_migration_inventory(
    manifest: BackupManifest,
    *,
    allow_unknown: bool,
) -> CheckResult:
    """Refuse a backup carrying migrations this build does not know about.

    A migration the tool has never seen means the artifact came from a newer
    schema than this code understands, so its post-restore expectations would
    be wrong.
    """
    from daylily_tapdb.schema_inventory import find_schema_root, schema_asset_files

    try:
        schema_root = find_schema_root(Path("tapdb_schema.sql"))
    except FileNotFoundError as exc:
        return CheckResult(id="migrations.known", status=STATUS_WARN, detail=str(exc))

    known = {path.name for path in schema_asset_files(schema_root)}
    recorded = {
        str(entry.get("name"))
        for entry in (manifest.migrations.get("asset_checksums") or [])
        if entry.get("name")
    }
    unknown = sorted(recorded - known)

    if not unknown:
        return CheckResult(
            id="migrations.known",
            status=STATUS_PASS,
            detail=f"{len(recorded)} schema asset(s) recognised",
        )
    return CheckResult(
        id="migrations.known",
        status=STATUS_WARN if allow_unknown else STATUS_FAIL,
        detail=(
            "backup references schema assets this build does not know: "
            + ", ".join(unknown)
        ),
        data={"unknown": unknown},
    )


def _check_identity(
    cfg: dict[str, Any],
    manifest: BackupManifest,
    *,
    allow_mismatch: bool,
) -> CheckResult:
    """Refuse to restore one domain's data over another's."""
    identity = manifest.target_identity
    mismatches = {}
    for key in ("domain_code", "owner_repo_name"):
        recorded = identity.get(key)
        current = cfg.get(key)
        if recorded and current and str(recorded) != str(current):
            mismatches[key] = {"backup": recorded, "target": current}

    if not mismatches:
        return CheckResult(
            id="identity.match",
            status=STATUS_PASS,
            detail=f"{identity.get('domain_code')}/{identity.get('owner_repo_name')}",
        )
    return CheckResult(
        id="identity.match",
        status=STATUS_WARN if allow_mismatch else STATUS_FAIL,
        detail="backup identity differs from the configured target",
        data=mismatches,
    )


def _check_governance(cfg: dict[str, Any], manifest: BackupManifest) -> CheckResult:
    """Compare governance registry checksums to what the backup recorded."""
    from daylily_tapdb.backup.manifest import sha256_file

    drifted: dict[str, Any] = {}
    compared: list[str] = []
    for key in ("prefix_ownership_registry_path", "domain_registry_path"):
        recorded = manifest.governance.get(key)
        if not isinstance(recorded, dict) or not recorded.get("sha256"):
            continue
        current_path = cfg.get(key)
        if not current_path:
            continue
        path = Path(str(current_path)).expanduser()
        current = sha256_file(path) if path.is_file() else None
        compared.append(key)
        if current != recorded.get("sha256"):
            drifted[key] = {"backup": recorded.get("sha256"), "target": current}

    if not drifted:
        # Distinguish "compared them and they match" from "had nothing to
        # compare". Reporting both as a bare pass let an unverifiable state
        # read as a verified one.
        if not compared:
            return CheckResult(
                id="governance.registries",
                status=STATUS_SKIP,
                detail="backup recorded no registry checksums to compare",
            )
        return CheckResult(
            id="governance.registries",
            status=STATUS_PASS,
            detail=f"{len(compared)} registry checksum(s) match the backup",
            data={"compared": compared},
        )
    # A warning, not a failure: registries legitimately gain entries over time,
    # and refusing a restore because a prefix was added elsewhere would block
    # recovery for a reason unrelated to the data.
    return CheckResult(
        id="governance.registries",
        status=STATUS_WARN,
        detail="governance registries changed since the backup was taken",
        data=drifted,
    )


def _check_disk_space(
    manifest: BackupManifest,
    settings: dict[str, Any],
    storage: Any,
) -> CheckResult:
    """Require headroom over the artifact size for a local restore."""
    import shutil as _shutil

    if storage.describe().get("backend") != "local":
        return CheckResult(
            id="storage.capacity",
            status=STATUS_SKIP,
            detail="remote storage; capacity is not checked locally",
        )
    needed = int(
        sum(asset.bytes for asset in manifest.included_assets) * DISK_HEADROOM_FACTOR
    )
    try:
        free = _shutil.disk_usage(Path(settings["config_dir"])).free
    except OSError as exc:
        return CheckResult(id="storage.capacity", status=STATUS_WARN, detail=str(exc))
    return CheckResult(
        id="storage.capacity",
        status=STATUS_PASS if free >= needed else STATUS_FAIL,
        detail=f"{free // (1024 * 1024)} MiB free, {needed // (1024 * 1024)} MiB needed",
        data={"free_bytes": free, "needed_bytes": needed},
    )


def _check_rls_roles(
    session: Any,
    manifest: BackupManifest,
    *,
    archive_path: Optional[Path] = None,
) -> CheckResult:
    """When the archive carries policies, the roles they name must exist.

    Roles are cluster-scoped and deliberately excluded from the artifact, so a
    policy referencing a missing role fails partway through a restore, after
    mutation has begun. This reads the policy statements out of the archive and
    resolves each named role against the target, so that failure happens in
    preflight instead.

    Counting policies would prove nothing -- the roles are not in the table of
    contents, only in the statements themselves.
    """
    counts = manifest.content_inventory.get("counts_by_kind") or {}
    policy_count = int(counts.get("POLICY") or 0)
    if not policy_count:
        return CheckResult(
            id="rls.roles",
            status=STATUS_SKIP,
            detail="archive contains no policies",
        )
    if archive_path is None:
        return CheckResult(
            id="rls.roles",
            status=STATUS_WARN,
            detail=f"{policy_count} policies present but the archive was not read",
        )

    rendered = engine.run_command(
        engine.build_pg_restore_sql_command(archive_path, section="post-data")
    )
    if not rendered.ok:
        return CheckResult(
            id="rls.roles",
            status=STATUS_WARN,
            detail=f"could not render policy statements: {rendered.output[:160]}",
        )

    required = engine.policy_roles(rendered.stdout)
    if not required:
        return CheckResult(
            id="rls.roles",
            status=STATUS_PASS,
            detail=(
                f"{policy_count} policies apply to PUBLIC; no target roles required"
            ),
        )

    present = {
        str(name)
        for name in session.execute(text("SELECT rolname FROM pg_roles")).scalars()
    }
    missing = sorted(required - present)
    return CheckResult(
        id="rls.roles",
        status=STATUS_PASS if not missing else STATUS_FAIL,
        detail=(
            f"all {len(required)} policy role(s) exist on the target"
            if not missing
            else "policies reference roles the target does not have: "
            + ", ".join(missing)
        ),
        data={"required": sorted(required), "missing": missing},
    )


def _check_target_emptiness(
    cfg: dict[str, Any],
    *,
    mode: str,
    target_database: str,
    target_schema: str,
) -> CheckResult:
    """An isolated restore must not land on top of existing data.

    This inspects the **restore target** database, not the one currently
    configured -- they are different databases, and asking the wrong one would
    always see the live schema and refuse every restore.

    A database that does not exist yet is fine: it is created from
    ``template0`` and is therefore empty by construction. The case worth
    guarding is a pre-created ``--target-database``, which is how Aurora
    restores work when the role lacks CREATEDB.
    """
    if mode == MODE_IN_PLACE:
        return CheckResult(
            id="target.empty",
            status=STATUS_SKIP,
            detail="in-place replaces the configured schema by design",
        )

    if not _database_exists(cfg, target_database):
        return CheckResult(
            id="target.empty",
            status=STATUS_PASS,
            detail=f"{target_database} will be created from template0",
        )

    operator_cfg = service.connection_config_for_role(cfg, "operator")
    probe = engine.run_command(
        engine.build_psql_command(
            operator_cfg,
            sql=(
                "SELECT count(*) FROM information_schema.schemata "
                "WHERE schema_name = " + quote_literal(target_schema)
            ),
            database=target_database,
        ),
        env=engine.client_env(operator_cfg),
    )
    if not probe.ok:
        return CheckResult(
            id="target.empty",
            status=STATUS_WARN,
            detail=f"could not inspect {target_database}: {probe.output[:160]}",
        )
    occupied = probe.stdout.strip() not in ("", "0")
    return CheckResult(
        id="target.empty",
        status=STATUS_FAIL if occupied else STATUS_PASS,
        detail=(
            f"{target_database} already contains schema {target_schema}"
            if occupied
            else f"{target_database} exists and is free of {target_schema}"
        ),
    )


def _check_active_connections(session: Any, database: str) -> CheckResult:
    """Report other sessions on the target.

    Advisory, not blocking: the operator chooses the moment. An in-place
    restore renames the schema out from under anything connected.
    """
    try:
        count = session.execute(
            text(
                "SELECT count(*) FROM pg_stat_activity "
                "WHERE datname = :db AND pid <> pg_backend_pid()"
            ),
            {"db": database},
        ).scalar()
    except Exception as exc:  # pragma: no cover - permission dependent
        return CheckResult(id="target.connections", status=STATUS_SKIP, detail=str(exc))
    count = int(count or 0)
    return CheckResult(
        id="target.connections",
        status=STATUS_PASS if count == 0 else STATUS_WARN,
        detail=f"{count} other session(s) connected to {database}",
        data={"connections": count},
    )


def _check_createdb_privilege(session: Any, *, mode: str) -> CheckResult:
    """An isolated restore creates a database, which needs the privilege."""
    if mode == MODE_IN_PLACE:
        return CheckResult(id="target.createdb", status=STATUS_SKIP)
    try:
        allowed = session.execute(
            text("SELECT rolcreatedb FROM pg_roles WHERE rolname = current_user")
        ).scalar()
    except Exception as exc:  # pragma: no cover - permission dependent
        return CheckResult(id="target.createdb", status=STATUS_WARN, detail=str(exc))
    return CheckResult(
        id="target.createdb",
        status=STATUS_PASS if allowed else STATUS_FAIL,
        detail=(
            "current role may create databases"
            if allowed
            else "current role lacks CREATEDB; supply a pre-created "
            "--target-database instead"
        ),
    )


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------


def compute_plan_fingerprint(
    *,
    manifest: BackupManifest,
    target_label: str,
    mode: str,
    target_database: str,
    target_schema: str,
    tables: list[str],
    allow_identity_mismatch: bool = False,
    allow_unknown_migrations: bool = False,
    allow_unclaimable_prefixes: bool = False,
) -> str:
    """Hash everything that would change what an apply actually does.

    Re-planned at apply time and compared, so a confirmation can only ever
    authorise the exact operation it was shown.

    The override flags are part of that. They were omitted, which meant a plan
    staged with defaults -- and *blocked* on `identity.match` -- produced the
    same fingerprint as one staged with `allow_identity_mismatch`, so the
    blocked plan's fingerprint could be replayed with the override to restore
    one domain's data over another's. Anything that can turn a blocking check
    into a passing one changes what apply does and belongs in the hash.
    """
    return sha256_hex(
        canonical_bytes(
            {
                "manifest_checksum": manifest.checksum(),
                "backup_id": manifest.backup_id,
                "target_label": target_label,
                "mode": mode,
                "target_database": target_database,
                "target_schema": target_schema,
                "tables": sorted(tables),
                "allow_identity_mismatch": bool(allow_identity_mismatch),
                "allow_unknown_migrations": bool(allow_unknown_migrations),
                "allow_unclaimable_prefixes": bool(allow_unclaimable_prefixes),
            }
        )
    )


# ---------------------------------------------------------------------------
# plan_restore
# ---------------------------------------------------------------------------


def plan_restore(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_id: str,
    options: Optional[RestoreOptions] = None,
    now: Optional[datetime] = None,
) -> RestorePlan:
    """Stage a restore. Reads only -- never mutates the target."""
    resolved = options or RestoreOptions()
    mode = resolved.normalized_mode()
    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup_id)
    manifest = service._load_manifest(storage, prefix)

    source_schema = str(manifest.target_identity.get("schema_name") or "")
    target_schema = str(resolved.target_schema or source_schema)
    if mode == MODE_IN_PLACE:
        target_database = str(cfg["database"])
        target_schema = str(cfg["schema_name"])
    else:
        target_database = str(
            resolved.target_database or isolated_database_name(cfg, backup_id=backup_id)
        )

    checks: list[CheckResult] = []
    # Class first. This was only enforced inside `_restore_archive`, which runs
    # after the safety backup and after the live schema has been renamed aside
    # -- so restoring a template-pack in place cost a full safety backup, an
    # ACCESS EXCLUSIVE lock, and a window where the live schema name did not
    # exist, before refusing. The module's rule is that every check runs before
    # anything is mutated.
    checks.append(_check_restorable_class(manifest))
    scope_check = _check_restore_scope(cfg, manifest, mode=mode)
    checks.append(scope_check)
    checks.extend(_check_artifact_integrity(cfg, settings, backup_id))
    checks.append(
        _check_migration_inventory(
            manifest, allow_unknown=resolved.allow_unknown_migrations
        )
    )
    checks.append(
        _check_identity(cfg, manifest, allow_mismatch=resolved.allow_identity_mismatch)
    )
    checks.append(_check_governance(cfg, manifest))
    checks.append(
        _check_prefix_claimability(
            cfg, manifest, allow_unclaimable=resolved.allow_unclaimable_prefixes
        )
    )
    checks.append(_check_restore_client_version(manifest))
    checks.append(_check_disk_space(manifest, settings, storage))
    checks.append(
        _check_target_emptiness(
            cfg,
            mode=mode,
            target_database=target_database,
            target_schema=target_schema,
        )
    )

    # Stage the artifact once so policy statements can be read out of it; the
    # roles a policy grants to are not in the table of contents.
    import shutil as _shutil
    import tempfile as _tempfile

    staged_dir = Path(_tempfile.mkdtemp(prefix="tapdb-preflight-"))
    try:
        archive_path: Optional[Path] = None
        if manifest.included_assets:
            asset = manifest.included_assets[0]
            try:
                archive_path = storage.get_file(
                    f"{prefix}/{asset.name}", staged_dir / asset.name
                )
            except Exception:  # pragma: no cover - integrity check already failed
                archive_path = None

        try:
            with service.open_session(
                cfg,
                app_username="tapdb_restore_plan",
                connection_role="operator",
            ) as conn:
                with conn.session_scope(commit=False) as session:
                    versions = introspect.server_version(session)
                    checks.append(_check_version_compatibility(manifest, versions))
                    checks.append(
                        _check_rls_roles(session, manifest, archive_path=archive_path)
                    )
                    checks.append(_check_createdb_privilege(session, mode=mode))
                    checks.append(
                        _check_active_connections(session, str(cfg["database"]))
                    )
        except Exception as exc:
            raise _PreflightSessionError(str(exc)) from exc
    except _PreflightSessionError as exc:
        checks.append(
            CheckResult(
                id="target.reachable",
                status=STATUS_FAIL,
                detail=f"could not read the target: {exc}",
            )
        )
    finally:
        _shutil.rmtree(staged_dir, ignore_errors=True)

    fingerprint = compute_plan_fingerprint(
        manifest=manifest,
        target_label=service.target_label(cfg),
        mode=mode,
        target_database=target_database,
        target_schema=target_schema,
        tables=sorted(manifest.row_counts),
        allow_identity_mismatch=resolved.allow_identity_mismatch,
        allow_unknown_migrations=resolved.allow_unknown_migrations,
        allow_unclaimable_prefixes=resolved.allow_unclaimable_prefixes,
    )

    return RestorePlan(
        backup_id=backup_id,
        mode=mode,
        target_database=target_database,
        target_schema=target_schema,
        source_schema=source_schema,
        source_label=manifest.target_identity.get("target_label"),
        required_confirm_target=service.target_label(cfg),
        confirmation_required=confirmation_required(cfg, mode),
        plan_fingerprint=fingerprint,
        steps=(
            ["refuse in-place restore: archive is not physically schema-complete"]
            if mode == MODE_IN_PLACE and scope_check.failed
            else _describe_steps(
                mode=mode,
                target_database=target_database,
                target_schema=target_schema,
                keep_superseded=resolved.keep_superseded,
                source_schema=source_schema,
            )
        ),
        checks=checks,
        options=resolved.to_payload(),
    )


def _check_restore_scope(
    cfg: dict[str, Any], manifest: BackupManifest, *, mode: str
) -> CheckResult:
    """Require signed evidence that ``full`` is physically schema-complete."""
    del cfg, mode
    recorded = manifest.target_identity.get("data_scope")
    if not isinstance(recorded, dict):
        return CheckResult(
            id="identity.data_scope",
            status=STATUS_FAIL,
            detail="full backup does not declare physical-schema capture scope",
        )

    expected = {
        "mode": "physical_schema",
        "tenant_id": None,
        "row_security": "bypassed",
        "physical_schema_complete": True,
        "restore_mode": "isolated_or_in_place",
    }
    actual = {
        "mode": str(recorded.get("mode") or "").strip(),
        "tenant_id": str(recorded.get("tenant_id") or "").strip() or None,
        "row_security": str(recorded.get("row_security") or "").strip(),
        "physical_schema_complete": recorded.get("physical_schema_complete") is True,
        "restore_mode": str(recorded.get("restore_mode") or "").strip(),
    }
    if actual != expected:
        return CheckResult(
            id="identity.data_scope",
            status=STATUS_FAIL,
            detail="full backup is not authenticated as physically schema-complete",
            data={"recorded": actual, "required": expected},
        )

    return CheckResult(
        id="identity.data_scope",
        status=STATUS_PASS,
        detail="operator-authenticated archive is physically schema-complete",
        data=actual,
    )


def _check_restorable_class(manifest: BackupManifest) -> CheckResult:
    """Only a full logical backup can be fed to pg_restore.

    A template-pack is a JSON export and a provider-snapshot is a receipt for
    something the cloud provider holds; neither is an archive.
    """
    restorable = manifest.backup_class == BACKUP_CLASS_FULL
    return CheckResult(
        id="backup.restorable_class",
        status=STATUS_PASS if restorable else STATUS_FAIL,
        detail=(
            f"class {manifest.backup_class!r} can be restored"
            if restorable
            else (
                f"class {manifest.backup_class!r} cannot be restored with "
                "pg_restore; only full logical backups can"
            )
        ),
        data={"backup_class": manifest.backup_class},
    )


def _describe_steps(
    *,
    mode: str,
    target_database: str,
    target_schema: str,
    keep_superseded: bool,
    source_schema: str = "",
) -> list[str]:
    """Human-readable account of exactly what apply will do."""
    if mode == MODE_ISOLATED:
        steps = [
            f"CREATE DATABASE {target_database} TEMPLATE template0",
            f"pg_restore --single-transaction into {target_database}",
        ]
        # The rename is a real mutation and belongs in the account the operator
        # confirms. Omitting it is how `--target-schema` looked like a no-op
        # in the staged plan while the restore landed under the source name.
        if source_schema and source_schema != target_schema:
            steps.append(f"ALTER SCHEMA {source_schema} RENAME TO {target_schema}")
        steps += [
            "run post-restore verification",
            "on failure: drop the created database",
        ]
        return steps
    return [
        "create a safety backup of the current target (precondition)",
        f"ALTER SCHEMA {target_schema} RENAME TO {target_schema}_superseded_<ts>",
        f"pg_restore --single-transaction recreates {target_schema}",
        "run post-restore verification against the restored schema",
        (
            "on success: keep the superseded schema"
            if keep_superseded
            else "on success: DROP SCHEMA <superseded> CASCADE"
        ),
        "on failure: drop the partial schema and rename the original back",
    ]


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def _require_policy_allows(cfg: dict[str, Any], *, operation: str) -> None:
    policy = str(cfg.get("destructive_operations") or POLICY_CONFIRM_REQUIRED).strip()
    if policy == POLICY_BLOCKED:
        raise BackupPolicyBlockedError(
            f"Destructive operation '{operation}' is blocked for target "
            f"{service.target_label(cfg)}.",
            detail={"policy": policy, "operation": operation},
        )


def confirmation_required(cfg: dict[str, Any], mode: str) -> bool:
    """Whether this restore demands the typed target label.

    Per the plan, only an in-place restore does -- an isolated restore builds a
    separate database and never touches live data, so exacting a typed label
    for it is friction without safety.

    This is the *single* definition. ``restore_backup`` enforces it, the staged
    plan advertises it, and the surfaces render from it. When a surface decided
    this for itself, the GUI ended up demanding a label for isolated restores
    that the service then ignored -- a form asserting a control that did not
    exist.
    """
    if mode != MODE_IN_PLACE:
        return False
    policy = str(cfg.get("destructive_operations") or POLICY_CONFIRM_REQUIRED).strip()
    return policy != POLICY_ALLOWED


def _require_confirmation(cfg: dict[str, Any], confirm_target: Optional[str]) -> None:
    if not confirmation_required(cfg, MODE_IN_PLACE):
        return
    expected = service.target_label(cfg)
    if confirm_target != expected:
        raise RestoreConfirmationError(
            "Typed confirmation does not match the target label.",
            detail={"required_confirm_target": expected},
        )


# ---------------------------------------------------------------------------
# restore_backup
# ---------------------------------------------------------------------------


def restore_backup(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_id: str,
    options: Optional[RestoreOptions] = None,
    confirm_target: Optional[str] = None,
    plan_fingerprint: Optional[str] = None,
    dry_run: bool = False,
    actor: Optional[Actor] = None,
    now: Optional[datetime] = None,
    record_receipt: bool = True,
) -> RestoreResult:
    """Apply a restore, after re-staging and re-checking everything.

    ``record_receipt=False`` is for callers that own the audit record
    themselves -- a rehearsal is not a restore, and logging it as one would
    put drills that never touched live data into the restore trail.
    """
    resolved = options or RestoreOptions()
    mode = resolved.normalized_mode()
    resolved_actor = actor or Actor(surface=SURFACE_CLI)

    # Re-stage rather than trust what the caller was shown.
    plan = plan_restore(cfg, settings, backup_id=backup_id, options=resolved, now=now)

    if plan_fingerprint is not None and plan_fingerprint != plan.plan_fingerprint:
        raise RestoreStageStaleError(
            "The staged plan no longer matches current state; re-stage and "
            "confirm again.",
            detail={
                "staged": plan_fingerprint,
                "current": plan.plan_fingerprint,
            },
        )

    if dry_run:
        return RestoreResult(
            backup_id=backup_id,
            mode=mode,
            target_database=plan.target_database,
            target_schema=plan.target_schema,
            checks=plan.checks,
            dry_run=True,
            plan=plan,
        )

    if mode == MODE_IN_PLACE:
        _require_policy_allows(cfg, operation="restore in place")
        _require_confirmation(cfg, confirm_target)

    if not plan.ok:
        raise BackupVerificationError(
            "Restore preflight failed; the target was not modified.",
            detail={"checks": [c.to_payload() for c in plan.blocking]},
        )

    # Populated by ``_restore_in_place`` as soon as the safety backup is
    # published, so the failure receipt below can name it even though no
    # ``RestoreResult`` came back.
    progress: dict[str, Any] = {}
    try:
        if mode == MODE_ISOLATED:
            result = _restore_isolated(cfg, settings, plan=plan, backup_id=backup_id)
        else:
            result = _restore_in_place(
                cfg,
                settings,
                plan=plan,
                backup_id=backup_id,
                keep_superseded=resolved.keep_superseded,
                actor=resolved_actor,
                now=now,
                progress=progress,
            )
    except Exception as exc:
        if record_receipt:
            write_receipt(
                service.receipts_directory(settings),
                operation=OPERATION_RESTORE,
                status=STATUS_FAILED,
                actor=resolved_actor,
                backup_id=backup_id,
                target_label=service.target_label(cfg),
                # A failed in-place restore is exactly when the safety backup
                # matters most -- the target was touched and rolled back, and if
                # that rollback degraded, this backup is the only copy of
                # production. Omitting the id here left an English sentence in
                # the manifest note as the sole link.
                detail={
                    "mode": mode,
                    "error": str(exc),
                    "safety_backup_id": progress.get("safety_backup_id"),
                },
                receipt_mirror=settings.get("receipt_mirror") or {},
            )
        raise

    receipt = (
        write_receipt(
            service.receipts_directory(settings),
            operation=OPERATION_RESTORE,
            status=STATUS_SUCCEEDED if result.ok else STATUS_FAILED,
            actor=resolved_actor,
            backup_id=backup_id,
            target_label=service.target_label(cfg),
            detail={
                "mode": mode,
                "target_database": result.target_database,
                "target_schema": result.target_schema,
                "quarantined": result.quarantined,
                "safety_backup_id": result.safety_backup_id,
                # Advancing a sequence past what the archive recorded is a
                # deliberate divergence from the backup. It belongs in the
                # audit trail, not just in the returned object.
                "sequences_advanced": result.sequences_advanced,
                # The post-restore verdicts, so "succeeded" is backed by the
                # evidence rather than asserted on its own.
                "checks": service.compact_checks(result.checks),
            },
            receipt_mirror=settings.get("receipt_mirror") or {},
        )
        if record_receipt
        else None
    )

    # ``replace`` rather than rebuilding field by field. The rebuilt version
    # silently dropped ``sequences_advanced``, so an in-place restore that had
    # correctly advanced a sequence reported that it had not -- the fix worked
    # and the audit trail said otherwise. Any field added later would have been
    # lost the same way.
    return replace(
        result,
        receipt_id=receipt.receipt_id if receipt else None,
        plan=plan,
    )


def _admin_sql(
    cfg: dict[str, Any], sql: str, *, database: Optional[str] = None
) -> None:
    """Run one maintenance statement, raising on failure."""
    operator_cfg = service.connection_config_for_role(cfg, "operator")
    result = engine.run_command(
        engine.build_psql_command(
            operator_cfg, sql=sql, database=database or "postgres"
        ),
        env=engine.client_env(operator_cfg),
    )
    if not result.ok:
        raise BackupVerificationError(
            f"Maintenance statement failed: {result.output[:300]}",
            detail={"sql": sql},
        )


def _restore_archive(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_id: str,
    database: str,
) -> None:
    """Fetch the artifact and run pg_restore into ``database``."""
    import shutil as _shutil
    import tempfile as _tempfile

    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup_id)
    manifest = service._load_manifest(storage, prefix)
    if manifest.backup_class != BACKUP_CLASS_FULL:
        raise BackupVerificationError(
            f"Backup class {manifest.backup_class!r} cannot be restored with "
            "pg_restore; only full logical backups can.",
        )

    staged = Path(_tempfile.mkdtemp(prefix="tapdb-restore-"))
    try:
        asset = manifest.included_assets[0]
        local = storage.get_file(f"{prefix}/{asset.name}", staged / asset.name)
        operator_cfg = service.connection_config_for_role(cfg, "operator")
        result = engine.run_command(
            engine.build_pg_restore_command(
                operator_cfg, archive_path=local, database=database
            ),
            env=engine.client_env(operator_cfg),
        )
        if not result.ok:
            raise BackupVerificationError(
                f"pg_restore failed: {result.output[:400]}",
                detail={"database": database},
            )
    finally:
        _shutil.rmtree(staged, ignore_errors=True)


def _database_exists(cfg: dict[str, Any], database: str) -> bool:
    """Return whether a database exists on the target cluster.

    The name is quoted as a *literal* here, not an identifier -- it lands
    inside ``'...'``. Interpolating it raw would let an operator-supplied
    ``--target-database`` inject SQL that runs with restore privileges.
    """
    operator_cfg = service.connection_config_for_role(cfg, "operator")
    result = engine.run_command(
        engine.build_psql_command(
            operator_cfg,
            sql=(
                "SELECT 1 FROM pg_database WHERE datname = " + quote_literal(database)
            ),
            database="postgres",
        ),
        env=engine.client_env(operator_cfg),
    )
    return bool(result.ok and result.stdout.strip())


def _restore_isolated(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    plan: RestorePlan,
    backup_id: str,
) -> RestoreResult:
    """Restore into a fresh database, leaving live data untouched."""
    database = plan.target_database
    created = False

    if not _database_exists(cfg, database):
        _admin_sql(
            cfg,
            f"CREATE DATABASE {quote_ident(database)} TEMPLATE template0",
        )
        created = True

    try:
        _restore_archive(cfg, settings, backup_id=backup_id, database=database)
        # The archive recreates the schema it was captured from. Restoring
        # under a different name is therefore a rename afterwards, not a
        # pg_restore option -- and it has to happen before verification, which
        # inspects `plan.target_schema`.
        _rename_restored_schema(
            cfg,
            database=database,
            source_schema=plan.source_schema,
            target_schema=plan.target_schema,
        )
        _restore_runtime_access(cfg, database=database, schema=plan.target_schema)
        checks = _post_restore_checks(
            cfg,
            settings,
            backup_id=backup_id,
            database=database,
            schema=plan.target_schema,
        )
    except Exception:
        # A failed restore must not leave a half-populated database behind to
        # be mistaken for a recovery target.
        if created:
            _drop_database(cfg, database)
        raise

    quarantined = any(check.failed for check in checks)
    return RestoreResult(
        backup_id=backup_id,
        mode=MODE_ISOLATED,
        target_database=database,
        target_schema=plan.target_schema,
        checks=checks,
        quarantined=quarantined,
    )


def _rollback_in_place(
    cfg: dict[str, Any],
    *,
    database: str,
    schema: str,
    superseded: str,
) -> None:
    """Put the original schema back, whatever else fails.

    Ordering and error handling both matter here, because this runs while the
    live schema name is occupied by data that failed verification:

    * the drop is attempted first, but a failure must not abort the rollback --
      the rename-back is what restores service, and leaving it unattempted is
      strictly worse than leaving a stray schema behind;
    * if the rename-back cannot be completed, that is escalated, because the
      target is now in a state no operator would infer from the original
      error: the live name is missing or wrong and the real data is parked
      under ``superseded``. Saying so explicitly is the only safe outcome.

    The caller re-raises the original failure, which is the one that explains
    *why* the restore was rolled back.
    """
    drop_error: Optional[Exception] = None
    try:
        _admin_sql(
            cfg,
            f"DROP SCHEMA IF EXISTS {quote_ident(schema)} CASCADE",
            database=database,
        )
    except Exception as exc:  # noqa: BLE001 - recovery must continue regardless
        drop_error = exc

    try:
        _admin_sql(
            cfg,
            f"ALTER SCHEMA {quote_ident(superseded)} RENAME TO {quote_ident(schema)}",
            database=database,
        )
    except Exception as exc:
        raise BackupVerificationError(
            "Restore failed AND the rollback could not restore the original "
            f"schema. The previous data is intact under {superseded!r} in "
            f"{database!r} and must be renamed back to {schema!r} by hand "
            "before the target is used.",
            detail={
                "database": database,
                "schema": schema,
                "superseded_schema": superseded,
                "rename_error": str(exc),
                "drop_error": str(drop_error) if drop_error else None,
            },
        ) from exc


def _rename_restored_schema(
    cfg: dict[str, Any],
    *,
    database: str,
    source_schema: str,
    target_schema: str,
) -> None:
    """Rename a freshly restored schema to the requested name.

    A no-op when the names match, which is the common case.

    Safe by construction: the schema contains no hard-coded schema names,
    ``nextval`` calls are unqualified, and IDENTITY columns link by OID -- so
    renaming after the restore leaves a fully functional schema. This is only
    ever applied to an isolated restore, where the database was just created
    or verified empty of both names.
    """
    if not source_schema or not target_schema or source_schema == target_schema:
        return
    _admin_sql(
        cfg,
        f"ALTER SCHEMA {quote_ident(source_schema)} "
        f"RENAME TO {quote_ident(target_schema)}",
        database=database,
    )


def _drop_database(cfg: dict[str, Any], database: str) -> None:
    operator_cfg = service.connection_config_for_role(cfg, "operator")
    engine.run_command(
        engine.build_psql_command(
            operator_cfg,
            sql=f"DROP DATABASE IF EXISTS {quote_ident(database)}",
            database="postgres",
        ),
        env=engine.client_env(operator_cfg),
    )


def _restore_runtime_access(cfg: dict[str, Any], *, database: str, schema: str) -> None:
    """Restore the explicit runtime role grants omitted by ``--no-acl``."""

    runtime_user = str(cfg.get("user") or "").strip()
    if not runtime_user:
        raise BackupVerificationError("restore target has no configured runtime user")
    operator_cfg = service.connection_config_for_role(cfg, "operator")
    operator_user = str(operator_cfg["user"])
    quoted_schema = quote_ident(schema)
    quoted_runtime = quote_ident(runtime_user)
    quoted_operator = quote_ident(operator_user)
    sql = "\n".join(
        (
            "BEGIN;",
            "DO $tapdb_rebind$ DECLARE fn record; BEGIN "
            "FOR fn IN SELECT n.nspname, p.proname, "
            "pg_get_function_identity_arguments(p.oid) AS args "
            "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
            f"WHERE n.nspname = {quote_literal(schema)} "
            "AND EXISTS (SELECT 1 FROM unnest(COALESCE(p.proconfig, ARRAY[]::text[])) "
            "setting WHERE split_part(setting, '=', 1) = 'search_path') LOOP "
            "EXECUTE format('ALTER FUNCTION %I.%I(%s) SET search_path TO "
            "%I, pg_catalog, pg_temp', "
            "fn.nspname, fn.proname, fn.args, fn.nspname); END LOOP; "
            "END $tapdb_rebind$;",
            f"GRANT USAGE ON SCHEMA {quoted_schema} TO {quoted_runtime};",
            "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "
            f"{quoted_schema} TO {quoted_runtime};",
            "GRANT USAGE, SELECT, UPDATE ON ALL SEQUENCES IN SCHEMA "
            f"{quoted_schema} TO {quoted_runtime};",
            f"ALTER DEFAULT PRIVILEGES FOR ROLE {quoted_operator} IN SCHEMA "
            f"{quoted_schema} GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES "
            f"TO {quoted_runtime};",
            f"ALTER DEFAULT PRIVILEGES FOR ROLE {quoted_operator} IN SCHEMA "
            f"{quoted_schema} GRANT USAGE, SELECT, UPDATE ON SEQUENCES "
            f"TO {quoted_runtime};",
            f"UPDATE {quoted_schema}.tapdb_runtime_principal_scope SET "
            f"schema_name = {quote_literal(schema)} WHERE "
            f"role_name = {quote_literal(runtime_user)};",
            f"REVOKE ALL ON TABLE {quoted_schema}.tapdb_runtime_principal_scope "
            f"FROM {quoted_runtime};",
            "COMMIT;",
        )
    )
    _admin_sql(cfg, sql, database=database)


def _restore_in_place(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    plan: RestorePlan,
    backup_id: str,
    keep_superseded: bool,
    actor: Actor,
    now: Optional[datetime],
    progress: Optional[dict[str, Any]] = None,
) -> RestoreResult:
    """Replace the configured schema without ever destroying it unverified.

    The live schema is renamed aside, the replacement restored beside it, and
    the previous schema dropped only once verification passes. Every failure
    path renames the original back.

    ``progress`` is an out-parameter for the caller's failure receipt. The
    safety backup is published before anything is renamed, so on the exception
    path it exists but the ``RestoreResult`` carrying its id never returns --
    leaving the most load-bearing backup in the system linked only by an
    English note. Recording it here means the failure receipt can name it.
    """
    record = progress if progress is not None else {}
    database = str(cfg["database"])
    schema = str(cfg["schema_name"])
    superseded = superseded_schema_name(schema, now=now)

    # Precondition: a safety backup must exist before anything is touched.
    # allow_drift is deliberate. The safety backup's job is to capture
    # whatever is on the target *right now*, not to certify that it matches
    # the schema assets. Letting the drift gate veto it would refuse in-place
    # restores on exactly the databases most likely to need one.
    safety = service.create_backup(
        cfg,
        settings,
        allow_drift=True,
        note=f"pre-restore safety backup for {backup_id}",
        actor=actor,
        # The note above is prose; this is the machine-readable form. Without
        # it, the only way to recognise the last copy of production is to regex
        # an English sentence.
        provenance={
            "created_by": PROVENANCE_RESTORE,
            "restored_backup_id": backup_id,
        },
    )
    # Recorded immediately after publication and before the first rename, so
    # every subsequent failure path can still name it. Reads ``backup_id``
    # rather than ``manifest.backup_id`` -- identical in value, but ``manifest``
    # is Optional on BackupResult, and the success path below uses the same
    # accessor. The two receipts must never be able to disagree.
    record["safety_backup_id"] = safety.backup_id

    _admin_sql(
        cfg,
        f"ALTER SCHEMA {quote_ident(schema)} RENAME TO {quote_ident(superseded)}",
        database=database,
    )

    try:
        _restore_archive(cfg, settings, backup_id=backup_id, database=database)
        _restore_runtime_access(cfg, database=database, schema=schema)
        # Before anything verifies or uses the restored schema, push every
        # sequence past what the target had *actually* issued. The archive only
        # knows the positions as of the backup, so without this an identifier
        # minted after the backup -- and rolled back by this restore -- would
        # be handed out again to a different object.
        advanced = _reconcile_sequences_after_restore(
            cfg, database=database, schema=schema, floor=safety.manifest.sequences
        )
        checks = _post_restore_checks(
            cfg, settings, backup_id=backup_id, database=database, schema=schema
        )
        if any(check.failed for check in checks):
            raise BackupVerificationError(
                "Post-restore verification failed.",
                detail={"checks": [c.to_payload() for c in checks]},
            )
    except Exception:
        # Roll back to exactly the previous state: discard whatever was
        # restored and put the original schema back under its own name.
        #
        # **The rename-back must be attempted even if the drop fails.** These
        # were two bare statements, so a `DROP SCHEMA` that hit a lock timeout
        # -- entirely possible, since preflight only *warns* about active
        # connections -- skipped the rename entirely and replaced the original
        # error with the drop's. The live schema name was then left holding the
        # rewound, verification-failed data while the real data sat under
        # `_superseded_`, and applications reconnected to it silently. The
        # runbook promises the opposite.
        _rollback_in_place(cfg, database=database, schema=schema, superseded=superseded)
        raise

    if not keep_superseded:
        _admin_sql(
            cfg,
            f"DROP SCHEMA IF EXISTS {quote_ident(superseded)} CASCADE",
            database=database,
        )

    return RestoreResult(
        backup_id=backup_id,
        mode=MODE_IN_PLACE,
        target_database=database,
        target_schema=schema,
        checks=checks,
        safety_backup_id=safety.backup_id,
        superseded_schema=superseded if keep_superseded else None,
        sequences_advanced=advanced,
    )


def _reconcile_sequences_after_restore(
    cfg: dict[str, Any],
    *,
    database: str,
    schema: str,
    floor: list[Any],
) -> dict[str, Any]:
    """Push restored sequences past everything the target had already issued."""
    from daylily_tapdb.backup import postrestore

    probe_cfg = dict(cfg)
    probe_cfg["database"] = database
    probe_cfg["schema_name"] = schema

    with service.open_session(
        probe_cfg,
        app_username="tapdb_restore_seq",
        connection_role="operator",
    ) as conn:
        with conn.session_scope(commit=True) as session:
            return postrestore.reconcile_sequences_to_floor(
                session, schema, floor=floor
            )


# ---------------------------------------------------------------------------
# Post-restore verification (extended by step 5)
# ---------------------------------------------------------------------------


def _post_restore_checks(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_id: str,
    database: str,
    schema: str,
) -> list[CheckResult]:
    """Run the full post-restore suite against a restored schema.

    This is what the in-place rollback decision reads: any failure here means
    the previous schema is renamed back and the restored one discarded.
    """
    from daylily_tapdb.backup import postrestore

    storage = service.storage_for(settings)
    prefix = service.find_backup_prefix(cfg, storage, backup_id)
    manifest = service._load_manifest(storage, prefix)

    probe_cfg = dict(cfg)
    probe_cfg["database"] = database
    probe_cfg["schema_name"] = schema

    with service.open_session(
        probe_cfg,
        app_username="tapdb_restore_verify",
        connection_role="operator",
    ) as conn:
        with conn.session_scope(commit=False) as session:
            return postrestore.run_all(session, probe_cfg, manifest, schema=schema)


# ---------------------------------------------------------------------------
# rehearse_restore
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RehearsalEvidence:
    """Durable proof that a backup was restored and verified end to end."""

    backup_id: str
    rehearsal_id: str
    started_at: str
    finished_at: str
    database: str
    schema: str
    checks: list[CheckResult] = field(default_factory=list)
    evidence_key: Optional[str] = None
    kept: bool = False
    dry_run: bool = False
    error: Optional[str] = None
    #: Set once the receipt is written; lets a surface link to the durable
    #: record rather than re-reporting a transient result.
    receipt_id: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and not any(c.failed for c in self.checks)

    def to_payload(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "rehearsal_id": self.rehearsal_id,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "database": self.database,
            "schema": self.schema,
            "ok": self.ok,
            "kept": self.kept,
            "dry_run": self.dry_run,
            "error": self.error,
            "checks": [check.to_payload() for check in self.checks],
            "evidence_key": self.evidence_key,
            "receipt_id": self.receipt_id,
        }


def rehearse_restore(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_id: str,
    keep: bool = False,
    dry_run: bool = False,
    actor: Optional[Actor] = None,
    now: Optional[datetime] = None,
) -> RehearsalEvidence:
    """Restore a backup into a throwaway database and prove it verifies.

    A backup nobody has ever restored is a hypothesis. This turns it into a
    tested claim, without touching live data: the restore lands in its own
    database, the full post-restore suite runs against it, and the result is
    written to storage as durable evidence.

    Evidence is written **even when verification fails** -- a failed rehearsal
    is the most valuable kind, and losing that record would defeat the purpose
    of running one.
    """
    resolved_actor = actor or Actor(surface=SURFACE_CLI)
    moment = now or datetime.now(UTC)
    stamp = _stamp(moment)
    prefix_name = str(settings.get("rehearsal_database_prefix") or "tapdb_rehearsal")
    database = f"{prefix_name}_{stamp}"
    schema = str(cfg["schema_name"])

    if dry_run:
        plan = plan_restore(
            cfg,
            settings,
            backup_id=backup_id,
            options=RestoreOptions(target_database=database),
            now=moment,
        )
        return RehearsalEvidence(
            backup_id=backup_id,
            rehearsal_id=stamp,
            started_at=moment.isoformat(),
            finished_at=moment.isoformat(),
            database=database,
            schema=schema,
            checks=plan.checks,
            dry_run=True,
        )

    checks: list[CheckResult] = []
    error: Optional[str] = None
    try:
        result = restore_backup(
            cfg,
            settings,
            backup_id=backup_id,
            options=RestoreOptions(mode=MODE_ISOLATED, target_database=database),
            actor=resolved_actor,
            now=moment,
            # A rehearsal is not a restore. Letting it write a restore receipt
            # would put drills that never touched live data into the trail an
            # operator reads to answer "what has been restored here?".
            record_receipt=False,
        )
        checks = list(result.checks)
    except Exception as exc:
        error = str(exc)

    finished = datetime.now(UTC)
    evidence = RehearsalEvidence(
        backup_id=backup_id,
        rehearsal_id=stamp,
        started_at=moment.isoformat(),
        finished_at=finished.isoformat(),
        database=database,
        schema=schema,
        checks=checks,
        kept=keep,
        error=error,
    )

    # Write evidence before teardown, so a failure during cleanup cannot cost
    # us the record of what the rehearsal found.
    evidence_key = _write_rehearsal_evidence(cfg, settings, evidence)

    receipt = write_receipt(
        service.receipts_directory(settings),
        operation=OPERATION_REHEARSE,
        status=STATUS_SUCCEEDED if evidence.ok else STATUS_FAILED,
        actor=resolved_actor,
        backup_id=backup_id,
        target_label=service.target_label(cfg),
        detail={
            "rehearsal_id": stamp,
            "database": database,
            "evidence_key": evidence_key,
            "kept": keep,
            "error": error,
            "checks": service.compact_checks(list(checks)),
        },
        receipt_mirror=settings.get("receipt_mirror") or {},
    )

    if not keep:
        _drop_database(cfg, database)

    # ``replace`` rather than listing fields: the equivalent rebuild in
    # ``restore_backup`` silently dropped a newly added field, so the fix
    # worked while the result said otherwise.
    return replace(
        evidence,
        evidence_key=evidence_key,
        kept=keep,
        receipt_id=receipt.receipt_id if receipt else None,
    )


def _write_rehearsal_evidence(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    evidence: RehearsalEvidence,
) -> Optional[str]:
    """Persist rehearsal evidence to storage; never raise."""
    from daylily_tapdb.backup.storage import rehearsal_key

    try:
        storage = service.storage_for(settings)
        key = rehearsal_key(
            str(cfg["client_id"]),
            str(cfg["database_name"]),
            evidence.backup_id,
            evidence.rehearsal_id,
        )
        storage.put_bytes(key, canonical_bytes(evidence.to_payload()))
        return key
    except Exception:  # pragma: no cover - storage failure must not mask result
        return None


__all__ = [
    "DISK_HEADROOM_FACTOR",
    "MODE_IN_PLACE",
    "MODE_ISOLATED",
    "RESTORE_MODES",
    "RehearsalEvidence",
    "RestoreOptions",
    "RestorePlan",
    "RestoreResult",
    "compute_plan_fingerprint",
    "isolated_database_name",
    "plan_restore",
    "rehearse_restore",
    "restore_backup",
    "superseded_schema_name",
]
