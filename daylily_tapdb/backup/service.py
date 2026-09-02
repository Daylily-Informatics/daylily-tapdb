"""The backup lifecycle service -- the one implementation all surfaces share.

The CLI, the admin API, and the embedded GUI are thin adapters over these
functions. Nothing here imports typer or FastAPI, and nothing here builds its
own ``pg_dump`` command: that lives in ``engine.py``, so a fix to how backups
are taken is a fix everywhere at once.

Every function takes a resolved ``cfg`` (``get_db_config()``) plus ``settings``
(``get_backup_settings()``) and returns a dataclass with ``to_payload()``.
Mutating operations emit a receipt centrally, so all three surfaces get an
audit trail without any of them remembering to write one.
"""

from __future__ import annotations

import json
import secrets
import shutil
import tempfile
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional

from daylily_tapdb.backup import engine, introspect, template_pack
from daylily_tapdb.backup.errors import (
    BackupNotFoundError,
    BackupVerificationError,
)
from daylily_tapdb.backup.inventory import (
    excluded_state_payload,
    schema_asset_checksums,
    state_inventory_payload,
    summarize_inventory,
)
from daylily_tapdb.backup.manifest import (
    BACKUP_CLASS_FULL,
    BACKUP_CLASS_PROVIDER_SNAPSHOT,
    BACKUP_CLASS_TEMPLATE_PACK,
    BACKUP_CLASSES,
    CONSISTENCY_BEST_EFFORT,
    CONSISTENCY_SNAPSHOT,
    PROVENANCE_OPERATOR,
    AssetRef,
    BackupManifest,
    canonical_bytes,
    sha256_file,
    sha256_hex,
    sign_manifest,
    signature_scheme,
)
from daylily_tapdb.backup.receipts import (
    OPERATION_CREATE,
    OPERATION_VERIFY,
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    SURFACE_CLI,
    Actor,
    write_receipt,
)
from daylily_tapdb.backup.storage import (
    MANIFEST_CHECKSUM_KEY,
    MANIFEST_KEY,
    backup_prefix,
    build_storage_backend,
    database_prefix,
    discover_backup_prefixes,
)

VERIFY_QUICK = "quick"
VERIFY_DEEP = "deep"

STATUS_PASS = "pass"
STATUS_FAIL = "fail"
STATUS_WARN = "warn"
STATUS_SKIP = "skip"


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckResult:
    """One named check with a verdict."""

    id: str
    status: str
    detail: str = ""
    data: dict[str, Any] = field(default_factory=dict)

    @property
    def failed(self) -> bool:
        return self.status == STATUS_FAIL

    def to_payload(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "status": self.status,
            "detail": self.detail,
            "data": self.data,
        }


@dataclass(frozen=True)
class BackupPlan:
    """What a backup would capture, without capturing it."""

    backup_class: str
    target_label: str
    schema_name: str
    storage: dict[str, Any]
    checks: list[CheckResult] = field(default_factory=list)
    would_capture: dict[str, Any] = field(default_factory=dict)

    @property
    def blocking(self) -> list[CheckResult]:
        return [check for check in self.checks if check.failed]

    @property
    def ok(self) -> bool:
        return not self.blocking

    def to_payload(self) -> dict[str, Any]:
        return {
            "backup_class": self.backup_class,
            "target_label": self.target_label,
            "schema_name": self.schema_name,
            "storage": self.storage,
            "ok": self.ok,
            "checks": [check.to_payload() for check in self.checks],
            "would_capture": self.would_capture,
        }


@dataclass(frozen=True)
class VerifyReport:
    """Integrity verdict for one stored backup."""

    backup_id: str
    level: str
    checks: list[CheckResult] = field(default_factory=list)
    #: Set once the receipt is written, so a surface can link to the durable
    #: record of this run instead of re-reporting a transient result.
    receipt_id: Optional[str] = None

    @property
    def ok(self) -> bool:
        return not any(check.failed for check in self.checks)

    def to_payload(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "level": self.level,
            "ok": self.ok,
            "receipt_id": self.receipt_id,
            "checks": [check.to_payload() for check in self.checks],
        }


@dataclass(frozen=True)
class BackupResult:
    """Outcome of a create run."""

    backup_id: str
    backup_class: str
    storage_prefix: str
    manifest: Optional[BackupManifest] = None
    verify: Optional[VerifyReport] = None
    receipt_id: Optional[str] = None
    dry_run: bool = False

    def to_payload(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "backup_class": self.backup_class,
            "storage_prefix": self.storage_prefix,
            "dry_run": self.dry_run,
            "receipt_id": self.receipt_id,
            "manifest": self.manifest.to_payload() if self.manifest else None,
            "verify": self.verify.to_payload() if self.verify else None,
        }


@dataclass(frozen=True)
class BackupSummary:
    """One row in a listing."""

    backup_id: str
    backup_class: str
    created_at: Optional[str]
    status: str
    storage_prefix: str
    target_label: Optional[str]
    row_totals: Optional[int]
    bytes: int

    def to_payload(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "backup_class": self.backup_class,
            "created_at": self.created_at,
            "status": self.status,
            "storage_prefix": self.storage_prefix,
            "target_label": self.target_label,
            "row_totals": self.row_totals,
            "bytes": self.bytes,
        }


@dataclass(frozen=True)
class BackupListing:
    """Every backup discoverable for a target."""

    entries: list[BackupSummary] = field(default_factory=list)
    storage: dict[str, Any] = field(default_factory=dict)
    #: Prefixes whose manifest could not be read. Reported rather than
    #: dropped: silently shortening the list would read as "this is
    #: everything you have", which is the opposite of the truth.
    damaged: list[str] = field(default_factory=list)

    def to_payload(self) -> dict[str, Any]:
        return {
            "count": len(self.entries),
            "storage": self.storage,
            "backups": [entry.to_payload() for entry in self.entries],
            "damaged": self.damaged,
        }


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def target_label(cfg: dict[str, Any]) -> str:
    """Return the typed-confirmation label for a target.

    Identical to the CLI's destructive-confirmation label so an operator types
    the same string regardless of which surface they are on.
    """
    return (
        f"{cfg.get('client_id')}/{cfg.get('database_name')}/"
        f"{cfg.get('schema_name')}@{cfg.get('database')}"
    )


def compact_checks(checks: list[CheckResult]) -> list[dict[str, Any]]:
    """Reduce checks to what belongs in a receipt.

    Verification results were previously transient: the CLI printed them and
    the GUI discarded them, so after a restore the only durable evidence was
    "succeeded". An operator asking *what was actually verified* had nothing to
    read, and neither did an auditor.

    ``data`` is dropped deliberately. It carries per-check diagnostic payloads
    that can be large on failure (every mismatched row count, every drifted
    object), and receipts are immutable, hash-chained, and read in full on
    every status page. The verdict and its one-line reason are what make the
    record meaningful; the diagnostics belong in the operation's own output.
    """
    return [
        {"id": check.id, "status": check.status, "detail": check.detail}
        for check in checks
    ]


def storage_for(settings: dict[str, Any]) -> Any:
    """Build the configured storage backend."""
    return build_storage_backend(
        settings.get("storage_uri") or "",
        config_dir=Path(settings["config_dir"]),
    )


def receipts_directory(settings: dict[str, Any]) -> Path:
    """Return where receipts live for this target."""
    return Path(settings["config_dir"]) / "backups" / "receipts"


def new_backup_id(backup_class: str, *, now: Optional[datetime] = None) -> str:
    """Mint a sortable, collision-resistant backup id."""
    moment = (now or datetime.now(UTC)).astimezone(UTC)
    return f"{backup_class}-{moment.strftime('%Y%m%dT%H%M%SZ')}-{secrets.token_hex(3)}"


def _validate_backup_class(backup_class: str) -> str:
    normalized = str(backup_class or "").strip().lower()
    if normalized not in BACKUP_CLASSES:
        raise ValueError(
            f"Unknown backup class {backup_class!r}; expected one of: "
            + ", ".join(BACKUP_CLASSES)
        )
    return normalized


def _artifact_name(backup_class: str) -> str:
    if backup_class == BACKUP_CLASS_TEMPLATE_PACK:
        return template_pack.TEMPLATE_PACK_ARTIFACT
    if backup_class == BACKUP_CLASS_PROVIDER_SNAPSHOT:
        return "snapshot-receipt.json"
    return engine.DEFAULT_ARTIFACT_NAME


def open_session(
    cfg: dict[str, Any], *, app_username: str, connection_role: str = "runtime"
):
    """Open a TAPDB session for the configured target.

    Deliberately mirrors the CLI's connection construction rather than reusing
    it, so the service stays free of typer imports.
    """
    from daylily_tapdb.connection import TAPDBConnection

    connection_cfg = connection_config_for_role(cfg, connection_role)
    iam_auth = str(connection_cfg.get("iam_auth") or "").strip().lower() in {
        "true",
        "1",
        "yes",
        "on",
    }
    # `if "password" in cfg` rather than `or None`: an empty password is a
    # legitimate value under trust auth, and TAPDBConnection distinguishes
    # "" (fine) from None (rejected). Collapsing them breaks every
    # trust-authenticated local target.
    db_pass = connection_cfg.get("password") if "password" in connection_cfg else None
    return TAPDBConnection(
        db_hostname=f"{connection_cfg['host']}:{connection_cfg['port']}",
        db_hostaddr=connection_cfg.get("hostaddr") or None,
        db_user=connection_cfg["user"],
        db_pass=db_pass,
        secret_arn=connection_cfg.get("secret_arn") or None,
        db_name=connection_cfg["database"],
        engine_type=str(connection_cfg.get("engine_type") or "local").strip().lower(),
        region=str(connection_cfg.get("region") or "us-west-2").strip(),
        iam_auth=iam_auth,
        app_username=app_username,
        domain_code=str(connection_cfg["domain_code"]),
        owner_repo_name=str(connection_cfg["owner_repo_name"]),
        schema_name=str(connection_cfg["schema_name"]),
        tenant_id=connection_cfg.get("tenant_id") or None,
        allow_global_rows=bool(connection_cfg.get("allow_global_claims")),
        config_identity=str(connection_cfg["config_path"]),
        connection_role=connection_role,
    )


def connection_config_for_role(
    cfg: dict[str, Any], connection_role: str
) -> dict[str, Any]:
    """Select explicit runtime or distinct operator authentication fields."""

    if connection_role == "runtime":
        return dict(cfg)
    if connection_role != "operator":
        raise ValueError("connection_role must be 'runtime' or 'operator'")
    if cfg.get("operator_configured") is not True:
        raise RuntimeError(
            "full backup and restore require target.operator credentials"
        )
    operator_user = str(cfg.get("operator_user") or "").strip()
    runtime_user = str(cfg.get("user") or "").strip()
    if not operator_user or operator_user == runtime_user:
        raise RuntimeError(
            "target.operator.user must be non-empty and distinct from target.user"
        )
    selected = dict(cfg)
    selected.update(
        {
            "user": operator_user,
            "password": (
                cfg.get("operator_password") if "operator_password" in cfg else None
            ),
            "secret_arn": cfg.get("operator_secret_arn") or None,
            "iam_auth": cfg.get("operator_iam_auth") or False,
            "tenant_id": None,
            "allow_global_claims": True,
        }
    )
    return selected


def _connection_role_for_backup_class(backup_class: str) -> str | None:
    """Return the database role whose visibility defines an artifact.

    Provider snapshots are created by the RDS API, not by a PostgreSQL
    session, so assigning them a database role would be a category error.
    """

    if backup_class == BACKUP_CLASS_FULL:
        return "operator"
    if backup_class == BACKUP_CLASS_TEMPLATE_PACK:
        return "runtime"
    if backup_class == BACKUP_CLASS_PROVIDER_SNAPSHOT:
        return None
    raise ValueError(f"Unknown backup class: {backup_class!r}")


def _target_identity(
    cfg: dict[str, Any], *, backup_class: str = BACKUP_CLASS_FULL
) -> dict[str, Any]:
    """Identity recorded in every manifest -- deliberately credential-free."""
    tenant_id = str(cfg.get("tenant_id") or "").strip() or None
    if backup_class == BACKUP_CLASS_FULL:
        data_scope = {
            "mode": "physical_schema",
            "tenant_id": None,
            "row_security": "bypassed",
            "physical_schema_complete": True,
            "restore_mode": "isolated_or_in_place",
        }
    elif backup_class == BACKUP_CLASS_TEMPLATE_PACK:
        data_scope = {
            "mode": "tenant_and_global" if tenant_id else "global_only",
            "tenant_id": tenant_id,
            "row_security": "enforced",
            "physical_schema_complete": False,
            "restore_mode": "not_applicable",
        }
    elif backup_class == BACKUP_CLASS_PROVIDER_SNAPSHOT:
        data_scope = {
            "mode": "provider_cluster_snapshot",
            "tenant_id": None,
            "row_security": "not_applicable",
            # This field is the logical schema-completeness claim consumed by
            # pg_restore preflight. A provider receipt cannot make that claim.
            "physical_schema_complete": False,
            "restore_mode": "provider_cutover",
        }
    else:
        raise ValueError(f"Unknown backup class: {backup_class!r}")
    return {
        "client_id": cfg.get("client_id"),
        "database_name": cfg.get("database_name"),
        "database": cfg.get("database"),
        "schema_name": cfg.get("schema_name"),
        "domain_code": cfg.get("domain_code"),
        "owner_repo_name": cfg.get("owner_repo_name"),
        "engine_type": cfg.get("engine_type"),
        "host": cfg.get("host"),
        "port": str(cfg.get("port") or ""),
        "cluster_identifier": cfg.get("cluster_identifier") or None,
        "target_label": target_label(cfg),
        "data_scope": data_scope,
    }


def _tool_block() -> dict[str, Any]:
    """Record which tooling produced the artifact."""
    from daylily_tapdb import __version__

    return {
        "package_version": __version__,
        "pg_dump_version": engine.client_version("pg_dump"),
        "pg_restore_version": engine.client_version("pg_restore"),
    }


def _schema_drift(session: Any, cfg: dict[str, Any]) -> dict[str, Any]:
    """Compare expected schema assets against the live schema.

    Expectations come from ``tapdb_schema.sql`` plus ``migrations/*.sql`` -- the
    same files a new object is added to -- so this needs no maintenance when
    the schema grows.
    """
    from daylily_tapdb.euid import GENERIC_TEMPLATE_PREFIX
    from daylily_tapdb.schema_inventory import (
        diff_schema_inventory,
        drift_entry_counts,
        find_schema_root,
        load_expected_schema_inventory,
        load_live_schema_inventory,
        schema_asset_files,
    )

    schema_root = find_schema_root(Path("tapdb_schema.sql"))
    asset_paths = schema_asset_files(schema_root)
    # The dynamic per-prefix sequence is named for the generic_template
    # identity prefix, not the domain code -- matching the drift check in
    # cli/db.py. Deriving it any other way reports phantom drift on every
    # target.
    dynamic_sequence = f"{GENERIC_TEMPLATE_PREFIX.lower()}_instance_seq"

    expected = load_expected_schema_inventory(
        asset_paths, dynamic_sequence_name=dynamic_sequence
    )
    live = load_live_schema_inventory(session, schema_name=str(cfg["schema_name"]))
    report = diff_schema_inventory(
        expected,
        live,
        env="target",
        database=str(cfg["database"]),
        # strict=True is what makes the drift gate a real tripwire. Without it
        # `unexpected` is never populated, so an object created by hand-DDL --
        # outside the migration path, and therefore absent from the schema
        # assets a restore is verified against -- would sail through and be
        # captured in a backup claiming to match those assets.
        strict=True,
        expected_asset_paths=[str(path.resolve()) for path in asset_paths],
    )
    payload = report.to_payload()
    payload["counts"] = {
        "expected": report.expected.counts(),
        "live": report.live.counts(),
        "missing": drift_entry_counts(report.missing),
        "unexpected": drift_entry_counts(report.unexpected),
    }
    payload["has_drift"] = report.has_drift
    payload["asset_checksums"] = schema_asset_checksums(asset_paths)
    payload["live_summary"] = summarize_inventory(live)
    return payload


def _governance_block(cfg: dict[str, Any]) -> dict[str, Any]:
    """Record the governance context a restore must still satisfy."""
    entries: dict[str, Any] = {}
    for key in ("prefix_ownership_registry_path", "domain_registry_path"):
        raw = cfg.get(key)
        if not raw:
            entries[key] = None
            continue
        path = Path(str(raw)).expanduser()
        entries[key] = {
            "path": str(path),
            "sha256": sha256_file(path) if path.is_file() else None,
        }
    entries["domain_code"] = cfg.get("domain_code")
    entries["owner_repo_name"] = cfg.get("owner_repo_name")
    return entries


def _not_applicable_drift() -> dict[str, Any]:
    return {
        "has_drift": None,
        "counts": {},
        "asset_checksums": [],
        "status": "not_applicable",
    }


def _excluded_state_for_backup_class(backup_class: str) -> list[dict[str, Any]]:
    """Describe exclusions without applying full-logical claims to other classes."""

    if backup_class == BACKUP_CLASS_FULL:
        return [
            item
            for item in excluded_state_payload()
            if item.get("key") != "rows_outside_rls_scope"
        ]
    if backup_class == BACKUP_CLASS_TEMPLATE_PACK:
        return [
            {
                "key": "non_template_database_state",
                "title": "Non-template database state",
                "disposition": "excluded",
                "detail": "Only RLS-visible active template definitions are exported.",
            },
            {
                "key": "destination_owned_template_identity",
                "title": "Destination-owned template identity",
                "disposition": "excluded",
                "detail": "Database EUIDs, row timestamps, and sequence state are omitted.",
            },
            {
                "key": "rows_outside_rls_scope",
                "title": "Rows outside the runtime RLS scope",
                "disposition": "excluded",
                "detail": "The template pack contains only rows visible to the runtime role.",
            },
        ]
    if backup_class == BACKUP_CLASS_PROVIDER_SNAPSHOT:
        return [
            {
                "key": "tapdb_content_inventory",
                "title": "TapDB content inventory",
                "disposition": "excluded",
                "detail": (
                    "The receipt references an opaque provider-held cluster snapshot; "
                    "its contents are not asserted until a provider restore is inspected."
                ),
            },
            {
                "key": "config_and_external_identity",
                "title": "TapDB config and external identity-provider state",
                "disposition": "excluded",
                "detail": "Neither local config nor Cognito state is part of an RDS snapshot.",
            },
        ]
    raise ValueError(f"Unknown backup class: {backup_class!r}")


def _state_inventory_for_backup_class(backup_class: str) -> list[dict[str, Any]]:
    """Return only inventory claims that apply to the selected backup class."""

    if backup_class == BACKUP_CLASS_FULL:
        return [
            item
            for item in state_inventory_payload()
            if item.get("key") != "rows_outside_rls_scope"
        ]
    if backup_class in {BACKUP_CLASS_TEMPLATE_PACK, BACKUP_CLASS_PROVIDER_SNAPSHOT}:
        return []
    raise ValueError(f"Unknown backup class: {backup_class!r}")


# ---------------------------------------------------------------------------
# plan
# ---------------------------------------------------------------------------


def plan_backup(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_class: str = BACKUP_CLASS_FULL,
    strict_drift: bool = False,
) -> BackupPlan:
    """Report what a backup would do. Never mutates anything, anywhere."""
    resolved_class = _validate_backup_class(backup_class)
    storage = storage_for(settings)
    checks: list[CheckResult] = []
    would_capture: dict[str, Any] = {}

    if resolved_class == BACKUP_CLASS_PROVIDER_SNAPSHOT:
        from daylily_tapdb.backup import snapshots

        checks.append(
            CheckResult(
                id="client.pg_dump",
                status=STATUS_SKIP,
                detail="not used by provider-snapshot",
            )
        )
        cluster_identifier = str(
            settings.get("provider_snapshots_cluster_identifier")
            or cfg.get("cluster_identifier")
            or ""
        ).strip()
        try:
            snapshots.require_enabled(cfg, settings)
            if not cluster_identifier:
                raise ValueError("provider snapshot cluster identifier is required")
            checks.append(
                CheckResult(
                    id="provider.snapshot_config",
                    status=STATUS_PASS,
                    detail=f"configured cluster {cluster_identifier}",
                )
            )
        except Exception as exc:
            checks.append(
                CheckResult(
                    id="provider.snapshot_config",
                    status=STATUS_FAIL,
                    detail=str(exc),
                )
            )
        would_capture = {
            "provider_snapshot": {
                "cluster_identifier": cluster_identifier or None,
                "inventory": "opaque_until_provider_restore",
            },
            "data_scope": _target_identity(cfg, backup_class=resolved_class)[
                "data_scope"
            ],
            "state_inventory": [],
            "excluded_state": _excluded_state_for_backup_class(resolved_class),
        }
        checks.append(_free_space_check(storage, settings))
        return BackupPlan(
            backup_class=resolved_class,
            target_label=target_label(cfg),
            schema_name=str(cfg["schema_name"]),
            storage=storage.describe(),
            checks=checks,
            would_capture=would_capture,
        )

    dump_version = (
        engine.client_version("pg_dump")
        if resolved_class == BACKUP_CLASS_FULL
        else None
    )
    checks.append(
        CheckResult(
            id="client.pg_dump",
            status=(
                STATUS_PASS
                if dump_version
                else (
                    STATUS_FAIL if resolved_class == BACKUP_CLASS_FULL else STATUS_SKIP
                )
            ),
            detail=(
                dump_version
                or (
                    "pg_dump not found on PATH"
                    if resolved_class == BACKUP_CLASS_FULL
                    else "not used by template-pack"
                )
            ),
        )
    )

    try:
        connection_role = _connection_role_for_backup_class(resolved_class)
        assert connection_role is not None
        with open_session(
            cfg,
            app_username="tapdb_backup_plan",
            connection_role=connection_role,
        ) as conn:
            with conn.session_scope(commit=False) as session:
                schema_name = str(cfg["schema_name"])
                versions = introspect.server_version(session)
                visible_tables = introspect.list_tables(session, schema_name)
                if resolved_class == BACKUP_CLASS_FULL:
                    tables = visible_tables
                    sequences = introspect.capture_sequences(session, schema_name)
                    drift = _schema_drift(session, cfg)
                else:
                    tables = (
                        ["generic_template"]
                        if "generic_template" in visible_tables
                        else []
                    )
                    sequences = []
                    drift = _not_applicable_drift()

                would_capture = {
                    "tables": tables,
                    "table_count": len(tables),
                    "sequence_count": len(sequences),
                    "postgres": versions,
                    "data_scope": _target_identity(cfg, backup_class=resolved_class)[
                        "data_scope"
                    ],
                    # Issue #89 item 2: the state inventory says what a backup
                    # covers *and what it does not*. It existed only as an
                    # unused export, so the one command whose whole job is
                    # answering "what would this capture?" never showed it --
                    # and the excluded half is the part an operator most needs
                    # before assuming a restore is sufficient.
                    "state_inventory": _state_inventory_for_backup_class(
                        resolved_class
                    ),
                    "excluded_state": _excluded_state_for_backup_class(resolved_class),
                }

                checks.append(
                    CheckResult(
                        id="target.reachable",
                        status=STATUS_PASS,
                        detail=str(versions.get("server_version") or ""),
                        data=versions,
                    )
                )
                checks.append(
                    CheckResult(
                        id="schema.present",
                        status=STATUS_PASS if tables else STATUS_FAIL,
                        detail=f"{len(tables)} table(s) in {schema_name}",
                    )
                )

                drifted = bool(drift.get("has_drift"))
                checks.append(
                    CheckResult(
                        id="schema.drift",
                        status=(
                            STATUS_SKIP
                            if resolved_class != BACKUP_CLASS_FULL
                            else (
                                STATUS_PASS
                                if not drifted
                                else (STATUS_FAIL if strict_drift else STATUS_WARN)
                            )
                        ),
                        detail=(
                            "not applicable to template-pack"
                            if resolved_class != BACKUP_CLASS_FULL
                            else (
                                "no drift"
                                if not drifted
                                else "live schema differs from the schema assets"
                            )
                        ),
                        data=drift.get("counts", {}),
                    )
                )

                server_version_text = versions.get("server_version")
                if resolved_class != BACKUP_CLASS_FULL:
                    checks.append(
                        CheckResult(
                            id="version.compatible",
                            status=STATUS_SKIP,
                            detail="pg_dump is not used by template-pack",
                        )
                    )
                else:
                    try:
                        engine.assert_dump_client_is_new_enough(
                            client_version_text=dump_version,
                            server_version_text=server_version_text,
                        )
                        # Say what was actually compared. A bare "✓" with no
                        # detail reads as though the check failed to render,
                        # and it hides the two numbers an operator needs when
                        # it *does* fail.
                        client_major = engine.parse_version_major(dump_version)
                        server_major = engine.parse_version_major(server_version_text)
                        checks.append(
                            CheckResult(
                                id="version.compatible",
                                status=STATUS_PASS,
                                detail=(
                                    f"pg_dump {client_major} can dump server {server_major}"
                                    if client_major and server_major
                                    else "version comparison unavailable"
                                ),
                                data={
                                    "pg_dump_major": client_major,
                                    "server_major": server_major,
                                },
                            )
                        )
                    except Exception as exc:
                        checks.append(
                            CheckResult(
                                id="version.compatible",
                                status=STATUS_FAIL,
                                detail=str(exc),
                            )
                        )
    except Exception as exc:
        checks.append(
            CheckResult(
                id="target.reachable",
                status=STATUS_FAIL,
                detail=f"could not read the target: {exc}",
            )
        )

    checks.append(_free_space_check(storage, settings))

    return BackupPlan(
        backup_class=resolved_class,
        target_label=target_label(cfg),
        schema_name=str(cfg["schema_name"]),
        storage=storage.describe(),
        checks=checks,
        would_capture=would_capture,
    )


def _free_space_check(storage: Any, settings: dict[str, Any]) -> CheckResult:
    """Report free space, or skip when the destination is remote.

    Uses ``describe()`` rather than probing a path, so planning cannot leave a
    directory behind on a target it only meant to inspect.
    """
    if storage.describe().get("backend") != "local":
        return CheckResult(
            id="storage.free_space",
            status=STATUS_SKIP,
            detail="remote storage; capacity is not checked locally",
        )
    try:
        usage = shutil.disk_usage(Path(settings["config_dir"]))
    except OSError as exc:
        return CheckResult(id="storage.free_space", status=STATUS_WARN, detail=str(exc))
    return CheckResult(
        id="storage.free_space",
        status=STATUS_PASS,
        detail=f"{usage.free // (1024 * 1024)} MiB free",
        data={"free_bytes": usage.free},
    )


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


def create_backup(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_class: str = BACKUP_CLASS_FULL,
    dry_run: bool = False,
    allow_drift: bool = False,
    note: Optional[str] = None,
    actor: Optional[Actor] = None,
    existing_snapshot: Optional[str] = None,
    provenance: Optional[dict[str, Any]] = None,
) -> BackupResult:
    """Capture a backup and record an immutable receipt.

    The database is only ever read. Drift blocks the run unless explicitly
    allowed, so an object created by hand-DDL cannot silently ride along in a
    backup that claims to match the schema assets.

    ``provenance`` records *why* this backup exists -- ``{"created_by":
    "restore", "restored_backup_id": ...}`` for a pre-restore safety backup.
    Omitting it records ``{"created_by": "operator"}``, never ``{}``: an empty
    block has to keep meaning "manifest predates this field", so routine
    backups must say so positively. It reaches the manifest constructor and is
    therefore covered by the signature; see ``_capture``.
    """
    resolved_class = _validate_backup_class(backup_class)
    resolved_actor = actor or Actor(surface=SURFACE_CLI)
    storage = storage_for(settings)
    now = datetime.now(UTC)
    backup_id = new_backup_id(resolved_class, now=now)
    prefix = backup_prefix(
        str(cfg["client_id"]),
        str(cfg["database_name"]),
        resolved_class,
        backup_id,
    )

    if dry_run:
        plan = plan_backup(cfg, settings, backup_class=resolved_class)
        return BackupResult(
            backup_id=backup_id,
            backup_class=resolved_class,
            storage_prefix=prefix,
            verify=VerifyReport(backup_id=backup_id, level="plan", checks=plan.checks),
            dry_run=True,
        )

    staging = Path(tempfile.mkdtemp(prefix="tapdb-backup-"))
    try:
        manifest = _capture(
            cfg,
            settings,
            backup_class=resolved_class,
            backup_id=backup_id,
            staging=staging,
            allow_drift=allow_drift,
            note=note,
            now=now,
            storage=storage,
            existing_snapshot=existing_snapshot,
            provenance=provenance,
        )
        _publish(storage, prefix, manifest, staging)
        report = verify_backup(
            cfg,
            settings,
            backup_id=backup_id,
            level=VERIFY_QUICK,
            record_receipt=False,
        )
        if not report.ok:
            raise BackupVerificationError(
                "Backup failed its own post-write verification.",
                detail={"backup_id": backup_id, "report": report.to_payload()},
            )
    except Exception as exc:
        # Leave no half-written backup behind to be mistaken for a good one.
        try:
            storage.delete_prefix(prefix)
        except Exception:  # pragma: no cover - cleanup must not mask the cause
            pass
        write_receipt(
            receipts_directory(settings),
            operation=OPERATION_CREATE,
            status=STATUS_FAILED,
            actor=resolved_actor,
            backup_id=backup_id,
            backup_class=resolved_class,
            target_label=target_label(cfg),
            detail={"error": str(exc), "note": note},
            receipt_mirror=settings.get("receipt_mirror") or {},
        )
        raise
    finally:
        shutil.rmtree(staging, ignore_errors=True)

    receipt = write_receipt(
        receipts_directory(settings),
        operation=OPERATION_CREATE,
        status=STATUS_SUCCEEDED,
        actor=resolved_actor,
        backup_id=backup_id,
        backup_class=resolved_class,
        target_label=target_label(cfg),
        detail={
            "storage_prefix": prefix,
            "note": note,
            "row_counts": manifest.row_counts,
            "consistency": manifest.consistency.get("mode"),
            # A create is not trusted until it has verified its own artifact,
            # which it does above with `record_receipt=False`. Recording those
            # verdicts here is what makes "succeeded" mean something a reader
            # can check, rather than a claim with nothing behind it.
            "checks": compact_checks(report.checks),
        },
        receipt_mirror=settings.get("receipt_mirror") or {},
    )

    return BackupResult(
        backup_id=backup_id,
        backup_class=resolved_class,
        storage_prefix=prefix,
        manifest=manifest,
        verify=report,
        receipt_id=receipt.receipt_id,
    )


def _capture(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_class: str,
    backup_id: str,
    staging: Path,
    allow_drift: bool,
    note: Optional[str],
    now: datetime,
    storage: Any,
    existing_snapshot: Optional[str] = None,
    provenance: Optional[dict[str, Any]] = None,
) -> BackupManifest:
    """Read the target and build the artifact plus its manifest."""
    schema_name = str(cfg["schema_name"])
    artifact = staging / _artifact_name(backup_class)

    connection_role = _connection_role_for_backup_class(backup_class)
    if connection_role is None:
        from daylily_tapdb.backup import snapshots

        receipt = (
            snapshots.describe_cluster_snapshot(
                cfg, settings, snapshot_identifier=existing_snapshot
            )
            if existing_snapshot
            else snapshots.create_cluster_snapshot(cfg, settings, now=now)
        )
        artifact.write_bytes(canonical_bytes(receipt))
        content_inventory = {
            "snapshot_identifier": receipt.get("snapshot_identifier"),
            "cluster_identifier": receipt.get("cluster_identifier"),
            "engine_version": receipt.get("engine_version"),
            "encrypted": receipt.get("encrypted"),
            "capture_scope": "provider_cluster_snapshot",
            "tapdb_inventory": "opaque_until_provider_restore",
        }
        versions = {"server_version": receipt.get("engine_version")}
        snapshot = None
        backend = {"cluster_identifier": receipt.get("cluster_identifier")}
        drift = _not_applicable_drift()
        row_counts = {}
        representatives = []
        migrations = []
        sequences = []
    else:
        connection_cfg = connection_config_for_role(cfg, connection_role)
        with open_session(
            cfg,
            app_username="tapdb_backup_create",
            connection_role=connection_role,
        ) as conn:
            # A dedicated repeatable-read connection, not a scoped session: the
            # snapshot must outlive every read *and* the dump subprocess, and a
            # scoped session cannot export one at all (see snapshot_transaction).
            with introspect.snapshot_transaction(conn) as (session, snapshot):
                backend = introspect.resolved_backend_address(session)
                versions = introspect.server_version(session)
                drift = (
                    _schema_drift(session, cfg)
                    if backup_class == BACKUP_CLASS_FULL
                    else _not_applicable_drift()
                )
                if drift.get("has_drift") and not allow_drift:
                    raise BackupVerificationError(
                        "Live schema has drifted from the schema assets. Re-run with "
                        "allow_drift to capture anyway.",
                        detail={"drift": drift.get("counts", {})},
                    )

                if backup_class == BACKUP_CLASS_TEMPLATE_PACK:
                    pack = template_pack.build_template_pack(
                        session, schema_name, note=note
                    )
                    problems = template_pack.validate_template_pack(pack)
                    if problems:
                        raise BackupVerificationError(
                            "Exported template pack failed validation.",
                            detail={"problems": problems},
                        )
                    artifact.write_bytes(canonical_bytes(pack))
                    content_inventory = template_pack.pack_summary(pack) | {
                        "visibility_scope": _target_identity(
                            cfg, backup_class=backup_class
                        )["data_scope"]
                    }
                    row_counts = {"generic_template": len(pack["templates"])}
                    representatives = []
                    migrations = []
                    sequences = []
                else:
                    row_counts = introspect.capture_row_counts(session, schema_name)
                    representatives = introspect.capture_representative_objects(
                        session, schema_name
                    )
                    migrations = introspect.capture_migrations(session, schema_name)
                    # Degrade rather than lie: a snapshot we cannot pin the dump
                    # to would yield manifest counts that silently disagree with
                    # the archive, which is worse than an honest `best_effort`.
                    # Only a remote target can serve the dump from a different
                    # backend than the snapshot session. A local connection
                    # always reaches the same postmaster.
                    pinned_snapshot = snapshot
                    if (
                        snapshot
                        and str(cfg.get("engine_type") or "").lower() == "aurora"
                        and not backend.get("address")
                    ):
                        pinned_snapshot = None
                    content_inventory = _run_dump(
                        connection_cfg,
                        schema_name=schema_name,
                        artifact=artifact,
                        snapshot=pinned_snapshot,
                        backend=backend,
                        transaction_context=conn.transaction_context(),
                    )
                    if snapshot and pinned_snapshot is None:
                        snapshot = None
                    # Sequences are non-transactional: read after the dump so
                    # the recorded value is a lower bound on the live one.
                    sequences = introspect.capture_sequences(session, schema_name)

    manifest = BackupManifest(
        backup_id=backup_id,
        backup_class=backup_class,
        tool=_tool_block(),
        target_identity=_target_identity(cfg, backup_class=backup_class),
        postgres=versions,
        consistency={
            "mode": (
                "provider_snapshot"
                if backup_class == BACKUP_CLASS_PROVIDER_SNAPSHOT
                else (CONSISTENCY_SNAPSHOT if snapshot else CONSISTENCY_BEST_EFFORT)
            ),
            "snapshot": snapshot,
            "backend": backend,
        },
        migrations={
            "applied": migrations,
            "asset_checksums": drift.get("asset_checksums", []),
        },
        schema_drift=(
            drift.get("counts", {}) | {"has_drift": drift.get("has_drift")}
            if backup_class == BACKUP_CLASS_FULL
            else {"status": "not_applicable", "has_drift": None}
        ),
        row_counts=row_counts,
        sequences=sequences,
        representative_objects=representatives,
        content_inventory=content_inventory,
        governance=_governance_block(cfg),
        included_assets=[AssetRef.from_file(artifact)],
        excluded_state=_excluded_state_for_backup_class(backup_class),
        storage=storage.describe(),
        encryption={"mode": settings.get("encryption_mode", "none")},
        retention={"keep_last": settings.get("keep_last")},
        # Passed to the constructor, never assigned afterwards. The signature
        # below covers whatever fields exist at that moment, so setting
        # provenance after signing would put it in the stored bytes but outside
        # the signature -- `signature_scheme` would return "invalid" and every
        # safety backup would fail its own verification.
        #
        # Defaulting to "operator" rather than {} is what makes the field
        # useful. Absence has to keep meaning "written before this field
        # existed"; if routine backups also stored {}, a reader could never
        # tell a legacy manifest from a new ordinary one, and the unknown case
        # would never decay -- it would grow forever alongside the store.
        provenance=dict(provenance or {"created_by": PROVENANCE_OPERATOR}),
        timestamps={"started_at": now.isoformat(), "note": note},
    )
    manifest.signature = sign_manifest(
        manifest.to_payload(),
        mode=str(settings.get("signing_mode") or "none"),
        kms_key_arn=str(settings.get("signing_kms_key_arn") or ""),
    )
    return manifest


def _client_resolved_address(host: str) -> Optional[str]:
    """Resolve a hostname from *this* machine, preferring IPv4.

    Used to pin a snapshot-consistent dump to the same backend the snapshot
    session reached. It must be the client's view: the server's own
    `inet_server_addr()` is a VPC-internal address that a client outside the
    VPC cannot route to.
    """
    import socket

    if not host:
        return None
    try:
        infos = socket.getaddrinfo(host, None)
    except OSError:
        return None
    for info in infos:
        if info[0] == socket.AF_INET:
            return str(info[4][0])
    return str(infos[0][4][0]) if infos else None


def _run_dump(
    cfg: dict[str, Any],
    *,
    schema_name: str,
    artifact: Path,
    snapshot: Optional[str],
    transaction_context: Any,
    backend: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Run pg_dump and return the archive's own content inventory.

    When a snapshot is in play the dump subprocess **must** reach the same
    backend that exported it -- Aurora's reader and writer endpoints resolve to
    different hosts, and `--snapshot` against the wrong one either fails or
    silently produces a manifest whose counts disagree with the archive.
    Plan section 3.2 requires pinning via `PGHOSTADDR`, which was recorded in
    the manifest but never actually applied.
    """
    from daylily_tapdb.security_context import transaction_context_pgoptions

    env = engine.client_env(cfg)
    env["PGOPTIONS"] = transaction_context_pgoptions(transaction_context)
    # Pin to the address the *client* resolves, never to the one the server
    # reports for itself.
    #
    # `inet_server_addr()` returns the backend's VPC-internal address
    # (172.31.x.x). Pinning to it works only from inside the VPC; from anywhere
    # else pg_dump fails with "Network is unreachable" -- which is exactly what
    # happened the first time this ran against a real Aurora cluster. The goal
    # is only that the session and the dump reach the *same* backend, and
    # resolving the endpoint once on this side achieves that while staying
    # routable from wherever the client actually is.
    #
    # An operator-configured `hostaddr` still wins: that is how a private
    # cluster is reached through a tunnel, where `host` carries the real
    # endpoint for verify-full TLS and `hostaddr` points at the local end.
    if (
        snapshot
        and not cfg.get("hostaddr")
        and str(cfg.get("engine_type") or "").lower() == "aurora"
    ):
        resolved = _client_resolved_address(str(cfg.get("host") or ""))
        if resolved:
            env["PGHOSTADDR"] = resolved
    result = engine.run_command(
        engine.build_pg_dump_command(
            cfg,
            schema_name=schema_name,
            output_path=artifact,
            snapshot=snapshot,
        ),
        env=env,
    )
    if not result.ok:
        raise BackupVerificationError(
            f"pg_dump failed: {result.output[:500]}",
            detail={"returncode": result.returncode},
        )

    listing = engine.run_command(
        engine.build_pg_restore_list_command(artifact), env=env
    )
    if not listing.ok:
        raise BackupVerificationError(
            f"Could not read the archive table of contents: {listing.output[:400]}"
        )
    inventory = engine.parse_toc(listing.stdout)

    seen = inventory.schema_names_seen()
    if seen and seen != [schema_name]:
        raise BackupVerificationError(
            "Dump is not scoped to the configured schema.",
            detail={"expected": schema_name, "found": seen},
        )
    return inventory.to_payload()


def _publish(
    storage: Any,
    prefix: str,
    manifest: BackupManifest,
    staging: Path,
) -> None:
    """Write artifact, manifest, and detached checksum to storage."""
    for asset in manifest.included_assets:
        storage.put_file(f"{prefix}/{asset.name}", staging / asset.name)
    manifest_bytes = manifest.to_bytes()
    storage.put_bytes(f"{prefix}/{MANIFEST_KEY}", manifest_bytes)
    storage.put_bytes(
        f"{prefix}/{MANIFEST_CHECKSUM_KEY}",
        (sha256_hex(manifest_bytes) + "\n").encode("utf-8"),
    )


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------


def _load_manifest(storage: Any, prefix: str) -> BackupManifest:
    """Read and parse a stored manifest.

    Parsing is guarded as well as fetching: an unreadable manifest is a
    damaged *backup*, not a crash in whatever happened to be reading it.
    """
    try:
        raw = storage.get_bytes(f"{prefix}/{MANIFEST_KEY}")
    except Exception as exc:
        raise BackupNotFoundError(
            f"No manifest at {prefix}", detail={"prefix": prefix}
        ) from exc
    try:
        return BackupManifest.from_bytes(raw)
    except (ValueError, KeyError, TypeError) as exc:
        raise BackupVerificationError(
            f"Manifest at {prefix} could not be parsed",
            detail={"prefix": prefix, "error": str(exc)},
        ) from exc


def find_backup_prefix(
    cfg: dict[str, Any],
    storage: Any,
    backup_id: str,
) -> str:
    """Locate a backup by id without needing to know its class."""
    root = database_prefix(str(cfg["client_id"]), str(cfg["database_name"]))
    for prefix in discover_backup_prefixes(storage.list_keys(root)):
        if prefix.rsplit("/", 1)[-1] == backup_id:
            return prefix
    raise BackupNotFoundError(
        f"No backup named {backup_id}", detail={"backup_id": backup_id}
    )


def verify_backup(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_id: Optional[str] = None,
    path: Optional[Path] = None,
    level: str = VERIFY_DEEP,
    actor: Optional[Actor] = None,
    record_receipt: bool = True,
) -> VerifyReport:
    """Verify a backup's integrity. Reads only -- never touches a database.

    ``quick`` checks checksums and the archive's table of contents. ``deep``
    additionally reads every data block, which is what allows a corrupted
    backup to be rejected before a restore mutates anything.
    """
    resolved_level = str(level or VERIFY_DEEP).lower()
    checks: list[CheckResult] = []
    storage = storage_for(settings)

    if path is not None:
        artifact = Path(path)
        resolved_id = backup_id or artifact.stem
        # `quick` must mean quick here too. This always ran the deep read, so
        # `--level quick` on a loose archive silently did the expensive thing
        # and reported `level: quick` -- and the run left no receipt at all,
        # unlike every other verify.
        checks.append(_toc_only_check(artifact))
        if resolved_level == VERIFY_DEEP:
            checks.append(_deep_read_check(artifact))
        report = VerifyReport(
            backup_id=resolved_id, level=resolved_level, checks=checks
        )
        if record_receipt:
            receipt = write_receipt(
                receipts_directory(settings),
                operation=OPERATION_VERIFY,
                status=STATUS_SUCCEEDED if report.ok else STATUS_FAILED,
                actor=actor or Actor(surface=SURFACE_CLI),
                backup_id=resolved_id,
                target_label=target_label(cfg),
                detail={
                    "level": resolved_level,
                    "ok": report.ok,
                    "path": str(artifact),
                    "checks": compact_checks(report.checks),
                },
                receipt_mirror=settings.get("receipt_mirror") or {},
            )
            report = replace(report, receipt_id=receipt.receipt_id)
        return report

    if not backup_id:
        raise ValueError("verify_backup requires either backup_id or path")

    prefix = find_backup_prefix(cfg, storage, backup_id)
    manifest = _load_manifest(storage, prefix)

    manifest_bytes = storage.get_bytes(f"{prefix}/{MANIFEST_KEY}")
    try:
        recorded = storage.get_bytes(f"{prefix}/{MANIFEST_CHECKSUM_KEY}")
        expected = recorded.decode("utf-8").strip()
    except Exception:
        expected = ""
    actual = sha256_hex(manifest_bytes)
    checks.append(
        CheckResult(
            id="manifest.checksum",
            status=STATUS_PASS if expected == actual else STATUS_FAIL,
            detail=(
                "manifest matches its detached checksum"
                if expected == actual
                else "manifest bytes do not match manifest.sha256"
            ),
        )
    )

    # The signature block, which had no caller at all until now -- so a broken
    # signature would have shipped indefinitely without anyone noticing.
    import json as _json

    scheme = signature_scheme(
        _json.loads(manifest_bytes.decode("utf-8")), manifest.signature
    )
    checks.append(
        CheckResult(
            id="manifest.signature",
            status={
                "valid": STATUS_PASS,
                "legacy": STATUS_WARN,
                "invalid": STATUS_FAIL,
            }[scheme],
            detail={
                "valid": (
                    f"{(manifest.signature or {}).get('algorithm', 'none')} "
                    "signature matches the manifest"
                ),
                "legacy": (
                    "signature predates the signature-scope fix; manifest bytes "
                    "are still covered by manifest.sha256"
                ),
                "invalid": "manifest signature does not match its payload",
            }[scheme],
            data={
                "algorithm": (manifest.signature or {}).get("algorithm"),
                "scheme": scheme,
            },
        )
    )

    staged = Path(tempfile.mkdtemp(prefix="tapdb-verify-"))
    try:
        for asset in manifest.included_assets:
            local = storage.get_file(f"{prefix}/{asset.name}", staged / asset.name)
            digest = sha256_file(local)
            checks.append(
                CheckResult(
                    id=f"asset.checksum:{asset.name}",
                    status=STATUS_PASS if digest == asset.sha256 else STATUS_FAIL,
                    detail=(
                        "matches manifest"
                        if digest == asset.sha256
                        else "artifact checksum does not match the manifest"
                    ),
                )
            )
            if manifest.backup_class == BACKUP_CLASS_FULL:
                checks.append(_toc_check(local, manifest))
                if resolved_level == VERIFY_DEEP:
                    checks.append(_deep_read_check(local))
    finally:
        shutil.rmtree(staged, ignore_errors=True)

    report = VerifyReport(backup_id=backup_id, level=resolved_level, checks=checks)

    receipt = None
    if record_receipt:
        receipt = write_receipt(
            receipts_directory(settings),
            operation=OPERATION_VERIFY,
            status=STATUS_SUCCEEDED if report.ok else STATUS_FAILED,
            actor=actor or Actor(surface=SURFACE_CLI),
            backup_id=backup_id,
            backup_class=manifest.backup_class,
            target_label=target_label(cfg),
            detail={
                "level": resolved_level,
                "ok": report.ok,
                "checks": compact_checks(report.checks),
            },
            receipt_mirror=settings.get("receipt_mirror") or {},
        )

    return replace(report, receipt_id=receipt.receipt_id if receipt else None)


def _toc_check(artifact: Path, manifest: BackupManifest) -> CheckResult:
    """Compare the archive's own table of contents to the manifest."""
    listing = engine.run_command(engine.build_pg_restore_list_command(artifact))
    if not listing.ok:
        return CheckResult(
            id="archive.toc",
            status=STATUS_FAIL,
            detail=f"could not read table of contents: {listing.output[:200]}",
        )
    inventory = engine.parse_toc(listing.stdout)
    recorded = manifest.content_inventory.get("schema_names_seen") or []
    seen = inventory.schema_names_seen()
    if recorded and seen != recorded:
        return CheckResult(
            id="archive.toc",
            status=STATUS_FAIL,
            detail="archive schema scope does not match the manifest",
            data={"manifest": recorded, "archive": seen},
        )
    return CheckResult(
        id="archive.toc",
        status=STATUS_PASS,
        detail=f"{len(inventory.entries)} entries; scope {seen}",
        data=inventory.to_payload(),
    )


def _toc_only_check(artifact: Path) -> CheckResult:
    """Prove the archive's table of contents is readable, without a deep read.

    This is what `--level quick` means for a loose archive: it establishes the
    file is a real pg_dump custom-format archive and enumerates its contents,
    without decompressing every data block.
    """
    result = engine.run_command(engine.build_pg_restore_list_command(artifact))
    if not result.ok:
        return CheckResult(
            id="archive.toc",
            status=STATUS_FAIL,
            detail=f"table of contents unreadable: {result.output[:200]}",
        )
    inventory = engine.parse_toc(result.stdout)
    return CheckResult(
        id="archive.toc",
        status=STATUS_PASS,
        detail=(
            f"{len(inventory.entries)} entries; scope {inventory.schema_names_seen()}"
        ),
        data={"counts_by_kind": inventory.counts_by_kind()},
    )


def _deep_read_check(artifact: Path) -> CheckResult:
    """Decompress and read every data block without touching a database."""
    result = engine.run_command(engine.build_pg_restore_verify_command(artifact))
    return CheckResult(
        id="archive.deep_read",
        status=STATUS_PASS if result.ok else STATUS_FAIL,
        detail=(
            "archive reads end to end"
            if result.ok
            else f"archive is unreadable: {result.output[:200]}"
        ),
    )


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def list_backups(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    backup_class: Optional[str] = None,
    limit: Optional[int] = None,
) -> BackupListing:
    """List discoverable backups, newest first.

    Discovery scans for manifests rather than reading an index, so a backup
    copied in by hand is listed and a damaged index cannot hide a good backup.
    """
    storage = storage_for(settings)
    root = database_prefix(str(cfg["client_id"]), str(cfg["database_name"]))
    wanted = _validate_backup_class(backup_class) if backup_class else None

    entries: list[BackupSummary] = []
    damaged: list[str] = []
    for prefix in discover_backup_prefixes(storage.list_keys(root)):
        try:
            manifest = _load_manifest(storage, prefix)
        except (BackupNotFoundError, BackupVerificationError):
            # One damaged backup must not hide every good one -- that is the
            # exact failure the index-free design exists to avoid. Record it
            # so the damage is visible instead of silently swallowed.
            damaged.append(prefix)
            continue
        if wanted and manifest.backup_class != wanted:
            continue
        entries.append(
            BackupSummary(
                backup_id=manifest.backup_id,
                backup_class=manifest.backup_class,
                created_at=manifest.timestamps.get("started_at"),
                status=manifest.status,
                storage_prefix=prefix,
                target_label=manifest.target_identity.get("target_label"),
                row_totals=(
                    sum(manifest.row_counts.values()) if manifest.row_counts else None
                ),
                bytes=sum(asset.bytes for asset in manifest.included_assets),
            )
        )

    entries.sort(key=lambda item: (item.created_at or "", item.backup_id), reverse=True)
    if limit is not None and limit >= 0:
        entries = entries[:limit]
    return BackupListing(
        entries=entries,
        storage=storage.describe(),
        damaged=sorted(damaged),
    )


# ---------------------------------------------------------------------------
# Health -- the alerting contract
#
# One rule governs every verdict below: **exit 0 must mean recoverable.** A
# check that can pass while the target cannot actually be restored is worse
# than no check, because it converts "nobody is watching" into "something is
# watching and says we are fine".
#
# Deliberately needs no database. Everything here reads receipts, storage and
# config, so health still answers when the database is down -- which is exactly
# when somebody is asking. Exit 2 therefore means config or storage could not
# be consulted; it never means "the database is down".
# ---------------------------------------------------------------------------

#: Sources health draws on. Named so an unavailable one can be reported
#: precisely rather than collapsing into "something went wrong".
#: Backup classes that are an actual route back to working data.
#:
#: ``template-pack`` is deliberately absent. The runbook is explicit that it is
#: "a configuration export, not a recovery tool", and ``restore_backup``
#: refuses it outright -- so a store of nothing but template packs has no
#: recovery path at all, however many rows it lists.
RECOVERY_CLASSES = (BACKUP_CLASS_FULL, BACKUP_CLASS_PROVIDER_SNAPSHOT)

#: Artifact size above which health skips the full checksum read. Health is
#: polled; `backup verify` is the audit. Hashing a 50 GB dump on every poll
#: would move terabytes a day and is not what a liveness check is for.
HEALTH_VERIFY_MAX_BYTES = 1024 * 1024 * 1024

#: Check ids each source is responsible for. Kept as explicit tuples so an
#: unavailable source still emits a complete, predictable set of rows -- a
#: consumer must never have to guess whether a missing id means "not checked"
#: or "not implemented".
RECEIPT_CHECK_IDS = (
    "health.receipt_chain",
    "health.last_attempt",
    "health.cadence",
    "health.never_run",
    "health.rehearsal_age",
    "health.interrupted_prune",
    "health.receipt_mirror",
)

STORAGE_CHECK_IDS = (
    "health.inventory",
    "health.recovery_point",
    "health.receipt_coverage",
    "health.damaged",
    "health.hollow_backup",
    "health.newest_verifies",
    "health.storage_safety",
)

SOURCE_RECEIPTS = "receipts"
SOURCE_STORAGE = "storage"
SOURCE_SETTINGS = "settings"

HEALTH_OK = 0
HEALTH_FAILING = 1
HEALTH_UNAVAILABLE = 2


@dataclass(frozen=True)
class HealthReport:
    """Every health verdict for one target, plus how to exit on them."""

    target_label: str
    checks: list[CheckResult] = field(default_factory=list)
    #: Sources that could not be consulted at all. Distinct from a failed
    #: check: "I looked and it is broken" and "I could not look" are different
    #: operational problems and get different exit codes.
    unavailable: list[str] = field(default_factory=list)

    @property
    def failing(self) -> list[CheckResult]:
        return [check for check in self.checks if check.failed]

    @property
    def warnings(self) -> list[CheckResult]:
        return [check for check in self.checks if check.status == STATUS_WARN]

    @property
    def status(self) -> str:
        if self.failing:
            return "failing"
        if self.unavailable:
            return "unavailable"
        return "warn" if self.warnings else "ok"

    @property
    def exit_code(self) -> int:
        """1 if anything failed, else 2 if anything was unreachable, else 0.

        Failures outrank unavailability deliberately. A tampered receipt chain
        plus an unreachable bucket must exit 1, not 2 -- health *did* reach a
        verdict, and burying it under "could not answer" would suppress the
        alert exactly when someone has broken the audit trail.
        """
        if self.failing:
            return HEALTH_FAILING
        if self.unavailable:
            return HEALTH_UNAVAILABLE
        return HEALTH_OK

    def to_payload(self) -> dict[str, Any]:
        return {
            "target_label": self.target_label,
            "status": self.status,
            "exit_code": self.exit_code,
            "ok": self.exit_code == HEALTH_OK,
            "unavailable": self.unavailable,
            "failing": [check.id for check in self.failing],
            "warnings": [check.id for check in self.warnings],
            "checks": [check.to_payload() for check in self.checks],
        }


def _check(
    check_id: str,
    ok: bool,
    *,
    ok_detail: str,
    fail_detail: str,
    failed_status: str = STATUS_FAIL,
    data: Optional[dict] = None,
) -> CheckResult:
    """Build one verdict, so every check below reads the same way."""
    return CheckResult(
        id=check_id,
        status=STATUS_PASS if ok else failed_status,
        detail=ok_detail if ok else fail_detail,
        data=data or {},
    )


def _health_receipt_checks(
    receipts_dir: Path,
    receipts: list[Any],
    *,
    settings: dict[str, Any],
    now: Optional[datetime],
) -> list[CheckResult]:
    """Verdicts derived from the local receipt store."""
    from daylily_tapdb.backup.receipts import (
        OPERATION_REHEARSE,
        _parse_created_at,
        _receipt_paths,
        _try_parse,
        derive_backup_status,
        read_head,
        verify_receipt_chain,
    )

    checks: list[CheckResult] = []
    moment = now or datetime.now(UTC)

    # -- chain integrity, split by cause -------------------------------------
    #
    # Tampering and local corruption both surface as findings, but they are not
    # the same incident and must not carry the same verdict. `write_receipt`
    # deliberately lets a corrupt file keep its sequence number rather than
    # block writes, so one partial write -- disk full, host reset mid-write --
    # leaves a permanent sequence gap with no repair command. Failing hard on
    # that pages forever, and the only way to silence it is to delete the
    # receipts directory, which destroys the audit trail. So a gap explained by
    # an unparseable file on disk warns and names the file; anything else, a
    # hash mismatch or a missing tip, is tampering and fails.
    chain = verify_receipt_chain(receipts, head=read_head(receipts_dir))
    unreadable = [
        path.name for path in _receipt_paths(receipts_dir) if _try_parse(path) is None
    ]
    # One unreadable file produces *two* findings, not one: the sequence gap it
    # leaves, and -- if it was the first receipt -- "first receipt must not
    # chain to a predecessor", because the surviving oldest receipt still
    # records a predecessor hash. Both are symptoms of the same skipped file.
    # Requiring every finding to be a sequence gap therefore graded ordinary
    # local corruption as tampering.
    #
    # A hash mismatch or a head-anchor mismatch is never in this family; those
    # mean the bytes were edited or the tip was removed, and they fail whether
    # or not an unreadable file is also present.
    corruption_symptoms = ("expected sequence", "first receipt must not chain")
    corruption_explains_it = bool(unreadable) and all(
        any(symptom in finding for symptom in corruption_symptoms)
        for finding in chain.findings
    )

    if chain.ok:
        checks.append(
            CheckResult(
                id="health.receipt_chain",
                status=STATUS_PASS,
                detail=f"receipt chain intact ({chain.count} receipts)",
                data={"count": chain.count},
            )
        )
    elif corruption_explains_it:
        checks.append(
            CheckResult(
                id="health.receipt_chain",
                status=STATUS_WARN,
                detail=(
                    "receipt chain has a sequence gap explained by unreadable "
                    f"file(s): {', '.join(unreadable)}. Local corruption, not "
                    "tampering -- the audit trail is incomplete but not forged."
                ),
                data={"findings": chain.findings, "unreadable": unreadable},
            )
        )
    else:
        checks.append(
            CheckResult(
                id="health.receipt_chain",
                status=STATUS_FAIL,
                detail="receipt chain does not verify: " + "; ".join(chain.findings),
                data={"findings": chain.findings, "unreadable": unreadable},
            )
        )

    interval = float(settings.get("expected_interval_hours") or 0)
    status = derive_backup_status(
        receipts, expected_interval_hours=interval, now=moment
    )

    # -- last attempt --------------------------------------------------------
    #
    # Keyed on `last_attempt_status`, NOT on `status == "failing"`.
    # `derive_backup_status` evaluates `never_run` first and returns a single
    # scalar, so a target whose every backup has failed reports `never_run` and
    # masks `failing` entirely. Thirty consecutive nightly failures on a new
    # production target would otherwise be a warning.
    last_attempt_status = status.get("last_attempt_status")
    checks.append(
        _check(
            "health.last_attempt",
            last_attempt_status != STATUS_FAILED,
            ok_detail=(
                "last backup attempt succeeded"
                if last_attempt_status
                else "no backup attempt recorded yet"
            ),
            fail_detail=(
                f"the last backup attempt failed at {status.get('last_attempt_at')}"
            ),
            data={
                "last_attempt_at": status.get("last_attempt_at"),
                "last_attempt_status": last_attempt_status,
                "last_success_at": status.get("last_success_at"),
            },
        )
    )

    # -- cadence -------------------------------------------------------------
    cadence_configured = bool(status.get("cadence_configured"))
    never_run = status.get("status") == "never_run"
    checks.append(
        _check(
            "health.cadence",
            status.get("status") != "stale",
            ok_detail=(
                f"last success {status.get('age_hours')}h ago, within the "
                f"{interval}h cadence"
                if cadence_configured and not never_run
                else "no cadence to be late against"
            ),
            fail_detail=(
                f"last success was {status.get('age_hours')}h ago, past the "
                f"{interval}h cadence"
            ),
            data={
                "expected_interval_hours": interval,
                "age_hours": status.get("age_hours"),
            },
        )
    )

    # `never_run` is informational and never independently drives the exit code.
    #
    # It was designed as a warning so a freshly provisioned target would not
    # page the moment it exists. That rationale does not survive
    # `health.inventory`: a target with no backups genuinely cannot be
    # recovered, and grading that as noise is the exact failure this command
    # exists to prevent. So inventory fails first, and every other route to
    # `never_run` -- backups with no create receipt, or a create receipt that
    # failed -- is already failed by receipt coverage or last attempt.
    #
    # It is kept because "this target has never had a successful backup" is
    # worth reading in the JSON next to whichever check did fail.
    checks.append(
        _check(
            "health.never_run",
            not never_run,
            ok_detail="a backup has succeeded at least once",
            fail_detail="no backup has ever succeeded for this target",
            failed_status=STATUS_WARN,
        )
    )

    # -- rehearsal age -------------------------------------------------------
    #
    # Only *succeeded* rehearsals count, and a failed newest rehearsal is its
    # own failure. Age alone would keep a nightly rehearsal that fails every
    # night permanently green -- recovery demonstrated broken, health silent.
    rehearsal_days = float(settings.get("expected_rehearsal_interval_days") or 0)
    rehearsals = [r for r in receipts if r.operation == OPERATION_REHEARSE]
    newest = rehearsals[-1] if rehearsals else None
    succeeded = [r for r in rehearsals if r.succeeded]
    newest_ok = succeeded[-1] if succeeded else None

    if newest is not None and not newest.succeeded:
        checks.append(
            CheckResult(
                id="health.rehearsal_age",
                status=STATUS_FAIL,
                detail=(
                    f"the most recent restore rehearsal failed at "
                    f"{newest.created_at} -- recovery is demonstrated broken"
                ),
                data={"last_rehearsal_at": newest.created_at, "last_ok": False},
            )
        )
    elif rehearsal_days <= 0:
        checks.append(
            CheckResult(
                id="health.rehearsal_age",
                status=STATUS_WARN,
                detail=(
                    "no rehearsal cadence configured "
                    "(backup.expected_rehearsal_interval_days)"
                ),
                data={"expected_rehearsal_interval_days": rehearsal_days},
            )
        )
    elif newest_ok is None:
        checks.append(
            CheckResult(
                id="health.rehearsal_age",
                status=STATUS_WARN,
                detail="no restore rehearsal has ever succeeded",
                data={"expected_rehearsal_interval_days": rehearsal_days},
            )
        )
    else:
        parsed = _parse_created_at(newest_ok.created_at)
        age_days = (
            None if parsed is None else (moment - parsed).total_seconds() / 86400.0
        )
        overdue = age_days is not None and age_days > rehearsal_days
        checks.append(
            _check(
                "health.rehearsal_age",
                not overdue,
                ok_detail=(
                    f"last successful rehearsal {age_days:.1f}d ago"
                    if age_days is not None
                    else "last rehearsal succeeded"
                ),
                fail_detail=(
                    f"last successful rehearsal was {age_days:.1f}d ago, past the "
                    f"{rehearsal_days}d cadence"
                ),
                data={
                    "age_days": None if age_days is None else round(age_days, 2),
                    "expected_rehearsal_interval_days": rehearsal_days,
                },
            )
        )

    # -- interrupted prune ---------------------------------------------------
    #
    # Reads receipts written by `backup prune`, which does not exist yet. It is
    # implemented now because the intent/outcome pair is only useful if
    # something consumes it, and a detector with no reader is how the first
    # draft of this plan left a known-detectable state undetected.
    #
    # **Warns rather than fails, deliberately.** Receipts are immutable, so a
    # dangling intent can never be cleared by writing anything; as a failure it
    # would page forever and the only way to silence it would be deleting the
    # audit trail -- which trains operators to do the single most destructive
    # thing available.
    #
    # It is safe to warn because the *damage* an interrupted prune causes --
    # a half-deleted prefix -- is caught independently by
    # `health.hollow_backup` and `health.damaged`, both of which fail. So the
    # case with real damage still pages, on its own merits; what this adds is
    # "go and look", which is a warning. As a failure it did the opposite of
    # what was wanted: paged forever when there was no damage, and paged twice
    # when there was.
    #
    # Prune itself owes the real fix (reconcile dangling intents on its next
    # run, so normal operation clears them) -- recorded as a binding
    # requirement for pass 2.
    # Vocabulary imported, not retyped. Health and prune previously spelled
    # these as literals on both sides *and* in both test suites, so a rename
    # would have left everything green with the detector dead.
    from daylily_tapdb.backup.receipts import (
        OPERATION_PRUNE,
        PRUNE_PHASE_INTENT,
        PRUNE_PHASE_OUTCOME,
    )

    intents = {
        r.detail.get("prune_id")
        for r in receipts
        if r.operation == OPERATION_PRUNE
        and r.detail.get("phase") == PRUNE_PHASE_INTENT
    }
    # An outcome that still lists work as `remaining` has not resolved
    # anything: the prefix is still half-deleted and nothing will raise the
    # signal again, so treating it as closed would retire the only "go and
    # look" prompt while the damage stands.
    outcomes = {
        r.detail.get("prune_id")
        for r in receipts
        if r.operation == OPERATION_PRUNE
        and r.detail.get("phase") == PRUNE_PHASE_OUTCOME
        and not (r.detail.get("remaining") or [])
    }
    dangling = sorted(pid for pid in intents - outcomes if pid)
    checks.append(
        _check(
            "health.interrupted_prune",
            not dangling,
            ok_detail="no interrupted prune",
            fail_detail=(
                "a prune started but never finished, so a backup prefix may be "
                f"half-deleted: {', '.join(dangling)}. Any actual damage is "
                "reported by health.hollow_backup and health.damaged."
            ),
            failed_status=STATUS_WARN,
            data={"prune_ids": dangling},
        )
    )

    # -- mirror freshness ----------------------------------------------------
    #
    # The mirror is best-effort and write-only, which means a mirror that has
    # silently written nothing for a month is behaviourally identical to a
    # working one from every other surface's point of view. Without this check
    # the feature ships in the same state its own docstring criticises: plumbed
    # end to end and observable by nobody.
    mirror = settings.get("receipt_mirror") or {}
    mirror_uri = str(mirror.get("uri") or "").strip()
    if not mirror_uri:
        checks.append(
            CheckResult(
                id="health.receipt_mirror",
                status=STATUS_SKIP,
                detail="no receipt mirror configured",
            )
        )
    elif not receipts:
        checks.append(
            CheckResult(
                id="health.receipt_mirror",
                status=STATUS_SKIP,
                detail="no receipts to mirror yet",
            )
        )
    else:
        newest_local = receipts[-1].sequence
        try:
            from daylily_tapdb.backup.storage import build_storage_backend

            backend = build_storage_backend(mirror_uri)
            mirrored = json.loads(backend.get_bytes("head.json").decode("utf-8"))
            newest_mirrored = int(mirrored.get("sequence") or 0)
        except Exception as exc:  # noqa: BLE001
            checks.append(
                CheckResult(
                    id="health.receipt_mirror",
                    status=STATUS_WARN,
                    detail=f"receipt mirror could not be read: {exc}",
                    data={"uri": mirror_uri},
                )
            )
        else:
            checks.append(
                _check(
                    "health.receipt_mirror",
                    newest_mirrored >= newest_local,
                    ok_detail=f"mirror is current at sequence {newest_mirrored}",
                    fail_detail=(
                        f"mirror is at sequence {newest_mirrored} but the local "
                        f"chain is at {newest_local} -- receipts are not reaching it"
                    ),
                    failed_status=STATUS_WARN,
                    data={
                        "uri": mirror_uri,
                        "mirrored": newest_mirrored,
                        "local": newest_local,
                    },
                )
            )

    return checks


def _health_settings_checks(settings: dict[str, Any]) -> list[CheckResult]:
    """Verdicts about configuration itself."""
    invalid = list(settings.get("invalid_fields") or [])
    interval = float(settings.get("expected_interval_hours") or 0)

    if "expected_interval_hours" in invalid:
        # Present but unparseable must FAIL, not warn. `_float` returns the
        # default on a parse error, and that default is 0 -- meaning "no
        # cadence" -- so `expected_interval_hours: "24h"` silently disarms the
        # only scheduler-stopped detector there is. A typo in the field that
        # arms the alarm must never look like a deliberate choice not to arm it.
        return [
            CheckResult(
                id="health.cadence_configured",
                status=STATUS_FAIL,
                detail=(
                    "backup.expected_interval_hours is set but is not a number, "
                    "so it was ignored and no staleness alarm is armed"
                ),
                data={"invalid_fields": invalid},
            )
        ]

    return [
        _check(
            "health.cadence_configured",
            interval > 0,
            ok_detail=f"cadence configured: every {interval}h",
            fail_detail=(
                "no backup cadence configured "
                "(backup.expected_interval_hours); staleness cannot be detected"
            ),
            failed_status=STATUS_WARN,
            data={"expected_interval_hours": interval},
        )
    ]


def _health_storage_safety_checks(
    storage: Any, listing: BackupListing
) -> list[CheckResult]:
    """Whether the store protects backups from deletion.

    Extracted so every path through `_health_storage_checks` emits it. It
    previously sat inline after an early `return`, so a target whose newest
    recovery point exceeded the read limit -- i.e. most real ones -- silently
    lost the row, breaking the same completeness guarantee the receipts
    source enforces.
    """
    checks: list[CheckResult] = []
    # -- storage safety ------------------------------------------------------
    #
    # `deletion_capability` arrives with prune. Absence is read as "unknown"
    # and skipped rather than assumed safe -- and because this is a warn-only
    # row, an unlocked bucket never pages: it is the normal state today and is
    # infrastructure's to change.
    probe = getattr(storage, "deletion_capability", None)
    if probe is None:
        checks.append(
            CheckResult(
                id="health.storage_safety",
                status=STATUS_SKIP,
                detail="this storage backend does not report deletion capability",
            )
        )
    else:
        try:
            capability = probe()
        except Exception as exc:  # noqa: BLE001
            checks.append(
                CheckResult(
                    id="health.storage_safety",
                    status=STATUS_WARN,
                    detail=f"storage safety probe failed: {exc}",
                )
            )
        else:
            # Warn only where the protection is both meaningful and missing.
            #
            # A local filesystem has no Object Lock or versioning concept, and
            # TapDB is not going to give it one -- warning about their absence
            # would fire on every developer machine and every local deployment
            # forever. An alert nobody can act on is the kind that gets muted,
            # which is what the whole warn/fail split exists to avoid.
            #
            # Unknown is different from absent, and always warns: a denied
            # probe means the bucket may well be locked.
            backend_name = str((listing.storage or {}).get("backend") or "")
            if capability.get("reclaims") is None:
                checks.append(
                    CheckResult(
                        id="health.storage_safety",
                        status=STATUS_WARN,
                        detail=(
                            "storage protections could not be determined: "
                            f"{capability.get('reason')}"
                        ),
                        data=capability,
                    )
                )
            elif backend_name == "local":
                checks.append(
                    CheckResult(
                        id="health.storage_safety",
                        status=STATUS_PASS,
                        detail=(
                            "local storage: Object Lock and versioning do not apply"
                        ),
                        data=capability,
                    )
                )
            else:
                concerns = []
                if capability.get("object_lock") is not True:
                    concerns.append("no Object Lock")
                if capability.get("versioning") not in ("Enabled",):
                    concerns.append("no versioning")
                checks.append(
                    _check(
                        "health.storage_safety",
                        not concerns,
                        ok_detail="storage has Object Lock and versioning",
                        fail_detail=(
                            "storage protections absent: " + ", ".join(concerns)
                        ),
                        failed_status=STATUS_WARN,
                        data=capability,
                    )
                )

    return checks


def _health_storage_checks(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    storage: Any,
    listing: BackupListing,
    create_receipts: Optional[int],
) -> list[CheckResult]:
    """Verdicts that require reading the backup store."""
    checks: list[CheckResult] = []

    # -- inventory -----------------------------------------------------------
    #
    # The check that makes exit 0 mean something. Receipts record *attempts*,
    # not inventory: retarget `backup.storage.uri` by one character, or let a
    # lifecycle rule expire the objects, and every receipt-derived check still
    # passes against a store with nothing in it. This also covers the case
    # where the receipts directory is deleted outright -- an empty chain
    # verifies as ok with count 0, so total erasure would otherwise report
    # healthier than partial truncation.
    recoverable = [
        entry for entry in listing.entries if entry.backup_class in RECOVERY_CLASSES
    ]

    checks.append(
        _check(
            "health.inventory",
            bool(listing.entries),
            ok_detail=f"{len(listing.entries)} backup(s) present",
            fail_detail=("no backups exist for this target -- nothing to recover from"),
            data={"count": len(listing.entries), "storage": listing.storage},
        )
    )

    # -- an actual recovery path ---------------------------------------------
    #
    # Counting rows is not the contract; being able to get the data back is.
    # A target backed up nightly as `template-pack` produces a long, healthy
    # looking listing and cannot be restored at all -- `restore_backup` rejects
    # every class but `full`, and a template pack carries no instance data by
    # design. Without this, `health.inventory` passes on exactly the store that
    # most needs the alarm.
    checks.append(
        _check(
            "health.recovery_point",
            bool(recoverable),
            ok_detail=(
                f"{len(recoverable)} recovery-point backup(s) "
                f"({', '.join(sorted({e.backup_class for e in recoverable}))})"
            ),
            fail_detail=(
                f"{len(listing.entries)} backup(s) exist but none is a recovery "
                "point -- template packs are a configuration export, not a way "
                "back to the data"
            ),
            data={
                "recoverable": len(recoverable),
                "classes": sorted({e.backup_class for e in listing.entries}),
            },
        )
    )

    # -- receipt coverage ----------------------------------------------------
    #
    # Backups present with no create receipt at all means the audit trail was
    # destroyed, and nothing else here would notice: the chain verifies
    # vacuously at count 0, `never_run` only warns, and inventory passes
    # because the backups really are there. Without this, wiping the receipt
    # store entirely reports healthier than corrupting one file of it -- which
    # rewards the more destructive act.
    #
    # A backup hand-copied in from another host trips this legitimately. That
    # is still worth surfacing: it is a backup whose provenance this host
    # cannot account for.
    if create_receipts is not None:
        checks.append(
            _check(
                "health.receipt_coverage",
                not (listing.entries and create_receipts == 0),
                ok_detail="backups are accounted for by create receipts",
                fail_detail=(
                    f"{len(listing.entries)} backup(s) exist but no create receipt "
                    "does -- the audit trail is missing, not merely incomplete"
                ),
                data={
                    "backups": len(listing.entries),
                    "create_receipts": create_receipts,
                },
            )
        )

    checks.append(
        _check(
            "health.damaged",
            not listing.damaged,
            ok_detail="every discovered backup has a readable manifest",
            fail_detail=(
                f"{len(listing.damaged)} backup prefix(es) have an unreadable "
                f"manifest: {', '.join(listing.damaged)}"
            ),
            data={"damaged": listing.damaged},
        )
    )

    # -- hollow backups ------------------------------------------------------
    #
    # `list_backups` reports `bytes` summed from the *manifest*, not from
    # storage, and `status` straight off the manifest -- so a prefix whose
    # artifacts are gone still lists as a full-size, complete, correctly-dated
    # backup. It is not "damaged" either, because the manifest parses. Only
    # verify would notice, and nothing runs verify automatically.
    hollow: list[dict[str, Any]] = []
    unchecked: list[str] = []
    for entry in listing.entries:
        # `_load_manifest` is inside the guard too. It re-reads a manifest that
        # `list_backups` already parsed, so a prefix deleted between the two
        # reads -- by a lifecycle rule, or by another run's cleanup -- would
        # otherwise propagate out of `health_report` entirely and turn a
        # perfectly healthy store into exit 2, discarding every verdict already
        # computed.
        try:
            # `list_sizes` is the same request as `list_keys` on S3 -- the
            # listing already carries Size -- so comparing bytes costs nothing
            # over comparing names. A host-supplied backend may not implement
            # it, in which case presence is all that can be checked.
            sizer = getattr(storage, "list_sizes", None)
            if sizer is not None:
                sizes = sizer(entry.storage_prefix)
            else:
                sizes = dict.fromkeys(storage.list_keys(entry.storage_prefix), -1)
            manifest = _load_manifest(storage, entry.storage_prefix)
        except Exception as exc:  # noqa: BLE001
            unchecked.append(f"{entry.backup_id}: {exc}")
            continue

        missing: list[str] = []
        truncated: list[dict[str, Any]] = []
        for asset in manifest.included_assets:
            key = f"{entry.storage_prefix}/{asset.name}"
            if key not in sizes:
                missing.append(asset.name)
                continue
            actual = sizes[key]
            # -1 marks "this backend cannot report size"; never treat unknown
            # as agreement.
            if actual >= 0 and actual != asset.bytes:
                truncated.append(
                    {"name": asset.name, "expected": asset.bytes, "actual": actual}
                )

        if missing or truncated:
            hollow.append(
                {
                    "backup_id": entry.backup_id,
                    "missing": missing,
                    "truncated": truncated,
                }
            )

    # An entry that could not be read is *not* evidence of health. Silently
    # dropping it -- the previous behaviour -- meant that if every per-prefix
    # read failed (S3 throttling at scale, or a bucket policy allowing LIST at
    # the root but denying it on sub-prefixes) this reported "every listed
    # backup's artifacts are present in storage" having checked nothing at all.
    if hollow:
        checks.append(
            CheckResult(
                id="health.hollow_backup",
                status=STATUS_FAIL,
                detail=(
                    "backup(s) list as complete but their artifacts are missing "
                    "or the wrong size: "
                    + ", ".join(item["backup_id"] for item in hollow)
                ),
                data={"hollow": hollow, "unchecked": unchecked},
            )
        )
    elif unchecked and len(unchecked) == len(listing.entries):
        checks.append(
            CheckResult(
                id="health.hollow_backup",
                status=STATUS_SKIP,
                detail=(
                    f"none of {len(listing.entries)} backup(s) could be inspected: "
                    + "; ".join(unchecked[:3])
                ),
                data={"unchecked": unchecked},
            )
        )
    else:
        checks.append(
            CheckResult(
                id="health.hollow_backup",
                status=STATUS_WARN if unchecked else STATUS_PASS,
                detail=(
                    f"{len(unchecked)} backup(s) could not be inspected: "
                    + "; ".join(unchecked[:3])
                    if unchecked
                    else (
                        "every listed backup's artifacts are present and the "
                        "size the manifest records"
                    )
                ),
                data={"unchecked": unchecked},
            )
        )

    # -- newest actually verifies -------------------------------------------
    #
    # Listing only proves a manifest parses; `_load_manifest` never touches the
    # artifact bytes, the detached checksum, or the archive TOC. A Glacier
    # transition or a partial sync that keeps the small JSON manifests leaves a
    # tidy list of entirely unrestorable backups. `verify_backup` needs no
    # database, so this fits inside health's no-DB guarantee.
    if recoverable:
        newest = recoverable[0]
        artifact_bytes = newest.bytes
        # 0 disables the cap entirely -- always checksum. Distinct from "unset",
        # which inherits the default; `get_backup_settings` resolves both.
        cap = int(settings.get("health_verify_max_bytes", HEALTH_VERIFY_MAX_BYTES))
        if cap and artifact_bytes > cap:
            # Presence and size are already covered by `hollow_backup`; what is
            # skipped here is the full byte-for-byte checksum, which belongs to
            # `backup verify`.
            checks.append(
                CheckResult(
                    id="health.newest_verifies",
                    status=STATUS_SKIP,
                    detail=(
                        f"{newest.backup_id} is {artifact_bytes} bytes, above the "
                        f"{cap}-byte health read limit -- run `tapdb backup "
                        "verify` to checksum it, or set "
                        "backup.health_verify_max_bytes: 0 to always read"
                    ),
                    data={
                        "backup_id": newest.backup_id,
                        "bytes": artifact_bytes,
                        "limit": cap,
                    },
                )
            )
            # Fall through rather than return: an early exit here skipped
            # `health.storage_safety` entirely, so the row set was incomplete
            # on any target whose newest recovery point exceeds the read limit
            # -- i.e. most real ones. The completeness guarantee has to hold on
            # every path, not just the cheap one.
            return checks + _health_storage_safety_checks(storage, listing)
        try:
            report = verify_backup(
                cfg,
                settings,
                backup_id=newest.backup_id,
                level=VERIFY_QUICK,
                record_receipt=False,  # health writes nothing, ever
            )
        except Exception as exc:  # noqa: BLE001
            checks.append(
                CheckResult(
                    id="health.newest_verifies",
                    status=STATUS_FAIL,
                    detail=f"the newest backup could not be verified: {exc}",
                    data={"backup_id": newest.backup_id},
                )
            )
        else:
            # A missing `pg_restore` is not a broken backup.
            #
            # Quick verification checks manifest and asset checksums -- which
            # need nothing but the bytes -- and then reads the archive's table
            # of contents, which shells out. `_toc_check` reports a missing
            # binary the same way it reports a corrupt archive, so health
            # decides by looking for the tool itself rather than parsing an
            # error string. Health is meant to run anywhere a monitoring job
            # runs, and a slim container without the PostgreSQL client tools
            # would otherwise report every backup in the fleet as unrestorable.
            import shutil as _shutil

            toolchain = _shutil.which("pg_restore") is not None
            failures = [c for c in report.checks if c.failed]
            attributable = [
                c for c in failures if toolchain or not c.id.startswith("archive.")
            ]

            if attributable:
                checks.append(
                    CheckResult(
                        id="health.newest_verifies",
                        status=STATUS_FAIL,
                        detail=(
                            f"newest backup {newest.backup_id} fails verification: "
                            + "; ".join(c.id for c in attributable)
                        ),
                        data={
                            "backup_id": newest.backup_id,
                            "failed": [c.id for c in attributable],
                        },
                    )
                )
            elif failures:
                checks.append(
                    CheckResult(
                        id="health.newest_verifies",
                        status=STATUS_WARN,
                        detail=(
                            f"checksums for {newest.backup_id} are intact, but the "
                            "archive could not be inspected because pg_restore is "
                            "not installed here"
                        ),
                        data={
                            "backup_id": newest.backup_id,
                            "unverified": [c.id for c in failures],
                        },
                    )
                )
            else:
                checks.append(
                    CheckResult(
                        id="health.newest_verifies",
                        status=STATUS_PASS,
                        detail=f"newest backup {newest.backup_id} verifies",
                        data={"backup_id": newest.backup_id},
                    )
                )
    else:
        checks.append(
            CheckResult(
                id="health.newest_verifies",
                status=STATUS_SKIP,
                detail="no recovery-point backup to verify",
            )
        )

    return checks + _health_storage_safety_checks(storage, listing)


def health_report(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    now: Optional[datetime] = None,
) -> HealthReport:
    """Answer "can this target be recovered?" without touching a database.

    The one invariant: **exit 0 means recoverable.** Every check exists because
    without it some state reported healthy while recovery was impossible --
    every backup failing but none ever succeeding, a store retargeted to an
    empty prefix, manifests present with their artifacts gone, a rehearsal
    failing nightly on schedule.

    Sources are consulted independently and a failure to reach one never
    suppresses a verdict already reached from another. Storage being
    unreachable and the receipt chain being tampered with is exit 1, not exit
    2: the tamper finding is real and must page.
    """
    checks: list[CheckResult] = []
    unavailable: list[str] = []
    create_receipts: Optional[int] = None

    # -- receipts (local filesystem; effectively always readable) ------------
    try:
        receipts_dir = receipts_directory(settings)
        from daylily_tapdb.backup.receipts import OPERATION_CREATE as _CREATE
        from daylily_tapdb.backup.receipts import read_receipts

        receipts = read_receipts(receipts_dir)
        create_receipts = sum(1 for r in receipts if r.operation == _CREATE)
        checks.extend(
            _health_receipt_checks(receipts_dir, receipts, settings=settings, now=now)
        )
    except Exception as exc:  # noqa: BLE001
        unavailable.append(SOURCE_RECEIPTS)
        # Every receipt-derived check gets a SKIP row, not just the chain.
        # Emitting one row where the success path emits seven leaves a consumer
        # keying on check ids with a KeyError, and makes "this check is absent"
        # indistinguishable from "this check was never implemented".
        checks.extend(
            CheckResult(
                id=check_id,
                status=STATUS_SKIP,
                detail=f"receipts could not be read: {exc}",
            )
            for check_id in RECEIPT_CHECK_IDS
        )

    # -- settings ------------------------------------------------------------
    #
    # Guarded like the others: a raising check must degrade to a skipped source,
    # never discard the verdicts already computed above it.
    try:
        checks.extend(_health_settings_checks(settings))
    except Exception as exc:  # noqa: BLE001
        unavailable.append(SOURCE_SETTINGS)
        checks.append(
            CheckResult(
                id="health.cadence_configured",
                status=STATUS_SKIP,
                detail=f"settings could not be evaluated: {exc}",
            )
        )

    # -- storage -------------------------------------------------------------
    #
    # Storage reachability is decided *first*, and the listing error is not
    # swallowed into an empty listing. An inaccessible bucket and an empty one
    # look identical from `entries == []`, so degrading to "no backups" here
    # would report exit 1 for an unreachable bucket -- or, worse, if the
    # degradation were quieter, exit 0.
    try:
        storage = storage_for(settings)
        listing = list_backups(cfg, settings)
    except Exception as exc:  # noqa: BLE001
        unavailable.append(SOURCE_STORAGE)
        checks.extend(
            CheckResult(
                id=check_id,
                status=STATUS_SKIP,
                detail=f"storage could not be read: {exc}",
            )
            for check_id in STORAGE_CHECK_IDS
        )
    else:
        try:
            checks.extend(
                _health_storage_checks(cfg, settings, storage, listing, create_receipts)
            )
        except Exception as exc:  # noqa: BLE001
            unavailable.append(SOURCE_STORAGE)
            checks.extend(
                CheckResult(
                    id=check_id,
                    status=STATUS_SKIP,
                    detail=f"storage checks failed: {exc}",
                )
                for check_id in STORAGE_CHECK_IDS
            )

    return HealthReport(
        target_label=target_label(cfg),
        checks=checks,
        unavailable=unavailable,
    )


__all__ = [
    "STATUS_FAIL",
    "STATUS_PASS",
    "STATUS_SKIP",
    "STATUS_WARN",
    "VERIFY_DEEP",
    "VERIFY_QUICK",
    "BackupListing",
    "BackupPlan",
    "BackupResult",
    "BackupSummary",
    "CheckResult",
    "HealthReport",
    "VerifyReport",
    "create_backup",
    "find_backup_prefix",
    "health_report",
    "list_backups",
    "new_backup_id",
    "open_session",
    "plan_backup",
    "receipts_directory",
    "storage_for",
    "target_label",
    "verify_backup",
]
