"""The state inventory: what a TAPDB backup captures, references, and omits.

This is issue #89 item 2, and it is deliberately *derived* rather than
enumerated. No table, function, trigger, or sequence is named here:

* what gets captured is decided by ``pg_dump --schema``, so any object added to
  the schema later is included the day it is created;
* what is *expected* comes from ``schema_inventory``, which parses
  ``tapdb_schema.sql`` plus ``migrations/*.sql`` -- the same files a new object
  is added to.

What this module does hold is the part that genuinely cannot be introspected:
the **rationale** for each category, especially the exclusions. A restore
operator needs to know that roles and grants are not in the artifact, and why.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Sequence

from daylily_tapdb.backup.manifest import sha256_file
from daylily_tapdb.schema_inventory import TapdbSchemaInventory

#: Restorable from the backup artifact itself.
DISPOSITION_CAPTURED = "captured"
#: Recorded in the manifest so a restore can be validated, but not restorable
#: from it -- these live outside the database.
DISPOSITION_REFERENCED = "referenced"
#: Deliberately absent from the artifact.
DISPOSITION_EXCLUDED = "excluded"


@dataclass(frozen=True)
class StateCategory:
    """One class of state, and this system's stance on it."""

    key: str
    title: str
    disposition: str
    detail: str
    rationale: str = ""

    def to_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "key": self.key,
            "title": self.title,
            "disposition": self.disposition,
            "detail": self.detail,
        }
        if self.rationale:
            payload["rationale"] = self.rationale
        return payload


STATE_INVENTORY: tuple[StateCategory, ...] = (
    StateCategory(
        key="tables",
        title="Tables and row data",
        disposition=DISPOSITION_CAPTURED,
        detail=(
            "Every table in the target schema, enumerated live at backup time "
            "rather than from a fixed list."
        ),
    ),
    StateCategory(
        key="sequences",
        title="Sequences (library, per-prefix, and IDENTITY)",
        disposition=DISPOSITION_CAPTURED,
        detail=(
            "All three classes: static library sequences, dynamic "
            "{prefix}_instance_seq sequences, and the IDENTITY sequences "
            "behind every uid/id primary key."
        ),
        rationale=(
            "Library sequence values are not reconstructable from table data, "
            "and resetting any EUID sequence would reissue identifiers that "
            "have already been handed out."
        ),
    ),
    StateCategory(
        key="functions",
        title="Functions",
        disposition=DISPOSITION_CAPTURED,
        detail="All functions owned by the target schema.",
        rationale=(
            "Triggers fire on insert, so the functions they call must exist "
            "before any data is restored."
        ),
    ),
    StateCategory(
        key="triggers",
        title="Triggers",
        disposition=DISPOSITION_CAPTURED,
        detail="EUID generation, audit writing, and soft-delete triggers.",
    ),
    StateCategory(
        key="indexes_constraints",
        title="Indexes and constraints",
        disposition=DISPOSITION_CAPTURED,
        detail="Primary keys, unique constraints, and indexes in the schema.",
    ),
    StateCategory(
        key="rls_policies",
        title="Row-level security policies",
        disposition=DISPOSITION_CAPTURED,
        detail=(
            "Captured when present. Policies reference roles, which are not "
            "in the artifact -- restore preflight checks that the roles named "
            "by any POLICY entry exist on the target."
        ),
    ),
    StateCategory(
        key="migration_history",
        title="Applied migration history",
        disposition=DISPOSITION_CAPTURED,
        detail=(
            "The _tapdb_migrations rows, captured as an ordinary table in the "
            "schema and also mirrored into the manifest for preflight "
            "comparison."
        ),
    ),
    StateCategory(
        key="governance_registries",
        title="Prefix-ownership and domain registries",
        disposition=DISPOSITION_REFERENCED,
        detail=(
            "Paths and SHA-256 checksums recorded in the manifest so a "
            "restore can prove the governance context still matches."
        ),
        rationale=(
            "These files live in the repository, not the database; a backup "
            "cannot restore them, but it can detect that they changed."
        ),
    ),
    StateCategory(
        key="target_identity",
        title="Target identity (client, database, schema, domain, owner)",
        disposition=DISPOSITION_REFERENCED,
        detail=(
            "Recorded so a restore into a different domain or owner repo "
            "fails loudly instead of silently cross-contaminating targets."
        ),
    ),
    StateCategory(
        key="schema_assets",
        title="Schema source assets",
        disposition=DISPOSITION_REFERENCED,
        detail=(
            "Checksums of tapdb_schema.sql and each migration file, so the "
            "tool can tell whether it understands the schema it is restoring."
        ),
    ),
    StateCategory(
        key="roles_grants",
        title="Roles, grants, and object ownership",
        disposition=DISPOSITION_EXCLUDED,
        detail="Dumped with --no-owner --no-acl.",
        rationale=(
            "Roles are cluster-scoped, not schema-scoped. Carrying them would "
            "make a backup unrestorable onto any cluster with a different "
            "role set, which is exactly the isolated-target case restores "
            "depend on."
        ),
    ),
    StateCategory(
        key="extensions",
        title="PostgreSQL extensions",
        disposition=DISPOSITION_EXCLUDED,
        detail="TAPDB's schema requires no extensions.",
        rationale=(
            "Nothing to capture. If a future migration adds one, it becomes a "
            "restore-target prerequisite and belongs in preflight."
        ),
    ),
    StateCategory(
        key="other_schemas",
        title="Other schemas in the same database",
        disposition=DISPOSITION_EXCLUDED,
        detail="Capture is scoped to the configured schema_name.",
        rationale=(
            "Schema scoping is what makes a backup provably about one tenant "
            "of a shared database. The dump's table of contents records which "
            "schema was captured."
        ),
    ),
    StateCategory(
        key="rows_outside_rls_scope",
        title="Rows outside the runtime forced-RLS scope",
        disposition=DISPOSITION_EXCLUDED,
        detail=(
            "A database template-pack contains global templates plus the configured "
            "tenant's templates, or global templates only when tenant_id is empty."
        ),
        rationale=(
            "The runtime role is intentionally NOBYPASSRLS. Hidden tenant templates "
            "cannot be claimed as captured, so the signed manifest records the exact "
            "scope. Full backups use the distinct operator role and do not include "
            "this exclusion."
        ),
    ),
    StateCategory(
        key="cluster_settings",
        title="Cluster settings and parameter groups",
        disposition=DISPOSITION_EXCLUDED,
        detail="Server configuration is infrastructure, not application state.",
        rationale=(
            "TAPDB consumes the cluster; it does not manage it. Provisioning "
            "belongs to the infrastructure repo."
        ),
    ),
    StateCategory(
        key="config_file",
        title="The TAPDB config file",
        disposition=DISPOSITION_EXCLUDED,
        detail="~/.config/tapdb/<client>/<db>/tapdb-config.yaml is never copied.",
        rationale=(
            "It carries credentials. Backups are copied to shared storage; "
            "credentials must not travel with them."
        ),
    ),
    StateCategory(
        key="identity_provider_state",
        title="Cognito / identity-provider state",
        disposition=DISPOSITION_EXCLUDED,
        detail="User pools, app clients, and federated identities.",
        rationale=(
            "Owned by the identity provider. Restoring TAPDB rows does not "
            "and should not recreate authentication state."
        ),
    ),
)


def categories(disposition: Optional[str] = None) -> list[StateCategory]:
    """Return state categories, optionally filtered by disposition."""
    if disposition is None:
        return list(STATE_INVENTORY)
    return [item for item in STATE_INVENTORY if item.disposition == disposition]


def excluded_state_payload() -> list[dict[str, Any]]:
    """Return the manifest's ``excluded_state`` block."""
    return [item.to_payload() for item in categories(DISPOSITION_EXCLUDED)]


def state_inventory_payload() -> list[dict[str, Any]]:
    """Return the full state inventory for docs and the plan command."""
    return [item.to_payload() for item in STATE_INVENTORY]


def summarize_inventory(inventory: TapdbSchemaInventory) -> dict[str, Any]:
    """Summarize a schema inventory into manifest-friendly counts and names."""
    return {
        "schema_name": inventory.schema_name,
        "counts": inventory.counts(),
        "tables": sorted(inventory.tables),
        "sequences": sorted(inventory.sequences),
        "functions": sorted(inventory.functions),
        "triggers": {
            table: sorted(names)
            for table, names in sorted(inventory.triggers.items())
            if names
        },
    }


def schema_asset_checksums(asset_paths: Sequence[Path]) -> list[dict[str, Any]]:
    """Return name/sha256 pairs for the schema source assets.

    Missing files are recorded with a null checksum rather than raising: a
    manifest that says "this migration was absent" is more useful during
    incident response than a backup that refuses to run.
    """
    entries: list[dict[str, Any]] = []
    for path in asset_paths:
        resolved = Path(path)
        entries.append(
            {
                "name": resolved.name,
                "sha256": sha256_file(resolved) if resolved.is_file() else None,
            }
        )
    return entries


__all__ = [
    "DISPOSITION_CAPTURED",
    "DISPOSITION_EXCLUDED",
    "DISPOSITION_REFERENCED",
    "STATE_INVENTORY",
    "StateCategory",
    "categories",
    "excluded_state_payload",
    "schema_asset_checksums",
    "state_inventory_payload",
    "summarize_inventory",
]
