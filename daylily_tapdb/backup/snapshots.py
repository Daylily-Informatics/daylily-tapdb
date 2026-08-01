"""Backup class (c): provider-snapshot receipts.

An Aurora cluster snapshot is taken and owned by AWS, not by TAPDB. What TAPDB
can usefully do is *record* one: which snapshot, of which cluster, at what
engine version, encrypted under which key. That receipt is what makes a
provider snapshot auditable alongside the logical backups, and what lets an
operator choose between "restore the schema" and "cut over to a cluster
snapshot" from the same inventory.

Deliberately narrow. There is no scheduling, no retention enforcement, and no
standing infrastructure here -- those belong to the dayhoff CDK companion
issue. This module creates or describes a snapshot and writes down what it saw.

Two guards keep it inert unless explicitly turned on:

* ``backup.provider_snapshots.enabled`` must be true, and the target must be an
  Aurora engine. A local Postgres has no provider to snapshot.
* boto3 is imported lazily, so the CLI never pays for AWS libraries -- or
  requires them to be installed -- on any other code path.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Optional

from daylily_tapdb.backup.engine import ENGINE_AURORA
from daylily_tapdb.backup.errors import BackupPolicyBlockedError

SNAPSHOT_RECEIPT_ARTIFACT = "snapshot-receipt.json"


def provider_snapshots_enabled(cfg: dict[str, Any], settings: dict[str, Any]) -> bool:
    """Return whether provider snapshots are configured for this target."""
    return bool(
        settings.get("provider_snapshots_enabled")
        and str(cfg.get("engine_type") or "").strip().lower() == ENGINE_AURORA
    )


def require_enabled(cfg: dict[str, Any], settings: dict[str, Any]) -> None:
    """Raise unless provider snapshots are available for this target."""
    if (
        settings.get("provider_snapshots_enabled")
        and str(cfg.get("engine_type") or "").strip().lower() != ENGINE_AURORA
    ):
        raise BackupPolicyBlockedError(
            "Provider snapshots require an Aurora target; this target is "
            f"engine_type={cfg.get('engine_type')!r}.",
            detail={"engine_type": cfg.get("engine_type")},
        )
    if not settings.get("provider_snapshots_enabled"):
        raise BackupPolicyBlockedError(
            "Provider snapshots are disabled. Set "
            "backup.provider_snapshots.enabled to true to use them.",
            detail={"setting": "backup.provider_snapshots.enabled"},
        )


def _rds_client(region: str) -> Any:
    """Build an RDS client, importing boto3 only when actually needed."""
    try:
        import boto3  # noqa: PLC0415 - deliberate lazy import
    except ImportError as exc:  # pragma: no cover - environment dependent
        raise RuntimeError(
            "Provider snapshots require boto3. Install it, or set "
            "backup.provider_snapshots.enabled to false."
        ) from exc
    return boto3.client("rds", region_name=region)


def _cluster_identifier(cfg: dict[str, Any], settings: dict[str, Any]) -> str:
    identifier = str(
        settings.get("provider_snapshots_cluster_identifier")
        or cfg.get("cluster_identifier")
        or ""
    ).strip()
    if not identifier:
        raise BackupPolicyBlockedError(
            "No cluster identifier configured. Set "
            "backup.provider_snapshots.cluster_identifier.",
            detail={"setting": "backup.provider_snapshots.cluster_identifier"},
        )
    return identifier


def _jsonable(value: Any) -> Any:
    """Coerce an AWS response value into something JSON can hold."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat()
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return str(value)


def build_snapshot_receipt(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Reduce an RDS snapshot description to an auditable receipt.

    Only identifiers, versions, timestamps, and the encryption *key id* are
    kept. Nothing here may carry a credential -- these receipts sit next to
    manifests in shared storage, and the manifest's own secret scanner would
    reject a payload containing one.
    """
    return {
        "snapshot_identifier": snapshot.get("DBClusterSnapshotIdentifier"),
        "snapshot_arn": snapshot.get("DBClusterSnapshotArn"),
        "cluster_identifier": snapshot.get("DBClusterIdentifier"),
        "status": snapshot.get("Status"),
        "engine": snapshot.get("Engine"),
        "engine_version": snapshot.get("EngineVersion"),
        "snapshot_type": snapshot.get("SnapshotType"),
        "allocated_storage_gb": snapshot.get("AllocatedStorage"),
        "encrypted": bool(snapshot.get("StorageEncrypted")),
        # Key *id* only: it names the key, it is not the key.
        "kms_key_id": snapshot.get("KmsKeyId"),
        "cluster_create_time": _jsonable(snapshot.get("ClusterCreateTime")),
        "snapshot_create_time": _jsonable(snapshot.get("SnapshotCreateTime")),
        "availability_zones": _jsonable(snapshot.get("AvailabilityZones")),
    }


def create_cluster_snapshot(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    snapshot_identifier: Optional[str] = None,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """Create a new Aurora cluster snapshot and return its receipt."""
    require_enabled(cfg, settings)
    cluster = _cluster_identifier(cfg, settings)
    moment = (now or datetime.now(UTC)).astimezone(UTC)
    identifier = snapshot_identifier or (
        f"tapdb-{cluster}-{moment.strftime('%Y%m%dt%H%M%SZ')}".lower()
    )

    client = _rds_client(str(cfg.get("region") or "us-west-2"))
    response = client.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier=identifier,
        DBClusterIdentifier=cluster,
    )
    return build_snapshot_receipt(response.get("DBClusterSnapshot") or {})


def describe_cluster_snapshot(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    snapshot_identifier: str,
) -> dict[str, Any]:
    """Describe an existing snapshot without creating anything.

    This is the ``--existing`` path: an operator who already has a snapshot
    (taken by a schedule, or by hand during an incident) can record it in the
    TAPDB inventory without TAPDB having produced it.
    """
    require_enabled(cfg, settings)
    client = _rds_client(str(cfg.get("region") or "us-west-2"))
    response = client.describe_db_cluster_snapshots(
        DBClusterSnapshotIdentifier=snapshot_identifier
    )
    snapshots = response.get("DBClusterSnapshots") or []
    if not snapshots:
        raise BackupPolicyBlockedError(
            f"No cluster snapshot named {snapshot_identifier!r} was found.",
            detail={"snapshot_identifier": snapshot_identifier},
        )
    return build_snapshot_receipt(snapshots[0])


__all__ = [
    "SNAPSHOT_RECEIPT_ARTIFACT",
    "build_snapshot_receipt",
    "create_cluster_snapshot",
    "describe_cluster_snapshot",
    "provider_snapshots_enabled",
    "require_enabled",
]
