"""Provider-snapshot receipts (backup class c), without touching AWS.

boto3 is never imported here. The guards, the receipt shape, and the secret
hygiene are all testable without a cloud account, and the RDS calls themselves
are exercised against a fake client.
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime

import pytest

from daylily_tapdb.backup import snapshots
from daylily_tapdb.backup.errors import BackupPolicyBlockedError
from daylily_tapdb.backup.manifest import assert_no_secrets

AURORA_CFG = {
    "engine_type": "aurora",
    "region": "us-west-2",
    "cluster_identifier": "tapdb-prod-cluster",
}
ENABLED = {
    "provider_snapshots_enabled": True,
    "provider_snapshots_cluster_identifier": "tapdb-prod-cluster",
}

AWS_SNAPSHOT = {
    "DBClusterSnapshotIdentifier": "tapdb-prod-20260728t120000z",
    "DBClusterSnapshotArn": "arn:aws:rds:us-west-2:1234:cluster-snapshot:tapdb",
    "DBClusterIdentifier": "tapdb-prod-cluster",
    "Status": "creating",
    "Engine": "aurora-postgresql",
    "EngineVersion": "16.4",
    "SnapshotType": "manual",
    "AllocatedStorage": 100,
    "StorageEncrypted": True,
    "KmsKeyId": "arn:aws:kms:us-west-2:1234:key/abc-123",
    "ClusterCreateTime": datetime(2026, 1, 1, tzinfo=UTC),
    "SnapshotCreateTime": datetime(2026, 7, 28, 12, tzinfo=UTC),
    "AvailabilityZones": ["us-west-2a", "us-west-2b"],
    # Fields that must not survive into the receipt.
    "MasterUsername": "tapdb_admin",
    "TagList": [{"Key": "secret", "Value": "do-not-record"}],
}


class FakeRdsClient:
    def __init__(self, snapshots_list=None):
        self._snapshots = snapshots_list if snapshots_list is not None else []
        self.calls: list[tuple] = []

    def create_db_cluster_snapshot(
        self, *, DBClusterSnapshotIdentifier, DBClusterIdentifier
    ):
        self.calls.append(("create", DBClusterSnapshotIdentifier, DBClusterIdentifier))
        return {"DBClusterSnapshot": dict(AWS_SNAPSHOT)}

    def describe_db_cluster_snapshots(self, *, DBClusterSnapshotIdentifier):
        self.calls.append(("describe", DBClusterSnapshotIdentifier))
        return {"DBClusterSnapshots": self._snapshots}


@pytest.fixture
def fake_rds(monkeypatch):
    client = FakeRdsClient([dict(AWS_SNAPSHOT)])
    monkeypatch.setattr(snapshots, "_rds_client", lambda region: client)
    return client


# ---------------------------------------------------------------------------
# guards
# ---------------------------------------------------------------------------


def test_disabled_by_default():
    assert snapshots.provider_snapshots_enabled(AURORA_CFG, {}) is False


def test_enabled_only_for_aurora():
    assert snapshots.provider_snapshots_enabled(AURORA_CFG, ENABLED) is True
    local = {**AURORA_CFG, "engine_type": "local"}
    assert snapshots.provider_snapshots_enabled(local, ENABLED) is False


def test_require_enabled_refuses_when_disabled():
    with pytest.raises(BackupPolicyBlockedError, match="disabled"):
        snapshots.require_enabled(AURORA_CFG, {"provider_snapshots_enabled": False})


def test_require_enabled_refuses_a_non_aurora_target():
    # A local Postgres has no provider to snapshot; failing loudly beats
    # producing a receipt describing nothing.
    local = {**AURORA_CFG, "engine_type": "local"}

    with pytest.raises(BackupPolicyBlockedError, match="Aurora"):
        snapshots.require_enabled(local, ENABLED)


def test_a_missing_cluster_identifier_is_refused(fake_rds):
    with pytest.raises(BackupPolicyBlockedError, match="cluster identifier"):
        snapshots.create_cluster_snapshot(
            {"engine_type": "aurora", "region": "us-west-2"},
            {"provider_snapshots_enabled": True},
        )


# ---------------------------------------------------------------------------
# receipt shape and hygiene
# ---------------------------------------------------------------------------


def test_receipt_records_the_auditable_facts():
    receipt = snapshots.build_snapshot_receipt(AWS_SNAPSHOT)

    assert receipt["snapshot_identifier"] == "tapdb-prod-20260728t120000z"
    assert receipt["cluster_identifier"] == "tapdb-prod-cluster"
    assert receipt["engine_version"] == "16.4"
    assert receipt["encrypted"] is True
    assert receipt["availability_zones"] == ["us-west-2a", "us-west-2b"]


def test_receipt_records_the_key_id_but_no_secret_material():
    receipt = snapshots.build_snapshot_receipt(AWS_SNAPSHOT)

    # The key id names the key; it is not the key.
    assert receipt["kms_key_id"].startswith("arn:aws:kms:")
    assert "MasterUsername" not in receipt
    assert "TagList" not in receipt


def test_receipt_drops_everything_not_explicitly_recorded():
    receipt = snapshots.build_snapshot_receipt(
        {**AWS_SNAPSHOT, "SomeFutureField": "unexpected"}
    )

    assert "SomeFutureField" not in receipt


def test_receipt_passes_the_manifest_secret_scanner():
    # Receipts land beside manifests in shared storage, so they must satisfy
    # the same rule: no secret-shaped keys, no credential-bearing URIs.
    assert_no_secrets(snapshots.build_snapshot_receipt(AWS_SNAPSHOT))


def test_datetimes_become_iso_strings():
    receipt = snapshots.build_snapshot_receipt(AWS_SNAPSHOT)

    assert receipt["snapshot_create_time"] == "2026-07-28T12:00:00+00:00"
    assert isinstance(receipt["cluster_create_time"], str)


def test_an_empty_description_yields_a_null_receipt():
    receipt = snapshots.build_snapshot_receipt({})

    assert receipt["snapshot_identifier"] is None
    assert receipt["encrypted"] is False


# ---------------------------------------------------------------------------
# RDS interaction
# ---------------------------------------------------------------------------


def test_create_mints_a_deterministic_identifier(fake_rds):
    receipt = snapshots.create_cluster_snapshot(
        AURORA_CFG, ENABLED, now=datetime(2026, 7, 28, 12, tzinfo=UTC)
    )

    kind, identifier, cluster = fake_rds.calls[0]
    assert kind == "create"
    assert identifier == "tapdb-tapdb-prod-cluster-20260728t120000z"
    assert cluster == "tapdb-prod-cluster"
    assert receipt["cluster_identifier"] == "tapdb-prod-cluster"


def test_create_accepts_an_explicit_identifier(fake_rds):
    snapshots.create_cluster_snapshot(
        AURORA_CFG, ENABLED, snapshot_identifier="my-own-name"
    )

    assert fake_rds.calls[0][1] == "my-own-name"


def test_describe_creates_nothing(fake_rds):
    receipt = snapshots.describe_cluster_snapshot(
        AURORA_CFG, ENABLED, snapshot_identifier="tapdb-prod-20260728t120000z"
    )

    # The --existing path records a snapshot TAPDB did not produce.
    assert [c[0] for c in fake_rds.calls] == ["describe"]
    assert receipt["snapshot_identifier"] == "tapdb-prod-20260728t120000z"


def test_describing_an_unknown_snapshot_is_refused(monkeypatch):
    monkeypatch.setattr(snapshots, "_rds_client", lambda region: FakeRdsClient([]))

    with pytest.raises(BackupPolicyBlockedError, match="No cluster snapshot"):
        snapshots.describe_cluster_snapshot(
            AURORA_CFG, ENABLED, snapshot_identifier="absent"
        )


def test_the_module_never_imports_boto3_on_its_own():
    sys.modules.pop("boto3", None)
    import importlib

    importlib.reload(snapshots)
    snapshots.provider_snapshots_enabled(AURORA_CFG, ENABLED)
    snapshots.build_snapshot_receipt(AWS_SNAPSHOT)

    # The CLI imports this package on every invocation; pulling in the AWS SDK
    # would tax every unrelated command.
    assert "boto3" not in sys.modules
