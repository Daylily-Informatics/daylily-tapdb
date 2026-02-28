"""Tests for Aurora CloudFormation stack management (T4).

All boto3 CloudFormation calls are mocked — no live AWS required.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from daylily_tapdb.aurora.config import AuroraConfig
from daylily_tapdb.aurora.stack_manager import (
    AuroraStackManager,
    _cfn_events_summary,
    _load_metadata,
    _save_metadata,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_cfn():
    """Return a MagicMock pretending to be a boto3 CloudFormation client."""
    return MagicMock()


@pytest.fixture()
def mock_ec2():
    """Return a MagicMock pretending to be a boto3 EC2 client."""
    ec2 = MagicMock()
    ec2.describe_vpcs.return_value = {
        "Vpcs": [{"VpcId": "vpc-default123", "IsDefault": True}]
    }
    ec2.describe_subnets.return_value = {
        "Subnets": [
            {"SubnetId": "subnet-aaa"},
            {"SubnetId": "subnet-bbb"},
        ]
    }
    return ec2


@pytest.fixture()
def manager(mock_cfn, mock_ec2):
    """Return an AuroraStackManager with mocked CFN and EC2 clients."""
    return AuroraStackManager(
        cfn_client=mock_cfn, ec2_client=mock_ec2, region="us-west-2"
    )


@pytest.fixture()
def sample_config():
    """Return a minimal AuroraConfig for testing."""
    return AuroraConfig(
        region="us-west-2",
        cluster_identifier="test-cluster",
        vpc_id="vpc-abc123",
    )


@pytest.fixture()
def metadata_path(tmp_path, monkeypatch):
    """Redirect stack metadata to a temp directory."""
    meta_file = tmp_path / "aurora-stacks.json"
    monkeypatch.setattr(
        "daylily_tapdb.aurora.stack_manager.STACK_METADATA_PATH", meta_file
    )
    return meta_file


# ---------------------------------------------------------------------------
# Constructor
# ---------------------------------------------------------------------------


class TestConstructor:
    def test_accepts_injected_client(self, mock_cfn, mock_ec2):
        mgr = AuroraStackManager(cfn_client=mock_cfn, ec2_client=mock_ec2)
        assert mgr._cfn is mock_cfn
        assert mgr._ec2 is mock_ec2

    def test_missing_boto3_raises_import_error(self):
        with patch.dict("sys.modules", {"boto3": None}):
            with pytest.raises(ImportError, match="boto3 is required"):
                AuroraStackManager()


# ---------------------------------------------------------------------------
# get_stack_status
# ---------------------------------------------------------------------------


class TestGetStackStatus:
    def test_returns_status_and_outputs(self, manager, mock_cfn):
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [
                {
                    "StackStatus": "CREATE_COMPLETE",
                    "Outputs": [
                        {
                            "OutputKey": "ClusterEndpoint",
                            "OutputValue": "my.ep.rds.amazonaws.com",
                        },
                        {
                            "OutputKey": "ClusterPort",
                            "OutputValue": "5432",
                        },
                    ],
                }
            ]
        }
        result = manager.get_stack_status("tapdb-test")
        assert result["status"] == "CREATE_COMPLETE"
        assert result["outputs"]["ClusterEndpoint"] == "my.ep.rds.amazonaws.com"
        assert result["outputs"]["ClusterPort"] == "5432"

    def test_raises_on_missing_stack(self, manager, mock_cfn):
        mock_cfn.describe_stacks.side_effect = Exception("Stack not found")
        with pytest.raises(RuntimeError, match="not found"):
            manager.get_stack_status("nonexistent")

    def test_raises_on_empty_stacks_list(self, manager, mock_cfn):
        mock_cfn.describe_stacks.return_value = {"Stacks": []}
        with pytest.raises(RuntimeError, match="not found"):
            manager.get_stack_status("tapdb-empty")


# ---------------------------------------------------------------------------
# wait_for_stack
# ---------------------------------------------------------------------------


class TestWaitForStack:
    def test_returns_immediately_on_target_status(self, manager, mock_cfn):
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [{"StackStatus": "CREATE_COMPLETE", "Outputs": []}]
        }
        result = manager.wait_for_stack("tapdb-test", "CREATE_COMPLETE", timeout=5)
        assert result["status"] == "CREATE_COMPLETE"

    def test_returns_on_terminal_failure(self, manager, mock_cfn):
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [{"StackStatus": "CREATE_FAILED", "Outputs": []}]
        }
        result = manager.wait_for_stack("tapdb-test", "CREATE_COMPLETE", timeout=5)
        assert result["status"] == "CREATE_FAILED"

    def test_delete_complete_on_missing_stack(self, manager, mock_cfn):
        mock_cfn.describe_stacks.side_effect = Exception("does not exist")
        result = manager.wait_for_stack("tapdb-test", "DELETE_COMPLETE", timeout=5)
        assert result["status"] == "DELETE_COMPLETE"

    @patch("daylily_tapdb.aurora.stack_manager.time.sleep")
    def test_timeout(self, mock_sleep, manager, mock_cfn):
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [{"StackStatus": "CREATE_IN_PROGRESS", "Outputs": []}]
        }
        manager.wait_for_stack("tapdb-test", "CREATE_COMPLETE", timeout=0)


# ------------------------------------------------------------------
# create_stack
# ------------------------------------------------------------------

_FAKE_STACK_ID = "arn:aws:cloudformation:us-west-2:123:stack/tapdb-test-cluster/abc"
_FAKE_SHORT_ID = "arn:aws:cf:us-west-2:123:stack/x/y"


class TestCreateStack:
    @patch("daylily_tapdb.aurora.stack_manager.time.sleep")
    def test_create_stack_success(
        self,
        mock_sleep,
        manager,
        mock_cfn,
        sample_config,
        metadata_path,
    ):
        mock_cfn.create_stack.return_value = {
            "StackId": _FAKE_STACK_ID,
        }
        ep = "ep.rds.amazonaws.com"
        secret = "arn:aws:sm:us-west-2:123:secret:pw"
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [
                {
                    "StackStatus": "CREATE_COMPLETE",
                    "Outputs": [
                        {
                            "OutputKey": "ClusterEndpoint",
                            "OutputValue": ep,
                        },
                        {
                            "OutputKey": "SecretArn",
                            "OutputValue": secret,
                        },
                    ],
                }
            ]
        }

        result = manager.create_stack(sample_config)

        assert result["stack_name"] == "tapdb-test-cluster"
        assert "arn:" in result["stack_id"]
        assert result["outputs"]["ClusterEndpoint"] == ep

        # Verify metadata was saved
        assert metadata_path.exists()
        meta = json.loads(metadata_path.read_text())
        assert "tapdb-test-cluster" in meta
        assert meta["tapdb-test-cluster"]["status"] == "CREATE_COMPLETE"

    @patch("daylily_tapdb.aurora.stack_manager.time.sleep")
    def test_create_stack_failure_raises(
        self,
        mock_sleep,
        manager,
        mock_cfn,
        sample_config,
        metadata_path,
    ):
        mock_cfn.create_stack.return_value = {
            "StackId": _FAKE_SHORT_ID,
        }
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [{"StackStatus": "CREATE_FAILED", "Outputs": []}]
        }
        mock_cfn.describe_stack_events.return_value = {
            "StackEvents": [],
        }

        with pytest.raises(RuntimeError, match="CREATE_FAILED"):
            manager.create_stack(sample_config)

    def test_create_stack_passes_correct_params(
        self,
        manager,
        mock_cfn,
        sample_config,
        metadata_path,
    ):
        mock_cfn.create_stack.return_value = {
            "StackId": _FAKE_SHORT_ID,
        }
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [{"StackStatus": "CREATE_COMPLETE", "Outputs": []}]
        }

        manager.create_stack(sample_config)

        call_kwargs = mock_cfn.create_stack.call_args[1]
        assert call_kwargs["StackName"] == "tapdb-test-cluster"
        assert call_kwargs["Capabilities"] == ["CAPABILITY_IAM"]
        param_keys = {p["ParameterKey"] for p in call_kwargs["Parameters"]}
        assert "ClusterIdentifier" in param_keys
        assert "VpcId" in param_keys
        assert "SubnetIds" in param_keys

        # SubnetIds should be comma-separated
        subnet_param = next(
            p for p in call_kwargs["Parameters"] if p["ParameterKey"] == "SubnetIds"
        )
        assert "subnet-aaa" in subnet_param["ParameterValue"]
        assert "subnet-bbb" in subnet_param["ParameterValue"]


# ------------------------------------------------------------------
# VPC / subnet auto-discovery
# ------------------------------------------------------------------


class TestResolveVpcAndSubnets:
    def test_uses_config_vpc_id(self, manager, mock_ec2):
        """When config has vpc_id, use it (don't discover)."""
        config = AuroraConfig(vpc_id="vpc-explicit", cluster_identifier="x")
        vpc_id, subnets = manager._resolve_vpc_and_subnets(config)
        assert vpc_id == "vpc-explicit"
        mock_ec2.describe_vpcs.assert_not_called()

    def test_auto_discovers_default_vpc(self, manager, mock_ec2):
        """When config.vpc_id is empty, discover default VPC."""
        config = AuroraConfig(vpc_id="", cluster_identifier="x")
        vpc_id, subnets = manager._resolve_vpc_and_subnets(config)
        assert vpc_id == "vpc-default123"
        assert len(subnets) == 2
        mock_ec2.describe_vpcs.assert_called_once()

    def test_no_default_vpc_raises(self, manager, mock_ec2):
        """No default VPC and no --vpc-id → RuntimeError."""
        mock_ec2.describe_vpcs.return_value = {"Vpcs": []}
        config = AuroraConfig(vpc_id="", cluster_identifier="x")
        with pytest.raises(RuntimeError, match="No default VPC"):
            manager._resolve_vpc_and_subnets(config)

    def test_no_subnets_raises(self, manager, mock_ec2):
        """VPC exists but has no subnets → RuntimeError."""
        mock_ec2.describe_subnets.return_value = {"Subnets": []}
        config = AuroraConfig(vpc_id="vpc-empty", cluster_identifier="x")
        with pytest.raises(RuntimeError, match="No subnets found"):
            manager._resolve_vpc_and_subnets(config)


# ------------------------------------------------------------------
# initiate_create_stack
# ------------------------------------------------------------------


class TestInitiateCreateStack:
    def test_returns_without_waiting(self, manager, mock_cfn, mock_ec2):
        mock_cfn.create_stack.return_value = {"StackId": _FAKE_STACK_ID}
        config = AuroraConfig(cluster_identifier="bg-test")
        result = manager.initiate_create_stack(config)
        assert result["stack_name"] == "tapdb-bg-test"
        assert "arn:" in result["stack_id"]
        assert result["vpc_id"] == "vpc-default123"
        # Should NOT have called describe_stacks (no waiting)
        mock_cfn.describe_stacks.assert_not_called()


# ------------------------------------------------------------------
# delete_stack
# ------------------------------------------------------------------


class TestDeleteStack:
    @patch("daylily_tapdb.aurora.stack_manager.time.sleep")
    def test_delete_with_retain_networking(
        self, mock_sleep, manager, mock_cfn, metadata_path
    ):
        meta = {"tapdb-test": {"status": "CREATE_COMPLETE"}}
        metadata_path.write_text(json.dumps(meta))

        mock_cfn.describe_stacks.side_effect = Exception("does not exist")

        result = manager.delete_stack("tapdb-test", retain_networking=True)

        assert result["status"] == "DELETE_COMPLETE"
        call_kwargs = mock_cfn.delete_stack.call_args[1]
        assert "RetainResources" in call_kwargs
        assert "ClusterSecurityGroup" in call_kwargs["RetainResources"]

        meta = json.loads(metadata_path.read_text())
        assert meta["tapdb-test"]["status"] == "DELETE_COMPLETE"

    @patch("daylily_tapdb.aurora.stack_manager.time.sleep")
    def test_delete_without_retain(self, mock_sleep, manager, mock_cfn, metadata_path):
        mock_cfn.describe_stacks.side_effect = Exception("does not exist")

        result = manager.delete_stack("tapdb-test", retain_networking=False)

        assert result["status"] == "DELETE_COMPLETE"
        call_kwargs = mock_cfn.delete_stack.call_args[1]
        assert "RetainResources" not in call_kwargs


# ---------------------------------------------------------------------------
# detect_existing_resources
# ---------------------------------------------------------------------------


class TestDetectExistingResources:
    def test_finds_tapdb_stacks(self, manager, mock_cfn):
        paginator = MagicMock()
        mock_cfn.get_paginator.return_value = paginator
        paginator.paginate.return_value = [
            {
                "StackSummaries": [
                    {"StackName": "tapdb-dev"},
                    {"StackName": "other-stack"},
                ]
            }
        ]
        mock_cfn.describe_stacks.return_value = {
            "Stacks": [
                {
                    "StackStatus": "CREATE_COMPLETE",
                    "Outputs": [],
                    "Tags": [
                        {"Key": "lsmc-project", "Value": "tapdb-us-west-2"},
                    ],
                }
            ]
        }

        result = manager.detect_existing_resources()

        assert "tapdb-dev" in result
        assert "other-stack" not in result
        assert result["tapdb-dev"]["status"] == "CREATE_COMPLETE"

    def test_empty_when_no_stacks(self, manager, mock_cfn):
        paginator = MagicMock()
        mock_cfn.get_paginator.return_value = paginator
        paginator.paginate.return_value = [{"StackSummaries": []}]

        result = manager.detect_existing_resources()
        assert result == {}


# ---------------------------------------------------------------------------
# Metadata helpers
# ---------------------------------------------------------------------------


class TestMetadataHelpers:
    def test_load_missing_file(self, metadata_path):
        assert _load_metadata() == {}

    def test_save_and_load(self, metadata_path):
        data = {"tapdb-test": {"status": "CREATE_COMPLETE", "region": "us-west-2"}}
        _save_metadata(data)
        loaded = _load_metadata()
        assert loaded == data

    def test_creates_parent_dirs(self, tmp_path, monkeypatch):
        deep = tmp_path / "a" / "b" / "c" / "stacks.json"
        monkeypatch.setattr(
            "daylily_tapdb.aurora.stack_manager.STACK_METADATA_PATH", deep
        )
        _save_metadata({"test": True})
        assert deep.exists()


# ---------------------------------------------------------------------------
# CFN events summary
# ---------------------------------------------------------------------------


class TestCfnEventsSummary:
    def test_formats_events(self):
        client = MagicMock()
        client.describe_stack_events.return_value = {
            "StackEvents": [
                {
                    "Timestamp": "2026-01-01T00:00:00Z",
                    "LogicalResourceId": "AuroraCluster",
                    "ResourceStatus": "CREATE_FAILED",
                    "ResourceStatusReason": "Limit exceeded",
                },
            ]
        }
        summary = _cfn_events_summary(client, "tapdb-test")
        assert "AuroraCluster" in summary
        assert "CREATE_FAILED" in summary
        assert "Limit exceeded" in summary

    def test_handles_exception(self):
        client = MagicMock()
        client.describe_stack_events.side_effect = Exception("boom")
        summary = _cfn_events_summary(client, "tapdb-test")
        assert "unable to retrieve" in summary
