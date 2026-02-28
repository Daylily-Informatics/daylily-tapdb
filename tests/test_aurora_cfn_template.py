"""Tests for Aurora PostgreSQL CloudFormation template generator."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import pytest

# Load module directly to avoid importing the full daylily_tapdb package
# (which requires sqlalchemy and other deps not needed for this test).
_spec = importlib.util.spec_from_file_location(
    "cfn_template",
    str(
        Path(__file__).resolve().parent.parent
        / "daylily_tapdb"
        / "aurora"
        / "cfn_template.py"
    ),
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

generate_template = _mod.generate_template
save_template = _mod.save_template


@pytest.fixture
def template():
    """Generate a fresh template dict."""
    return generate_template()


# --- Structure ---


class TestTemplateStructure:
    def test_has_format_version(self, template):
        assert template["AWSTemplateFormatVersion"] == "2010-09-09"

    def test_has_description(self, template):
        assert "Aurora PostgreSQL" in template["Description"]

    def test_has_parameters(self, template):
        assert "Parameters" in template

    def test_has_resources(self, template):
        assert "Resources" in template

    def test_has_outputs(self, template):
        assert "Outputs" in template

    def test_has_conditions(self, template):
        assert "HasProject" in template["Conditions"]


# --- Parameters ---

REQUIRED_PARAMS = [
    "ClusterIdentifier",
    "InstanceClass",
    "EngineVersion",
    "VpcId",
    "SubnetIds",
    "MasterUsername",
    "DatabaseName",
    "IngressCIDR",
    "CostCenter",
    "Project",
]


class TestParameters:
    @pytest.mark.parametrize("param", REQUIRED_PARAMS)
    def test_required_parameter_exists(self, template, param):
        assert param in template["Parameters"]

    def test_instance_class_default(self, template):
        assert template["Parameters"]["InstanceClass"]["Default"] == "db.r6g.large"

    def test_engine_version_default(self, template):
        assert template["Parameters"]["EngineVersion"]["Default"] == "15.4"

    def test_vpc_id_type(self, template):
        assert template["Parameters"]["VpcId"]["Type"] == "AWS::EC2::VPC::Id"

    def test_subnet_ids_type(self, template):
        assert (
            template["Parameters"]["SubnetIds"]["Type"]
            == "List<AWS::EC2::Subnet::Id>"
        )


# --- Resources ---

REQUIRED_RESOURCES = [
    "DBSubnetGroup",
    "ClusterSecurityGroup",
    "MasterSecret",
    "AuroraCluster",
    "WriterInstance",
]


class TestResources:
    @pytest.mark.parametrize("resource", REQUIRED_RESOURCES)
    def test_required_resource_exists(self, template, resource):
        assert resource in template["Resources"]

    def test_subnet_group_type(self, template):
        r = template["Resources"]["DBSubnetGroup"]
        assert r["Type"] == "AWS::RDS::DBSubnetGroup"

    def test_subnet_group_uses_ref(self, template):
        """SubnetIds must use Ref (not Fn::ImportValue)."""
        props = template["Resources"]["DBSubnetGroup"]["Properties"]
        assert props["SubnetIds"] == {"Ref": "SubnetIds"}
        # Verify no ImportValue anywhere in subnet group
        import json

        raw = json.dumps(props)
        assert "ImportValue" not in raw

    def test_security_group_type(self, template):
        r = template["Resources"]["ClusterSecurityGroup"]
        assert r["Type"] == "AWS::EC2::SecurityGroup"

    def test_security_group_ingress_port(self, template):
        sg = template["Resources"]["ClusterSecurityGroup"]["Properties"]
        ingress = sg["SecurityGroupIngress"][0]
        assert ingress["FromPort"] == 5432
        assert ingress["ToPort"] == 5432

    def test_secret_type(self, template):
        r = template["Resources"]["MasterSecret"]
        assert r["Type"] == "AWS::SecretsManager::Secret"

    def test_cluster_engine(self, template):
        props = template["Resources"]["AuroraCluster"]["Properties"]
        assert props["Engine"] == "aurora-postgresql"

    def test_cluster_iam_auth_enabled(self, template):
        props = template["Resources"]["AuroraCluster"]["Properties"]
        assert props["EnableIAMDatabaseAuthentication"] is True

    def test_cluster_storage_encrypted(self, template):
        props = template["Resources"]["AuroraCluster"]["Properties"]
        assert props["StorageEncrypted"] is True

    def test_writer_instance_engine(self, template):
        props = template["Resources"]["WriterInstance"]["Properties"]
        assert props["Engine"] == "aurora-postgresql"

    def test_writer_not_publicly_accessible(self, template):
        props = template["Resources"]["WriterInstance"]["Properties"]
        assert props["PubliclyAccessible"] is False


# --- Tags ---


class TestTags:
    @pytest.mark.parametrize("resource", REQUIRED_RESOURCES)
    def test_resource_has_tags(self, template, resource):
        props = template["Resources"][resource]["Properties"]
        assert "Tags" in props
        tag_keys = [t["Key"] for t in props["Tags"]]
        assert "lsmc-cost-center" in tag_keys
        assert "lsmc-project" in tag_keys


# --- Outputs ---


class TestOutputs:
    def test_cluster_endpoint_output(self, template):
        assert "ClusterEndpoint" in template["Outputs"]

    def test_secret_arn_output(self, template):
        assert "SecretArn" in template["Outputs"]

    def test_security_group_id_output(self, template):
        assert "SecurityGroupId" in template["Outputs"]


# --- Serialization ---


class TestSerialization:
    def test_template_is_json_serializable(self, template):
        result = json.dumps(template, indent=2)
        assert len(result) > 100

    def test_save_template(self, tmp_path):
        out = tmp_path / "test-template.json"
        result = save_template(out)
        assert result == out
        assert out.exists()
        data = json.loads(out.read_text())
        assert "Resources" in data
        assert "AuroraCluster" in data["Resources"]
