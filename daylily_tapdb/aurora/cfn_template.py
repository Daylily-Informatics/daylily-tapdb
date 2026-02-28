"""CloudFormation template generator for Aurora PostgreSQL clusters."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

# Defaults
DEFAULT_INSTANCE_CLASS = "db.r6g.large"
DEFAULT_ENGINE_VERSION = "16.6"
DEFAULT_COST_CENTER = "global"
DEFAULT_INGRESS_CIDR = "10.0.0.0/8"


def _build_tags(
    cost_center: str = DEFAULT_COST_CENTER,
    project: str | None = None,
    region: str = "us-west-2",
) -> list[dict[str, str]]:
    """Build standard LSMC cost tags."""
    if project is None:
        project = f"tapdb-{region}"
    return [
        {"Key": "lsmc-cost-center", "Value": cost_center},
        {"Key": "lsmc-project", "Value": project},
    ]


def _parameters() -> dict[str, Any]:
    """CloudFormation Parameters section."""
    return {
        "ClusterIdentifier": {
            "Type": "String",
            "Description": "Identifier for the Aurora PostgreSQL cluster",
        },
        "InstanceClass": {
            "Type": "String",
            "Default": DEFAULT_INSTANCE_CLASS,
            "Description": "DB instance class for the writer instance",
        },
        "EngineVersion": {
            "Type": "String",
            "Default": DEFAULT_ENGINE_VERSION,
            "Description": "Aurora PostgreSQL engine version",
        },
        "VpcId": {
            "Type": "AWS::EC2::VPC::Id",
            "Description": "VPC ID for the Aurora cluster",
        },
        "MasterUsername": {
            "Type": "String",
            "Default": "tapdb_admin",
            "Description": "Master username for the Aurora cluster",
        },
        "DatabaseName": {
            "Type": "String",
            "Default": "tapdb",
            "Description": "Name of the default database",
        },
        "SubnetIds": {
            "Type": "List<AWS::EC2::Subnet::Id>",
            "Description": "Subnet IDs for the DB subnet group",
        },
        "IngressCIDR": {
            "Type": "String",
            "Default": DEFAULT_INGRESS_CIDR,
            "Description": "CIDR block allowed to connect on port 5432",
        },
        "PubliclyAccessible": {
            "Type": "String",
            "Default": "false",
            "AllowedValues": ["true", "false"],
            "Description": "Whether the DB instance is publicly accessible",
        },
        "DeletionProtection": {
            "Type": "String",
            "Default": "true",
            "AllowedValues": ["true", "false"],
            "Description": "Whether deletion protection is enabled on the cluster",
        },
        "CostCenter": {
            "Type": "String",
            "Default": DEFAULT_COST_CENTER,
            "Description": "Value for lsmc-cost-center tag",
        },
        "Project": {
            "Type": "String",
            "Default": "",
            "Description": (
                "Value for lsmc-project tag. If empty, defaults to tapdb-{region}."
            ),
        },
    }


def _resources() -> dict[str, Any]:
    """CloudFormation Resources section."""
    tags_with_refs = [
        {"Key": "lsmc-cost-center", "Value": {"Ref": "CostCenter"}},
        {
            "Key": "lsmc-project",
            "Value": {
                "Fn::If": [
                    "HasProject",
                    {"Ref": "Project"},
                    {"Fn::Sub": "tapdb-${AWS::Region}"},
                ]
            },
        },
    ]

    return {
        "DBSubnetGroup": {
            "Type": "AWS::RDS::DBSubnetGroup",
            "Properties": {
                "DBSubnetGroupDescription": {
                    "Fn::Sub": "Subnet group for ${ClusterIdentifier}"
                },
                "SubnetIds": {"Ref": "SubnetIds"},
                "Tags": tags_with_refs,
            },
        },
        "ClusterSecurityGroup": {
            "Type": "AWS::EC2::SecurityGroup",
            "Properties": {
                "GroupDescription": {
                    "Fn::Sub": "Security group for Aurora cluster ${ClusterIdentifier}"
                },
                "VpcId": {"Ref": "VpcId"},
                "SecurityGroupIngress": [
                    {
                        "IpProtocol": "tcp",
                        "FromPort": 5432,
                        "ToPort": 5432,
                        "CidrIp": {"Ref": "IngressCIDR"},
                    }
                ],
                "Tags": tags_with_refs,
            },
        },
        "MasterSecret": {
            "Type": "AWS::SecretsManager::Secret",
            "Properties": {
                "Name": {"Fn::Sub": "${ClusterIdentifier}-master-password"},
                "Description": {
                    "Fn::Sub": "Master password for Aurora cluster ${ClusterIdentifier}"
                },
                "GenerateSecretString": {
                    "SecretStringTemplate": {
                        "Fn::Sub": '{"username": "${MasterUsername}"}'
                    },
                    "GenerateStringKey": "password",
                    "PasswordLength": 32,
                    "ExcludeCharacters": '"@/\\',
                },
                "Tags": tags_with_refs,
            },
        },
        "AuroraCluster": {
            "Type": "AWS::RDS::DBCluster",
            "Properties": {
                "DBClusterIdentifier": {"Ref": "ClusterIdentifier"},
                "Engine": "aurora-postgresql",
                "EngineVersion": {"Ref": "EngineVersion"},
                "DatabaseName": {"Ref": "DatabaseName"},
                "MasterUsername": {
                    "Fn::Sub": (
                        "{{resolve:secretsmanager:"
                        "${MasterSecret}:SecretString:username}}"
                    )
                },
                "MasterUserPassword": {
                    "Fn::Sub": (
                        "{{resolve:secretsmanager:"
                        "${MasterSecret}:SecretString:password}}"
                    )
                },
                "DBSubnetGroupName": {"Ref": "DBSubnetGroup"},
                "VpcSecurityGroupIds": [
                    {"Fn::GetAtt": ["ClusterSecurityGroup", "GroupId"]}
                ],
                "EnableIAMDatabaseAuthentication": True,
                "StorageEncrypted": True,
                "DeletionProtection": {
                    "Fn::If": ["IsDeletionProtectionEnabled", True, False]
                },
                "Tags": tags_with_refs,
            },
        },
        "WriterInstance": {
            "Type": "AWS::RDS::DBInstance",
            "Properties": {
                "DBClusterIdentifier": {"Ref": "AuroraCluster"},
                "DBInstanceIdentifier": {"Fn::Sub": "${ClusterIdentifier}-writer"},
                "DBInstanceClass": {"Ref": "InstanceClass"},
                "Engine": "aurora-postgresql",
                "PubliclyAccessible": {
                    "Fn::If": ["IsPubliclyAccessible", True, False]
                },
                "Tags": tags_with_refs,
            },
        },
        "DatabaseIAMPolicy": {
            "Type": "AWS::IAM::ManagedPolicy",
            "Properties": {
                "ManagedPolicyName": {
                    "Fn::Sub": "tapdb-${ClusterIdentifier}-rds-connect"
                },
                "Description": {
                    "Fn::Sub": (
                        "Allow IAM database auth to TAPDB "
                        "${ClusterIdentifier} Aurora cluster"
                    )
                },
                "PolicyDocument": {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Action": "rds-db:connect",
                            "Resource": {
                                "Fn::Sub": (
                                    "arn:aws:rds-db:${AWS::Region}:"
                                    "${AWS::AccountId}:dbuser:"
                                    "${AuroraCluster.DbClusterResourceId}"
                                    "/${MasterUsername}"
                                )
                            },
                        }
                    ],
                },
            },
        },
    }


def _outputs() -> dict[str, Any]:
    """CloudFormation Outputs section."""
    return {
        "ClusterEndpoint": {
            "Description": "Aurora cluster writer endpoint",
            "Value": {"Fn::GetAtt": ["AuroraCluster", "Endpoint.Address"]},
            "Export": {"Name": {"Fn::Sub": "${ClusterIdentifier}-endpoint"}},
        },
        "ClusterPort": {
            "Description": "Aurora cluster port",
            "Value": {"Fn::GetAtt": ["AuroraCluster", "Endpoint.Port"]},
        },

        "SecretArn": {
            "Description": "ARN of the Secrets Manager secret for master credentials",
            "Value": {"Ref": "MasterSecret"},
        },
        "SecurityGroupId": {
            "Description": "Security group ID for the Aurora cluster",
            "Value": {"Fn::GetAtt": ["ClusterSecurityGroup", "GroupId"]},
        },
        "DatabaseIAMPolicyArn": {
            "Description": "ARN of the IAM policy for database authentication",
            "Value": {"Ref": "DatabaseIAMPolicy"},
        },
    }


def generate_template() -> dict[str, Any]:
    """Generate a complete CloudFormation template for Aurora PostgreSQL.

    Returns:
        dict: CloudFormation template as a Python dictionary.
    """
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Aurora PostgreSQL cluster for TAPDB",
        "Conditions": {
            "HasProject": {"Fn::Not": [{"Fn::Equals": [{"Ref": "Project"}, ""]}]},
            "IsPubliclyAccessible": {
                "Fn::Equals": [{"Ref": "PubliclyAccessible"}, "true"]
            },
            "IsDeletionProtectionEnabled": {
                "Fn::Equals": [{"Ref": "DeletionProtection"}, "true"]
            },
        },
        "Parameters": _parameters(),
        "Resources": _resources(),
        "Outputs": _outputs(),
    }


def save_template(output_path: str | Path | None = None) -> Path:
    """Generate and save the CloudFormation template to a JSON file.

    Args:
        output_path: Where to save. Defaults to the bundled templates dir.

    Returns:
        Path to the saved file.
    """
    if output_path is None:
        output_path = Path(__file__).parent / "templates" / "aurora-postgres.json"
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    template = generate_template()
    output_path.write_text(json.dumps(template, indent=2) + "\n")
    return output_path
