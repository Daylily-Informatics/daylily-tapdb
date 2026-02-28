"""CloudFormation stack management for Aurora PostgreSQL clusters.

Provides ``AuroraStackManager`` — the Python layer that creates, monitors,
and deletes CloudFormation stacks using the template from ``cfn_template.py``.
Stack metadata is cached locally in ``~/.config/tapdb/aurora-stacks.json``.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from daylily_tapdb.aurora.cfn_template import generate_template
from daylily_tapdb.aurora.config import AuroraConfig

logger = logging.getLogger(__name__)

STACK_METADATA_PATH = Path.home() / ".config" / "tapdb" / "aurora-stacks.json"

# Terminal CFN states
_TERMINAL_STATES = {
    "CREATE_COMPLETE",
    "CREATE_FAILED",
    "DELETE_COMPLETE",
    "DELETE_FAILED",
    "ROLLBACK_COMPLETE",
    "ROLLBACK_FAILED",
    "UPDATE_COMPLETE",
    "UPDATE_FAILED",
    "UPDATE_ROLLBACK_COMPLETE",
    "UPDATE_ROLLBACK_FAILED",
}


def _load_metadata() -> dict[str, Any]:
    """Load stack metadata from local cache."""
    if STACK_METADATA_PATH.exists():
        return json.loads(STACK_METADATA_PATH.read_text())
    return {}


def _save_metadata(data: dict[str, Any]) -> None:
    """Persist stack metadata to local cache."""
    STACK_METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)
    STACK_METADATA_PATH.write_text(json.dumps(data, indent=2, default=str) + "\n")


def _cfn_events_summary(cfn_client: Any, stack_name: str, limit: int = 10) -> str:
    """Return a human-readable summary of recent CFN stack events."""
    try:
        resp = cfn_client.describe_stack_events(StackName=stack_name)
        events = resp.get("StackEvents", [])[:limit]
    except Exception:
        return "(unable to retrieve stack events)"
    lines: list[str] = []
    for ev in events:
        ts = ev.get("Timestamp", "")
        res = ev.get("LogicalResourceId", "")
        status = ev.get("ResourceStatus", "")
        reason = ev.get("ResourceStatusReason", "")
        line = f"  {ts}  {res}: {status}"
        if reason:
            line += f" — {reason}"
        lines.append(line)
    return "\n".join(lines) if lines else "(no events)"


class AuroraStackManager:
    """Manages CloudFormation stacks for Aurora PostgreSQL clusters."""

    def __init__(
        self,
        cfn_client: Any | None = None,
        ec2_client: Any | None = None,
        region: str = "us-west-2",
    ) -> None:
        self.region = region
        if cfn_client is not None:
            self._cfn = cfn_client
        else:
            try:
                import boto3
            except ImportError as exc:
                raise ImportError(
                    "boto3 is required for Aurora support. "
                    "Install it with: pip install daylily-tapdb[aurora]"
                ) from exc
            self._cfn = boto3.client("cloudformation", region_name=region)
        if ec2_client is not None:
            self._ec2 = ec2_client
        else:
            try:
                import boto3
            except ImportError as exc:
                raise ImportError(
                    "boto3 is required for Aurora support. "
                    "Install it with: pip install daylily-tapdb[aurora]"
                ) from exc
            self._ec2 = boto3.client("ec2", region_name=region)

    # ------------------------------------------------------------------
    # create_stack
    # ------------------------------------------------------------------

    def _resolve_vpc_and_subnets(
        self, config: AuroraConfig
    ) -> tuple[str, list[str]]:
        """Resolve VPC ID and subnet IDs from config or auto-discovery.

        If ``config.vpc_id`` is set, use that VPC. Otherwise discover the
        default VPC.  Then look up subnets for the resolved VPC.

        Returns:
            (vpc_id, subnet_ids)

        Raises:
            RuntimeError: If no VPC can be resolved or no subnets found.
        """
        vpc_id = config.vpc_id

        if not vpc_id:
            vpcs = self._ec2.describe_vpcs(
                Filters=[{"Name": "isDefault", "Values": ["true"]}]
            )
            vpc_list = vpcs.get("Vpcs", [])
            if not vpc_list:
                raise RuntimeError(
                    "No default VPC found and --vpc-id was not provided. "
                    "Please specify a VPC with --vpc-id."
                )
            vpc_id = vpc_list[0]["VpcId"]
            logger.info("Auto-discovered default VPC: %s", vpc_id)

        subnets_resp = self._ec2.describe_subnets(
            Filters=[{"Name": "vpc-id", "Values": [vpc_id]}]
        )
        subnet_ids = [s["SubnetId"] for s in subnets_resp.get("Subnets", [])]
        if not subnet_ids:
            raise RuntimeError(
                f"No subnets found for VPC {vpc_id}. "
                "Ensure the VPC has at least two subnets."
            )
        return vpc_id, subnet_ids

    def initiate_create_stack(
        self, config: AuroraConfig
    ) -> dict[str, str]:
        """Start stack creation without waiting for completion.

        Args:
            config: Aurora configuration with cluster parameters.

        Returns:
            dict with keys: stack_name, stack_id, vpc_id.

        Raises:
            RuntimeError: If VPC/subnets cannot be resolved.
        """
        vpc_id, subnet_ids = self._resolve_vpc_and_subnets(config)

        stack_name = f"tapdb-{config.cluster_identifier}"
        template_body = json.dumps(generate_template())

        tags = [{"Key": k, "Value": v} for k, v in config.tags.items()]
        cost = config.tags.get("lsmc-cost-center", "global")
        proj = config.tags.get("lsmc-project", "")
        params = [
            {
                "ParameterKey": "ClusterIdentifier",
                "ParameterValue": config.cluster_identifier,
            },
            {"ParameterKey": "InstanceClass", "ParameterValue": config.instance_class},
            {"ParameterKey": "EngineVersion", "ParameterValue": config.engine_version},
            {"ParameterKey": "VpcId", "ParameterValue": vpc_id},
            {
                "ParameterKey": "SubnetIds",
                "ParameterValue": ",".join(subnet_ids),
            },
            {"ParameterKey": "CostCenter", "ParameterValue": cost},
            {"ParameterKey": "Project", "ParameterValue": proj},
            {
                "ParameterKey": "PubliclyAccessible",
                "ParameterValue": "true" if config.publicly_accessible else "false",
            },
        ]

        logger.info(
            "Creating CloudFormation stack %s in %s ...",
            stack_name,
            config.region,
        )
        resp = self._cfn.create_stack(
            StackName=stack_name,
            TemplateBody=template_body,
            Parameters=params,
            Tags=tags,
            Capabilities=["CAPABILITY_IAM"],
        )
        stack_id = resp["StackId"]
        logger.info("Stack creation initiated: %s", stack_id)

        return {
            "stack_name": stack_name,
            "stack_id": stack_id,
            "vpc_id": vpc_id,
        }

    def create_stack(
        self,
        config: AuroraConfig,
        callback: Callable[[str, float], None] | None = None,
    ) -> dict[str, Any]:
        """Create a CloudFormation stack and wait for completion.

        Args:
            config: Aurora configuration with cluster parameters.
            callback: Optional progress callback ``(status, elapsed_secs)``.

        Returns:
            dict with keys: stack_name, stack_id, outputs (after completion).

        Raises:
            RuntimeError: If stack creation fails or VPC/subnets unresolvable.
        """
        initiated = self.initiate_create_stack(config)
        stack_name = initiated["stack_name"]
        stack_id = initiated["stack_id"]

        result = self.wait_for_stack(
            stack_name, "CREATE_COMPLETE", callback=callback
        )
        if result["status"] != "CREATE_COMPLETE":
            events = _cfn_events_summary(self._cfn, stack_name)
            raise RuntimeError(
                f"Stack {stack_name} ended in {result['status']}.\n"
                f"Recent events:\n{events}"
            )

        meta = _load_metadata()
        meta[stack_name] = {
            "stack_id": stack_id,
            "region": config.region,
            "cluster_identifier": config.cluster_identifier,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "status": "CREATE_COMPLETE",
        }
        _save_metadata(meta)

        return {
            "stack_name": stack_name,
            "stack_id": stack_id,
            "outputs": result.get("outputs", {}),
        }

    # ------------------------------------------------------------------
    # delete_stack
    # ------------------------------------------------------------------

    def delete_stack(
        self, stack_name: str, retain_networking: bool = True
    ) -> dict[str, str]:
        """Delete a CloudFormation stack.

        Args:
            stack_name: Name of the stack to delete.
            retain_networking: If True, retain VPC security group and subnet
                group resources for reuse in future deployments.

        Returns:
            dict with stack_name and final status.
        """
        kwargs: dict[str, Any] = {"StackName": stack_name}
        if retain_networking:
            kwargs["RetainResources"] = ["ClusterSecurityGroup", "DBSubnetGroup"]
            logger.info(
                "Deleting stack %s (retaining networking resources) ...",
                stack_name,
            )
        else:
            logger.info("Deleting stack %s (all resources) ...", stack_name)

        self._cfn.delete_stack(**kwargs)

        result = self.wait_for_stack(stack_name, "DELETE_COMPLETE")
        if result["status"] != "DELETE_COMPLETE":
            events = _cfn_events_summary(self._cfn, stack_name)
            raise RuntimeError(
                f"Stack deletion ended in {result['status']}.\nRecent events:\n{events}"
            )

        meta = _load_metadata()
        if stack_name in meta:
            meta[stack_name]["status"] = "DELETE_COMPLETE"
            meta[stack_name]["deleted_at"] = datetime.now(timezone.utc).isoformat()
            _save_metadata(meta)

        return {"stack_name": stack_name, "status": "DELETE_COMPLETE"}

    # ------------------------------------------------------------------
    # get_stack_status
    # ------------------------------------------------------------------

    def get_stack_status(self, stack_name: str) -> dict[str, Any]:
        """Return current stack status and outputs.

        Args:
            stack_name: Name of the CloudFormation stack.

        Returns:
            dict with keys: stack_name, status, outputs.

        Raises:
            RuntimeError: If the stack does not exist.
        """
        try:
            resp = self._cfn.describe_stacks(StackName=stack_name)
        except Exception as exc:
            raise RuntimeError(f"Stack {stack_name} not found: {exc}") from exc

        stacks = resp.get("Stacks", [])
        if not stacks:
            raise RuntimeError(f"Stack {stack_name} not found.")

        stack = stacks[0]
        outputs: dict[str, str] = {}
        for out in stack.get("Outputs", []):
            outputs[out["OutputKey"]] = out["OutputValue"]

        return {
            "stack_name": stack_name,
            "status": stack["StackStatus"],
            "outputs": outputs,
        }

    # ------------------------------------------------------------------
    # detect_existing_resources
    # ------------------------------------------------------------------

    def detect_existing_resources(self, region: str | None = None) -> dict[str, Any]:
        """Find existing tapdb resources by ``lsmc-project`` tag.

        Scans CloudFormation stacks in the given region for stacks tagged
        with ``lsmc-project`` starting with ``tapdb-``.

        Args:
            region: AWS region to scan.  Defaults to ``self.region``.

        Returns:
            dict mapping stack names to their status and outputs.
        """
        region = region or self.region
        found: dict[str, Any] = {}
        paginator = self._cfn.get_paginator("list_stacks")
        active_statuses = [
            "CREATE_COMPLETE",
            "UPDATE_COMPLETE",
            "UPDATE_ROLLBACK_COMPLETE",
            "ROLLBACK_COMPLETE",
        ]
        for page in paginator.paginate(StackStatusFilter=active_statuses):
            for summary in page.get("StackSummaries", []):
                name = summary["StackName"]
                if not name.startswith("tapdb-"):
                    continue
                try:
                    info = self.get_stack_status(name)
                    resp = self._cfn.describe_stacks(StackName=name)
                    stack = resp["Stacks"][0]
                    tags = {t["Key"]: t["Value"] for t in stack.get("Tags", [])}
                    if tags.get("lsmc-project", "").startswith("tapdb-"):
                        found[name] = {
                            "status": info["status"],
                            "outputs": info["outputs"],
                            "tags": tags,
                        }
                except Exception:
                    logger.debug("Skipping stack %s (describe failed)", name)
        return found

    # ------------------------------------------------------------------
    # wait_for_stack
    # ------------------------------------------------------------------

    def wait_for_stack(
        self,
        stack_name: str,
        target_status: str,
        timeout: int = 900,
        callback: Callable[[str, float], None] | None = None,
    ) -> dict[str, Any]:
        """Poll stack status at a fixed 5-second interval until terminal state.

        Args:
            stack_name: Name of the CloudFormation stack.
            target_status: The desired terminal status (e.g. CREATE_COMPLETE).
            timeout: Maximum seconds to wait (default 900 = 15 min).
            callback: Optional ``(status, elapsed_seconds)`` called each poll.

        Returns:
            dict with keys: status, outputs (if available).
        """
        start = time.monotonic()
        interval = 5.0

        while True:
            elapsed = time.monotonic() - start
            if elapsed > timeout:
                logger.error(
                    "Timed out waiting for stack %s (%.0fs elapsed)",
                    stack_name,
                    elapsed,
                )
                return {"status": "TIMEOUT", "outputs": {}}

            try:
                info = self.get_stack_status(stack_name)
                status = info["status"]
            except RuntimeError:
                if target_status == "DELETE_COMPLETE":
                    return {"status": "DELETE_COMPLETE", "outputs": {}}
                raise

            logger.info("Stack %s: %s (%.0fs elapsed)", stack_name, status, elapsed)

            if callback is not None:
                callback(status, elapsed)

            if status == target_status:
                return {"status": status, "outputs": info.get("outputs", {})}

            if status in _TERMINAL_STATES and status != target_status:
                return {"status": status, "outputs": info.get("outputs", {})}

            time.sleep(interval)
