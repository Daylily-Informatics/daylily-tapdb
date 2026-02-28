"""Aurora-specific configuration dataclass.

Holds all parameters needed to provision and connect to an AWS RDS Aurora
PostgreSQL cluster.  The ``tags`` field always includes the mandatory
``lsmc-cost-center`` and ``lsmc-project`` keys (with sensible defaults).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


def _default_tags() -> dict[str, str]:
    return {
        "lsmc-cost-center": "global",
        "lsmc-project": "tapdb-us-west-2",
    }


@dataclass
class AuroraConfig:
    """Configuration for an Aurora PostgreSQL cluster.

    Attributes:
        region: AWS region (e.g. ``us-west-2``).
        cluster_identifier: RDS cluster identifier.
        instance_class: Instance class (e.g. ``db.r6g.large``).
        engine_version: Aurora PostgreSQL engine version.
        vpc_id: VPC to deploy into.
        subnet_ids: Subnets for the DB subnet group.
        security_group_ids: Security groups attached to the cluster.
        iam_auth: Whether IAM database authentication is enabled.
        ssl: Whether to require SSL connections.
        tags: AWS resource tags.  ``lsmc-cost-center`` and ``lsmc-project``
            are always present (defaults: ``global`` / ``tapdb-{region}``).
    """

    region: str = "us-west-2"
    cluster_identifier: str = ""
    instance_class: str = "db.r6g.large"
    engine_version: str = "16.6"
    vpc_id: str = ""
    subnet_ids: list[str] = field(default_factory=list)
    security_group_ids: list[str] = field(default_factory=list)
    iam_auth: bool = True
    ssl: bool = True
    publicly_accessible: bool = True
    tags: dict[str, str] = field(default_factory=_default_tags)

    def __post_init__(self) -> None:
        # Ensure mandatory tags are always present with correct defaults.
        self.tags.setdefault("lsmc-cost-center", "global")
        # Always recompute lsmc-project from region unless caller explicitly set it.
        if (
            "lsmc-project" not in self.tags
            or self.tags["lsmc-project"] == _default_tags()["lsmc-project"]
        ):
            self.tags["lsmc-project"] = f"tapdb-{self.region}"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AuroraConfig:
        """Build an ``AuroraConfig`` from a plain dict (e.g. parsed YAML)."""
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}  # type: ignore[attr-defined]
        filtered = {k: v for k, v in data.items() if k in known_fields}
        return cls(**filtered)
