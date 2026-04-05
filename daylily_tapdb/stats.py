"""Aggregate statistics for TapDB tables.

Provides typed query helpers for template, instance, and lineage
table-level summary statistics.

Example::

    from daylily_tapdb.stats import get_template_stats

    with conn.session_scope(commit=False) as session:
        ts = get_template_stats(session)
        print(ts.total, ts.distinct_types)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass
class TemplateStats:
    """Aggregate statistics for ``generic_template``."""

    total: int
    distinct_types: int
    distinct_subtypes: int
    distinct_categories: int
    latest_created: Optional[datetime]
    earliest_created: Optional[datetime]
    average_age: Optional[timedelta]
    singleton_count: int


@dataclass
class InstanceStats:
    """Aggregate statistics for ``generic_instance``."""

    total: int
    distinct_types: int
    distinct_polymorphic_discriminators: int
    distinct_categories: int
    distinct_subtypes: int
    latest_created: Optional[datetime]
    earliest_created: Optional[datetime]
    average_age: Optional[timedelta]


@dataclass
class LineageStats:
    """Aggregate statistics for ``generic_instance_lineage``."""

    total: int
    distinct_parent_types: int
    distinct_child_types: int
    distinct_polymorphic_discriminators: int
    distinct_categories: int
    latest_created: Optional[datetime]
    earliest_created: Optional[datetime]
    average_age: Optional[timedelta]


def get_template_stats(
    session: Session,
    *,
    include_deleted: bool = False,
    domain_code: Optional[str] = None,
    issuer_app_code: Optional[str] = None,
) -> TemplateStats:
    """Return aggregate statistics for the ``generic_template`` table."""
    where = ["is_deleted = :is_deleted"]
    params: dict = {"is_deleted": include_deleted}
    if domain_code is not None:
        where.append("domain_code = :domain_code")
        params["domain_code"] = domain_code
    if issuer_app_code is not None:
        where.append("issuer_app_code = :issuer_app_code")
        params["issuer_app_code"] = issuer_app_code
    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT type) AS distinct_types,
            COUNT(DISTINCT subtype) AS distinct_subtypes,
            COUNT(DISTINCT category) AS distinct_categories,
            MAX(created_dt) AS latest_created,
            MIN(created_dt) AS earliest_created,
            AVG(AGE(NOW(), created_dt)) AS average_age,
            COUNT(CASE WHEN is_singleton THEN 1 END) AS singleton_count
        FROM generic_template
        WHERE {' AND '.join(where)}
    """
    row = session.execute(text(sql), params).mappings().one()
    return TemplateStats(
        total=row["total"],
        distinct_types=row["distinct_types"],
        distinct_subtypes=row["distinct_subtypes"],
        distinct_categories=row["distinct_categories"],
        latest_created=row["latest_created"],
        earliest_created=row["earliest_created"],
        average_age=row["average_age"],
        singleton_count=row["singleton_count"],
    )


def get_instance_stats(
    session: Session,
    *,
    include_deleted: bool = False,
    domain_code: Optional[str] = None,
    issuer_app_code: Optional[str] = None,
) -> InstanceStats:
    """Return aggregate statistics for the ``generic_instance`` table."""
    where = ["is_deleted = :is_deleted"]
    params: dict = {"is_deleted": include_deleted}
    if domain_code is not None:
        where.append("domain_code = :domain_code")
        params["domain_code"] = domain_code
    if issuer_app_code is not None:
        where.append("issuer_app_code = :issuer_app_code")
        params["issuer_app_code"] = issuer_app_code
    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT type) AS distinct_types,
            COUNT(DISTINCT polymorphic_discriminator) AS distinct_poly,
            COUNT(DISTINCT category) AS distinct_categories,
            COUNT(DISTINCT subtype) AS distinct_subtypes,
            MAX(created_dt) AS latest_created,
            MIN(created_dt) AS earliest_created,
            AVG(AGE(NOW(), created_dt)) AS average_age
        FROM generic_instance
        WHERE {' AND '.join(where)}
    """
    row = session.execute(text(sql), params).mappings().one()
    return InstanceStats(
        total=row["total"],
        distinct_types=row["distinct_types"],
        distinct_polymorphic_discriminators=row["distinct_poly"],
        distinct_categories=row["distinct_categories"],
        distinct_subtypes=row["distinct_subtypes"],
        latest_created=row["latest_created"],
        earliest_created=row["earliest_created"],
        average_age=row["average_age"],
    )


def get_lineage_stats(
    session: Session,
    *,
    include_deleted: bool = False,
    domain_code: Optional[str] = None,
    issuer_app_code: Optional[str] = None,
) -> LineageStats:
    """Return aggregate statistics for the ``generic_instance_lineage`` table."""
    where = ["is_deleted = :is_deleted"]
    params: dict = {"is_deleted": include_deleted}
    if domain_code is not None:
        where.append("domain_code = :domain_code")
        params["domain_code"] = domain_code
    if issuer_app_code is not None:
        where.append("issuer_app_code = :issuer_app_code")
        params["issuer_app_code"] = issuer_app_code
    sql = f"""
        SELECT
            COUNT(*) AS total,
            COUNT(DISTINCT parent_type) AS distinct_parent_types,
            COUNT(DISTINCT child_type) AS distinct_child_types,
            COUNT(DISTINCT polymorphic_discriminator) AS distinct_poly,
            COUNT(DISTINCT category) AS distinct_categories,
            MAX(created_dt) AS latest_created,
            MIN(created_dt) AS earliest_created,
            AVG(AGE(NOW(), created_dt)) AS average_age
        FROM generic_instance_lineage
        WHERE {' AND '.join(where)}
    """
    row = session.execute(text(sql), params).mappings().one()
    return LineageStats(
        total=row["total"],
        distinct_parent_types=row["distinct_parent_types"],
        distinct_child_types=row["distinct_child_types"],
        distinct_polymorphic_discriminators=row["distinct_poly"],
        distinct_categories=row["distinct_categories"],
        latest_created=row["latest_created"],
        earliest_created=row["earliest_created"],
        average_age=row["average_age"],
    )
