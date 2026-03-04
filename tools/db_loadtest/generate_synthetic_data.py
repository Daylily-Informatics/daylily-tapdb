#!/usr/bin/env python3
"""Generate synthetic TAPDB load-test data for pgbench harnesses.

This script seeds generic instances and lineage edges in a way that mirrors TAPDB
usage patterns while staying reproducible and tunable.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from pathlib import Path

import psycopg2
from psycopg2.extras import execute_values


DEFAULT_HOST = "localhost"
DEFAULT_PORT = 5533
DEFAULT_DB = "tapdb_tapdb_dev"
DEFAULT_USER = os.environ.get("USER", "postgres")


@dataclass
class RunSummary:
    run_id: str
    template_uuid: int
    tenant_count: int
    base_instance_count: int
    all_instance_count: int
    lineage_edges_created: int
    revision_edges_created: int
    min_instance_uuid: int
    max_instance_uuid: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate synthetic TAPDB data for db load testing."
    )
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--dbname", default=DEFAULT_DB)
    parser.add_argument("--user", default=DEFAULT_USER)
    parser.add_argument("--password", default=os.environ.get("PGPASSWORD", ""))
    parser.add_argument(
        "--dsn",
        default=None,
        help="Optional full psycopg2 DSN. If provided, host/port/db/user/password are ignored.",
    )

    parser.add_argument("--tenants", type=int, default=2)
    parser.add_argument("--instances-per-tenant", type=int, default=1000)
    parser.add_argument("--lineage-edges", type=int, default=2000)

    parser.add_argument("--revision-chain-count", type=int, default=0)
    parser.add_argument("--revision-chain-length", type=int, default=0)

    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--truncate-first",
        action="store_true",
        help=(
            "Soft-delete prior loadtest rows before seeding new data. "
            "(TAPDB delete triggers convert deletes to soft deletes.)"
        ),
    )
    parser.add_argument(
        "--write-vars",
        default=None,
        help="Optional output path for shell variables consumed by pgbench commands.",
    )

    parser.add_argument(
        "--max-depth",
        type=int,
        default=4,
        help="Default max_depth value to emit for read_graph_recursive pgbench script.",
    )
    parser.add_argument(
        "--latest-limit",
        type=int,
        default=50,
        help="Default latest_limit value to emit for list_latest_revisions pgbench script.",
    )

    args = parser.parse_args()

    if args.tenants < 1:
        parser.error("--tenants must be >= 1")
    if args.instances_per_tenant < 1:
        parser.error("--instances-per-tenant must be >= 1")
    if args.lineage_edges < 0:
        parser.error("--lineage-edges must be >= 0")
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")
    if args.revision_chain_count < 0:
        parser.error("--revision-chain-count must be >= 0")
    if args.revision_chain_length < 0:
        parser.error("--revision-chain-length must be >= 0")
    if args.revision_chain_count > 0 and args.revision_chain_length < 2:
        parser.error("--revision-chain-length must be >= 2 when --revision-chain-count > 0")
    if args.max_depth < 1:
        parser.error("--max-depth must be >= 1")
    if args.latest_limit < 1:
        parser.error("--latest-limit must be >= 1")

    return args


def connect(args: argparse.Namespace):
    if args.dsn:
        return psycopg2.connect(args.dsn)
    return psycopg2.connect(
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        password=args.password,
    )


def ensure_loadtest_template(cur) -> int:
    cur.execute(
        """
        INSERT INTO generic_template (
            name,
            polymorphic_discriminator,
            category,
            type,
            subtype,
            version,
            instance_prefix,
            instance_polymorphic_identity,
            json_addl,
            bstatus,
            is_singleton,
            is_deleted
        )
        VALUES (
            'Loadtest Node Template',
            'generic_template',
            'generic',
            'generic',
            'loadtest_node',
            '1.0',
            'GX',
            'generic_instance',
            jsonb_build_object(
                'description', 'Synthetic template for TAPDB DB load testing',
                'loadtest', true
            ),
            'active',
            false,
            false
        )
        ON CONFLICT (category, type, subtype, version)
        DO UPDATE SET
            name = EXCLUDED.name,
            instance_prefix = EXCLUDED.instance_prefix,
            instance_polymorphic_identity = EXCLUDED.instance_polymorphic_identity,
            bstatus = EXCLUDED.bstatus,
            is_singleton = EXCLUDED.is_singleton,
            is_deleted = false,
            json_addl = COALESCE(generic_template.json_addl, '{}'::jsonb) || EXCLUDED.json_addl,
            modified_dt = CURRENT_TIMESTAMP
        RETURNING uuid
        """
    )
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("Failed to create or fetch loadtest template uuid")
    return int(row[0])


def soft_cleanup(cur, template_uuid: int) -> None:
    # TAPDB soft-delete triggers transform DELETE into is_deleted=true updates.
    cur.execute(
        """
        DELETE FROM generic_instance_lineage
        WHERE is_deleted = FALSE
          AND (
                relationship_type IN ('loadtest_rel', 'revision_of')
             OR COALESCE(json_addl->>'loadtest', 'false') = 'true'
          )
        """
    )
    cur.execute(
        """
        DELETE FROM generic_instance
        WHERE is_deleted = FALSE
          AND template_uuid = %s
        """,
        (template_uuid,),
    )


def insert_base_instances(
    cur,
    *,
    run_id: str,
    template_uuid: int,
    tenants: int,
    instances_per_tenant: int,
    batch_size: int,
) -> int:
    sql = """
        INSERT INTO generic_instance (
            name,
            polymorphic_discriminator,
            category,
            type,
            subtype,
            version,
            template_uuid,
            json_addl,
            bstatus,
            is_singleton
        )
        VALUES %s
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)"

    total_target = tenants * instances_per_tenant
    inserted = 0
    batch = []

    for tenant_idx in range(1, tenants + 1):
        tenant_id = f"tenant_{tenant_idx:04d}"
        for ordinal in range(1, instances_per_tenant + 1):
            payload = {
                "loadtest": True,
                "run_id": run_id,
                "kind": "base",
                "tenant_id": tenant_id,
                "tenant_ord": tenant_idx,
                "instance_ord": ordinal,
            }
            batch.append(
                (
                    f"lt-{run_id}-t{tenant_idx:04d}-i{ordinal:08d}",
                    "generic_instance",
                    "generic",
                    "generic",
                    "loadtest_node",
                    "1.0",
                    template_uuid,
                    json.dumps(payload, separators=(",", ":")),
                    "active",
                    False,
                )
            )
            if len(batch) >= batch_size:
                execute_values(cur, sql, batch, template=template, page_size=batch_size)
                inserted += len(batch)
                batch.clear()
                print(f"Inserted base instances: {inserted}/{total_target}")

    if batch:
        execute_values(cur, sql, batch, template=template, page_size=batch_size)
        inserted += len(batch)
        print(f"Inserted base instances: {inserted}/{total_target}")

    return inserted


def get_run_instance_bounds(cur, *, run_id: str) -> tuple[int, int, int]:
    cur.execute(
        """
        SELECT
            COUNT(*)::bigint AS cnt,
            MIN(uuid)::bigint AS min_uuid,
            MAX(uuid)::bigint AS max_uuid
        FROM generic_instance
        WHERE is_deleted = FALSE
          AND COALESCE(json_addl->>'loadtest', 'false') = 'true'
          AND json_addl->>'run_id' = %s
        """,
        (run_id,),
    )
    row = cur.fetchone()
    if not row or row[0] is None or row[0] == 0:
        raise RuntimeError("No instances were found for the current run")
    count, min_uuid, max_uuid = int(row[0]), int(row[1]), int(row[2])
    return count, min_uuid, max_uuid


def get_base_instance_count(cur, *, run_id: str) -> int:
    cur.execute(
        """
        SELECT COUNT(*)::bigint
        FROM generic_instance
        WHERE is_deleted = FALSE
          AND COALESCE(json_addl->>'loadtest', 'false') = 'true'
          AND json_addl->>'run_id' = %s
          AND COALESCE(json_addl->>'kind', '') = 'base'
        """,
        (run_id,),
    )
    row = cur.fetchone()
    return int(row[0] or 0)


def insert_synthetic_edges(cur, *, run_id: str, lineage_edges: int) -> int:
    if lineage_edges == 0:
        return 0

    base_count = get_base_instance_count(cur, run_id=run_id)
    if base_count < 2:
        raise RuntimeError("Need at least two base instances to create lineage edges")

    capacity = base_count * (base_count - 1)
    if lineage_edges > capacity:
        raise RuntimeError(
            f"Requested {lineage_edges} lineage edges exceeds unique directed capacity "
            f"{capacity} for {base_count} base instances"
        )

    cur.execute(
        """
        WITH base_nodes AS (
            SELECT
                uuid,
                row_number() OVER (ORDER BY uuid) - 1 AS idx
            FROM generic_instance
            WHERE is_deleted = FALSE
              AND COALESCE(json_addl->>'loadtest', 'false') = 'true'
              AND json_addl->>'run_id' = %s
              AND COALESCE(json_addl->>'kind', '') = 'base'
        ),
        node_count AS (
            SELECT COUNT(*)::bigint AS n FROM base_nodes
        ),
        edge_plan AS (
            SELECT
                gs.i::bigint AS edge_ix,
                (gs.i %% nc.n)::bigint AS parent_idx,
                (1 + ((gs.i / nc.n) %% (nc.n - 1)))::bigint AS offset,
                nc.n AS n
            FROM generate_series(0, %s - 1) AS gs(i)
            CROSS JOIN node_count nc
        ),
        pairs AS (
            SELECT
                ep.edge_ix,
                ep.parent_idx,
                ((ep.parent_idx + ep.offset) %% ep.n)::bigint AS child_idx
            FROM edge_plan ep
        )
        INSERT INTO generic_instance_lineage (
            name,
            polymorphic_discriminator,
            category,
            type,
            subtype,
            version,
            parent_instance_uuid,
            child_instance_uuid,
            relationship_type,
            json_addl,
            bstatus,
            is_singleton
        )
        SELECT
            ('lt-edge-' || %s || '-' || pairs.edge_ix::text),
            'generic_instance_lineage',
            'lineage',
            'lineage',
            'generic',
            '1.0',
            parent.uuid,
            child.uuid,
            'loadtest_rel',
            jsonb_build_object(
                'loadtest', true,
                'run_id', %s,
                'kind', 'synthetic_edge',
                'edge_ix', pairs.edge_ix
            ),
            'active',
            false
        FROM pairs
        JOIN base_nodes parent ON parent.idx = pairs.parent_idx
        JOIN base_nodes child ON child.idx = pairs.child_idx
        ON CONFLICT DO NOTHING
        """,
        (run_id, lineage_edges, run_id, run_id),
    )

    # ON CONFLICT DO NOTHING keeps this idempotent if accidentally rerun with same run_id.
    return int(cur.rowcount)


def insert_revision_chains(
    cur,
    *,
    run_id: str,
    template_uuid: int,
    tenants: int,
    chain_count: int,
    chain_length: int,
    rng: random.Random,
) -> tuple[int, int]:
    if chain_count == 0:
        return 0, 0

    revision_instance_count = 0
    revision_edge_count = 0

    for chain_id in range(1, chain_count + 1):
        tenant_ord = rng.randint(1, tenants)
        tenant_id = f"tenant_{tenant_ord:04d}"
        previous_uuid: int | None = None

        for rev in range(1, chain_length + 1):
            payload = {
                "loadtest": True,
                "run_id": run_id,
                "kind": "revision",
                "tenant_id": tenant_id,
                "chain_id": chain_id,
                "revision": rev,
            }

            cur.execute(
                """
                INSERT INTO generic_instance (
                    name,
                    polymorphic_discriminator,
                    category,
                    type,
                    subtype,
                    version,
                    template_uuid,
                    json_addl,
                    bstatus,
                    is_singleton
                )
                VALUES (
                    %s,
                    'generic_instance',
                    'generic',
                    'generic',
                    'loadtest_node',
                    '1.0',
                    %s,
                    %s::jsonb,
                    'active',
                    false
                )
                RETURNING uuid
                """,
                (
                    f"lt-{run_id}-rev-chain{chain_id:04d}-r{rev:04d}",
                    template_uuid,
                    json.dumps(payload, separators=(",", ":")),
                ),
            )
            current_uuid = int(cur.fetchone()[0])
            revision_instance_count += 1

            if previous_uuid is not None:
                edge_payload = {
                    "loadtest": True,
                    "run_id": run_id,
                    "kind": "revision_edge",
                    "chain_id": chain_id,
                    "to_revision": rev,
                }
                # Direction: newer revision points to prior revision.
                cur.execute(
                    """
                    INSERT INTO generic_instance_lineage (
                        name,
                        polymorphic_discriminator,
                        category,
                        type,
                        subtype,
                        version,
                        parent_instance_uuid,
                        child_instance_uuid,
                        relationship_type,
                        json_addl,
                        bstatus,
                        is_singleton
                    )
                    VALUES (
                        %s,
                        'generic_instance_lineage',
                        'lineage',
                        'lineage',
                        'generic',
                        '1.0',
                        %s,
                        %s,
                        'revision_of',
                        %s::jsonb,
                        'active',
                        false
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        f"lt-{run_id}-rev-chain{chain_id:04d}-r{rev:04d}-revision_of-r{rev-1:04d}",
                        current_uuid,
                        previous_uuid,
                        json.dumps(edge_payload, separators=(",", ":")),
                    ),
                )
                revision_edge_count += cur.rowcount

            previous_uuid = current_uuid

    return revision_instance_count, revision_edge_count


def write_vars_file(path_str: str, *, summary: RunSummary, max_depth: int, latest_limit: int) -> None:
    path = Path(path_str).expanduser().resolve()
    path.parent.mkdir(parents=True, exist_ok=True)

    content = (
        "# Generated by tools/db_loadtest/generate_synthetic_data.py\n"
        f"template_uuid={summary.template_uuid}\n"
        f"tenant_count={summary.tenant_count}\n"
        f"min_instance_uuid={summary.min_instance_uuid}\n"
        f"max_instance_uuid={summary.max_instance_uuid}\n"
        f"max_depth={max_depth}\n"
        f"latest_limit={latest_limit}\n"
    )

    path.write_text(content, encoding="utf-8")
    print(f"Wrote pgbench variable file: {path}")


def print_pgbench_commands(
    summary: RunSummary,
    *,
    host: str,
    port: int,
    dbname: str,
    user: str,
    max_depth: int,
    latest_limit: int,
) -> None:
    base = "tools/db_loadtest/pgbench"
    vars_block = (
        f"-D template_uuid={summary.template_uuid} "
        f"-D tenant_count={summary.tenant_count} "
        f"-D min_instance_uuid={summary.min_instance_uuid} "
        f"-D max_instance_uuid={summary.max_instance_uuid} "
        f"-D max_depth={max_depth} "
        f"-D latest_limit={latest_limit}"
    )

    print("\nRecommended pgbench commands:\n")
    print(
        f"pgbench -h {host} -p {port} -U {user} -n -c 8 -j 4 -T 60 "
        f"{vars_block} "
        f"-f {base}/insert_instance.sql "
        f"{dbname}"
    )
    print(
        f"pgbench -h {host} -p {port} -U {user} -n -c 8 -j 4 -T 60 "
        f"{vars_block} "
        f"-f {base}/insert_lineage_edge.sql "
        f"{dbname}"
    )
    print(
        f"pgbench -h {host} -p {port} -U {user} -n -c 16 -j 4 -T 60 "
        f"{vars_block} "
        f"-f {base}/read_graph_recursive.sql "
        f"{dbname}"
    )
    print(
        f"pgbench -h {host} -p {port} -U {user} -n -c 8 -j 4 -T 60 "
        f"{vars_block} "
        f"-f {base}/list_latest_revisions.sql "
        f"{dbname}"
    )
    print(
        f"DB_HOST={host} DB_PORT={port} DB_NAME={dbname} DB_USER={user} "
        f"TEMPLATE_UUID={summary.template_uuid} TENANT_COUNT={summary.tenant_count} "
        f"MIN_INSTANCE_UUID={summary.min_instance_uuid} MAX_INSTANCE_UUID={summary.max_instance_uuid} "
        f"MAX_DEPTH={max_depth} LATEST_LIMIT={latest_limit} "
        f"{base}/run_app_shaped.sh"
    )


def run(args: argparse.Namespace) -> RunSummary:
    rng = random.Random(args.seed)
    run_id = time.strftime("%Y%m%d%H%M%S") + f"-{args.seed}"
    print(f"Starting synthetic data generation for run_id={run_id}")

    conn = connect(args)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            template_uuid = ensure_loadtest_template(cur)
            print(f"Using loadtest template uuid={template_uuid}")

            if args.truncate_first:
                print("Soft-cleaning prior loadtest rows (--truncate-first)")
                soft_cleanup(cur, template_uuid)

            base_inserted = insert_base_instances(
                cur,
                run_id=run_id,
                template_uuid=template_uuid,
                tenants=args.tenants,
                instances_per_tenant=args.instances_per_tenant,
                batch_size=args.batch_size,
            )

            edges_created = insert_synthetic_edges(
                cur,
                run_id=run_id,
                lineage_edges=args.lineage_edges,
            )

            rev_instances, rev_edges = insert_revision_chains(
                cur,
                run_id=run_id,
                template_uuid=template_uuid,
                tenants=args.tenants,
                chain_count=args.revision_chain_count,
                chain_length=args.revision_chain_length,
                rng=rng,
            )

            all_count, min_uuid, max_uuid = get_run_instance_bounds(cur, run_id=run_id)

        conn.commit()

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return RunSummary(
        run_id=run_id,
        template_uuid=template_uuid,
        tenant_count=args.tenants,
        base_instance_count=base_inserted,
        all_instance_count=all_count,
        lineage_edges_created=edges_created,
        revision_edges_created=rev_edges,
        min_instance_uuid=min_uuid,
        max_instance_uuid=max_uuid,
    )


def main() -> int:
    args = parse_args()
    try:
        summary = run(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1

    print("\nGeneration complete:")
    print(f"  run_id:                {summary.run_id}")
    print(f"  template_uuid:         {summary.template_uuid}")
    print(f"  tenant_count:          {summary.tenant_count}")
    print(f"  base instances:        {summary.base_instance_count}")
    print(f"  all instances:         {summary.all_instance_count}")
    print(f"  lineage edges:         {summary.lineage_edges_created}")
    print(f"  revision edges:        {summary.revision_edges_created}")
    print(f"  min instance uuid:     {summary.min_instance_uuid}")
    print(f"  max instance uuid:     {summary.max_instance_uuid}")

    if args.write_vars:
        write_vars_file(
            args.write_vars,
            summary=summary,
            max_depth=args.max_depth,
            latest_limit=args.latest_limit,
        )

    print_pgbench_commands(
        summary,
        host=args.host,
        port=args.port,
        dbname=args.dbname,
        user=args.user,
        max_depth=args.max_depth,
        latest_limit=args.latest_limit,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
