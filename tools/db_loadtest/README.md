# TAPDB DB Load Test Harness

This toolkit provides repeatable local DB load tests for TAPDB hot paths, with emphasis on:

- `generic_instance_lineage` write/read pressure
- `audit_log` latest-change read pressure

It includes:

- Synthetic data generator: `generate_synthetic_data.py`
- Custom `pgbench` scripts in `pgbench/`
- App-shaped mixed workload runner: `pgbench/run_app_shaped.sh`

## Prerequisites

1. Local PostgreSQL is running and reachable.
2. TAPDB schema is already applied and seeded.
3. `pgbench` is installed.
4. Python environment includes `psycopg2` (`daylily-tapdb` dependency already includes `psycopg2-binary`).

Quick checks:

```bash
pgbench --version
python tools/db_loadtest/generate_synthetic_data.py --help
```

If you need a fresh local TAPDB first:

```bash
mkdir -p "$HOME/.config/tapdb/tapdb/tapdb"
tapdb --config "$HOME/.config/tapdb/tapdb/tapdb/tapdb-config.yaml" db-config init \
  --client-id tapdb \
  --database-name tapdb \
  --owner-repo-name daylily-tapdb \
  --env dev \
  --domain-code dev=Z \
  --domain-registry-path "$PWD/daylily_tapdb/etc/domain_code_registry.json" \
  --prefix-ownership-registry-path "$PWD/daylily_tapdb/etc/prefix_ownership_registry.json" \
  --db-port dev=5533 \
  --ui-port dev=8911
tapdb --config "$HOME/.config/tapdb/tapdb/tapdb/tapdb-config.yaml" --env dev bootstrap local --no-gui
```

## 1) Generate Synthetic Data

Default connection target is `localhost:5533`, DB `tapdb_tapdb_dev`.

```bash
python tools/db_loadtest/generate_synthetic_data.py \
  --host localhost \
  --port 5533 \
  --dbname tapdb_tapdb_dev \
  --user "$USER" \
  --tenants 2 \
  --instances-per-tenant 1000 \
  --lineage-edges 2000 \
  --write-vars tools/db_loadtest/.loadtest.vars
```

Optional revision chains:

```bash
python tools/db_loadtest/generate_synthetic_data.py \
  --tenants 2 \
  --instances-per-tenant 1000 \
  --lineage-edges 2000 \
  --revision-chain-count 10 \
  --revision-chain-length 8
```

Notes:

- `--truncate-first` issues `DELETE` for prior loadtest rows. In TAPDB this becomes soft-delete (`is_deleted=true`) because of triggers.
- The generator inserts its own synthetic `generic/generic/loadtest_node/1.0` template row directly for benchmark purposes.
- Generator stores tenant markers in `json_addl.tenant_id` for compatibility; it does not currently populate native `tenant_id` UUID columns.
- Generator prints `template_uid`, `tenant_count`, `min_instance_uid`, `max_instance_uid` for use with `pgbench -D`.
- If you see many sequence-exists `NOTICE` lines from audit triggers, run with `PGOPTIONS='-c client_min_messages=warning'`.

## 2) Run Focused pgbench Workloads

If you wrote vars to file:

```bash
source tools/db_loadtest/.loadtest.vars
```

Common variable block:

```bash
VARS=(
  -D template_uid="$template_uid"
  -D tenant_count="$tenant_count"
  -D min_instance_uid="$min_instance_uid"
  -D max_instance_uid="$max_instance_uid"
  -D max_depth="${max_depth:-4}"
  -D latest_limit="${latest_limit:-50}"
)
```

### Insert instance

```bash
pgbench -h localhost -p 5533 -U "$USER" \
  -n -c 8 -j 4 -T 60 "${VARS[@]}" \
  -f tools/db_loadtest/pgbench/insert_instance.sql \
  tapdb_tapdb_dev
```

### Insert lineage edge

```bash
pgbench -h localhost -p 5533 -U "$USER" \
  -n -c 8 -j 4 -T 60 "${VARS[@]}" \
  -f tools/db_loadtest/pgbench/insert_lineage_edge.sql \
  tapdb_tapdb_dev
```

### Read graph (recursive CTE)

```bash
pgbench -h localhost -p 5533 -U "$USER" \
  -n -c 16 -j 4 -T 60 "${VARS[@]}" \
  -f tools/db_loadtest/pgbench/read_graph_recursive.sql \
  tapdb_tapdb_dev
```

### List latest revisions (from audit)

```bash
pgbench -h localhost -p 5533 -U "$USER" \
  -n -c 8 -j 4 -T 60 "${VARS[@]}" \
  -f tools/db_loadtest/pgbench/list_latest_revisions.sql \
  tapdb_tapdb_dev
```

## 3) Run App-Shaped Mixed Workload

Default weighted mix:

- `read_graph_recursive`: 50
- `list_latest_revisions`: 20
- `insert_lineage_edge`: 20
- `insert_instance`: 10

Run:

```bash
DB_HOST=localhost \
DB_PORT=5533 \
DB_NAME=tapdb_tapdb_dev \
DB_USER="$USER" \
TEMPLATE_UID="$template_uid" \
TENANT_COUNT="$tenant_count" \
MIN_INSTANCE_UID="$min_instance_uid" \
MAX_INSTANCE_UID="$max_instance_uid" \
MAX_DEPTH="${max_depth:-4}" \
LATEST_LIMIT="${latest_limit:-50}" \
CLIENTS=16 JOBS=4 DURATION=120 \
tools/db_loadtest/pgbench/run_app_shaped.sh
```

## 4) Recommended Scale Ramps

### Ramp 0 (smoke)

- `--tenants 2`
- `--instances-per-tenant 1000`
- `--lineage-edges 2000`
- `pgbench -T 60`

### Ramp 1 (dev laptop)

- `--tenants 5`
- `--instances-per-tenant 10000`
- `--lineage-edges 100000`
- `pgbench -T 180`

### Ramp 2 (stress)

- `--tenants 10`
- `--instances-per-tenant 50000`
- `--lineage-edges 1000000`
- `pgbench -T 300`

### Ramp 3 (soak)

- Same data size as Ramp 2
- Mixed workload for `20-30` minutes

## 5) Interpreting pgbench Output

`pgbench` outputs the key metrics needed for this harness:

- `latency average` (ms)
- `tps` excluding and including connect overhead

Track these across ramps to identify inflection points in:

- lineage insert throughput (`insert_lineage_edge.sql`)
- recursive graph traversal latency (`read_graph_recursive.sql`)
- audit lookup latency (`list_latest_revisions.sql`)

## 6) Inspect pg_stat_statements

Enable extension (requires proper DB privileges and often restart for preload setting):

```sql
ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';
-- restart postgres if needed
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
```

Reset stats before a run:

```sql
SELECT pg_stat_statements_reset();
```

Inspect top TAPDB-related statements after a run:

```sql
SELECT
  calls,
  total_exec_time,
  mean_exec_time,
  rows,
  LEFT(query, 200) AS query_prefix
FROM pg_stat_statements
WHERE query ILIKE '%generic_instance_lineage%'
   OR query ILIKE '%audit_log%'
ORDER BY total_exec_time DESC
LIMIT 25;
```

Useful companion checks:

```sql
SELECT relname, n_live_tup, n_dead_tup
FROM pg_stat_user_tables
WHERE relname IN ('generic_instance', 'generic_instance_lineage', 'audit_log')
ORDER BY relname;
```
