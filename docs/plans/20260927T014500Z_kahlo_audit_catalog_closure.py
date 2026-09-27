"""Root-run, read-only Kahlo 10.1.7 audit catalog/migration closure capture.

No audit row query, inventory capture, preflight, migration, role, fence or apply.
The exact prior helper supplies reviewed host/config/package guards only.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import sys

from sqlalchemy import text
from daylily_tapdb.identity_inventory import (
    _metadata, catalog_capture_context, catalog_tables, physical_target,
)
from daylily_tapdb.migration_identity import (
    _apply_operator_context, _migration_assets, _tracking_rows, write_json_receipt,
)
from daylily_tapdb.runtime_principal import operator_connection

ROOT = Path('/home/ubuntu/atlas-globaldag-20260926-operator')
GUARD_SHA256 = 'f00b40c13e6da6169747c6bca90b78b0b199b889a0453759e5e4351c338623cc'


def require(ok, message):
    if not ok:
        raise ValueError(message)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--inventory-helper', required=True, type=Path)
    parser.add_argument('--receipt-name', required=True)
    args = parser.parse_args()
    out = None
    try:
        os.umask(0o077)
        helper = args.inventory_helper
        require(helper.is_absolute() and helper.is_file() and not helper.is_symlink(), 'Exact regular guard helper required')
        require(helper.resolve().is_relative_to(ROOT), 'Guard helper must be staged under the exact operator root')
        require(hashlib.sha256(helper.read_bytes()).hexdigest() == GUARD_SHA256, 'Guard helper hash mismatch')
        spec = importlib.util.spec_from_file_location('reviewed_inventory_guards', helper)
        require(spec is not None and spec.loader is not None, 'Guard helper loader unavailable')
        guards = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(guards)
        guards.host_guard()
        require(re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,95}', args.receipt_name) is not None, 'Exact new receipt name required')
        parent = ROOT / 'kahlo' / 'receipts'
        require(parent.is_dir() and not parent.is_symlink() and parent.stat().st_mode & 0o077 == 0, 'Private receipt parent required')
        candidate = parent / args.receipt_name
        candidate.mkdir(mode=0o700)
        out = candidate  # Never write a failure into a pre-existing receipt directory.
        cfg, target, source_version, inputs = guards.configured_target('kahlo', None)
        assets = _migration_assets(guards.packaged_migrations())
        require(len(assets) <= 128, 'Unexpected migration asset count')
        require(sum(len(a['expanded_source'].encode()) for a in assets) <= 8 * 1024 * 1024, 'Unexpected expanded migration bytes')
        with operator_connection(cfg, isolation_level='REPEATABLE READ', read_only=True) as connection:
            require(connection.execute(text('SHOW transaction_read_only')).scalar_one() == 'on', 'Read-only transaction required')
            connection.execute(text("SET LOCAL statement_timeout = '30s'"))
            connection.execute(text("SET LOCAL lock_timeout = '3s'"))
            _apply_operator_context(connection, target)
            tracking = _tracking_rows(connection)
            require(len(tracking) <= 512, 'Unexpected migration tracking count')
            applied = [row['filename'] for row in tracking]
            require(len(applied) == len(set(applied)), 'Duplicate applied migration filenames')
            with catalog_capture_context(connection, schema_name=cfg['schema_name']):
                tables = catalog_tables(connection, cfg['schema_name'])
                matches = [table for table in tables if table['name'] == 'audit_log']
                require(len(matches) == 1, 'Exactly one physical audit relation required')
                audit = _metadata(connection, matches[0])
                policies = [dict(row) for row in connection.execute(text(
                    'SELECT p.polname AS name, ARRAY(SELECT CASE WHEN r.role_oid=0 THEN '\
                    "'PUBLIC' ELSE pg_catalog.pg_get_userbyid(r.role_oid)::text END "\
                    'FROM unnest(p.polroles) AS r(role_oid) ORDER BY r.role_oid) AS roles '\
                    'FROM pg_catalog.pg_policy p WHERE p.polrelid=:oid ORDER BY p.polname'
                ), {'oid': matches[0]['oid']}).mappings()]
                roles_by_name = {row['name']: row['roles'] for row in policies}
                require(set(roles_by_name) == {row['name'] for row in audit['policies']}, 'Audit policy catalog mismatch')
                for policy in audit['policies']:
                    policy['roles'] = roles_by_name[policy['name']]
                physical = physical_target(connection, target)
        known = {asset['filename'] for asset in assets}
        receipt = {
            'contract': 'kahlo.audit-catalog-migration-closure/v1',
            'captured_at': datetime.now(timezone.utc).isoformat(),
            'operator_version': '10.1.7', 'source_version': source_version,
            'source_version_evidence': 'operator_declared',
            'database_mutations': False, 'audit_rows_read': False,
            'target': target, 'physical_target': physical, 'inputs': inputs,
            'applied_migrations': tracking,
            'applied_not_in_package': sorted(set(applied) - known),
            'pending_migrations': [a['filename'] for a in assets if a['filename'] not in applied],
            'packaged_assets': [{k: v for k, v in a.items() if k != 'path'} for a in assets],
            'audit_catalog': audit,
            'table_catalog': [{k: v for k, v in table.items() if k != 'oid'} for table in tables],
            'guard_helper_sha256': GUARD_SHA256,
        }
        path = out / 'audit-catalog-closure.json'
        write_json_receipt(path, receipt)
        print(json.dumps({'status': 'captured', 'receipt_path': str(path),
                          'receipt_sha256': guards.file_hash(path),
                          'applied_count': len(applied),
                          'pending_count': len(receipt['pending_migrations']),
                          'unmatched_applied_count': len(receipt['applied_not_in_package']),
                          'audit_rows_read': False, 'database_mutations': False}, sort_keys=True))
        return 0
    except Exception as exc:
        failure = {'status': 'failed', 'error_type': type(exc).__name__,
                   'database_mutations': False, 'audit_rows_read': False}
        if out is not None:
            write_json_receipt(out / 'failure.json', failure)
        print(json.dumps(failure | {'receipt_directory': str(out) if out else None}), file=sys.stderr)
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
