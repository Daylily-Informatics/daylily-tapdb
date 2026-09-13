#!/usr/bin/env python3
"""Explicit manual additive SQL application after native fence release.

This is not native full-preservation migration acceptance. The user waived
another full database inventory. No native guard is modified or monkeypatched.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import runpy
import sys

from sqlalchemy import text
from daylily_tapdb.identity_inventory import seal_receipt, validate_receipt
from daylily_tapdb.migration_identity import write_json_receipt
from daylily_tapdb.runtime_principal import (
    operator_connection, bind_runtime_principal, set_runtime_identity_access,
)

ROOT = Path('/home/ubuntu/bloom_ops/tapdb101-20260911')
RECEIPTS = ROOT / 'receipts'
RUNTIME = Path('/home/ubuntu/.config/tapdb/bloom/bloom-day/tapdb-config.yaml')
MANUAL = RECEIPTS / 'identity-1015-manual-additive-result.json'
BIND = RECEIPTS / 'identity-1015-manual-runtime-bind-plan.json'
GRANT = RECEIPTS / 'identity-1015-manual-owner-3364-grant-plan.json'
MIGRATION = '20260913_082300_runtime_identity_authorization.sql'
ASSET_SHA256 = '59792b57c78d0471b40743c1a9974b0a131f43465ffc36aa848725d1a957bc11'
SCHEMA = 'tapdb_bloom_lsmcok1_local'
REASON = 'Restore authorization lookup for the existing Ursa token owner'

def require(condition, message):
    if not condition:
        raise ValueError(message)

def inputs():
    require(importlib.metadata.version('daylily-tapdb') == '10.1.5', 'Exact operator package 10.1.5 required')
    prep = runpy.run_path(str(ROOT / 'release/scripts/bloom_tapdb10_principal_prepare.py'))
    cfg, evidence = prep['_inputs'](argparse.Namespace(
        runtime_config=RUNTIME, operator_config=ROOT / 'operator.yaml',
        manifest=ROOT / 'release/config/migrations/bloom_tapdb_10_1_conversion.json'))
    require(cfg['config_path'] == str(RUNTIME), 'Runtime config identity changed')
    require(cfg['user'] == 'bloom_runtime_10' and cfg['operator_user'] == 'dayhoff', 'Principal mismatch')
    require(cfg['database'] == 'tapdb_bloom_prod' and cfg['schema_name'] == SCHEMA, 'Database/schema mismatch')
    require(cfg['owner_repo_name'] == 'bloom' and cfg['domain_code'] == 'M', 'Scope mismatch')
    require(not cfg['tenant_id'] and not cfg.get('additional_tenant_ids') and cfg['allow_global_claims'] is True, 'Tenant scope changed')
    return cfg, evidence

def apply_sql(args, cfg, evidence):
    require(args.confirm_bloom_stopped, 'Lead must affirm Bloom remains stopped')
    require(args.release_receipt is not None and args.release_receipt.is_absolute(), 'Exact native release receipt required')
    released = json.loads(args.release_receipt.read_text())
    validate_receipt(released, 'tapdb-sequence-apply/v1')
    require(released['phase'] == 'committed', 'Native allocator outcome must be committed')
    fence = released['writer_fence_release']
    validate_receipt(fence, 'tapdb-writer-fence/v1')
    require(fence['phase'] == 'released', 'Native original ACL release must be complete')
    require(fence['target']['database'] == 'tapdb_bloom_prod' and fence['target']['schema_name'] == SCHEMA, 'Wrong released target')
    require(fence['physical_target']['database_oid'] == 17040, 'Wrong physical database')
    distribution = importlib.metadata.distribution('daylily-tapdb')
    candidates = [item for item in distribution.files or [] if str(item).endswith('schema/runtime_identity_authorization.sql')]
    require(len(candidates) == 1, 'Exactly one packaged additive asset is required')
    asset_path = Path(distribution.locate_file(candidates[0])).resolve()
    asset = asset_path.read_bytes()
    require(hashlib.sha256(asset).hexdigest() == ASSET_SHA256, 'Published additive SQL digest mismatch')
    intent_path = MANUAL.with_name(MANUAL.stem + '.intent.json')
    require(not MANUAL.exists() and not intent_path.exists(), 'Manual operation already recorded; do not replay')
    intent = seal_receipt({
        'schema_version': 'tapdb-manual-additive-application/v1', 'status': 'intent',
        'operation': 'manual-packaged-additive-sql', 'native_full_preservation_acceptance': False,
        'authorization': 'User explicitly authorized the minimal workaround and waived another full database inventory',
        'database': 'tapdb_bloom_prod', 'database_oid': 17040, 'schema': SCHEMA,
        'operator': 'dayhoff', 'runtime_user': 'bloom_runtime_10',
        'migration_filename': MIGRATION, 'asset_path': str(asset_path), 'asset_sha256': ASSET_SHA256,
        'operator_package': '10.1.5', 'inputs': evidence,
        'native_release_receipt': str(args.release_receipt), 'native_release_sha256': released['sha256'],
        'bloom_stopped_affirmed_by_operator': True,
        'planned_effects': ['create exact private identity access table and its FK constraints',
            'enable/force RLS and operator-only policy on new table',
            'create exact authorization resolver with pinned search_path and PUBLIC execute revoked',
            'insert exact filename into native migration tracking in the same SQL transaction'],
        'created_at': datetime.now(timezone.utc).isoformat(),
    })
    write_json_receipt(intent_path, intent)
    with operator_connection(cfg, isolation_level='SERIALIZABLE') as connection:
        connection.execute(text('SET LOCAL search_path TO "tapdb_bloom_lsmcok1_local", pg_catalog, pg_temp'))
        existing = connection.execute(text('SELECT 1 FROM "tapdb_bloom_lsmcok1_local"._tapdb_migrations WHERE filename = :filename'), {'filename': MIGRATION}).first()
        require(existing is None, 'Migration filename already exists; do not replace tracking')
        # This exact asset has no includes or transaction control. Percent
        # escaping matches native migration SQL execution with psycopg2.
        connection.exec_driver_sql(asset.decode('utf-8').replace('%', '%%'))
        connection.execute(text('INSERT INTO "tapdb_bloom_lsmcok1_local"._tapdb_migrations (filename) VALUES (:filename)'), {'filename': MIGRATION})
    result = seal_receipt({**{key: value for key, value in intent.items() if key != 'sha256'},
        'status': 'committed', 'intent_sha256': intent['sha256'],
        'committed_at': datetime.now(timezone.utc).isoformat()})
    write_json_receipt(MANUAL, result)
    return MANUAL, 'committed'

def principal_phase(args, cfg, evidence):
    manual = json.loads(MANUAL.read_text())
    validate_receipt(manual, 'tapdb-manual-additive-application/v1')
    require(manual['status'] == 'committed' and manual['asset_sha256'] == ASSET_SHA256 and manual['inputs'] == evidence, 'Manual application/input evidence mismatch')
    applying = args.phase.endswith('-apply')
    receipt = BIND if args.phase.startswith('bind-') else GRANT
    context = receipt.with_name(receipt.stem + '.inputs.json')
    if applying:
        require(json.loads(context.read_text()) == evidence, 'Protected input references changed')
    else:
        require(not context.exists(), 'Plan input evidence already exists')
    if args.phase.startswith('bind-'):
        native = bind_runtime_principal(cfg, apply=applying, receipt_path=receipt)
    else:
        bound = json.loads(BIND.with_name(BIND.stem + '.result.json').read_text())
        require(bound['status'] == 'applied' and bound['target']['config_path'] == str(RUNTIME), 'Fresh runtime binding required')
        native = set_runtime_identity_access(cfg, user_uid=3364, user_euid='M-SYS-9SX2', enabled=True,
            reason=REASON, receipt_path=receipt, apply=applying)
        require(native['identity']['role'] == 'ADMIN' and native['identity']['is_active'] is True, 'Persisted owner authorization changed')
    if not applying:
        with context.open('x') as handle:
            json.dump(evidence, handle, sort_keys=True, indent=2)
            handle.write('\n')
    return receipt.with_name(receipt.stem + '.result.json') if applying else receipt, native['status']

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['apply-sql', 'bind-plan', 'bind-apply', 'grant-plan', 'grant-apply'])
    parser.add_argument('--release-receipt', type=Path)
    parser.add_argument('--confirm-bloom-stopped', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    cfg, evidence = inputs()
    receipt, status = apply_sql(args, cfg, evidence) if args.phase == 'apply-sql' else principal_phase(args, cfg, evidence)
    print(json.dumps({'phase': args.phase, 'status': status, 'receipt': str(receipt)}))

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'status': 'stopped', 'error_class': type(exc).__name__}), file=sys.stderr)
        raise SystemExit(1)
