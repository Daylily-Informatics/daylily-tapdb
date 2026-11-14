#!/usr/bin/env python3
"""Operator-run native bind/grant phases; never stores or prints credentials."""
import argparse
import importlib.metadata
import json
import os
from pathlib import Path
import runpy

from daylily_tapdb.runtime_principal import bind_runtime_principal, set_runtime_identity_access

ROOT = Path('/home/ubuntu/bloom_ops/tapdb101-20260911')
RUNTIME = Path('/home/ubuntu/.config/tapdb/bloom/bloom-day/tapdb-config.yaml')
RECEIPTS = ROOT / 'receipts'
BIND = RECEIPTS / 'identity-1014-runtime-bind-plan.json'
GRANT = RECEIPTS / 'identity-1014-owner-3364-grant-plan.json'
REASON = 'Restore authorization lookup for the existing Ursa token owner'

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('phase', choices=['bind-plan', 'bind-apply', 'grant-plan', 'grant-apply'])
    args = parser.parse_args()
    os.umask(0o077)
    assert importlib.metadata.version('daylily-tapdb') == '10.1.4', 'Wrong native release'
    result = json.loads((RECEIPTS / 'schema-identity-1014-result.json').read_text())
    assert result['principal_binding_required'] is True, 'Migration completion receipt required'
    # Reuse the existing owning helper's reviewed runtime/operator input checks.
    # Its CLI version gate is 10.1.3; no old native operations are invoked here.
    preparation = runpy.run_path(str(ROOT / 'release/scripts/bloom_tapdb10_principal_prepare.py'))
    cfg, evidence = preparation['_inputs'](argparse.Namespace(
        runtime_config=RUNTIME,
        operator_config=ROOT / 'operator.yaml',
        manifest=ROOT / 'release/config/migrations/bloom_tapdb_10_1_conversion.json',
    ))
    assert cfg['config_path'] == str(RUNTIME)
    assert cfg['user'] == 'bloom_runtime_10' and cfg['operator_user'] == 'dayhoff'
    assert cfg['database'] == 'tapdb_bloom_prod' and cfg['schema_name'] == 'tapdb_bloom_lsmcok1_local'
    assert cfg['domain_code'] == 'M' and cfg['owner_repo_name'] == 'bloom'
    assert not cfg['tenant_id'] and not cfg.get('additional_tenant_ids') and cfg['allow_global_claims'] is True
    applying = args.phase.endswith('-apply')
    receipt = BIND if args.phase.startswith('bind-') else GRANT
    context = receipt.with_name(receipt.stem + '.inputs.json')
    if applying:
        assert json.loads(context.read_text()) == evidence, 'Protected input references changed'
    else:
        assert not context.exists(), 'Input evidence already exists'
    if args.phase.startswith('bind-'):
        native = bind_runtime_principal(cfg, apply=applying, receipt_path=receipt)
        if applying:
            assert native['status'] == 'applied' and native['runtime_temp_denied'] and native['privileges_verified']
    else:
        bound = json.loads(BIND.with_name(BIND.stem + '.result.json').read_text())
        assert bound['status'] == 'applied' and bound['target']['config_path'] == str(RUNTIME)
        native = set_runtime_identity_access(cfg, user_uid=3364, user_euid='M-SYS-9SX2',
            enabled=True, reason=REASON, receipt_path=receipt, apply=applying)
        identity = native['identity']
        assert identity['uid'] == 3364 and identity['euid'] == 'M-SYS-9SX2'
        assert identity['role'] == 'ADMIN' and identity['is_active'] is True
        assert identity['issuer_app_code'] == 'daylily-tapdb' and identity['tenant_id'] is None
        if applying:
            assert native['status'] == 'applied' and native['ordinary_scope_unchanged'] is True
    if not applying:
        with context.open('x') as handle:
            json.dump(evidence, handle, sort_keys=True, indent=2)
            handle.write('\n')
    output = receipt.with_name(receipt.stem + '.result.json') if applying else receipt
    print(json.dumps({'phase': args.phase, 'status': native['status'], 'receipt': str(output)}))

if __name__ == '__main__':
    main()
