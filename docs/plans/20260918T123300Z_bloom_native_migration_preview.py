"""Read-only native preview of the reviewed Bloom resolver migration.

Only public TapDB APIs are used. No apply, bind, grant, template, user, token,
or schema mutation is exposed. Candidate SQL is inspected, never executed.
"""
from datetime import UTC, datetime
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path

from sqlalchemy import text

from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.migration_identity import build_migration_preflight, write_json_receipt
from daylily_tapdb.runtime_principal import operator_connection

OPS = Path('/home/ubuntu/bloom_ops/tapdb101-20260911')
STAGE = Path('/home/ubuntu/bloom_ops/kahlo-resolver-20260918T123300Z')
NEW_MIGRATION = '20260918_070400_system_user_authorization_contract.sql'


def main():
    os.umask(0o077)
    started = datetime.now(UTC).isoformat()
    manifest = json.loads((STAGE / 'candidate.json').read_text())
    if manifest['base_commit'] != '1566aa345f5d85826b0637660fbd4dac21242e49':
        raise ValueError('Unexpected reviewed source base')
    actual = {str(p.relative_to(STAGE)): hashlib.sha256(p.read_bytes()).hexdigest()
              for p in (STAGE / 'schema').rglob('*.sql')}
    if actual != manifest['candidate_assets']:
        raise ValueError('Staged candidate assets differ from reviewed source')
    if importlib.metadata.version('daylily-tapdb') != '10.1.5':
        raise ValueError('Read-only preview requires the inspected native operator release')
    output = STAGE / 'native-preview.json'
    status_path = STAGE / 'preview-status.json'
    if output.exists() or status_path.exists():
        raise ValueError('Create-exclusive preview receipt already exists')
    cfg = get_db_config(config_path=OPS / 'operator.yaml', client_id='bloom', database_name='bloom-day')
    required = {'user': 'bloom_runtime_10', 'operator_user': 'dayhoff',
                'database': 'tapdb_bloom_prod', 'schema_name': 'tapdb_bloom_lsmcok1_local',
                'domain_code': 'M', 'owner_repo_name': 'bloom', 'tenant_id': '',
                'allow_global_claims': True}
    if any(cfg[key] != expected for key, expected in required.items()):
        raise ValueError('Explicit Bloom target differs from reviewed target')
    target = {key: cfg[key] for key in ('engine_type', 'host', 'port', 'database',
                                       'schema_name', 'domain_code', 'owner_repo_name')}
    target['config_identity'] = cfg['config_path']
    for key in ('server_port', 'inventory_limits'):
        if key in cfg:
            target[key] = cfg[key]
    mappings = OPS / 'receipts/source-sequence-mappings.json'
    family = OPS / 'receipts/recovery-family.json'
    target['sequence_mappings'] = json.loads(mappings.read_text())
    recovery_family = json.loads(family.read_text())
    status = {'schema': 'bloom.native-resolver-preview/1', 'started_at': started,
              'candidate_manifest_sha256': hashlib.sha256((STAGE / 'candidate.json').read_bytes()).hexdigest(),
              'candidate_base_commit': manifest['base_commit'], 'candidate_source': manifest['source_files'],
              'operator_package': '10.1.5', 'proposed_package': '10.1.6',
              'database': cfg['database'], 'schema_name': cfg['schema_name'],
              'runtime_user': cfg['user'], 'operator_user': cfg['operator_user'],
              'mappings_path': str(mappings), 'mappings_sha256': hashlib.sha256(mappings.read_bytes()).hexdigest(),
              'recovery_family_path': str(family), 'recovery_family_sha256': hashlib.sha256(family.read_bytes()).hexdigest(),
              'journal_path': str(OPS / 'journal'), 'applied': False}
    payload = None
    try:
        with operator_connection(cfg, isolation_level='REPEATABLE READ', read_only=True) as connection:
            try:
                read_only = connection.execute(text('SHOW transaction_read_only')).scalar_one()
                if read_only != 'on':
                    raise RuntimeError('Native operator transaction is not read-only')
                status['transaction_read_only'] = read_only
                payload = build_migration_preflight(
                    connection, migrations_dir=STAGE / 'schema/migrations', target=target,
                    receipts_dir=OPS / 'journal', recovery_family=recovery_family)
            finally:
                connection.rollback()
                status['rolled_back'] = True
        write_json_receipt(output, payload)
        pending = payload['pending_migrations']
        status.update(status='captured', native_receipt=str(output),
                      evidence_sha256=payload['evidence_sha256'], pending_migrations=pending,
                      only_requested_migration_pending=[p['filename'] for p in pending] == [NEW_MIGRATION],
                      recovery_pending_count=len(payload.get('allocator_recovery', {}).get('state', {}).get('pending', {})))
    except Exception as exc:
        status.update(status='blocked', error_type=type(exc).__name__)
        if type(exc).__module__.startswith('daylily_tapdb'):
            status['native_error'] = str(exc)
    status['completed_at'] = datetime.now(UTC).isoformat()
    write_json_receipt(status_path, status)
    print(json.dumps(status, sort_keys=True))
    return 0 if status['status'] == 'captured' else 1


if __name__ == '__main__':
    raise SystemExit(main())
