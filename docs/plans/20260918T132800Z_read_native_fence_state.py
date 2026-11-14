"""Read exact existing Bloom fence authority; no lock/fence/reconcile/apply."""
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path

from sqlalchemy import text

from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.migration_identity import write_json_receipt
from daylily_tapdb.runtime_principal import operator_connection
from daylily_tapdb.sequence_fence import active_epoch, acl_snapshot, census, control_state, read_fence_history
from daylily_tapdb.sequences import SequenceProtectionError


def main():
    ops = Path('/home/ubuntu/bloom_ops/tapdb101-20260911')
    output = Path('/home/ubuntu/bloom_ops/kahlo-resolver-20260918T123300Z/fence-owner-read.json')
    if output.exists():
        raise ValueError('Create-exclusive owner receipt already exists')
    family = json.loads((ops / 'receipts/recovery-family.json').read_text())
    target = family['origin']['target']
    if target['database'] != 'tapdb_bloom_prod' or target['schema_name'] != 'tapdb_bloom_lsmcok1_local':
        raise ValueError('Unexpected persisted family target')
    started = datetime.now(UTC).isoformat()
    history, head = read_fence_history(ops / 'journal')
    epoch = active_epoch(history, target)
    if epoch is None or epoch.receipt_id != '000033-20260913T090834Z':
        raise ValueError('Exact unresolved fence epoch changed since review')
    cfg = get_db_config(config_path=ops / 'control-operator.yaml')
    result = {'schema': 'bloom.native-fence-owner-read/1', 'read_only': True,
              'started_at': started, 'family_id': family['family_id'], 'journal_head': head,
              'target': target, 'active_epoch': {'receipt_id': epoch.receipt_id,
                  'phase': epoch.detail['phase'], 'status': epoch.status},
              'fence_events': [{'receipt_id': r.receipt_id, 'phase': r.detail.get('phase'),
                                'status': r.status}
                               for r in history if r.operation == 'sequence_writer_fence'],
              'applied': False}
    with operator_connection(cfg, isolation_level='REPEATABLE READ', read_only=True) as connection:
        try:
            read_only = connection.execute(text('SHOW transaction_read_only')).scalar_one()
            if read_only != 'on':
                raise ValueError('Control transaction is not read-only')
            result['transaction_read_only'] = read_only
            state = control_state(connection, target)
            acl = acl_snapshot(connection, state['database_oid'])
            result['control_state'] = state
            result['acl_matches_original_epoch'] = acl['entries'] == epoch.detail['original_acl']['entries']
            result['acl_sha256'] = hashlib.sha256(json.dumps(acl, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
            result['bloom_runtime_has_direct_connect'] = any(
                r['grantee_name'] == 'bloom_runtime_10' and r['privilege_type'] == 'CONNECT'
                for r in acl['entries'])
            result['public_has_direct_connect'] = any(
                r['grantee_name'] == 'PUBLIC' and r['privilege_type'] == 'CONNECT'
                for r in acl['entries'])
            result['postgres_timeouts'] = {name: connection.execute(text('SELECT current_setting(:name)'), {'name': name}).scalar_one()
                for name in ('statement_timeout', 'lock_timeout', 'idle_in_transaction_session_timeout')}
            try:
                census(connection, database=target['database'], database_oid=state['database_oid'])
                result['native_census'] = {'clear': True}
            except SequenceProtectionError as exc:
                result['native_census'] = {'clear': False, 'reason': str(exc)}
        finally:
            connection.rollback()
            result['rolled_back'] = True
    if read_fence_history(ops / 'journal')[1] != head:
        raise ValueError('Fence journal changed during owner read')
    result['completed_at'] = datetime.now(UTC).isoformat()
    write_json_receipt(output, result)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
