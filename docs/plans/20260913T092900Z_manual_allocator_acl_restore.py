#!/usr/bin/env python3
"""Manual monotonic floor/ACL restoration from trusted completed quarantine.

No full database inventory; no native epoch closure is claimed or forged.
"""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
from sqlalchemy import text
from daylily_tapdb.cli.db_config import get_db_config
from daylily_tapdb.runtime_principal import operator_session
from daylily_tapdb.sequences import build_sequence_advance_plan, sequence_next_value
from daylily_tapdb.sequence_fence import control_state, acl_snapshot, restore_acl, lock_session
from daylily_tapdb.identity_inventory import seal_receipt, quote_identifier
from daylily_tapdb.migration_identity import write_json_receipt
from daylily_tapdb.backup.receipts import Actor, write_receipt

ROOT = Path('/home/ubuntu/bloom_ops/tapdb101-20260911')
RECEIPTS = ROOT / 'receipts'
QPATH = RECEIPTS / 'identity-1015-quarantine.json'
RESULT = RECEIPTS / 'identity-1015-manual-acl-result.json'
SCHEMA = 'tapdb_bloom_lsmcok1_local'

def require(ok, message):
    if not ok:
        raise ValueError(message)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--confirm-bloom-stopped', action='store_true', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    require(args.confirm_bloom_stopped, 'Bloom must remain stopped')
    q = json.loads(QPATH.read_text())
    require(q['phase'] == 'quarantined' and q['schema_version'] == 'tapdb-writer-fence-takeover/v1', 'Completed quarantine required')
    target = q['target']
    require(target['database'] == 'tapdb_bloom_prod' and target['schema_name'] == SCHEMA, 'Target mismatch')
    require(q['physical_target']['database_oid'] == 17040 and q['operator_role'] == 'dayhoff', 'Physical/operator mismatch')
    inventory = q['sequence_inventory']
    plan = build_sequence_advance_plan(inventory, floors=q['retained_floors'])
    expected = {item['name']: item for item in inventory['sequences']}
    planned = {item['name']: item['next_value'] for item in plan['advances']}
    intended = [name for name in planned if planned[name] > sequence_next_value(expected[name])]
    require(len(expected) == 66 and len(intended) == 56, 'Reviewed offline floor set changed')
    require(all(planned[name] == sequence_next_value(expected[name]) + 1 for name in intended), 'Reviewed +1 floor changes changed')
    require(not RESULT.exists(), 'Manual result already exists; do not replay')
    intent_path = RESULT.with_name(RESULT.stem + '.intent.json')
    require(not intent_path.exists(), 'Manual intent already exists; inspect before retry')
    cfg = get_db_config(config_path=ROOT / 'operator.yaml', client_id='bloom', database_name='bloom-day')
    control_cfg = get_db_config(config_path=ROOT / 'control-operator.yaml')
    require(cfg['database'] == target['database'] and cfg['schema_name'] == SCHEMA and cfg['operator_user'] == 'dayhoff', 'Operator config mismatch')
    intent = seal_receipt({
        'schema_version': 'tapdb-manual-acl-restoration/v1', 'status': 'intent',
        'target': target, 'physical_target': q['physical_target'], 'operator': 'dayhoff',
        'quarantine_receipt': str(QPATH), 'quarantine_sha256': q['sha256'],
        'native_epoch_intent_receipt_id': q['intent_receipt_id'], 'native_epoch_reconciled': False,
        'native_full_preservation_acceptance': False,
        'authorization': 'User authorized trust of completed quarantine, bounded monotonic floor repair and exact ACL restoration without another full inventory',
        'reviewed_required_changes': {name: planned[name] for name in intended},
        'original_acl': q['original_acl'], 'created_at': datetime.now(timezone.utc).isoformat(),
    })
    write_json_receipt(intent_path, intent)
    actor = Actor(surface='cli', username='dayhoff')
    journal_intent = write_receipt(ROOT / 'journal', operation='manual_allocator_acl_restoration', status='intent', actor=actor, detail=intent)
    actual_changes = []
    with operator_session(control_cfg, isolation_level='SERIALIZABLE') as control:
        with control.begin():
            state = control_state(control, target)
            require(state['database_oid'] == 17040 and state['operator_role'] == state['owner_role'] == 'dayhoff', 'Control identity mismatch')
            require(state['datallowconn'] is True, 'Expected open operator quarantine')
            lock_session(control, state['database_oid'])
            require(acl_snapshot(control,17040)['entries'] == q['quarantine_acl']['entries'], 'Quarantine ACL changed')
        with operator_session(cfg, isolation_level='SERIALIZABLE') as connection:
            with connection.begin():
                require(connection.execute(text('SELECT oid::bigint FROM pg_database WHERE datname=current_database()')).scalar_one() == 17040, 'Wrong live physical target')
                # Only catalog/sequence-state reads. No generic object table is read.
                catalog = {row['name']: dict(row) for row in connection.execute(text("SELECT c.oid::bigint AS oid,c.relname AS name,pg_get_userbyid(c.relowner) AS owner,s.seqincrement AS increment_by,s.seqmin AS min_value,s.seqmax AS max_value,s.seqstart AS start_value,s.seqcache AS cache_size,s.seqcycle AS cycle FROM pg_sequence s JOIN pg_class c ON c.oid=s.seqrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema"), {'schema':SCHEMA}).mappings()}
                require(set(catalog) == set(expected), 'Sequence names changed')
                other_clients = connection.execute(text("SELECT count(*) FROM pg_stat_activity WHERE datid=:oid AND backend_type='client backend' AND pid<>pg_backend_pid()"), {'oid':17040}).scalar_one()
                require(other_clients == 0, 'Another target client is connected')
                for name in sorted(expected):
                    old = expected[name]
                    current = catalog[name]
                    require(all(current[field] == old[field] for field in ('owner','increment_by','min_value','max_value','start_value','cache_size','cycle')), 'Sequence definition or owner changed')
                    qualified = quote_identifier(SCHEMA) + '.' + quote_identifier(name)
                    before = dict(connection.execute(text('SELECT last_value,is_called FROM ' + qualified)).mappings().one())
                    current_next = sequence_next_value({**old, **before})
                    new_next = max(current_next, planned[name])
                    if new_next > current_next:
                        connection.execute(text('ALTER SEQUENCE ' + qualified + ' RESTART WITH ' + str(int(new_next))))
                        actual_changes.append({'name':name,'oid':current['oid'],'from_next':current_next,'to_next':new_next})
                    after = dict(connection.execute(text('SELECT last_value,is_called FROM ' + qualified)).mappings().one())
                    require(sequence_next_value({**old,**after}) >= new_next, 'Sequence floor not retained')
                    require(connection.execute(text('SELECT to_regclass(:name)::oid::bigint'), {'name':qualified}).scalar_one() == current['oid'], 'Sequence OID changed')
        # ALTER SEQUENCE RESTART committed above; never lower even if live state
        # advanced beyond the trusted snapshot. Restore only original CONNECT ACL.
        with control.begin():
            state = control_state(control, target)
            require(state['database_oid'] == 17040 and state['datallowconn'] is True, 'Control gate changed')
            require(acl_snapshot(control,17040)['entries'] == q['quarantine_acl']['entries'], 'Quarantine ACL changed before restoration')
            restore_acl(control, state=state, original_acl=q['original_acl'])
    result = seal_receipt({**{key:value for key,value in intent.items() if key != 'sha256'},
        'status':'committed','intent_sha256':intent['sha256'],
        'manual_journal_intent_receipt_id':journal_intent.receipt_id,
        'allocator_floors_satisfied':True,'original_acl_restored':True,
        'actual_sequence_changes':actual_changes,
        'committed_at':datetime.now(timezone.utc).isoformat()})
    write_receipt(ROOT / 'journal', operation='manual_allocator_acl_restoration', status='committed', actor=actor, detail=result)
    write_json_receipt(RESULT,result)
    print(json.dumps({'status':'committed','receipt':str(RESULT),'sequence_changes':len(actual_changes),'native_epoch_reconciled':False}))

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'status':'stopped','error_class':type(exc).__name__}),file=sys.stderr)
        raise SystemExit(1)
