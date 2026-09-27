"""Small pure contract checks; no database, network or container dependency."""
from copy import deepcopy
from dataclasses import asdict
import json
from pathlib import Path

import pytest

from daylily_tapdb import audit_inventory as audit
from daylily_tapdb.identity_inventory import (
    IdentityInventoryError, InventoryLimits, canonical_json,
    seal_receipt, verify_identity_inventory, with_inventory_limits,
)
from daylily_tapdb.migration_identity import _migration_assets, _migration_tables, _validate_scope_and_sequences


def table(rows, *, final=False):
    digest = audit.AuditDigest()
    for row in rows:
        value = row['changed_by']
        digest.add(json.dumps(row), value is None or value.strip(' ') == '')
    summary = digest.finish()
    return dict(kind='r', owner='operator', is_partition=False,
                rls_enabled=final, rls_forced=final,
                columns=[dict(name='uid', data_type='bigint', nullable=False),
                         dict(name='changed_by', data_type='text', nullable=not final)],
                constraints=[], indexes=[], triggers=[], dependencies=[],
                primary_key=['uid'], immutable_columns=['uid'],
                policies=audit._policies('source_schema', 'operator') if final else [],
                audit_digest=summary, row_count=summary['row_count'], content_sha256=summary['raw_sha256'])


def rows():
    return [dict(uid=i, changed_by=value, domain_code='domain', issuer_app_code='owner',
                 is_deleted=False, payload={'unchanged': i})
            for i, value in enumerate((None, '', '   ', 'existing actor', '\t'), 1)]


def transformed(source):
    return [dict(row, changed_by=audit.REPLACEMENT)
            if row['changed_by'] is None or row['changed_by'].strip(' ') == '' else dict(row)
            for row in source]


def receipt(audit_table):
    summary = audit_table['audit_digest']
    return seal_receipt(dict(schema_version=audit.VERSION, inventory_mode=audit.MODE,
        schema_name='source_schema', target={'target': 'same'}, physical_target={'database': 'same'},
        limits=asdict(InventoryLimits()),
        usage={'rows': summary['row_count'], 'evidence_bytes': len(canonical_json(summary).encode()),
               'largest_source_row_bytes': summary['largest_source_row_bytes']},
        tables={'audit_log': audit_table}))


def test_exact_attribution_digest_preserves_space_tab_and_all_other_values():
    before, after = table(rows()), table(transformed(rows()), final=True)
    audit.compare(before, after, schema='source_schema', transform=True)
    assert before['audit_digest']['transformed_rows'] == 3
    assert after['audit_digest']['transformed_rows'] == 0
    for bad in (rows(), transformed(rows())[:-1], transformed(rows()) + [dict(rows()[0], uid=6)]):
        with pytest.raises(IdentityInventoryError):
            audit.compare(before, table(bad, final=True), schema='source_schema', transform=True)
    bad = transformed(rows())
    bad[3]['payload'] = {'unexpected': 'mutation'}
    with pytest.raises(IdentityInventoryError):
        audit.compare(before, table(bad, final=True), schema='source_schema', transform=True)


def test_digest_order_duplicates_and_unapproved_catalog_changes_fail():
    with pytest.raises(IdentityInventoryError):
        table(list(reversed(rows())))
    with pytest.raises(IdentityInventoryError):
        table([rows()[0], rows()[0]])
    before, after = table(rows()), table(transformed(rows()), final=True)
    for bad in ('owner', 'policy_role', 'extra_dependency', 'column'):
        changed = deepcopy(after)
        if bad == 'owner':
            changed['owner'] = 'other'
        elif bad == 'policy_role':
            changed['policies'][1]['roles'] = ['PUBLIC']
        elif bad == 'extra_dependency':
            changed['dependencies'].append({'kind': 'n', 'dependent': 'unexpected', 'referenced': 'unexpected'})
        else:
            changed['columns'][0]['nullable'] = True
        with pytest.raises(IdentityInventoryError):
            audit.compare(before, changed, schema='source_schema', transform=True)


def test_native_shared_verifier_and_no_op_mode_propagation():
    before, after = receipt(table(rows())), receipt(table(transformed(rows()), final=True))
    assert verify_identity_inventory(before, before)['ok']
    conversion = {'schema_version': 'tapdb-identity-conversion/v1',
                  'tables': {'audit_log': {'audit_contract': audit.TRANSFORMATION}}, 'added_tables': []}
    assert verify_identity_inventory(before, after, conversion_manifest=conversion)['ok']
    with pytest.raises(IdentityInventoryError):
        verify_identity_inventory(before, after)
    retained = with_inventory_limits({}, before)
    assert retained['inventory_mode'] == audit.MODE
    with pytest.raises(IdentityInventoryError):
        with_inventory_limits({'inventory_mode': 'other'}, before)
    wrong = seal_receipt(dict(after, inventory_mode=None))
    with pytest.raises(IdentityInventoryError):
        verify_identity_inventory(before, wrong)


def test_migration_projection_retains_scope_checks_without_audit_rows():
    evidence = receipt(table(rows()))
    projected = _migration_tables(evidence)
    assert 'rows' not in projected['audit_log']
    _validate_scope_and_sequences({'tables': projected}, validate_generators=False)
    projected['audit_log']['audit_digest']['invalid_scope_rows'] = 1
    with pytest.raises(Exception, match='missing domain/owner'):
        _validate_scope_and_sequences({'tables': projected}, validate_generators=False)


def test_compact_usage_and_evidence_shape_are_checked():
    original = receipt(table(rows()))
    for mutation in ('row_count', 'source_bytes', 'rows'):
        altered = deepcopy(original)
        if mutation == 'rows':
            altered['tables']['audit_log']['rows'] = {}
        else:
            altered['tables']['audit_log']['audit_digest'][mutation] += 1
        altered = seal_receipt(altered)
        with pytest.raises(IdentityInventoryError):
            verify_identity_inventory(original, altered)


def test_only_exact_reviewed_expanded_asset_closure_is_admitted():
    assets = _migration_assets(Path(__file__).resolve().parents[1] / 'schema/migrations')
    pending = [a for a in assets if a['filename'] in audit.REVIEWED_ASSETS]
    assert audit.validate_pending(pending)
    assert not audit.validate_pending([])
    with pytest.raises(IdentityInventoryError):
        audit.validate_pending(pending[:-1])
    pending[0]['sha256'] = '0' * 64
    with pytest.raises(IdentityInventoryError):
        audit.validate_pending(pending)


def test_cli_source_current_postflight_and_recovery_keep_explicit_mode(monkeypatch, tmp_path):
    """Exercise real receipt plumbing; substitute only database/journal IO."""
    from types import SimpleNamespace
    from daylily_tapdb import identity_inventory as identity, migration_identity as migration, sequences
    from daylily_tapdb.backup import recovery, source_contract
    from daylily_tapdb.cli import identity as cli
    from tests.test_recovery_floor import TARGET, inventory as sequence_fixture

    seq = sequence_fixture()
    before = receipt(table(rows()))
    before = seal_receipt(dict(before, target=TARGET, schema_name=TARGET['schema_name'],
                               physical_target=seq['physical_target']))
    after_table = table(transformed(rows()), final=True)
    after_table['policies'] = audit._policies(TARGET['schema_name'], 'operator')
    after = seal_receipt(dict(receipt(after_table), target=TARGET,
                              schema_name=TARGET['schema_name'], physical_target=seq['physical_target']))
    cfg = dict(TARGET, config_path=TARGET['config_identity'], inventory_mode=audit.MODE,
               inventory_limits=before['limits'])
    monkeypatch.setattr(cli, 'get_db_config', lambda: cfg)
    _, target = cli._resolve()
    assert target['inventory_mode'] == audit.MODE
    current = before
    captures = []
    def capture(*args, **kwargs):
        assert kwargs['target']['inventory_mode'] == audit.MODE
        captures.append(kwargs['target']['inventory_mode'])
        return current
    monkeypatch.setattr(identity, 'capture_identity_inventory', capture)
    monkeypatch.setattr(sequences, 'capture_sequence_inventory', lambda *a, **k: seq)
    source = source_contract.capture_source_contract(object(), schema_name=TARGET['schema_name'],
                                                     target=target, source_version='9.0.9')
    source_contract.validate_source_contract(source)
    directory = Path(__file__).resolve().parents[1] / 'schema/migrations'
    assets = _migration_assets(directory)
    applied = [{'filename': a['filename']} for a in assets if a['filename'] not in audit.REVIEWED_ASSETS]
    monkeypatch.setattr(migration, '_tracking_rows', lambda connection: applied)
    monkeypatch.setattr(migration, '_apply_operator_context', lambda *a: None)
    connection = SimpleNamespace(execute=lambda *a, **k: SimpleNamespace(scalar_one=lambda: TARGET['schema_name']))
    preflight = migration.build_migration_preflight(connection, migrations_dir=directory,
        target=target, source_contract=source, _reviewed_recovery={})
    # apply's current recapture reconstructs the mode from the source receipt.
    checked = migration.build_migration_preflight(connection, migrations_dir=directory,
        target=dict(TARGET), source_contract=source, _reviewed_recovery={})
    assert migration._receipt_comparable(preflight) == migration._receipt_comparable(checked)
    current = after
    applied = [{'filename': a['filename']} for a in assets]
    for _ in ('interim', 'postflight'):
        postflight = migration.build_migration_preflight(connection, migrations_dir=directory,
            target=target, _reviewed_recovery={})
        migration._verify_physical_preservation(preflight, postflight, {}, set())
    monkeypatch.setattr(sequences, 'validate_writer_fence', lambda *a, **k: None)
    monkeypatch.setattr(sequences, 'record_sequence_advance_outcome', lambda *a, **k: {})
    monkeypatch.setattr(recovery, 'finish_recovery', lambda *a, **k: SimpleNamespace(receipt_id='receipt', checksum=lambda: 'checksum'))
    result = migration.MigrationResult(dict(target=TARGET, postflight=postflight,
                                           sequence_result={}, recovery_intent={}))
    assert migration.finalize_migration_recovery(connection, result, receipts_dir=tmp_path,
                                                  writer_fence={})['status'] == 'committed'
    assert len(captures) == 8
