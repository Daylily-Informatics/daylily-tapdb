"""Explicit bounded audit evidence for the reviewed pre-10 operator migration.

This format never drops audit content. It replaces per-row retained dictionaries
with two ordered whole-row digests, one raw and one exact attribution projection.
No runtime or database schema contract changes are made here.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping

from sqlalchemy import text

from daylily_tapdb.identity_inventory import MAX_AUDIT_SOURCE_BYTES

MODE = 'audit_log_ordered_digest/v1'
VERSION = 'tapdb-identity-inventory/audit-digest-v1'
TRANSFORMATION = 'audit_log.changed_by:pg_trim_empty_to_pre92_unattributed/v1'
REPLACEMENT = 'migration:pre-9.2-unattributed'


def fail(message: str) -> None:
    from daylily_tapdb.identity_inventory import IdentityInventoryError
    raise IdentityInventoryError(message)


def validate_mode(value: Any) -> str:
    if value != MODE:
        fail('Unknown explicit inventory_mode')
    return MODE


class AuditDigest:
    """One row at a time; fixed-size hash state and counters only."""

    def __init__(self) -> None:
        self.raw = hashlib.sha256(b'tapdb.audit.whole-row.ordered/v1\0')
        self.expected = self.raw.copy()
        self.previous_uid: int | None = None
        self.count = self.transformed = self.source_bytes = self.largest = 0
        self.active = self.deleted = self.invalid_scope = 0

    def add(self, raw: str, replace: bool) -> None:
        from daylily_tapdb.identity_inventory import canonical_json
        values = json.loads(raw, parse_float=str)
        uid = values.get('uid')
        if type(uid) is not int or (self.previous_uid is not None and uid <= self.previous_uid):
            fail('Audit digest requires strictly increasing unique bigint uid')
        if type(replace) is not bool:
            fail('Audit attribution predicate must be a SQL boolean')
        self.previous_uid = uid
        self.count += 1
        size = len(raw.encode('utf-8'))
        self.source_bytes += size
        self.largest = max(self.largest, size)
        self.active += values.get('is_deleted') is False
        self.deleted += values.get('is_deleted') is True
        self.invalid_scope += any(not str(values.get(k) or '').strip() for k in ('domain_code', 'issuer_app_code'))
        # An audit row is bounded by capture(). Encode it in one pass with the
        # same canonical JSON options as content_hash; keep the v1 framing.
        row_hash = hashlib.sha256(canonical_json(values).encode('utf-8')).hexdigest()
        raw_frame = canonical_json([uid, row_hash]).encode('utf-8')
        self.raw.update(len(raw_frame).to_bytes(8, 'big') + raw_frame)
        expected_frame = raw_frame
        if replace:
            projected = dict(values, changed_by=REPLACEMENT)
            projected_hash = hashlib.sha256(canonical_json(projected).encode('utf-8')).hexdigest()
            expected_frame = canonical_json([uid, projected_hash]).encode('utf-8')
        self.expected.update(len(expected_frame).to_bytes(8, 'big') + expected_frame)
        self.transformed += replace

    def finish(self, *, max_source_bytes: int = MAX_AUDIT_SOURCE_BYTES) -> dict[str, Any]:
        digests = []
        for digest in (self.raw, self.expected):
            final = digest.copy()
            final.update(b'\0row-count\0' + self.count.to_bytes(8, 'big'))
            digests.append(final.hexdigest())
        return dict(mode=MODE, transformation=TRANSFORMATION,
                    raw_sha256=digests[0], expected_sha256=digests[1],
                    row_count=self.count, transformed_rows=self.transformed,
                    source_bytes=self.source_bytes, largest_source_row_bytes=self.largest,
                    active_count=self.active, soft_deleted_count=self.deleted,
                    invalid_scope_rows=self.invalid_scope, max_source_bytes=max_source_bytes)


def capture(connection, *, schema: str, entry: dict, limits, processed_rows: int, receipt_bytes: int) -> dict:
    from daylily_tapdb.identity_inventory import InventoryLimitExceededError, quote_identifier
    columns = {c['name']: c for c in entry['columns']}
    if (entry['kind'] != 'r' or entry['is_partition'] or entry['primary_key'] != ['uid']
            or columns.get('uid', {}).get('data_type') != 'bigint'
            or columns['uid']['nullable']
            or columns.get('changed_by', {}).get('data_type') != 'text'
            or not {'domain_code', 'issuer_app_code', 'is_deleted'} <= set(columns)):
        fail('Unsupported audit catalog for ordered digest')
    digest = AuditDigest()
    sql = text(f'SELECT to_jsonb(t)::text, (changed_by IS NULL OR trim(changed_by)=\'\') '
               f'FROM ONLY {quote_identifier(schema)}.audit_log t ORDER BY uid')
    cursor = connection.execute(sql, execution_options={'stream_results': True, 'yield_per': 64})
    try:
        for raw, replace in cursor:
            size = len(raw.encode('utf-8'))
            for name, attempted, cap in (
                ('max_rows', processed_rows + digest.count + 1, limits.max_rows),
                ('max_row_bytes', size, limits.max_row_bytes),
                ('max_audit_source_bytes', digest.source_bytes + size, limits.max_audit_source_bytes),
            ):
                if attempted > cap:
                    raise InventoryLimitExceededError(table='audit_log', processed_rows=processed_rows + digest.count,
                        accumulated_bytes=receipt_bytes, attempted_bytes=size, limit_name=name,
                        configured_value=cap, attempted_value=attempted)
            digest.add(raw, replace)
    finally:
        cursor.close()
    return digest.finish(max_source_bytes=limits.max_audit_source_bytes)


def validate_summary(table: Mapping[str, Any], *, max_source_bytes: int | None = None) -> None:
    summary = table.get('audit_digest')
    fields = {'mode', 'transformation', 'raw_sha256', 'expected_sha256', 'row_count',
              'transformed_rows', 'source_bytes', 'largest_source_row_bytes',
              'active_count', 'soft_deleted_count', 'invalid_scope_rows', 'max_source_bytes'}
    if not isinstance(summary, Mapping) or set(summary) != fields or 'rows' in table:
        fail('Invalid compact audit evidence shape')
    if summary['mode'] != MODE or summary['transformation'] != TRANSFORMATION:
        fail('Unknown compact audit algorithm/transformation')
    for key in fields - {'mode', 'transformation', 'raw_sha256', 'expected_sha256'}:
        if type(summary[key]) is not int or not 0 <= summary[key] <= 2**63 - 1:
            fail('Invalid compact audit counter')
    for key in ('raw_sha256', 'expected_sha256'):
        if not isinstance(summary[key], str) or not re.fullmatch('[a-f0-9]{64}', summary[key]):
            fail('Invalid compact audit digest')
    if (summary['max_source_bytes'] < 1
            or (max_source_bytes is not None and summary['max_source_bytes'] != max_source_bytes)
            or summary['source_bytes'] > summary['max_source_bytes']
            or summary['row_count'] != table['row_count']
            or summary['active_count'] + summary['soft_deleted_count'] != summary['row_count']
            or summary['transformed_rows'] > summary['row_count']
            or summary['invalid_scope_rows'] > summary['row_count']
            or summary['largest_source_row_bytes'] > summary['source_bytes']
            or table['content_sha256'] != summary['raw_sha256']
            or (summary['transformed_rows'] == 0 and summary['raw_sha256'] != summary['expected_sha256'])):
        fail('Inconsistent compact audit evidence')


def _policies(schema: str, owner: str) -> list[dict]:
    from daylily_tapdb.identity_inventory import quote_identifier
    # pg_get_expr under catalog_capture_context qualifies these schema functions.
    # Ordinary lower-case identifiers are deparsed without quotes by PostgreSQL.
    s = schema if re.fullmatch('[a-z_][a-z0-9_]*', schema) else quote_identifier(schema)
    domain = f'(domain_code = {s}.tapdb_current_domain_code())'
    app = f'(issuer_app_code = {s}.tapdb_current_owner_repo_name())'
    tenants = f'(tenant_id = ANY ({s}.tapdb_allowed_tenant_ids()))'
    using = f'({domain} AND {app} AND ((tenant_id IS NULL) OR {tenants}))'
    check = f'({domain} AND {app} AND ({tenants} OR ((tenant_id IS NULL) AND (({s}.tapdb_current_tenant_id() IS NULL) OR {s}.tapdb_allow_global_rows()))))'
    return [dict(name='audit_log_scope_isolation', command='*', permissive=True, using=using, with_check=check, roles=['PUBLIC']),
            dict(name='tapdb_operator_access', command='*', permissive=True, using='true', with_check='true', roles=[owner])]


def verify_catalog(before: Mapping, after: Mapping, *, schema: str, transform: bool) -> None:
    row_fields = {'audit_digest', 'row_count', 'content_sha256'}
    old = {k: v for k, v in before.items() if k not in row_fields}
    new = {k: v for k, v in after.items() if k not in row_fields}
    if not transform:
        if old != new:
            fail('Compact audit catalog changed without its exact contract')
        return
    if old['policies'] not in ([], _policies(schema, old['owner'])):
        fail('Unsupported original audit policies')
    expected = dict(old, rls_enabled=True, rls_forced=True,
                    policies=_policies(schema, old['owner']),
                    columns=[dict(c, nullable=False) if c['name'] == 'changed_by' else c for c in old['columns']])
    # Only dependencies of the two canonical policies may be added. Policy
    # definitions and roles are separately exact; all other dependencies stay exact.
    relation = f'{schema}.audit_log'
    allowed = {
        ('a', f'policy {p} on table {relation}', f'table {relation}')
        for p in ('audit_log_scope_isolation', 'tapdb_operator_access')
    } | {
        ('n', f'policy audit_log_scope_isolation on table {relation}', f'column {c} of table {relation}')
        for c in ('domain_code', 'issuer_app_code', 'tenant_id')
    }
    def stripped(items):
        return [x for x in items if (x['kind'], x['dependent'], x['referenced']) not in allowed]
    expected['dependencies'] = stripped(old['dependencies'])
    new['dependencies'] = stripped(new['dependencies'])
    if expected != new:
        fail('Compact audit catalog differs from the exact attribution/RLS transition')


def compare(before: Mapping, after: Mapping, *, schema: str, transform: bool) -> None:
    validate_summary(before)
    validate_summary(after)
    old, new = before['audit_digest'], after['audit_digest']
    if not transform and old != new:
        fail('Compact audit evidence changed without its exact contract')
    if any(old[k] != new[k] for k in ('active_count', 'soft_deleted_count', 'invalid_scope_rows')):
        fail('Compact audit preservation counters changed')
    digest = old['expected_sha256'] if transform else old['raw_sha256']
    if (old['row_count'] != new['row_count'] or new['raw_sha256'] != digest
            or (transform and new['transformed_rows'] != 0)):
        fail('Compact audit whole-row preservation mismatch')
    verify_catalog(before, after, schema=schema, transform=transform)


# Exact expanded native 10.1.7 assets reviewed for this operator-only format.
REVIEWED_ASSETS = {
    '20260902_010000_natural_identity_and_owner_uniqueness.sql':
        '587ad63ac6b30a6dfc3ab3d65fe07255fd5bfc9a0feb7951629c204a116d6fc9',
    '20260902_010100_legacy_outbox_message_conversion.sql':
        'd1823c4c32cbc2e7c652d14a6d935550a62c0fc9a6492a6cc3f15208d6f68f34',
    '20260902_020000_force_rls_and_audit_attribution.sql':
        '562f958c9997e1b26d4775202df9c9f82960242a48ffc88ba42b516163fee5d6',
    '20260903_031820_runtime_ddl_guard.sql':
        'b307e07e955779e0185b778fb7b5d142cc0f42967df78dfdd2bd86afc6f46b03',
    '20260904_061819_tenant_scoped_natural_identity.sql':
        '61bbe0fd833575c8074f8ff00ff1bc2726bf026b687040f4966474f658b51cb3',
    '20260910_203200_aurora_operator_principals.sql':
        'c07630419a9e9b9063c8d019c713e6d57e744b5eb60fd27ba501817215537ae4',
    '20260910_220000_sequence_prefix_bindings.sql':
        '99329ae1bb85e0bfff071ef68b3bce87e75d186f79601338dfdb9447ab6ca3fc',
    '20260910_233000_pin_managed_allocator_resolution.sql':
        'dbf4a84e00530f31305aa7981a823db2d66133bf0c0b10bc786223a7ad37d01e',
    '20260911_164426_runtime_tenant_allowlist.sql':
        '931d195f4374716c8af025e667356986156a074ad10f04555c766ea84b962781',
    '20260911_171000_allowlisted_lineage_scope.sql':
        '2101070f803b328bf8f300266317ba8dff111f70dbc3600543bfbf2c1b5b68ba',
    '20260911_210000_versioned_xrf_scope.sql':
        '3f8f5509fb92e31ce725a796ab266060cb5c0d7920df39dedf4fd13002f64288',
    '20260913_082300_runtime_identity_authorization.sql':
        '984a88f8e43967a3fc97401e6d12737ecadb9a2e390c4d4a4e80bb1f22f0007a',
    '20260918_070400_system_user_authorization_contract.sql':
        'e3b7313fd91777c86d3d7393c1e461f043b077b094c6dbbd340b02b70d74baf2',
    '20260926_220000_reviewed_scope_correction.sql':
        '1f92acd0347ecf4287ac66a9f0631023419e75c120265bea1a25d5aa3ab3579e',
}


# The later four exact assets reapply the canonical audit catalog and have
# no audit row transformation. Full audit summary and catalog equality apply.
# The XRF wrapper includes the RLS source already bundled since TapDB 13.1.0.
# Keep its current expansion separate from the older pre-10 closure above.
AUDIT_UNCHANGED_RLS_CLOSURE = {
    '20260911_210000_versioned_xrf_scope.sql':
        '521f9219b5556334ab7084e07a18ef701a505aa8a21e01e0175e1d769318cd1e',
    '20260913_082300_runtime_identity_authorization.sql':
        REVIEWED_ASSETS['20260913_082300_runtime_identity_authorization.sql'],
    '20260918_070400_system_user_authorization_contract.sql':
        REVIEWED_ASSETS['20260918_070400_system_user_authorization_contract.sql'],
    '20260926_220000_reviewed_scope_correction.sql':
        REVIEWED_ASSETS['20260926_220000_reviewed_scope_correction.sql'],
}

# These exact migrations modify issuance-profile metadata or functions only.
UNCHANGED_AUDIT_ASSETS = {
    '20261114_150000_template_issuance_prefix.sql': '74fa76e3b5fc5a3eed8990858d42dd81545e0240a2c57e01077089c923325bcc',
    '20261114_160000_reconcile_template_issuance.sql': '1b6fc52b3660eafb0d113c9fd912700e59d1b05de66aae653ee21831c2e7c49a',
    '20261114_180000_meridian_separated_checksum.sql': 'f432a71df4844eadadc6e5f31ae03d7129928413806e8f659033757ef24e2ebc',
}


def validate_pending(pending: list[dict]) -> bool:
    observed = {item["filename"]: item["sha256"] for item in pending}
    if len(observed) != len(pending):
        fail("Duplicate pending migration")
    for name, expected in UNCHANGED_AUDIT_ASSETS.items():
        if name in observed:
            if observed.pop(name) != expected:
                fail("Audit-preserving migration differs from its exact reviewed source")
    if observed not in ({}, REVIEWED_ASSETS, AUDIT_UNCHANGED_RLS_CLOSURE):
        fail("Compact audit mode requires an exact reviewed migration closure")
    return observed == REVIEWED_ASSETS
