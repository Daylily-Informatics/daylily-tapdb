"""Authored, unrun focused checks for the receipt-bound operator correction."""
from copy import deepcopy
from types import SimpleNamespace

import pytest

from daylily_tapdb import audit_uid_sequence_denial as denial
from daylily_tapdb import runtime_principal as rp


class Rows(list):
    def mappings(self):
        return self

    def one(self):
        assert len(self) == 1
        return self[0]

    def scalars(self):
        return self


class AuditCatalog:
    def __init__(self):
        self.cfg = {"database": "qualification", "schema_name": "objects",
                    "operator_user": "operator", "user": "runtime"}
        self.identity = {"database": "qualification", "database_oid": 12,
                         "session_user": "operator", "current_user": "operator"}
        self.sequence = {"oid": 23, "name": "audit_log_uid_seq", "schema": "objects",
                         "owner_oid": 10, "owner": "operator", "table_oid": 22,
                         "table_name": "audit_log", "table_owner": "operator",
                         "column_number": 1, "column_name": "uid", "identity_kind": "d",
                         "type_oid": 20, "dependency_type": "i", "ownership_dependencies": 1}
        self.roles = {"runtime": 11, "other_runtime": 13}
        self.acl = [{"grantor": 10, "grantee": oid, "privilege": privilege, "grantable": False}
                    for oid, privileges in [(10, ["SELECT", "UPDATE", "USAGE"]),
                                            (11, ["SELECT", "USAGE"]), (14, ["USAGE"])]
                    for privilege in privileges]
        self.acl.sort(key=lambda row: (row['grantor'], row['grantee'], row['privilege'], row['grantable']))
        self.commands = []
        self.role_flags = {}
        self.ineffective = False
        self.unrelated_drift = False

    def execute(self, statement, params=None):
        sql = str(statement)
        self.commands.append(sql)
        if 'tapdb_audit_uid:identity' in sql:
            return Rows([deepcopy(self.identity)])
        if 'tapdb_audit_uid:ownership' in sql:
            return Rows([deepcopy(self.sequence)])
        if 'tapdb_audit_uid:acl' in sql:
            return Rows(deepcopy(self.acl))
        if 'SELECT role_name::text' in sql:
            return Rows(sorted(self.roles))
        if 'tapdb_audit_uid:principal' in sql:
            oid = self.roles[params['role']]
            privileges = {r['privilege'] for r in self.acl if r['grantee'] == oid}
            return Rows([{'oid': oid, 'name': params['role'], 'superuser': False,
                         'owner_member': False, 'predefined_authority': False,
                         'external_authority': False,
                         **{p.lower(): p in privileges for p in ('USAGE', 'SELECT', 'UPDATE')},
                         **self.role_flags}])
        if sql.startswith('REVOKE '):
            assert sql.endswith(' RESTRICT') and 'CASCADE' not in sql
            if not self.ineffective:
                role = next(name for name in self.roles if f'FROM "{name}" ' in sql)
                self.acl = [r for r in self.acl if r['grantee'] != self.roles[role]]
            if self.unrelated_drift:
                self.acl = [r for r in self.acl if r['grantee'] != 14]
            return Rows()
        raise AssertionError(sql)


def test_planning_is_read_only_and_covers_every_bound_runtime():
    conn = AuditCatalog()
    plan = denial.plan_audit_uid_sequence_denial(conn, conn.cfg)
    assert [r['name'] for r in plan['principals']] == ['other_runtime', 'runtime']
    assert plan['revokes'] == [{'role': 'runtime', 'role_oid': 11,
                               'privileges': ['SELECT', 'USAGE'], 'behavior': 'RESTRICT'}]
    assert all(not sql.startswith(('REVOKE', 'GRANT', 'ALTER')) for sql in conn.commands)


def test_apply_removes_only_exact_runtime_grants_and_preserves_audit_writer():
    conn = AuditCatalog()
    before = deepcopy(conn.acl)
    plan = denial.plan_audit_uid_sequence_denial(conn, conn.cfg, roles=['runtime'])
    result = denial.apply_audit_uid_sequence_denial(conn, conn.cfg, plan=plan)
    assert result['effective_runtime_privileges_absent'] is True
    assert result['plan_sha256'] == plan['sha256']
    assert conn.acl == [r for r in before if r['grantee'] != 11]
    assert [sql for sql in conn.commands if sql.startswith('REVOKE')] == [
        'REVOKE SELECT, USAGE ON SEQUENCE "objects"."audit_log_uid_seq" FROM "runtime" RESTRICT']
    assert not any(sql.startswith(('GRANT', 'ALTER')) for sql in conn.commands)


@pytest.mark.parametrize('flag', ['superuser', 'owner_member', 'predefined_authority', 'external_authority'])
def test_non_direct_effective_authority_is_never_repaired(flag):
    conn = AuditCatalog()
    conn.role_flags[flag] = True
    with pytest.raises(denial.AuditUIDSequenceDenialError, match='exclusively direct'):
        denial.plan_audit_uid_sequence_denial(conn, conn.cfg)
    assert not any(sql.startswith('REVOKE') for sql in conn.commands)


@pytest.mark.parametrize('field,value', [('owner', 'other'), ('table_owner', 'other'),
    ('dependency_type', 'a'), ('ownership_dependencies', 2), ('type_oid', 23),
    ('identity_kind', ''), ('name', 'unrelated_sequence'), ('schema', 'other')])
def test_allocator_ownership_or_identity_drift_is_rejected(field, value):
    conn = AuditCatalog()
    conn.sequence[field] = value
    with pytest.raises(denial.AuditUIDSequenceDenialError, match='contract differs'):
        denial.plan_audit_uid_sequence_denial(conn, conn.cfg)


@pytest.mark.parametrize('change', ['foreign_grantor', 'downstream'])
def test_unsupported_grant_chain_fails_during_planning(change):
    conn = AuditCatalog()
    if change == 'foreign_grantor':
        next(r for r in conn.acl if r['grantee'] == 11)['grantor'] = 15
    else:
        conn.acl.append({'grantor': 11, 'grantee': 15, 'privilege': 'USAGE', 'grantable': False})
    with pytest.raises(denial.AuditUIDSequenceDenialError):
        denial.plan_audit_uid_sequence_denial(conn, conn.cfg)
    assert not any(sql.startswith('REVOKE') for sql in conn.commands)


def test_stale_acl_plan_has_no_mutation():
    conn = AuditCatalog()
    plan = denial.plan_audit_uid_sequence_denial(conn, conn.cfg)
    conn.acl.append({'grantor': 10, 'grantee': 15, 'privilege': 'SELECT', 'grantable': False})
    with pytest.raises(denial.AuditUIDSequenceDenialError, match='plan changed'):
        denial.apply_audit_uid_sequence_denial(conn, conn.cfg, plan=plan)
    assert not any(sql.startswith('REVOKE') for sql in conn.commands)


@pytest.mark.parametrize('field', ['ineffective', 'unrelated_drift'])
def test_apply_fails_if_denial_or_unrelated_acl_preservation_fails(field):
    conn = AuditCatalog()
    plan = denial.plan_audit_uid_sequence_denial(conn, conn.cfg)
    setattr(conn, field, True)
    with pytest.raises(denial.AuditUIDSequenceDenialError, match='preservation failed'):
        denial.apply_audit_uid_sequence_denial(conn, conn.cfg, plan=plan)


def test_binding_planned_exception_is_exact_and_final_denial_is_not_skipped():
    conn = AuditCatalog()
    denial_plan = denial.plan_audit_uid_sequence_denial(conn, conn.cfg, roles=['runtime'])
    plan = {'grants': [], 'objects': [{'oid': 23, 'name': 'audit_log_uid_seq', 'kind': 'S'}],
            'functions': [], 'routine_grants': [], 'audit_uid_sequence_denial': denial_plan}
    leaked = SimpleNamespace(execute=lambda *_: SimpleNamespace(scalar_one=lambda: True))
    target = {'user': 'runtime', 'schema_name': 'objects'}
    rp._verify_bound_permissions(leaked, target, plan, unmanaged_only=True, planned_audit_uid_denial=True)
    with pytest.raises(rp.RuntimePrincipalError, match='audit_log_uid_seq'):
        rp._verify_bound_permissions(leaked, target, plan)
    plan['objects'][0]['oid'] = 24
    with pytest.raises(rp.RuntimePrincipalError):
        rp._verify_bound_permissions(leaked, target, plan, unmanaged_only=True, planned_audit_uid_denial=True)
