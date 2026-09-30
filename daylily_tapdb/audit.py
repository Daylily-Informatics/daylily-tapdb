"""Scoped audit reads. Audit entry identity and changed-record identity are distinct."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from sqlalchemy import text
from sqlalchemy.orm import Session


@dataclass
class AuditEntry:
    euid: str  # Changed object; retained public field meaning.
    changed_by: str | None
    operation_type: str | None
    changed_at: datetime
    name: str | None
    polymorphic_discriminator: str | None
    category: str | None
    type: str | None
    subtype: str | None
    bstatus: str | None
    old_value: str | None
    new_value: str | None
    audit_uid: int
    audit_euid: str
    record_table: str
    record_uid: int
    domain_code: str
    issuer_app_code: str
    current_context: dict[str, Any] | None
    evidence_format: str | None
    revision: int | None
    attribution: dict[str, Any] | None
    before_state: dict[str, Any] | None
    after_state: dict[str, Any] | None


_AUDIT_TRAIL_SQL = """
SELECT al.*, COALESCE(to_jsonb(gt), to_jsonb(gi), to_jsonb(gil)) AS current_context
FROM audit_log al
LEFT JOIN generic_template gt ON al.rel_table_name = 'generic_template'
 AND al.rel_table_uid_fk = gt.uid AND al.rel_table_euid_fk = gt.euid
 AND al.domain_code = gt.domain_code AND al.issuer_app_code = gt.issuer_app_code
LEFT JOIN generic_instance gi ON al.rel_table_name = 'generic_instance'
 AND al.rel_table_uid_fk = gi.uid AND al.rel_table_euid_fk = gi.euid
 AND al.domain_code = gi.domain_code AND al.issuer_app_code = gi.issuer_app_code
LEFT JOIN generic_instance_lineage gil ON al.rel_table_name = 'generic_instance_lineage'
 AND al.rel_table_uid_fk = gil.uid AND al.rel_table_euid_fk = gil.euid
 AND al.domain_code = gil.domain_code AND al.issuer_app_code = gil.issuer_app_code
"""


def query_audit_trail(
    session: Session, *, changed_by: str | None = None, euid: str | None = None,
    since: datetime | None = None, until: datetime | None = None,
    domain_code: str | None = None, issuer_app_code: str | None = None,
    operation_type: str | None = None, record_table: str | None = None,
    actor_issuer: str | None = None, actor_subject: str | None = None,
    service_identity: str | None = None, operation_id: str | None = None,
    request_id: str | None = None, revision: int | None = None,
    cursor: tuple[datetime, int] | None = None,
    limit: int = 500, order: Literal['asc', 'desc'] = 'desc',
) -> list[AuditEntry]:
    """Read stable (timestamp, audit UID) pages under the caller's RLS scope.

    Descriptions refer to historical row evidence, never today's joined name.
    Older column-only records have null historical fields; current descriptions
    are exposed separately as ``current_context``. Cursor is the last row's
    ``(changed_at, audit_uid)``. Time filtering does not establish commit order.
    """
    if order not in {'asc', 'desc'} or not 1 <= limit <= 10000:
        raise ValueError('order must be asc/desc and limit must be 1..10000')
    if operation_type is not None:
        operation_type = operation_type.upper()
        if operation_type not in {'INSERT', 'UPDATE', 'DELETE'}:
            raise ValueError('operation_type must be INSERT, UPDATE, or DELETE')
    if record_table is not None and record_table not in {
        'generic_template', 'generic_instance', 'generic_instance_lineage'
    }:
        raise ValueError('invalid record_table')
    params: dict[str, Any] = {'limit': limit}
    clauses = []
    filters = {
        'changed_by': ('al.changed_by', changed_by),
        'euid': ('al.rel_table_euid_fk', euid),
        'domain_code': ('al.domain_code', domain_code),
        'issuer_app_code': ('al.issuer_app_code', issuer_app_code),
        'operation_type': ('al.operation_type', operation_type),
        'record_table': ('al.rel_table_name', record_table),
        'actor_issuer': ("al.json_addl->'attribution'->>'actor_issuer'", actor_issuer),
        'actor_subject': ("al.json_addl->'attribution'->>'actor_subject'", actor_subject),
        'service_identity': ("al.json_addl->'attribution'->>'service_identity'", service_identity),
        'operation_id': ("al.json_addl->'attribution'->>'operation_id'", operation_id),
        'request_id': ("al.json_addl->'attribution'->>'request_id'", request_id),
        'revision': ("(CASE WHEN al.json_addl->>'format'='tapdb.revision/v1' THEN al.json_addl->>'revision' END)::bigint", revision),
    }
    for key, (column, value) in filters.items():
        if value is not None:
            clauses.append(f'{column} = :{key}')
            params[key] = value
    for key, op, value in [('since', '>=', since), ('until', '<', until)]:
        if value is not None:
            clauses.append(f'al.changed_at {op} :{key}')
            params[key] = value
    if cursor is not None:
        clauses.append(f"(al.changed_at, al.uid) {'>' if order == 'asc' else '<'} (:cursor_time, :cursor_uid)")
        params.update(cursor_time=cursor[0], cursor_uid=cursor[1])
    sql = _AUDIT_TRAIL_SQL
    if clauses:
        sql += '\nWHERE ' + ' AND '.join(clauses)
    direction = order.upper()
    sql += f'\nORDER BY al.changed_at {direction}, al.uid {direction} LIMIT :limit'
    entries = []
    for row in session.execute(text(sql), params).mappings():
        evidence = row['json_addl'] or {}
        full = evidence.get('format') == 'tapdb.revision/v1'
        state = (evidence.get('after') or evidence.get('before') or {}) if full else {}
        entries.append(AuditEntry(
            euid=row['rel_table_euid_fk'], changed_by=row['changed_by'],
            operation_type=row['operation_type'], changed_at=row['changed_at'],
            **{key: state.get(key) for key in ('name', 'polymorphic_discriminator', 'category', 'type', 'subtype', 'bstatus')},
            old_value=row['old_value'], new_value=row['new_value'],
            audit_uid=row['uid'], audit_euid=row['euid'],
            record_table=row['rel_table_name'], record_uid=row['rel_table_uid_fk'],
            domain_code=row['domain_code'], issuer_app_code=row['issuer_app_code'],
            current_context=row['current_context'], evidence_format='tapdb.revision/v1' if full else 'legacy_incomplete',
            revision=evidence.get('revision') if full else None, attribution=evidence.get('attribution') if full else None,
            before_state=evidence.get('before') if full else None, after_state=evidence.get('after') if full else None,
        ))
    return entries
