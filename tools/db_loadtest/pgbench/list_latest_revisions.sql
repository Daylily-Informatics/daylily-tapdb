SELECT
    latest.rel_table_name,
    latest.rel_table_uuid_fk,
    latest.rel_table_euid_fk,
    latest.operation_type,
    latest.changed_at
FROM (
    SELECT DISTINCT ON (a.rel_table_name, a.rel_table_uuid_fk)
        a.rel_table_name,
        a.rel_table_uuid_fk,
        a.rel_table_euid_fk,
        a.operation_type,
        a.changed_at
    FROM audit_log a
    WHERE a.is_deleted = false
      AND a.rel_table_name IN ('generic_instance', 'generic_instance_lineage')
    ORDER BY a.rel_table_name, a.rel_table_uuid_fk, a.changed_at DESC
) AS latest
ORDER BY latest.changed_at DESC
LIMIT :latest_limit;
