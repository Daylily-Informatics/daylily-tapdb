\set parent_uid random(:min_instance_uid, :max_instance_uid)
\set child_uid random(:min_instance_uid, :max_instance_uid)
\set tenant_bucket random(1, :tenant_count)

INSERT INTO generic_instance_lineage (
    name,
    polymorphic_discriminator,
    category,
    type,
    subtype,
    version,
    parent_instance_uid,
    child_instance_uid,
    relationship_type,
    json_addl,
    bstatus,
    is_singleton
)
SELECT
    format('pgbench-edge-%s-%s', :parent_uid, :child_uid),
    'generic_instance_lineage',
    'lineage',
    'lineage',
    'generic',
    '1.0',
    p.uid,
    c.uid,
    'loadtest_rel',
    jsonb_build_object(
        'tenant_id', format('tenant_%s', :tenant_bucket),
        'loadtest', true,
        'source', 'pgbench_insert_lineage_edge'
    ),
    'active',
    false
FROM generic_instance p
JOIN generic_instance c ON c.uid = :child_uid
WHERE p.uid = :parent_uid
  AND p.is_deleted = false
  AND c.is_deleted = false
  AND p.uid <> c.uid
ON CONFLICT DO NOTHING;
