\set parent_uuid random(:min_instance_uuid, :max_instance_uuid)
\set child_uuid random(:min_instance_uuid, :max_instance_uuid)
\set tenant_bucket random(1, :tenant_count)

INSERT INTO generic_instance_lineage (
    name,
    polymorphic_discriminator,
    category,
    type,
    subtype,
    version,
    parent_instance_uuid,
    child_instance_uuid,
    relationship_type,
    json_addl,
    bstatus,
    is_singleton
)
SELECT
    format('pgbench-edge-%s-%s', :parent_uuid, :child_uuid),
    'generic_instance_lineage',
    'lineage',
    'lineage',
    'generic',
    '1.0',
    p.uuid,
    c.uuid,
    'loadtest_rel',
    jsonb_build_object(
        'tenant_id', format('tenant_%s', :tenant_bucket),
        'loadtest', true,
        'source', 'pgbench_insert_lineage_edge'
    ),
    'active',
    false
FROM generic_instance p
JOIN generic_instance c ON c.uuid = :child_uuid
WHERE p.uuid = :parent_uuid
  AND p.is_deleted = false
  AND c.is_deleted = false
  AND p.uuid <> c.uuid
ON CONFLICT DO NOTHING;
