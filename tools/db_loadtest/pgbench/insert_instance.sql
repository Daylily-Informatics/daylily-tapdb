\set tenant_bucket random(1, :tenant_count)

INSERT INTO generic_instance (
    name,
    polymorphic_discriminator,
    category,
    type,
    subtype,
    version,
    template_uuid,
    json_addl,
    bstatus,
    is_singleton
)
VALUES (
    format('pgbench-inst-%s-%s', :tenant_bucket, txid_current()),
    'generic_instance',
    'generic',
    'generic',
    'loadtest_node',
    '1.0',
    :template_uuid,
    jsonb_build_object(
        'tenant_id', format('tenant_%s', :tenant_bucket),
        'loadtest', true,
        'source', 'pgbench_insert_instance'
    ),
    'active',
    false
);
