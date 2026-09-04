-- Add tenant-scoped natural identities without rewriting existing rows.
-- Existing identity rows remain global because their tenant_id stays NULL.

CREATE UNIQUE INDEX IF NOT EXISTS idx_generic_instance_natural_identity_global
    ON generic_instance (
        domain_code, issuer_app_code, template_uid, identity_key
    )
    WHERE identity_key IS NOT NULL AND tenant_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_generic_instance_natural_identity_tenant
    ON generic_instance (
        domain_code, issuer_app_code, tenant_id, template_uid, identity_key
    )
    WHERE identity_key IS NOT NULL AND tenant_id IS NOT NULL;

DROP INDEX IF EXISTS idx_generic_instance_natural_identity;

ALTER TABLE generic_instance
    DROP CONSTRAINT IF EXISTS ck_generic_instance_identity_key_global;
