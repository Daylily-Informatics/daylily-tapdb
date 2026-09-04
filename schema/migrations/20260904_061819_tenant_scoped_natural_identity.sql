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

-- Permit tenant-owned lineage to canonical public-global opaque identifiers.
-- The endpoint remains a global, typed XRF and tenant-scoped opaque identifiers
-- remain ineligible for this explicit cross-scope exception.
CREATE OR REPLACE FUNCTION tapdb_validate_lineage_endpoint_scope()
RETURNS TRIGGER AS $$
DECLARE
    parent_domain TEXT;
    parent_owner TEXT;
    parent_tenant UUID;
    child_domain TEXT;
    child_owner TEXT;
    child_tenant UUID;
    child_category TEXT;
    child_type TEXT;
    child_subtype TEXT;
    child_version TEXT;
    child_json_addl JSONB;
    approved_global_link BOOLEAN;
BEGIN
    IF NEW.parent_instance_uid = NEW.child_instance_uid THEN
        RAISE EXCEPTION 'TapDB lineage endpoints are unavailable in the current scope';
    END IF;

    SELECT domain_code, issuer_app_code, tenant_id
      INTO parent_domain, parent_owner, parent_tenant
      FROM generic_instance
     WHERE uid = NEW.parent_instance_uid
       AND is_deleted IS FALSE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'TapDB lineage endpoints are unavailable in the current scope';
    END IF;

    SELECT domain_code, issuer_app_code, tenant_id,
           category, type, subtype, version, json_addl
      INTO child_domain, child_owner, child_tenant,
           child_category, child_type, child_subtype, child_version,
           child_json_addl
      FROM generic_instance
     WHERE uid = NEW.child_instance_uid
       AND is_deleted IS FALSE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'TapDB lineage endpoints are unavailable in the current scope';
    END IF;

    IF parent_domain <> NEW.domain_code
       OR parent_owner <> NEW.issuer_app_code
       OR child_domain <> NEW.domain_code
       OR child_owner <> NEW.issuer_app_code THEN
        RAISE EXCEPTION 'TapDB lineage endpoints are unavailable in the current scope';
    END IF;

    IF parent_tenant IS NOT DISTINCT FROM NEW.tenant_id
       AND child_tenant IS NOT DISTINCT FROM NEW.tenant_id THEN
        RETURN NEW;
    END IF;

    approved_global_link := COALESCE(
        NEW.json_addl #> '{properties,approved_global_link}',
        'false'::jsonb
    ) = 'true'::jsonb;
    IF NEW.tenant_id IS NOT NULL
       AND parent_tenant IS NOT DISTINCT FROM NEW.tenant_id
       AND child_tenant IS NULL
       AND approved_global_link
       AND (child_category, child_type, child_version) =
           ('reference', 'external_identifier', '1.0')
       AND (
           child_subtype = 'tapdb_object'
           OR (
               child_subtype = 'opaque'
               AND child_json_addl #>> '{properties,scope}' = 'public_global'
           )
       ) THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'TapDB lineage endpoints are unavailable in the current scope';
END;
$$ LANGUAGE plpgsql;

DO $tapdb_pin_lineage_scope_search_path$
DECLARE
    scope_schema TEXT := current_schema();
BEGIN
    EXECUTE format(
        'ALTER FUNCTION %I.tapdb_validate_lineage_endpoint_scope() '
        'SET search_path TO %I, pg_catalog, pg_temp',
        scope_schema,
        scope_schema
    );
END;
$tapdb_pin_lineage_scope_search_path$;
