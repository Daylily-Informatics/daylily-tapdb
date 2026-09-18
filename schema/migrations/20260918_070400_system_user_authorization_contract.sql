-- Correct the canonical system-user authorization contract.
-- The actual actor discriminator remains required; optional creation hints
-- are not authorization authority. No template, actor, grant or token changes.
-- Replace only the existing native function and pin its existing search path.
-- Source: schema/runtime_identity_authorization.sql in this release.

CREATE OR REPLACE FUNCTION tapdb_resolve_user_authorization(user_uid BIGINT)
RETURNS JSONB LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path FROM CURRENT AS $$
DECLARE
    scope_schema TEXT := current_schema();
    identity_result JSONB;
BEGIN
    -- The assertion checks the authenticated session_user's immutable binding.
    -- Custom context settings are assertions only, never grant authority.
    EXECUTE pg_catalog.format('SELECT %I.tapdb_assert_runtime_role()', scope_schema);
    IF user_uid IS NULL OR user_uid <= 0 THEN
        RETURN NULL;
    END IF;
    EXECUTE pg_catalog.format($query$
        SELECT pg_catalog.jsonb_build_object(
            'uid', actor.uid, 'euid', actor.euid,
            'role', actor.json_addl->>'role',
            'is_active', NOT actor.is_deleted
                AND lower(COALESCE(actor.bstatus::text, '')) = 'active'
                AND COALESCE(lower(actor.json_addl->>'is_active') IN ('true', '1', 'yes', 'on'), false)
        )
        FROM %1$I.generic_instance actor
        JOIN %1$I.generic_template template ON template.uid = actor.template_uid
        JOIN %1$I.tapdb_runtime_principal_scope binding
          ON binding.role_name = $2
        WHERE actor.uid = $1
          AND actor.domain_code = binding.domain_code
          AND actor.polymorphic_discriminator = 'actor_instance'
          AND actor.category = 'actor' AND actor.type = 'user' AND actor.subtype = 'system'
          AND template.category = 'actor' AND template.type = 'user'
          AND template.subtype = 'system' AND template.version = '1.0'
          AND template.domain_code = actor.domain_code
          AND template.issuer_app_code = actor.issuer_app_code
          AND (template.tenant_id IS NULL OR template.tenant_id = actor.tenant_id)
          AND template.polymorphic_discriminator = 'actor_template'
          -- Authorization uses the persisted actor's explicit discriminator.
          -- The packaged system-user template omits the optional factory hint.
          AND lower(COALESCE(template.bstatus::text, '')) = 'active'
          AND NOT template.is_deleted
          AND (
              (actor.issuer_app_code = binding.issuer_app_code
               AND (actor.tenant_id IS NULL OR actor.tenant_id = binding.tenant_id
                    OR actor.tenant_id = ANY(binding.additional_tenant_ids)))
              OR
              (actor.issuer_app_code = 'daylily-tapdb' AND actor.tenant_id IS NULL
               AND EXISTS (
                   SELECT 1 FROM %1$I.tapdb_runtime_identity_access identity_grant
                   WHERE identity_grant.role_name = binding.role_name
                     AND identity_grant.user_uid = actor.uid AND identity_grant.user_euid = actor.euid
                     AND identity_grant.enabled
               ))
          )
    $query$, scope_schema) INTO identity_result USING user_uid, session_user;
    RETURN identity_result;
END;
$$;

DO $tapdb_pin_identity_search_path$
DECLARE
    scope_schema TEXT := current_schema();
    function_name TEXT;
BEGIN
    FOREACH function_name IN ARRAY ARRAY['tapdb_resolve_user_authorization'] LOOP
        EXECUTE pg_catalog.format(
            'ALTER FUNCTION %I.%I(pg_catalog.int8) SET search_path TO %I, pg_catalog, pg_temp',
            scope_schema, function_name, scope_schema
        );
    END LOOP;
END;
$tapdb_pin_identity_search_path$;
