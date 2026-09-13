-- Canonical additive authorization-only identity capability.
-- No existing user, token, scope, audit row or ordinary RLS policy is changed.

-- Exact canonical-user authorization grants are native principal policy, not
-- replicated user records. Runtime principals cannot read or change this table.
CREATE TABLE IF NOT EXISTS tapdb_runtime_identity_access (
    role_name NAME NOT NULL REFERENCES tapdb_runtime_principal_scope(role_name),
    user_uid BIGINT NOT NULL REFERENCES generic_instance(uid),
    user_euid TEXT NOT NULL,
    enabled BOOLEAN NOT NULL,
    plan_sha256 TEXT NOT NULL CHECK (plan_sha256 ~ '^[0-9a-f]{64}$'),
    approved_by NAME NOT NULL,
    approved_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (role_name, user_uid)
);
ALTER TABLE tapdb_runtime_identity_access ENABLE ROW LEVEL SECURITY;
ALTER TABLE tapdb_runtime_identity_access FORCE ROW LEVEL SECURITY;
REVOKE ALL ON tapdb_runtime_identity_access FROM PUBLIC;

-- Add only the identity policy; existing object RLS is left untouched.
DO $tapdb_identity_operator_policy$
DECLARE
    scope_schema TEXT := current_schema();
    operator_role NAME;
BEGIN
    SELECT pg_catalog.pg_get_userbyid(nspowner) INTO STRICT operator_role
      FROM pg_catalog.pg_namespace WHERE nspname = scope_schema;
    IF session_user <> operator_role OR current_user <> session_user THEN
        RAISE EXCEPTION 'Identity policy installation requires the exact schema operator';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_class relation
        JOIN pg_catalog.pg_namespace namespace ON namespace.oid = relation.relnamespace
        WHERE namespace.nspname = scope_schema
          AND relation.relname = 'tapdb_runtime_identity_access'
          AND pg_catalog.pg_get_userbyid(relation.relowner) = operator_role
    ) THEN
        RAISE EXCEPTION 'Identity access table must belong to the exact schema operator';
    END IF;
    EXECUTE pg_catalog.format(
        'DROP POLICY IF EXISTS tapdb_operator_access ON %I.tapdb_runtime_identity_access',
        scope_schema
    );
    EXECUTE pg_catalog.format(
        'CREATE POLICY tapdb_operator_access ON %I.tapdb_runtime_identity_access TO %I USING (true) WITH CHECK (true)',
        scope_schema, operator_role
    );
END;
$tapdb_identity_operator_policy$;

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
          AND template.instance_polymorphic_identity = 'actor_instance'
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

REVOKE ALL ON FUNCTION tapdb_resolve_user_authorization(BIGINT) FROM PUBLIC;
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
