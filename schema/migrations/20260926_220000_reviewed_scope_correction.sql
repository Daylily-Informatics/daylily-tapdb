-- Replace only the native lineage scope function. No table/schema/data changes,
-- historical scope transformations, new rows, policies, triggers or allocators.
-- Native operator scope correction is a separate reviewed operation after this
-- identity-preserving physical migration. Keep body identical to schema/rls.sql.

CREATE OR REPLACE FUNCTION tapdb_validate_lineage_endpoint_scope()
RETURNS TRIGGER AS $$
DECLARE
    parent_domain TEXT;
    parent_owner TEXT;
    parent_tenant UUID;
    parent_category TEXT;
    parent_type TEXT;
    child_domain TEXT;
    child_owner TEXT;
    child_tenant UUID;
    child_category TEXT;
    child_type TEXT;
    child_subtype TEXT;
    child_json_addl JSONB;
    approved_global_link BOOLEAN;
    parent_euid TEXT;
    child_euid TEXT;
    scope_plan JSONB;
    scope_transition JSONB;
    transition_index INTEGER;
    transition_count INTEGER;
BEGIN
    IF NEW.parent_instance_uid = NEW.child_instance_uid THEN
        RAISE EXCEPTION 'TapDB lineage endpoints are unavailable in the current scope';
    END IF;

    SELECT domain_code, issuer_app_code, tenant_id, euid, category, type
      INTO parent_domain, parent_owner, parent_tenant, parent_euid,
           parent_category, parent_type
      FROM generic_instance
     WHERE uid = NEW.parent_instance_uid
       AND is_deleted IS FALSE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'TapDB lineage endpoints are unavailable in the current scope';
    END IF;

    SELECT domain_code, issuer_app_code, tenant_id,
           category, type, subtype, json_addl, euid
      INTO child_domain, child_owner, child_tenant,
           child_category, child_type, child_subtype,
           child_json_addl, child_euid
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

    -- tapdb.reviewed-scope-correction/v1: an exact instruction manifest for an
    -- authenticated operator, never a runtime GUC authorization credential.
    -- Ordinary operators retain the existing rules below; this path never
    -- authorizes INSERTs. Runtime shared-domain links are checked separately.
    IF TG_OP = 'UPDATE'
       AND tapdb_session_role_is_operator()
       AND current_user = session_user
       AND COALESCE(current_setting('tapdb.reviewed_scope_correction', true), '') <> '' THEN
        scope_plan := current_setting('tapdb.reviewed_scope_correction')::jsonb;
        IF scope_plan->>'contract' IS DISTINCT FROM 'tapdb.reviewed-scope-correction/v1'
           OR scope_plan->>'operator_user' IS DISTINCT FROM session_user::text
           OR scope_plan->>'database' IS DISTINCT FROM current_database()
           OR scope_plan->>'schema_name' IS DISTINCT FROM current_schema()
           OR scope_plan->>'domain_code' IS DISTINCT FROM NEW.domain_code
           OR scope_plan->>'owner_repo_name' IS DISTINCT FROM NEW.issuer_app_code
           OR scope_plan->>'config_path' IS DISTINCT FROM current_setting('session.current_config_identity', true)
           OR scope_plan->>'actor' IS DISTINCT FROM tapdb_current_actor()
           OR scope_plan->>'transaction_id' IS DISTINCT FROM pg_current_xact_id()::text
           OR scope_plan->>'backend_pid' IS DISTINCT FROM pg_backend_pid()::text
           OR COALESCE(scope_plan->>'plan_sha256', '') !~ '^[0-9a-f]{64}$'
           OR COALESCE(scope_plan->>'operation_id', '') = ''
           OR jsonb_typeof(scope_plan->'transitions') IS DISTINCT FROM 'array' THEN
            RAISE EXCEPTION 'Reviewed scope correction context does not match authenticated transaction';
        END IF;
        SELECT count(*) INTO transition_count
          FROM jsonb_array_elements(scope_plan->'transitions') item
         WHERE item->>'uid' = OLD.uid::text AND item->>'euid' = OLD.euid;
        IF transition_count <> 1 THEN
            RAISE EXCEPTION 'Lineage transition is not uniquely admitted by reviewed scope plan';
        END IF;
        SELECT item, ordinal::integer - 1 INTO scope_transition, transition_index
          FROM jsonb_array_elements(scope_plan->'transitions') WITH ORDINALITY admitted(item, ordinal)
         WHERE item->>'uid' = OLD.uid::text AND item->>'euid' = OLD.euid;
        IF OLD.is_deleted IS DISTINCT FROM false
           OR scope_transition->>'old_tenant' IS DISTINCT FROM OLD.tenant_id::text
           OR scope_transition->>'new_tenant' IS DISTINCT FROM NEW.tenant_id::text
           OR scope_transition->>'parent_euid' IS DISTINCT FROM parent_euid
           OR scope_transition->>'child_euid' IS DISTINCT FROM child_euid
           OR scope_transition->>'parent_tenant' IS DISTINCT FROM parent_tenant::text
           OR scope_transition->>'child_tenant' IS DISTINCT FROM child_tenant::text
           OR scope_transition->>'protected_sha256' IS DISTINCT FROM
              encode(sha256(convert_to((to_jsonb(OLD) - ARRAY['tenant_id','modified_dt'])::text, 'UTF8')), 'hex')
           OR (to_jsonb(OLD) - ARRAY['tenant_id','modified_dt','is_deleted']) IS DISTINCT FROM
              (to_jsonb(NEW) - ARRAY['tenant_id','modified_dt','is_deleted'])
           OR jsonb_typeof(scope_transition->'retire') IS DISTINCT FROM 'boolean'
           OR NEW.is_deleted IS DISTINCT FROM (scope_transition->>'retire')::boolean
           OR NEW.tenant_id IS NULL THEN
            RAISE EXCEPTION 'Lineage transition differs from exact reviewed scope plan';
        END IF;
        scope_plan := jsonb_set(scope_plan, '{transitions}', (scope_plan->'transitions') - transition_index);
        PERFORM set_config('tapdb.reviewed_scope_correction', scope_plan::text, true);
        RETURN NEW;
    END IF;

    IF parent_tenant IS NOT DISTINCT FROM NEW.tenant_id
       AND child_tenant IS NOT DISTINCT FROM NEW.tenant_id THEN
        RETURN NEW;
    END IF;

    -- A single authorized tenant can use its explicitly writable global domain
    -- catalog in either direction. The relationship belongs to the scoped
    -- endpoint, never to the global catalog. RLS has checked both endpoints;
    -- these resolvers read the authenticated role binding, not client GUCs.
    -- Canonical external references keep their existing dedicated rules.
    IF NOT tapdb_session_role_is_operator()
       AND tapdb_allow_global_rows()
       AND ((parent_tenant IS NULL) <> (child_tenant IS NULL))
       AND NEW.tenant_id IS NOT DISTINCT FROM COALESCE(parent_tenant, child_tenant)
       AND NEW.tenant_id = ANY(tapdb_allowed_tenant_ids())
       AND (parent_category, parent_type) <> ('reference', 'external_identifier')
       AND (child_category, child_type) <> ('reference', 'external_identifier') THEN
        RETURN NEW;
    END IF;

    -- An explicit service allowlist authorizes links among its visible scopes.
    -- Both active endpoints have already passed caller RLS and exact owner/domain
    -- checks above; the lineage row must still pass its own RLS WITH CHECK.
    -- A normal single-tenant principal retains the typed-global exception below.
    IF NOT tapdb_session_role_is_operator()
       AND cardinality(array_remove(
           tapdb_allowed_tenant_ids(), tapdb_current_tenant_id()
       )) > 0 THEN
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
       AND (child_category, child_type) =
           ('reference', 'external_identifier')
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

DO $tapdb_pin_scope_correction$
DECLARE scope_schema TEXT := current_schema();
BEGIN
    EXECUTE format('ALTER FUNCTION %I.tapdb_validate_lineage_endpoint_scope() SET search_path TO %I, pg_catalog, pg_temp', scope_schema, scope_schema);
END;
$tapdb_pin_scope_correction$;
