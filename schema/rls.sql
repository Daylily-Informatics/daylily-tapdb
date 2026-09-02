-- TapDB forced row-level isolation and fail-closed audit attribution.
--
-- This asset is idempotent. Fresh schema application and upgrade migrations
-- both consume this exact file so the two paths cannot drift.

-- Runtime scope is bound to the authenticated PostgreSQL login, not to custom
-- GUCs supplied by the client.  Custom GUCs remain useful as an explicit
-- context assertion and for privileged operator sessions, but PostgreSQL lets
-- ordinary users set arbitrary custom GUCs.  They therefore cannot be the RLS
-- authority.
CREATE TABLE IF NOT EXISTS tapdb_runtime_principal_scope (
    role_name NAME PRIMARY KEY,
    config_identity TEXT NOT NULL,
    schema_name NAME NOT NULL,
    domain_code TEXT NOT NULL
        CHECK (domain_code ~ '^[0-9A-HJ-KMNP-TV-Z]{1,4}$'),
    issuer_app_code TEXT NOT NULL
        CHECK (issuer_app_code ~ '^[a-z0-9]+([._-][a-z0-9]+)*$'),
    tenant_id UUID,
    allow_global_rows BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
ALTER TABLE tapdb_runtime_principal_scope ENABLE ROW LEVEL SECURITY;
ALTER TABLE tapdb_runtime_principal_scope FORCE ROW LEVEL SECURITY;
REVOKE ALL ON tapdb_runtime_principal_scope FROM PUBLIC;

CREATE OR REPLACE FUNCTION tapdb_session_role_is_operator()
RETURNS BOOLEAN LANGUAGE sql STABLE SECURITY DEFINER
SET search_path FROM CURRENT AS $$
    SELECT COALESCE(
        (SELECT rolsuper OR rolbypassrls
           FROM pg_catalog.pg_roles
          WHERE rolname = session_user),
        FALSE
    )
$$;

-- Keep scope resolvers self-contained. PostgreSQL maintenance clients such as
-- pg_dump intentionally replace search_path with pg_catalog, so a resolver
-- that calls an unqualified sibling validation function fails even when the
-- complete transaction context is present.
CREATE OR REPLACE FUNCTION tapdb_current_domain_code()
RETURNS TEXT LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path FROM CURRENT AS $$
DECLARE
    raw_code TEXT;
BEGIN
    IF tapdb_session_role_is_operator() THEN
        raw_code := current_setting('session.current_domain_code', true);
    ELSE
        SELECT domain_code INTO raw_code
          FROM tapdb_runtime_principal_scope
         WHERE role_name = session_user;
    END IF;
    IF raw_code IS NULL OR trim(raw_code) = '' THEN
        RAISE EXCEPTION 'session.current_domain_code is required';
    END IF;
    raw_code := upper(trim(raw_code));
    IF raw_code !~ '^[0-9A-HJ-KMNP-TV-Z]{1,4}$' THEN
        RAISE EXCEPTION 'session.current_domain_code must be a valid domain code';
    END IF;
    RETURN raw_code;
END;
$$;

CREATE OR REPLACE FUNCTION tapdb_current_owner_repo_name()
RETURNS TEXT LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path FROM CURRENT AS $$
DECLARE
    raw_code TEXT;
BEGIN
    IF tapdb_session_role_is_operator() THEN
        raw_code := current_setting('session.current_owner_repo_name', true);
    ELSE
        SELECT issuer_app_code INTO raw_code
          FROM tapdb_runtime_principal_scope
         WHERE role_name = session_user;
    END IF;
    IF raw_code IS NULL OR trim(raw_code) = '' THEN
        RAISE EXCEPTION 'session.current_owner_repo_name is required';
    END IF;
    raw_code := lower(trim(raw_code));
    IF raw_code !~ '^[a-z0-9]+([._-][a-z0-9]+)*$' THEN
        RAISE EXCEPTION 'session.current_owner_repo_name is invalid';
    END IF;
    RETURN raw_code;
END;
$$;

CREATE OR REPLACE FUNCTION tapdb_current_tenant_id()
RETURNS UUID LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path FROM CURRENT AS $$
DECLARE
    raw_value TEXT;
BEGIN
    IF tapdb_session_role_is_operator() THEN
        raw_value := current_setting('session.current_tenant_id', true);
    ELSE
        SELECT COALESCE(tenant_id::TEXT, '') INTO raw_value
          FROM tapdb_runtime_principal_scope
         WHERE role_name = session_user;
    END IF;
    IF raw_value IS NULL THEN
        RAISE EXCEPTION 'session.current_tenant_id is required (empty means global scope)';
    END IF;
    raw_value := trim(raw_value);
    IF raw_value = '' THEN
        RETURN NULL;
    END IF;
    RETURN raw_value::UUID;
EXCEPTION WHEN invalid_text_representation THEN
    RAISE EXCEPTION 'session.current_tenant_id must be empty or a valid UUID';
END;
$$;

CREATE OR REPLACE FUNCTION tapdb_current_actor()
RETURNS TEXT LANGUAGE plpgsql STABLE AS $$
DECLARE
    raw_value TEXT;
BEGIN
    raw_value := current_setting('session.current_username', true);
    IF raw_value IS NULL OR trim(raw_value) = '' THEN
        RAISE EXCEPTION 'session.current_username is required for audit attribution';
    END IF;
    RETURN trim(raw_value);
END;
$$;

CREATE OR REPLACE FUNCTION tapdb_allow_global_rows()
RETURNS BOOLEAN LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path FROM CURRENT AS $$
DECLARE
    raw_value TEXT;
BEGIN
    IF tapdb_session_role_is_operator() THEN
        raw_value := current_setting('session.allow_global_rows', true);
    ELSE
        SELECT allow_global_rows::TEXT INTO raw_value
          FROM tapdb_runtime_principal_scope
         WHERE role_name = session_user;
    END IF;
    IF raw_value IS NULL OR trim(raw_value) = '' THEN
        RETURN FALSE;
    END IF;
    IF lower(trim(raw_value)) NOT IN ('true', 'false') THEN
        RAISE EXCEPTION 'session.allow_global_rows must be true or false';
    END IF;
    RETURN lower(trim(raw_value)) = 'true';
END;
$$;

CREATE OR REPLACE FUNCTION tapdb_assert_runtime_role()
RETURNS VOID LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path FROM CURRENT AS $$
DECLARE
    role_is_superuser BOOLEAN;
    role_bypasses_rls BOOLEAN;
    role_can_assume_operator BOOLEAN;
    bound_config_identity TEXT;
    bound_schema_name NAME;
    bound_domain_code TEXT;
    bound_owner_repo_name TEXT;
    bound_tenant_id UUID;
    bound_allow_global_rows BOOLEAN;
BEGIN
    SELECT rolsuper, rolbypassrls
      INTO role_is_superuser, role_bypasses_rls
      FROM pg_roles
     WHERE rolname = session_user;
    SELECT EXISTS (
        SELECT 1
          FROM pg_roles candidate
         WHERE (candidate.rolsuper OR candidate.rolbypassrls)
           AND pg_has_role(session_user, candidate.oid, 'MEMBER')
    ) INTO role_can_assume_operator;
    IF role_is_superuser IS NULL THEN
        RAISE EXCEPTION 'TapDB runtime role could not be resolved';
    END IF;
    IF role_is_superuser OR role_bypasses_rls OR role_can_assume_operator THEN
        RAISE EXCEPTION
            'TapDB runtime role % must not be SUPERUSER or BYPASSRLS', current_user;
    END IF;

    SELECT config_identity, schema_name, domain_code, issuer_app_code,
           tenant_id, allow_global_rows
      INTO bound_config_identity, bound_schema_name, bound_domain_code,
           bound_owner_repo_name, bound_tenant_id, bound_allow_global_rows
      FROM tapdb_runtime_principal_scope
     WHERE role_name = session_user;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'TapDB runtime role % has no immutable scope binding', session_user;
    END IF;

    IF current_setting('session.current_config_identity', true)
           IS DISTINCT FROM bound_config_identity
       OR current_setting('session.current_schema_name', true)
           IS DISTINCT FROM bound_schema_name::TEXT
       OR upper(trim(current_setting('session.current_domain_code', true)))
           IS DISTINCT FROM bound_domain_code
       OR lower(trim(current_setting('session.current_owner_repo_name', true)))
           IS DISTINCT FROM bound_owner_repo_name
       OR trim(current_setting('session.current_tenant_id', true))
           IS DISTINCT FROM COALESCE(bound_tenant_id::TEXT, '')
       OR lower(trim(current_setting('session.allow_global_rows', true)))
           IS DISTINCT FROM bound_allow_global_rows::TEXT THEN
        RAISE EXCEPTION
            'TapDB runtime context does not match immutable scope binding for role %',
            session_user;
    END IF;
END;
$$;

-- `pg_temp` is implicitly searched first when it is omitted from search_path.
-- Bake the operator-owned target schema first and put pg_temp last so a runtime
-- principal cannot shadow the protected binding table or helper functions.
DO $tapdb_pin_scope_search_path$
DECLARE
    scope_schema TEXT := current_schema();
    function_name TEXT;
BEGIN
    FOREACH function_name IN ARRAY ARRAY[
        'tapdb_session_role_is_operator',
        'tapdb_current_domain_code',
        'tapdb_current_owner_repo_name',
        'tapdb_current_tenant_id',
        'tapdb_allow_global_rows',
        'tapdb_assert_runtime_role'
    ] LOOP
        EXECUTE format(
            'ALTER FUNCTION %I.%I() SET search_path TO %I, pg_catalog, pg_temp',
            scope_schema,
            function_name,
            scope_schema
        );
    END LOOP;
END;
$tapdb_pin_scope_search_path$;

-- Historical rows are explicitly marked once; unknown/fallback attribution is
-- forbidden for all future writes.
UPDATE audit_log
   SET changed_by = 'migration:pre-9.2-unattributed'
 WHERE changed_by IS NULL OR trim(changed_by) = '';
ALTER TABLE audit_log ALTER COLUMN changed_by SET NOT NULL;

CREATE OR REPLACE FUNCTION soft_delete_row()
RETURNS TRIGGER AS $$
DECLARE
    app_username TEXT;
BEGIN
    app_username := tapdb_current_actor();
    EXECUTE format('UPDATE %I SET is_deleted = TRUE WHERE uid = $1', TG_TABLE_NAME)
    USING OLD.uid;
    INSERT INTO audit_log (
        rel_table_name, rel_table_uid_fk, rel_table_euid_fk,
        tenant_id, domain_code, issuer_app_code,
        changed_by, operation_type, old_value
    ) VALUES (
        TG_TABLE_NAME, OLD.uid, OLD.euid,
        OLD.tenant_id, OLD.domain_code, OLD.issuer_app_code,
        app_username, 'DELETE', row_to_json(OLD)::TEXT
    );
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION record_update()
RETURNS TRIGGER AS $$
DECLARE
    r RECORD;
    column_name TEXT;
    old_value TEXT;
    new_value TEXT;
    app_username TEXT;
BEGIN
    app_username := tapdb_current_actor();
    FOR r IN SELECT * FROM json_each_text(row_to_json(NEW)) LOOP
        column_name := r.key;
        new_value := r.value;
        EXECUTE format('SELECT ($1).%I', column_name) USING OLD INTO old_value;
        IF old_value IS DISTINCT FROM new_value THEN
            INSERT INTO audit_log (
                rel_table_name, column_name, old_value, new_value,
                changed_by, rel_table_uid_fk, rel_table_euid_fk,
                tenant_id, domain_code, issuer_app_code, operation_type
            ) VALUES (
                TG_TABLE_NAME, column_name, old_value, new_value,
                app_username, NEW.uid, NEW.euid,
                NEW.tenant_id, NEW.domain_code, NEW.issuer_app_code, TG_OP
            );
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION record_insert()
RETURNS TRIGGER AS $$
DECLARE
    app_username TEXT;
BEGIN
    app_username := tapdb_current_actor();
    INSERT INTO audit_log (
        rel_table_name, rel_table_uid_fk, rel_table_euid_fk,
        tenant_id, domain_code, issuer_app_code,
        changed_by, operation_type
    ) VALUES (
        TG_TABLE_NAME, NEW.uid, NEW.euid,
        NEW.tenant_id, NEW.domain_code, NEW.issuer_app_code,
        app_username, 'INSERT'
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

ALTER TABLE generic_template ENABLE ROW LEVEL SECURITY;
ALTER TABLE generic_template FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS generic_template_tenant_isolation ON generic_template;
DROP POLICY IF EXISTS generic_template_domain_isolation ON generic_template;
DROP POLICY IF EXISTS generic_template_scope_isolation ON generic_template;
CREATE POLICY generic_template_scope_isolation ON generic_template
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (tenant_id IS NULL OR tenant_id = tapdb_current_tenant_id())
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = tapdb_current_tenant_id()
            OR (tenant_id IS NULL AND (
                tapdb_current_tenant_id() IS NULL OR tapdb_allow_global_rows()
            ))
        )
    );

ALTER TABLE generic_instance ENABLE ROW LEVEL SECURITY;
ALTER TABLE generic_instance FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS generic_instance_tenant_isolation ON generic_instance;
DROP POLICY IF EXISTS generic_instance_domain_isolation ON generic_instance;
DROP POLICY IF EXISTS generic_instance_scope_isolation ON generic_instance;
CREATE POLICY generic_instance_scope_isolation ON generic_instance
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (tenant_id IS NULL OR tenant_id = tapdb_current_tenant_id())
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = tapdb_current_tenant_id()
            OR (tenant_id IS NULL AND (
                tapdb_current_tenant_id() IS NULL OR tapdb_allow_global_rows()
            ))
        )
    );

ALTER TABLE generic_instance_lineage ENABLE ROW LEVEL SECURITY;
ALTER TABLE generic_instance_lineage FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS generic_instance_lineage_tenant_isolation ON generic_instance_lineage;
DROP POLICY IF EXISTS generic_instance_lineage_domain_isolation ON generic_instance_lineage;
DROP POLICY IF EXISTS generic_instance_lineage_scope_isolation ON generic_instance_lineage;
CREATE POLICY generic_instance_lineage_scope_isolation ON generic_instance_lineage
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (tenant_id IS NULL OR tenant_id = tapdb_current_tenant_id())
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = tapdb_current_tenant_id()
            OR (tenant_id IS NULL AND (
                tapdb_current_tenant_id() IS NULL OR tapdb_allow_global_rows()
            ))
        )
    );

-- RLS on the lineage row alone is insufficient: PostgreSQL's referential
-- integrity checks are not an authorization boundary, so a caller that guesses
-- another tenant's BIGINT uid must not be able to create a visible local edge to
-- that hidden endpoint. Validate both endpoints under the caller's active RLS
-- context before accepting any insert or update.
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
           category, type, subtype, version
      INTO child_domain, child_owner, child_tenant,
           child_category, child_type, child_subtype, child_version
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
       AND (child_category, child_type, child_subtype, child_version) =
           ('reference', 'external_identifier', 'tapdb_object', '1.0') THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'TapDB lineage endpoints are unavailable in the current scope';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS zz_tapdb_validate_lineage_endpoint_scope
    ON generic_instance_lineage;
CREATE TRIGGER zz_tapdb_validate_lineage_endpoint_scope
    BEFORE INSERT OR UPDATE ON generic_instance_lineage
    FOR EACH ROW EXECUTE FUNCTION tapdb_validate_lineage_endpoint_scope();

ALTER TABLE audit_log ENABLE ROW LEVEL SECURITY;
ALTER TABLE audit_log FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS audit_log_tenant_isolation ON audit_log;
DROP POLICY IF EXISTS audit_log_domain_isolation ON audit_log;
DROP POLICY IF EXISTS audit_log_scope_isolation ON audit_log;
CREATE POLICY audit_log_scope_isolation ON audit_log
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (tenant_id IS NULL OR tenant_id = tapdb_current_tenant_id())
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = tapdb_current_tenant_id()
            OR (tenant_id IS NULL AND (
                tapdb_current_tenant_id() IS NULL OR tapdb_allow_global_rows()
            ))
        )
    );

ALTER TABLE outbox_event ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox_event FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_event_tenant_isolation ON outbox_event;
DROP POLICY IF EXISTS outbox_event_domain_isolation ON outbox_event;
DROP POLICY IF EXISTS outbox_event_scope_isolation ON outbox_event;
CREATE POLICY outbox_event_scope_isolation ON outbox_event
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (tenant_id IS NULL OR tenant_id = tapdb_current_tenant_id())
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = tapdb_current_tenant_id()
            OR (tenant_id IS NULL AND (
                tapdb_current_tenant_id() IS NULL OR tapdb_allow_global_rows()
            ))
        )
    );

ALTER TABLE outbox_event_attempt ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox_event_attempt FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_event_attempt_domain_isolation ON outbox_event_attempt;
DROP POLICY IF EXISTS outbox_event_attempt_scope_isolation ON outbox_event_attempt;
CREATE POLICY outbox_event_attempt_scope_isolation ON outbox_event_attempt
    USING (
        EXISTS (
            SELECT 1 FROM outbox_event event
             WHERE event.id = outbox_event_attempt.outbox_event_id
               AND event.domain_code = tapdb_current_domain_code()
               AND event.issuer_app_code = tapdb_current_owner_repo_name()
               AND (event.tenant_id IS NULL OR event.tenant_id = tapdb_current_tenant_id())
        )
    )
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM outbox_event event
             WHERE event.id = outbox_event_attempt.outbox_event_id
               AND event.domain_code = tapdb_current_domain_code()
               AND event.issuer_app_code = tapdb_current_owner_repo_name()
               AND (
                   event.tenant_id = tapdb_current_tenant_id()
                   OR (event.tenant_id IS NULL AND (
                       tapdb_current_tenant_id() IS NULL OR tapdb_allow_global_rows()
                   ))
               )
        )
    );

ALTER TABLE inbox_message ENABLE ROW LEVEL SECURITY;
ALTER TABLE inbox_message FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS inbox_message_domain_isolation ON inbox_message;
DROP POLICY IF EXISTS inbox_message_scope_isolation ON inbox_message;
CREATE POLICY inbox_message_scope_isolation ON inbox_message
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (tenant_id IS NULL OR tenant_id = tapdb_current_tenant_id())
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = tapdb_current_tenant_id()
            OR (tenant_id IS NULL AND (
                tapdb_current_tenant_id() IS NULL OR tapdb_allow_global_rows()
            ))
        )
    );

ALTER TABLE tapdb_identity_prefix_config ENABLE ROW LEVEL SECURITY;
ALTER TABLE tapdb_identity_prefix_config FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tapdb_identity_prefix_config_domain_isolation ON tapdb_identity_prefix_config;
DROP POLICY IF EXISTS tapdb_identity_prefix_config_scope_isolation ON tapdb_identity_prefix_config;
CREATE POLICY tapdb_identity_prefix_config_scope_isolation
    ON tapdb_identity_prefix_config
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
    );

-- Legacy conversion provenance inherits visibility from both the converted
-- message and its delivery row. The mapping intentionally has no duplicated
-- scope columns whose values could drift from those authoritative records.
ALTER TABLE tapdb_legacy_outbox_mapping ENABLE ROW LEVEL SECURITY;
ALTER TABLE tapdb_legacy_outbox_mapping FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tapdb_legacy_outbox_mapping_scope_isolation
    ON tapdb_legacy_outbox_mapping;
CREATE POLICY tapdb_legacy_outbox_mapping_scope_isolation
    ON tapdb_legacy_outbox_mapping
    USING (
        EXISTS (
            SELECT 1
              FROM generic_instance message
             WHERE message.uid = tapdb_legacy_outbox_mapping.message_uid
        )
        AND EXISTS (
            SELECT 1
              FROM outbox_event event
             WHERE event.id = tapdb_legacy_outbox_mapping.old_outbox_id
               AND event.message_uid = tapdb_legacy_outbox_mapping.message_uid
        )
    )
    WITH CHECK (
        EXISTS (
            SELECT 1
              FROM generic_instance message
             WHERE message.uid = tapdb_legacy_outbox_mapping.message_uid
        )
        AND EXISTS (
            SELECT 1
              FROM outbox_event event
             WHERE event.id = tapdb_legacy_outbox_mapping.old_outbox_id
               AND (
                   event.message_uid IS NULL
                   OR event.message_uid = tapdb_legacy_outbox_mapping.message_uid
               )
        )
    );

CREATE OR REPLACE FUNCTION tapdb_reject_legacy_outbox_mapping_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'tapdb_legacy_outbox_mapping is immutable';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tapdb_legacy_outbox_mapping_immutable
    ON tapdb_legacy_outbox_mapping;
CREATE TRIGGER tapdb_legacy_outbox_mapping_immutable
    BEFORE UPDATE OR DELETE ON tapdb_legacy_outbox_mapping
    FOR EACH ROW EXECUTE FUNCTION tapdb_reject_legacy_outbox_mapping_mutation();

-- Trigger functions execute under the caller's privileges, but their object
-- resolution must still be pinned to the trusted target schema.  Putting
-- pg_temp last prevents temporary audit/instance tables from shadowing the
-- authoritative relations used by audit and lineage enforcement.
DO $tapdb_pin_trigger_search_path$
DECLARE
    scope_schema TEXT := current_schema();
    function_name TEXT;
BEGIN
    FOREACH function_name IN ARRAY ARRAY[
        'tapdb_current_actor',
        'soft_delete_row',
        'record_update',
        'record_insert',
        'tapdb_validate_lineage_endpoint_scope',
        'tapdb_reject_legacy_outbox_mapping_mutation'
    ] LOOP
        EXECUTE format(
            'ALTER FUNCTION %I.%I() SET search_path TO %I, pg_catalog, pg_temp',
            scope_schema,
            function_name,
            scope_schema
        );
    END LOOP;
END;
$tapdb_pin_trigger_search_path$;
