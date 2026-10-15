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
-- Existing bindings retain exactly their old scope. New bindings may explicitly
-- add a finite set of tenants; ordinary runtime sessions cannot edit this table.
ALTER TABLE tapdb_runtime_principal_scope
    ADD COLUMN IF NOT EXISTS additional_tenant_ids UUID[] NOT NULL DEFAULT '{}'
        CHECK (array_position(additional_tenant_ids, NULL) IS NULL);
ALTER TABLE tapdb_runtime_principal_scope ENABLE ROW LEVEL SECURITY;
ALTER TABLE tapdb_runtime_principal_scope FORCE ROW LEVEL SECURITY;
REVOKE ALL ON tapdb_runtime_principal_scope FROM PUBLIC;

-- Aurora administrators are not PostgreSQL superusers. Complete operator
-- access is an explicit policy for the exact schema owner, including the
-- scope table read by SECURITY DEFINER resolvers. FORCE RLS remains enabled.
-- Install this before resolvers or historical audit repair can read rows.
DO $tapdb_operator_policies$
DECLARE
    managed_schema NAME := current_schema();
    operator_role NAME;
    relation RECORD;
BEGIN
    SELECT pg_catalog.pg_get_userbyid(nspowner) INTO STRICT operator_role
      FROM pg_catalog.pg_namespace WHERE nspname = managed_schema;
    IF NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_roles
         WHERE rolname = session_user AND (rolsuper OR rolbypassrls)
    ) AND (session_user <> operator_role OR current_user <> session_user) THEN
        RAISE EXCEPTION 'Canonical RLS installation requires the exact schema owner';
    END IF;
    FOR relation IN
        SELECT c.relname, c.relowner FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = managed_schema AND c.relkind IN ('r', 'p')
          AND c.relname IN (
              'generic_template', 'generic_instance', 'generic_instance_lineage',
              'audit_log', 'outbox_event', 'outbox_event_attempt', 'inbox_message',
              'tapdb_identity_prefix_config', 'tapdb_legacy_outbox_mapping',
              'tapdb_runtime_principal_scope', 'tapdb_history_epoch', 'tapdb_history_baseline'
          )
    LOOP
        IF relation.relowner <> (SELECT oid FROM pg_catalog.pg_roles
                                 WHERE rolname = operator_role) THEN
            RAISE EXCEPTION 'Managed table % has a different operator owner', relation.relname;
        END IF;
        EXECUTE format('DROP POLICY IF EXISTS tapdb_operator_access ON %I.%I',
                       managed_schema, relation.relname);
        EXECUTE format(
            'CREATE POLICY tapdb_operator_access ON %I.%I TO %I USING (true) WITH CHECK (true)',
            managed_schema, relation.relname, operator_role
        );
    END LOOP;
END;
$tapdb_operator_policies$;

CREATE OR REPLACE FUNCTION tapdb_session_role_is_operator()
RETURNS BOOLEAN LANGUAGE sql STABLE SECURITY DEFINER
SET search_path FROM CURRENT AS $$
    SELECT COALESCE(
        (SELECT rolsuper OR rolbypassrls OR oid = (
             SELECT nspowner FROM pg_catalog.pg_namespace
              WHERE nspname = current_schema()
         )
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

CREATE OR REPLACE FUNCTION tapdb_allowed_tenant_ids()
RETURNS UUID[] LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path FROM CURRENT AS $$
DECLARE
    primary_tenant UUID;
    additional_tenants UUID[];
BEGIN
    IF tapdb_session_role_is_operator() THEN
        primary_tenant := tapdb_current_tenant_id();
        RETURN array_remove(ARRAY[primary_tenant], NULL);
    END IF;
    SELECT tenant_id, additional_tenant_ids
      INTO primary_tenant, additional_tenants
      FROM tapdb_runtime_principal_scope
     WHERE role_name = session_user;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'TapDB runtime role has no immutable scope binding';
    END IF;
    RETURN array_remove(ARRAY[primary_tenant], NULL) || additional_tenants;
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
    bound_additional_tenant_ids UUID[];
    bound_allow_global_rows BOOLEAN;
BEGIN
    SELECT rolsuper OR rolcreatedb OR rolcreaterole OR rolreplication, rolbypassrls
      INTO role_is_superuser, role_bypasses_rls
      FROM pg_roles
     WHERE rolname = session_user;
    SELECT EXISTS (
        SELECT 1
          FROM pg_roles candidate
         WHERE candidate.rolname <> 'rds_iam'
           AND candidate.rolname <> session_user
           AND pg_has_role(session_user, candidate.oid, 'MEMBER')
    ) INTO role_can_assume_operator;
    IF role_is_superuser IS NULL THEN
        RAISE EXCEPTION 'TapDB runtime role could not be resolved';
    END IF;
    IF role_is_superuser OR role_bypasses_rls OR role_can_assume_operator THEN
        RAISE EXCEPTION
            'TapDB runtime role % must not be SUPERUSER or BYPASSRLS', current_user;
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_catalog.pg_auth_members membership
        JOIN pg_catalog.pg_roles member ON member.oid = membership.member
        WHERE member.rolname = session_user AND membership.admin_option
    ) OR EXISTS (
        SELECT 1 FROM pg_catalog.pg_db_role_setting settings
        JOIN pg_catalog.pg_roles role ON role.oid = settings.setrole
        WHERE role.rolname = session_user AND cardinality(settings.setconfig) > 0
    ) OR pg_catalog.has_database_privilege(session_user, current_database(), 'CREATE')
      OR pg_catalog.has_schema_privilege(session_user, current_schema(), 'CREATE') THEN
        RAISE EXCEPTION 'TapDB runtime role has forbidden membership, settings, or CREATE authority';
    END IF;

    SELECT config_identity, schema_name, domain_code, issuer_app_code,
           tenant_id, additional_tenant_ids, allow_global_rows
      INTO bound_config_identity, bound_schema_name, bound_domain_code,
           bound_owner_repo_name, bound_tenant_id, bound_additional_tenant_ids,
           bound_allow_global_rows
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
       OR current_setting('session.additional_tenant_ids', true)::UUID[]
           IS DISTINCT FROM bound_additional_tenant_ids
       OR lower(trim(current_setting('session.allow_global_rows', true)))
           IS DISTINCT FROM bound_allow_global_rows::TEXT THEN
        RAISE EXCEPTION
            'TapDB runtime context does not match immutable scope binding for role %',
            session_user;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION tapdb_reject_runtime_scope_mutation()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'TapDB runtime principal scope binding is immutable';
END;
$$;
DROP TRIGGER IF EXISTS tapdb_runtime_scope_immutable ON tapdb_runtime_principal_scope;
CREATE TRIGGER tapdb_runtime_scope_immutable
    BEFORE UPDATE OR DELETE ON tapdb_runtime_principal_scope
    FOR EACH ROW EXECUTE FUNCTION tapdb_reject_runtime_scope_mutation();

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
        'tapdb_allowed_tenant_ids',
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

-- Old audit rows are evidence, including missing attribution. Never rewrite them.
CREATE OR REPLACE FUNCTION tapdb_current_attribution()
RETURNS JSONB LANGUAGE plpgsql STABLE AS $$
DECLARE
    envelope JSONB;
    field_name TEXT;
BEGIN
    envelope := NULLIF(current_setting('session.tapdb_attribution', true), '')::jsonb;
    IF envelope IS NULL OR jsonb_typeof(envelope) <> 'object'
       OR jsonb_typeof(envelope->'version') IS DISTINCT FROM 'number'
       OR envelope->>'version' IS DISTINCT FROM '1'
       OR envelope->>'actor_kind' IS NULL
       OR envelope->>'actor_kind' NOT IN ('human', 'service') THEN
        RAISE EXCEPTION 'TapDB 11 write contract mismatch: explicit attribution v1 is required';
    END IF;
    FOREACH field_name IN ARRAY ARRAY[
        'actor_issuer', 'actor_subject', 'service_identity', 'request_id', 'operation_id'
    ] LOOP
        IF jsonb_typeof(envelope->field_name) IS DISTINCT FROM 'string'
           OR btrim(envelope->>field_name) = ''
           OR btrim(envelope->>field_name) <> envelope->>field_name
           OR envelope->>field_name ~ '[[:cntrl:]]' THEN
            RAISE EXCEPTION 'TapDB 11 attribution requires exact nonempty %', field_name;
        END IF;
    END LOOP;
    IF NOT tapdb_session_role_is_operator() THEN
        PERFORM tapdb_assert_runtime_role();
    END IF;
    RETURN envelope || jsonb_build_object(
        'database_principal', session_user, 'database', current_database(),
        'domain_code', tapdb_current_domain_code(),
        'issuer_app_code', tapdb_current_owner_repo_name(),
        'tenant_id', tapdb_current_tenant_id());
END;
$$;

CREATE OR REPLACE FUNCTION soft_delete_row()
RETURNS TRIGGER AS $$
BEGIN
    -- The resulting UPDATE produces one audited state transition, never two.
    EXECUTE format('UPDATE %I.%I SET is_deleted = TRUE WHERE uid = $1', TG_TABLE_SCHEMA, TG_TABLE_NAME)
    USING OLD.uid;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION tapdb_set_record_revision()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM tapdb_current_attribution();
    IF TG_OP = 'INSERT' THEN
        IF NEW.record_revision <> 0 THEN
            RAISE EXCEPTION 'record_revision is database owned';
        END IF;
        NEW.record_revision := 1;
    ELSE
        IF NEW.record_revision IS DISTINCT FROM OLD.record_revision THEN
            RAISE EXCEPTION 'record_revision is database owned';
        END IF;
        IF (to_jsonb(NEW) - 'modified_dt') = (to_jsonb(OLD) - 'modified_dt') THEN
            RETURN NULL;
        END IF;
        NEW.record_revision := OLD.record_revision + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION record_insert()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER
SET search_path FROM CURRENT AS $$
DECLARE
    attribution JSONB;
    prior JSONB;
    resulting JSONB;
    epoch_id UUID;
    governing_template JSONB;
BEGIN
    attribution := tapdb_current_attribution();
    resulting := to_jsonb(NEW);
    prior := CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) ELSE NULL END;
    SELECT epoch INTO STRICT epoch_id FROM tapdb_history_epoch WHERE active;
    IF TG_TABLE_NAME = 'generic_instance' THEN
        SELECT jsonb_build_object('uid', uid, 'euid', euid, 'domain_code', domain_code,
            'issuer_app_code', issuer_app_code, 'revision', record_revision)
        INTO STRICT governing_template FROM generic_template
        WHERE uid = (resulting->>'template_uid')::bigint;
    ELSIF TG_TABLE_NAME = 'generic_template' THEN
        governing_template := jsonb_build_object('uid', NEW.uid, 'euid', NEW.euid,
            'domain_code', NEW.domain_code, 'issuer_app_code', NEW.issuer_app_code,
            'revision', NEW.record_revision);
    END IF;
    -- Pinned schema plus explicit trigger schema prevents temporary shadowing.
    EXECUTE format('INSERT INTO %I.audit_log (
        rel_table_name, rel_table_uid_fk, rel_table_euid_fk,
        tenant_id, domain_code, issuer_app_code, changed_by, operation_type,
        changed_at, json_addl) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp(),$9)', TG_TABLE_SCHEMA)
    USING TG_TABLE_NAME, NEW.uid, NEW.euid, NEW.tenant_id, NEW.domain_code,
        NEW.issuer_app_code, attribution->>'actor_subject', TG_OP,
        jsonb_build_object('format', 'tapdb.revision/v1', 'before', prior,
            'after', resulting, 'attribution', attribution,
            'revision', NEW.record_revision, 'transaction_id', pg_current_xact_id()::text,
            'epoch', epoch_id, 'governing_template', governing_template);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION record_update()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER
SET search_path FROM CURRENT AS $$
DECLARE
    attribution JSONB;
    prior JSONB;
    resulting JSONB;
    epoch_id UUID;
    governing_template JSONB;
BEGIN
    attribution := tapdb_current_attribution();
    resulting := to_jsonb(NEW);
    prior := CASE WHEN TG_OP = 'UPDATE' THEN to_jsonb(OLD) ELSE NULL END;
    SELECT epoch INTO STRICT epoch_id FROM tapdb_history_epoch WHERE active;
    IF TG_TABLE_NAME = 'generic_instance' THEN
        SELECT jsonb_build_object('uid', uid, 'euid', euid, 'domain_code', domain_code,
            'issuer_app_code', issuer_app_code, 'revision', record_revision)
        INTO STRICT governing_template FROM generic_template
        WHERE uid = (resulting->>'template_uid')::bigint;
    ELSIF TG_TABLE_NAME = 'generic_template' THEN
        governing_template := jsonb_build_object('uid', NEW.uid, 'euid', NEW.euid,
            'domain_code', NEW.domain_code, 'issuer_app_code', NEW.issuer_app_code,
            'revision', NEW.record_revision);
    END IF;
    -- Pinned schema plus explicit trigger schema prevents temporary shadowing.
    EXECUTE format('INSERT INTO %I.audit_log (
        rel_table_name, rel_table_uid_fk, rel_table_euid_fk,
        tenant_id, domain_code, issuer_app_code, changed_by, operation_type,
        changed_at, json_addl) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,clock_timestamp(),$9)', TG_TABLE_SCHEMA)
    USING TG_TABLE_NAME, NEW.uid, NEW.euid, NEW.tenant_id, NEW.domain_code,
        NEW.issuer_app_code, attribution->>'actor_subject', TG_OP,
        jsonb_build_object('format', 'tapdb.revision/v1', 'before', prior,
            'after', resulting, 'attribution', attribution,
            'revision', NEW.record_revision, 'transaction_id', pg_current_xact_id()::text,
            'epoch', epoch_id, 'governing_template', governing_template);
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION tapdb_reject_audit_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'TapDB audit history is append-only; mutation is forbidden';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS tapdb_audit_no_mutation ON audit_log;
CREATE TRIGGER tapdb_audit_no_mutation
    BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE FUNCTION tapdb_reject_audit_mutation();
DROP TRIGGER IF EXISTS tapdb_audit_no_truncate ON audit_log;
CREATE TRIGGER tapdb_audit_no_truncate
    BEFORE TRUNCATE ON audit_log
    FOR EACH STATEMENT EXECUTE FUNCTION tapdb_reject_audit_mutation();
DROP TRIGGER IF EXISTS zzzz_tapdb_revision ON generic_template;
CREATE TRIGGER zzzz_tapdb_revision
    BEFORE INSERT OR UPDATE ON generic_template
    FOR EACH ROW EXECUTE FUNCTION tapdb_set_record_revision();
DROP TRIGGER IF EXISTS zzzz_tapdb_revision ON generic_instance;
CREATE TRIGGER zzzz_tapdb_revision
    BEFORE INSERT OR UPDATE ON generic_instance
    FOR EACH ROW EXECUTE FUNCTION tapdb_set_record_revision();
DROP TRIGGER IF EXISTS zzzz_tapdb_revision ON generic_instance_lineage;
CREATE TRIGGER zzzz_tapdb_revision
    BEFORE INSERT OR UPDATE ON generic_instance_lineage
    FOR EACH ROW EXECUTE FUNCTION tapdb_set_record_revision();
DO $tapdb_pin_audit_search_path$
DECLARE
    scope_schema TEXT := current_schema();
    function_name TEXT;
BEGIN
    FOREACH function_name IN ARRAY ARRAY[
        'record_insert', 'record_update', 'soft_delete_row',
        'tapdb_set_record_revision', 'tapdb_current_attribution', 'tapdb_reject_audit_mutation'
    ] LOOP
        EXECUTE format('ALTER FUNCTION %I.%I() SET search_path TO %I, pg_catalog, pg_temp',
            scope_schema, function_name, scope_schema);
    END LOOP;
END;
$tapdb_pin_audit_search_path$;

ALTER TABLE generic_template ENABLE ROW LEVEL SECURITY;
ALTER TABLE generic_template FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS generic_template_tenant_isolation ON generic_template;
DROP POLICY IF EXISTS generic_template_domain_isolation ON generic_template;
DROP POLICY IF EXISTS generic_template_scope_isolation ON generic_template;
CREATE POLICY generic_template_scope_isolation ON generic_template
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (tenant_id IS NULL OR tenant_id = ANY(tapdb_allowed_tenant_ids()))
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = ANY(tapdb_allowed_tenant_ids())
            OR (tenant_id IS NULL AND (
                tapdb_current_tenant_id() IS NULL OR tapdb_allow_global_rows()
            ))
        )
    );

-- Resolve immutable domain/owner bindings once per statement, not per row.
-- Tenant checks and write authority remain unchanged.
ALTER TABLE generic_instance ENABLE ROW LEVEL SECURITY;
ALTER TABLE generic_instance FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS generic_instance_tenant_isolation ON generic_instance;
DROP POLICY IF EXISTS generic_instance_domain_isolation ON generic_instance;
DROP POLICY IF EXISTS generic_instance_scope_isolation ON generic_instance;
CREATE POLICY generic_instance_scope_isolation ON generic_instance
    USING (
        domain_code = (SELECT tapdb_current_domain_code())
        AND issuer_app_code = (SELECT tapdb_current_owner_repo_name())
        AND (tenant_id IS NULL OR tenant_id = ANY(tapdb_allowed_tenant_ids()))
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = ANY(tapdb_allowed_tenant_ids())
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
        domain_code = (SELECT tapdb_current_domain_code())
        AND issuer_app_code = (SELECT tapdb_current_owner_repo_name())
        AND (tenant_id IS NULL OR tenant_id = ANY(tapdb_allowed_tenant_ids()))
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = ANY(tapdb_allowed_tenant_ids())
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
        AND (tenant_id IS NULL OR tenant_id = ANY(tapdb_allowed_tenant_ids()))
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = ANY(tapdb_allowed_tenant_ids())
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
        AND (tenant_id IS NULL OR tenant_id = ANY(tapdb_allowed_tenant_ids()))
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = ANY(tapdb_allowed_tenant_ids())
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
               AND (event.tenant_id IS NULL OR event.tenant_id = ANY(tapdb_allowed_tenant_ids()))
        )
    )
    WITH CHECK (
        EXISTS (
            SELECT 1 FROM outbox_event event
             WHERE event.id = outbox_event_attempt.outbox_event_id
               AND event.domain_code = tapdb_current_domain_code()
               AND event.issuer_app_code = tapdb_current_owner_repo_name()
               AND (
                   event.tenant_id = ANY(tapdb_allowed_tenant_ids())
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
        AND (tenant_id IS NULL OR tenant_id = ANY(tapdb_allowed_tenant_ids()))
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = ANY(tapdb_allowed_tenant_ids())
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
        , 'tapdb_reject_runtime_scope_mutation',
        'set_generic_template_euid',
        'set_generic_instance_euid',
        'set_generic_instance_lineage_euid',
        'set_audit_log_euid'
    ] LOOP
        EXECUTE format(
            'ALTER FUNCTION %I.%I() SET search_path TO %I, pg_catalog, pg_temp',
            scope_schema,
            function_name,
            scope_schema
        );
    END LOOP;
    EXECUTE pg_catalog.format(
        'ALTER FUNCTION %I.tapdb_get_identity_prefix(pg_catalog.text) SET search_path TO %I, pg_catalog, pg_temp',
        scope_schema,
        scope_schema
    );
END;
$tapdb_pin_trigger_search_path$;

ALTER TABLE tapdb_history_epoch ENABLE ROW LEVEL SECURITY;
ALTER TABLE tapdb_history_epoch FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tapdb_history_epoch_scope_isolation ON tapdb_history_epoch;
CREATE POLICY tapdb_history_epoch_scope_isolation ON tapdb_history_epoch USING (true) WITH CHECK (false);

ALTER TABLE tapdb_history_baseline ENABLE ROW LEVEL SECURITY;
ALTER TABLE tapdb_history_baseline FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tapdb_history_baseline_tenant_isolation ON tapdb_history_baseline;
DROP POLICY IF EXISTS tapdb_history_baseline_domain_isolation ON tapdb_history_baseline;
DROP POLICY IF EXISTS tapdb_history_baseline_scope_isolation ON tapdb_history_baseline;
CREATE POLICY tapdb_history_baseline_scope_isolation ON tapdb_history_baseline
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (tenant_id IS NULL OR tenant_id = ANY(tapdb_allowed_tenant_ids()))
    )
    WITH CHECK (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_owner_repo_name()
        AND (
            tenant_id = ANY(tapdb_allowed_tenant_ids())
            OR (tenant_id IS NULL AND (
                tapdb_current_tenant_id() IS NULL OR tapdb_allow_global_rows()
            ))
        )
    );

DROP TRIGGER IF EXISTS tapdb_baseline_no_mutation ON tapdb_history_baseline;
CREATE TRIGGER tapdb_baseline_no_mutation
    BEFORE UPDATE OR DELETE ON tapdb_history_baseline
    FOR EACH ROW EXECUTE FUNCTION tapdb_reject_audit_mutation();
DROP TRIGGER IF EXISTS tapdb_baseline_no_truncate ON tapdb_history_baseline;
CREATE TRIGGER tapdb_baseline_no_truncate
    BEFORE TRUNCATE ON tapdb_history_baseline
    FOR EACH STATEMENT EXECUTE FUNCTION tapdb_reject_audit_mutation();

-- Defense against stale/inherited INSERT grants during explicit adoption.
CREATE OR REPLACE FUNCTION tapdb_guard_audit_insert()
RETURNS TRIGGER AS $$
DECLARE
    writer NAME;
BEGIN
    SELECT pg_catalog.pg_get_userbyid(p.proowner) INTO STRICT writer
    FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
    WHERE n.nspname=TG_TABLE_SCHEMA AND p.proname='record_insert' AND p.pronargs=0;
    IF current_user <> writer OR pg_trigger_depth() < 2 THEN
        RAISE EXCEPTION 'Audit append requires the native domain mutation trigger';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS tapdb_audit_append_guard ON audit_log;
CREATE TRIGGER tapdb_audit_append_guard
    BEFORE INSERT ON audit_log
    FOR EACH ROW EXECUTE FUNCTION tapdb_guard_audit_insert();


-- Native receipt identities cannot be rewritten into replaying a different operation.
CREATE OR REPLACE FUNCTION tapdb_guard_integrity_metadata()
RETURNS TRIGGER AS $$
DECLARE
    props JSONB;
    prior JSONB;
BEGIN
    IF NEW.category='governance' AND NEW.type='correction_receipt'
       AND NEW.subtype='generic' AND NEW.version='1.0' THEN
        IF TG_OP='UPDATE' AND (to_jsonb(OLD)-'modified_dt') IS DISTINCT FROM (to_jsonb(NEW)-'modified_dt') THEN
            RAISE EXCEPTION 'Native correction receipts are immutable';
        END IF;
        props := NEW.json_addl->'properties';
        IF props->>'format' IS DISTINCT FROM 'tapdb.correction-receipt/v1'
           OR jsonb_typeof(props->'revisions') IS DISTINCT FROM 'object'
           OR NULLIF(props->>'operation_id','') IS NULL
           OR NULLIF(props->>'plan_sha256','') IS NULL THEN
            RAISE EXCEPTION 'Invalid native correction receipt';
        END IF;
    END IF;
    IF NEW.category='reference' AND NEW.type='annotation'
       AND NEW.subtype='generic' AND NEW.version='1.0' THEN
        props := NEW.json_addl->'properties';
        IF props->>'format' IS DISTINCT FROM 'tapdb.reference-annotation/v1'
           OR jsonb_typeof(props->'data') IS DISTINCT FROM 'object'
           OR NULLIF(props->>'authority','') IS NULL
           OR NULLIF(props->>'key','') IS NULL THEN
            RAISE EXCEPTION 'Invalid reference annotation';
        END IF;
        IF props->>'authority' IS DISTINCT FROM tapdb_current_attribution()->>'service_identity' THEN
            RAISE EXCEPTION 'Annotation writes require the owning authority';
        END IF;
        IF TG_OP='UPDATE' THEN
            prior := OLD.json_addl->'properties';
            IF props->>'authority' IS DISTINCT FROM prior->>'authority'
               OR props->>'key' IS DISTINCT FROM prior->>'key' THEN
                RAISE EXCEPTION 'Annotation authority and key are immutable';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS tapdb_integrity_metadata_guard ON generic_instance;
CREATE TRIGGER tapdb_integrity_metadata_guard
    BEFORE INSERT OR UPDATE ON generic_instance
    FOR EACH ROW EXECUTE FUNCTION tapdb_guard_integrity_metadata();
DO $tapdb_pin_integrity_search_path$
DECLARE
    scope_schema TEXT := current_schema();
    function_name TEXT;
BEGIN
    FOREACH function_name IN ARRAY ARRAY[
        'tapdb_guard_integrity_metadata', 'tapdb_guard_audit_insert'
    ] LOOP
        EXECUTE format('ALTER FUNCTION %I.%I() SET search_path TO %I, pg_catalog, pg_temp',
            scope_schema, function_name, scope_schema);
    END LOOP;
END;
$tapdb_pin_integrity_search_path$;
