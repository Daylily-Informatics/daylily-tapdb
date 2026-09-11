-- tapdb-managed-allocator-functions:start
-- Resolve configured EUID prefix for TapDB-owned identity tables.
CREATE OR REPLACE FUNCTION tapdb_get_identity_prefix(entity_name TEXT)
RETURNS TEXT LANGUAGE plpgsql STABLE STRICT AS $$
DECLARE
    prefix TEXT;
    dc TEXT;
    owner_repo_name TEXT;
BEGIN
    dc := tapdb_current_domain_code();
    owner_repo_name := tapdb_current_owner_repo_name();

    EXECUTE pg_catalog.format(
        'SELECT p.prefix FROM %I.tapdb_identity_prefix_config p '
        'WHERE p.entity = $1 AND p.domain_code = $2 AND p.issuer_app_code = $3',
        pg_catalog.current_schema()
    ) INTO prefix USING entity_name, dc, owner_repo_name;

    IF prefix IS NULL THEN
        RAISE EXCEPTION
            'Missing EUID prefix configuration for entity "%" (domain=%, owner_repo=%) in tapdb_identity_prefix_config',
            entity_name, dc, owner_repo_name;
    END IF;

    prefix := tapdb_validate_meridian_prefix(prefix);
    IF prefix IN ('GX', 'TGX') THEN
        RAISE EXCEPTION
            'Identity entity "%" is configured with unresolved/reserved prefix "%"',
            entity_name,
            prefix;
    END IF;

    RETURN prefix;
END;
$$;

-- EUID auto-generation for generic_template (Meridian-conformant)
CREATE OR REPLACE FUNCTION set_generic_template_euid()
RETURNS TRIGGER AS $$
DECLARE
    prefix TEXT;
    dc TEXT;
    seq_val BIGINT;
    seq_name TEXT;
BEGIN
    -- Populate domain_code and issuer_app_code from session context
    NEW.domain_code := tapdb_current_domain_code();
    NEW.issuer_app_code := tapdb_current_owner_repo_name();

    IF NEW.euid IS NULL OR NEW.euid = '' THEN
        prefix := tapdb_get_identity_prefix('generic_template');
        seq_name := pg_catalog.format(
            '%I.%I', TG_TABLE_SCHEMA, pg_catalog.lower(prefix) || '_instance_seq'
        );
        BEGIN
            seq_val := pg_catalog.nextval(seq_name::pg_catalog.regclass);
        EXCEPTION WHEN undefined_table OR undefined_object THEN
            RAISE EXCEPTION
                'Missing EUID sequence % for prefix %. Create and initialize it before inserting rows.',
                seq_name, prefix;
        END;
        NEW.euid_prefix := prefix;
        NEW.euid_seq := seq_val;
        NEW.euid := meridian_generate_euid(prefix, seq_val);
    ELSE
        prefix := COALESCE(NEW.euid_prefix, meridian_euid_prefix(NEW.euid));
        seq_val := COALESCE(NEW.euid_seq, meridian_euid_seq_from_euid(NEW.euid));
        dc := meridian_euid_domain_code(NEW.euid);
        prefix := tapdb_validate_meridian_prefix(prefix);
        IF NEW.euid <> meridian_generate_euid(prefix, seq_val, dc) THEN
            RAISE EXCEPTION 'Provided EUID does not match provided/generated prefix+seq: %', NEW.euid;
        END IF;
        NEW.euid_prefix := prefix;
        NEW.euid_seq := seq_val;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- EUID auto-generation for generic_instance (Meridian-conformant)
CREATE OR REPLACE FUNCTION set_generic_instance_euid()
RETURNS TRIGGER AS $$
DECLARE
    prefix TEXT;
    dc TEXT;
    seq_val BIGINT;
    seq_name TEXT;
BEGIN
    -- Populate domain_code and issuer_app_code from session context
    NEW.domain_code := tapdb_current_domain_code();
    NEW.issuer_app_code := tapdb_current_owner_repo_name();

    IF NEW.euid IS NULL OR NEW.euid = '' THEN
        EXECUTE pg_catalog.format(
            'SELECT t.instance_prefix FROM %I.generic_template t '
            'WHERE t.uid = $1 AND t.domain_code = $2 AND t.issuer_app_code = $3 '
            'AND t.is_deleted IS FALSE',
            TG_TABLE_SCHEMA
        ) INTO prefix USING NEW.template_uid, NEW.domain_code, NEW.issuer_app_code;

        IF prefix IS NULL THEN
            RAISE EXCEPTION
                'Missing template instance_prefix for generic_instance template_uid % (domain=%, owner_repo=%)',
                NEW.template_uid, NEW.domain_code, NEW.issuer_app_code;
        END IF;

        prefix := tapdb_validate_meridian_prefix(prefix);
        seq_name := pg_catalog.format(
            '%I.%I', TG_TABLE_SCHEMA, pg_catalog.lower(prefix) || '_instance_seq'
        );

        BEGIN
            seq_val := pg_catalog.nextval(seq_name::pg_catalog.regclass);
        EXCEPTION WHEN undefined_table OR undefined_object THEN
            RAISE EXCEPTION
                'Missing EUID sequence % for instance_prefix %. Create and initialize it before inserting instances.',
                seq_name, prefix;
        END;

        NEW.euid_prefix := prefix;
        NEW.euid_seq := seq_val;
        NEW.euid := meridian_generate_euid(prefix, seq_val);
    ELSE
        prefix := COALESCE(NEW.euid_prefix, meridian_euid_prefix(NEW.euid));
        seq_val := COALESCE(NEW.euid_seq, meridian_euid_seq_from_euid(NEW.euid));
        dc := meridian_euid_domain_code(NEW.euid);
        prefix := tapdb_validate_meridian_prefix(prefix);
        IF NEW.euid <> meridian_generate_euid(prefix, seq_val, dc) THEN
            RAISE EXCEPTION 'Provided EUID does not match provided/generated prefix+seq: %', NEW.euid;
        END IF;
        NEW.euid_prefix := prefix;
        NEW.euid_seq := seq_val;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- EUID auto-generation for generic_instance_lineage (Meridian-conformant)
CREATE OR REPLACE FUNCTION set_generic_instance_lineage_euid()
RETURNS TRIGGER AS $$
DECLARE
    prefix TEXT;
    dc TEXT;
    seq_val BIGINT;
    seq_name TEXT;
BEGIN
    -- Populate domain_code and issuer_app_code from session context
    NEW.domain_code := tapdb_current_domain_code();
    NEW.issuer_app_code := tapdb_current_owner_repo_name();

    IF NEW.euid IS NULL OR NEW.euid = '' THEN
        prefix := tapdb_get_identity_prefix('generic_instance_lineage');
        seq_name := pg_catalog.format(
            '%I.%I', TG_TABLE_SCHEMA, pg_catalog.lower(prefix) || '_instance_seq'
        );
        BEGIN
            seq_val := pg_catalog.nextval(seq_name::pg_catalog.regclass);
        EXCEPTION WHEN undefined_table OR undefined_object THEN
            RAISE EXCEPTION
                'Missing EUID sequence % for prefix %. Create and initialize it before inserting rows.',
                seq_name, prefix;
        END;
        NEW.euid_prefix := prefix;
        NEW.euid_seq := seq_val;
        NEW.euid := meridian_generate_euid(prefix, seq_val);
    ELSE
        prefix := COALESCE(NEW.euid_prefix, meridian_euid_prefix(NEW.euid));
        seq_val := COALESCE(NEW.euid_seq, meridian_euid_seq_from_euid(NEW.euid));
        dc := meridian_euid_domain_code(NEW.euid);
        prefix := tapdb_validate_meridian_prefix(prefix);
        IF NEW.euid <> meridian_generate_euid(prefix, seq_val, dc) THEN
            RAISE EXCEPTION 'Provided EUID does not match provided/generated prefix+seq: %', NEW.euid;
        END IF;
        NEW.euid_prefix := prefix;
        NEW.euid_seq := seq_val;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- EUID auto-generation for audit_log (config-driven prefix)
CREATE OR REPLACE FUNCTION set_audit_log_euid()
RETURNS TRIGGER AS $$
DECLARE
    prefix TEXT;
    dc TEXT;
    seq_val BIGINT;
    seq_name TEXT;
BEGIN
    -- Populate domain_code and issuer_app_code from session context
    NEW.domain_code := tapdb_current_domain_code();
    NEW.issuer_app_code := tapdb_current_owner_repo_name();

    IF NEW.euid IS NULL OR NEW.euid = '' THEN
        prefix := tapdb_get_identity_prefix('audit_log');
        seq_name := pg_catalog.format(
            '%I.%I', TG_TABLE_SCHEMA, pg_catalog.lower(prefix) || '_instance_seq'
        );
        BEGIN
            seq_val := pg_catalog.nextval(seq_name::pg_catalog.regclass);
        EXCEPTION WHEN undefined_table OR undefined_object THEN
            RAISE EXCEPTION
                'Missing EUID sequence % for prefix %. Create and initialize it before inserting rows.',
                seq_name, prefix;
        END;

        NEW.euid_prefix := prefix;
        NEW.euid_seq := seq_val;
        NEW.euid := meridian_generate_euid(prefix, seq_val);
    ELSE
        prefix := COALESCE(NEW.euid_prefix, meridian_euid_prefix(NEW.euid));
        seq_val := COALESCE(NEW.euid_seq, meridian_euid_seq_from_euid(NEW.euid));
        dc := meridian_euid_domain_code(NEW.euid);
        prefix := tapdb_validate_meridian_prefix(prefix);
        IF NEW.euid <> meridian_generate_euid(prefix, seq_val, dc) THEN
            RAISE EXCEPTION 'Provided EUID does not match provided/generated prefix+seq: %', NEW.euid;
        END IF;
        NEW.euid_prefix := prefix;
        NEW.euid_seq := seq_val;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Keep control-table and helper resolution bound to the operator-owned schema.
-- Explicit relation qualification above also prevents a missing persistent
-- object from falling through to a temporary object with the same name.
DO $tapdb_pin_allocator_search_path$
DECLARE
    scope_schema TEXT := pg_catalog.current_schema();
    function_name TEXT;
BEGIN
    FOREACH function_name IN ARRAY ARRAY[
        'tapdb_get_identity_prefix',
        'set_generic_template_euid',
        'set_generic_instance_euid',
        'set_generic_instance_lineage_euid',
        'set_audit_log_euid'
    ] LOOP
        EXECUTE pg_catalog.format(
            'ALTER FUNCTION %I.%I(%s) SET search_path TO %I, pg_catalog, pg_temp',
            scope_schema,
            function_name,
            CASE WHEN function_name = 'tapdb_get_identity_prefix' THEN 'pg_catalog.text' ELSE '' END,
            scope_schema
        );
    END LOOP;
END;
$tapdb_pin_allocator_search_path$;
-- tapdb-managed-allocator-functions:end
