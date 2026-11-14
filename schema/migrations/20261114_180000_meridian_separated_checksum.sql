-- Meridian 1.0.0 format transition: replace functions only.
-- No stored identifiers, relationships, allocator values or floors change.
-- Existing two-hyphen IDs retain their checksum; new allocations use three.

-- Meridian 1.0.0: structural parsing selects v2 or v3 without rewriting IDs.
CREATE OR REPLACE FUNCTION meridian_euid_parts(euid TEXT)
RETURNS TEXT[] LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    parts TEXT[] := string_to_array(euid, '-');
BEGIN
    IF array_length(parts, 1) = 3 AND length(parts[3]) >= 2 THEN
        parts := ARRAY[parts[1], parts[2], left(parts[3], -1), right(parts[3], 1)];
    ELSIF array_length(parts, 1) = 4 THEN
        NULL;
    ELSE
        RAISE EXCEPTION 'Invalid Meridian EUID structure: %', euid;
    END IF;
    IF parts[1] !~ '^[0-9A-HJ-KMNP-TV-Z]{1,4}$'
       OR parts[2] !~ '^[0-9A-HJ-KMNP-TV-Z]{1,4}$'
       OR parts[3] !~ '^[1-9A-HJ-KMNP-TV-Z][0-9A-HJ-KMNP-TV-Z]*$'
       OR parts[4] !~ '^[0-9A-HJ-KMNP-TV-Z]$' THEN
        RAISE EXCEPTION 'Invalid Meridian EUID components: %', euid;
    END IF;
    RETURN parts;
END;
$$;

CREATE OR REPLACE FUNCTION meridian_euid_domain_code(euid TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT AS $$
BEGIN
    RETURN (meridian_euid_parts(euid))[1];
END;
$$;

CREATE OR REPLACE FUNCTION meridian_euid_prefix(euid TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT AS $$
BEGIN
    RETURN (meridian_euid_parts(euid))[2];
END;
$$;

CREATE OR REPLACE FUNCTION meridian_euid_seq_from_euid(euid TEXT)
RETURNS BIGINT LANGUAGE plpgsql IMMUTABLE STRICT AS $$
BEGIN
    RETURN crockford_base32_decode((meridian_euid_parts(euid))[3]);
END;
$$;

-- Original arithmetic is unchanged for v2's separator-free payload.
-- For v3, each separator is zero-valued and consumes a weight position.
CREATE OR REPLACE FUNCTION meridian_luhn_mod32_check(payload TEXT)
RETURNS CHAR LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    alphabet TEXT := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    i INT; ch CHAR; v INT; factor INT; p INT;
    total INT := 0; payload_len INT; check_value INT;
BEGIN
    payload_len := length(payload);
    IF payload_len = 0 THEN
        RAISE EXCEPTION 'Meridian checksum payload must be nonempty';
    END IF;
    FOR i IN REVERSE payload_len..1 LOOP
        ch := substr(payload, i, 1);
        v := CASE WHEN ch = '-' THEN 0 ELSE position(ch IN alphabet) - 1 END;
        IF v < 0 THEN
            RAISE EXCEPTION 'Invalid character "%" in EUID payload', ch;
        END IF;
        factor := CASE WHEN (payload_len - i) % 2 = 0 THEN 2 ELSE 1 END;
        p := v * factor;
        total := total + (p / 32) + (p % 32);
    END LOOP;
    check_value := (32 - (total % 32)) % 32;
    RETURN substr(alphabet, check_value + 1, 1);
END;
$$;

-- Preserve supplied identifiers: compare their own format/checksum and allocation.
CREATE OR REPLACE FUNCTION meridian_euid_matches(euid TEXT, prefix TEXT, seq_val BIGINT, domain_code TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    parts TEXT[] := meridian_euid_parts(euid);
    payload TEXT;
BEGIN
    IF parts[1] <> domain_code OR parts[2] <> prefix
       OR crockford_base32_decode(parts[3]) <> seq_val THEN
        RETURN FALSE;
    END IF;
    IF length(euid) - length(replace(euid, '-', '')) = 2 THEN
        payload := parts[1] || parts[2] || parts[3];
    ELSE
        payload := parts[1] || '-' || parts[2] || '-' || parts[3] || '-';
    END IF;
    RETURN parts[4] = meridian_luhn_mod32_check(payload);
END;
$$;

-- New issuance always uses DOMAIN-PREFIX-BODY-CHECK.
CREATE OR REPLACE FUNCTION meridian_generate_euid(prefix TEXT, seq_val BIGINT, domain_code TEXT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE AS $$
DECLARE
    normalized_prefix TEXT;
    normalized_dc TEXT;
    payload TEXT;
BEGIN
    normalized_prefix := tapdb_validate_meridian_prefix(prefix);
    IF domain_code IS NULL OR trim(domain_code) = '' THEN
        RAISE EXCEPTION 'domain_code is required for Meridian EUID generation';
    END IF;
    normalized_dc := tapdb_validate_domain_code(domain_code);
    payload := normalized_dc || '-' || normalized_prefix || '-'
        || crockford_base32_encode(seq_val) || '-';
    RETURN payload || meridian_luhn_mod32_check(payload);
END;
$$;

CREATE OR REPLACE FUNCTION meridian_generate_euid(prefix TEXT, seq_val BIGINT)
RETURNS TEXT LANGUAGE plpgsql STABLE STRICT AS $$
BEGIN
    RETURN meridian_generate_euid(prefix, seq_val, tapdb_current_domain_code());
END;
$$;

-- tapdb-managed-allocator-functions:start
-- Profiles are explicitly installed by the native operator bootstrap. Prefix
-- projection never changes template type or the session's producer attribution.
CREATE OR REPLACE FUNCTION tapdb_resolve_issuance_prefix(declared_prefix TEXT)
RETURNS TEXT LANGUAGE plpgsql STABLE STRICT AS $$
DECLARE
    profile RECORD;
BEGIN
    EXECUTE pg_catalog.format(
        'SELECT version, mode, prefix, template_prefix FROM %I.tapdb_issuance_profile '
        'WHERE domain_code = $1 AND issuer_app_code = $2', pg_catalog.current_schema()
    ) INTO profile USING tapdb_current_domain_code(), tapdb_current_owner_repo_name();
    IF profile.version IS NULL THEN
        RAISE EXCEPTION 'Missing explicit native issuance profile for domain %, owner %',
            tapdb_current_domain_code(), tapdb_current_owner_repo_name();
    END IF;
    IF profile.version <> 1 THEN
        RAISE EXCEPTION 'Unsupported native issuance profile version %', profile.version;
    END IF;
    IF profile.mode = 'canonical' AND profile.prefix IS NULL THEN
        IF profile.template_prefix IS NOT NULL THEN
            PERFORM tapdb_validate_meridian_prefix(profile.template_prefix);
        END IF;
        RETURN tapdb_validate_meridian_prefix(declared_prefix);
    ELSIF profile.mode = 'single_prefix' AND profile.prefix IS NOT NULL AND profile.template_prefix IS NULL THEN
        RETURN tapdb_validate_meridian_prefix(profile.prefix);
    ELSIF profile.mode = 'template_split' AND profile.prefix IS NOT NULL
          AND profile.template_prefix IS NOT NULL AND profile.template_prefix <> profile.prefix THEN
        PERFORM tapdb_validate_meridian_prefix(profile.template_prefix);
        RETURN tapdb_validate_meridian_prefix(profile.prefix);
    END IF;
    RAISE EXCEPTION 'Malformed explicit native issuance profile';
END;
$$;

CREATE OR REPLACE FUNCTION tapdb_require_instance_prefix(declared_prefix TEXT)
RETURNS TEXT LANGUAGE plpgsql STABLE AS $$
DECLARE
    normalized TEXT := tapdb_validate_meridian_prefix(declared_prefix);
    effective TEXT;
BEGIN
    effective := tapdb_resolve_issuance_prefix(normalized);
    IF normalized IS DISTINCT FROM effective THEN
        RAISE EXCEPTION 'Declared prefix % conflicts with explicit issuance prefix %',
            normalized, effective;
    END IF;
    RETURN effective;
END;
$$;

-- Resolve configured EUID prefix for TapDB-owned identity tables.
CREATE OR REPLACE FUNCTION tapdb_get_identity_prefix(entity_name TEXT)
RETURNS TEXT LANGUAGE plpgsql STABLE STRICT AS $$
DECLARE
    prefix TEXT;
    dc TEXT;
    owner_repo_name TEXT;
    profile RECORD;
    expected TEXT;
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

    -- Validate the complete profile through the instance resolver first. Only
    -- the template's own identity may use the separate template allocator.
    expected := tapdb_resolve_issuance_prefix(prefix);
    EXECUTE pg_catalog.format(
        'SELECT mode, template_prefix FROM %I.tapdb_issuance_profile '
        'WHERE domain_code = $1 AND issuer_app_code = $2', pg_catalog.current_schema()
    ) INTO profile USING dc, owner_repo_name;
    IF entity_name = 'generic_template' AND profile.template_prefix IS NOT NULL THEN
        expected := tapdb_validate_meridian_prefix(profile.template_prefix);
    END IF;
    prefix := tapdb_validate_meridian_prefix(prefix);
    IF prefix IS DISTINCT FROM expected THEN
        RAISE EXCEPTION 'Identity entity % prefix % conflicts with explicit issuance prefix %',
            entity_name, prefix, expected;
    END IF;
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
    template_allocator TEXT;
BEGIN
    -- Populate domain_code and issuer_app_code from session context
    NEW.domain_code := tapdb_current_domain_code();
    NEW.issuer_app_code := tapdb_current_owner_repo_name();
    NEW.instance_prefix := tapdb_require_instance_prefix(NEW.instance_prefix);

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
        EXECUTE pg_catalog.format(
            'SELECT template_prefix FROM %I.tapdb_issuance_profile '
            'WHERE domain_code = $1 AND issuer_app_code = $2', TG_TABLE_SCHEMA
        ) INTO template_allocator USING NEW.domain_code, NEW.issuer_app_code;
        IF template_allocator IS NOT NULL THEN
            prefix := tapdb_validate_meridian_prefix(prefix);
            IF prefix IS DISTINCT FROM tapdb_get_identity_prefix('generic_template') THEN
                RAISE EXCEPTION 'Provided template EUID prefix conflicts with explicit template issuance prefix';
            END IF;
        ELSE
            prefix := tapdb_require_instance_prefix(prefix);
        END IF;
        IF meridian_euid_matches(NEW.euid, prefix, seq_val, dc) IS NOT TRUE THEN
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

        prefix := tapdb_require_instance_prefix(prefix);
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
        prefix := tapdb_require_instance_prefix(prefix);
        IF meridian_euid_matches(NEW.euid, prefix, seq_val, dc) IS NOT TRUE THEN
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
        prefix := tapdb_require_instance_prefix(prefix);
        IF meridian_euid_matches(NEW.euid, prefix, seq_val, dc) IS NOT TRUE THEN
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
        prefix := tapdb_require_instance_prefix(prefix);
        IF meridian_euid_matches(NEW.euid, prefix, seq_val, dc) IS NOT TRUE THEN
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
        'tapdb_resolve_issuance_prefix',
        'tapdb_require_instance_prefix',
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
            CASE WHEN function_name IN ('tapdb_get_identity_prefix', 'tapdb_resolve_issuance_prefix', 'tapdb_require_instance_prefix') THEN 'pg_catalog.text' ELSE '' END,
            scope_schema
        );
    END LOOP;
END;
$tapdb_pin_allocator_search_path$;
-- tapdb-managed-allocator-functions:end
