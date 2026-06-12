-- Derive instance EUID prefixes from the referenced template, not taxonomy.
-- This is DDL-only function evolution; it is not evidence migration.

BEGIN;

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
        SELECT t.instance_prefix INTO prefix
          FROM generic_template t
         WHERE t.uid = NEW.template_uid
           AND t.domain_code = NEW.domain_code
           AND t.issuer_app_code = NEW.issuer_app_code
           AND t.is_deleted IS FALSE;

        IF prefix IS NULL THEN
            RAISE EXCEPTION
                'Missing template instance_prefix for generic_instance template_uid % (domain=%, owner_repo=%)',
                NEW.template_uid, NEW.domain_code, NEW.issuer_app_code;
        END IF;

        prefix := tapdb_validate_meridian_prefix(prefix);
        seq_name := lower(prefix) || '_instance_seq';

        BEGIN
            EXECUTE format('SELECT nextval(%L)', seq_name) INTO seq_val;
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

COMMIT;
