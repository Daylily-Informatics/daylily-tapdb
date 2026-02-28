-- TAPDB Schema v0.1.0
-- Templated Abstract Polymorphic Database
-- PostgreSQL 13+ required

-- Optional pgcrypto (provides gen_random_uuid()). Some minimal Postgres builds
-- may not ship with contrib extensions.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_available_extensions WHERE name = 'pgcrypto') THEN
        BEGIN
            EXECUTE 'CREATE EXTENSION IF NOT EXISTS pgcrypto';
        EXCEPTION WHEN insufficient_privilege THEN
            -- We'll fall back to tapdb_gen_uuid() when we can't install extensions.
            NULL;
        END;
    END IF;
END $$;

-- UUID generator that prefers pgcrypto's gen_random_uuid(), but falls back to an
-- md5-based UUID when pgcrypto isn't available.
CREATE OR REPLACE FUNCTION tapdb_gen_uuid()
RETURNS UUID AS $$
DECLARE
    v UUID;
BEGIN
    BEGIN
        EXECUTE 'SELECT gen_random_uuid()' INTO v;
        RETURN v;
    EXCEPTION WHEN undefined_function OR feature_not_supported THEN
        RETURN md5(random()::text || clock_timestamp()::text)::uuid;
    END;
END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------
-- SEQUENCES
--------------------------------------------------------------------------------

-- Core sequences (always required)
CREATE SEQUENCE IF NOT EXISTS generic_template_seq;
-- GX is the default instance prefix
CREATE SEQUENCE IF NOT EXISTS gx_instance_seq;
CREATE SEQUENCE IF NOT EXISTS generic_instance_lineage_seq;
CREATE SEQUENCE IF NOT EXISTS audit_log_seq;

-- Optional library sequences
CREATE SEQUENCE IF NOT EXISTS wx_instance_seq;   -- WX (workflow)
CREATE SEQUENCE IF NOT EXISTS wsx_instance_seq;  -- WSX (workflow_step)
CREATE SEQUENCE IF NOT EXISTS xx_instance_seq;   -- XX (action)
CREATE SEQUENCE IF NOT EXISTS ay_instance_seq;   -- AY (assay)

--------------------------------------------------------------------------------
-- TABLES
--------------------------------------------------------------------------------

-- generic_template: Blueprint definitions
CREATE TABLE IF NOT EXISTS generic_template (
    -- Primary identification
    uuid UUID PRIMARY KEY DEFAULT tapdb_gen_uuid(),
    euid TEXT UNIQUE NOT NULL,  -- Meridian EUID set by trigger (set_generic_template_euid)
    name TEXT NOT NULL,

    -- Type hierarchy (category/type/subtype/version)
    polymorphic_discriminator TEXT NOT NULL,
    category TEXT NOT NULL,
    type TEXT NOT NULL,
    subtype TEXT NOT NULL,
    version TEXT NOT NULL,

    CONSTRAINT unique_template_code UNIQUE (category, type, subtype, version),

    -- Instance configuration
    instance_prefix TEXT NOT NULL,
    instance_polymorphic_identity TEXT,

    -- Flexible data storage
    json_addl JSONB NOT NULL DEFAULT '{}'::jsonb,
    json_addl_schema JSONB,

    -- Status and lifecycle
    bstatus TEXT NOT NULL,
    is_singleton BOOLEAN NOT NULL DEFAULT TRUE,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,

    -- Timestamps
    created_dt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_dt TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- generic_instance: Concrete objects created from templates
CREATE TABLE IF NOT EXISTS generic_instance (
    -- Primary identification
    uuid UUID PRIMARY KEY DEFAULT tapdb_gen_uuid(),
    euid TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,

    -- Type hierarchy (copied from template: category/type/subtype/version)
    polymorphic_discriminator TEXT NOT NULL,
    category TEXT NOT NULL,
    type TEXT NOT NULL,
    subtype TEXT NOT NULL,
    version TEXT NOT NULL,

    -- Template reference
    template_uuid UUID NOT NULL REFERENCES generic_template(uuid),

    -- Flexible data storage
    json_addl JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Status and lifecycle
    bstatus TEXT NOT NULL,
    is_singleton BOOLEAN NOT NULL DEFAULT FALSE,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,

    -- Timestamps
    created_dt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_dt TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- generic_instance_lineage: Directed edges between instances
CREATE TABLE IF NOT EXISTS generic_instance_lineage (
    -- Primary identification
    uuid UUID PRIMARY KEY DEFAULT tapdb_gen_uuid(),
    euid TEXT UNIQUE NOT NULL,  -- Meridian EUID set by trigger (set_generic_instance_lineage_euid)
    name TEXT NOT NULL,

    -- Type hierarchy (category/type/subtype/version)
    polymorphic_discriminator TEXT NOT NULL,
    category TEXT NOT NULL DEFAULT 'lineage',
    type TEXT NOT NULL DEFAULT 'lineage',
    subtype TEXT NOT NULL DEFAULT 'generic',
    version TEXT NOT NULL DEFAULT '1.0',

    -- Relationship definition
    parent_instance_uuid UUID NOT NULL REFERENCES generic_instance(uuid),
    child_instance_uuid UUID NOT NULL REFERENCES generic_instance(uuid),
    parent_type TEXT,
    child_type TEXT,
    relationship_type TEXT NOT NULL DEFAULT 'generic',

    -- Flexible data storage
    json_addl JSONB NOT NULL DEFAULT '{}'::jsonb,

    -- Status and lifecycle
    bstatus TEXT NOT NULL DEFAULT 'active',
    is_singleton BOOLEAN NOT NULL DEFAULT FALSE,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,

    -- Timestamps
    created_dt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_dt TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- audit_log: Change tracking
CREATE TABLE IF NOT EXISTS audit_log (
    uuid UUID PRIMARY KEY DEFAULT tapdb_gen_uuid(),
    rel_table_name TEXT NOT NULL,
    column_name TEXT,
    rel_table_uuid_fk UUID NOT NULL,
    rel_table_euid_fk TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT,
    changed_by TEXT,
    changed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    operation_type TEXT CHECK (operation_type IN ('INSERT', 'UPDATE', 'DELETE')),
    json_addl JSONB,
    category TEXT,
    deleted_record_json JSONB,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    is_singleton BOOLEAN NOT NULL DEFAULT FALSE
);

-- tapdb_user: Application user management
CREATE TABLE IF NOT EXISTS tapdb_user (
    uuid UUID PRIMARY KEY DEFAULT tapdb_gen_uuid(),
    username TEXT UNIQUE NOT NULL,
    email TEXT UNIQUE,
    display_name TEXT,
    role TEXT NOT NULL DEFAULT 'user' CHECK (role IN ('admin', 'user')),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    require_password_change BOOLEAN NOT NULL DEFAULT FALSE,
    password_hash TEXT,
    created_dt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_dt TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_login_dt TIMESTAMP WITH TIME ZONE,
    json_addl JSONB DEFAULT '{}'::jsonb
);

-- _tapdb_migrations: migration tracking for schema evolution (not used for fresh installs)
CREATE TABLE IF NOT EXISTS _tapdb_migrations (
    filename TEXT PRIMARY KEY,
    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

--------------------------------------------------------------------------------
-- HARDENING (idempotent-ish ALTERs for existing installs)
--------------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'unique_template_code'
          AND conrelid = 'generic_template'::regclass
    ) THEN
        ALTER TABLE generic_template ADD CONSTRAINT unique_template_code
            UNIQUE (category, type, subtype, version);
    END IF;
END $$;

ALTER TABLE generic_instance ALTER COLUMN euid SET NOT NULL;

ALTER TABLE generic_template ALTER COLUMN json_addl SET DEFAULT '{}'::jsonb;
ALTER TABLE generic_instance ALTER COLUMN json_addl SET DEFAULT '{}'::jsonb;

--------------------------------------------------------------------------------
-- INDEXES
--------------------------------------------------------------------------------

-- generic_template indexes
CREATE INDEX IF NOT EXISTS idx_generic_template_polymorphic_discriminator ON generic_template(polymorphic_discriminator);
CREATE INDEX IF NOT EXISTS idx_generic_template_type ON generic_template(type);
CREATE INDEX IF NOT EXISTS idx_generic_template_is_deleted ON generic_template(is_deleted);

-- generic_instance indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_generic_instance_unique_singleton_key
    ON generic_instance (category, type, subtype, version)
    WHERE is_singleton = TRUE;
CREATE INDEX IF NOT EXISTS idx_generic_instance_polymorphic_discriminator ON generic_instance(polymorphic_discriminator);
CREATE INDEX IF NOT EXISTS idx_generic_instance_type ON generic_instance(type);
CREATE INDEX IF NOT EXISTS idx_generic_instance_euid ON generic_instance(euid);
CREATE INDEX IF NOT EXISTS idx_generic_instance_is_deleted ON generic_instance(is_deleted);
CREATE INDEX IF NOT EXISTS idx_generic_instance_template_uuid ON generic_instance(template_uuid);
CREATE INDEX IF NOT EXISTS idx_generic_instance_category ON generic_instance(category);
CREATE INDEX IF NOT EXISTS idx_generic_instance_subtype ON generic_instance(subtype);
CREATE INDEX IF NOT EXISTS idx_generic_instance_version ON generic_instance(version);
CREATE INDEX IF NOT EXISTS idx_generic_instance_mod_dt ON generic_instance(modified_dt);
CREATE INDEX IF NOT EXISTS idx_generic_instance_singleton ON generic_instance(is_singleton);
CREATE INDEX IF NOT EXISTS idx_generic_instance_json_addl_gin ON generic_instance USING GIN (json_addl);

-- generic_instance_lineage indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_lineage_unique_edge
    ON generic_instance_lineage (parent_instance_uuid, child_instance_uuid, relationship_type)
    WHERE is_deleted = FALSE;
CREATE INDEX IF NOT EXISTS idx_generic_instance_lineage_parent ON generic_instance_lineage(parent_instance_uuid);
CREATE INDEX IF NOT EXISTS idx_generic_instance_lineage_child ON generic_instance_lineage(child_instance_uuid);
CREATE INDEX IF NOT EXISTS idx_generic_instance_lineage_is_deleted ON generic_instance_lineage(is_deleted);

-- audit_log indexes
CREATE INDEX IF NOT EXISTS idx_audit_log_rel_table_name ON audit_log(rel_table_name);
CREATE INDEX IF NOT EXISTS idx_audit_log_rel_table_uuid_fk ON audit_log(rel_table_uuid_fk);
CREATE INDEX IF NOT EXISTS idx_audit_log_rel_table_euid_fk ON audit_log(rel_table_euid_fk);
CREATE INDEX IF NOT EXISTS idx_audit_log_is_deleted ON audit_log(is_deleted);
CREATE INDEX IF NOT EXISTS idx_audit_log_operation_type ON audit_log(operation_type);
CREATE INDEX IF NOT EXISTS idx_audit_log_changed_at ON audit_log(changed_at);
CREATE INDEX IF NOT EXISTS idx_audit_log_changed_by ON audit_log(changed_by);
CREATE INDEX IF NOT EXISTS idx_audit_log_json_addl_gin ON audit_log USING GIN (json_addl);

-- tapdb_user indexes
CREATE INDEX IF NOT EXISTS idx_tapdb_user_role ON tapdb_user(role);
CREATE INDEX IF NOT EXISTS idx_tapdb_user_is_active ON tapdb_user(is_active);


--------------------------------------------------------------------------------
-- FUNCTIONS
--------------------------------------------------------------------------------

-- Meridian EUID: Crockford Base32 encode (positive integer → unpadded text)
CREATE OR REPLACE FUNCTION crockford_base32_encode(val BIGINT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    alphabet TEXT := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    result TEXT := '';
    remainder BIGINT;
BEGIN
    IF val <= 0 THEN
        RAISE EXCEPTION 'EUID body must be a positive integer, got %', val;
    END IF;
    WHILE val > 0 LOOP
        remainder := val % 32;
        result := substr(alphabet, (remainder + 1)::integer, 1) || result;
        val := val / 32;
    END LOOP;
    RETURN result;
END;
$$;

-- Meridian EUID: Luhn-style MOD 32 check character (SPEC.md §7.5)
CREATE OR REPLACE FUNCTION meridian_luhn_mod32_check(payload TEXT)
RETURNS CHAR LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    alphabet TEXT := '0123456789ABCDEFGHJKMNPQRSTVWXYZ';
    i INT; ch CHAR; v INT; factor INT; p INT;
    total INT := 0; payload_len INT; check_value INT;
BEGIN
    payload_len := length(payload);
    FOR i IN REVERSE payload_len..1 LOOP
        ch := substr(payload, i, 1);
        v := position(ch IN alphabet) - 1;
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

-- Meridian EUID: Generate full EUID string (PREFIX-BODYCHECK)
CREATE OR REPLACE FUNCTION meridian_generate_euid(prefix TEXT, seq_val BIGINT)
RETURNS TEXT LANGUAGE plpgsql IMMUTABLE STRICT AS $$
DECLARE
    body TEXT; payload TEXT; check_char CHAR;
BEGIN
    body := crockford_base32_encode(seq_val);
    payload := prefix || body;
    check_char := meridian_luhn_mod32_check(payload);
    RETURN prefix || '-' || body || check_char;
END;
$$;

-- EUID auto-generation for generic_instance (Meridian-conformant)
CREATE OR REPLACE FUNCTION set_generic_instance_euid()
RETURNS TRIGGER AS $$
DECLARE
    prefix TEXT;
    seq_val BIGINT;
    seq_name TEXT;
BEGIN
    -- Get prefix from template
    SELECT instance_prefix INTO prefix FROM generic_template WHERE uuid = NEW.template_uuid;

    -- Default prefix if template not found or no prefix set
    IF prefix IS NULL THEN
        prefix := 'GX';
    END IF;

    -- Dynamic sequence resolution (allows new prefixes without trigger changes)
    seq_name := lower(prefix) || '_instance_seq';

    BEGIN
        -- Try to use prefix-specific sequence
        EXECUTE format('SELECT nextval(%L)', seq_name) INTO seq_val;
	EXCEPTION WHEN undefined_table OR undefined_object THEN
	    RAISE EXCEPTION
	        'Missing EUID sequence % for instance_prefix %. Create and initialize it before inserting instances.',
	        seq_name, prefix;
    END;

    NEW.euid := meridian_generate_euid(prefix, seq_val);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- EUID auto-generation for generic_template (Meridian-conformant)
CREATE OR REPLACE FUNCTION set_generic_template_euid()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.euid IS NULL OR NEW.euid = '' THEN
        NEW.euid := meridian_generate_euid('GT', nextval('generic_template_seq'));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- EUID auto-generation for generic_instance_lineage (Meridian-conformant)
CREATE OR REPLACE FUNCTION set_generic_instance_lineage_euid()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.euid IS NULL OR NEW.euid = '' THEN
        NEW.euid := meridian_generate_euid('GN', nextval('generic_instance_lineage_seq'));
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Soft delete function (prevents actual deletion, sets is_deleted flag)
CREATE OR REPLACE FUNCTION soft_delete_row()
RETURNS TRIGGER AS $$
DECLARE
    app_username TEXT;
BEGIN
    -- Get current user for audit
    BEGIN
        app_username := current_setting('session.current_username', true);
    EXCEPTION WHEN OTHERS THEN
        app_username := current_user;
    END;

    -- Soft delete only the row in the triggering table (dynamic SQL)
    EXECUTE format('UPDATE %I SET is_deleted = TRUE WHERE uuid = $1', TG_TABLE_NAME)
    USING OLD.uuid;

    -- Record deletion in audit log with full record snapshot
    INSERT INTO audit_log (
        rel_table_name, rel_table_uuid_fk, rel_table_euid_fk,
        changed_by, operation_type, old_value
    ) VALUES (
        TG_TABLE_NAME, OLD.uuid, OLD.euid,
        app_username, 'DELETE', row_to_json(OLD)::TEXT
    );

    RETURN NULL;  -- Prevent actual deletion
END;
$$ LANGUAGE plpgsql;

-- Record UPDATE operations in audit log
CREATE OR REPLACE FUNCTION record_update()
RETURNS TRIGGER AS $$
DECLARE
    r RECORD;
    column_name TEXT;
    old_value TEXT;
    new_value TEXT;
    app_username TEXT;
BEGIN
    BEGIN
        app_username := current_setting('session.current_username', true);
    EXCEPTION WHEN OTHERS THEN
        app_username := current_user;
    END;

    FOR r IN SELECT * FROM json_each_text(row_to_json(NEW)) LOOP
        column_name := r.key;
        new_value := r.value;
        EXECUTE format('SELECT ($1).%I', column_name) USING OLD INTO old_value;

        IF old_value IS DISTINCT FROM new_value THEN
            INSERT INTO audit_log (rel_table_name, column_name, old_value, new_value,
                                   changed_by, rel_table_uuid_fk, rel_table_euid_fk, operation_type)
            VALUES (TG_TABLE_NAME, column_name, old_value, new_value,
                    app_username, NEW.uuid, NEW.euid, TG_OP);
        END IF;
    END LOOP;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Record INSERT operations in audit log
CREATE OR REPLACE FUNCTION record_insert()
RETURNS TRIGGER AS $$
DECLARE
    app_username TEXT;
BEGIN
    BEGIN
        app_username := current_setting('session.current_username', true);
    EXCEPTION WHEN OTHERS THEN
        app_username := current_user;
    END;

    INSERT INTO audit_log (rel_table_name, rel_table_uuid_fk, rel_table_euid_fk,
                           changed_by, operation_type)
    VALUES (TG_TABLE_NAME, NEW.uuid, NEW.euid, app_username, 'INSERT');

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Auto-update modified_dt timestamp
CREATE OR REPLACE FUNCTION update_modified_dt()
RETURNS TRIGGER AS $$
BEGIN
    NEW.modified_dt = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;


--------------------------------------------------------------------------------
-- TRIGGERS
--------------------------------------------------------------------------------

-- EUID trigger for generic_template (Meridian-conformant)
DROP TRIGGER IF EXISTS trigger_set_generic_template_euid ON generic_template;
CREATE TRIGGER trigger_set_generic_template_euid
    BEFORE INSERT ON generic_template
    FOR EACH ROW EXECUTE FUNCTION set_generic_template_euid();

-- EUID trigger for generic_instance (Meridian-conformant)
DROP TRIGGER IF EXISTS trigger_set_generic_instance_euid ON generic_instance;
CREATE TRIGGER trigger_set_generic_instance_euid
    BEFORE INSERT ON generic_instance
    FOR EACH ROW EXECUTE FUNCTION set_generic_instance_euid();

-- EUID trigger for generic_instance_lineage (Meridian-conformant)
DROP TRIGGER IF EXISTS trigger_set_generic_instance_lineage_euid ON generic_instance_lineage;
CREATE TRIGGER trigger_set_generic_instance_lineage_euid
    BEFORE INSERT ON generic_instance_lineage
    FOR EACH ROW EXECUTE FUNCTION set_generic_instance_lineage_euid();

-- Soft delete triggers (BEFORE DELETE)
DROP TRIGGER IF EXISTS soft_delete_generic_template ON generic_template;
CREATE TRIGGER soft_delete_generic_template
    BEFORE DELETE ON generic_template
    FOR EACH ROW EXECUTE FUNCTION soft_delete_row();

DROP TRIGGER IF EXISTS soft_delete_generic_instance ON generic_instance;
CREATE TRIGGER soft_delete_generic_instance
    BEFORE DELETE ON generic_instance
    FOR EACH ROW EXECUTE FUNCTION soft_delete_row();

DROP TRIGGER IF EXISTS soft_delete_generic_instance_lineage ON generic_instance_lineage;
CREATE TRIGGER soft_delete_generic_instance_lineage
    BEFORE DELETE ON generic_instance_lineage
    FOR EACH ROW EXECUTE FUNCTION soft_delete_row();

-- Audit triggers (AFTER INSERT)
DROP TRIGGER IF EXISTS audit_insert_generic_template ON generic_template;
CREATE TRIGGER audit_insert_generic_template
    AFTER INSERT ON generic_template
    FOR EACH ROW EXECUTE FUNCTION record_insert();

DROP TRIGGER IF EXISTS audit_insert_generic_instance ON generic_instance;
CREATE TRIGGER audit_insert_generic_instance
    AFTER INSERT ON generic_instance
    FOR EACH ROW EXECUTE FUNCTION record_insert();

DROP TRIGGER IF EXISTS audit_insert_generic_instance_lineage ON generic_instance_lineage;
CREATE TRIGGER audit_insert_generic_instance_lineage
    AFTER INSERT ON generic_instance_lineage
    FOR EACH ROW EXECUTE FUNCTION record_insert();

-- Audit triggers (AFTER UPDATE)
DROP TRIGGER IF EXISTS audit_update_generic_template ON generic_template;
CREATE TRIGGER audit_update_generic_template
    AFTER UPDATE ON generic_template
    FOR EACH ROW EXECUTE FUNCTION record_update();

DROP TRIGGER IF EXISTS audit_update_generic_instance ON generic_instance;
CREATE TRIGGER audit_update_generic_instance
    AFTER UPDATE ON generic_instance
    FOR EACH ROW EXECUTE FUNCTION record_update();

DROP TRIGGER IF EXISTS audit_update_generic_instance_lineage ON generic_instance_lineage;
CREATE TRIGGER audit_update_generic_instance_lineage
    AFTER UPDATE ON generic_instance_lineage
    FOR EACH ROW EXECUTE FUNCTION record_update();

-- Modified timestamp triggers (BEFORE UPDATE)
DROP TRIGGER IF EXISTS update_modified_dt_generic_template ON generic_template;
CREATE TRIGGER update_modified_dt_generic_template
    BEFORE UPDATE ON generic_template
    FOR EACH ROW EXECUTE FUNCTION update_modified_dt();

DROP TRIGGER IF EXISTS update_modified_dt_generic_instance ON generic_instance;
CREATE TRIGGER update_modified_dt_generic_instance
    BEFORE UPDATE ON generic_instance
    FOR EACH ROW EXECUTE FUNCTION update_modified_dt();

DROP TRIGGER IF EXISTS update_modified_dt_generic_instance_lineage ON generic_instance_lineage;
CREATE TRIGGER update_modified_dt_generic_instance_lineage
    BEFORE UPDATE ON generic_instance_lineage
    FOR EACH ROW EXECUTE FUNCTION update_modified_dt();

--------------------------------------------------------------------------------
-- END OF SCHEMA
--------------------------------------------------------------------------------
