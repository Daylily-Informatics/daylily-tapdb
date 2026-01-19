-- TAPDB Schema v0.1.0
-- Templated Abstract Polymorphic Database
-- PostgreSQL 14+ required (for gen_random_uuid())

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;

--------------------------------------------------------------------------------
-- SEQUENCES
--------------------------------------------------------------------------------

-- Core sequences (always required)
CREATE SEQUENCE IF NOT EXISTS generic_template_seq;
CREATE SEQUENCE IF NOT EXISTS generic_instance_seq;
CREATE SEQUENCE IF NOT EXISTS generic_instance_lineage_seq;
CREATE SEQUENCE IF NOT EXISTS audit_log_seq;

-- Optional library sequences
CREATE SEQUENCE IF NOT EXISTS wx_instance_seq;   -- WX (workflow)
CREATE SEQUENCE IF NOT EXISTS wsx_instance_seq;  -- WSX (workflow_step)
CREATE SEQUENCE IF NOT EXISTS xx_instance_seq;   -- XX (action)

--------------------------------------------------------------------------------
-- TABLES
--------------------------------------------------------------------------------

-- generic_template: Blueprint definitions
CREATE TABLE IF NOT EXISTS generic_template (
    -- Primary identification
    uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    euid TEXT UNIQUE NOT NULL DEFAULT ('GT' || nextval('generic_template_seq')),
    name TEXT NOT NULL,

    -- Type hierarchy
    polymorphic_discriminator TEXT NOT NULL,
    super_type TEXT NOT NULL,
    btype TEXT NOT NULL,
    b_sub_type TEXT NOT NULL,
    version TEXT NOT NULL,

    -- Instance configuration
    instance_prefix TEXT NOT NULL,
    instance_polymorphic_identity TEXT,

    -- Flexible data storage
    json_addl JSONB NOT NULL,
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
    uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    euid TEXT UNIQUE,
    name TEXT NOT NULL,

    -- Type hierarchy (copied from template)
    polymorphic_discriminator TEXT NOT NULL,
    super_type TEXT NOT NULL,
    btype TEXT NOT NULL,
    b_sub_type TEXT NOT NULL,
    version TEXT NOT NULL,

    -- Template reference
    template_uuid UUID NOT NULL REFERENCES generic_template(uuid),

    -- Flexible data storage
    json_addl JSONB NOT NULL,

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
    uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    euid TEXT UNIQUE NOT NULL DEFAULT ('GL' || nextval('generic_instance_lineage_seq')),
    name TEXT NOT NULL,

    -- Type hierarchy
    polymorphic_discriminator TEXT NOT NULL,
    super_type TEXT NOT NULL DEFAULT 'lineage',
    btype TEXT NOT NULL DEFAULT 'lineage',
    b_sub_type TEXT NOT NULL DEFAULT 'generic',
    version TEXT NOT NULL DEFAULT '1.0',

    -- Relationship definition
    parent_instance_uuid UUID NOT NULL REFERENCES generic_instance(uuid),
    child_instance_uuid UUID NOT NULL REFERENCES generic_instance(uuid),
    parent_type TEXT,
    child_type TEXT,
    relationship_type TEXT NOT NULL DEFAULT 'generic',

    -- Flexible data storage
    json_addl JSONB NOT NULL DEFAULT '{}',

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
    uuid UUID PRIMARY KEY DEFAULT gen_random_uuid(),
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
    super_type TEXT,
    deleted_record_json JSONB,
    is_deleted BOOLEAN NOT NULL DEFAULT FALSE,
    is_singleton BOOLEAN NOT NULL DEFAULT FALSE
);

--------------------------------------------------------------------------------
-- INDEXES
--------------------------------------------------------------------------------

-- generic_template indexes
CREATE INDEX IF NOT EXISTS idx_generic_template_polymorphic_discriminator ON generic_template(polymorphic_discriminator);
CREATE INDEX IF NOT EXISTS idx_generic_template_btype ON generic_template(btype);
CREATE INDEX IF NOT EXISTS idx_generic_template_is_deleted ON generic_template(is_deleted);

-- generic_instance indexes
CREATE UNIQUE INDEX IF NOT EXISTS idx_generic_instance_unique_singleton_key
    ON generic_instance (super_type, btype, b_sub_type, version)
    WHERE is_singleton = TRUE;
CREATE INDEX IF NOT EXISTS idx_generic_instance_polymorphic_discriminator ON generic_instance(polymorphic_discriminator);
CREATE INDEX IF NOT EXISTS idx_generic_instance_type ON generic_instance(btype);
CREATE INDEX IF NOT EXISTS idx_generic_instance_euid ON generic_instance(euid);
CREATE INDEX IF NOT EXISTS idx_generic_instance_is_deleted ON generic_instance(is_deleted);
CREATE INDEX IF NOT EXISTS idx_generic_instance_template_uuid ON generic_instance(template_uuid);
CREATE INDEX IF NOT EXISTS idx_generic_instance_super_type ON generic_instance(super_type);
CREATE INDEX IF NOT EXISTS idx_generic_instance_b_sub_type ON generic_instance(b_sub_type);
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


--------------------------------------------------------------------------------
-- FUNCTIONS
--------------------------------------------------------------------------------

-- EUID auto-generation for generic_instance
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
    EXCEPTION WHEN undefined_table THEN
        -- Fallback to generic sequence if prefix sequence doesn't exist
        seq_val := nextval('generic_instance_seq');
    END;

    NEW.euid := prefix || seq_val;
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

-- EUID trigger for generic_instance
DROP TRIGGER IF EXISTS trigger_set_generic_instance_euid ON generic_instance;
CREATE TRIGGER trigger_set_generic_instance_euid
    BEFORE INSERT ON generic_instance
    FOR EACH ROW EXECUTE FUNCTION set_generic_instance_euid();

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
