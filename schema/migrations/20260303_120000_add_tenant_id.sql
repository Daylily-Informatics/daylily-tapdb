-- 20260303_120000_add_tenant_id.sql
--
-- Add nullable tenant_id UUID columns to TapDB core tables and audit_log.
-- Update audit trigger functions to persist tenant_id into audit_log rows.

ALTER TABLE generic_template ADD COLUMN IF NOT EXISTS tenant_id UUID;
ALTER TABLE generic_instance ADD COLUMN IF NOT EXISTS tenant_id UUID;
ALTER TABLE generic_instance_lineage ADD COLUMN IF NOT EXISTS tenant_id UUID;
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS tenant_id UUID;

CREATE INDEX IF NOT EXISTS idx_generic_instance_tenant_template_created_dt
    ON generic_instance (tenant_id, category, type, subtype, version, created_dt DESC);

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
        tenant_id, changed_by, operation_type, old_value
    ) VALUES (
        TG_TABLE_NAME, OLD.uuid, OLD.euid,
        OLD.tenant_id, app_username, 'DELETE', row_to_json(OLD)::TEXT
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
                                   changed_by, rel_table_uuid_fk, rel_table_euid_fk,
                                   tenant_id, operation_type)
            VALUES (TG_TABLE_NAME, column_name, old_value, new_value,
                    app_username, NEW.uuid, NEW.euid,
                    NEW.tenant_id, TG_OP);
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
                           tenant_id, changed_by, operation_type)
    VALUES (TG_TABLE_NAME, NEW.uuid, NEW.euid, NEW.tenant_id, app_username, 'INSERT');

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Recreate triggers to ensure they reference updated functions.

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

