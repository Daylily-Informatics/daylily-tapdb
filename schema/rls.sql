-- Row Level Security (RLS) scaffolding for TapDB tenant isolation.
--
-- This file is intentionally NOT applied by default.
-- Apply manually once the application sets `session.current_tenant_id` per request/txn.
--
-- Example (per-transaction):
--   SET LOCAL session.current_tenant_id = '00000000-0000-0000-0000-000000000000';

-- generic_template (optional; templates may be global/shared)
ALTER TABLE generic_template ENABLE ROW LEVEL SECURITY;
CREATE POLICY generic_template_tenant_isolation
    ON generic_template
    USING (tenant_id IS NULL OR tenant_id = current_setting('session.current_tenant_id', true)::uuid);

-- generic_instance
ALTER TABLE generic_instance ENABLE ROW LEVEL SECURITY;
CREATE POLICY generic_instance_tenant_isolation
    ON generic_instance
    USING (tenant_id = current_setting('session.current_tenant_id', true)::uuid);

-- generic_instance_lineage
ALTER TABLE generic_instance_lineage ENABLE ROW LEVEL SECURITY;
CREATE POLICY generic_instance_lineage_tenant_isolation
    ON generic_instance_lineage
    USING (tenant_id = current_setting('session.current_tenant_id', true)::uuid);

-- audit_log
ALTER TABLE audit_log ENABLE ROW LEVEL SECURITY;
CREATE POLICY audit_log_tenant_isolation
    ON audit_log
    USING (tenant_id = current_setting('session.current_tenant_id', true)::uuid);

-- outbox_event
ALTER TABLE outbox_event ENABLE ROW LEVEL SECURITY;
CREATE POLICY outbox_event_tenant_isolation
    ON outbox_event
    USING (tenant_id = current_setting('session.current_tenant_id', true)::uuid);

