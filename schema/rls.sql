-- Row Level Security (RLS) for TapDB — compound domain + tenant isolation.
--
-- This file is intentionally NOT applied by default.
-- Apply manually once the application sets session variables per request/txn.
--
-- Required session variables (set by TAPDBConnection per-session):
--   session.current_domain_code    — Crockford 1-4 char domain code
--   session.current_app_code       — Crockford 1-4 char issuer app code
--   session.current_tenant_id      — UUID (optional for templates)
--
-- Example (per-transaction):
--   SET LOCAL session.current_domain_code = 'T';
--   SET LOCAL session.current_app_code    = 'TAPD';
--   SET LOCAL session.current_tenant_id   = '00000000-0000-0000-0000-000000000000';

-- ---------------------------------------------------------------------------
-- Helper: check domain + app match
-- ---------------------------------------------------------------------------
-- Rows match if their domain_code + issuer_app_code match the session.
-- Empty domain_code on the row means "global" (visible to all domains).

-- ---------------------------------------------------------------------------
-- generic_template (templates may be global, so tenant_id is nullable)
-- ---------------------------------------------------------------------------
ALTER TABLE generic_template ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS generic_template_tenant_isolation ON generic_template;
CREATE POLICY generic_template_domain_isolation
    ON generic_template
    USING (
        (domain_code = '' OR domain_code = tapdb_current_domain_code())
        AND (issuer_app_code = '' OR issuer_app_code = tapdb_current_app_code())
        AND (tenant_id IS NULL OR tenant_id = current_setting('session.current_tenant_id', true)::uuid)
    );

-- ---------------------------------------------------------------------------
-- generic_instance
-- ---------------------------------------------------------------------------
ALTER TABLE generic_instance ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS generic_instance_tenant_isolation ON generic_instance;
CREATE POLICY generic_instance_domain_isolation
    ON generic_instance
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_app_code()
        AND (tenant_id IS NULL OR tenant_id = current_setting('session.current_tenant_id', true)::uuid)
    );

-- ---------------------------------------------------------------------------
-- generic_instance_lineage
-- ---------------------------------------------------------------------------
ALTER TABLE generic_instance_lineage ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS generic_instance_lineage_tenant_isolation ON generic_instance_lineage;
CREATE POLICY generic_instance_lineage_domain_isolation
    ON generic_instance_lineage
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_app_code()
        AND (tenant_id IS NULL OR tenant_id = current_setting('session.current_tenant_id', true)::uuid)
    );

-- ---------------------------------------------------------------------------
-- audit_log
-- ---------------------------------------------------------------------------
ALTER TABLE audit_log ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS audit_log_tenant_isolation ON audit_log;
CREATE POLICY audit_log_domain_isolation
    ON audit_log
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_app_code()
        AND (tenant_id IS NULL OR tenant_id = current_setting('session.current_tenant_id', true)::uuid)
    );

-- ---------------------------------------------------------------------------
-- outbox_event
-- ---------------------------------------------------------------------------
ALTER TABLE outbox_event ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_event_tenant_isolation ON outbox_event;
CREATE POLICY outbox_event_domain_isolation
    ON outbox_event
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_app_code()
        AND (tenant_id IS NULL OR tenant_id = current_setting('session.current_tenant_id', true)::uuid)
    );

-- ---------------------------------------------------------------------------
-- outbox_event_attempt (scoped via outbox_event FK, but add domain for safety)
-- ---------------------------------------------------------------------------
ALTER TABLE outbox_event_attempt ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS outbox_event_attempt_domain_isolation ON outbox_event_attempt;
CREATE POLICY outbox_event_attempt_domain_isolation
    ON outbox_event_attempt
    USING (
        EXISTS (
            SELECT 1 FROM outbox_event e
            WHERE e.id = outbox_event_attempt.outbox_event_id
              AND e.domain_code = tapdb_current_domain_code()
              AND e.issuer_app_code = tapdb_current_app_code()
        )
    );

-- ---------------------------------------------------------------------------
-- inbox_message
-- ---------------------------------------------------------------------------
ALTER TABLE inbox_message ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS inbox_message_domain_isolation ON inbox_message;
CREATE POLICY inbox_message_domain_isolation
    ON inbox_message
    USING (
        domain_code = tapdb_current_domain_code()
        AND issuer_app_code = tapdb_current_app_code()
        AND (tenant_id IS NULL OR tenant_id = current_setting('session.current_tenant_id', true)::uuid)
    );

-- ---------------------------------------------------------------------------
-- tapdb_identity_prefix_config (scoped or global)
-- ---------------------------------------------------------------------------
ALTER TABLE tapdb_identity_prefix_config ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tapdb_identity_prefix_config_domain_isolation ON tapdb_identity_prefix_config;
CREATE POLICY tapdb_identity_prefix_config_domain_isolation
    ON tapdb_identity_prefix_config
    USING (
        domain_code = '' OR domain_code = tapdb_current_domain_code()
        AND (issuer_app_code = '' OR issuer_app_code = tapdb_current_app_code())
    );
