-- Migration: Add domain_code + issuer_app_code columns and update SQL functions
-- Applies to existing databases; new databases get these via base schema.
--
-- Safe to re-run: all statements are idempotent (IF NOT EXISTS / OR REPLACE).

BEGIN;

-- ============================================================
-- 1. Add columns (idempotent via DO blocks)
-- ============================================================

DO $$ BEGIN
    ALTER TABLE generic_template ADD COLUMN domain_code TEXT NOT NULL DEFAULT '';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE generic_template ADD COLUMN issuer_app_code TEXT NOT NULL DEFAULT '';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE generic_instance ADD COLUMN domain_code TEXT NOT NULL DEFAULT '';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE generic_instance ADD COLUMN issuer_app_code TEXT NOT NULL DEFAULT '';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE generic_instance_lineage ADD COLUMN domain_code TEXT NOT NULL DEFAULT '';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE generic_instance_lineage ADD COLUMN issuer_app_code TEXT NOT NULL DEFAULT '';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE audit_log ADD COLUMN domain_code TEXT NOT NULL DEFAULT '';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE audit_log ADD COLUMN issuer_app_code TEXT NOT NULL DEFAULT '';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE outbox_event ADD COLUMN tenant_id UUID;
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE outbox_event ADD COLUMN domain_code TEXT NOT NULL DEFAULT '';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

DO $$ BEGIN
    ALTER TABLE outbox_event ADD COLUMN issuer_app_code TEXT NOT NULL DEFAULT '';
EXCEPTION WHEN duplicate_column THEN NULL;
END $$;

-- ============================================================
-- 2. Update unique constraint on generic_template
-- ============================================================

-- Drop old constraint (if it exists) and create new one including domain_code, issuer_app_code
ALTER TABLE generic_template DROP CONSTRAINT IF EXISTS unique_template_code;
ALTER TABLE generic_template ADD CONSTRAINT unique_template_code
    UNIQUE (domain_code, issuer_app_code, category, type, subtype, version);

-- ============================================================
-- 3. Indexes for domain filtering
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_generic_template_domain
    ON generic_template (domain_code, issuer_app_code);

CREATE INDEX IF NOT EXISTS idx_generic_instance_domain
    ON generic_instance (domain_code, issuer_app_code);

CREATE INDEX IF NOT EXISTS idx_generic_instance_lineage_domain
    ON generic_instance_lineage (domain_code, issuer_app_code);

CREATE INDEX IF NOT EXISTS idx_audit_log_domain
    ON audit_log (domain_code, issuer_app_code);

CREATE INDEX IF NOT EXISTS idx_outbox_event_domain
    ON outbox_event (domain_code, issuer_app_code);

COMMIT;
