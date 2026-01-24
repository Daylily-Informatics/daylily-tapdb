-- Migration: Rename type hierarchy fields
-- super_type -> category, btype -> type, b_sub_type -> subtype
-- Version: 001
-- Date: 2026-01-20

-- This migration renames the confusing type hierarchy fields to clearer names:
--   super_type  -> category
--   btype       -> type  
--   b_sub_type  -> subtype

BEGIN;

--------------------------------------------------------------------------------
-- RENAME COLUMNS IN generic_template
--------------------------------------------------------------------------------
ALTER TABLE generic_template RENAME COLUMN super_type TO category;
ALTER TABLE generic_template RENAME COLUMN btype TO type;
ALTER TABLE generic_template RENAME COLUMN b_sub_type TO subtype;

--------------------------------------------------------------------------------
-- RENAME COLUMNS IN generic_instance
--------------------------------------------------------------------------------
ALTER TABLE generic_instance RENAME COLUMN super_type TO category;
ALTER TABLE generic_instance RENAME COLUMN btype TO type;
ALTER TABLE generic_instance RENAME COLUMN b_sub_type TO subtype;

--------------------------------------------------------------------------------
-- RENAME COLUMNS IN generic_instance_lineage
--------------------------------------------------------------------------------
ALTER TABLE generic_instance_lineage RENAME COLUMN super_type TO category;
ALTER TABLE generic_instance_lineage RENAME COLUMN btype TO type;
ALTER TABLE generic_instance_lineage RENAME COLUMN b_sub_type TO subtype;

--------------------------------------------------------------------------------
-- RENAME COLUMN IN audit_log
--------------------------------------------------------------------------------
ALTER TABLE audit_log RENAME COLUMN super_type TO category;

--------------------------------------------------------------------------------
-- DROP AND RECREATE INDEXES WITH NEW NAMES
-- (PostgreSQL doesn't support renaming indexes directly in all versions)
--------------------------------------------------------------------------------

-- generic_template indexes
DROP INDEX IF EXISTS idx_generic_template_btype;
CREATE INDEX IF NOT EXISTS idx_generic_template_type ON generic_template(type);

-- generic_instance indexes  
DROP INDEX IF EXISTS idx_generic_instance_unique_singleton_key;
CREATE UNIQUE INDEX idx_generic_instance_unique_singleton_key
    ON generic_instance (category, type, subtype, version)
    WHERE is_singleton = TRUE;

DROP INDEX IF EXISTS idx_generic_instance_type;
CREATE INDEX idx_generic_instance_type ON generic_instance(type);

DROP INDEX IF EXISTS idx_generic_instance_super_type;
CREATE INDEX idx_generic_instance_category ON generic_instance(category);

DROP INDEX IF EXISTS idx_generic_instance_b_sub_type;
CREATE INDEX idx_generic_instance_subtype ON generic_instance(subtype);

COMMIT;

-- Verify the migration
-- SELECT column_name FROM information_schema.columns 
-- WHERE table_name = 'generic_template' AND column_name IN ('category', 'type', 'subtype');

