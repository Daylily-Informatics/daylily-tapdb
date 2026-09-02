-- Add exact natural identity and correct template uniqueness to include owner.
-- Existing generic_instance rows intentionally retain identity_key = NULL.

ALTER TABLE generic_instance
    ADD COLUMN IF NOT EXISTS identity_key VARCHAR(512);

ALTER TABLE generic_instance
    DROP CONSTRAINT IF EXISTS ck_generic_instance_identity_key_global;
ALTER TABLE generic_instance
    ADD CONSTRAINT ck_generic_instance_identity_key_global
    CHECK (identity_key IS NULL OR tenant_id IS NULL) NOT VALID;

ALTER TABLE generic_instance
    DROP CONSTRAINT IF EXISTS ck_generic_instance_identity_key_format;
ALTER TABLE generic_instance
    ADD CONSTRAINT ck_generic_instance_identity_key_format
    CHECK (
        identity_key IS NULL OR (
            identity_key ~ '^[a-z][a-z0-9._/-]*:[^[:cntrl:]]+$'
            AND char_length(identity_key) <= 512
        )
    ) NOT VALID;

ALTER TABLE generic_instance
    VALIDATE CONSTRAINT ck_generic_instance_identity_key_global;
ALTER TABLE generic_instance
    VALIDATE CONSTRAINT ck_generic_instance_identity_key_format;

CREATE UNIQUE INDEX IF NOT EXISTS idx_generic_instance_natural_identity
    ON generic_instance (
        domain_code, issuer_app_code, template_uid, identity_key
    )
    WHERE identity_key IS NOT NULL;

ALTER TABLE generic_template DROP CONSTRAINT IF EXISTS unique_template_code;
ALTER TABLE generic_template ADD CONSTRAINT unique_template_code
    UNIQUE (
        domain_code, issuer_app_code, category, type, subtype, version
    );
