-- Explicit canonical future-template prefix; retained EUIDs are unchanged.
ALTER TABLE tapdb_issuance_profile ADD COLUMN IF NOT EXISTS template_prefix TEXT
    CHECK (template_prefix IS NULL OR
        (mode = 'canonical' AND template_prefix ~ '^[0-9A-HJ-KMNP-TV-Z]{1,4}$'));
