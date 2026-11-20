-- Prospective issuance: retain all existing templates, identities and sequences.
-- tapdb-allow-schema: tapdb_issuance_profile
-- tapdb-allow-schema: generic_instance
ALTER TABLE tapdb_issuance_profile ADD COLUMN IF NOT EXISTS external_reference_prefix TEXT
    CONSTRAINT tapdb_issuance_profile_reference_prefix_check CHECK (
        external_reference_prefix IS NULL OR
        (mode = 'canonical' AND external_reference_prefix ~ '^[0-9A-HJ-KMNP-TV-Z]{1,4}$'));

-- tapdb-include: ../allocator_functions.sql
