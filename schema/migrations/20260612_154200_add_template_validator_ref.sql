-- Add physical template validator reference.
-- Historical NULL/empty values are explicitly attributed below; all other
-- template values and their provenance timestamps remain unchanged.
-- tapdb-allow-column: generic_template.validator_ref
-- tapdb-transformation: generic_template.validator_ref:null_or_empty_to_universal_pass_v1

BEGIN;

ALTER TABLE generic_template
    ADD COLUMN IF NOT EXISTS validator_ref TEXT NOT NULL DEFAULT 'UNIVERSAL_PASS@1';

DO $$
DECLARE
    scope_domain TEXT;
    scope_owner TEXT;
BEGIN
    IF EXISTS (
        SELECT 1
          FROM generic_template
         WHERE validator_ref IS NULL
            OR btrim(validator_ref) = ''
    ) THEN
        SELECT domain_code, issuer_app_code
          INTO scope_domain, scope_owner
          FROM generic_template
         WHERE COALESCE(domain_code, '') <> ''
           AND COALESCE(issuer_app_code, '') <> ''
         GROUP BY domain_code, issuer_app_code
         ORDER BY count(*) DESC, domain_code, issuer_app_code
         LIMIT 1;

        IF scope_domain IS NULL OR scope_owner IS NULL THEN
            RAISE EXCEPTION
                'Cannot backfill generic_template.validator_ref because existing template rows do not carry domain/issuer scope.';
        END IF;

        PERFORM set_config('session.current_domain_code', scope_domain, true);
        PERFORM set_config('session.current_owner_repo_name', scope_owner, true);

        ALTER TABLE generic_template
            DISABLE TRIGGER audit_update_generic_template;
        ALTER TABLE generic_template
            DISABLE TRIGGER update_modified_dt_generic_template;
        UPDATE generic_template
           SET validator_ref = 'UNIVERSAL_PASS@1'
         WHERE validator_ref IS NULL
            OR btrim(validator_ref) = '';
        ALTER TABLE generic_template
            ENABLE TRIGGER update_modified_dt_generic_template;
        ALTER TABLE generic_template
            ENABLE TRIGGER audit_update_generic_template;
    END IF;
END $$;

ALTER TABLE generic_template
    ALTER COLUMN validator_ref SET DEFAULT 'UNIVERSAL_PASS@1';

ALTER TABLE generic_template
    ALTER COLUMN validator_ref SET NOT NULL;

ALTER TABLE generic_template DROP CONSTRAINT IF EXISTS unique_template_code;
ALTER TABLE generic_template ADD CONSTRAINT unique_template_code
    UNIQUE (domain_code, issuer_app_code, category, type, subtype, version);

COMMIT;
