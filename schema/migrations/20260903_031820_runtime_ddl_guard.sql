-- Deny CREATE in the managed schema to every bound runtime principal.
--
-- This migration only narrows one schema privilege. It does not update rows,
-- identities, EUIDs, UIDs, lineage, audit evidence, timestamps, or sequence
-- definitions/state.
DO $tapdb_runtime_schema_create_guard$
DECLARE
    managed_schema NAME := pg_catalog.current_schema();
    bound_runtime_role NAME;
BEGIN
    EXECUTE pg_catalog.format(
        'REVOKE CREATE ON SCHEMA %I FROM PUBLIC', managed_schema
    );

    FOR bound_runtime_role IN
        SELECT role_name
          FROM tapdb_runtime_principal_scope
         WHERE schema_name = managed_schema
         ORDER BY role_name
    LOOP
        EXECUTE pg_catalog.format(
            'REVOKE CREATE ON SCHEMA %I FROM %I',
            managed_schema,
            bound_runtime_role
        );
        IF pg_catalog.has_schema_privilege(
            bound_runtime_role,
            managed_schema,
            'CREATE'
        ) THEN
            RAISE EXCEPTION
                'TapDB runtime role % retains CREATE on managed schema %',
                bound_runtime_role,
                managed_schema;
        END IF;
    END LOOP;
END;
$tapdb_runtime_schema_create_guard$;
