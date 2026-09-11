-- Catalog evidence for the five exact optional generators declared by TapDB.
-- This package is the mapping authority. Names alone are not mapping evidence.
-- No absent generator is created; no sequence definition or state is changed.
-- Existing unrelated or contradictory comments require explicit review.
-- tapdb-add-prefix-binding: wx_instance_seq:WX
-- tapdb-add-prefix-binding: wsx_instance_seq:WSX
-- tapdb-add-prefix-binding: xx_instance_seq:XX
-- tapdb-add-prefix-binding: ay_instance_seq:AY
-- tapdb-add-prefix-binding: msg_instance_seq:MSG

DO $tapdb_prefix_binding$
DECLARE
    target_schema name := pg_catalog.current_schema();
    target_schema_oid oid;
    target_owner oid;
    binding record;
    generator record;
    expected_comment text;
BEGIN
    SELECT n.oid, n.nspowner INTO target_schema_oid, target_owner
      FROM pg_catalog.pg_namespace n WHERE n.nspname = target_schema;
    IF target_schema_oid IS NULL THEN
        RAISE EXCEPTION 'TapDB sequence prefix binding requires an explicit existing schema';
    END IF;

    FOR binding IN
        SELECT * FROM (VALUES
            ('wx_instance_seq', 'WX'),
            ('wsx_instance_seq', 'WSX'),
            ('xx_instance_seq', 'XX'),
            ('ay_instance_seq', 'AY'),
            ('msg_instance_seq', 'MSG')
        ) AS bindings(sequence_name, prefix)
    LOOP
        SELECT c.oid, c.relkind, c.relowner, c.relpersistence,
               s.seqtypid, s.seqstart, s.seqincrement, s.seqmin, s.seqmax,
               s.seqcache, s.seqcycle,
               pg_catalog.obj_description(c.oid, 'pg_class') AS catalog_comment
          INTO generator
          FROM pg_catalog.pg_class c
          LEFT JOIN pg_catalog.pg_sequence s ON s.seqrelid = c.oid
         WHERE c.relnamespace = target_schema_oid
           AND c.relname = binding.sequence_name;
        IF NOT FOUND THEN
            -- Optional historical generators remain absent; never claim or
            -- create an allocator merely because this package knows its name.
            CONTINUE;
        END IF;
        IF generator.relkind <> 'S' THEN
            RAISE EXCEPTION 'Unsafe TapDB prefix binding %.%: expected a sequence, found relation kind %',
                target_schema, binding.sequence_name, generator.relkind;
        END IF;
        IF generator.relowner <> target_owner THEN
            RAISE EXCEPTION 'Unsafe TapDB prefix binding %.%: sequence owner must equal schema owner',
                target_schema, binding.sequence_name;
        END IF;
        IF generator.relpersistence <> 'p'
           OR generator.seqtypid IS DISTINCT FROM 'pg_catalog.int8'::pg_catalog.regtype
           OR generator.seqstart IS DISTINCT FROM 1::bigint
           OR generator.seqincrement IS DISTINCT FROM 1::bigint
           OR generator.seqmin IS DISTINCT FROM 1::bigint
           OR generator.seqmax IS DISTINCT FROM 9223372036854775807::bigint
           OR generator.seqcache IS DISTINCT FROM 1::bigint
           OR generator.seqcycle IS DISTINCT FROM false THEN
            RAISE EXCEPTION 'Unsafe TapDB prefix binding %.%: requires a permanent bigint sequence with START 1 INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 NO CYCLE',
                target_schema, binding.sequence_name;
        END IF;
        IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_depend d
             WHERE d.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
               AND d.objid = generator.oid
               AND d.deptype IN ('a', 'i', 'e')
        ) THEN
            RAISE EXCEPTION 'Unsafe TapDB prefix binding %.%: optional generator must be standalone, not column-owned or extension-owned',
                target_schema, binding.sequence_name;
        END IF;

        expected_comment := 'tapdb-prefix-binding/v1:' || binding.prefix;
        IF generator.catalog_comment IS NOT NULL
           AND generator.catalog_comment <> expected_comment THEN
            RAISE EXCEPTION 'Conflicting TapDB prefix binding comment on %.%: refusing to overwrite existing catalog metadata',
                target_schema, binding.sequence_name;
        END IF;
        EXECUTE pg_catalog.format('COMMENT ON SEQUENCE %I.%I IS %L',
            target_schema, binding.sequence_name, expected_comment);
    END LOOP;
END;
$tapdb_prefix_binding$;
