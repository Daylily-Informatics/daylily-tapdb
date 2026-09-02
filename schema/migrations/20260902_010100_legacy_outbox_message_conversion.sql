-- Convert legacy outbox payload rows to canonical message instances.
-- tapdb-transformation: outbox_event.message_uid:null_to_legacy_mapping_v1
-- tapdb-allow-new-rows: generic_instance
-- tapdb-allow-new-rows: tapdb_legacy_outbox_mapping
-- tapdb-allow-sequence: generic_instance_uid_seq
-- tapdb-allow-sequence: msg_instance_seq

CREATE TABLE IF NOT EXISTS tapdb_legacy_outbox_mapping (
    old_outbox_id BIGINT PRIMARY KEY,
    old_event_id UUID NOT NULL UNIQUE,
    message_uid BIGINT NOT NULL UNIQUE REFERENCES generic_instance(uid),
    message_euid TEXT NOT NULL UNIQUE,
    message_euid_seq BIGINT NOT NULL,
    source_sha256 TEXT NOT NULL,
    mapped_dt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE outbox_event ADD COLUMN IF NOT EXISTS message_uid BIGINT;

DO $$
DECLARE
    has_legacy_event_id BOOLEAN;
    legacy_row RECORD;
    message_template RECORD;
    new_message RECORD;
    template_count BIGINT;
    row_offset BIGINT := 0;
    generic_uid_sequence REGCLASS;
    message_euid_sequence REGCLASS;
    generic_uid_last BIGINT;
    message_euid_last BIGINT;
    generic_uid_called BOOLEAN;
    message_euid_called BOOLEAN;
    generic_uid_increment BIGINT;
    message_euid_increment BIGINT;
    allocated_uid BIGINT;
    allocated_message_seq BIGINT;
BEGIN
    -- Stabilize timestamptz rendering in row_to_json so source_sha256 is
    -- identical for the same restored row regardless of the caller's session
    -- timezone. This is transaction-local and does not rewrite stored values.
    PERFORM set_config('TimeZone', 'UTC', true);

    SELECT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = 'outbox_event'
          AND column_name = 'event_id'
    ) INTO has_legacy_event_id;

    IF NOT has_legacy_event_id THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM outbox_event
        WHERE COALESCE(domain_code, '') = ''
           OR COALESCE(issuer_app_code, '') = ''
    ) THEN
        RAISE EXCEPTION
            'Legacy outbox conversion requires explicit domain_code and issuer_app_code on every row';
    END IF;

    generic_uid_sequence := pg_get_serial_sequence(
        'generic_instance', 'uid'
    )::REGCLASS;
    message_euid_sequence := to_regclass('msg_instance_seq');
    IF generic_uid_sequence IS NULL OR message_euid_sequence IS NULL THEN
        RAISE EXCEPTION
            'Legacy outbox conversion requires generic_instance uid and MSG identity sequences';
    END IF;
    EXECUTE format(
        'SELECT last_value, is_called FROM %s', generic_uid_sequence
    ) INTO generic_uid_last, generic_uid_called;
    EXECUTE format(
        'SELECT last_value, is_called FROM %s', message_euid_sequence
    ) INTO message_euid_last, message_euid_called;
    SELECT seqincrement INTO generic_uid_increment
    FROM pg_sequence WHERE seqrelid = generic_uid_sequence;
    SELECT seqincrement INTO message_euid_increment
    FROM pg_sequence WHERE seqrelid = message_euid_sequence;
    IF generic_uid_increment <= 0 OR message_euid_increment <= 0 THEN
        RAISE EXCEPTION
            'Legacy outbox conversion requires positive identity sequence increments';
    END IF;

    ALTER TABLE generic_instance
        DISABLE TRIGGER audit_insert_generic_instance;

    FOR legacy_row IN EXECUTE
        'SELECT id, event_id, tenant_id, domain_code, issuer_app_code, '
        'event_type, aggregate_euid, payload, destination, dedupe_key, status, '
        'attempt_count, next_attempt_at, last_error, created_dt, delivered_dt '
        'FROM outbox_event WHERE message_uid IS NULL ORDER BY id'
    LOOP
        SELECT count(*)
        INTO template_count
        FROM generic_template
        WHERE domain_code = legacy_row.domain_code
          AND issuer_app_code = legacy_row.issuer_app_code
          AND category = 'message'
          AND type = 'webhook'
          AND subtype = 'event'
          AND version = '1.0'
          AND is_deleted = FALSE;

        IF template_count <> 1 THEN
            RAISE EXCEPTION
                'Legacy outbox row % requires exactly one active message/webhook/event/1.0 template in domain %, owner %, found %',
                legacy_row.id, legacy_row.domain_code,
                legacy_row.issuer_app_code, template_count;
        END IF;

        SELECT uid, instance_prefix, polymorphic_discriminator, category,
               type, subtype, version
        INTO message_template
        FROM generic_template
        WHERE domain_code = legacy_row.domain_code
          AND issuer_app_code = legacy_row.issuer_app_code
          AND category = 'message'
          AND type = 'webhook'
          AND subtype = 'event'
          AND version = '1.0'
          AND is_deleted = FALSE;
        IF upper(message_template.instance_prefix) <> 'MSG' THEN
            RAISE EXCEPTION
                'Legacy outbox row % message template must use MSG instance_prefix',
                legacy_row.id;
        END IF;

        PERFORM set_config(
            'session.current_domain_code', legacy_row.domain_code, true
        );
        PERFORM set_config(
            'session.current_owner_repo_name', legacy_row.issuer_app_code, true
        );

        allocated_uid := generic_uid_last
            + CASE WHEN generic_uid_called THEN generic_uid_increment ELSE 0 END
            + row_offset * generic_uid_increment;
        allocated_message_seq := message_euid_last
            + CASE WHEN message_euid_called THEN message_euid_increment ELSE 0 END
            + row_offset * message_euid_increment;

        INSERT INTO generic_instance (
            uid,
            euid,
            euid_prefix,
            euid_seq,
            machine_uuid,
            name,
            tenant_id,
            polymorphic_discriminator,
            category,
            type,
            subtype,
            version,
            template_uid,
            json_addl,
            bstatus,
            is_singleton,
            is_deleted,
            created_dt,
            modified_dt
        ) VALUES (
            allocated_uid,
            meridian_generate_euid(
                'MSG', allocated_message_seq, legacy_row.domain_code
            ),
            'MSG',
            allocated_message_seq,
            legacy_row.event_id,
            'legacy-outbox:' || legacy_row.id::text,
            legacy_row.tenant_id,
            replace(message_template.polymorphic_discriminator, '_template', '_instance'),
            message_template.category,
            message_template.type,
            message_template.subtype,
            message_template.version,
            message_template.uid,
            jsonb_build_object(
                'event_type', legacy_row.event_type,
                'aggregate_euid', legacy_row.aggregate_euid,
                'payload', legacy_row.payload,
                'metadata', jsonb_build_object(
                    'legacy_outbox_id', legacy_row.id,
                    'destination', legacy_row.destination,
                    'dedupe_key', legacy_row.dedupe_key,
                    'status', legacy_row.status,
                    'attempt_count', legacy_row.attempt_count,
                    'next_attempt_at', legacy_row.next_attempt_at,
                    'last_error', legacy_row.last_error,
                    'delivered_dt', legacy_row.delivered_dt
                )
            ),
            'active',
            FALSE,
            FALSE,
            legacy_row.created_dt,
            legacy_row.created_dt
        )
        RETURNING uid, euid, euid_seq INTO new_message;

        INSERT INTO tapdb_legacy_outbox_mapping (
            old_outbox_id,
            old_event_id,
            message_uid,
            message_euid,
            message_euid_seq,
            source_sha256,
            mapped_dt
        ) VALUES (
            legacy_row.id,
            legacy_row.event_id,
            new_message.uid,
            new_message.euid,
            new_message.euid_seq,
            encode(
                sha256(convert_to(row_to_json(legacy_row)::text, 'UTF8')),
                'hex'
            ),
            legacy_row.created_dt
        );

        UPDATE outbox_event
        SET message_uid = new_message.uid
        WHERE id = legacy_row.id;

        row_offset := row_offset + 1;
    END LOOP;

    ALTER TABLE generic_instance
        ENABLE TRIGGER audit_insert_generic_instance;
END $$;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM outbox_event WHERE message_uid IS NULL) THEN
        RAISE EXCEPTION 'outbox_event.message_uid conversion is incomplete';
    END IF;
END $$;

ALTER TABLE outbox_event ALTER COLUMN message_uid SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'outbox_event_message_uid_fkey'
          AND conrelid = 'outbox_event'::regclass
    ) THEN
        ALTER TABLE outbox_event
            ADD CONSTRAINT outbox_event_message_uid_fkey
            FOREIGN KEY (message_uid) REFERENCES generic_instance(uid);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_outbox_event_message_uid
    ON outbox_event(message_uid);
