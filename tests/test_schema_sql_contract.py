from pathlib import Path

from daylily_tapdb.models.base import tapdb_core


def test_session_scope_functions_precede_outbox_defaults():
    schema_path = Path(__file__).resolve().parents[1] / "schema" / "tapdb_schema.sql"
    schema_sql = schema_path.read_text()

    domain_fn = schema_sql.index(
        "CREATE OR REPLACE FUNCTION tapdb_current_domain_code()"
    )
    app_fn = schema_sql.index(
        "CREATE OR REPLACE FUNCTION tapdb_current_owner_repo_name()"
    )

    first_domain_default = schema_sql.index("DEFAULT tapdb_current_domain_code()")
    first_app_default = schema_sql.index("DEFAULT tapdb_current_owner_repo_name()")

    assert domain_fn < first_domain_default
    assert app_fn < first_app_default


def test_core_timestamps_are_db_managed_timestamptz():
    schema_path = Path(__file__).resolve().parents[1] / "schema" / "tapdb_schema.sql"
    schema_sql = schema_path.read_text()

    assert (
        "created_dt TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP"
        in schema_sql
    )
    assert (
        "modified_dt TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP" in schema_sql
    )
    assert "NEW.modified_dt = CURRENT_TIMESTAMP;" in schema_sql


def test_orm_core_timestamps_are_timezone_aware():
    created_dt = tapdb_core.created_dt
    modified_dt = tapdb_core.modified_dt

    assert created_dt.type.timezone is True
    assert modified_dt.type.timezone is True
    assert created_dt.server_default is not None
    assert modified_dt.server_default is not None


def test_template_validator_ref_is_physical_schema_and_orm_column():
    from daylily_tapdb.models.template import generic_template

    schema_path = Path(__file__).resolve().parents[1] / "schema" / "tapdb_schema.sql"
    schema_sql = schema_path.read_text()

    assert "validator_ref TEXT NOT NULL DEFAULT 'UNIVERSAL_PASS@1'" in schema_sql
    assert "validator_ref" in generic_template.__table__.columns
    assert generic_template.__table__.columns["validator_ref"].nullable is False


def test_instance_euid_trigger_uses_template_instance_prefix_not_taxonomy():
    schema_path = Path(__file__).resolve().parents[1] / "schema" / "tapdb_schema.sql"
    schema_sql = schema_path.read_text()
    function_body = schema_sql.split(
        "CREATE OR REPLACE FUNCTION set_generic_instance_euid()", 1
    )[1].split("$$ LANGUAGE plpgsql;", 1)[0]

    assert "SELECT t.instance_prefix INTO prefix" in function_body
    assert "t.uid = NEW.template_uid" in function_body
    assert "tapdb_validate_meridian_prefix(NEW.category)" not in function_body
