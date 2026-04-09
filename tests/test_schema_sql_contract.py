from pathlib import Path

from daylily_tapdb.models.base import tapdb_core


def test_session_scope_functions_precede_outbox_defaults():
    schema_path = Path(__file__).resolve().parents[1] / "schema" / "tapdb_schema.sql"
    schema_sql = schema_path.read_text()

    domain_fn = schema_sql.index(
        "CREATE OR REPLACE FUNCTION tapdb_current_domain_code()"
    )
    app_fn = schema_sql.index("CREATE OR REPLACE FUNCTION tapdb_current_app_code()")

    first_domain_default = schema_sql.index("DEFAULT tapdb_current_domain_code()")
    first_app_default = schema_sql.index("DEFAULT tapdb_current_app_code()")

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


def test_template_uniqueness_is_scope_aware_in_schema_sql():
    schema_path = Path(__file__).resolve().parents[1] / "schema" / "tapdb_schema.sql"
    schema_sql = schema_path.read_text()

    assert (
        "UNIQUE (domain_code, issuer_app_code, category, type, subtype, version)"
        in schema_sql
    )
    assert "UNIQUE (category, type, subtype, version);" not in schema_sql


def test_orm_core_timestamps_are_timezone_aware():
    created_dt = tapdb_core.created_dt
    modified_dt = tapdb_core.modified_dt

    assert created_dt.type.timezone is True
    assert modified_dt.type.timezone is True
    assert created_dt.server_default is not None
    assert modified_dt.server_default is not None
