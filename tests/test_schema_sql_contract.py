from pathlib import Path


def test_session_scope_functions_precede_outbox_defaults():
    schema_path = Path(__file__).resolve().parents[1] / "schema" / "tapdb_schema.sql"
    schema_sql = schema_path.read_text()

    domain_fn = schema_sql.index("CREATE OR REPLACE FUNCTION tapdb_current_domain_code()")
    app_fn = schema_sql.index("CREATE OR REPLACE FUNCTION tapdb_current_app_code()")

    first_domain_default = schema_sql.index("DEFAULT tapdb_current_domain_code()")
    first_app_default = schema_sql.index("DEFAULT tapdb_current_app_code()")

    assert domain_fn < first_domain_default
    assert app_fn < first_app_default
