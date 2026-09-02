from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from daylily_tapdb import connection as connection_module
from daylily_tapdb import migration_identity as migration
from daylily_tapdb import sequences, user_store
from daylily_tapdb.factory import instance as instance_module


class _ScalarResult:
    def __init__(self, value):
        self.value = value

    def scalar_one(self):
        return self.value

    def one_or_none(self):
        return self.value


class _SequenceResult:
    def __init__(self, value):
        self.value = value

    def one(self):
        return self.value


class _SequenceSession:
    def __init__(self, state):
        self.state = state

    def execute(self, statement, _params=None):
        if "WITH" in str(statement):
            return _SequenceResult(self.state)
        return None


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"increment": 2}, "ambiguous issuance settings"),
        ({"cycle": True}, "ambiguous issuance settings"),
        ({"cache_size": 2}, "ambiguous issuance settings"),
        ({"desired_next": 11, "maximum_value": 10}, "cannot advance"),
    ],
)
def test_branch_campaign_sequence_rejects_unsafe_advancement(changes, message):
    state = {
        "desired_next": 3,
        "current_next": 1,
        "increment": 1,
        "cycle": False,
        "cache_size": 1,
        "maximum_value": 100,
    }
    state.update(changes)

    with pytest.raises(ValueError, match=message):
        sequences.ensure_instance_prefix_sequence(
            _SequenceSession(SimpleNamespace(**state)), "GX"
        )


class _DisposableEngine:
    def __init__(self, *, error: Exception | None = None):
        self.error = error

    def dispose(self):
        if self.error:
            raise self.error


def _patch_connection_construction(monkeypatch, engine=None):
    engine = engine or _DisposableEngine()
    monkeypatch.setattr(connection_module, "create_engine", lambda *_a, **_k: engine)
    monkeypatch.setattr(connection_module, "sessionmaker", lambda bind: lambda: None)
    return engine


def _connection_kwargs(**overrides):
    values = {
        "db_url": "sqlite:///:memory:",
        "db_user": "tapdb",
        "app_username": "branch-campaign",
        "domain_code": "Z",
        "owner_repo_name": "daylily-tapdb",
        "config_identity": "/abs/tapdb-config.yaml",
        "engine_type": "local",
    }
    values.update(overrides)
    return values


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"db_user": ""}, "db_user is required"),
        ({"app_username": ""}, "app_username is required"),
        ({"connection_role": "root"}, "connection_role"),
        ({"engine_type": "sqlite"}, "engine_type"),
        (
            {"db_url": None, "engine_type": "aurora", "db_hostname": "cluster"},
            "explicit port",
        ),
        ({"db_url": None, "db_hostname": None, "db_pass": "pw"}, "db_hostname"),
        (
            {"db_url": None, "db_hostname": "localhost:5432", "db_pass": None},
            "db_pass",
        ),
        (
            {
                "db_url": None,
                "db_hostname": "localhost:5432",
                "db_pass": "pw",
                "db_name": "",
            },
            "db_name",
        ),
    ],
)
def test_branch_campaign_connection_rejects_incomplete_configuration(
    monkeypatch, overrides, message
):
    _patch_connection_construction(monkeypatch)
    with pytest.raises(ValueError, match=message):
        connection_module.TAPDBConnection(**_connection_kwargs(**overrides))


def test_branch_campaign_connection_search_path_and_cleanup_fail_closed(
    monkeypatch, caplog
):
    engine = _patch_connection_construction(
        monkeypatch, _DisposableEngine(error=RuntimeError("dispose failed"))
    )
    connection = connection_module.TAPDBConnection(**_connection_kwargs())

    sqlite_session = SimpleNamespace(
        bind=SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
    )
    assert connection._set_session_search_path(sqlite_session, local=True) is None

    postgres_session = SimpleNamespace(
        bind=SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    )
    with pytest.raises(ValueError, match="schema_name is required"):
        connection._set_session_search_path(postgres_session, local=True)

    assert connection.__exit__(RuntimeError, RuntimeError("body failed"), None) is False
    assert engine.error is not None
    assert "Exception in context" in caplog.text
    assert "Error disposing engine" in caplog.text

    connection.engine = None
    assert connection.close() is None


def test_branch_campaign_connection_session_scope_handles_context_failures(monkeypatch):
    _patch_connection_construction(monkeypatch)
    connection = connection_module.TAPDBConnection(**_connection_kwargs())

    class Transaction:
        rolled_back = False

        def rollback(self):
            self.rolled_back = True

    class Session:
        bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

        def __init__(self):
            self.transaction = Transaction()
            self.closed = False

        def begin(self):
            return self.transaction

        def close(self):
            self.closed = True

    session = Session()
    connection._Session = lambda: session
    with pytest.raises(ValueError, match="schema_name is required"):
        with connection.session_scope():
            pass
    assert session.transaction.rolled_back is True
    assert session.closed is True


class _MappingsResult:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def first(self):
        return self.row

    def all(self):
        return list(self.row or [])

    def fetchone(self):
        return self.row


class _UserSession:
    def __init__(self, rows, *, bind=None):
        self.rows = list(rows)
        self.bind = bind
        self.statements = []

    def execute(self, statement, _params=None):
        self.statements.append(str(statement))
        return _MappingsResult(self.rows.pop(0) if self.rows else None)


def _actor_row(**changes):
    row = {
        "uid": 7,
        "euid": "test-actor",
        "created_dt": None,
        "modified_dt": None,
        "login_identifier": "",
        "email": " Person@Example.com ",
        "display_name": " Person ",
        "role": "unexpected",
        "is_active": True,
        "require_password_change": False,
        "password_hash": None,
        "last_login_dt": None,
        "cognito_username": None,
        "preferences": None,
    }
    row.update(changes)
    return row


def test_branch_campaign_user_store_detects_dialect_through_get_bind():
    postgresql = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))

    class DeferredBind:
        bind = None

        def get_bind(self):
            return postgresql

    class BrokenBind:
        bind = None

        def get_bind(self):
            raise RuntimeError("unbound")

    assert connection_module.is_postgresql_session(DeferredBind()) is False
    assert connection_module.is_postgresql_session(BrokenBind()) is False


def test_branch_campaign_user_store_template_lookup_does_not_mutate_fixed_scope():
    session = _UserSession([(42, "SYS")])
    assert user_store._get_system_user_template_uid(session) == 42
    assert session.rows == []
    statement = session.statements[0]
    assert "set_config" not in statement
    assert "domain_code = tapdb_current_domain_code()" in statement
    assert "issuer_app_code = tapdb_current_owner_repo_name()" in statement


def test_branch_campaign_all_system_user_queries_use_exact_bound_scope():
    where = user_store._SYSTEM_USER_WHERE
    assert "domain_code = tapdb_current_domain_code()" in where
    assert "issuer_app_code = tapdb_current_owner_repo_name()" in where
    assert "daylily-tapdb" not in where


def test_branch_campaign_user_store_maps_fallback_identity_and_role():
    user = user_store._row_to_actor_user(_actor_row())
    assert user.username == "person@example.com"
    assert user.email == "person@example.com"
    assert user.role == "user"
    assert user.preferences["display_timezone"]


@pytest.mark.parametrize(
    "lookup",
    [
        user_store.get_by_login_identifier,
        user_store.get_by_login_or_email,
        lambda session, _identifier, **kwargs: user_store.get_by_uid(
            session, 7, **kwargs
        ),
    ],
)
def test_branch_campaign_user_store_inactive_lookup_can_return_none(lookup):
    assert lookup(_UserSession([None]), "person", include_inactive=True) is None


def test_branch_campaign_user_store_lists_inactive_rows_without_filter():
    session = _UserSession([[_actor_row(login_identifier="person")]])
    users = user_store.list_users(session, include_inactive=True)
    assert [user.username for user in users] == ["person"]


def test_branch_campaign_user_store_create_reports_missing_winner(monkeypatch):
    lookups = iter([None, None])
    monkeypatch.setattr(
        user_store,
        "get_by_login_identifier",
        lambda *_args, **_kwargs: next(lookups),
    )
    monkeypatch.setattr(user_store, "_get_system_user_template_uid", lambda _s: 9)
    monkeypatch.setattr(user_store, "get_by_uid", lambda *_a, **_k: None)

    class Nested:
        def commit(self):
            return None

    class Session:
        bind = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))

        def begin_nested(self):
            return Nested()

        def execute(self, _statement, _params=None):
            return _MappingsResult((10,))

    with pytest.raises(RuntimeError, match="Failed to create actor user"):
        user_store.create_or_get(Session(), login_identifier="missing@example.com")


def test_branch_campaign_user_store_integrity_before_savepoint_is_not_masked(
    monkeypatch,
):
    monkeypatch.setattr(
        user_store, "get_by_login_identifier", lambda *_args, **_kwargs: None
    )
    error = IntegrityError("owner lookup", {}, RuntimeError("conflict"))
    monkeypatch.setattr(
        user_store,
        "_get_system_user_template_uid",
        lambda _session: (_ for _ in ()).throw(error),
    )
    with pytest.raises(IntegrityError) as raised:
        user_store.create_or_get(object(), login_identifier="race@example.com")
    assert raised.value is error


def test_branch_campaign_factory_validates_claim_inputs():
    with pytest.raises(ValueError, match="must be a string"):
        instance_module.validate_identity_key(123)

    non_postgres = SimpleNamespace(dialect=SimpleNamespace(name="sqlite"))
    session = SimpleNamespace(
        get_bind=lambda: non_postgres, in_transaction=lambda: True
    )
    with pytest.raises(RuntimeError, match="require PostgreSQL"):
        instance_module._require_active_postgresql_transaction(session)

    postgres = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    session = SimpleNamespace(get_bind=lambda: postgres, in_transaction=lambda: False)
    with pytest.raises(RuntimeError, match="active transaction"):
        instance_module._require_active_postgresql_transaction(session)

    assert instance_module._parse_bool(None, default=True) is True
    assert instance_module._parse_bool("unknown", default=False) is False


def test_branch_campaign_factory_reuses_materialized_action_group():
    template = SimpleNamespace(
        json_addl={
            "action_imports": {
                "first": "action/core/first/1.0",
                "second": "action/core/second/1.0",
            }
        }
    )
    action_template = SimpleNamespace(
        uid=uuid4(),
        euid="test-action",
        type="core",
        json_addl={"action_definition": {"handler": "tests:handler"}},
    )
    manager = SimpleNamespace(get_template=lambda *_a, **_k: action_template)
    groups = instance_module.materialize_actions(
        object(), template, manager, domain_code="z"
    )
    assert set(groups["core_actions"]) == {"first", "second"}


def test_branch_campaign_factory_claim_requires_scope_and_template():
    postgres = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
    session = SimpleNamespace(get_bind=lambda: postgres, in_transaction=lambda: True)
    no_scope = instance_module.InstanceFactory(
        SimpleNamespace(get_template=lambda *_a, **_k: None)
    )
    with pytest.raises(ValueError, match="domain_code is required"):
        no_scope.claim_instance_by_identity(
            session, template_code="a/b/c/1.0", identity_key="test:key", name="x"
        )

    scoped = instance_module.InstanceFactory(
        SimpleNamespace(get_template=lambda *_a, **_k: None), domain_code="z"
    )
    with pytest.raises(ValueError, match="Template not found"):
        scoped.claim_instance_by_identity(
            session, template_code="a/b/c/1.0", identity_key="test:key", name="x"
        )


def test_branch_campaign_factory_normalizes_non_mapping_properties():
    factory = instance_module.InstanceFactory(object(), domain_code="z")
    template = SimpleNamespace(category="actor", type="user", subtype="system")
    payload = {
        "properties": "malformed",
        "login_identifier": " Top@Example.com ",
        "role": "user",
    }
    factory._normalize_system_user_json_addl(template, payload)
    assert payload["properties"]["login_identifier"] == "top@example.com"


def test_branch_campaign_factory_empty_layout_is_a_noop():
    factory = instance_module.InstanceFactory(object(), domain_code="z")
    template = SimpleNamespace(json_addl={"instantiation_layouts": []})
    assert (
        factory._create_children(
            object(), SimpleNamespace(), template, depth=0, visited=set()
        )
        is None
    )


def test_branch_campaign_migration_json_and_include_validation(tmp_path):
    assert migration._jsonable(b"\x00\xff") == "00ff"
    assert migration._jsonable({2: Decimal("1.5")}) == {"2": "1.5"}
    assert migration._jsonable((date(2026, 9, 2), object()))[0] == "2026-09-02"

    schema_root = tmp_path / "schema"
    migrations = schema_root / "migrations"
    migrations.mkdir(parents=True)
    cyclic = migrations / "cyclic.sql"
    cyclic.write_text("-- tapdb-include: cyclic.sql\n", encoding="utf-8")
    with pytest.raises(migration.MigrationPreflightError, match="cyclic"):
        migration._expand_migration_source(cyclic, schema_root=schema_root)

    absolute = migrations / "absolute.sql"
    absolute.write_text(f"-- tapdb-include: {cyclic}\n", encoding="utf-8")
    with pytest.raises(migration.MigrationPreflightError, match="absolute"):
        migration._expand_migration_source(absolute, schema_root=schema_root)

    with pytest.raises(migration.MigrationPreflightError, match="escapes schema root"):
        migration._expand_migration_source(
            tmp_path / "outside.sql", schema_root=schema_root
        )

    nonsql = schema_root / "notes.txt"
    nonsql.write_text("text", encoding="utf-8")
    with pytest.raises(migration.MigrationPreflightError, match="not a SQL file"):
        migration._expand_migration_source(nonsql, schema_root=schema_root)


class _MigrationConnection:
    def __init__(self, *, in_transaction=True, values=()):
        self.transaction = in_transaction
        self.values = list(values)
        self.executed = []

    def in_transaction(self):
        return self.transaction

    def execute(self, statement, params=None):
        self.executed.append((str(statement), params))
        return _ScalarResult(self.values.pop(0) if self.values else True)

    def exec_driver_sql(self, statement):
        self.executed.append((statement, None))


def test_branch_campaign_migration_tracking_and_operator_context_fail_closed():
    with pytest.raises(migration.MigrationPreflightError, match="missing"):
        migration._tracking_rows(_MigrationConnection(values=[False]))

    with pytest.raises(migration.MigrationPreflightError, match="active transaction"):
        migration._apply_operator_context(
            _MigrationConnection(in_transaction=False), {}
        )

    with pytest.raises(migration.MigrationPreflightError, match="missing search_path"):
        migration._apply_operator_context(_MigrationConnection(), {})

    connection = _MigrationConnection(
        values=[True] * 9 + [("tapdb_operator", False, True)]
    )
    migration._apply_operator_context(
        connection,
        {
            "schema_name": "tapdb",
            "domain_code": "Z",
            "owner_repo_name": "repo",
            "config_identity": "/abs/tapdb-config.yaml",
        },
    )
    assert len(connection.executed) == 10
    assert connection.executed[0][1] == {"name": "TimeZone", "value": "UTC"}


def _sequence_state(**changes):
    state = {
        "name": "gx_instance_seq",
        "owner_table": None,
        "owner_column": None,
        "last_value": 10,
        "is_called": True,
        "start_value": 1,
        "minimum_value": 1,
        "maximum_value": 100,
        "increment": 1,
        "cycle": False,
        "cache_size": 1,
    }
    state.update(changes)
    return state


def _identity_row(key=1, **identity):
    return {
        "key": [key],
        "identity": identity,
        "column_sha256": {
            column: migration._sha256(value) for column, value in identity.items()
        },
    }


@pytest.mark.parametrize(
    ("sequence_changes", "message"),
    [
        ({"increment": 0}, "positive increment"),
        ({"cycle": True}, "must not cycle"),
        ({"cache_size": 2}, "ambiguous cached state"),
        ({"last_value": 4, "is_called": False}, "behind assigned"),
    ],
)
def test_branch_campaign_migration_rejects_ambiguous_generators(
    sequence_changes, message
):
    snapshot = {
        "tables": {
            "generic_instance": {
                "columns": ["euid_prefix", "euid_seq"],
                "rows": [_identity_row(euid_prefix="GX", euid_seq=5)],
            }
        },
        "sequences": [_sequence_state(**sequence_changes)],
    }
    with pytest.raises(migration.MigrationPreflightError, match=message):
        migration._validate_scope_and_sequences(snapshot)


def test_branch_campaign_migration_validates_missing_scope_and_sequence():
    missing_scope = {
        "tables": {
            "generic_instance": {
                "columns": ["domain_code"],
                "rows": [_identity_row(domain_code="")],
            }
        },
        "sequences": [],
    }
    with pytest.raises(migration.MigrationPreflightError, match="missing domain_code"):
        migration._validate_scope_and_sequences(missing_scope)

    missing_sequence = {
        "tables": {
            "generic_instance": {
                "columns": ["euid_prefix", "euid_seq"],
                "rows": [_identity_row(euid_prefix="GX", euid_seq=2)],
            }
        },
        "sequences": [],
    }
    with pytest.raises(migration.MigrationPreflightError, match="gx_instance_seq"):
        migration._validate_scope_and_sequences(missing_sequence)

    migration._validate_scope_and_sequences(missing_sequence, validate_generators=False)


@pytest.mark.parametrize(
    "table_name",
    ("outbox_event_attempt", "inbox_message", "tapdb_identity_prefix_config"),
)
def test_branch_campaign_migration_rejects_missing_scope_on_all_scoped_tables(
    table_name,
):
    snapshot = {
        "tables": {
            table_name: {
                "columns": ["domain_code", "issuer_app_code"],
                "rows": [
                    _identity_row(
                        domain_code="Z",
                        issuer_app_code="",
                    )
                ],
            }
        },
        "sequences": [],
    }

    with pytest.raises(migration.MigrationPreflightError, match="missing issuer"):
        migration._validate_scope_and_sequences(snapshot)


def _preservation_receipt(*, rows=None, sequences=None, allowed=()):
    return {
        "pending_migrations": [
            {
                "allowed_columns": [],
                "allowed_new_rows": [],
                "allowed_sequences": list(allowed),
            }
        ],
        "tables": {
            "generic_instance": {"rows": rows or [_identity_row(uid=1, euid="test-1")]}
        },
        "sequences": sequences or [_sequence_state()],
    }


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda receipt: receipt["tables"].clear(), "table disappeared"),
        (
            lambda receipt: receipt["tables"]["generic_instance"].update(rows=[]),
            "row keys changed",
        ),
        (
            lambda receipt: receipt["tables"]["generic_instance"]["rows"][0][
                "column_sha256"
            ].update(euid="changed"),
            "undeclared change",
        ),
        (lambda receipt: receipt.update(sequences=[]), "sequence inventory changed"),
        (
            lambda receipt: receipt["sequences"][0].update(last_value=11),
            "undeclared identity sequence",
        ),
    ],
)
def test_branch_campaign_migration_preservation_detects_undeclared_changes(
    mutate, message
):
    before = _preservation_receipt()
    after = json.loads(json.dumps(before))
    mutate(after)
    with pytest.raises(migration.MigrationReceiptMismatchError, match=message):
        migration._verify_preservation(before, after)


def test_branch_campaign_migration_preservation_validates_declared_sequence_change():
    before = _preservation_receipt(allowed=["gx_instance_seq"])

    definition_changed = json.loads(json.dumps(before))
    definition_changed["sequences"][0].update(last_value=11, cache_size=2)
    with pytest.raises(
        migration.MigrationReceiptMismatchError, match="definition changed"
    ):
        migration._verify_preservation(before, definition_changed)

    backwards = json.loads(json.dumps(before))
    backwards["sequences"][0].update(last_value=9)
    with pytest.raises(
        migration.MigrationReceiptMismatchError, match="invalid.*advance"
    ):
        migration._verify_preservation(before, backwards)

    forward = json.loads(json.dumps(before))
    forward["sequences"][0].update(last_value=11)
    migration._verify_preservation(before, forward)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"increment": 0}, "non-positive"),
        ({"increment": 2, "last_value": 2}, "not aligned"),
        ({"last_value": 2, "maximum_value": 5}, "without wrapping"),
    ],
)
def test_branch_campaign_migration_sequence_advance_rejects_unsafe_state(
    changes, message
):
    sequence_values = {"last_value": 2, "is_called": False, **changes}
    sequence = _sequence_state(**sequence_values)
    interim = {
        "sequences": [sequence],
        "tables": {
            "generic_instance": {
                "columns": ["euid_seq"],
                "rows": [_identity_row(euid_seq=5)],
            }
        },
    }
    sequence["owner_table"] = "generic_instance"
    sequence["owner_column"] = "euid_seq"
    preflight = {"pending_migrations": [{"allowed_sequences": ["gx_instance_seq"]}]}
    with pytest.raises(migration.MigrationReceiptMismatchError, match=message):
        migration._advance_permitted_identity_sequences(
            _MigrationConnection(),
            preflight=preflight,
            interim=interim,
            schema_name="tapdb",
        )


def test_branch_campaign_migration_sequence_advance_skips_and_restarts():
    preflight = {"pending_migrations": [{"allowed_sequences": ["gx_instance_seq"]}]}
    unowned = _sequence_state(name="ignored_seq")
    empty = _sequence_state(name="gx_instance_seq")
    connection = _MigrationConnection()
    migration._advance_permitted_identity_sequences(
        connection,
        preflight=preflight,
        interim={"sequences": [unowned, empty], "tables": {}},
        schema_name="tapdb",
    )
    assert connection.executed == []

    restart = _sequence_state(last_value=2, is_called=False)
    interim = {
        "sequences": [restart],
        "tables": {
            "generic_instance": {
                "columns": ["euid_prefix", "euid_seq"],
                "rows": [_identity_row(euid_prefix="GX", euid_seq=5)],
            }
        },
    }
    migration._advance_permitted_identity_sequences(
        connection,
        preflight=preflight,
        interim=interim,
        schema_name='tap"db',
    )
    assert connection.executed == [
        ('ALTER SEQUENCE "tap""db"."gx_instance_seq" RESTART WITH 6', None)
    ]


def test_branch_campaign_migration_preflight_and_receipts_fail_closed(
    tmp_path, monkeypatch
):
    with pytest.raises(migration.MigrationPreflightError, match="schema_name"):
        migration.build_migration_preflight(
            _MigrationConnection(), migrations_dir=tmp_path, target={}
        )

    monkeypatch.setattr(migration, "_apply_operator_context", lambda *_a, **_k: None)
    with pytest.raises(migration.MigrationPreflightError, match="does not match"):
        migration.build_migration_preflight(
            _MigrationConnection(values=["other"]),
            migrations_dir=tmp_path,
            target={"schema_name": "tapdb"},
        )

    with pytest.raises(migration.MigrationPreflightError, match="must be absolute"):
        migration.write_json_receipt(Path("receipt.json"), {})

    existing = tmp_path / "existing.json"
    existing.write_text("{}", encoding="utf-8")
    with pytest.raises(migration.MigrationPreflightError, match="already exists"):
        migration.write_json_receipt(existing, {})

    missing = tmp_path / "missing.json"
    with pytest.raises(migration.MigrationPreflightError, match="existing absolute"):
        migration.load_json_receipt(missing)

    malformed = tmp_path / "malformed.json"
    malformed.write_text("[]", encoding="utf-8")
    with pytest.raises(migration.MigrationPreflightError, match="JSON object"):
        migration.load_json_receipt(malformed)

    wrong_version = tmp_path / "wrong-version.json"
    wrong_version.write_text(json.dumps({"receipt_version": "v0"}), encoding="utf-8")
    with pytest.raises(migration.MigrationPreflightError, match="unsupported"):
        migration.load_json_receipt(wrong_version)

    bad_hash = tmp_path / "bad-hash.json"
    bad_hash.write_text(
        json.dumps(
            {
                "receipt_version": migration.RECEIPT_VERSION,
                "evidence_sha256": "invalid",
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(migration.MigrationPreflightError, match="hash mismatch"):
        migration.load_json_receipt(bad_hash)
