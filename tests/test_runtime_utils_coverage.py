from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine

import daylily_tapdb.audit as audit_mod
import daylily_tapdb.cli.admin_server as admin_server_mod
import daylily_tapdb.outbox.inbox as inbox_mod
import daylily_tapdb.outbox.queries as outbox_queries_mod
import daylily_tapdb.stats as stats_mod
import daylily_tapdb.web as web_mod
import daylily_tapdb.web.factory as web_factory_mod
import daylily_tapdb.web.runtime as runtime_mod
from daylily_tapdb.web.bridge import TapdbHostBridge


class _MappingResult:
    def __init__(self, rows):
        self._rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self._rows)

    def one(self):
        return self._rows[0]

    def __iter__(self):
        return iter(self._rows)


class _ScalarResult:
    def __init__(self, rows=None, scalar=None):
        self._rows = list(rows or [])
        self._scalar = scalar

    def all(self):
        return list(self._rows)

    def scalars(self):
        return self

    def scalar_one_or_none(self):
        return self._scalar


class _QueryStub:
    def __init__(self, first_value=None, one_value=None):
        self._first_value = first_value
        self._one_value = one_value

    def filter(self, *_args, **_kwargs):
        return self

    def first(self):
        return self._first_value

    def one(self):
        return self._one_value


def test_query_audit_trail_builds_filtered_sql_and_dataclasses():
    changed_at = datetime(2026, 4, 7, 10, 0, tzinfo=timezone.utc)
    captured = {}

    class _Session:
        def execute(self, statement, params):
            captured["sql"] = str(statement)
            captured["params"] = dict(params)
            return _MappingResult(
                [
                    {
                        "euid": "GX1",
                        "changed_by": "admin",
                        "operation_type": "UPDATE",
                        "changed_at": changed_at,
                        "name": "Root Tube",
                        "polymorphic_discriminator": "generic_instance",
                        "category": "container",
                        "type": "tube",
                        "subtype": "sample",
                        "bstatus": "active",
                        "old_value": "old",
                        "new_value": "new",
                    }
                ]
            )

    rows = audit_mod.query_audit_trail(
        _Session(),
        changed_by="admin",
        euid="GX1",
        since=changed_at,
        domain_code="DAY",
        issuer_app_code="CORE",
        limit=25,
        order="asc",
    )

    assert rows == [
        audit_mod.AuditEntry(
            euid="GX1",
            changed_by="admin",
            operation_type="UPDATE",
            changed_at=changed_at,
            name="Root Tube",
            polymorphic_discriminator="generic_instance",
            category="container",
            type="tube",
            subtype="sample",
            bstatus="active",
            old_value="old",
            new_value="new",
        )
    ]
    assert "al.changed_by = :changed_by" in captured["sql"]
    assert "al.rel_table_euid_fk = :euid" in captured["sql"]
    assert "al.changed_at >= :since" in captured["sql"]
    assert "al.domain_code = :domain_code" in captured["sql"]
    assert "al.issuer_app_code = :issuer_app_code" in captured["sql"]
    assert "ORDER BY al.changed_at ASC" in captured["sql"]
    assert captured["params"]["limit"] == 25


@pytest.mark.parametrize(
    ("fn", "expected_type", "expected_key"),
    [
        (stats_mod.get_template_stats, stats_mod.TemplateStats, "singleton_count"),
        (
            stats_mod.get_instance_stats,
            stats_mod.InstanceStats,
            "distinct_polymorphic_discriminators",
        ),
        (
            stats_mod.get_lineage_stats,
            stats_mod.LineageStats,
            "distinct_parent_types",
        ),
    ],
)
def test_stats_queries_include_optional_filters(fn, expected_type, expected_key):
    now = datetime(2026, 4, 7, 10, 30, tzinfo=timezone.utc)
    captured = {}

    class _Session:
        def execute(self, statement, params):
            captured["sql"] = str(statement)
            captured["params"] = dict(params)
            return _MappingResult(
                [
                    {
                        "total": 4,
                        "distinct_types": 2,
                        "distinct_subtypes": 3,
                        "distinct_categories": 1,
                        "latest_created": now,
                        "earliest_created": now - timedelta(days=7),
                        "average_age": timedelta(days=2),
                        "singleton_count": 1,
                        "distinct_poly": 2,
                        "distinct_parent_types": 2,
                        "distinct_child_types": 3,
                    }
                ]
            )

    result = fn(
        _Session(),
        include_deleted=True,
        domain_code="DAY",
        issuer_app_code="CORE",
    )

    assert isinstance(result, expected_type)
    assert getattr(result, expected_key) >= 1
    assert captured["params"]["is_deleted"] is True
    assert captured["params"]["domain_code"] == "DAY"
    assert captured["params"]["issuer_app_code"] == "CORE"
    assert "domain_code = :domain_code" in captured["sql"]
    assert "issuer_app_code = :issuer_app_code" in captured["sql"]


def test_receive_message_handles_insert_and_existing_paths():
    message_uuid = uuid.uuid4()
    receipt_uuid = uuid.uuid4()
    received_dt = datetime(2026, 4, 7, 11, 0, tzinfo=timezone.utc)

    class _InsertSession:
        def __init__(self) -> None:
            self.flushed = False

        def execute(self, _stmt):
            return SimpleNamespace(
                first=lambda: SimpleNamespace(
                    receipt_machine_uuid=receipt_uuid,
                    status="received",
                    received_dt=received_dt,
                )
            )

        def flush(self):
            self.flushed = True

    inserted = inbox_mod.receive_message(
        _InsertSession(),
        message_machine_uuid=message_uuid,
        payload={"ok": True},
        domain_code="DAY",
        issuer_app_code="CORE",
        source_destination="atlas",
    )
    assert inserted.message_machine_uuid == message_uuid
    assert inserted.receipt_machine_uuid == receipt_uuid
    assert inserted.status == "received"

    existing_row = SimpleNamespace(
        receipt_machine_uuid=uuid.uuid4(),
        status="processed",
        received_dt=received_dt,
        processed_dt=received_dt + timedelta(minutes=5),
    )

    class _ExistingSession:
        def execute(self, _stmt):
            return SimpleNamespace(first=lambda: None)

        def query(self, _model):
            return _QueryStub(one_value=existing_row)

    existing = inbox_mod.receive_message(
        _ExistingSession(),
        message_machine_uuid=message_uuid,
        payload={"ok": True},
    )
    assert existing.status == "processed"
    assert existing.processed_dt == existing_row.processed_dt


def test_inbox_status_helpers_execute_and_flush():
    captured = {"count": 0}
    expected = SimpleNamespace(message_machine_uuid=uuid.uuid4())

    class _Session:
        def execute(self, _stmt):
            captured["count"] += 1

        def flush(self):
            captured["flushed"] = captured.get("flushed", 0) + 1

        def query(self, _model):
            return _QueryStub(first_value=expected)

    session = _Session()
    inbox_mod.mark_inbox_processing(session, expected.message_machine_uuid)
    inbox_mod.mark_inbox_processed(session, expected.message_machine_uuid)
    inbox_mod.mark_inbox_failed(
        session,
        expected.message_machine_uuid,
        error_code="E1",
        error_message="x" * 20_000,
    )
    inbox_mod.mark_inbox_rejected(
        session,
        expected.message_machine_uuid,
        error_code="E2",
        error_message="bad",
    )

    assert captured["count"] == 4
    assert captured["flushed"] == 4
    assert (
        inbox_mod.get_inbox_message_by_machine_uuid(
            session, expected.message_machine_uuid
        )
        is expected
    )


def test_outbox_query_helpers_return_structured_results():
    event = SimpleNamespace(id=1, status="failed")
    attempt = SimpleNamespace(attempt_no=1)
    rows = iter(
        [
            _ScalarResult(
                rows=[
                    SimpleNamespace(status="pending", cnt=3),
                    SimpleNamespace(status="failed", cnt=1),
                ]
            ),
            _ScalarResult(rows=[event]),
            _ScalarResult(rows=[event]),
            _ScalarResult(rows=[attempt]),
            _ScalarResult(
                rows=[
                    SimpleNamespace(status="received", cnt=2),
                    SimpleNamespace(status="processed", cnt=4),
                ]
            ),
            _ScalarResult(rows=[event]),
            _ScalarResult(rows=[event]),
            _ScalarResult(scalar=event),
            _ScalarResult(scalar=event),
        ]
    )

    class _Session:
        def execute(self, _query):
            return next(rows)

    session = _Session()
    summary = outbox_queries_mod.outbox_status_summary(
        session, domain_code="DAY", issuer_app_code="CORE"
    )
    assert summary.pending == 3
    assert summary.failed == 1
    assert outbox_queries_mod.list_failed_events(
        session, domain_code="DAY", issuer_app_code="CORE", limit=5
    ) == [event]
    assert outbox_queries_mod.list_stale_delivering(
        session, domain_code="DAY", limit=5
    ) == [event]
    assert outbox_queries_mod.get_event_attempts(session, 1) == [attempt]
    inbox_summary = outbox_queries_mod.inbox_status_summary(
        session, domain_code="DAY", issuer_app_code="CORE"
    )
    assert inbox_summary.received == 2
    assert inbox_summary.processed == 4
    assert outbox_queries_mod.list_events_by_destination(
        session,
        "atlas",
        domain_code="DAY",
        issuer_app_code="CORE",
        status="failed",
        limit=3,
    ) == [event]
    assert outbox_queries_mod.list_by_destination(session, "atlas") == [event]
    assert (
        outbox_queries_mod.get_outbox_event_by_receipt_uuid(session, uuid.uuid4())
        is event
    )
    assert (
        outbox_queries_mod.lookup_by_machine_uuid(
            session,
            uuid.uuid4(),
            domain_code="DAY",
            issuer_app_code="CORE",
        )
        is event
    )


def test_admin_server_context_file_helpers_round_trip(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        admin_server_mod,
        "resolve_context",
        lambda **_kwargs: SimpleNamespace(ui_dir=lambda env: tmp_path / "ui" / env),
    )

    parser = admin_server_mod._build_parser()
    args = parser.parse_args(
        [
            "--config",
            "/tmp/tapdb.yaml",
            "--env",
            "dev",
            "--host",
            "127.0.0.1",
            "--port",
            "8911",
            "--ssl-keyfile",
            "/tmp/key.pem",
            "--ssl-certfile",
            "/tmp/cert.pem",
        ]
    )
    assert args.reload is False

    path = admin_server_mod._write_context_file(
        config_path="/tmp/tapdb.yaml",
        env_name="dev",
        host="127.0.0.1",
        port=8911,
    )
    monkeypatch.chdir(path.parent)
    assert admin_server_mod._context_file_path() == path.parent / "context.json"
    assert admin_server_mod._read_context_file() == {
        "config_path": str(Path("/tmp/tapdb.yaml").expanduser().resolve()),
        "env_name": "dev",
        "host": "127.0.0.1",
        "port": 8911,
    }

    (path.parent / "context.json").write_text(json.dumps(["bad"]), encoding="utf-8")
    with pytest.raises(RuntimeError, match="Invalid TAPDB admin context file"):
        admin_server_mod._read_context_file()


def test_admin_server_load_build_and_main_paths(tmp_path, monkeypatch):
    fake_app = SimpleNamespace(state=SimpleNamespace())
    fake_admin_main = SimpleNamespace(app=fake_app)
    set_calls = []
    monkeypatch.setattr(
        admin_server_mod, "set_cli_context", lambda **kwargs: set_calls.append(kwargs)
    )
    monkeypatch.setattr(
        admin_server_mod.importlib,
        "import_module",
        lambda name: fake_admin_main if name == "admin.main" else None,
    )
    monkeypatch.setattr(admin_server_mod.importlib, "reload", lambda module: module)

    app = admin_server_mod.load_admin_app(config_path="/tmp/tapdb.yaml", env_name="dev")
    assert app is fake_app
    assert app.state.tapdb_admin_module is fake_admin_main
    assert set_calls == [{"config_path": "/tmp/tapdb.yaml", "env_name": "dev"}]

    monkeypatch.setattr(
        admin_server_mod,
        "_read_context_file",
        lambda: {"config_path": "/tmp/tapdb.yaml", "env_name": "dev"},
    )
    monkeypatch.setattr(web_mod, "create_tapdb_web_app", lambda **kwargs: kwargs)
    assert admin_server_mod.build_app() == {
        "config_path": "/tmp/tapdb.yaml",
        "env_name": "dev",
    }

    monkeypatch.setattr(
        admin_server_mod,
        "_read_context_file",
        lambda: {"config_path": "", "env_name": "dev"},
    )
    with pytest.raises(RuntimeError, match="context file is incomplete"):
        admin_server_mod.build_app()

    parser = SimpleNamespace(
        parse_args=lambda: SimpleNamespace(
            config="/tmp/tapdb.yaml",
            env="dev",
            host="127.0.0.1",
            port=8911,
            ssl_keyfile="/tmp/key.pem",
            ssl_certfile="/tmp/cert.pem",
            reload=True,
        )
    )
    monkeypatch.setattr(admin_server_mod, "_build_parser", lambda: parser)
    context_file = tmp_path / "ui" / "context.json"
    context_file.parent.mkdir(parents=True, exist_ok=True)
    context_file.write_text("{}", encoding="utf-8")
    monkeypatch.setattr(
        admin_server_mod, "_write_context_file", lambda **_kwargs: context_file
    )

    run_calls = []
    monkeypatch.setitem(
        sys.modules,
        "uvicorn",
        SimpleNamespace(run=lambda *args, **kwargs: run_calls.append((args, kwargs))),
    )

    admin_server_mod.main()
    assert run_calls[0][0] == ("daylily_tapdb.cli.admin_server:build_app",)
    assert run_calls[0][1]["factory"] is True
    assert run_calls[0][1]["reload"] is True


def test_runtime_db_connection_session_scope_commit_and_rollback():
    class _Trans:
        def __init__(self):
            self.commits = 0
            self.rollbacks = 0

        def commit(self):
            self.commits += 1

        def rollback(self):
            self.rollbacks += 1

    class _Session:
        def __init__(self):
            self.trans = _Trans()
            self.closed = 0
            self.executed = []

        def begin(self):
            return self.trans

        def execute(self, *args):
            self.executed.append(args)

        def close(self):
            self.closed += 1

    session = _Session()
    bundle = runtime_mod.RuntimeBundle(
        config_path="/tmp/tapdb.yaml",
        env_name="dev",
        engine=create_engine("sqlite://"),
        SessionFactory=lambda: session,
        cfg={},
        schema_name="tapdb_unit",
    )
    conn = runtime_mod.RuntimeDBConnection(bundle)
    conn.app_username = "tester"

    with conn.session_scope(commit=False) as active:
        assert active is session
    assert session.trans.commits == 0
    assert session.trans.rollbacks == 1
    assert session.closed == 1
    assert session.executed

    session = _Session()
    conn = runtime_mod.RuntimeDBConnection(
        runtime_mod.RuntimeBundle(
            config_path="/tmp/tapdb.yaml",
            env_name="dev",
            engine=create_engine("sqlite://"),
            SessionFactory=lambda: session,
            cfg={},
            schema_name="tapdb_unit",
        )
    )
    with conn.session_scope(commit=True):
        pass
    assert session.trans.commits == 1
    assert session.trans.rollbacks == 0

    session = _Session()
    conn = runtime_mod.RuntimeDBConnection(
        runtime_mod.RuntimeBundle(
            config_path="/tmp/tapdb.yaml",
            env_name="dev",
            engine=create_engine("sqlite://"),
            SessionFactory=lambda: session,
            cfg={},
            schema_name="tapdb_unit",
        )
    )
    with pytest.raises(RuntimeError, match="boom"):
        with conn.session_scope(commit=True):
            raise RuntimeError("boom")
    assert session.trans.rollbacks == 1


def test_runtime_db_connection_session_scope_sets_search_path():
    class _Trans:
        def commit(self):
            return None

        def rollback(self):
            return None

    class _Session:
        def __init__(self):
            self.bind = SimpleNamespace(dialect=SimpleNamespace(name="postgresql"))
            self.statements = []

        def begin(self):
            return _Trans()

        def execute(self, stmt, params=None):
            self.statements.append((str(stmt), params or {}))

        def close(self):
            return None

    session = _Session()
    conn = runtime_mod.RuntimeDBConnection(
        runtime_mod.RuntimeBundle(
            config_path="/tmp/tapdb.yaml",
            env_name="dev",
            engine=create_engine("sqlite://"),
            SessionFactory=lambda: session,
            cfg={"schema_name": "tapdb_dev"},
            schema_name="tapdb_dev",
        )
    )

    with conn.session_scope(commit=False):
        pass

    assert "set_config('search_path'" in session.statements[0][0]
    assert session.statements[0][1]["schema_name"] == "tapdb_dev"


def test_runtime_engine_helpers_cover_auth_modes_and_cache(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        runtime_mod,
        "get_admin_settings_for_env",
        lambda env_name, config_path: {
            "db_pool_size": 3,
            "db_max_overflow": 4,
            "db_pool_timeout": 5,
            "db_pool_recycle": 6,
        },
    )
    monkeypatch.setattr(
        runtime_mod,
        "create_engine",
        lambda url, **kwargs: (
            captured.setdefault("engines", []).append((url, kwargs))
            or SimpleNamespace(dispose=lambda: None)
        ),
    )
    runtime_mod._create_engine(
        runtime_mod.URL.create("postgresql+psycopg2", username="tapdb", host="db"),
        config_path="/tmp/tapdb.yaml",
        env_name="dev",
        echo_sql=True,
    )
    assert captured["engines"][0][1]["pool_size"] == 3
    assert captured["engines"][0][1]["echo"] is True

    listeners = {}
    monkeypatch.setattr(
        runtime_mod.event,
        "listen",
        lambda _engine, name, fn, **_kwargs: listeners.setdefault(name, fn),
    )
    monkeypatch.setattr(
        runtime_mod.AuroraConnectionBuilder,
        "get_iam_auth_token",
        lambda **_kwargs: "iam-token",
    )
    runtime_mod._attach_aurora_password_provider(
        SimpleNamespace(),
        region="us-west-2",
        host="db.local",
        port=5432,
        user="tapdb",
        aws_profile="default",
        iam_auth=True,
        secret_arn=None,
        password="",
    )
    cparams = {}
    listeners["do_connect"](None, None, None, cparams)
    assert cparams["password"] == "iam-token"

    listeners = {}
    monkeypatch.setattr(
        runtime_mod.event,
        "listen",
        lambda _engine, name, fn, **_kwargs: listeners.setdefault(name, fn),
    )
    runtime_mod._attach_aurora_password_provider(
        SimpleNamespace(),
        region="us-west-2",
        host="db.local",
        port=5432,
        user="tapdb",
        aws_profile=None,
        iam_auth=False,
        secret_arn=None,
        password="secret",
    )
    cparams = {}
    listeners["do_connect"](None, None, None, cparams)
    assert cparams["password"] == "secret"

    listeners = {}
    monkeypatch.setattr(
        runtime_mod.event,
        "listen",
        lambda _engine, name, fn, **_kwargs: listeners.setdefault(name, fn),
    )
    runtime_mod._attach_aurora_password_provider(
        SimpleNamespace(),
        region="us-west-2",
        host="db.local",
        port=5432,
        user="tapdb",
        aws_profile=None,
        iam_auth=False,
        secret_arn=None,
        password="",
    )
    with pytest.raises(ValueError, match="requires a password or secret_arn"):
        listeners["do_connect"](None, None, None, {})

    created = []
    monkeypatch.setattr(
        runtime_mod,
        "get_db_config_for_env",
        lambda env, config_path: {
            "config_path": "/resolved/tapdb.yaml",
            "engine_type": "local",
            "host": "localhost",
            "port": "5432",
            "database": "tapdb_dev",
            "schema_name": "tapdb_dev",
            "user": "tapdb",
            "password": "",
        },
    )
    monkeypatch.setattr(
        runtime_mod,
        "_build_engine_for_cfg",
        lambda cfg, *, config_path, env_name: (
            created.append((cfg, config_path, env_name))
            or SimpleNamespace(dispose=lambda: None)
        ),
    )
    runtime_mod._clear_runtime_cache_for_tests()
    conn1 = runtime_mod.get_db("/tmp/tapdb.yaml", "DEV")
    conn2 = runtime_mod.get_db("/tmp/tapdb.yaml", "dev")
    assert len(created) == 1
    assert conn1._bundle is conn2._bundle
    runtime_mod._clear_runtime_cache_for_tests()

    with pytest.raises(RuntimeError, match="env name is required"):
        runtime_mod.get_db("/tmp/tapdb.yaml", "")

    monkeypatch.setattr(
        runtime_mod,
        "get_db_config_for_env",
        lambda env, config_path: {
            "config_path": "/resolved/tapdb.yaml",
            "engine_type": "local",
            "host": "localhost",
            "port": "5432",
            "database": "tapdb_dev",
            "user": "tapdb",
            "password": "",
        },
    )
    runtime_mod._clear_runtime_cache_for_tests()
    with pytest.raises(RuntimeError, match="schema_name"):
        runtime_mod.get_db("/tmp/tapdb.yaml", "dev")


@pytest.mark.anyio
async def test_web_factory_helpers_require_user_and_build_apps(monkeypatch):
    request = SimpleNamespace(state=SimpleNamespace())

    async def _user(_request):
        return {"uid": 1, "username": "admin"}

    monkeypatch.setattr("admin.auth.get_current_user", _user)
    user = await web_factory_mod.require_tapdb_api_user(request)
    assert user["uid"] == 1
    assert request.state.user == user

    async def _anon(_request):
        return None

    monkeypatch.setattr("admin.auth.get_current_user", _anon)
    with pytest.raises(HTTPException, match="tapdb_auth_required"):
        await web_factory_mod.require_tapdb_api_user(
            SimpleNamespace(state=SimpleNamespace())
        )

    assert (
        web_factory_mod._requested_path(
            SimpleNamespace(
                scope={
                    "root_path": "/tapdb",
                    "path": "/graph",
                    "query_string": b"depth=2",
                }
            )
        )
        == "/tapdb/graph?depth=2"
    )

    configured = []
    attached = []
    fake_app = SimpleNamespace(state=SimpleNamespace(tapdb_admin_module="admin-main"))
    monkeypatch.setattr(
        admin_server_mod,
        "load_admin_app",
        lambda **kwargs: fake_app,
    )
    monkeypatch.setattr(
        web_factory_mod,
        "_configure_template_environment",
        lambda admin_main, bridge: configured.append((admin_main, bridge)),
    )
    monkeypatch.setattr(
        web_factory_mod,
        "_attach_canonical_dag_router",
        lambda app, **kwargs: attached.append((app, kwargs)),
    )

    host_bridge = TapdbHostBridge(auth_mode="host_session", service_name="dewey")
    wrapped = web_factory_mod.create_tapdb_web_app(
        config_path="/tmp/tapdb.yaml",
        env_name="dev",
        host_bridge=host_bridge,
    )
    assert isinstance(wrapped, web_factory_mod.TapdbHostBridgeMount)
    assert configured[0] == ("admin-main", host_bridge)
    assert attached[0][1]["service_name"] == "dewey"

    plain = web_factory_mod.create_tapdb_web_app(
        config_path="/tmp/tapdb.yaml",
        env_name="dev",
        host_bridge=None,
    )
    assert plain is fake_app
