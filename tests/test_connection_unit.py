import types

import pytest


@pytest.fixture(autouse=True)
def _preset_domain_env(monkeypatch):
    """All connection tests default to Z/daylily-tapdb domain/owner values."""
    monkeypatch.setenv("MERIDIAN_DOMAIN_CODE", "Z")
    monkeypatch.setenv("TAPDB_OWNER_REPO", "daylily-tapdb")


def test_connection_builds_default_url_and_creates_engine(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setenv("USER", "alice")

    called = {}

    class FakeEngine:
        def dispose(self):
            return None

    def fake_create_engine(url, **kwargs):
        called["url"] = url
        called["kwargs"] = kwargs
        return FakeEngine()

    # avoid needing a real SQLAlchemy Engine inside sessionmaker
    monkeypatch.setattr(m, "create_engine", fake_create_engine)
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)

    conn = m.TAPDBConnection(db_url=None, db_name="tapdb")
    assert "postgresql://alice:@localhost:5533/tapdb" == called["url"]
    assert conn.engine is not None


def test_set_session_username_logs_and_swallows_execute_error(monkeypatch, caplog):
    from daylily_tapdb import connection as m

    # minimal conn with no real engine/sessionmaker
    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(db_url="sqlite:///:memory:", app_username="pytest")

    class BadSession:
        def execute(self, *a, **k):
            raise RuntimeError("boom")

        def rollback(self):
            return None

    with caplog.at_level("WARNING"):
        conn._set_session_username(BadSession())


def test_set_session_domain_code_executes_for_postgresql(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(
        db_url="sqlite:///:memory:",
        domain_code="Z",
        owner_repo_name="daylily-tapdb",
        schema_name="tapdb_unit",
    )

    stmts: list[str] = []

    class Sess:
        def __init__(self):
            self.bind = types.SimpleNamespace(
                dialect=types.SimpleNamespace(name="postgresql")
            )

        def execute(self, stmt, params=None):
            stmts.append(str(stmt))

        def begin_nested(self):
            return types.SimpleNamespace(commit=lambda: None, rollback=lambda: None)

    conn._set_session_domain_code(Sess(), local=True)
    assert any("current_domain_code" in s for s in stmts)
    assert any("current_owner_repo_name" in s for s in stmts)


def test_missing_domain_env_raises(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.delenv("MERIDIAN_DOMAIN_CODE", raising=False)
    monkeypatch.delenv("TAPDB_OWNER_REPO", raising=False)
    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)

    with pytest.raises(ValueError, match="MERIDIAN_DOMAIN_CODE is required"):
        m.TAPDBConnection(db_url="sqlite:///:memory:")


def test_set_session_domain_code_skips_non_postgresql(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(db_url="sqlite:///:memory:")

    calls = {"execute": 0}

    class Sess:
        def __init__(self):
            self.bind = types.SimpleNamespace(
                dialect=types.SimpleNamespace(name="sqlite")
            )

        def execute(self, *_a, **_k):
            calls["execute"] += 1

    conn._set_session_domain_code(Sess(), local=True)
    assert calls["execute"] == 0


def test_session_scope_commit_true_commits(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(
        db_url="sqlite:///:memory:",
        app_username="pytest",
        schema_name="tapdb_unit",
    )

    class Trans:
        def __init__(self):
            self.committed = False
            self.rolled_back = False

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

    class Sess:
        def __init__(self):
            self.bind = types.SimpleNamespace(
                dialect=types.SimpleNamespace(name="postgresql")
            )
            self.trans = Trans()
            self.closed = False
            self.executed = False

        def begin(self):
            return self.trans

        def begin_nested(self):
            return types.SimpleNamespace(commit=lambda: None, rollback=lambda: None)

        def execute(self, *a, **k):
            self.executed = True

        def close(self):
            self.closed = True

    s = Sess()
    conn._Session = lambda: s

    with conn.session_scope(commit=True) as session:
        assert session is s

    assert s.trans.committed is True
    assert s.trans.rolled_back is False
    assert s.closed is True
    assert s.executed is True


def test_session_scope_commit_false_rolls_back(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(
        db_url="sqlite:///:memory:",
        app_username="pytest",
        schema_name="tapdb_unit",
    )

    class Trans:
        def __init__(self):
            self.committed = False
            self.rolled_back = False

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

    class Sess:
        def __init__(self):
            self.bind = types.SimpleNamespace(
                dialect=types.SimpleNamespace(name="postgresql")
            )
            self.trans = Trans()

        def begin(self):
            return self.trans

        def begin_nested(self):
            return types.SimpleNamespace(commit=lambda: None, rollback=lambda: None)

        def execute(self, *a, **k):
            return None

        def close(self):
            return None

    s = Sess()
    conn._Session = lambda: s

    with conn.session_scope(commit=False):
        pass

    assert s.trans.committed is False
    assert s.trans.rolled_back is True


def test_session_scope_exception_rolls_back_and_reraises(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(
        db_url="sqlite:///:memory:",
        app_username="pytest",
        schema_name="tapdb_unit",
    )

    class Trans:
        def __init__(self):
            self.rolled_back = False

        def commit(self):
            raise AssertionError("should not commit")

        def rollback(self):
            self.rolled_back = True

    class Sess:
        def __init__(self):
            self.bind = types.SimpleNamespace(
                dialect=types.SimpleNamespace(name="postgresql")
            )
            self.trans = Trans()

        def begin(self):
            return self.trans

        def begin_nested(self):
            return types.SimpleNamespace(commit=lambda: None, rollback=lambda: None)

        def execute(self, *a, **k):
            return None

        def close(self):
            return None

    s = Sess()
    conn._Session = lambda: s

    try:
        with conn.session_scope(commit=True):
            raise ValueError("fail")
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")

    assert s.trans.rolled_back is True


def test_reflect_tables_and_close_handle_exceptions(monkeypatch, caplog):
    from daylily_tapdb import connection as m

    class Engine:
        def __init__(self):
            self.raise_dispose = False

        def dispose(self):
            if self.raise_dispose:
                raise RuntimeError("dispose boom")

    monkeypatch.setattr(m, "create_engine", lambda *a, **k: Engine())
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(db_url="sqlite:///:memory:")

    prepared = {}

    class AB:
        def prepare(self, autoload_with):
            prepared["autoload_with"] = autoload_with

    conn.AutomapBase = AB()
    conn.reflect_tables()
    assert prepared["autoload_with"] is conn.engine

    conn.engine.raise_dispose = True
    with caplog.at_level("WARNING"):
        conn.close()


def test_set_session_timezone_utc_skips_non_postgresql(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(db_url="sqlite:///:memory:")

    calls = {"execute": 0}

    class Sess:
        def __init__(self):
            self.bind = types.SimpleNamespace(
                dialect=types.SimpleNamespace(name="sqlite")
            )

        def execute(self, *_a, **_k):
            calls["execute"] += 1

    conn._set_session_timezone_utc(Sess(), local=True)
    assert calls["execute"] == 0


def test_set_session_timezone_utc_is_noop_for_postgresql(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(db_url="sqlite:///:memory:")

    calls = {"execute": 0}

    class Sess:
        def __init__(self):
            self.bind = types.SimpleNamespace(
                dialect=types.SimpleNamespace(name="postgresql")
            )

        def execute(self, *_a, **_k):
            calls["execute"] += 1

    conn._set_session_timezone_utc(Sess(), local=True)
    assert calls["execute"] == 0


def test_set_session_search_path_executes_for_postgresql(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(db_url="sqlite:///:memory:", schema_name="tapdb_unit")

    calls = []

    class Sess:
        def __init__(self):
            self.bind = types.SimpleNamespace(
                dialect=types.SimpleNamespace(name="postgresql")
            )

        def begin_nested(self):
            return types.SimpleNamespace(commit=lambda: None, rollback=lambda: None)

        def execute(self, stmt, params=None):
            calls.append((str(stmt), params))

    conn._set_session_search_path(Sess(), local=True)
    assert "set_config('search_path'" in calls[0][0]
    assert calls[0][1]["schema_name"] == "tapdb_unit"


def test_set_session_search_path_requires_schema_for_postgresql(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(db_url="sqlite:///:memory:")

    class Sess:
        def __init__(self):
            self.bind = types.SimpleNamespace(
                dialect=types.SimpleNamespace(name="postgresql")
            )

    with pytest.raises(ValueError, match="schema_name is required"):
        conn._set_session_search_path(Sess(), local=True)


def test_session_scope_domain_setup_failure_does_not_abort_outer_transaction(
    monkeypatch,
):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(
        db_url="sqlite:///:memory:",
        app_username="pytest",
        schema_name="tapdb_unit",
    )

    class Trans:
        def __init__(self):
            self.committed = False
            self.rolled_back = False

        def commit(self):
            self.committed = True

        def rollback(self):
            self.rolled_back = True

    class Nested:
        def __init__(self, owner):
            self.owner = owner

        def commit(self):
            self.owner.in_savepoint = False

        def rollback(self):
            self.owner.in_savepoint = False
            self.owner.poisoned = False

    class Sess:
        def __init__(self):
            self.bind = types.SimpleNamespace(
                dialect=types.SimpleNamespace(name="postgresql")
            )
            self.trans = Trans()
            self.closed = False
            self.poisoned = False
            self.in_savepoint = False
            self.fail_domain_once = True
            self.body_executed = False

        def begin(self):
            return self.trans

        def begin_nested(self):
            self.in_savepoint = True
            return Nested(self)

        def execute(self, stmt, *_a, **_k):
            sql = str(stmt)
            if self.poisoned and not self.in_savepoint:
                raise RuntimeError("current transaction is aborted")
            if "current_domain_code" in sql and self.fail_domain_once:
                self.fail_domain_once = False
                self.poisoned = True
                raise RuntimeError("could not set session.current_domain_code")
            if sql == "SELECT 1":
                self.body_executed = True

        def close(self):
            self.closed = True

    s = Sess()
    conn._Session = lambda: s

    with conn.session_scope(commit=True) as session:
        session.execute("SELECT 1")

    assert s.body_executed is True
    assert s.trans.committed is True
    assert s.trans.rolled_back is False
    assert s.closed is True
