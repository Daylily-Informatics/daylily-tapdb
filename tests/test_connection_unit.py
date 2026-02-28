import types


def test_connection_builds_default_url_and_creates_engine(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setenv("PGPORT", "5544")
    monkeypatch.setenv("PGPASSWORD", "pw")
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
    assert "postgresql://alice:pw@localhost:5544/tapdb" == called["url"]
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

    with caplog.at_level("WARNING"):
        conn._set_session_username(BadSession())


def test_session_scope_commit_true_commits(monkeypatch):
    from daylily_tapdb import connection as m

    monkeypatch.setattr(
        m, "create_engine", lambda *a, **k: types.SimpleNamespace(dispose=lambda: None)
    )
    monkeypatch.setattr(m, "sessionmaker", lambda bind: lambda: None)
    conn = m.TAPDBConnection(db_url="sqlite:///:memory:", app_username="pytest")

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
            self.trans = Trans()
            self.closed = False
            self.executed = False

        def begin(self):
            return self.trans

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
    conn = m.TAPDBConnection(db_url="sqlite:///:memory:", app_username="pytest")

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
            self.trans = Trans()

        def begin(self):
            return self.trans

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
    conn = m.TAPDBConnection(db_url="sqlite:///:memory:", app_username="pytest")

    class Trans:
        def __init__(self):
            self.rolled_back = False

        def commit(self):
            raise AssertionError("should not commit")

        def rollback(self):
            self.rolled_back = True

    class Sess:
        def __init__(self):
            self.trans = Trans()

        def begin(self):
            return self.trans

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
