from __future__ import annotations

import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import typer

from daylily_tapdb import lineage
from daylily_tapdb.cli import user as user_cli
from daylily_tapdb.outbox import inbox, queries, repository, worker
from daylily_tapdb.outbox.contracts import DeliveryResult


class _OutputRecorder:
    def __init__(self):
        self.messages: list[tuple[str, str]] = []

    def _record(self, level: str, message: object) -> None:
        self.messages.append((level, str(message)))

    def success(self, message: object) -> None:
        self._record("success", message)

    def error(self, message: object) -> None:
        self._record("error", message)

    def print_text(self, message: object) -> None:
        self._record("text", message)

    def contains(self, text: str) -> bool:
        return any(text in message for _, message in self.messages)


@pytest.fixture
def user_output(monkeypatch: pytest.MonkeyPatch) -> _OutputRecorder:
    recorder = _OutputRecorder()
    monkeypatch.setattr(user_cli, "ccyo_out", recorder)
    return recorder


class _UserConnection:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    @contextmanager
    def session_scope(self, commit: bool = False):
        yield SimpleNamespace(commit=commit)


def test_user_outbox_lineage_campaign_formats_dates_without_inference() -> None:
    when = datetime(2026, 9, 2, 12, 34, tzinfo=UTC)
    assert user_cli._format_date(None) == "-"
    assert user_cli._format_date(when) == "2026-09-02"
    assert user_cli._format_date(when, include_time=True) == "2026-09-02 12:34"
    assert user_cli._format_date("   ") == "-"
    assert user_cli._format_date("2026-09-02T11:22:00Z", include_time=True) == (
        "2026-09-02 11:22"
    )
    assert user_cli._format_date("not-a-date") == "not-a-date"


@pytest.mark.parametrize(
    ("call", "patch_name", "message"),
    [
        (lambda: user_cli.user_set_role("alice", "owner"), None, "Invalid role"),
        (lambda: user_cli.user_set_role("alice", "admin"), "set_role", "not found"),
        (lambda: user_cli.user_deactivate("alice"), "set_active", "not found"),
        (lambda: user_cli.user_activate("alice"), "set_active", "not found"),
        (
            lambda: user_cli.user_set_password("alice", "secret"),
            "set_password_hash",
            "not found",
        ),
    ],
)
def test_user_outbox_lineage_campaign_user_mutations_fail_loudly(
    monkeypatch: pytest.MonkeyPatch,
    user_output: _OutputRecorder,
    call,
    patch_name: str | None,
    message: str,
) -> None:
    monkeypatch.setattr(
        user_cli, "_open_connection", lambda *_a, **_k: _UserConnection()
    )
    monkeypatch.setattr(user_cli, "_hash_password", lambda _password: "hash")
    if patch_name:
        monkeypatch.setattr(user_cli, patch_name, lambda *_a, **_k: False)
    with pytest.raises(typer.Exit):
        call()
    assert user_output.contains(message)


def test_user_outbox_lineage_campaign_delete_force_and_missing_paths(
    monkeypatch: pytest.MonkeyPatch, user_output: _OutputRecorder
) -> None:
    confirmations: list[str] = []
    monkeypatch.setattr(
        user_cli.typer,
        "confirm",
        lambda prompt: confirmations.append(prompt) or True,
    )
    monkeypatch.setattr(
        user_cli, "_open_connection", lambda *_a, **_k: _UserConnection()
    )
    monkeypatch.setattr(user_cli, "soft_delete", lambda *_a, **_k: True)
    user_cli.user_delete("alice", force=False)
    assert confirmations == ["Permanently delete user 'alice'?"]

    confirmations.clear()
    user_cli.user_delete("bob", force=True)
    assert confirmations == []

    monkeypatch.setattr(user_cli, "soft_delete", lambda *_a, **_k: False)
    with pytest.raises(typer.Exit):
        user_cli.user_delete("missing", force=True)
    assert user_output.contains("not found")


class _Result:
    def __init__(self, *, rows=None, scalar=None):
        self.rows = list(rows or [])
        self.scalar = scalar

    def all(self):
        return list(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None

    def scalars(self):
        return self

    def scalar_one_or_none(self):
        return self.scalar

    def scalar_one(self):
        return self.scalar

    def one(self):
        return self.rows[0]

    def one_or_none(self):
        return self.rows[0] if self.rows else None

    def mappings(self):
        return self


class _QuerySession:
    def __init__(self, results):
        self.results = list(results)
        self.statements: list[object] = []
        self.flushes = 0
        self.added: list[object] = []

    def execute(self, statement, *_args, **_kwargs):
        self.statements.append(statement)
        return self.results.pop(0)

    def flush(self):
        self.flushes += 1

    def add(self, value):
        self.added.append(value)


def test_user_outbox_lineage_campaign_inbox_insert_includes_explicit_scope() -> None:
    message_uuid = uuid.uuid4()
    receipt_uuid = uuid.uuid4()
    received = datetime.now(UTC)
    tenant_id = uuid.uuid4()
    result = SimpleNamespace(
        receipt_machine_uuid=receipt_uuid,
        status="received",
        received_dt=received,
    )
    session = _QuerySession([_Result(rows=[result])])
    receipt = inbox.receive_message(
        session,
        message_machine_uuid=message_uuid,
        payload={"event": "created"},
        tenant_id=tenant_id,
        domain_code="Z",
        issuer_app_code="repo",
        source_domain_code="A",
        source_issuer_app_code="sender",
        source_destination="svc://receiver",
    )
    compiled = session.statements[0].compile().params
    assert compiled["tenant_id"] == tenant_id
    assert compiled["domain_code"] == "Z"
    assert compiled["issuer_app_code"] == "repo"
    assert compiled["source_domain_code"] == "A"
    assert compiled["source_issuer_app_code"] == "sender"
    assert compiled["source_destination"] == "svc://receiver"
    assert receipt.receipt_machine_uuid == receipt_uuid
    assert session.flushes == 1


def test_user_outbox_lineage_campaign_inbox_conflict_returns_existing() -> None:
    message_uuid = uuid.uuid4()
    existing = SimpleNamespace(
        receipt_machine_uuid=uuid.uuid4(),
        status="processed",
        received_dt=datetime.now(UTC),
        processed_dt=datetime.now(UTC),
    )
    session = MagicMock()
    session.execute.return_value.first.return_value = None
    session.query.return_value.filter.return_value.one.return_value = existing
    receipt = inbox.receive_message(
        session, message_machine_uuid=message_uuid, payload={}
    )
    assert receipt.status == "processed"
    assert receipt.processed_dt == existing.processed_dt
    session.flush.assert_not_called()


def test_user_outbox_lineage_campaign_inbox_transitions_render_expected_values() -> (
    None
):
    session = _QuerySession([_Result(), _Result(), _Result(), _Result()])
    message_uuid = uuid.uuid4()
    inbox.mark_inbox_processing(session, message_uuid)
    inbox.mark_inbox_processed(session, message_uuid)
    inbox.mark_inbox_failed(
        session,
        message_uuid,
        error_code="failed",
        error_message="x" * 10_001,
    )
    inbox.mark_inbox_rejected(session, message_uuid, error_code="nope")
    params = [statement.compile().params for statement in session.statements]
    assert params[0]["status"] == "processing"
    assert params[1]["status"] == "processed"
    assert len(params[2]["error_message"]) == 10_000
    assert params[3]["error_message"] is None
    assert session.flushes == 4


def test_user_outbox_lineage_campaign_query_helpers_apply_all_scopes() -> None:
    rows = [
        SimpleNamespace(status="pending", cnt=2),
        SimpleNamespace(status="failed", cnt=1),
    ]
    session = _QuerySession(
        [
            _Result(rows=rows),
            _Result(rows=["failed-event"]),
            _Result(rows=["stale-event"]),
            _Result(rows=[SimpleNamespace(status="processed", cnt=3)]),
            _Result(rows=["destination-event"]),
            _Result(scalar="machine-event"),
        ]
    )
    outbox_summary = queries.outbox_status_summary(
        session, domain_code="Z", issuer_app_code="repo"
    )
    assert outbox_summary.pending == 2
    assert outbox_summary.failed == 1
    assert queries.list_failed_events(
        session, domain_code="Z", issuer_app_code="repo", limit=4
    ) == ["failed-event"]
    assert queries.list_stale_delivering(session, domain_code="Z", limit=5) == [
        "stale-event"
    ]
    inbox_summary = queries.inbox_status_summary(
        session, domain_code="Z", issuer_app_code="repo"
    )
    assert inbox_summary.processed == 3
    assert queries.list_events_by_destination(
        session,
        "svc://receiver",
        domain_code="Z",
        issuer_app_code="repo",
        status="failed",
        limit=6,
    ) == ["destination-event"]
    assert (
        queries.lookup_by_machine_uuid(
            session,
            uuid.uuid4(),
            domain_code="Z",
            issuer_app_code="repo",
        )
        == "machine-event"
    )
    rendered = [str(statement) for statement in session.statements]
    assert all("domain_code" in statement for statement in rendered)
    assert "issuer_app_code" in rendered[0]
    assert "status" in rendered[4]


def test_user_outbox_lineage_campaign_query_helpers_allow_unscoped_observation() -> (
    None
):
    session = _QuerySession(
        [
            _Result(rows=[]),
            _Result(rows=[]),
            _Result(rows=[]),
            _Result(rows=[]),
            _Result(rows=[]),
            _Result(scalar=None),
        ]
    )
    assert queries.outbox_status_summary(session).pending == 0
    assert queries.list_failed_events(session) == []
    assert queries.list_stale_delivering(session) == []
    assert queries.inbox_status_summary(session).received == 0
    assert queries.list_events_by_destination(session, "svc://receiver") == []
    assert queries.lookup_by_machine_uuid(session, uuid.uuid4()) is None


def test_user_outbox_lineage_campaign_repository_builders_include_explicit_scope() -> (
    None
):
    tenant_id = uuid.uuid4()
    enqueue = repository._build_enqueue_stmt(
        message_uid=7,
        destination="svc://receiver",
        dedupe_key="key",
        tenant_id=tenant_id,
        domain_code="Z",
        issuer_app_code="repo",
    )
    params = enqueue.compile().params
    assert params["tenant_id"] == tenant_id
    assert params["domain_code"] == "Z"
    assert params["issuer_app_code"] == "repo"

    claim = repository._build_claim_select(
        batch_size=3, domain_code="Z", issuer_app_code="repo"
    )
    rendered = str(claim.compile())
    assert "domain_code" in rendered
    assert "issuer_app_code" in rendered


def test_user_outbox_lineage_campaign_repository_scope_resolution_paths() -> None:
    direct = _QuerySession([])
    assert repository._resolve_session_scope(
        direct, domain_code="Z", issuer_app_code="repo"
    ) == ("Z", "repo")
    assert direct.statements == []

    session = _QuerySession([_Result(rows=[("A", "owner")])])
    assert repository._resolve_session_scope(
        session, domain_code="Z", issuer_app_code=None
    ) == ("Z", "owner")


def test_user_outbox_lineage_campaign_repository_lookup_paths() -> None:
    missing = _QuerySession([_Result(scalar=None)])
    assert (
        repository._lookup_existing_machine_uuid(
            missing,
            "svc://receiver",
            "key",
            domain_code="Z",
            issuer_app_code="repo",
        )
        is None
    )

    machine_uuid = uuid.uuid4()
    found = _QuerySession([_Result(scalar=7), _Result(scalar=machine_uuid)])
    assert (
        repository._lookup_existing_machine_uuid(
            found,
            "svc://receiver",
            "key",
            domain_code="Z",
            issuer_app_code="repo",
        )
        == machine_uuid
    )

    with pytest.raises(ValueError, match="Message uid 8 not found"):
        repository._lookup_message_scope(_QuerySession([_Result()]), 8)


class _Nested:
    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


class _EnqueueSession(_QuerySession):
    def __init__(self, results):
        super().__init__(results)
        self.nested = _Nested()

    def begin_nested(self):
        return self.nested


def test_user_outbox_lineage_campaign_enqueue_success_conflict_and_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    machine_uuid = uuid.uuid4()
    message = SimpleNamespace(uid=7, machine_uuid=machine_uuid)
    monkeypatch.setattr(
        repository, "_resolve_session_scope", lambda *_a, **_k: ("Z", "repo")
    )
    monkeypatch.setattr(
        repository, "_lookup_existing_machine_uuid", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        repository, "_create_message_instance", lambda *_a, **_k: message
    )
    session = _EnqueueSession([_Result(scalar=11)])
    result = repository.enqueue_event(
        session,
        uuid.uuid4(),
        "created",
        None,
        {},
        "svc://receiver",
        "key",
    )
    assert result == machine_uuid
    assert session.nested.committed is True

    winner_uuid = uuid.uuid4()
    winner_lookups = iter([None, winner_uuid])
    monkeypatch.setattr(
        repository,
        "_lookup_existing_machine_uuid",
        lambda *_a, **_k: next(winner_lookups),
    )
    conflict = _EnqueueSession([_Result(scalar=None)])
    assert (
        repository.enqueue_event(
            conflict,
            uuid.uuid4(),
            "created",
            None,
            {},
            "svc://receiver",
            "key",
        )
        == winner_uuid
    )
    assert conflict.nested.rolled_back is True

    monkeypatch.setattr(
        repository, "_lookup_existing_machine_uuid", lambda *_a, **_k: None
    )
    monkeypatch.setattr(
        repository,
        "_create_message_instance",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("create failed")),
    )
    failed = _EnqueueSession([])
    with pytest.raises(RuntimeError, match="create failed"):
        repository.enqueue_event(
            failed,
            uuid.uuid4(),
            "created",
            None,
            {},
            "svc://receiver",
            "key",
        )
    assert failed.nested.rolled_back is True


def test_user_outbox_lineage_campaign_enqueue_missing_conflict_winner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    lookups = iter([None, None])
    monkeypatch.setattr(
        repository, "_resolve_session_scope", lambda *_a, **_k: ("Z", "repo")
    )
    monkeypatch.setattr(
        repository,
        "_lookup_existing_machine_uuid",
        lambda *_a, **_k: next(lookups),
    )
    monkeypatch.setattr(
        repository,
        "_create_message_instance",
        lambda *_a, **_k: SimpleNamespace(uid=7, machine_uuid=uuid.uuid4()),
    )
    with pytest.raises(RuntimeError, match="disappeared"):
        repository.enqueue_event(
            _EnqueueSession([_Result(scalar=None)]),
            uuid.uuid4(),
            "created",
            None,
            {},
            "svc://receiver",
            "key",
        )


def test_user_outbox_lineage_campaign_fanout_claim_and_optional_receipts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        repository,
        "_lookup_message_scope",
        lambda *_a, **_k: (None, "Z", "repo"),
    )
    fanout = _QuerySession([_Result(scalar=1), _Result(scalar=None)])
    assert repository.enqueue_fanout(
        fanout, 7, [("svc://a", "a"), ("svc://b", "b")]
    ) == [1]

    empty = _QuerySession([_Result(rows=[])])
    assert repository.claim_events(empty) == []

    events = [SimpleNamespace(attempt_count=None), SimpleNamespace(attempt_count=3)]
    claimed = _QuerySession([_Result(rows=events)])
    assert repository.claim_events(claimed, lock_timeout_s=10) == events
    assert events[0].attempt_count == 1
    assert events[1].attempt_count == 4
    assert events[0].claim_token == events[1].claim_token

    receipt_uuid = uuid.uuid4()
    updates = _QuerySession([_Result(), _Result(), _Result(), _Result()])
    repository.mark_received(updates, 1, receipt_machine_uuid=receipt_uuid)
    repository.mark_processed(updates, 2, receipt_machine_uuid=receipt_uuid)
    repository.mark_received(updates, 3)
    repository.mark_processed(updates, 4)
    params = [statement.compile().params for statement in updates.statements]
    assert params[0]["receipt_machine_uuid"] == receipt_uuid
    assert params[1]["receipt_machine_uuid"] == receipt_uuid
    assert "receipt_machine_uuid" not in params[2]
    assert "receipt_machine_uuid" not in params[3]


def test_user_outbox_lineage_campaign_record_attempt_bounds_evidence() -> None:
    session = _QuerySession([])

    def assign_uid():
        session.flushes += 1
        session.added[-1].uid = 91

    session.flush = assign_uid
    tenant_id = uuid.uuid4()
    uid = repository.record_attempt(
        session,
        outbox_event_id=7,
        attempt_no=2,
        transport_status="transport_failed",
        tenant_id=tenant_id,
        domain_code="Z",
        issuer_app_code="repo",
        transport_error="e" * 10_001,
        response_body_excerpt="b" * 10_001,
    )
    attempt = session.added[0]
    assert uid == 91
    assert attempt.tenant_id == tenant_id
    assert attempt.domain_code == "Z"
    assert attempt.issuer_app_code == "repo"
    assert len(attempt.transport_error) == 10_000
    assert len(attempt.response_body_excerpt) == 10_000

    session = _QuerySession([])
    session.flush = lambda: setattr(session.added[-1], "uid", 92)
    assert (
        repository.record_attempt(
            session,
            outbox_event_id=8,
            attempt_no=1,
            transport_status="attempted",
        )
        == 92
    )
    assert session.added[0].transport_error is None
    assert session.added[0].response_body_excerpt is None


class _WorkerSession:
    def __init__(self):
        self.expunged: list[object] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def begin(self):
        return self

    def expunge(self, value):
        self.expunged.append(value)


def _event(identifier: int, *, attempts: int, message=True):
    return SimpleNamespace(
        id=identifier,
        attempt_count=attempts,
        message=SimpleNamespace(uid=identifier) if message else None,
        tenant_id=None,
        domain_code="Z",
        issuer_app_code="repo",
        destination="svc://receiver",
    )


def test_user_outbox_lineage_campaign_worker_claim_failure_and_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        worker,
        "claim_events",
        lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("claim failed")),
    )
    assert worker.dispatch_batch(_WorkerSession, lambda _event: None) == 0

    monkeypatch.setattr(worker, "claim_events", lambda *_a, **_k: [])
    assert worker.dispatch_batch(_WorkerSession, lambda _event: None) == 0


def test_user_outbox_lineage_campaign_worker_routes_every_delivery_outcome(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = [
        _event(1, attempts=1, message=True),
        _event(2, attempts=1, message=False),
        _event(3, attempts=1),
        _event(4, attempts=10),
        _event(5, attempts=10),
        _event(6, attempts=2),
    ]
    monkeypatch.setattr(worker, "claim_events", lambda *_a, **_k: events)
    receipts = {identifier: uuid.uuid4() for identifier in (1, 2)}
    results = {
        1: DeliveryResult.processed(receipts[1]),
        2: DeliveryResult(success=True, receipt_machine_uuid=receipts[2]),
        3: DeliveryResult.rejected("rejected"),
        4: DeliveryResult.transport_failed("terminal", retryable=False),
        5: DeliveryResult.transport_failed("exhausted", retryable=True),
        6: DeliveryResult.transport_failed("retry", retryable=True),
    }
    calls: list[tuple[str, int, dict]] = []
    monkeypatch.setattr(
        worker,
        "record_attempt",
        lambda _session, **kwargs: (
            calls.append(("attempt", kwargs["outbox_event_id"], kwargs)) or 1
        ),
    )
    for name in (
        "mark_processed",
        "mark_received",
        "mark_rejected",
        "mark_dead_letter",
        "mark_failed",
    ):
        monkeypatch.setattr(
            worker,
            name,
            lambda _session, event_id, _name=name, **kwargs: calls.append(
                (_name, event_id, kwargs)
            ),
        )

    assert (
        worker.dispatch_batch(
            _WorkerSession,
            lambda event: results[event.id],
            max_attempts=10,
            domain_code="Z",
            issuer_app_code="repo",
        )
        == 6
    )
    transitions = [
        (name, identifier) for name, identifier, _ in calls if name != "attempt"
    ]
    assert transitions == [
        ("mark_processed", 1),
        ("mark_received", 2),
        ("mark_rejected", 3),
        ("mark_dead_letter", 4),
        ("mark_dead_letter", 5),
        ("mark_failed", 6),
    ]
    received = next(
        kwargs
        for name, identifier, kwargs in calls
        if name == "mark_received" and identifier == 2
    )
    assert received["receipt_status"] == "received"


def test_user_outbox_lineage_campaign_worker_loop_sleeps_only_when_idle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StopLoopError(Exception):
        pass

    outcomes = iter([1, 0, StopLoopError()])

    def dispatch(*_args, **_kwargs):
        value = next(outcomes)
        if isinstance(value, Exception):
            raise value
        return value

    sleeps: list[float] = []
    monkeypatch.setattr(worker, "dispatch_batch", dispatch)
    monkeypatch.setattr(worker.time, "sleep", lambda delay: sleeps.append(delay))
    with pytest.raises(StopLoopError):
        worker.run_dispatch_loop(
            _WorkerSession,
            lambda _event: DeliveryResult.received(uuid.uuid4()),
            poll_interval_s=-2,
        )
    assert sleeps == [0.0]
    assert worker._retry_delay_s(-3) == 1.0
    assert worker._retry_delay_s(100) == 1800.0


class _LineageQuery:
    def __init__(self, values):
        self.values = list(values)

    def __iter__(self):
        return iter(self.values)

    def count(self):
        return len(self.values)

    def first(self):
        return self.values[0] if self.values else None

    def all(self):
        return list(self.values)

    def marker(self):
        return "delegated"

    def __getitem__(self, item):
        return self.values[item]


def test_user_outbox_lineage_campaign_lineage_proxy_list_and_query_semantics() -> None:
    listed = lineage.LineageQueryProxy(["a", "b"])
    assert list(listed) == ["a", "b"]
    assert len(listed) == 2
    assert bool(listed) is True
    assert listed.all() == ["a", "b"]
    assert listed.first() == "a"
    assert listed.count() == 2
    assert listed[1] == "b"

    empty = lineage.LineageQueryProxy([])
    assert empty.first() is None
    assert bool(empty) is False

    proxied = lineage.LineageQueryProxy(_LineageQuery(["x"]))
    assert len(proxied) == 1
    assert bool(proxied) is True
    assert proxied.all() == ["x"]
    assert proxied.first() == "x"
    assert proxied.count() == 1
    assert proxied.marker() == "delegated"


def test_user_outbox_lineage_campaign_lineage_graph_deduplicates_nodes_and_keeps_edges() -> (
    None
):
    rows = [
        {
            "euid": "node-a",
            "uid": 1,
            "name": "A",
            "type": "sample",
            "category": "object",
            "subtype": "a",
            "version": "1.0",
            "depth": 0,
            "lineage_euid": None,
            "lineage_parent_euid": None,
            "lineage_child_euid": None,
            "relationship_type": None,
        },
        {
            "euid": "node-a",
            "uid": 1,
            "name": "A",
            "type": "sample",
            "category": "object",
            "subtype": "a",
            "version": "1.0",
            "depth": 1,
            "lineage_euid": "lineage-1",
            "lineage_parent_euid": "node-a",
            "lineage_child_euid": "node-b",
            "relationship_type": "contains",
        },
        {
            "euid": "node-b",
            "uid": 2,
            "name": "B",
            "type": "sample",
            "category": "object",
            "subtype": "b",
            "version": "1.0",
            "depth": 1,
            "lineage_euid": "lineage-1",
            "lineage_parent_euid": "node-a",
            "lineage_child_euid": "node-b",
            "relationship_type": "contains",
        },
    ]
    graph = lineage.get_lineage_graph(_QuerySession([_Result(rows=rows)]), "node-a")
    assert [node.euid for node in graph.nodes] == ["node-a", "node-b"]
    assert len(graph.edges) == 2
    assert graph.edges[0].relationship_type == "contains"
