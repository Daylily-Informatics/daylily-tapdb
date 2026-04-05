from __future__ import annotations

import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from daylily_tapdb.outbox import worker
from daylily_tapdb.outbox.contracts import DeliveryResult


@dataclass
class _FakeMessage:
    """Simulates the generic_instance message object."""

    machine_uuid: str = "fake-uuid"
    json_addl: dict = None

    def __post_init__(self):
        if self.json_addl is None:
            self.json_addl = {"event_type": "test", "payload": {}}


@dataclass
class _FakeEvent:
    id: int
    destination: str
    attempt_count: int
    message: _FakeMessage = None

    def __post_init__(self):
        if self.message is None:
            self.message = _FakeMessage()


class _FakeSession:
    def __init__(self) -> None:
        self.expunge_calls: list[object] = []
        self.added: list[object] = []

    def expunge(self, obj: object) -> None:
        self.expunge_calls.append(obj)

    def add(self, obj: object) -> None:
        self.added.append(obj)

    def flush(self) -> None:
        # Assign a fake uid to any added objects that need one
        for obj in self.added:
            if hasattr(obj, "uid") and obj.uid is None:
                obj.uid = 999

    @contextmanager
    def begin(self):
        yield


class _SessionFactory:
    def __init__(self) -> None:
        self.sessions: list[_FakeSession] = []

    def __call__(self) -> _FakeSession:
        session = _FakeSession()
        self.sessions.append(session)
        return _SessionContext(session)


class _SessionContext:
    def __init__(self, session: _FakeSession) -> None:
        self._session = session

    def __enter__(self) -> _FakeSession:
        return self._session

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _StopLoopError(Exception):
    pass


def test_retry_delay_exponential_and_capped():
    assert worker._retry_delay_s(-5) == 1.0
    assert worker._retry_delay_s(0) == 1.0
    assert worker._retry_delay_s(1) == 2.0
    assert worker._retry_delay_s(2) == 4.0
    assert worker._retry_delay_s(31) == 1800.0
    assert worker._retry_delay_s(999) == 1800.0


def test_run_dispatch_loop_marks_delivered_on_success(monkeypatch: pytest.MonkeyPatch):
    session_factory = _SessionFactory()
    event = _FakeEvent(id=7, destination="atlas", attempt_count=0)
    claimed_returns = [[event], []]
    claim_calls: list[tuple[int, int]] = []
    received_ids: list[int] = []
    failed_calls: list[dict] = []
    delivered_events: list[int] = []
    sleep_calls: list[float] = []
    attempt_calls: list[dict] = []

    def _claim_events(session, batch_size: int, lock_timeout_s: int, **kwargs):
        claim_calls.append((batch_size, lock_timeout_s))
        return claimed_returns.pop(0)

    def _mark_received(_session, event_id: int, **kwargs):
        received_ids.append(event_id)

    def _mark_failed(_session, event_id: int, *, error: str, next_attempt_at: datetime):
        failed_calls.append(
            {"event_id": event_id, "error": error, "next_attempt_at": next_attempt_at}
        )

    def _record_attempt(_session, **kwargs):
        attempt_calls.append(kwargs)
        return 1

    def _deliver_fn(ev: _FakeEvent):
        delivered_events.append(ev.id)
        return DeliveryResult.received(uuid.uuid4())

    def _sleep(seconds: float):
        sleep_calls.append(seconds)
        raise _StopLoopError()

    monkeypatch.setattr(worker, "claim_events", _claim_events)
    monkeypatch.setattr(worker, "mark_received", _mark_received)
    monkeypatch.setattr(worker, "mark_failed", _mark_failed)
    monkeypatch.setattr(worker, "record_attempt", _record_attempt)
    monkeypatch.setattr(worker.time, "sleep", _sleep)

    with pytest.raises(_StopLoopError):
        worker.run_dispatch_loop(
            session_factory=session_factory,
            deliver_fn=_deliver_fn,
            batch_size=3,
            poll_interval_s=0.25,
            lock_timeout_s=19,
        )

    assert claim_calls == [(3, 19), (3, 19)]
    assert delivered_events == [7]
    assert received_ids == [7]
    assert failed_calls == []
    assert sleep_calls == [0.25]
    assert len(attempt_calls) == 1
    # Worker must expunge both the message and the event itself
    assert any(
        event.message in session.expunge_calls and event in session.expunge_calls
        for session in session_factory.sessions
    )


def test_run_dispatch_loop_marks_failed_and_schedules_retry(
    monkeypatch: pytest.MonkeyPatch,
):
    session_factory = _SessionFactory()
    event = _FakeEvent(id=9, destination="atlas", attempt_count=2)
    claimed_returns = [[event], []]
    failed_calls: list[dict] = []
    received_ids: list[int] = []
    fixed_now = datetime(2026, 3, 29, 8, 30, 0, tzinfo=UTC)

    class _FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now if tz is not None else fixed_now.replace(tzinfo=None)

    def _claim_events(_session, batch_size: int, lock_timeout_s: int, **kwargs):
        _ = (batch_size, lock_timeout_s)
        return claimed_returns.pop(0)

    def _mark_failed(_session, event_id: int, *, error: str, next_attempt_at: datetime):
        failed_calls.append(
            {"event_id": event_id, "error": error, "next_attempt_at": next_attempt_at}
        )

    def _mark_received(_session, event_id: int, **kwargs):
        received_ids.append(event_id)

    def _record_attempt(_session, **kwargs):
        return 1

    def _deliver_fn(_ev: _FakeEvent):
        raise RuntimeError("network is down")

    def _sleep(_seconds: float):
        raise _StopLoopError()

    monkeypatch.setattr(worker, "datetime", _FixedDateTime)
    monkeypatch.setattr(worker, "claim_events", _claim_events)
    monkeypatch.setattr(worker, "mark_failed", _mark_failed)
    monkeypatch.setattr(worker, "mark_received", _mark_received)
    monkeypatch.setattr(worker, "record_attempt", _record_attempt)
    monkeypatch.setattr(worker.time, "sleep", _sleep)

    with pytest.raises(_StopLoopError):
        worker.run_dispatch_loop(
            session_factory=session_factory,
            deliver_fn=_deliver_fn,
            max_attempts=10,
            poll_interval_s=0.1,
        )

    assert received_ids == []
    assert len(failed_calls) == 1
    assert failed_calls[0]["event_id"] == 9
    assert "network is down" in failed_calls[0]["error"]
    assert failed_calls[0]["next_attempt_at"] == fixed_now + timedelta(seconds=4)


def test_run_dispatch_loop_max_attempts_dead_letters(
    monkeypatch: pytest.MonkeyPatch,
):
    session_factory = _SessionFactory()
    event = _FakeEvent(id=11, destination="atlas", attempt_count=10)
    claimed_returns = [[event], []]
    dead_letter_calls: list[dict] = []

    def _claim_events(_session, batch_size: int, lock_timeout_s: int, **kwargs):
        _ = (batch_size, lock_timeout_s)
        return claimed_returns.pop(0)

    def _mark_dead_letter(_session, event_id: int, *, error: str = ""):
        dead_letter_calls.append({"event_id": event_id, "error": error})

    def _record_attempt(_session, **kwargs):
        return 1

    def _deliver_fn(_ev: _FakeEvent):
        raise RuntimeError("retry exhausted")

    def _sleep(_seconds: float):
        raise _StopLoopError()

    monkeypatch.setattr(worker, "claim_events", _claim_events)
    monkeypatch.setattr(worker, "mark_dead_letter", _mark_dead_letter)
    monkeypatch.setattr(worker, "record_attempt", _record_attempt)
    monkeypatch.setattr(worker.time, "sleep", _sleep)

    with pytest.raises(_StopLoopError):
        worker.run_dispatch_loop(
            session_factory=session_factory,
            deliver_fn=_deliver_fn,
            max_attempts=10,
            poll_interval_s=0.1,
        )

    assert len(dead_letter_calls) == 1
    assert dead_letter_calls[0]["event_id"] == 11
