from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest

from daylily_tapdb.outbox import worker


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

    def expunge(self, obj: object) -> None:
        self.expunge_calls.append(obj)

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
    delivered_ids: list[int] = []
    failed_calls: list[dict] = []
    delivered_events: list[int] = []
    sleep_calls: list[float] = []

    def _claim_events(session, batch_size: int, lock_timeout_s: int, **kwargs):
        claim_calls.append((batch_size, lock_timeout_s))
        return claimed_returns.pop(0)

    def _mark_delivered(_session, event_id: int):
        delivered_ids.append(event_id)

    def _mark_failed(_session, event_id: int, *, error: str, next_attempt_at: datetime):
        failed_calls.append(
            {"event_id": event_id, "error": error, "next_attempt_at": next_attempt_at}
        )

    def _deliver_fn(ev: _FakeEvent):
        delivered_events.append(ev.id)

    def _sleep(seconds: float):
        sleep_calls.append(seconds)
        raise _StopLoopError()

    monkeypatch.setattr(worker, "claim_events", _claim_events)
    monkeypatch.setattr(worker, "mark_delivered", _mark_delivered)
    monkeypatch.setattr(worker, "mark_failed", _mark_failed)
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
    assert delivered_ids == [7]
    assert failed_calls == []
    assert sleep_calls == [0.25]
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
    delivered_ids: list[int] = []
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

    def _mark_delivered(_session, event_id: int):
        delivered_ids.append(event_id)

    def _deliver_fn(_ev: _FakeEvent):
        raise RuntimeError("network is down")

    def _sleep(_seconds: float):
        raise _StopLoopError()

    monkeypatch.setattr(worker, "datetime", _FixedDateTime)
    monkeypatch.setattr(worker, "claim_events", _claim_events)
    monkeypatch.setattr(worker, "mark_failed", _mark_failed)
    monkeypatch.setattr(worker, "mark_delivered", _mark_delivered)
    monkeypatch.setattr(worker.time, "sleep", _sleep)

    with pytest.raises(_StopLoopError):
        worker.run_dispatch_loop(
            session_factory=session_factory,
            deliver_fn=_deliver_fn,
            max_attempts=10,
            poll_interval_s=0.1,
        )

    assert delivered_ids == []
    assert len(failed_calls) == 1
    assert failed_calls[0]["event_id"] == 9
    assert "network is down" in failed_calls[0]["error"]
    assert failed_calls[0]["next_attempt_at"] == fixed_now + timedelta(seconds=4)


def test_run_dispatch_loop_max_attempts_schedules_far_future_retry(
    monkeypatch: pytest.MonkeyPatch,
):
    session_factory = _SessionFactory()
    event = _FakeEvent(id=11, destination="atlas", attempt_count=10)
    claimed_returns = [[event], []]
    failed_calls: list[dict] = []
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

    def _deliver_fn(_ev: _FakeEvent):
        raise RuntimeError("retry exhausted")

    def _sleep(_seconds: float):
        raise _StopLoopError()

    monkeypatch.setattr(worker, "datetime", _FixedDateTime)
    monkeypatch.setattr(worker, "claim_events", _claim_events)
    monkeypatch.setattr(worker, "mark_failed", _mark_failed)
    monkeypatch.setattr(worker.time, "sleep", _sleep)

    with pytest.raises(_StopLoopError):
        worker.run_dispatch_loop(
            session_factory=session_factory,
            deliver_fn=_deliver_fn,
            max_attempts=10,
            poll_interval_s=0.1,
        )

    assert len(failed_calls) == 1
    assert failed_calls[0]["event_id"] == 11
    assert failed_calls[0]["next_attempt_at"] == fixed_now + timedelta(days=365)
