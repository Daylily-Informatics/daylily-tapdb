from __future__ import annotations

from types import SimpleNamespace

import pytest

import daylily_tapdb.actions.dispatcher as m


class _FakeSession:
    def __init__(self) -> None:
        self.added: list[object] = []
        self.flushed = False

    def add(self, obj: object) -> None:
        self.added.append(obj)

    def flush(self) -> None:
        self.flushed = True


class _TestDispatcher(m.ActionDispatcher):
    def do_action_ok(self, instance, action_ds, captured_data):
        _ = (instance, action_ds)
        return {"status": "success", "echo": captured_data.get("value")}

    def do_action_boom(self, instance, action_ds, captured_data):
        _ = (instance, action_ds, captured_data)
        raise RuntimeError("boom")


def _instance_with_action(action_key: str = "ok") -> SimpleNamespace:
    return SimpleNamespace(
        uid=101,
        euid="GX-101",
        json_addl={
            "action_groups": {
                "core_actions": {
                    action_key: {
                        "action_executed": "0",
                        "executed_datetime": [],
                    }
                }
            }
        },
    )


def test_execute_action_returns_error_when_handler_missing():
    dispatcher = _TestDispatcher()
    session = _FakeSession()
    instance = _instance_with_action("missing")

    result = dispatcher.execute_action(
        session=session,
        instance=instance,
        action_group="core_actions",
        action_key="missing",
        action_ds={"action_template_uid": 42},
    )

    assert result["status"] == "error"
    assert "No handler for action: missing" in result["message"]
    assert (
        instance.json_addl["action_groups"]["core_actions"]["missing"][
            "action_executed"
        ]
        == "0"
    )
    assert session.added == []


def test_execute_action_success_updates_tracking_and_creates_record(
    monkeypatch: pytest.MonkeyPatch,
):
    dispatcher = _TestDispatcher()
    session = _FakeSession()
    instance = _instance_with_action("ok")
    calls: dict[str, object] = {}
    flag_calls: list[tuple[object, str]] = []

    def _fake_create_action_record(
        _session,
        _instance,
        action_group,
        action_key,
        action_ds,
        captured_data,
        result,
        user,
    ):
        calls["payload"] = {
            "group": action_group,
            "key": action_key,
            "action_ds": action_ds,
            "captured_data": captured_data,
            "result": result,
            "user": user,
        }

    monkeypatch.setattr(dispatcher, "_create_action_record", _fake_create_action_record)
    monkeypatch.setattr(
        m, "flag_modified", lambda obj, field: flag_calls.append((obj, field))
    )

    result = dispatcher.execute_action(
        session=session,
        instance=instance,
        action_group="core_actions",
        action_key="ok",
        action_ds={"action_template_uid": 42},
        captured_data={"value": 7},
        create_action_record=True,
        user="admin",
    )

    assert result == {"status": "success", "echo": 7}
    action_def = instance.json_addl["action_groups"]["core_actions"]["ok"]
    assert action_def["action_executed"] == "1"
    assert len(action_def["executed_datetime"]) == 1
    assert flag_calls == [(instance, "json_addl")]
    assert calls["payload"] == {
        "group": "core_actions",
        "key": "ok",
        "action_ds": {"action_template_uid": 42},
        "captured_data": {"value": 7},
        "result": {"status": "success", "echo": 7},
        "user": "admin",
    }


def test_execute_action_error_updates_tracking_and_skips_record(
    monkeypatch: pytest.MonkeyPatch,
):
    dispatcher = _TestDispatcher()
    session = _FakeSession()
    instance = _instance_with_action("boom")
    created = {"called": False}
    flag_calls: list[tuple[object, str]] = []

    monkeypatch.setattr(
        dispatcher,
        "_create_action_record",
        lambda *_args, **_kwargs: created.__setitem__("called", True),
    )
    monkeypatch.setattr(
        m, "flag_modified", lambda obj, field: flag_calls.append((obj, field))
    )

    result = dispatcher.execute_action(
        session=session,
        instance=instance,
        action_group="core_actions",
        action_key="boom",
        action_ds={"action_template_uid": 42},
        captured_data={"value": 1},
    )

    assert result["status"] == "error"
    assert "boom" in result["message"]
    action_def = instance.json_addl["action_groups"]["core_actions"]["boom"]
    assert action_def["action_executed"] == "1"
    assert len(action_def["executed_datetime"]) == 1
    assert created["called"] is False
    assert flag_calls == [(instance, "json_addl")]


def test_update_action_tracking_noop_for_missing_group_or_key(
    monkeypatch: pytest.MonkeyPatch,
):
    dispatcher = _TestDispatcher()
    instance = SimpleNamespace(json_addl={"action_groups": {}})
    flag_calls: list[tuple[object, str]] = []
    monkeypatch.setattr(
        m, "flag_modified", lambda obj, field: flag_calls.append((obj, field))
    )

    dispatcher._update_action_tracking(
        instance=instance,
        action_group="core_actions",
        action_key="ok",
        result={"status": "success"},
    )

    assert flag_calls == []


def test_create_action_record_requires_action_template_uid():
    dispatcher = _TestDispatcher()
    session = _FakeSession()
    instance = _instance_with_action("ok")

    with pytest.raises(ValueError, match="action_template_uid"):
        dispatcher._create_action_record(
            session=session,
            instance=instance,
            action_group="core_actions",
            action_key="ok",
            action_ds={},
            captured_data={},
            result={"status": "success"},
            user="admin",
        )


def test_create_action_record_adds_action_instance(monkeypatch: pytest.MonkeyPatch):
    dispatcher = _TestDispatcher()
    session = _FakeSession()
    instance = _instance_with_action("ok")

    monkeypatch.setattr(
        m,
        "action_instance",
        lambda **kwargs: SimpleNamespace(**kwargs),
    )

    dispatcher._create_action_record(
        session=session,
        instance=instance,
        action_group="core_actions",
        action_key="ok",
        action_ds={"action_template_uid": "77"},
        captured_data={"v": 1},
        result={"status": "success"},
        user="admin",
    )

    assert len(session.added) == 1
    record = session.added[0]
    assert record.name == "ok@GX-101"
    assert record.template_uid == 77
    assert record.subtype == "ok"
    assert record.json_addl["target_instance_uid"] == 101
    assert record.json_addl["action_group"] == "core_actions"
    assert record.json_addl["captured_data"] == {"v": 1}
    assert record.json_addl["result"] == {"status": "success"}
    assert record.json_addl["executed_by"] == "admin"
    assert session.flushed is True
