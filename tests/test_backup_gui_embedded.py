"""Embedded GUI backup pages.

Two properties matter beyond "the page renders": the pages are admin-only, and
the restore form cannot become a way to apply something the operator was not
shown. The staged fingerprint and the typed label are checked server-side, so
the disabled button is a courtesy rather than the control.

Follows ``test_gui_embedded.py``: a host bridge supplies the user, and the
backup layer is faked so these stay fast and DB-free. The real service is
covered end to end elsewhere.
"""

from __future__ import annotations

import re
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from daylily_tapdb.backup.receipts import Actor, ChainVerification, Receipt
from daylily_tapdb.backup.service import (
    BackupResult,
    BackupSummary,
    CheckResult,
    VerifyReport,
)
from daylily_tapdb.backup.verify import (
    RehearsalEvidence,
    RestorePlan,
    RestoreResult,
)
from daylily_tapdb.gui import create_tapdb_gui_app
from daylily_tapdb.web.bridge import TapdbHostBridge

LABEL = "testclient/testdb/tapdb_prod@tapdb_shared"
BACKUP_ID = "full-abc123"

# ---------------------------------------------------------------------------
# Fake payloads, DERIVED from the real dataclasses rather than hand-written.
#
# These were literal dictionaries, and they drifted from reality four separate
# times: a template would read a field the fake had never carried, and the page
# blew up with an undefined-attribute error that said nothing about the cause.
# Each time the fix was to add the missing key and then a test comparing keys
# -- which only ever covered the level that had just broken. The next drift was
# always one level further down.
#
# Building them through `to_payload()` removes the failure mode instead of
# guarding it: the shape *is* the production shape, so a field added to
# `Receipt` or `BackupSummary` appears here automatically. Only the outer dicts
# that `views` assembles are still written by hand, and
# `test_backup_gui_live.py::test_embedded_test_fakes_match_real_view_output`
# pins those against real `views` output.
# ---------------------------------------------------------------------------

STATUS = {
    "target_label": LABEL,
    "status": "ok",
    "cadence": {
        "configured": True,
        "expected_interval_hours": 24.0,
        "next_due_at": "2026-07-30T12:00:00+00:00",
    },
    "last_success_at": "2026-07-29T12:00:00+00:00",
    "last_success_backup_id": BACKUP_ID,
    "last_attempt_at": "2026-07-29T12:00:00+00:00",
    "last_attempt_status": "succeeded",
    "age_hours": 2.0,
    "receipt_count": 3,
    "receipt_chain": ChainVerification(ok=True, count=3).to_payload(),
    "recent_receipts": [
        Receipt(
            sequence=1,
            receipt_id="000001-20260729T120000Z",
            created_at="2026-07-29T12:00:00+00:00",
            operation="backup_create",
            status="succeeded",
            actor=Actor(surface="gui", username="admin@example.com"),
            backup_id=BACKUP_ID,
            backup_class="full",
            target_label=LABEL,
            detail={"checks": [{"id": "asset.checksum", "status": "pass"}]},
        ).to_payload()
    ],
    "storage": {"backend": "local", "uri": "file:///tmp/store"},
}

INVENTORY = {
    "status": STATUS,
    "count": 1,
    "storage": STATUS["storage"],
    "damaged": [],
    "backups": [
        BackupSummary(
            backup_id=BACKUP_ID,
            backup_class="full",
            created_at="2026-07-29T12:00:00+00:00",
            status="complete",
            storage_prefix="c/d/full/full-abc123",
            target_label=LABEL,
            row_totals=42,
            bytes=1024,
        ).to_payload()
    ],
}

REVIEW = {
    **RestorePlan(
        backup_id=BACKUP_ID,
        mode="in_place",
        target_database="tapdb_shared",
        target_schema="tapdb_prod",
        source_label=LABEL,
        required_confirm_target=LABEL,
        plan_fingerprint="fingerprint-aaa",
        confirmation_required=True,
        steps=["create a safety backup", "rename the schema aside", "restore"],
        checks=[
            CheckResult(id="archive.deep_read", status="pass", detail="reads"),
        ],
    ).to_payload(),
    # The two keys `restore_review_context` adds on top of the plan.
    "blocking": [],
    "target_label": LABEL,
}


@pytest.fixture
def gui(monkeypatch):
    """Build an embedded GUI with the backup layer faked out."""
    calls: dict = {"applied": [], "created": [], "verified": [], "rehearsed": []}

    monkeypatch.setattr(
        "daylily_tapdb.gui.router.get_db_config",
        lambda config_path: {
            "client_id": "testclient",
            "domain_code": "Z",
            "owner_repo_name": "daylily-tapdb",
            "domain_registry_path": "daylily_tapdb/etc/domain_code_registry.json",
            "prefix_ownership_registry_path": (
                "daylily_tapdb/etc/prefix_ownership_registry.json"
            ),
        },
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.get_admin_settings",
        lambda **_kwargs: {
            "target_name": "test",
            "production_like": False,
            "auth_mode": "host_session",
            "session_secret": "test-session-secret",
            "allowed_origins": [],
        },
    )
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.mount_tapdb_dag_surfaces",
        lambda *_args, **_kwargs: SimpleNamespace(mounted=True, diagnostic=""),
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_backup_settings",
        lambda **kwargs: {"config_dir": "/tmp", "storage_uri": ""},
    )

    import daylily_tapdb.backup.service as service_mod
    import daylily_tapdb.backup.verify as verify_mod
    import daylily_tapdb.backup.views as views_mod

    monkeypatch.setattr(views_mod, "inventory_context", lambda *a, **k: INVENTORY)
    monkeypatch.setattr(views_mod, "status_context", lambda *a, **k: STATUS)
    monkeypatch.setattr(
        views_mod, "restore_review_context", lambda *a, **k: dict(REVIEW)
    )

    def _apply(cfg, settings, **kwargs):
        calls["applied"].append(kwargs)
        from daylily_tapdb.backup.errors import (
            RestoreConfirmationError,
            RestoreStageStaleError,
        )

        if kwargs.get("plan_fingerprint") != REVIEW["plan_fingerprint"]:
            raise RestoreStageStaleError("stale stage")
        if kwargs.get("confirm_target") != LABEL:
            raise RestoreConfirmationError("wrong label")

        return RestoreResult(
            backup_id=BACKUP_ID,
            mode="in_place",
            target_database="tapdb_shared",
            target_schema="tapdb_prod",
            receipt_id="000003-fake",
        )

    monkeypatch.setattr(views_mod, "apply_restore_from_review", _apply)

    def _create(cfg, settings, **kwargs):
        calls["created"].append(kwargs)
        return BackupResult(
            backup_id=BACKUP_ID,
            backup_class="full",
            storage_prefix="c/d/full/full-abc123",
            receipt_id="000004-fake",
        )

    monkeypatch.setattr(service_mod, "create_backup", _create)

    def _verify(cfg, settings, **kwargs):
        # A real VerifyReport, not a stand-in with the two attributes the
        # route happens to read today. The stand-in version broke the moment
        # `receipt_id` was added, with an AttributeError that named the fake
        # rather than the change.
        calls["verified"].append(kwargs)
        return VerifyReport(backup_id=BACKUP_ID, level="deep", receipt_id="000001-fake")

    monkeypatch.setattr(service_mod, "verify_backup", _verify)

    def _rehearse(cfg, settings, **kwargs):
        calls["rehearsed"].append(kwargs)
        return RehearsalEvidence(
            backup_id=BACKUP_ID,
            rehearsal_id="20260729T120000Z",
            started_at="2026-07-29T12:00:00+00:00",
            finished_at="2026-07-29T12:01:00+00:00",
            database="tapdb_rehearsal_20260729T120000Z",
            schema="tapdb_prod",
            receipt_id="000002-fake",
        )

    monkeypatch.setattr(verify_mod, "rehearse_restore", _rehearse)

    def _build(role="admin"):
        bridge = TapdbHostBridge(
            auth_mode="host_session",
            login_url="/login",
            resolve_user=lambda _request: {
                "username": f"{role}@example.com",
                "email": f"{role}@example.com",
                "role": role,
            },
        )
        return TestClient(
            create_tapdb_gui_app(
                config_path="/tmp/tapdb-config.yaml", host_bridge=bridge
            ),
            base_url="https://localhost",
        )

    return _build, calls


# ---------------------------------------------------------------------------
# admin gating
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "method, path",
    [
        ("GET", "/admin/backups"),
        ("GET", "/api/admin/backups"),
        ("GET", "/api/admin/backups/status"),
        ("POST", "/admin/backups/create"),
        ("POST", "/admin/backups/full-abc123/verify"),
        ("POST", "/admin/backups/full-abc123/rehearse"),
        ("GET", "/admin/backups/full-abc123/restore"),
        ("GET", "/api/admin/backups/full-abc123/restore"),
        ("POST", "/admin/backups/full-abc123/restore"),
    ],
)
def test_a_non_admin_cannot_reach_any_backup_surface(gui, method, path):
    build, _calls = gui
    client = build(role="user")

    response = client.request(method, path, follow_redirects=False)

    assert response.status_code == 403, f"{method} {path} allowed a non-admin"


# ---------------------------------------------------------------------------
# status page
# ---------------------------------------------------------------------------


def test_the_backups_page_renders_the_status_banner(gui):
    build, _calls = gui

    response = build().get("/admin/backups")

    assert response.status_code == 200
    assert "Status: ok" in response.text
    assert LABEL in response.text
    assert "full-abc123" in response.text


def test_the_page_offers_verify_rehearse_and_stage_actions(gui):
    build, _calls = gui

    text = build().get("/admin/backups").text

    assert "Verify" in text
    assert "Rehearse" in text
    assert "Stage restore" in text


def test_recent_receipts_are_shown(gui):
    build, _calls = gui

    text = build().get("/admin/backups").text

    assert "backup_create" in text
    assert "admin@example.com" in text


def test_a_broken_receipt_chain_is_surfaced(gui, monkeypatch):
    import daylily_tapdb.backup.views as views_mod

    broken = {
        **INVENTORY,
        "status": {
            **STATUS,
            "receipt_chain": {"ok": False, "count": 2, "findings": ["tampered"]},
        },
    }
    monkeypatch.setattr(views_mod, "inventory_context", lambda *a, **k: broken)
    build, _calls = gui

    text = build().get("/admin/backups").text

    # A page that reads "healthy" while its audit trail is broken would be
    # worse than no page at all.
    assert "Receipt chain is broken" in text
    assert "tampered" in text


def test_a_never_run_target_says_so(gui, monkeypatch):
    import daylily_tapdb.backup.views as views_mod

    empty = {
        **INVENTORY,
        "count": 0,
        "backups": [],
        "status": {**STATUS, "status": "never_run"},
    }
    monkeypatch.setattr(views_mod, "inventory_context", lambda *a, **k: empty)
    build, _calls = gui

    text = build().get("/admin/backups").text

    assert "No successful backup has ever been recorded" in text


def test_damaged_backups_are_reported_on_the_page(gui, monkeypatch):
    import daylily_tapdb.backup.views as views_mod

    damaged = {**INVENTORY, "damaged": ["c/d/full/broken-1"]}
    monkeypatch.setattr(views_mod, "inventory_context", lambda *a, **k: damaged)
    build, _calls = gui

    text = build().get("/admin/backups").text

    assert "unreadable manifest" in text
    assert "broken-1" in text


def test_the_json_route_mirrors_the_page(gui):
    build, _calls = gui

    payload = build().get("/api/admin/backups").json()

    # Every page is paired with JSON so the GUI is self-sufficient when a host
    # embeds it without mounting the admin service.
    assert payload["count"] == 1
    assert payload["status"]["status"] == "ok"


def test_the_status_json_route_exists(gui):
    build, _calls = gui

    assert build().get("/api/admin/backups/status").json()["status"] == "ok"


def test_backups_appears_in_the_nav(gui):
    build, _calls = gui

    text = build().get("/admin/backups").text

    assert "/admin/backups" in text
    assert ">Backups<" in text or "Backups" in text


# ---------------------------------------------------------------------------
# review page
# ---------------------------------------------------------------------------


def test_the_review_page_shows_the_label_and_fingerprint(gui):
    build, _calls = gui

    text = build().get("/admin/backups/full-abc123/restore?mode=in-place").text

    assert LABEL in text
    assert "fingerprint-aaa" in text
    assert "create a safety backup" in text


def test_the_review_page_warns_about_in_place(gui):
    build, _calls = gui

    text = build().get("/admin/backups/full-abc123/restore?mode=in-place").text

    assert "replaces live data" in text


def test_the_review_page_is_read_only(gui):
    build, calls = gui

    build().get("/admin/backups/full-abc123/restore")

    # Staging must never apply anything.
    assert calls["applied"] == []


def test_a_blocked_plan_hides_the_apply_form(gui, monkeypatch):
    import daylily_tapdb.backup.views as views_mod

    blocked = {
        **REVIEW,
        "ok": False,
        "blocking": [
            {
                "id": "archive.deep_read",
                "status": "fail",
                "detail": "corrupt",
                "data": {},
            }
        ],
    }
    monkeypatch.setattr(views_mod, "restore_review_context", lambda *a, **k: blocked)
    build, _calls = gui

    text = build().get("/admin/backups/full-abc123/restore").text

    assert "Apply is unavailable" in text
    assert 'id="apply-form"' not in text


def test_a_malformed_reference_is_rejected(gui):
    build, _calls = gui

    response = build().get("/admin/backups/..%2F..%2Fetc/restore")

    assert response.status_code in (400, 404)


# ---------------------------------------------------------------------------
# apply
# ---------------------------------------------------------------------------


def test_a_correct_confirmation_applies_and_redirects(gui):
    build, calls = gui

    response = build().post(
        "/admin/backups/full-abc123/restore",
        data={
            "plan_fingerprint": "fingerprint-aaa",
            "confirm_target": LABEL,
            "mode": "in_place",
        },
        follow_redirects=False,
    )

    assert response.status_code == 303
    # Lands on the operation's evidence page, where the post-restore checks
    # are shown, rather than a bare notice that discards them.
    assert "/admin/backups/receipts/" in response.headers["location"]
    assert len(calls["applied"]) == 1


def test_a_wrong_label_re_renders_with_an_error(gui):
    build, calls = gui

    response = build().post(
        "/admin/backups/full-abc123/restore",
        data={
            "plan_fingerprint": "fingerprint-aaa",
            "confirm_target": "not-the-label",
            "mode": "in_place",
        },
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert "wrong label" in response.text
    assert calls["applied"], "the server must do the checking, not the browser"


def test_a_stale_fingerprint_re_renders_with_a_fresh_one(gui):
    build, _calls = gui

    response = build().post(
        "/admin/backups/full-abc123/restore",
        data={
            "plan_fingerprint": "an-old-fingerprint",
            "confirm_target": LABEL,
            "mode": "in_place",
        },
        follow_redirects=False,
    )

    assert response.status_code == 200
    assert "stale stage" in response.text
    # Handing back the stale fingerprint would let the operator retry straight
    # into the same refusal.
    assert "fingerprint-aaa" in response.text


def test_the_form_posts_through_the_shared_apply_view(gui):
    build, calls = gui

    build().post(
        "/admin/backups/full-abc123/restore",
        data={
            "plan_fingerprint": "fingerprint-aaa",
            "confirm_target": LABEL,
            "mode": "in_place",
        },
        follow_redirects=False,
    )

    # Plan section 4.2: the GUI POST and the admin API are literally the same
    # code path.
    assert calls["applied"][0]["backup_id"] == "full-abc123"
    assert calls["applied"][0]["confirm_target"] == LABEL


def test_the_disabled_button_is_not_the_control(gui):
    """The browser check is progressive enhancement only.

    A client that ignores the script -- curl, a script, a stale page -- still
    hits the server-side label check.
    """
    build, calls = gui
    text = build().get("/admin/backups/full-abc123/restore?mode=in-place").text

    button = re.search(r"<button[^>]*id=\"apply-button\"[^>]*>", text)
    assert button is not None, "apply button missing entirely"
    assert "disabled" in button.group(0), button.group(0)

    response = build().post(
        "/admin/backups/full-abc123/restore",
        data={
            "plan_fingerprint": "fingerprint-aaa",
            "confirm_target": "",
            "mode": "in_place",
        },
        follow_redirects=False,
    )
    assert response.status_code == 200
    assert "restore_applied" not in response.text


# ---------------------------------------------------------------------------
# create / verify / rehearse actions
# ---------------------------------------------------------------------------


def test_create_posts_and_redirects(gui):
    build, calls = gui

    response = build().post(
        "/admin/backups/create",
        data={"backup_class": "full", "note": "from the gui"},
        follow_redirects=False,
    )

    assert response.status_code == 303
    # Lands on the evidence page: a create runs its own quick verification and
    # those verdicts are shown rather than discarded.
    assert "/admin/backups/receipts/" in response.headers["location"]
    assert calls["created"][0]["note"] == "from the gui"


def test_create_records_the_gui_surface(gui):
    build, calls = gui

    build().post("/admin/backups/create", data={}, follow_redirects=False)

    actor = calls["created"][0]["actor"]
    assert actor.surface == "gui"
    assert actor.username == "admin@example.com"


def test_verify_posts_and_redirects(gui):
    build, calls = gui

    response = build().post("/admin/backups/full-abc123/verify", follow_redirects=False)

    assert response.status_code == 303
    assert calls["verified"][0]["backup_id"] == "full-abc123"


def test_rehearse_posts_and_redirects(gui):
    build, calls = gui

    response = build().post(
        "/admin/backups/full-abc123/rehearse", follow_redirects=False
    )

    assert response.status_code == 303
    assert calls["rehearsed"][0]["backup_id"] == "full-abc123"


def test_a_failing_action_redirects_with_an_error(gui, monkeypatch):
    import daylily_tapdb.backup.service as service_mod

    def _boom(*args, **kwargs):
        raise RuntimeError("storage unavailable")

    monkeypatch.setattr(service_mod, "create_backup", _boom)
    build, _calls = gui

    response = build().post("/admin/backups/create", data={}, follow_redirects=False)

    assert response.status_code == 303
    assert "error=" in response.headers["location"]
