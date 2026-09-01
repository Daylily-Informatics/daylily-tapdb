"""Embedded GUI backup pages, rendered against a real database.

``test_backup_gui_embedded.py`` fakes the backup layer so the gating and form
mechanics stay fast. That leaves one thing unproven: the templates are only
ever handed hand-written dictionaries, so a template could reference a field
the real ``views`` functions never produce and every test would still pass.

These tests close that gap by driving the actual routes against a real backup
in the ephemeral test cluster -- real ``views`` output, real Jinja rendering,
real HTTP. They are slower and DB-bound, which is why there are only a few:
enough to prove the contexts and templates agree.
"""

from __future__ import annotations

import re
import shutil
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text
from typer.testing import CliRunner

import daylily_tapdb.cli as cli_mod
from daylily_tapdb.backup import service
from daylily_tapdb.cli import framework_app
from daylily_tapdb.cli.context import clear_cli_context, set_cli_context
from daylily_tapdb.cli.db_config import get_backup_settings, get_db_config
from daylily_tapdb.gui import create_tapdb_gui_app
from daylily_tapdb.web.bridge import TapdbHostBridge

runner = CliRunner()

pytestmark = pytest.mark.skipif(
    not shutil.which("pg_dump") or not shutil.which("pg_restore"),
    reason="pg_dump/pg_restore not on PATH",
)


@pytest.fixture(autouse=True)
def _context(pg_instance, monkeypatch):
    monkeypatch.setenv("HOME", str(pg_instance["base"]))
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        config_path=pg_instance["config_path"],
    )
    monkeypatch.setattr(cli_mod, "PID_FILE", pg_instance["base"] / "ui.pid")
    monkeypatch.setattr(cli_mod, "LOG_FILE", pg_instance["base"] / "ui.log")
    yield
    clear_cli_context()


@pytest.fixture(scope="module")
def _schema_applied(pg_instance):
    clear_cli_context()
    set_cli_context(
        client_id="testclient",
        database_name="testdb",
        config_path=pg_instance["config_path"],
    )
    config = str(pg_instance["config_path"])
    applied = runner.invoke(
        framework_app, ["--config", config, "db", "schema", "apply"]
    )
    assert applied.exit_code == 0, applied.output
    seeded = runner.invoke(
        framework_app, ["--config", config, "db", "data", "seed", "--skip-existing"]
    )
    assert seeded.exit_code == 0, seeded.output
    return True


@pytest.fixture
def live(pg_instance, _schema_applied, tmp_path, monkeypatch):
    """A GUI client wired to the real backup layer and a real backup."""
    cfg = dict(get_db_config())
    settings = dict(get_backup_settings())
    settings["config_dir"] = str(tmp_path)
    settings["storage_uri"] = f"file://{tmp_path / 'store'}"

    # The router resolves these per request; pin them at the real values so the
    # routes exercise the genuine service rather than a fake.
    monkeypatch.setattr(
        "daylily_tapdb.gui.router.get_db_config",
        lambda config_path=None: dict(cfg),
    )
    monkeypatch.setattr(
        "daylily_tapdb.cli.db_config.get_backup_settings",
        lambda **kwargs: dict(settings),
    )

    created = service.create_backup(cfg, settings)

    bridge = TapdbHostBridge(
        auth_mode="host_session",
        login_url="/login",
        resolve_user=lambda _request: {
            "username": "admin@example.com",
            "email": "admin@example.com",
            "role": "admin",
        },
    )
    client = TestClient(
        create_tapdb_gui_app(
            config_path=str(pg_instance["config_path"]), host_bridge=bridge
        ),
        base_url="https://localhost",
    )
    return client, created, cfg, settings


def test_embedded_test_fakes_match_real_view_output(live):
    """The fast GUI tests' hand-written payloads must match reality.

    ``test_backup_gui_embedded.py`` fakes ``views`` to stay DB-free, which is
    the right trade -- but a fake that drifts turns those tests into an
    assertion about a fiction. This pins them to the real shapes, so adding a
    field to a context fails *here* with a readable diff rather than as a
    baffling template failure.
    """
    import tests.test_backup_gui_embedded as fakes
    from daylily_tapdb.backup import verify, views

    _client, created, cfg, settings = live

    real = {
        "STATUS": (views.status_context(cfg, settings), fakes.STATUS),
        "INVENTORY": (views.inventory_context(cfg, settings), fakes.INVENTORY),
        "REVIEW": (
            views.restore_review_context(
                cfg,
                settings,
                backup_id=created.backup_id,
                options=verify.RestoreOptions(mode="in-place"),
            ),
            fakes.REVIEW,
        ),
    }
    for name, (actual, fake) in real.items():
        assert set(fake) == set(actual), (
            f"{name} fake has drifted: "
            f"missing={sorted(set(actual) - set(fake))} "
            f"invented={sorted(set(fake) - set(actual))}"
        )

    assert set(fakes.STATUS["cadence"]) == set(real["STATUS"][0]["cadence"])
    assert set(fakes.INVENTORY["backups"][0]) == set(real["INVENTORY"][0]["backups"][0])

    # Nested list entries too. Checking only the top level let the fake
    # `recent_receipts` rows keep 5 of the real 12 keys, and the templates blew
    # up with an undefined-attribute error the moment one of the missing seven
    # was rendered. Every drift in this file so far has been one level down
    # from whatever was being compared.
    real_receipts = real["STATUS"][0]["recent_receipts"]
    fake_receipts = fakes.STATUS["recent_receipts"]
    assert real_receipts and fake_receipts, "no receipts to compare"
    assert set(fake_receipts[0]) == set(real_receipts[0]), (
        "recent_receipts fake has drifted: "
        f"missing={sorted(set(real_receipts[0]) - set(fake_receipts[0]))} "
        f"invented={sorted(set(fake_receipts[0]) - set(real_receipts[0]))}"
    )


def test_backups_page_renders_against_real_inventory(live):
    """The real inventory context satisfies every field backups.html reads."""
    client, created, _cfg, _settings = live
    response = client.get("/admin/backups")
    assert response.status_code == 200, response.text
    # Proves the page rendered actual data, not an empty table.
    assert created.backup_id in response.text


def test_restore_review_renders_against_real_plan(live):
    """The real plan context satisfies every field restore_review.html reads."""
    client, created, cfg, _settings = live
    response = client.get(f"/admin/backups/{created.backup_id}/restore")
    assert response.status_code == 200, response.text
    assert created.backup_id in response.text
    # The fingerprint the form will post back has to be on the page; without it
    # the apply step can only ever fail as stale.
    assert 'name="plan_fingerprint"' in response.text
    assert 'value=""' not in response.text.split('name="plan_fingerprint"')[1][:40]


def test_in_place_review_renders_the_destructive_warning(live):
    """The in-place branch of the template is reachable with a real payload.

    The template keys the warning off ``mode == "in_place"`` while the URL
    carries ``in-place``. If normalisation ever stopped happening, the page
    would silently render the reassuring "live data is not touched" copy for a
    destructive restore -- the single worst wrong string on this page.
    """
    client, created, _cfg, _settings = live
    response = client.get(
        f"/admin/backups/{created.backup_id}/restore", params={"mode": "in-place"}
    )
    assert response.status_code == 200, response.text
    assert "replaces live data" in response.text
    assert "Live data is not touched" not in response.text


def test_apply_with_real_fingerprint_and_label_restores(live):
    """The full GUI path: review, then post that review back, and it applies."""
    client, created, cfg, settings = live

    review = client.get(f"/api/admin/backups/{created.backup_id}/restore")
    assert review.status_code == 200, review.text
    payload = review.json()

    response = client.post(
        f"/admin/backups/{created.backup_id}/restore",
        data={
            "plan_fingerprint": payload["plan_fingerprint"],
            "confirm_target": payload["required_confirm_target"],
            "mode": payload["mode"],
        },
        follow_redirects=False,
    )
    assert response.status_code == 303, response.text[:2000]
    # Lands on the operation's evidence page rather than a bare notice, so the
    # post-restore checks are shown instead of discarded.
    assert "/admin/backups/receipts/" in response.headers["location"]

    # A restore that ran leaves a receipt; a restore that silently no-opped
    # would not.
    status = client.get("/api/admin/backups/status").json()
    operations = [r["operation"] for r in status["recent_receipts"]]
    assert "backup_restore" in operations, operations


def test_isolated_review_does_not_ask_for_a_label_it_will_not_check(live):
    """The page must not assert a control the service does not enforce.

    ``confirmation_required`` is false for an isolated restore, because
    ``restore_backup`` only checks the typed label in-place. Rendering the
    input anyway told the operator their typing authorised the restore when
    the server would have accepted any value.
    """
    client, created, _cfg, _settings = live

    review = client.get(f"/api/admin/backups/{created.backup_id}/restore").json()
    assert review["mode"] == "isolated"
    assert review["confirmation_required"] is False

    page = client.get(f"/admin/backups/{created.backup_id}/restore")
    assert 'name="confirm_target"' not in page.text
    assert "no typed confirmation is" in page.text

    # And the apply genuinely works without one -- the field is not merely
    # hidden while remaining mandatory underneath.
    applied = client.post(
        f"/admin/backups/{created.backup_id}/restore",
        data={"plan_fingerprint": review["plan_fingerprint"], "mode": "isolated"},
        follow_redirects=False,
    )
    assert applied.status_code == 303, applied.text[:2000]


def test_in_place_review_still_demands_the_typed_label(live):
    """The control the service *does* enforce is still rendered and enforced."""
    client, created, _cfg, _settings = live

    review = client.get(
        f"/api/admin/backups/{created.backup_id}/restore", params={"mode": "in-place"}
    ).json()
    assert review["confirmation_required"] is True

    page = client.get(
        f"/admin/backups/{created.backup_id}/restore", params={"mode": "in-place"}
    )
    assert 'name="confirm_target"' in page.text

    response = client.post(
        f"/admin/backups/{created.backup_id}/restore",
        data={
            "plan_fingerprint": review["plan_fingerprint"],
            "confirm_target": "not-the-label",
            "mode": "in_place",
        },
        follow_redirects=False,
    )
    # Re-rendered rather than redirected: the operator stays on the form.
    assert response.status_code == 200, response.status_code
    assert 'name="plan_fingerprint"' in response.text
    assert 'name="confirm_target"' in response.text


def test_in_place_is_reachable_without_hand_editing_the_url(live):
    """The destructive mode needs a real control, not a query string.

    `backups.html` links only to the default (isolated) review, and the review
    page carried `mode` in a hidden input. In-place restore was therefore
    reachable only by typing `?mode=in-place` into the address bar -- the plan
    (section 4.3) specified a mode select and it was never built.

    Every GUI test navigated by URL, so none of them noticed.
    """
    client, created, _cfg, _settings = live

    page = client.get(f"/admin/backups/{created.backup_id}/restore")

    assert 'name="mode"' in page.text, "no mode control on the review page"
    assert 'value="in-place"' in page.text, (
        "the review page offers no way to select an in-place restore"
    )


def test_the_mode_control_restages_rather_than_editing_the_apply_form(live):
    """Switching mode must re-run preflight, not flip a field.

    The plan fingerprint covers the mode. A mode changed inside the apply form
    would either be refused as stale or -- worse -- apply an operation whose
    warnings and step list the operator was never shown. The control is a GET
    form for that reason, and this pins it.
    """
    client, created, _cfg, _settings = live

    page = client.get(f"/admin/backups/{created.backup_id}/restore")
    forms = re.findall(r"<form[^>]*>", page.text)
    mode_forms = [f for f in forms if "get" in f.lower()]

    assert mode_forms, f"mode control is not a GET form; forms were: {forms}"

    # And the two modes really do produce different staged plans.
    isolated = client.get(
        f"/api/admin/backups/{created.backup_id}/restore", params={"mode": "isolated"}
    ).json()
    in_place = client.get(
        f"/api/admin/backups/{created.backup_id}/restore", params={"mode": "in-place"}
    ).json()

    assert isolated["plan_fingerprint"] != in_place["plan_fingerprint"]
    assert isolated["confirmation_required"] is False
    assert in_place["confirmation_required"] is True


# ---------------------------------------------------------------------------
# the evidence page: verification results must survive the operation
# ---------------------------------------------------------------------------


def test_applying_a_restore_shows_the_post_restore_checks(live):
    """A restore in the GUI must show what it verified, not just "applied".

    The ten post-restore checks were computed, used to decide whether to keep
    the restore, and then discarded: the GUI redirected with a one-word notice.
    An operator had no way to see whether row counts matched or whether EUIDs
    were still unique -- the two questions a restore exists to answer.
    """
    client, created, _cfg, _settings = live

    review = client.get(f"/api/admin/backups/{created.backup_id}/restore").json()
    applied = client.post(
        f"/admin/backups/{created.backup_id}/restore",
        data={"plan_fingerprint": review["plan_fingerprint"], "mode": "isolated"},
        follow_redirects=False,
    )

    assert applied.status_code == 303
    location = applied.headers["location"]
    assert "/admin/backups/receipts/" in location, (
        f"restore did not land on an evidence page: {location}"
    )

    page = client.get(location)
    assert page.status_code == 200, page.text[:500]
    for check_id in (
        "rowcounts.exact",
        "euid.uniqueness",
        "sequences.high_water",
        "schema.drift",
    ):
        assert check_id in page.text, f"{check_id} missing from the evidence page"


def test_verifying_shows_the_integrity_checks(live):
    client, created, _cfg, _settings = live

    response = client.post(
        f"/admin/backups/{created.backup_id}/verify", follow_redirects=False
    )
    page = client.get(response.headers["location"])

    assert "manifest.checksum" in page.text
    assert "archive.deep_read" in page.text


def test_the_checks_are_durable_not_just_rendered_once(live):
    """Reopening the receipt later shows the same evidence.

    This is why the page renders a receipt rather than a transient result: the
    question "what did that restore verify?" is usually asked days afterwards.
    """
    client, created, _cfg, _settings = live

    review = client.get(f"/api/admin/backups/{created.backup_id}/restore").json()
    applied = client.post(
        f"/admin/backups/{created.backup_id}/restore",
        data={"plan_fingerprint": review["plan_fingerprint"], "mode": "isolated"},
        follow_redirects=False,
    )
    location = applied.headers["location"]

    first = client.get(location).text
    second = client.get(location).text

    assert "rowcounts.exact" in second
    assert first == second, "the evidence page is not reproducible"


def test_past_operations_are_reachable_from_the_backups_page(live):
    """Evidence for *every* operation, not only the one just performed."""
    client, created, _cfg, _settings = live
    client.post(f"/admin/backups/{created.backup_id}/verify", follow_redirects=False)

    page = client.get("/admin/backups")

    assert "/admin/backups/receipts/" in page.text, (
        "recent activity does not link to any evidence"
    )


def test_a_missing_receipt_is_a_404_not_a_traceback(live):
    client, _created, _cfg, _settings = live

    assert client.get("/admin/backups/receipts/no-such-receipt").status_code == 404


def test_the_evidence_link_is_a_column_not_hidden_on_the_operation_name(live):
    """The evidence has to be findable without being told where it is.

    It was originally an anchor wrapped around the operation name, with nothing
    in the table indicating it was clickable -- the first person shown the page
    could not find it. The link now has its own column and says how many checks
    it leads to.
    """
    client, created, _cfg, _settings = live
    client.post(f"/admin/backups/{created.backup_id}/verify", follow_redirects=False)

    page = client.get("/admin/backups").text

    assert "<th>Evidence</th>" in page, "no Evidence column in recent activity"
    assert re.search(r">\s*View \d+ checks\s*<", page), (
        "the evidence link does not say what it leads to"
    )


def test_creating_a_backup_records_the_verification_it_already_ran(live):
    """`create` verifies its own artifact; those verdicts must be kept.

    The check was run and thrown away, so a create receipt asserted "succeeded"
    with nothing behind it.
    """
    client, _created, cfg, settings = live
    service.create_backup(cfg, settings, note="evidence test")

    receipts = client.get("/api/admin/backups/status").json()["recent_receipts"]
    creates = [r for r in receipts if r["operation"] == "backup_create"]

    assert creates, "no create receipt found"
    assert creates[0]["detail"].get("checks"), (
        "create recorded no verification checks despite running them"
    )


def test_the_gui_advertises_a_cli_equivalent_for_every_backup_page():
    """The info icon must not claim the backup pages are GUI-only.

    `commandForPage()` in lsmc-ui.js maps each page to its CLI equivalent and
    otherwise renders "No CLI equivalent for tapdb <path>". The backup pages
    were never registered, so the GUI told operators there was no CLI for them
    -- while a seven-command CLI is the primary surface, and plan section 4
    makes surface parity a contract.

    Asserted against the shipped asset rather than a rendered page: the string
    is chosen client-side, so the server response never contains it.
    """
    js = (
        Path(__file__).resolve().parents[1]
        / "daylily_tapdb"
        / "gui"
        / "static"
        / "js"
        / "lsmc-ui.js"
    ).read_text()

    body = js[js.index("function commandForPage()") :]
    body = body[: body.index("\n  }")]

    assert "/admin/backups" in body, "backups page has no CLI hint"
    assert "tapdb backup list" in body
    assert "tapdb backup restore-plan" in body
    # The restore branch must be tested before the bare /admin/backups branch,
    # or every restore page would report `tapdb backup list`.
    assert body.index("/restore") < body.index('includes("/admin/backups")'), (
        "the generic /admin/backups branch shadows the restore branch"
    )


def test_the_gui_can_capture_a_drifted_schema(live):
    """A drift refusal must be recoverable from the GUI, not only the CLI.

    `allow_drift` reached `create_backup` from the CLI and the admin API but
    not the GUI, so a drifted schema made the GUI's Create button fail
    permanently with no in-GUI way through -- while the runbook's remedy is a
    CLI flag and the parity table lists Create on all three surfaces.
    """
    client, _created, cfg, _settings = live
    schema = str(cfg["schema_name"])

    with service.open_session(cfg, app_username="pytest") as conn:
        with conn.session_scope(commit=True) as session:
            session.execute(
                text(
                    f'CREATE TABLE "{schema}".tapdb_gui_drift (uid bigint primary key)'
                )
            )
    try:
        assert 'name="allow_drift"' in client.get("/admin/backups").text, (
            "no allow_drift control on the create form"
        )

        refused = client.post(
            "/admin/backups/create",
            data={"backup_class": "full"},
            follow_redirects=False,
        )
        assert "error=" in refused.headers["location"], refused.headers["location"]

        allowed = client.post(
            "/admin/backups/create",
            data={"backup_class": "full", "allow_drift": "1"},
            follow_redirects=False,
        )
        assert "/admin/backups/receipts/" in allowed.headers["location"], (
            allowed.headers["location"]
        )
    finally:
        with service.open_session(cfg, app_username="pytest") as conn:
            with conn.session_scope(commit=True) as session:
                session.execute(
                    text(f'DROP TABLE IF EXISTS "{schema}".tapdb_gui_drift')
                )
