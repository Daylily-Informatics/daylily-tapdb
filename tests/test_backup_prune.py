"""``backup prune`` — the only operation here that destroys recoverability.

Three disciplines this file holds itself to, each because the obvious version
of the test passes on broken code:

**Anti-vacuity first.** ``HOLD_IDS`` is compared against the cases below, so a
rule added without a test fails rather than sitting unexercised. And each hold
is proved *individually* load-bearing: build a store where one hold is the only
thing protecting a victim, assert it is retained, then disable that one rule
and assert it becomes deletable. The second half is what catches a rule that is
dead because some *other* rule happens to cover the same case.

**``tree_digest`` over a disjoint root.** Every "nothing was deleted" assertion
compares a hash of the whole storage tree, because ``deleted == []`` only
proves what the code *claims*. The digest walks the filesystem directly
including dotfiles — ``LocalStorageBackend.list_keys`` skips them, so a digest
built on it would not notice ``.head`` disappearing, which is the file that
makes receipt truncation detectable. The fixture's storage root is deliberately
**outside** ``config_dir``: under default config ``receipts_directory`` is
``<config_dir>/backups/receipts`` and ``default_storage_uri`` is
``<config_dir>/backups``, so receipts live *inside* the storage root and every
applied prune would change the digest for reasons unrelated to deletion.

**Paired spy assertions.** "Zero calls on a spy" is satisfied identically by
correct code and by a spy the code never received. Every zero-call assertion
here is paired with a positive-call one on the same object.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pytest

from daylily_tapdb.backup import prune as prune_mod
from daylily_tapdb.backup import service
from daylily_tapdb.backup.manifest import (
    AssetRef,
    BackupManifest,
    canonical_bytes,
    sha256_hex,
    sign_manifest,
)
from daylily_tapdb.backup.receipts import (
    OPERATION_CREATE,
    OPERATION_RESTORE,
    STATUS_SUCCEEDED,
    SURFACE_CLI,
    Actor,
    read_receipts,
    write_receipt,
)
from daylily_tapdb.backup.storage import (
    MANIFEST_CHECKSUM_KEY,
    MANIFEST_KEY,
    backup_prefix,
    rehearsal_key,
)

CLIENT = "acme"
DATABASE = "prod"
SCHEMA = "tapdb_prod"
CFG = {
    "client_id": CLIENT,
    "database_name": DATABASE,
    "schema_name": SCHEMA,
    "database": "tapdb",
    "safety": {"destructive_operations": "confirm_required"},
}
LABEL = service.target_label(CFG)
ACTOR = Actor(surface=SURFACE_CLI, username="pytest")
BASE = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store(tmp_path: Path):
    """A target whose storage root is disjoint from its config dir."""
    return {
        "config_dir": tmp_path / "config",
        "storage_dir": tmp_path / "store",
        "settings": {
            "config_dir": str(tmp_path / "config"),
            "storage_uri": f"file://{tmp_path / 'store'}",
            "keep_last": 3,
        },
    }


def _add(
    store,
    backup_id: str,
    *,
    when: Optional[str] = "",
    backup_class: str = "full",
    status: str = "complete",
    provenance: Optional[dict] = None,
    note: str = "",
    target_label: str = LABEL,
    corrupt_checksum: bool = False,
) -> str:
    """Publish a real manifest through the real dataclass."""
    prefix = backup_prefix(CLIENT, DATABASE, backup_class, backup_id)
    root = store["storage_dir"] / prefix
    root.mkdir(parents=True, exist_ok=True)
    payload = b"artifact-bytes"
    (root / "tapdb.dump").write_bytes(payload)

    timestamps: dict[str, Any] = {}
    if when:
        timestamps["started_at"] = when
    if note:
        timestamps["note"] = note

    manifest = BackupManifest(
        backup_id=backup_id,
        backup_class=backup_class,
        status=status,
        target_identity={
            "client_id": CLIENT,
            "database_name": DATABASE,
            "schema_name": SCHEMA,
            "target_label": target_label,
        },
        included_assets=[
            AssetRef(name="tapdb.dump", bytes=len(payload), sha256=sha256_hex(payload))
        ],
        provenance=provenance if provenance is not None else {"created_by": "operator"},
        timestamps=timestamps,
    )
    manifest.signature = sign_manifest(manifest.to_payload(), mode="none")
    raw = canonical_bytes(manifest.to_payload())
    (root / MANIFEST_KEY).write_bytes(raw)
    (root / MANIFEST_CHECKSUM_KEY).write_bytes(
        (sha256_hex(b"wrong") if corrupt_checksum else sha256_hex(raw)).encode("utf-8")
    )
    return prefix


def _series(store, count: int, *, start_days_ago: int = 30, **kwargs) -> list[str]:
    """A run of dated backups, oldest first."""
    ids = []
    for index in range(count):
        backup_id = f"full-2026{index:04d}T000000Z-{index:06x}"
        _add(
            store,
            backup_id,
            when=(BASE - timedelta(days=start_days_ago - index)).isoformat(),
            **kwargs,
        )
        ids.append(backup_id)
    return ids


def _receipt(store, **kwargs):
    return write_receipt(
        store["config_dir"] / "backups" / "receipts",
        actor=ACTOR,
        target_label=LABEL,
        now=BASE,
        **kwargs,
    )


def tree_digest(root: Path) -> str:
    """SHA-256 over every file under ``root``, path and content.

    A raw filesystem walk, deliberately not ``storage.list_keys``: that skips
    dotfiles, so a digest built on it would not notice ``.head`` being deleted
    — the anchor that makes receipt truncation detectable. Content and relative
    path only; anything mtime-based would be flaky, because ``put_bytes`` and
    ``write_receipt`` both stage through temp files and rename.
    """
    if not root.exists():
        return "absent"
    digest = hashlib.sha256()
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        digest.update(str(path.relative_to(root)).encode("utf-8"))
        digest.update(path.read_bytes())
    return digest.hexdigest()


def _plan(store, **kwargs):
    # `now` is pinned to the same instant the fixtures are built around.
    # Without it every future-skew comparison runs against wall clock while the
    # data sits at a fixed BASE -- so `_victim_newest_successful`'s
    # `BASE + minutes` entries were future-dated until that minute passed in
    # real time, and the suite's correctness depended on when it was run.
    kwargs.setdefault("now", BASE)
    return prune_mod.plan_prune(CFG, store["settings"], **kwargs)


def _apply(store, **kwargs):
    kwargs.setdefault("confirm_target", LABEL)
    kwargs.setdefault("now", BASE)
    return prune_mod.prune_backups(
        CFG, store["settings"], apply=True, actor=ACTOR, **kwargs
    )


def _deletable_ids(plan) -> set[str]:
    return {item.backup_id for item in plan.deletable}


# ---------------------------------------------------------------------------
# anti-vacuity
# ---------------------------------------------------------------------------


def test_the_hold_registry_and_the_id_list_agree():
    """A rule that is not registered cannot execute; a registered id needs a rule.

    Enforced at import in ``prune.py`` too, but asserted here so the failure
    names the mismatch rather than surfacing as an import error.
    """
    assert set(prune_mod._HOLD_RULES) == set(prune_mod.HOLD_IDS)
    assert len(set(prune_mod.HOLD_IDS)) == len(prune_mod.HOLD_IDS), "duplicate id"


def test_releasable_holds_are_a_strict_subset():
    """``keep_last`` and the only-copy floors must have no flag at all.

    ``--release keep_last`` would collapse a 90-backup history to roughly one
    per class in a single command.
    """
    assert set(prune_mod.RELEASABLE_HOLDS) < set(prune_mod.HOLD_IDS)
    for forbidden in (
        prune_mod.HOLD_KEEP_LAST,
        prune_mod.HOLD_SAFETY_BACKUP,
        prune_mod.HOLD_ONLY_COPY_OF_TARGET,
        prune_mod.HOLD_ONLY_COPY_OF_CLASS,
        prune_mod.HOLD_NEWEST_SUCCESSFUL,
    ):
        assert forbidden not in prune_mod.RELEASABLE_HOLDS


def test_releasing_an_unknown_hold_is_rejected(store):
    _series(store, 2)
    with pytest.raises(prune_mod.PruneRefusedError):
        _plan(store, released=("keep_last",))
    with pytest.raises(prune_mod.PruneRefusedError):
        _plan(store, released=("not_a_hold",))


def test_every_hold_emitted_is_a_registered_id(store):
    """No rule may apply a hold under a literal string."""
    _series(store, 6)
    _add(store, "tpk-1", when=BASE.isoformat(), backup_class="template-pack")
    plan = _plan(store)

    seen = {hold for item in plan.candidates for hold in item.holds}
    assert seen <= set(prune_mod.HOLD_IDS), seen - set(prune_mod.HOLD_IDS)
    assert seen, "no holds applied at all -- the rules did not run"


# ---------------------------------------------------------------------------
# every hold, individually load-bearing
#
# Each case builds a store where exactly one hold protects a victim. The first
# assertion catches the rule being deleted; the second catches the rule being
# dead because something else already covered the case.
# ---------------------------------------------------------------------------


def _victim_undated(store):
    _series(store, 5)
    _add(store, "undated-1", when="")
    return "undated-1"


def _victim_unparseable(store):
    _series(store, 5)
    _add(store, "bad-date-1", when="unknown")
    return "bad-date-1"


def _victim_future(store):
    _series(store, 5)
    _add(store, "future-1", when="2099-01-01T00:00:00+00:00")
    return "future-1"


def _victim_checksum(store):
    _series(store, 5)
    _add(
        store,
        "corrupt-1",
        when=(BASE - timedelta(days=90)).isoformat(),
        corrupt_checksum=True,
    )
    return "corrupt-1"


def _victim_keep_last(store):
    ids = _series(store, 8)
    # Third-newest: inside the keep_last window, but not the newest, so
    # neither `newest_successful` nor `only_copy_of_class` also covers it.
    return ids[-3]


def _victim_newest_successful(store):
    """The newest *complete* backup, sitting outside the keep_last window.

    With keep_last 3 and the three newest all incomplete, the fourth-newest is
    the newest complete one -- held by `newest_successful` alone, since
    `keep_last` covers only the three above it and `only_copy_of_class` covers
    only the very newest.
    """
    ids = _series(store, 5)
    for index in range(3):
        _add(
            store,
            f"partial-{index}",
            when=(BASE + timedelta(minutes=index)).isoformat(),
            status="partial",
        )
    return ids[-1]


def _victim_only_copy_of_target(store):
    _add(store, "solo-1", when=BASE.isoformat())
    return "solo-1"


def _victim_only_copy_of_class(store):
    _series(store, 8)
    _add(
        store,
        "tpk-old",
        when=(BASE - timedelta(days=99)).isoformat(),
        backup_class="template-pack",
    )
    return "tpk-old"


def _victim_safety_backup(store):
    _series(store, 5)
    _add(
        store,
        "safety-1",
        when=(BASE - timedelta(days=90)).isoformat(),
        provenance={"created_by": "restore", "restored_backup_id": "full-00000"},
    )
    return "safety-1"


def _victim_recently_restored_from(store):
    _series(store, 5)
    _add(store, "restored-1", when=(BASE - timedelta(days=90)).isoformat())
    _receipt(
        store,
        operation=OPERATION_RESTORE,
        status=STATUS_SUCCEEDED,
        backup_id="restored-1",
    )
    return "restored-1"


def _victim_rehearsal_evidence(store):
    _series(store, 5)
    _add(store, "rehearsed-1", when=(BASE - timedelta(days=90)).isoformat())
    evidence = store["storage_dir"] / rehearsal_key(
        CLIENT, DATABASE, "rehearsed-1", "20260801T000000Z"
    )
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text("{}")
    return "rehearsed-1"


def _victim_provenance_unknown(store):
    _series(store, 5)
    # Older than any receipt, and carrying no structured provenance.
    _receipt(store, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _add(
        store, "legacy-1", when=(BASE - timedelta(days=900)).isoformat(), provenance={}
    )
    return "legacy-1"


def _victim_provider_snapshot(store):
    """Two snapshots, so `only_copy_of_class` protects the newer one and the
    older is left to `provider_snapshot_reference` alone."""
    _series(store, 5)
    _add(
        store,
        "snp-new",
        when=(BASE - timedelta(days=1)).isoformat(),
        backup_class="provider-snapshot",
    )
    _add(
        store,
        "snp-old",
        when=(BASE - timedelta(days=90)).isoformat(),
        backup_class="provider-snapshot",
    )
    return "snp-old"


def _victim_duplicate_backup_id(store):
    """Two prefixes claiming one id -- a hand-copied directory is enough."""
    _series(store, 5)
    _add(store, "dupe-1", when=(BASE - timedelta(days=90)).isoformat())
    _add(
        store,
        "dupe-1",
        when=(BASE - timedelta(days=91)).isoformat(),
        backup_class="template-pack",
    )
    return "dupe-1"


def _victim_damaged(store):
    _series(store, 5)
    prefix = _add(store, "damaged-1", when=(BASE - timedelta(days=90)).isoformat())
    (store["storage_dir"] / prefix / MANIFEST_KEY).write_text("{ not json")
    return "damaged-1"


HOLD_CASES = {
    prune_mod.HOLD_UNDATED: _victim_undated,
    prune_mod.HOLD_UNPARSEABLE_CREATED_AT: _victim_unparseable,
    prune_mod.HOLD_FUTURE_DATED: _victim_future,
    prune_mod.HOLD_CHECKSUM_MISMATCH: _victim_checksum,
    prune_mod.HOLD_KEEP_LAST: _victim_keep_last,
    prune_mod.HOLD_NEWEST_SUCCESSFUL: _victim_newest_successful,
    prune_mod.HOLD_ONLY_COPY_OF_TARGET: _victim_only_copy_of_target,
    prune_mod.HOLD_ONLY_COPY_OF_CLASS: _victim_only_copy_of_class,
    prune_mod.HOLD_SAFETY_BACKUP: _victim_safety_backup,
    prune_mod.HOLD_RECENTLY_RESTORED_FROM: _victim_recently_restored_from,
    prune_mod.HOLD_REHEARSAL_EVIDENCE: _victim_rehearsal_evidence,
    prune_mod.HOLD_PROVENANCE_UNKNOWN: _victim_provenance_unknown,
    prune_mod.HOLD_PROVIDER_SNAPSHOT_REFERENCE: _victim_provider_snapshot,
    prune_mod.HOLD_DAMAGED: _victim_damaged,
    prune_mod.HOLD_DUPLICATE_BACKUP_ID: _victim_duplicate_backup_id,
}


def test_every_hold_has_a_case():
    """Adding a hold without a case here fails, rather than going unexercised."""
    assert set(HOLD_CASES) == set(prune_mod.HOLD_IDS), {
        "uncovered": sorted(set(prune_mod.HOLD_IDS) - set(HOLD_CASES)),
        "unknown": sorted(set(HOLD_CASES) - set(prune_mod.HOLD_IDS)),
    }


#: Holds that cannot be the *sole* protection by construction, and are kept
#: anyway as defence in depth. Named explicitly so the distinction is a
#: decision rather than a fixture that quietly failed to isolate.
#:
#: ``only_copy_of_target`` fires only when a store holds one backup, and that
#: backup is necessarily also covered by ``keep_last`` (>= 1 by gate),
#: ``newest_successful`` and ``only_copy_of_class``. It exists so that a future
#: bug in the window arithmetic cannot empty a target completely.
#:
#: ``damaged`` fires on a prefix with no readable manifest, which therefore
#: also has no date (``undated``), no verifiable checksum
#: (``checksum_mismatch``) and no provenance (``provenance_unknown``). It
#: exists so the reason reported to an operator is the true one.
DEFENCE_IN_DEPTH_HOLDS = {
    prune_mod.HOLD_ONLY_COPY_OF_TARGET,
    prune_mod.HOLD_DAMAGED,
    # A duplicated id necessarily also trips `only_copy_of_class` on one of
    # the two prefixes, so it cannot be the sole protection either.
    prune_mod.HOLD_DUPLICATE_BACKUP_ID,
}

ISOLATABLE_HOLDS = sorted(set(HOLD_CASES) - DEFENCE_IN_DEPTH_HOLDS)


@pytest.mark.parametrize("hold_id", sorted(DEFENCE_IN_DEPTH_HOLDS))
def test_a_defence_in_depth_hold_still_fires_on_its_own(store, hold_id, monkeypatch):
    """Prove the rule works without claiming it is uniquely necessary.

    These two overlap other rules by construction, so the isolation test below
    cannot apply -- there is no store in which they are the only protection.
    Disabling every *other* rule shows each still fires, which is what makes it
    real defence in depth rather than dead code nobody noticed.
    """
    victim = HOLD_CASES[hold_id](store)

    only_this = {
        other: (
            prune_mod._HOLD_RULES[other] if other == hold_id else (lambda ctx: set())
        )
        for other in prune_mod.HOLD_IDS
    }
    monkeypatch.setattr(prune_mod, "_HOLD_RULES", only_this)

    plan = _plan(store, ignore_damaged=True)
    held = next(item for item in plan.candidates if item.backup_id == victim)

    assert held.holds == {hold_id}, sorted(held.holds)


@pytest.mark.parametrize("hold_id", ISOLATABLE_HOLDS)
def test_each_hold_actually_protects_its_victim(store, hold_id, monkeypatch):
    """Retained with the rule; deletable without it.

    Disabling differs by class deliberately. Releasable holds use the real
    ``--release`` path, so the shipped flag is exercised. The rest are
    monkeypatched out of the registry -- proving them load-bearing must not
    require shipping an override for ``keep_last`` or ``safety_backup``.
    """
    victim = HOLD_CASES[hold_id](store)

    plan = _plan(store, ignore_damaged=True)
    held = next(item for item in plan.candidates if item.backup_id == victim)
    assert hold_id in held.holds, (
        f"{hold_id} did not protect {victim}; holds were {sorted(held.holds)}"
    )

    if hold_id in prune_mod.RELEASABLE_HOLDS:
        released = _plan(store, released=(hold_id,), ignore_damaged=True)
    else:
        rules = dict(prune_mod._HOLD_RULES)
        rules[hold_id] = lambda ctx: set()
        monkeypatch.setattr(prune_mod, "_HOLD_RULES", rules)
        released = _plan(store, ignore_damaged=True)

    now_held = next(item for item in released.candidates if item.backup_id == victim)
    assert hold_id not in now_held.holds

    # The half that actually proves something. Asserting only "the hold is
    # gone" is satisfied by a rule that never mattered, because some other rule
    # was covering the same case all along. Requiring the victim to become
    # *deletable* is what makes each fixture construct a store where this hold
    # is genuinely the only protection.
    assert now_held.deletable, (
        f"{hold_id} may be redundant: with it disabled, {victim} is still held "
        f"by {sorted(now_held.holds)} -- so this case does not prove the rule "
        "is load-bearing"
    )


# ---------------------------------------------------------------------------
# the destructive path
# ---------------------------------------------------------------------------


def test_a_plan_deletes_nothing_and_writes_no_receipt(store):
    """The digest, not ``deleted == []``.

    ``deleted == []`` proves what the code claims about itself; the digest
    proves what it did. A plan writes no receipt either -- a plan is a read,
    and filling the audit trail with reads buries the writes.
    """
    _series(store, 8)
    before = tree_digest(store["storage_dir"])

    result = prune_mod.prune_backups(CFG, store["settings"], actor=ACTOR)

    assert result.dry_run is True
    assert tree_digest(store["storage_dir"]) == before
    assert read_receipts(store["config_dir"] / "backups" / "receipts") == []


def test_the_dry_run_default_is_isolated_from_the_typed_label(store):
    """The single most important test, and the one easiest to write uselessly.

    ``--apply`` and ``--confirm-target`` are two independent gates. Invoking
    with *neither* and asserting nothing was deleted passes even if the
    dry-run default is inverted, because the typed-label check would still
    refuse. Supplying the **correct label but no apply** isolates the default:
    the moment it flips, this store loses every eligible backup.
    """
    _series(store, 8)
    before = tree_digest(store["storage_dir"])

    result = prune_mod.prune_backups(
        CFG, store["settings"], confirm_target=LABEL, actor=ACTOR
    )

    assert result.dry_run is True
    assert result.plan.deletable, "the store must have eligible backups"
    assert tree_digest(store["storage_dir"]) == before


def test_applying_with_a_wrong_label_refuses_and_deletes_nothing(store):
    from daylily_tapdb.backup.errors import RestoreConfirmationError

    _series(store, 8)
    before = tree_digest(store["storage_dir"])

    with pytest.raises(RestoreConfirmationError):
        _apply(store, confirm_target="acme/prod/wrong@tapdb", allow_bulk=True)

    assert tree_digest(store["storage_dir"]) == before


def test_applying_deletes_only_the_unheld(store):
    ids = _series(store, 8)
    plan = _plan(store, allow_bulk=True)
    expected = _deletable_ids(plan)

    result = _apply(store, allow_bulk=True)

    assert {item["backup_id"] for item in result.deleted} == expected
    assert result.ok
    survivors = {
        entry.backup_id
        for entry in service.list_backups(CFG, store["settings"]).entries
    }
    assert survivors == set(ids) - expected
    assert len(survivors) >= store["settings"]["keep_last"]


def test_deletion_removes_the_manifest_last(store, monkeypatch):
    """Order survives an interruption.

    Manifest first would leave artifact bytes that ``discover_backup_prefixes``
    cannot see at all -- unreachable and unaccounted for. Manifest last leaves
    something that still lists and that ``health.hollow_backup`` reports.
    """
    _series(store, 8)
    storage = service.storage_for(store["settings"])
    order: list[str] = []
    real_delete = storage.delete

    def _spy(key):
        order.append(key)
        return real_delete(key)

    monkeypatch.setattr(service, "storage_for", lambda _s: storage)
    monkeypatch.setattr(storage, "delete", _spy)

    _apply(store, allow_bulk=True)

    assert order, "the spy was never called -- this test proves nothing"
    per_prefix: dict[str, list[str]] = {}
    for key in order:
        per_prefix.setdefault(key.rsplit("/", 1)[0], []).append(key)
    for prefix, keys in per_prefix.items():
        assert keys[-1].endswith(MANIFEST_KEY), (prefix, keys)
        assert keys[-2].endswith(MANIFEST_CHECKSUM_KEY), (prefix, keys)


def test_prune_never_uses_delete_prefix(store, monkeypatch):
    """``delete_prefix`` is ``shutil.rmtree`` on a resolved path.

    No per-key error attribution and no control over order, so an interruption
    lands wherever the walk happened to be. Paired with a positive assertion on
    ``delete``: zero calls on a spy is satisfied identically by correct code
    and by a spy nothing ever received.
    """
    _series(store, 8)
    storage = service.storage_for(store["settings"])
    calls = {"delete_prefix": 0, "delete": 0}
    real_delete = storage.delete

    def _spy_prefix(prefix):
        calls["delete_prefix"] += 1

    def _spy_delete(key):
        calls["delete"] += 1
        return real_delete(key)

    monkeypatch.setattr(service, "storage_for", lambda _s: storage)
    monkeypatch.setattr(storage, "delete_prefix", _spy_prefix)
    monkeypatch.setattr(storage, "delete", _spy_delete)

    _apply(store, allow_bulk=True)

    assert calls["delete"] > 0, "the spy was never wired in"
    assert calls["delete_prefix"] == 0


def test_an_applied_prune_writes_an_intent_and_an_outcome(store):
    """Two receipts, linked. The pair is the interrupted-prune detector."""
    _series(store, 8)
    result = _apply(store, allow_bulk=True)

    receipts = [
        r
        for r in read_receipts(store["config_dir"] / "backups" / "receipts")
        if r.operation == "backup_prune"
    ]
    phases = [(r.detail or {}).get("phase") for r in receipts]

    assert phases == ["intent", "outcome"]
    assert {(r.detail or {}).get("prune_id") for r in receipts} == {result.prune_id}
    # The intent names the prefixes, which is what reconciliation reads.
    assert (receipts[0].detail or {})["prefixes"]
    # The outcome reduces retained to counts, because receipts are read in full
    # on every status page.
    assert isinstance((receipts[1].detail or {})["retained"], dict)


# ---------------------------------------------------------------------------
# gates
# ---------------------------------------------------------------------------


def test_keep_last_of_zero_is_refused(store):
    """``db_config._int`` does not validate this; a hand-edited 0 arrives verbatim."""
    _series(store, 8)
    store["settings"]["keep_last"] = 0
    before = tree_digest(store["storage_dir"])

    plan = _plan(store)
    assert not plan.ok
    assert any(g.id == prune_mod.GATE_RETENTION_SANE for g in plan.blocking)

    with pytest.raises(prune_mod.PruneRefusedError):
        _apply(store, allow_bulk=True)
    assert tree_digest(store["storage_dir"]) == before


def test_a_broken_receipt_chain_refuses(store):
    """Three holds read receipts as evidence.

    If the chain does not verify, that evidence may have been edited -- and the
    rules being fooled are the ones protecting the most important backups.
    """
    _series(store, 8)
    _receipt(store, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _receipt(store, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    newest = sorted((store["config_dir"] / "backups" / "receipts").glob("*.json"))[-1]
    newest.chmod(0o600)
    newest.write_text('{"broken": true}')

    plan = _plan(store)

    assert not plan.ok
    assert any(g.id == prune_mod.GATE_RECEIPT_CHAIN for g in plan.blocking)


def test_a_damaged_manifest_refuses_until_ignored(store):
    _series(store, 8)
    prefix = _add(store, "damaged-x", when=(BASE - timedelta(days=99)).isoformat())
    (store["storage_dir"] / prefix / MANIFEST_KEY).write_text("{ nope")

    blocked = _plan(store)
    assert not blocked.ok
    gate = next(g for g in blocked.blocking if g.id == prune_mod.GATE_NO_DAMAGED)
    assert gate.escape == "--ignore-damaged"

    allowed = _plan(store, ignore_damaged=True)
    assert all(g.id != prune_mod.GATE_NO_DAMAGED for g in allowed.blocking)
    # Ignored, but never deletable: unknown is not grounds for deletion.
    victim = next(i for i in allowed.candidates if i.backup_id == "damaged-x")
    assert not victim.deletable


def test_the_delete_ceiling_refuses_a_bulk_plan(store):
    """The control that still means something once this runs unattended.

    The typed label becomes a constant in a config file the moment it is
    scheduled, and the policy gate lets `confirm_required` straight through.
    """
    _series(store, 40)
    plan = _plan(store)

    assert not plan.ok
    gate = next(g for g in plan.blocking if g.id == prune_mod.GATE_DELETE_CEILING)
    assert gate.escape == "--allow-bulk"
    assert _plan(store, allow_bulk=True).ok


def test_a_bucket_that_does_not_reclaim_is_refused(store, monkeypatch):
    _series(store, 8)
    storage = service.storage_for(store["settings"])
    monkeypatch.setattr(
        storage,
        "deletion_capability",
        lambda: {
            "reclaims": False,
            "reason": "versioning is Enabled",
            "versioning": "Enabled",
            "object_lock": False,
        },
    )
    monkeypatch.setattr(service, "storage_for", lambda _s: storage)

    plan = _plan(store, allow_bulk=True)
    gate = next(g for g in plan.gates if g.id == prune_mod.GATE_STORAGE_RECLAIMS)

    assert not gate.ok
    assert gate.escape == "--allow-delete-markers"
    assert _plan(store, allow_bulk=True, allow_delete_markers=True).ok


def test_object_lock_has_no_escape(store, monkeypatch):
    """An externally declared retention policy a TapDB prune can only conflict with."""
    _series(store, 8)
    storage = service.storage_for(store["settings"])
    monkeypatch.setattr(
        storage,
        "deletion_capability",
        lambda: {
            "reclaims": False,
            "reason": "Object Lock is configured",
            "versioning": None,
            "object_lock": True,
        },
    )
    monkeypatch.setattr(service, "storage_for", lambda _s: storage)

    for kwargs in (
        {"allow_delete_markers": True},
        {"allow_unknown_reclaim": True},
        {"allow_delete_markers": True, "allow_unknown_reclaim": True},
    ):
        plan = _plan(store, allow_bulk=True, **kwargs)
        gate = next(g for g in plan.gates if g.id == prune_mod.GATE_STORAGE_RECLAIMS)
        assert not gate.ok, kwargs
        assert gate.escape is None


def test_an_unknown_backend_fails_closed(store, monkeypatch):
    """Absence of the probe is unknown, never safe.

    TapDB is embedded by several hosts, and a host-supplied backend need not
    implement `deletion_capability`.
    """
    _series(store, 8)

    class _NoProbe:
        """Forwards everything except the probe, which it does not have."""

        def __init__(self, real):
            self._real = real

        def __getattr__(self, name):
            if name == "deletion_capability":
                raise AttributeError(name)
            return getattr(self._real, name)

    wrapped = _NoProbe(service.storage_for(store["settings"]))
    monkeypatch.setattr(service, "storage_for", lambda _s: wrapped)
    monkeypatch.setattr(prune_mod.service, "storage_for", lambda _s: wrapped)

    plan = _plan(store, allow_bulk=True)
    gate = next(g for g in plan.gates if g.id == prune_mod.GATE_STORAGE_RECLAIMS)
    assert not gate.ok
    assert gate.escape == "--allow-unknown-reclaim"


def test_a_foreign_target_is_excluded_not_held(store):
    """Exclusion, because a hold would still occupy a keep_last slot.

    With keep_last 3 and three foreign backups held-but-ranked, the window
    fills with backups prune may not delete and this target's entire history
    falls outside it with an empty hold set.
    """
    ids = _series(store, 8)
    for index in range(5):
        _add(
            store,
            f"foreign-{index}",
            when=(BASE - timedelta(minutes=index)).isoformat(),
            target_label="other/client/tapdb_other@tapdb",
        )

    plan = _plan(store, allow_bulk=True)

    excluded = {item["backup_id"] for item in plan.excluded}
    assert excluded == {f"foreign-{i}" for i in range(5)}
    assert all(not item.backup_id.startswith("foreign-") for item in plan.candidates)
    # The window still protects this target's newest, not the intruders'.
    held = {i.backup_id for i in plan.retained if prune_mod.HOLD_KEEP_LAST in i.holds}
    assert held <= set(ids)
    assert len(held) == store["settings"]["keep_last"]


# ---------------------------------------------------------------------------
# concurrency, reconciliation, and the ranking trap
# ---------------------------------------------------------------------------


def test_a_restore_between_plan_and_apply_aborts_the_apply(store):
    """The hazard re-reading the manifest does not cover.

    A concurrent restore never modifies the manifest it reads, so re-reading
    manifests proves nothing about it. It does write a receipt, so the head
    advancing is the signal that something happened this plan did not account
    for -- including a restore from a backup about to be deleted.
    """
    from daylily_tapdb.backup.errors import RestoreStageStaleError

    ids = _series(store, 8)
    plan = _plan(store, allow_bulk=True)
    before = tree_digest(store["storage_dir"])

    # Someone restores from the oldest backup, which this plan would delete.
    _receipt(
        store,
        operation=OPERATION_RESTORE,
        status=STATUS_SUCCEEDED,
        backup_id=ids[0],
    )

    with pytest.raises(RestoreStageStaleError):
        _apply(store, plan_fingerprint=plan.plan_fingerprint, allow_bulk=True)

    assert tree_digest(store["storage_dir"]) == before


def test_the_fingerprint_covers_the_receipt_head(store):
    ids = _series(store, 8)
    first = _plan(store, allow_bulk=True).plan_fingerprint

    _receipt(
        store, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED, backup_id=ids[0]
    )

    assert _plan(store, allow_bulk=True).plan_fingerprint != first


def test_an_interrupted_prune_is_reconciled_before_the_next_one(store):
    """The only thing that clears `health.interrupted_prune`.

    Receipts are immutable, so a dangling intent cannot be resolved by editing
    anything -- writing the missing outcome on the next run is the whole
    remediation path.
    """
    _series(store, 8)
    orphan_prefix = _add(
        store, "orphan-1", when=(BASE - timedelta(days=99)).isoformat()
    )
    # Genuinely half-deleted: artifacts gone, manifest still present. That is
    # exactly what an interruption between the two deletes leaves.
    (store["storage_dir"] / orphan_prefix / "tapdb.dump").unlink()

    _receipt(
        store,
        operation="backup_prune",
        status=STATUS_SUCCEEDED,
        detail={
            "phase": "intent",
            "prune_id": "prune-dead",
            "prefixes": [orphan_prefix],
        },
    )

    result = _apply(store, allow_bulk=True)

    assert "prune-dead" in result.reconciled
    outcomes = [
        r
        for r in read_receipts(store["config_dir"] / "backups" / "receipts")
        if r.operation == "backup_prune"
        and (r.detail or {}).get("prune_id") == "prune-dead"
        and (r.detail or {}).get("phase") == "outcome"
    ]
    assert len(outcomes) == 1
    detail = outcomes[0].detail or {}
    assert detail["reconciled"] is True
    assert orphan_prefix in detail["finished"]
    assert not (store["storage_dir"] / orphan_prefix).exists()


def test_reconciliation_never_deletes_an_untouched_prefix(store):
    """A crash between the intent and the first delete must not become a mass delete.

    The intent is written *before* any deletion, so that window leaves every
    planned prefix completely intact. Treating "still has files" as
    "half-deleted" would delete the whole planned set during reconciliation --
    bypassing the holds, the delete ceiling, the typed target label, and the
    receipt-head check that would have noticed a restore in the meantime.

    Reconciliation finishes damage; it does not resume a decision.
    """
    _series(store, 4)  # small store, keep_last 3 -> at most one deletable
    intact = _add(store, "intact-1", when=(BASE - timedelta(days=99)).isoformat())
    # Someone restored from it after the interrupted run planned to delete it.
    _receipt(
        store,
        operation=OPERATION_RESTORE,
        status=STATUS_SUCCEEDED,
        backup_id="intact-1",
    )
    _receipt(
        store,
        operation="backup_prune",
        status=STATUS_SUCCEEDED,
        detail={
            "phase": "intent",
            "prune_id": "prune-crashed",
            "prefixes": [intact],
        },
    )

    result = _apply(store, allow_bulk=True)

    assert "prune-crashed" in result.reconciled
    outcome = next(
        r
        for r in read_receipts(store["config_dir"] / "backups" / "receipts")
        if r.operation == "backup_prune"
        and (r.detail or {}).get("prune_id") == "prune-crashed"
        and (r.detail or {}).get("phase") == "outcome"
    )
    assert intact in (outcome.detail or {})["untouched"]
    assert (outcome.detail or {})["finished"] == []

    # Still there, because normal planning re-evaluated it and the restore
    # receipt now holds it.
    assert (store["storage_dir"] / intact / MANIFEST_KEY).exists()
    survivors = {
        entry.backup_id
        for entry in service.list_backups(CFG, store["settings"]).entries
    }
    assert "intact-1" in survivors


def test_undated_manifests_never_consume_the_window(store):
    """30 undated with keep_last 30 must not make every dated backup deletable.

    The trap: `list_backups` sorts on a raw string, so a missing `started_at`
    collapses to "" and sorts *oldest*. Ranked, they would fill the window from
    the bottom and evict everything real.
    """
    store["settings"]["keep_last"] = 30
    for index in range(30):
        _add(store, f"undated-{index}", when="")
    dated = _series(store, 5)

    plan = _plan(store, allow_bulk=True)

    # Asserting the exact surviving set, not merely that nothing is deletable:
    # `deletable == []` is also satisfied by "prune never deletes anything".
    assert _deletable_ids(plan) == set()
    for backup_id in dated:
        held = next(i for i in plan.candidates if i.backup_id == backup_id)
        assert prune_mod.HOLD_KEEP_LAST in held.holds


def test_a_bad_clock_cannot_evict_real_backups(store):
    """Future-dated entries are dropped from the ranking, not merely held.

    A hold still occupies a slot. An hourly cron through a skew window with
    `keep_last: 7` would otherwise fill every slot with 2099-dated manifests
    and leave every real backup with an empty hold set.
    """
    store["settings"]["keep_last"] = 3
    for index in range(5):
        _add(store, f"skewed-{index}", when=f"209{index}-01-01T00:00:00+00:00")
    dated = _series(store, 6)

    plan = _plan(store, allow_bulk=True)

    protected = {
        i.backup_id for i in plan.candidates if prune_mod.HOLD_KEEP_LAST in i.holds
    }
    assert protected == set(dated[-3:]), protected
    assert all(not b.startswith("skewed-") for b in protected)


def test_releasing_rehearsal_evidence_deletes_it_with_the_backup(store):
    """Postcondition: no evidence may reference a backup id that no longer exists.

    `rehearsals/<id>/` has no manifest, so it is invisible to every listing
    surface -- left behind it is unreachable and permanent.
    """
    _series(store, 8)
    _add(store, "rehearsed-x", when=(BASE - timedelta(days=99)).isoformat())
    evidence = store["storage_dir"] / rehearsal_key(
        CLIENT, DATABASE, "rehearsed-x", "20260801T000000Z"
    )
    evidence.parent.mkdir(parents=True, exist_ok=True)
    evidence.write_text("{}")

    _apply(
        store,
        released=(prune_mod.HOLD_REHEARSAL_EVIDENCE,),
        allow_bulk=True,
    )

    surviving = {
        entry.backup_id
        for entry in service.list_backups(CFG, store["settings"]).entries
    }
    assert "rehearsed-x" not in surviving
    assert not evidence.exists()
    # The postcondition, asserted directly.
    rehearsal_root = store["storage_dir"] / CLIENT / DATABASE / "rehearsals"
    if rehearsal_root.exists():
        for child in rehearsal_root.iterdir():
            assert child.name in surviving, f"orphaned evidence for {child.name}"


# ---------------------------------------------------------------------------
# properties
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("keep_last", [1, 2, 3, 5])
@pytest.mark.parametrize("total", [1, 4, 12, 30])
def test_invariants_hold_across_store_shapes(tmp_path, keep_last, total):
    """Invariants, plus a coverage assertion so they cannot hold vacuously.

    Every one of these is trivially true when nothing is deletable, and with
    ``DEFAULT_BACKUP_KEEP_LAST`` at 30 a realistic generated store produces
    exactly that. The parametrisation deliberately spans shapes where deletion
    does and does not occur, and ``test_the_property_sweep_actually_deletes``
    below asserts the sweep as a whole exercised both.
    """
    local = {
        "config_dir": tmp_path / "config",
        "storage_dir": tmp_path / "store",
        "settings": {
            "config_dir": str(tmp_path / "config"),
            "storage_uri": f"file://{tmp_path / 'store'}",
            "keep_last": keep_last,
        },
    }
    ids = _series(local, total, start_days_ago=total + 1)
    plan = prune_mod.plan_prune(CFG, local["settings"], allow_bulk=True, now=BASE)

    deletable = {i.backup_id for i in plan.deletable}
    retained = {i.backup_id for i in plan.retained}

    assert deletable & retained == set()
    assert deletable | retained == set(ids)
    assert len(deletable) + len(retained) == len(plan.candidates)
    # The floor: never fewer survivors than the window asks for.
    assert len(retained) >= min(keep_last, total)
    # The newest is never deletable, whatever the shape.
    assert ids[-1] in retained

    _PROPERTY_OUTCOMES.append(bool(deletable))


#: Records whether each parametrised case actually deleted anything, so the
#: sweep can prove it exercised the interesting half.
_PROPERTY_OUTCOMES: list[bool] = []


def test_the_property_sweep_actually_deletes():
    """Guards the guard.

    Without this, every invariant above could hold because the sweep never
    produced a single deletable backup -- an expensive way to assert
    ``set() & set() == set()``. Ordered after the sweep by name.
    """
    assert _PROPERTY_OUTCOMES, "the property sweep did not run"
    assert any(_PROPERTY_OUTCOMES), "no generated store ever had a deletable backup"
    assert not all(_PROPERTY_OUTCOMES), "no generated store was fully protected"


# ---------------------------------------------------------------------------
# gaps the mutation sweep exposed
# ---------------------------------------------------------------------------


def test_a_legacy_safety_backup_is_held_by_its_note_alone(store):
    """The regex path, which structured provenance otherwise hides.

    A safety backup written before `provenance` existed carries no structured
    evidence at all -- only the English note. Testing the hold exclusively
    through `provenance` leaves that path unexercised, and it is the one that
    covers every backup taken before this field shipped.

    The note may only *add* a hold. It is a regex over prose, so it can be
    wrong in the direction of over-protecting; it must never be able to cancel
    a hold another source applied.
    """
    _series(store, 8)
    _receipt(store, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)
    _add(
        store,
        "legacy-safety",
        when=(BASE - timedelta(days=2)).isoformat(),
        provenance={"created_by": "operator"},  # no restore provenance at all
        note="pre-restore safety backup for full-20260001T000000Z-000001",
    )

    plan = _plan(store, allow_bulk=True)
    held = next(i for i in plan.candidates if i.backup_id == "legacy-safety")

    assert prune_mod.HOLD_SAFETY_BACKUP in held.holds, sorted(held.holds)


def test_the_receipt_head_is_rechecked_even_without_a_fingerprint(store):
    """The head check must stand on its own.

    Passing ``plan_fingerprint`` makes the fingerprint comparison fire first,
    so a test that always supplies one cannot tell whether the pre-delete head
    re-read exists at all. A scheduled prune calls apply without a fingerprint
    -- that is the path this guards.
    """
    from daylily_tapdb.backup.errors import RestoreStageStaleError

    ids = _series(store, 8)
    before = tree_digest(store["storage_dir"])

    real_plan = prune_mod.plan_prune

    def _plan_then_restore(cfg, settings, **kwargs):
        """Plan, then simulate a restore landing before the first delete."""
        plan = real_plan(cfg, settings, **kwargs)
        if not getattr(_plan_then_restore, "fired", False):
            _plan_then_restore.fired = True
            _receipt(
                store,
                operation=OPERATION_RESTORE,
                status=STATUS_SUCCEEDED,
                backup_id=ids[0],
            )
        return plan

    prune_mod.plan_prune = _plan_then_restore
    try:
        with pytest.raises(RestoreStageStaleError):
            _apply(store, allow_bulk=True)  # no plan_fingerprint at all
    finally:
        prune_mod.plan_prune = real_plan

    assert tree_digest(store["storage_dir"]) == before


def test_an_applied_prune_without_an_intent_receipt_is_impossible(store):
    """The intent must exist *before* the first delete, not after.

    An intent written afterwards records history but detects nothing: the whole
    point of the pair is that an interruption leaves an intent with no outcome.
    Asserted by sequence number, because both receipts exist either way.
    """
    _series(store, 8)
    result = _apply(store, allow_bulk=True)

    receipts = read_receipts(store["config_dir"] / "backups" / "receipts")
    by_id = {r.receipt_id: r for r in receipts}
    intent = by_id[result.intent_receipt_id]
    outcome = by_id[result.outcome_receipt_id]

    assert intent.sequence < outcome.sequence
    assert (intent.detail or {})["phase"] == "intent"
    assert set((intent.detail or {})["backup_ids"]) == {
        item["backup_id"] for item in result.deleted
    }


def test_prefix_integrity_rejects_a_backup_in_the_wrong_class_directory(store):
    """The gate's reachable failure: a prefix that cannot recompute.

    Recomputation uses **cfg**, never the manifest's own ``target_identity``.
    Today that distinction is belt-and-braces -- an entry whose identity points
    elsewhere is already dropped by the foreign-target exclusion before this
    gate sees it -- but the exclusion is one condition, and a gate that trusted
    the manifest would be exploitable the moment that condition loosened.
    """
    _series(store, 8)
    # Hand-place a manifest under the wrong class directory, as a copy would.
    stray = backup_prefix(CLIENT, DATABASE, "template-pack", "misfiled-1")
    root = store["storage_dir"] / stray
    root.mkdir(parents=True, exist_ok=True)
    manifest = BackupManifest(
        backup_id="misfiled-1",
        backup_class="full",  # says full, stored under template-pack
        target_identity={
            "client_id": CLIENT,
            "database_name": DATABASE,
            "schema_name": SCHEMA,
            "target_label": LABEL,
        },
        provenance={"created_by": "operator"},
        timestamps={"started_at": BASE.isoformat()},
    )
    manifest.signature = sign_manifest(manifest.to_payload(), mode="none")
    raw = canonical_bytes(manifest.to_payload())
    (root / MANIFEST_KEY).write_bytes(raw)
    (root / MANIFEST_CHECKSUM_KEY).write_bytes(sha256_hex(raw).encode("utf-8"))

    plan = _plan(store, allow_bulk=True)

    gate = next(g for g in plan.gates if g.id == prune_mod.GATE_PREFIX_INTEGRITY)
    assert not gate.ok
    assert "misfiled-1" in gate.detail


# ---------------------------------------------------------------------------
# defects found by review — each of these shipped
# ---------------------------------------------------------------------------


def test_two_prefixes_sharing_an_id_are_both_held(store):
    """A dict keyed on backup_id collapsed duplicates and emptied one hold set.

    ``{item.backup_id: item}`` is last-writer-wins, so every rule's output
    landed on one candidate and the other kept **no holds at all** -- deletable
    with zero flags and every gate green. The safety model's core premise, that
    a bug must produce a positively-empty hold set rather than merely forget to
    add, was defeated by a dict comprehension.
    """
    _series(store, 5)
    _add(store, "dupe-1", when=(BASE - timedelta(days=90)).isoformat())
    _add(
        store,
        "dupe-1",
        when=(BASE - timedelta(days=91)).isoformat(),
        backup_class="template-pack",
    )

    plan = _plan(store, allow_bulk=True)
    shared = [i for i in plan.candidates if i.backup_id == "dupe-1"]

    assert len(shared) == 2, "the fixture did not produce two candidates"
    for candidate in shared:
        assert candidate.holds, f"{candidate.storage_prefix} had an empty hold set"
        assert prune_mod.HOLD_DUPLICATE_BACKUP_ID in candidate.holds


def test_damaged_prefixes_do_not_shrink_the_retention_window(store):
    """`--ignore-damaged` must not become `--release keep_last`.

    Subtracting damaged prefixes from the window removed protection from
    healthy backups without giving any to the damaged ones: a damaged
    candidate has no date, so it is not in the ranking and cannot occupy a
    slot. With keep_last 3 and four damaged prefixes the window protected
    nothing at all.
    """
    ids = _series(store, 8)
    for index in range(4):
        prefix = _add(
            store,
            f"broken-{index}",
            when=(BASE - timedelta(days=50 + index)).isoformat(),
        )
        (store["storage_dir"] / prefix / MANIFEST_KEY).write_text("{ nope")

    plan = _plan(store, ignore_damaged=True, allow_bulk=True)

    protected = {
        i.backup_id for i in plan.candidates if prune_mod.HOLD_KEEP_LAST in i.holds
    }
    assert protected == set(ids[-3:]), protected


def test_reconcile_refuses_a_prefix_that_is_not_a_backup_of_this_target(store):
    """Receipt content is evidence, not instruction.

    `detail["prefixes"]` is a string written by an earlier process and reaches
    reconciliation as a delete path. An intent naming the database root erased
    an entire store of backups that no rule made deletable, because
    `_prefix_state` saw no manifest at that level and called it half-deleted.
    Reconciliation runs before every gate, so the check has to live here.
    """
    _series(store, 8)
    store["settings"]["keep_last"] = 30  # nothing is eligible
    before = tree_digest(store["storage_dir"])

    for bad in ("acme/prod", "../../etc", "acme/prod/full", "other/db/full/x"):
        _receipt(
            store,
            operation="backup_prune",
            status=STATUS_SUCCEEDED,
            detail={
                "phase": "intent",
                "prune_id": f"evil-{abs(hash(bad))}",
                "prefixes": [bad],
            },
        )

    prune_mod.reconcile_interrupted(CFG, store["settings"], actor=ACTOR)

    assert tree_digest(store["storage_dir"]) == before


def test_reconcile_ignores_another_targets_intent(store):
    """Two schemas on one database legitimately share a receipts directory.

    `config_dir` is keyed on client and database only, while a target label
    also carries the schema -- so reconciling a sibling's intent would bypass
    the foreign-target exclusion, `prefix_integrity`, and the typed label.
    """
    _series(store, 8)
    victim = _add(store, "sibling-1", when=(BASE - timedelta(days=90)).isoformat())
    (store["storage_dir"] / victim / "tapdb.dump").unlink()  # genuinely partial

    write_receipt(
        store["config_dir"] / "backups" / "receipts",
        operation="backup_prune",
        status=STATUS_SUCCEEDED,
        actor=ACTOR,
        target_label="acme/prod/tapdb_other@tapdb",  # a different schema
        detail={
            "phase": "intent",
            "prune_id": "prune-sibling",
            "prefixes": [victim],
        },
        now=BASE,
    )

    reconciled = prune_mod.reconcile_interrupted(CFG, store["settings"], actor=ACTOR)

    assert reconciled == []
    assert (store["storage_dir"] / victim / MANIFEST_KEY).exists()


def test_reconcile_refuses_while_the_receipt_chain_is_broken(store):
    """It acts destructively on receipt content and runs before every gate.

    Deferring the chain check to `_gate_receipt_chain` would make that gate
    decorative for this path -- the deletions have already happened by the time
    it runs.
    """
    _series(store, 8)
    victim = _add(store, "chain-1", when=(BASE - timedelta(days=90)).isoformat())
    (store["storage_dir"] / victim / "tapdb.dump").unlink()
    _receipt(
        store,
        operation="backup_prune",
        status=STATUS_SUCCEEDED,
        detail={"phase": "intent", "prune_id": "p1", "prefixes": [victim]},
    )
    _receipt(store, operation=OPERATION_CREATE, status=STATUS_SUCCEEDED)

    tampered = sorted((store["config_dir"] / "backups" / "receipts").glob("*.json"))[-1]
    tampered.chmod(0o600)
    tampered.write_text('{"tampered": true}')

    reconciled = prune_mod.reconcile_interrupted(CFG, store["settings"], actor=ACTOR)

    assert reconciled == []
    assert (store["storage_dir"] / victim / MANIFEST_KEY).exists()


def test_reconcile_leaves_a_prefix_it_cannot_read(store, monkeypatch):
    """Unreadable is not half-deleted.

    `_hold_damaged` states the rule for planning -- an unreadable manifest
    means nothing is known, and unknown is not grounds for deletion.
    Reconciliation must not apply the opposite rule to the same condition; a
    throttle or an expiring credential would otherwise delete intact backups.
    """
    _series(store, 8)
    intact = _add(store, "unreadable-1", when=(BASE - timedelta(days=90)).isoformat())
    _receipt(
        store,
        operation="backup_prune",
        status=STATUS_SUCCEEDED,
        detail={"phase": "intent", "prune_id": "p-unread", "prefixes": [intact]},
    )

    real_load = service._load_manifest

    def _flaky(storage, prefix):
        if prefix == intact:
            raise RuntimeError("SlowDown: throttled")
        return real_load(storage, prefix)

    monkeypatch.setattr(service, "_load_manifest", _flaky)

    prune_mod.reconcile_interrupted(CFG, store["settings"], actor=ACTOR)

    assert (store["storage_dir"] / intact / MANIFEST_KEY).exists()


def test_a_refused_apply_writes_no_receipt_at_all(store):
    """The digest must cover receipts too, not only the storage tree.

    An intent written before the typed-label check would leave a dangling
    intent on every refusal -- and the *next* prune's reconciliation would then
    finish exactly the deletions the refusal was protecting against.
    """
    from daylily_tapdb.backup.errors import RestoreConfirmationError

    _series(store, 8)
    receipts_dir = store["config_dir"] / "backups" / "receipts"

    with pytest.raises(RestoreConfirmationError):
        _apply(store, confirm_target="wrong/label@nope", allow_bulk=True)

    assert read_receipts(receipts_dir) == []
    assert prune_mod.dangling_intents(read_receipts(receipts_dir)) == []


def test_a_mid_delete_failure_leaves_the_intent_dangling(store, monkeypatch):
    """The abort must not erase its own detector.

    Writing an outcome on failure removes the run from `dangling_intents`, so
    the half-deleted prefix is never reconciled by any later run -- making
    permanent precisely the state the abort exists to prevent.
    """
    _series(store, 12)
    storage = service.storage_for(store["settings"])
    real_delete = storage.delete
    seen = {"n": 0}

    def _fail_after_two(key):
        seen["n"] += 1
        if seen["n"] > 2:
            raise OSError("storage went away")
        return real_delete(key)

    monkeypatch.setattr(service, "storage_for", lambda _s: storage)
    monkeypatch.setattr(storage, "delete", _fail_after_two)

    with pytest.raises(prune_mod.PruneRefusedError):
        _apply(store, allow_bulk=True)

    receipts = read_receipts(store["config_dir"] / "backups" / "receipts")
    phases = [
        (r.detail or {}).get("phase") for r in receipts if r.operation == "backup_prune"
    ]
    assert phases == ["intent"], phases
    assert len(prune_mod.dangling_intents(receipts)) == 1


def test_prefixes_are_deleted_oldest_first(store, monkeypatch):
    """An interruption must leave a superset of the intended survivors."""
    _series(store, 12)
    storage = service.storage_for(store["settings"])
    order: list[str] = []
    real_delete = storage.delete

    def _spy(key):
        prefix = key.rsplit("/", 1)[0]
        if prefix not in order:
            order.append(prefix)
        return real_delete(key)

    monkeypatch.setattr(service, "storage_for", lambda _s: storage)
    monkeypatch.setattr(storage, "delete", _spy)

    plan = _plan(store, allow_bulk=True)
    _apply(store, allow_bulk=True)

    dates = {i.storage_prefix: i.created_at for i in plan.deletable}
    seen = [dates[p] for p in order if p in dates]
    assert seen == sorted(seen), "prefixes were not deleted oldest first"
    assert len(seen) > 1, "the spy saw too few prefixes to prove ordering"


def test_a_refused_plan_is_not_a_success(store):
    """`PruneResult.ok` must account for a refused plan, not only apply failures.

    `failed` is populated on the apply path alone, so keying solely on it made
    a *dry run* blocked by `storage_reclaims`, `receipt_chain` or
    `retention_sane` exit 0 -- and no health check reports prune gate state, so
    a scheduled prune could be refused every night, forever, in silence.
    """
    _series(store, 40)  # over the ceiling

    result = prune_mod.prune_backups(CFG, store["settings"], actor=ACTOR)

    assert result.dry_run is True
    assert not result.plan.ok
    assert not result.ok, "a refused plan reported success"

    # And a clean plan still succeeds.
    clean = prune_mod.prune_backups(
        CFG, store["settings"], actor=ACTOR, allow_bulk=True
    )
    assert clean.plan.ok
    assert clean.ok


def test_reclaimed_bytes_is_none_when_nothing_was_actually_freed(store, monkeypatch):
    """Delete markers free no space, and the number must say so.

    Reporting bytes "reclaimed" on a versioned bucket is worse than reporting
    nothing: it is a figure someone will put in a capacity report.
    """
    _series(store, 8)
    storage = service.storage_for(store["settings"])
    monkeypatch.setattr(
        storage,
        "deletion_capability",
        lambda: {
            "reclaims": False,
            "reason": "versioning is Enabled",
            "versioning": "Enabled",
            "object_lock": False,
        },
    )
    monkeypatch.setattr(service, "storage_for", lambda _s: storage)

    result = _apply(store, allow_bulk=True, allow_delete_markers=True)

    assert result.deleted, "nothing was deleted, so the assertion proves nothing"
    assert result.reclaimed_bytes is None

    # A backend that genuinely reclaims reports a real figure.
    monkeypatch.undo()
    _series(store, 8, start_days_ago=200)
    second = _apply(store, allow_bulk=True)
    assert second.reclaimed_bytes is not None and second.reclaimed_bytes > 0


def test_releasing_undated_does_not_empty_a_store_with_no_timestamps(store):
    """The floor must hold in the shape the release flag is documented for.

    `keep_last` and `newest_successful` both read the ranking. Scoped to
    *dated* entries only, an all-undated store with `--release undated` has an
    empty ranking, so both fire zero times -- and the promise that "no
    combination of releases can leave a target with nothing" is false in
    exactly the receipt-poor legacy store the flag exists for.

    Twelve undated backups, keep_last 7: seven survive by the window, not one
    by an accident of sort order.
    """
    store["settings"]["keep_last"] = 7
    for index in range(12):
        _add(store, f"legacy-{index:02d}", when="")

    plan = _plan(store, released=(prune_mod.HOLD_UNDATED,), allow_bulk=True)

    protected = {
        i.backup_id for i in plan.candidates if prune_mod.HOLD_KEEP_LAST in i.holds
    }
    assert len(protected) == 7, sorted(protected)
    assert len(plan.retained) >= 7
    assert any(prune_mod.HOLD_NEWEST_SUCCESSFUL in i.holds for i in plan.candidates), (
        "the newest-successful floor did not fire"
    )
    # And it really is deleting the rest, so this is not passing vacuously.
    assert len(plan.deletable) == 5
