"""Retention enforcement — the only operation here that destroys recoverability.

Its own module, deliberately. Everything else in this package either reads or
adds; this deletes, and it deserves a file a reviewer can hold in their head.

**The safety model is holds, not selection.** A backup is deletable *only when
its hold set is empty*. Nothing is ever chosen for deletion — things fail to be
protected. The difference matters because of how the two shapes fail: a bug in
"select what to delete" quietly adds something to the kill list, while a bug
here has to produce a positively-empty hold set. Forgetting to add a rule is
still a bug, which is why ``HOLD_IDS`` is the registry the implementation
iterates and the test suite asserts closure over it.

**Holds are strictly additive.** Every rule runs against every candidate and
contributes to a set. No precedence, no early return, no "resolved, stop
looking". The one place precedence exists is *inside* provenance resolution,
deciding which evidence answers "who created this" — and even there a weaker
source may only add a hold, never cancel one a stronger source declined to add.

Three things are excluded from the candidate set entirely rather than held,
because **a hold still occupies a ``keep_last`` slot**: another target's
backups, rehearsal evidence, and anything outside this target's prefix. With
``keep_last: 7`` and a sibling target backing up hourly, seven held-but-ranked
foreign backups would fill the window and leave *this* target's entire history
with an empty hold set.

Gates are different from holds: a gate aborts the whole run before a single
byte is deleted, because it describes a condition under which no deletion
decision can be trusted.
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Optional

from daylily_tapdb.backup import service
from daylily_tapdb.backup.errors import (
    BackupError,
    RestoreConfirmationError,
    RestoreStageStaleError,
)
from daylily_tapdb.backup.manifest import (
    BACKUP_CLASS_PROVIDER_SNAPSHOT,
    PROVENANCE_OPERATOR,
    PROVENANCE_RESTORE,
    BackupManifest,
    canonical_bytes,
    sha256_hex,
)
from daylily_tapdb.backup.receipts import (
    OPERATION_PRUNE,
    OPERATION_RESTORE,
    PRUNE_PHASE_INTENT,
    PRUNE_PHASE_OUTCOME,
    STATUS_FAILED,
    STATUS_SUCCEEDED,
    SURFACE_CLI,
    Actor,
    Receipt,
    _parse_created_at,
    read_head,
    read_receipts,
    verify_receipt_chain,
    write_receipt,
)
from daylily_tapdb.backup.storage import (
    MANIFEST_CHECKSUM_KEY,
    MANIFEST_KEY,
    REHEARSALS_SEGMENT,
    backup_prefix,
    database_prefix,
)

# ---------------------------------------------------------------------------
# Holds
# ---------------------------------------------------------------------------

HOLD_UNDATED = "undated"
HOLD_UNPARSEABLE_CREATED_AT = "unparseable_created_at"
HOLD_FUTURE_DATED = "future_dated"
HOLD_CHECKSUM_MISMATCH = "checksum_mismatch"
HOLD_KEEP_LAST = "keep_last"
HOLD_NEWEST_SUCCESSFUL = "newest_successful"
HOLD_ONLY_COPY_OF_TARGET = "only_copy_of_target"
HOLD_ONLY_COPY_OF_CLASS = "only_copy_of_class"
HOLD_SAFETY_BACKUP = "safety_backup"
HOLD_RECENTLY_RESTORED_FROM = "recently_restored_from"
HOLD_REHEARSAL_EVIDENCE = "rehearsal_evidence"
HOLD_PROVENANCE_UNKNOWN = "provenance_unknown"
HOLD_PROVIDER_SNAPSHOT_REFERENCE = "provider_snapshot_reference"
HOLD_DAMAGED = "damaged"
HOLD_DUPLICATE_BACKUP_ID = "duplicate_backup_id"

#: Every hold the implementation may apply. This is a registry, not
#: documentation: ``_HOLD_RULES`` is keyed by these ids and iterated, so a rule
#: that is not registered here cannot run at all. That closes the gap where a
#: hold applied under a literal string is invisible to both the id list and the
#: parametrised tests.
HOLD_IDS: tuple[str, ...] = (
    HOLD_UNDATED,
    HOLD_UNPARSEABLE_CREATED_AT,
    HOLD_FUTURE_DATED,
    HOLD_CHECKSUM_MISMATCH,
    HOLD_KEEP_LAST,
    HOLD_NEWEST_SUCCESSFUL,
    HOLD_ONLY_COPY_OF_TARGET,
    HOLD_ONLY_COPY_OF_CLASS,
    HOLD_SAFETY_BACKUP,
    HOLD_RECENTLY_RESTORED_FROM,
    HOLD_REHEARSAL_EVIDENCE,
    HOLD_PROVENANCE_UNKNOWN,
    HOLD_PROVIDER_SNAPSHOT_REFERENCE,
    HOLD_DAMAGED,
    HOLD_DUPLICATE_BACKUP_ID,
)

#: Holds an operator may switch off. Deliberately a small subset.
#:
#: ``--release keep_last`` would collapse a 90-backup history to roughly one
#: per class in a single command, and ``safety_backup``,
#: ``only_copy_of_target``, ``only_copy_of_class`` and ``newest_successful``
#: are the floor the whole model rests on — none of them has a flag at all.
#: The four here are the ones where an operator can reasonably know better
#: than the rule: stale rehearsal evidence, a receipt-poor store, and manifests
#: whose timestamps were never written or are unreadable.
RELEASABLE_HOLDS: tuple[str, ...] = (
    HOLD_REHEARSAL_EVIDENCE,
    HOLD_PROVENANCE_UNKNOWN,
    HOLD_UNDATED,
    HOLD_UNPARSEABLE_CREATED_AT,
)

# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------

GATE_RETENTION_SANE = "retention_sane"
GATE_RECEIPT_CHAIN = "receipt_chain"
GATE_NO_DAMAGED = "no_damaged"
GATE_STORAGE_RECLAIMS = "storage_reclaims"
GATE_POLICY = "policy"
GATE_PREFIX_INTEGRITY = "prefix_integrity"
GATE_DELETE_CEILING = "delete_ceiling"

GATE_IDS: tuple[str, ...] = (
    GATE_RETENTION_SANE,
    GATE_RECEIPT_CHAIN,
    GATE_NO_DAMAGED,
    GATE_STORAGE_RECLAIMS,
    GATE_POLICY,
    GATE_PREFIX_INTEGRITY,
    GATE_DELETE_CEILING,
)

#: A plan that would remove more than this many backups, or more than this
#: fraction of the store, refuses without ``--allow-bulk``.
#:
#: The typed ``--confirm-target`` label is the control for an operator at a
#: keyboard, and it is worth nothing once this runs from cron -- it becomes a
#: constant in a config file. A hand-edited ``keep_last: 1`` would then delete
#: an entire history unattended with every other gate satisfied. This is the
#: one control that still means something after the human is removed.
BULK_DELETE_COUNT = 25
BULK_DELETE_FRACTION = 0.5

#: How far into the future a ``started_at`` may sit before it is treated as a
#: bad clock rather than a real timestamp. A container before NTP sync or a VM
#: resumed from a snapshot can write one; a legitimately future backup cannot
#: exist.
FUTURE_SKEW_TOLERANCE = timedelta(hours=1)


class PruneRefusedError(BackupError):
    """A gate refused the run. Nothing was deleted."""

    code = "prune_refused"


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass
class Candidate:
    """One backup prune has considered, and why it did or did not survive."""

    backup_id: str
    backup_class: str
    storage_prefix: str
    created_at: Optional[datetime]
    raw_created_at: Optional[str]
    status: str
    bytes: int
    manifest: Optional[BackupManifest] = None
    #: Whether the stored manifest bytes match the detached `manifest.sha256`.
    #: None when it could not be established.
    checksum_ok: Optional[bool] = None
    holds: set[str] = field(default_factory=set)

    @property
    def deletable(self) -> bool:
        return not self.holds

    def to_payload(self) -> dict[str, Any]:
        return {
            "backup_id": self.backup_id,
            "backup_class": self.backup_class,
            "storage_prefix": self.storage_prefix,
            "created_at": self.raw_created_at,
            "status": self.status,
            "bytes": self.bytes,
            "holds": sorted(self.holds),
        }


@dataclass(frozen=True)
class GateResult:
    """One gate's verdict. A failed gate aborts before any deletion."""

    id: str
    ok: bool
    detail: str = ""
    escape: Optional[str] = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "ok": self.ok,
            "detail": self.detail,
            "escape": self.escape,
        }


@dataclass(frozen=True)
class PrunePlan:
    """What a prune would remove, and why everything else survives."""

    target_label: str
    keep_last: int
    candidates: list[Candidate] = field(default_factory=list)
    gates: list[GateResult] = field(default_factory=list)
    excluded: list[dict[str, Any]] = field(default_factory=list)
    released: list[str] = field(default_factory=list)
    #: Covers everything that would change the outcome, including the receipt
    #: head -- see ``plan_fingerprint`` in the module docstring of the apply
    #: path below.
    plan_fingerprint: str = ""
    receipt_head_sequence: int = 0

    @property
    def blocking(self) -> list[GateResult]:
        return [gate for gate in self.gates if not gate.ok]

    @property
    def ok(self) -> bool:
        return not self.blocking

    @property
    def deletable(self) -> list[Candidate]:
        return [item for item in self.candidates if item.deletable]

    @property
    def retained(self) -> list[Candidate]:
        return [item for item in self.candidates if not item.deletable]

    def holds_by_id(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for candidate in self.retained:
            for hold in candidate.holds:
                counts[hold] = counts.get(hold, 0) + 1
        return dict(sorted(counts.items()))

    def to_payload(self) -> dict[str, Any]:
        return {
            "target_label": self.target_label,
            "keep_last": self.keep_last,
            "ok": self.ok,
            "plan_fingerprint": self.plan_fingerprint,
            "receipt_head_sequence": self.receipt_head_sequence,
            "released": self.released,
            "gates": [gate.to_payload() for gate in self.gates],
            "excluded": self.excluded,
            "deletable": [item.to_payload() for item in self.deletable],
            "retained": [item.to_payload() for item in self.retained],
            "holds": self.holds_by_id(),
            "reclaimable_bytes": sum(item.bytes for item in self.deletable),
        }


@dataclass(frozen=True)
class PruneResult:
    """Outcome of an applied prune."""

    prune_id: str
    target_label: str
    dry_run: bool
    plan: PrunePlan
    deleted: list[dict[str, Any]] = field(default_factory=list)
    failed: list[dict[str, Any]] = field(default_factory=list)
    reclaimed_bytes: Optional[int] = None
    intent_receipt_id: Optional[str] = None
    outcome_receipt_id: Optional[str] = None
    reconciled: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Whether this run had nothing to report.

        A refused *plan* counts. `failed` is only ever populated on the apply
        path, so keying solely on it meant a dry run blocked by
        `storage_reclaims` or `retention_sane` exited 0 -- and since no health
        check reports prune gate state, a scheduled prune could be refused
        every night, forever, in silence.
        """
        return not self.failed and self.plan.ok

    def to_payload(self) -> dict[str, Any]:
        return {
            "prune_id": self.prune_id,
            "target_label": self.target_label,
            "dry_run": self.dry_run,
            "ok": self.ok,
            "deleted": self.deleted,
            "failed": self.failed,
            "reclaimed_bytes": self.reclaimed_bytes,
            "intent_receipt_id": self.intent_receipt_id,
            "outcome_receipt_id": self.outcome_receipt_id,
            "reconciled": self.reconciled,
            "plan": self.plan.to_payload(),
        }


# ---------------------------------------------------------------------------
# Hold rules
#
# Each takes the whole context and returns the set of backup ids it protects.
# They are independent by construction: no rule can see another's result, so
# none can short-circuit another, and the union is the hold set.
# ---------------------------------------------------------------------------


@dataclass
class _Context:
    """Everything the hold rules read. Assembled once, never mutated by them."""

    cfg: dict[str, Any]
    settings: dict[str, Any]
    keep_last: int
    candidates: list[Candidate]
    damaged: set[str]
    receipts: list[Receipt]
    rehearsed_ids: set[str]
    now: datetime

    @property
    def ranked(self) -> list[Candidate]:
        """Every rankable candidate, newest first, with undated entries last.

        Two tiers, deliberately. Dated entries come first, so undated ones can
        never displace a real backup from the window -- 30 undated manifests
        with ``keep_last: 30`` must not make every dated backup deletable.
        But they are still *present*, so ``keep_last`` and
        ``newest_successful`` have something to protect when a store has no
        usable timestamps at all and ``undated`` has been released.

        Damaged candidates are excluded: they carry no readable manifest, so
        nothing about their content or order is known, and ``damaged`` holds
        them anyway.
        """
        undated = sorted(
            [
                item
                for item in self.candidates
                if item.created_at is None and item.backup_id not in self.damaged
            ],
            key=lambda item: item.backup_id,
            reverse=True,
        )
        return self.dated + undated

    @property
    def dated(self) -> list[Candidate]:
        """Candidates with a usable timestamp, newest first.

        Ranking is done on a *parsed* datetime, never on the raw string
        ``list_backups`` sorts by. That sort is a plain string compare over an
        unvalidated manifest field, and both directions bite: a missing value
        sorts oldest and gets deleted first, while ``2099-...`` from a bad
        clock, or the literal ``"unknown"`` (``'u' > '2'``), sorts *newest* and
        evicts every real backup from the window.
        """
        horizon = self.now + FUTURE_SKEW_TOLERANCE
        return sorted(
            [
                item
                for item in self.candidates
                if item.created_at is not None and item.created_at <= horizon
            ],
            key=lambda item: (item.created_at, item.backup_id),
            reverse=True,
        )


def _hold_undated(ctx: _Context) -> set[str]:
    """No ``started_at`` at all.

    Held *outside* the window rather than ranked, so they never consume a
    ``keep_last`` slot. If they did, 30 undated manifests with ``keep_last:
    30`` would fill the window and make every real dated backup deletable --
    exactly inverted.
    """
    return {
        item.backup_id
        for item in ctx.candidates
        if item.created_at is None and not item.raw_created_at
    }


def _hold_unparseable_created_at(ctx: _Context) -> set[str]:
    """A ``started_at`` that is present but is not a timestamp."""
    return {
        item.backup_id
        for item in ctx.candidates
        if item.created_at is None and item.raw_created_at
    }


def _hold_future_dated(ctx: _Context) -> set[str]:
    """A timestamp far enough ahead that it can only be a bad clock.

    Left in the ranking these sit at the top permanently: an hourly cron
    through a seven-hour skew window with ``keep_last: 7`` fills every slot
    with 2099-dated manifests and leaves every real backup with an empty hold
    set. No hand-editing required.
    """
    horizon = ctx.now + FUTURE_SKEW_TOLERANCE
    return {
        item.backup_id
        for item in ctx.candidates
        if item.created_at is not None and item.created_at > horizon
    }


def _hold_checksum_mismatch(ctx: _Context) -> set[str]:
    """Manifest bytes that do not match their detached ``manifest.sha256``.

    Every other reader merely *displays* manifest content. Prune acts
    destructively on it -- ranking, class, and target all come from there -- so
    a manifest whose bytes were altered must not be trusted to decide what
    survives. ``_load_manifest`` never checks this; only ``verify_backup``
    does.
    """
    return {item.backup_id for item in ctx.candidates if item.checksum_ok is not True}


def _hold_keep_last(ctx: _Context) -> set[str]:
    """The newest ``keep_last`` backups, by parsed date.

    Global, not per-class. With 100 recent ``full`` backups and 5 older
    template packs, a global window evicts four of the five packs and
    ``only_copy_of_class`` saves exactly one. That is a real trade, made
    deliberately: per-class retention is a config change and a separate
    decision, not something to leave ambiguous here.

    **Damaged prefixes do not reduce this budget.** Subtracting them was
    strictly less safe than it sounds: a damaged candidate has no date, so it
    is not in ``dated`` and cannot occupy a slot -- the subtraction removed
    protection from healthy backups without giving any to the damaged ones.
    With ``keep_last: 7`` and eight damaged prefixes the window protected
    *nothing*, so ``--ignore-damaged`` quietly became the ``--release
    keep_last`` flag this module deliberately refuses to ship. Damaged
    prefixes are held by ``damaged`` and are simply not part of the ranking.
    """
    return {item.backup_id for item in ctx.ranked[: ctx.keep_last]}


def _hold_newest_successful(ctx: _Context) -> set[str]:
    """The most recent backup that reports itself complete.

    A floor of one, independent of ``keep_last``. It reads ``ranked`` rather
    than ``dated`` precisely so the claim holds: in an all-undated store with
    ``--release undated`` there are no dated entries at all, and a rule scoped
    to ``dated`` would fire zero times -- leaving the "no combination of
    releases can empty a target" promise false in exactly the shape the
    release flag is documented for.
    """
    for item in ctx.ranked:
        if item.status == "complete":
            return {item.backup_id}
    return set()


def _hold_only_copy_of_target(ctx: _Context) -> set[str]:
    """Never delete the last backup of a target, whatever else is released."""
    if len(ctx.candidates) <= 1:
        return {item.backup_id for item in ctx.candidates}
    return set()


def _hold_only_copy_of_class(ctx: _Context) -> set[str]:
    """The newest of each class survives.

    The floor that makes a global ``keep_last`` window tolerable: a burst of
    ``full`` backups cannot evict the only template pack or the only recorded
    provider snapshot.
    """
    held: set[str] = set()
    seen: set[str] = set()
    # `ranked` first (dated, then undated), then anything left -- future-dated
    # and damaged entries included. A class existing *only* as future-dated
    # would otherwise get no class hold, which is safe today only because
    # `future_dated` happens not to be releasable.
    for item in list(ctx.ranked) + list(ctx.candidates):
        if item.backup_class not in seen:
            seen.add(item.backup_class)
            held.add(item.backup_id)
    return held


def _hold_safety_backup(ctx: _Context) -> set[str]:
    """Pre-restore safety backups -- the last copy of production if a restore
    degraded.

    Resolution runs three sources, and every one of them may only *add*. There
    is no precedence that can conclude "not a safety backup" and stop the
    others running: a failed in-place restore leaves a backup whose own create
    receipt says ``operator`` and which no restore receipt names, so a
    precedence reading would give it an empty hold set and delete it.

    1. Structured ``manifest.provenance`` -- authoritative, and present on
       everything written since it shipped.
    2. A restore receipt naming it as ``safety_backup_id`` -- covers manifests
       written before the field existed.
    3. The English note ``manifest.timestamps.note`` -- a regex, and therefore
       hold-only. It may create a hold; it may never remove one.
    """
    held: set[str] = set()

    for item in ctx.candidates:
        manifest = item.manifest
        if manifest is None:
            continue
        if (manifest.provenance or {}).get("created_by") == PROVENANCE_RESTORE:
            held.add(item.backup_id)
        note = str((manifest.timestamps or {}).get("note") or "")
        if "pre-restore safety backup" in note.lower():
            held.add(item.backup_id)

    for receipt in ctx.receipts:
        safety_id = (receipt.detail or {}).get("safety_backup_id")
        if safety_id:
            held.add(str(safety_id))

    return held


def _hold_recently_restored_from(ctx: _Context) -> set[str]:
    """Anything a restore has ever read from.

    Production now contains what that backup held; deleting it removes the
    only artifact matching the live data.
    """
    return {
        str(receipt.backup_id)
        for receipt in ctx.receipts
        if receipt.operation == OPERATION_RESTORE and receipt.backup_id
    }


def _hold_rehearsal_evidence(ctx: _Context) -> set[str]:
    """Backups with rehearsal evidence stored against them.

    ``rehearsals/<backup_id>/`` has no manifest, so it is invisible to every
    listing surface -- deleting the backup orphans it permanently with nothing
    to notice. Released, the evidence is deleted in the same operation.
    """
    return {
        item.backup_id for item in ctx.candidates if item.backup_id in ctx.rehearsed_ids
    }


def _hold_provenance_unknown(ctx: _Context) -> set[str]:
    """Manifests older than the receipt store can account for.

    Without this, a receipt-poor store -- a rebuilt host, or receipts kept
    locally while backups live in S3 -- would have no evidence either way and
    the safety-backup rules would silently pass. Holding the unknown decays to
    nothing as new manifests carry ``provenance``, which is why routine
    backups record ``operator`` positively rather than leaving it empty.
    """
    oldest_receipt = None
    for receipt in ctx.receipts:
        parsed = _parse_created_at(receipt.created_at)
        if parsed is not None and (oldest_receipt is None or parsed < oldest_receipt):
            oldest_receipt = parsed

    held: set[str] = set()
    for item in ctx.candidates:
        manifest = item.manifest
        provenance = (manifest.provenance if manifest else None) or {}
        if provenance.get("created_by") in (PROVENANCE_OPERATOR, PROVENANCE_RESTORE):
            continue
        # No receipts at all, or no date to compare: nothing can vouch for it.
        if oldest_receipt is None or item.created_at is None:
            held.add(item.backup_id)
        # Older than anything the receipt store remembers, so the absence of a
        # restore receipt naming it proves nothing.
        elif item.created_at < oldest_receipt:
            held.add(item.backup_id)
        # Otherwise the receipts *do* cover this period. A manifest with no
        # provenance that no restore receipt names is a routine backup, and
        # holding it would mean the unknown case never decays -- it would grow
        # forever alongside the store, which is the failure this rule is meant
        # to avoid rather than cause.
    return held


def _hold_provider_snapshot_reference(ctx: _Context) -> set[str]:
    """Provider-snapshot manifests are the only index of a real cluster snapshot.

    The stored artifact is a few kilobytes of ``snapshot-receipt.json``, so
    deleting one looks nearly free by byte count while destroying the pointer
    to a whole-cluster backup and leaving a billed AWS resource nobody can
    find. Its lifecycle belongs to AWS, the same way bucket provisioning does.
    """
    return {
        item.backup_id
        for item in ctx.candidates
        if item.backup_class == BACKUP_CLASS_PROVIDER_SNAPSHOT
    }


def _hold_duplicate_backup_id(ctx: _Context) -> set[str]:
    """Any id claimed by more than one prefix.

    Backup ids are minted to be unique, so a collision means the store holds
    something nobody minted there -- a copied directory, an aborted staging
    dir, or a damaged prefix whose basename happens to match. Prune cannot tell
    which of the two a rule meant, so it deletes neither.
    """
    seen: dict[str, int] = {}
    for item in ctx.candidates:
        seen[item.backup_id] = seen.get(item.backup_id, 0) + 1
    return {backup_id for backup_id, count in seen.items() if count > 1}


def _hold_damaged(ctx: _Context) -> set[str]:
    """Prefixes whose manifest could not be read.

    Never deletable: an unreadable manifest means nothing is known about the
    backup, and "unknown" is not grounds for deletion.
    """
    return {item.backup_id for item in ctx.candidates if item.backup_id in ctx.damaged}


#: Hold id -> rule. Keyed by ``HOLD_IDS`` and iterated, so a rule that is not
#: registered cannot execute and a registered id with no rule fails at import.
_HOLD_RULES: dict[str, Callable[[_Context], set[str]]] = {
    HOLD_UNDATED: _hold_undated,
    HOLD_UNPARSEABLE_CREATED_AT: _hold_unparseable_created_at,
    HOLD_FUTURE_DATED: _hold_future_dated,
    HOLD_CHECKSUM_MISMATCH: _hold_checksum_mismatch,
    HOLD_KEEP_LAST: _hold_keep_last,
    HOLD_NEWEST_SUCCESSFUL: _hold_newest_successful,
    HOLD_ONLY_COPY_OF_TARGET: _hold_only_copy_of_target,
    HOLD_ONLY_COPY_OF_CLASS: _hold_only_copy_of_class,
    HOLD_SAFETY_BACKUP: _hold_safety_backup,
    HOLD_RECENTLY_RESTORED_FROM: _hold_recently_restored_from,
    HOLD_REHEARSAL_EVIDENCE: _hold_rehearsal_evidence,
    HOLD_PROVENANCE_UNKNOWN: _hold_provenance_unknown,
    HOLD_PROVIDER_SNAPSHOT_REFERENCE: _hold_provider_snapshot_reference,
    HOLD_DAMAGED: _hold_damaged,
    HOLD_DUPLICATE_BACKUP_ID: _hold_duplicate_backup_id,
}

if set(_HOLD_RULES) != set(HOLD_IDS):  # pragma: no cover - import-time contract
    raise RuntimeError(
        "prune hold registry and HOLD_IDS disagree: "
        f"unregistered={sorted(set(HOLD_IDS) - set(_HOLD_RULES))} "
        f"unlisted={sorted(set(_HOLD_RULES) - set(HOLD_IDS))}"
    )

if not set(RELEASABLE_HOLDS) <= set(HOLD_IDS):  # pragma: no cover
    raise RuntimeError("RELEASABLE_HOLDS contains an unknown hold id")


def apply_holds(ctx: _Context, *, released: tuple[str, ...] = ()) -> None:
    """Run every registered rule and record the union on each candidate.

    Additive by construction: each rule sees only the context, never another
    rule's output, so none can cancel another. A released hold is skipped
    entirely rather than subtracted afterwards, so a release can never remove a
    hold some *other* rule also applied.
    """
    # Grouped, never keyed one-to-one.
    #
    # ``{item.backup_id: item}`` collapses duplicates -- last writer wins -- so
    # with two prefixes sharing an id (a hand-copied directory, an aborted
    # staging dir, a damaged prefix whose basename matches a live backup) every
    # rule's output landed on one candidate and the other kept an **empty hold
    # set**. The safety model's core premise, that a bug must produce a
    # positively-empty hold set rather than merely forget to add, was defeated
    # by a dict comprehension.
    #
    # Applying to every candidate sharing the id fails in the protective
    # direction: an ambiguous id over-holds rather than under-holds.
    by_id: dict[str, list[Candidate]] = {}
    for item in ctx.candidates:
        by_id.setdefault(item.backup_id, []).append(item)

    for hold_id in HOLD_IDS:
        if hold_id in released:
            continue
        for backup_id in _HOLD_RULES[hold_id](ctx):
            for candidate in by_id.get(backup_id, ()):
                candidate.holds.add(hold_id)


# ---------------------------------------------------------------------------
# Candidate assembly
# ---------------------------------------------------------------------------

#: Reasons an entry never reaches the ranking at all.
EXCLUDED_FOREIGN_TARGET = "foreign_target"
EXCLUDED_OFF_PREFIX = "off_prefix"


def _rehearsed_backup_ids(storage: Any, cfg: dict[str, Any]) -> set[str]:
    """Backup ids that have rehearsal evidence stored against them."""
    root = f"{database_prefix(str(cfg['client_id']), str(cfg['database_name']))}/{REHEARSALS_SEGMENT}"
    ids: set[str] = set()
    try:
        keys = storage.list_keys(root)
    except Exception:  # noqa: BLE001 - absent evidence is not an error
        return ids
    for key in keys:
        remainder = key[len(root) :].strip("/") if key.startswith(root) else ""
        if remainder:
            ids.add(remainder.split("/", 1)[0])
    return ids


def _checksum_matches(storage: Any, prefix: str) -> Optional[bool]:
    """Whether stored manifest bytes match their detached checksum.

    ``None`` when it cannot be established -- which the checksum hold treats
    the same as a mismatch, because prune must not act destructively on
    manifest content it cannot vouch for.
    """
    try:
        raw = storage.get_bytes(f"{prefix}/{MANIFEST_KEY}")
        recorded = storage.get_bytes(f"{prefix}/{MANIFEST_CHECKSUM_KEY}")
    except Exception:  # noqa: BLE001
        return None
    return sha256_hex(raw) == recorded.decode("utf-8").strip()


def _build_candidates(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    storage: Any,
    listing: Any,
) -> tuple[list[Candidate], list[dict[str, Any]]]:
    """Turn a listing into candidates, excluding what must never be ranked.

    Exclusion is not a hold. A hold still occupies a ``keep_last`` slot, so a
    sibling target's hourly backups would fill the window and leave this
    target's whole history unprotected -- the same shape as the undated trap,
    one level up.
    """
    label = service.target_label(cfg)
    expected_root = database_prefix(str(cfg["client_id"]), str(cfg["database_name"]))

    candidates: list[Candidate] = []
    excluded: list[dict[str, Any]] = []

    for entry in listing.entries:
        if not entry.storage_prefix.startswith(f"{expected_root}/"):
            excluded.append(
                {
                    "backup_id": entry.backup_id,
                    "reason": EXCLUDED_OFF_PREFIX,
                    "storage_prefix": entry.storage_prefix,
                }
            )
            continue
        # A missing or empty target_label is foreign, not matching. Legacy and
        # hand-written manifests default it to {}, and "no identity" must never
        # be read as "this identity".
        if (entry.target_label or "") != label:
            excluded.append(
                {
                    "backup_id": entry.backup_id,
                    "reason": EXCLUDED_FOREIGN_TARGET,
                    "target_label": entry.target_label,
                }
            )
            continue

        manifest = None
        try:
            manifest = service._load_manifest(storage, entry.storage_prefix)
        except Exception:  # noqa: BLE001 - surfaced as `damaged`
            manifest = None

        candidates.append(
            Candidate(
                backup_id=entry.backup_id,
                backup_class=entry.backup_class,
                storage_prefix=entry.storage_prefix,
                created_at=_parse_created_at(entry.created_at or ""),
                raw_created_at=entry.created_at,
                status=entry.status,
                bytes=entry.bytes,
                manifest=manifest,
                checksum_ok=_checksum_matches(storage, entry.storage_prefix),
            )
        )

    # Damaged prefixes have no readable manifest, so `list_backups` never puts
    # them in `entries`. They still have to be candidates: an unreadable
    # manifest is not grounds for deletion, and they count toward `keep_last`
    # because as far as anyone knows they are perfectly good backups whose
    # manifest read merely failed.
    for prefix in listing.damaged:
        if not prefix.startswith(f"{expected_root}/"):
            continue
        candidates.append(
            Candidate(
                backup_id=prefix.rsplit("/", 1)[-1],
                backup_class="unknown",
                storage_prefix=prefix,
                created_at=None,
                raw_created_at=None,
                status="damaged",
                bytes=0,
                manifest=None,
                checksum_ok=None,
            )
        )

    return candidates, excluded


# ---------------------------------------------------------------------------
# Gates
# ---------------------------------------------------------------------------


def _gate_retention_sane(keep_last: int) -> GateResult:
    """``keep_last`` must be at least 1.

    ``db_config._int`` does not validate this: a hand-edited ``0`` reaches
    settings verbatim, and a window of zero makes every backup deletable at
    once.
    """
    return GateResult(
        id=GATE_RETENTION_SANE,
        ok=keep_last >= 1,
        detail=(
            f"keep_last is {keep_last}"
            if keep_last >= 1
            else f"keep_last must be at least 1, got {keep_last}"
        ),
    )


def _gate_receipt_chain(chain: Any) -> GateResult:
    """The receipt chain must verify.

    Three holds -- ``safety_backup``, ``recently_restored_from``,
    ``provenance_unknown`` -- read receipts as evidence. If the chain does not
    verify, that evidence may have been edited, and the rules that protect the
    most important backups are the ones being fooled.
    """
    return GateResult(
        id=GATE_RECEIPT_CHAIN,
        ok=bool(chain.ok),
        detail=(
            f"receipt chain intact ({chain.count} receipts)"
            if chain.ok
            else "receipt chain does not verify: " + "; ".join(chain.findings)
        ),
    )


def _gate_no_damaged(damaged: list[str], *, ignore: bool) -> GateResult:
    """Refuse while any manifest is unreadable.

    Deleting oldest-first is only the safe direction when the set is complete.
    With 20 of 30 manifests unreadable, the ranking describes a third of the
    store and any window computed from it is fiction.
    """
    if ignore:
        return GateResult(
            id=GATE_NO_DAMAGED,
            ok=True,
            detail=f"{len(damaged)} damaged prefix(es) ignored by request",
        )
    return GateResult(
        id=GATE_NO_DAMAGED,
        ok=not damaged,
        detail=(
            "every manifest is readable"
            if not damaged
            else f"{len(damaged)} prefix(es) have an unreadable manifest"
        ),
        escape=None if not damaged else "--ignore-damaged",
    )


def _gate_storage_reclaims(
    storage: Any, *, allow_delete_markers: bool, allow_unknown: bool
) -> GateResult:
    """Deleting must actually free the bytes.

    Absence of the probe is *unknown*, never *safe*: TapDB is embedded by
    several hosts and a host-supplied backend need not implement it. Failing
    closed on an unknown backend is the correct default for a delete path.
    """
    probe = getattr(storage, "deletion_capability", None)
    if probe is None:
        return GateResult(
            id=GATE_STORAGE_RECLAIMS,
            ok=allow_unknown,
            detail="this storage backend cannot report deletion capability",
            escape="--allow-unknown-reclaim",
        )
    try:
        capability = probe()
    except Exception as exc:  # noqa: BLE001
        return GateResult(
            id=GATE_STORAGE_RECLAIMS,
            ok=allow_unknown,
            detail=f"deletion capability could not be determined: {exc}",
            escape="--allow-unknown-reclaim",
        )

    reclaims = capability.get("reclaims")
    reason = str(capability.get("reason") or "")

    if reclaims is True:
        return GateResult(id=GATE_STORAGE_RECLAIMS, ok=True, detail=reason)
    if reclaims is None:
        return GateResult(
            id=GATE_STORAGE_RECLAIMS,
            ok=allow_unknown,
            detail=reason,
            escape="--allow-unknown-reclaim",
        )
    # Object Lock has no escape: the bucket carries an externally declared
    # retention policy, and a TapDB-side prune can only conflict with it.
    if capability.get("object_lock") is True:
        return GateResult(id=GATE_STORAGE_RECLAIMS, ok=False, detail=reason)
    return GateResult(
        id=GATE_STORAGE_RECLAIMS,
        ok=allow_delete_markers,
        detail=reason,
        escape="--allow-delete-markers",
    )


def _gate_policy(cfg: dict[str, Any]) -> GateResult:
    """The target's safety policy must permit destructive operations."""
    from daylily_tapdb.backup import verify as verify_mod

    try:
        verify_mod._require_policy_allows(cfg, operation="prune")
    except Exception as exc:  # noqa: BLE001
        return GateResult(id=GATE_POLICY, ok=False, detail=str(exc))
    return GateResult(
        id=GATE_POLICY, ok=True, detail="policy permits destructive operations"
    )


def _gate_prefix_integrity(
    cfg: dict[str, Any], candidates: list[Candidate]
) -> GateResult:
    """Every candidate prefix must be one this target could have written.

    Recomputed from **cfg**, never from the manifest's own ``target_identity``:
    a foreign manifest recomputes to its own prefix and would pass a check that
    trusted it.
    """
    offenders: list[str] = []
    for item in candidates:
        expected = backup_prefix(
            str(cfg["client_id"]),
            str(cfg["database_name"]),
            item.backup_class,
            item.backup_id,
        )
        # A damaged prefix has no readable class; check only the tail.
        if item.backup_class == "unknown":
            if not item.storage_prefix.endswith(f"/{item.backup_id}"):
                offenders.append(item.storage_prefix)
        elif item.storage_prefix != expected:
            offenders.append(item.storage_prefix)

    return GateResult(
        id=GATE_PREFIX_INTEGRITY,
        ok=not offenders,
        detail=(
            "every candidate prefix recomputes from this target's identity"
            if not offenders
            else "prefix(es) do not recompute: " + ", ".join(offenders)
        ),
    )


def _gate_delete_ceiling(deletable: int, total: int, *, allow_bulk: bool) -> GateResult:
    """Refuse a plan that would remove an implausible share of the store."""
    ceiling = min(BULK_DELETE_COUNT, max(1, int(total * BULK_DELETE_FRACTION)))
    if deletable <= ceiling or allow_bulk:
        return GateResult(
            id=GATE_DELETE_CEILING,
            ok=True,
            detail=(
                f"{deletable} of {total} backup(s) would be deleted"
                + (" (bulk allowed)" if allow_bulk and deletable > ceiling else "")
            ),
        )
    return GateResult(
        id=GATE_DELETE_CEILING,
        ok=False,
        detail=(
            f"{deletable} of {total} backup(s) would be deleted, above the "
            f"ceiling of {ceiling}. This is the control that still means "
            "something once prune runs unattended."
        ),
        escape="--allow-bulk",
    )


# ---------------------------------------------------------------------------
# Planning
# ---------------------------------------------------------------------------


def _plan_fingerprint(
    *,
    target_label: str,
    keep_last: int,
    released: tuple[str, ...],
    deletable: list[Candidate],
    head_sequence: int,
) -> str:
    """Pin everything that would change what apply does.

    **The receipt head sequence is in here deliberately**, and it is the part
    that is not obvious. Re-reading the manifest before deleting proves
    nothing about a concurrent *restore*: a restore never modifies the manifest
    it reads. But it does write a receipt, so the head advancing between plan
    and apply is the signal that something happened which the plan did not
    account for -- including a restore that read from a backup this plan was
    about to delete.
    """
    payload = canonical_bytes(
        {
            "target_label": target_label,
            "keep_last": keep_last,
            "released": sorted(released),
            "deletable": sorted(item.backup_id for item in deletable),
            "head_sequence": head_sequence,
        }
    )
    return sha256_hex(payload)


def plan_prune(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    released: tuple[str, ...] = (),
    ignore_damaged: bool = False,
    allow_delete_markers: bool = False,
    allow_unknown_reclaim: bool = False,
    allow_bulk: bool = False,
    now: Optional[datetime] = None,
) -> PrunePlan:
    """Decide what would be deleted. Reads only; never mutates anything.

    Order matters: candidates are assembled and every hold applied *before*
    any gate is consulted, so the plan can report exactly what would go even
    when a gate refuses the run. An operator investigating a refusal needs to
    see the consequence they are being protected from.
    """
    for hold in released:
        if hold not in RELEASABLE_HOLDS:
            raise PruneRefusedError(
                f"{hold!r} cannot be released. Releasable holds: "
                + ", ".join(RELEASABLE_HOLDS),
                detail={"hold": hold, "releasable": list(RELEASABLE_HOLDS)},
            )

    moment = now or datetime.now(UTC)
    storage = service.storage_for(settings)
    keep_last = int(settings.get("keep_last") or 0)

    listing = service.list_backups(cfg, settings)
    candidates, excluded = _build_candidates(cfg, settings, storage, listing)

    receipts_dir = service.receipts_directory(settings)
    receipts = read_receipts(receipts_dir)
    head = read_head(receipts_dir) or {}
    chain = verify_receipt_chain(receipts, head=head or None)

    ctx = _Context(
        cfg=cfg,
        settings=settings,
        keep_last=keep_last,
        candidates=candidates,
        damaged={item.backup_id for item in candidates if item.status == "damaged"},
        receipts=receipts,
        rehearsed_ids=_rehearsed_backup_ids(storage, cfg),
        now=moment,
    )
    apply_holds(ctx, released=released)

    deletable = [item for item in candidates if item.deletable]
    gates = [
        _gate_retention_sane(keep_last),
        _gate_receipt_chain(chain),
        _gate_no_damaged(sorted(ctx.damaged), ignore=ignore_damaged),
        _gate_storage_reclaims(
            storage,
            allow_delete_markers=allow_delete_markers,
            allow_unknown=allow_unknown_reclaim,
        ),
        _gate_policy(cfg),
        _gate_prefix_integrity(cfg, candidates),
        _gate_delete_ceiling(len(deletable), len(candidates), allow_bulk=allow_bulk),
    ]

    head_sequence = int(head.get("sequence") or 0)
    return PrunePlan(
        target_label=service.target_label(cfg),
        keep_last=keep_last,
        candidates=candidates,
        gates=gates,
        excluded=excluded,
        released=sorted(released),
        plan_fingerprint=_plan_fingerprint(
            target_label=service.target_label(cfg),
            keep_last=keep_last,
            released=released,
            deletable=deletable,
            head_sequence=head_sequence,
        ),
        receipt_head_sequence=head_sequence,
    )


# ---------------------------------------------------------------------------
# Applying
# ---------------------------------------------------------------------------


def dangling_intents(receipts: list[Receipt]) -> list[dict[str, Any]]:
    """Prune intents with no matching outcome, oldest first."""
    intents: dict[str, Receipt] = {}
    outcomes: set[str] = set()
    for receipt in receipts:
        if receipt.operation != OPERATION_PRUNE:
            continue
        detail = receipt.detail or {}
        prune_id = str(detail.get("prune_id") or "")
        if not prune_id:
            continue
        if detail.get("phase") == PRUNE_PHASE_INTENT:
            intents[prune_id] = receipt
        elif detail.get("phase") == PRUNE_PHASE_OUTCOME:
            outcomes.add(prune_id)
    return [
        {"prune_id": prune_id, "receipt": receipt}
        for prune_id, receipt in sorted(intents.items(), key=lambda kv: kv[1].sequence)
        if prune_id not in outcomes
    ]


def _reconcilable_prefix(cfg: dict[str, Any], prefix: str) -> bool:
    """Whether a prefix named in a receipt is one this target could have written.

    Receipt content is **evidence, not instruction**. ``detail["prefixes"]``
    was written by an earlier process and reaches reconciliation as a string;
    reconciliation then deletes it. Without this check the field is an
    arbitrary delete path: an intent naming ``"acme/prod"`` erased an entire
    store of backups that no rule made deletable, because ``_prefix_state`` saw
    no ``manifest.json`` at that level and called it half-deleted.

    A reconcilable prefix is exactly four segments -- client, database, class,
    backup id -- rooted at this target and recomputing to ``backup_prefix``.
    That is the same rule ``_gate_prefix_integrity`` applies to planning, and
    it must apply here too because reconciliation runs *before* every gate.
    """
    parts = [segment for segment in prefix.split("/") if segment]
    if len(parts) != 4:
        return False
    client_id, database_name, backup_class, backup_id = parts
    if client_id != str(cfg["client_id"]) or database_name != str(cfg["database_name"]):
        return False
    try:
        return backup_prefix(client_id, database_name, backup_class, backup_id) == (
            prefix.strip("/")
        )
    except ValueError:
        return False


def reconcile_interrupted(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    actor: Any,
    now: Optional[datetime] = None,
) -> list[str]:
    """Finish and record any prune that started but never reported an outcome.

    Required, not optional. Receipts are immutable, so a dangling intent can
    never be cleared by editing anything -- which is why
    ``health.interrupted_prune`` is a *warning* rather than a failure: as a
    failure it would page forever with no remediation, and the only way to
    silence it would be destroying the audit trail.

    Writing the missing outcome is the only thing that clears it, and doing so
    on the next prune makes the condition self-healing in the normal course of
    operations. A prune that planned and applied without reconciling first
    would leave its predecessor's warning permanent and its half-finished
    prefixes unfinished.
    """
    storage = service.storage_for(settings)
    receipts_dir = service.receipts_directory(settings)
    label = service.target_label(cfg)
    reconciled: list[str] = []

    receipts = read_receipts(receipts_dir)

    # The chain is checked *here*, not only in the gates, because
    # reconciliation acts destructively on receipt content and runs before any
    # gate. Trusting an unverified chain to name deletion targets is exactly
    # what `_gate_receipt_chain` exists to prevent -- doing it earlier in the
    # same command would make that gate decorative.
    if not verify_receipt_chain(receipts, head=read_head(receipts_dir) or None).ok:
        return reconciled

    for entry in dangling_intents(receipts):
        prune_id = entry["prune_id"]
        # Receipts for one client/database are a single store, but a target
        # label also carries schema and database name -- two schemas legitimately
        # share a receipts directory. Reconciling another target's intent would
        # bypass the foreign-target exclusion, `_gate_prefix_integrity`, and the
        # typed label all at once.
        if (entry["receipt"].target_label or "") != label:
            continue
        planned = list((entry["receipt"].detail or {}).get("prefixes") or [])
        finished: list[str] = []
        remaining: list[str] = []
        untouched: list[str] = []
        rejected: list[str] = []

        for prefix in planned:
            if not _reconcilable_prefix(cfg, prefix):
                rejected.append(prefix)
                continue
            try:
                state = _prefix_state(storage, prefix)
            except Exception:  # noqa: BLE001
                remaining.append(prefix)
                continue

            if state == _PREFIX_UNKNOWN:
                # Could not be read, so nothing is known about it. `_hold_damaged`
                # states the rule for planning -- "an unreadable manifest means
                # nothing is known about the backup, and 'unknown' is not grounds
                # for deletion" -- and reconciliation must not apply the opposite
                # rule to the same condition. A throttle, a 5xx, or an expiring
                # credential all land here.
                remaining.append(prefix)
            elif state == _PREFIX_GONE:
                finished.append(prefix)
            elif state == _PREFIX_INTACT:
                # **Untouched, so leave it entirely alone.**
                #
                # The intent receipt is written *before* the first delete, so a
                # crash in that window leaves every planned prefix completely
                # intact. Treating "still has files" as "half-deleted" would
                # then delete the whole planned set here -- bypassing the
                # holds, the delete ceiling, the typed target label, and the
                # receipt-head staleness check that would have noticed a
                # restore in the meantime. A crash at the worst possible moment
                # would convert into an unconditional mass delete on the next
                # run.
                #
                # Reconciliation's job is to finish damage, not to resume a
                # decision. An intact prefix is handed back to normal planning,
                # which runs immediately after and re-evaluates every rule
                # against current state.
                untouched.append(prefix)
            else:
                # Genuinely half-deleted: finish it in the same order the live
                # path uses, so an interruption here is no worse than the first.
                try:
                    _delete_prefix_in_order(storage, prefix)
                    finished.append(prefix)
                except Exception:  # noqa: BLE001
                    remaining.append(prefix)

        write_receipt(
            receipts_dir,
            operation=OPERATION_PRUNE,
            status=STATUS_SUCCEEDED if not remaining else STATUS_FAILED,
            actor=actor,
            target_label=service.target_label(cfg),
            detail={
                "phase": PRUNE_PHASE_OUTCOME,
                "prune_id": prune_id,
                "reconciled": True,
                "finished": finished,
                "remaining": remaining,
                # Paths that do not name a backup of this target. Recorded
                # rather than silently dropped: a receipt claiming them is
                # either corruption or tampering, and both are worth seeing.
                "rejected": rejected,
                # Intact prefixes are recorded, not deleted -- the audit trail
                # should show that the interrupted run's intent was abandoned
                # rather than silently resumed.
                "untouched": untouched,
            },
            receipt_mirror=settings.get("receipt_mirror") or {},
            now=now,
        )
        reconciled.append(prune_id)

    return reconciled


#: What an interrupted prune left behind at one prefix.
_PREFIX_GONE = "gone"
_PREFIX_INTACT = "intact"
_PREFIX_PARTIAL = "partial"
_PREFIX_UNKNOWN = "unknown"


def _prefix_state(storage: Any, prefix: str) -> str:
    """Classify a prefix as untouched, part-deleted, or fully deleted.

    The distinction exists because "has files" does not mean "was being
    deleted". Deletion order is artifacts, then ``manifest.sha256``, then
    ``manifest.json`` -- so a prefix is untouched only when the manifest, its
    detached checksum, and every asset the manifest lists are all still there.
    Anything else has been started on.
    """
    keys = set(storage.list_keys(prefix))
    if not keys:
        return _PREFIX_GONE

    manifest_key = f"{prefix}/{MANIFEST_KEY}"
    checksum_key = f"{prefix}/{MANIFEST_CHECKSUM_KEY}"
    if manifest_key not in keys or checksum_key not in keys:
        return _PREFIX_PARTIAL

    try:
        manifest = service._load_manifest(storage, prefix)
    except Exception:  # noqa: BLE001
        # Unreadable, which is *not* the same as half-deleted. Returning
        # PARTIAL here deleted fully intact backups whenever a manifest read
        # failed transiently -- the exact three cases (`throttle`, `5xx`,
        # expiring credential) that `_hold_damaged` names as benign.
        return _PREFIX_UNKNOWN

    for asset in manifest.included_assets:
        if f"{prefix}/{asset.name}" not in keys:
            return _PREFIX_PARTIAL
    return _PREFIX_INTACT


def _delete_prefix_in_order(storage: Any, prefix: str) -> list[str]:
    """Delete one backup's keys in the order that survives an interruption.

    ``artifacts -> manifest.sha256 -> manifest.json``. The manifest goes last
    because deleting it first leaves artifact bytes that
    ``discover_backup_prefixes`` cannot see at all -- unreachable and
    unaccounted for. With the manifest last, an interruption leaves something
    that still lists, and which ``health.hollow_backup`` reports as a failure
    because its recorded artifacts are gone.

    Per-key, never ``delete_prefix``: that is ``shutil.rmtree`` on a resolved
    path, with no per-key error attribution and no control over order.
    """
    keys = set(storage.list_keys(prefix))
    ordered: list[str] = []
    manifest_key = f"{prefix}/{MANIFEST_KEY}"
    checksum_key = f"{prefix}/{MANIFEST_CHECKSUM_KEY}"
    ordered.extend(sorted(keys - {manifest_key, checksum_key}))
    if checksum_key in keys:
        ordered.append(checksum_key)
    if manifest_key in keys:
        ordered.append(manifest_key)

    for key in ordered:
        storage.delete(key)

    _remove_empty_directory(storage, prefix)
    return ordered


def _remove_empty_directory(storage: Any, prefix: str) -> None:
    """Drop a now-empty directory on backends that have them.

    Local storage leaves one behind; S3 has no directories to leave. Harmless
    to listings either way -- ``discover_backup_prefixes`` keys on
    ``manifest.json``, and rehearsal evidence is found by file -- but an
    operator reading the filesystem should not see a backup id that no longer
    exists.
    """
    local = getattr(storage, "local_path", None)
    if local is None:
        return
    path = local(prefix)
    if path is not None and path.is_dir() and not any(path.iterdir()):
        path.rmdir()


def prune_backups(
    cfg: dict[str, Any],
    settings: dict[str, Any],
    *,
    apply: bool = False,
    confirm_target: Optional[str] = None,
    plan_fingerprint: Optional[str] = None,
    released: tuple[str, ...] = (),
    ignore_damaged: bool = False,
    allow_delete_markers: bool = False,
    allow_unknown_reclaim: bool = False,
    allow_bulk: bool = False,
    actor: Optional[Any] = None,
    now: Optional[datetime] = None,
) -> PruneResult:
    """Plan a prune and, with ``apply``, carry it out.

    Dry run is the default and writes no receipt at all: a plan is a read, and
    filling the audit trail with reads would bury the writes.
    """
    resolved_actor = actor or Actor(surface=SURFACE_CLI)
    prune_id = f"prune-{secrets.token_hex(4)}"
    reconciled: list[str] = []

    if apply:
        # Before planning: a half-finished predecessor changes what is on disk
        # and therefore what this plan should say.
        reconciled = reconcile_interrupted(cfg, settings, actor=resolved_actor, now=now)

    plan = plan_prune(
        cfg,
        settings,
        released=released,
        ignore_damaged=ignore_damaged,
        allow_delete_markers=allow_delete_markers,
        allow_unknown_reclaim=allow_unknown_reclaim,
        allow_bulk=allow_bulk,
        now=now,
    )

    if not apply:
        return PruneResult(
            prune_id=prune_id,
            target_label=plan.target_label,
            dry_run=True,
            plan=plan,
            reconciled=reconciled,
        )

    if not plan.ok:
        raise PruneRefusedError(
            "Prune refused; nothing was deleted.",
            detail={"gates": [gate.to_payload() for gate in plan.blocking]},
        )

    expected_label = service.target_label(cfg)
    if (confirm_target or "") != expected_label:
        raise RestoreConfirmationError(
            "Typed target label does not match this target.",
            detail={"expected": expected_label, "received": confirm_target},
        )

    if plan_fingerprint is not None and plan_fingerprint != plan.plan_fingerprint:
        raise RestoreStageStaleError(
            "The plan changed since it was reviewed; re-run and re-confirm.",
            detail={
                "expected": plan.plan_fingerprint,
                "received": plan_fingerprint,
            },
        )

    storage = service.storage_for(settings)
    receipts_dir = service.receipts_directory(settings)

    # Re-read the receipt head immediately before the first delete. A
    # concurrent restore never modifies the manifest it reads, so re-reading
    # manifests proves nothing about it -- but a restore does write a receipt,
    # so the head advancing is the signal that something happened this plan did
    # not account for, including a restore from a backup about to be deleted.
    current_head = int((read_head(receipts_dir) or {}).get("sequence") or 0)
    if current_head != plan.receipt_head_sequence:
        raise RestoreStageStaleError(
            "A backup operation was recorded while this prune was being "
            "planned; re-run and re-confirm.",
            detail={
                "planned_head": plan.receipt_head_sequence,
                "current_head": current_head,
            },
        )

    # Oldest first, so an interruption leaves a superset of the survivors.
    doomed = sorted(
        plan.deletable,
        key=lambda item: (
            item.created_at or datetime.min.replace(tzinfo=UTC),
            item.backup_id,
        ),
    )
    rehearsal_root = (
        f"{database_prefix(str(cfg['client_id']), str(cfg['database_name']))}"
        f"/{REHEARSALS_SEGMENT}"
    )

    intent = write_receipt(
        receipts_dir,
        operation=OPERATION_PRUNE,
        status=STATUS_SUCCEEDED,
        actor=resolved_actor,
        target_label=plan.target_label,
        detail={
            "phase": PRUNE_PHASE_INTENT,
            "prune_id": prune_id,
            "prefixes": [item.storage_prefix for item in doomed],
            "backup_ids": [item.backup_id for item in doomed],
            "released": plan.released,
        },
        receipt_mirror=settings.get("receipt_mirror") or {},
        now=now,
    )

    deleted: list[dict[str, Any]] = []
    failed: list[dict[str, Any]] = []
    reclaimed = 0

    for item in doomed:
        try:
            keys = _delete_prefix_in_order(storage, item.storage_prefix)
        except Exception as exc:  # noqa: BLE001
            # Once a prefix's first delete has begun, a later failure in
            # that prefix must fail the whole run rather than move on.
            # Continuing would make the half-deleted state permanent: it
            # still lists, still ranks, still occupies a keep_last slot,
            # and can satisfy `newest_successful` -- pushing a real backup
            # out of the window on the next run.
            failed.append(
                {
                    "backup_id": item.backup_id,
                    "storage_prefix": item.storage_prefix,
                    "error": str(exc),
                }
            )
            break

        if HOLD_REHEARSAL_EVIDENCE in released:
            # Released, the evidence goes with the backup it describes.
            # Left behind it is unreachable: `rehearsals/<id>/` has no
            # manifest, so no listing surface would ever show it again.
            evidence_prefix = f"{rehearsal_root}/{item.backup_id}"
            for key in sorted(storage.list_keys(evidence_prefix)):
                storage.delete(key)
            _remove_empty_directory(storage, evidence_prefix)

        deleted.append(
            {
                "backup_id": item.backup_id,
                "storage_prefix": item.storage_prefix,
                "keys": keys,
                "bytes": item.bytes,
            }
        )
        reclaimed += item.bytes
    if failed:
        # No outcome receipt. The intent/outcome pair is the interrupted-prune
        # detector, so writing an outcome here would remove this run from
        # `dangling_intents` and the half-deleted prefix would never be
        # reconciled by any later run -- making permanent exactly the state the
        # abort exists to prevent. The dangling intent is the record; health
        # reports it and the next prune finishes it.
        raise PruneRefusedError(
            f"Deleting {failed[0]['backup_id']} failed part-way through; the "
            "run stopped rather than leave a half-deleted backup and continue. "
            "The next prune will finish it.",
            detail={"failed": failed, "deleted": deleted, "prune_id": prune_id},
        )

    outcome = write_receipt(
        receipts_dir,
        operation=OPERATION_PRUNE,
        status=STATUS_SUCCEEDED,
        actor=resolved_actor,
        target_label=plan.target_label,
        detail={
            "phase": PRUNE_PHASE_OUTCOME,
            "prune_id": prune_id,
            "deleted": [item["backup_id"] for item in deleted],
            # Counts, not the full list: receipts are read in their
            # entirety on every status page.
            "retained": plan.holds_by_id(),
            "reclaimed_bytes": reclaimed,
        },
        receipt_mirror=settings.get("receipt_mirror") or {},
        now=now,
    )

    capability = getattr(storage, "deletion_capability", lambda: {})()
    return PruneResult(
        prune_id=prune_id,
        target_label=plan.target_label,
        dry_run=False,
        plan=plan,
        deleted=deleted,
        failed=failed,
        # Honest about delete markers: nothing was actually freed.
        reclaimed_bytes=reclaimed if capability.get("reclaims") is True else None,
        intent_receipt_id=intent.receipt_id,
        outcome_receipt_id=outcome.receipt_id,
        reconciled=reconciled,
    )


__all__ = [
    "BULK_DELETE_COUNT",
    "BULK_DELETE_FRACTION",
    "FUTURE_SKEW_TOLERANCE",
    "GATE_IDS",
    "HOLD_IDS",
    "RELEASABLE_HOLDS",
    "Candidate",
    "GateResult",
    "PrunePlan",
    "PruneRefusedError",
    "PruneResult",
    "apply_holds",
    "dangling_intents",
    "plan_prune",
    "prune_backups",
    "reconcile_interrupted",
]
