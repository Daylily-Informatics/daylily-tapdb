# TapDB Backup and Recovery Lifecycle Ledger

Controlling plan: `BACKUP_RECOVERY_PLAN.md` — a working document kept outside the repo; GitHub issue #89 is the durable specification, and this ledger is the record of what was built.
Ledger path: `docs/plans/20260730T000000Z_tapdb_backup_lifecycle_ledger.md`
Branch: `feat/backup-recovery-lifecycle`

## Gate 0 Baseline

- Repo: `/Users/jdurham38/Documents/LSMC/LSMC_Code/daylily-tapdb`
- Baseline ref: `12351b8` ("Merge TapDB 9.0.10 GUI themes")
- Working style: feature branch, no commits taken during implementation at the
  operator's instruction; the branch is pushed as one unit when complete.
- Live limits: **no test touches a shared or remote database.** All PostgreSQL
  tests run against the ephemeral `pg_instance` fixture (a cluster created
  under pytest's tmp dir on port 15438 and torn down afterwards). Plan §8
  Aurora verification is out of scope for this ledger and requires explicit
  approval, us-east-1 only, read-mostly.

## Control Ledger

| ID | Area | Requirement/Surface | Status | Category | Owner | Evidence | Terminal Note |
|---|---|---|---|---|---|---|---|
| PKG-001 | `daylily_tapdb/backup` | Pure modules: errors, manifest, inventory, storage, receipts. | SUCCESS | feature_implementation | orchestrator | `tests/test_backup_manifest.py`, `test_backup_storage.py`, `test_backup_inventory.py`, `test_backup_receipts.py`. | Package imports without boto3 or a DB connection — `test_backup_package_contract.py`. |
| PKG-002 | `backup/engine.py` | Sole owner of `pg_dump`/`pg_restore`/`psql` invocation; `AuroraSchemaDeployer.client_env` refactored to share it. | SUCCESS | feature_implementation | orchestrator | `tests/test_backup_engine.py`; TOC parser against a real `pg_restore --list` fixture at `tests/fixtures/pg_restore_toc_full.txt`. | Containment enforced by `test_backup_package_contract.py::test_only_the_engine_shells_out`. |
| PKG-003 | `backup/service.py` | `plan`/`create`/`verify`/`list` plus config wiring under `backup:`. | SUCCESS | feature_implementation | orchestrator | `tests/test_backup_service_pg.py`, `test_backup_config.py`, `test_backup_service_edges.py`. | Credential-bearing storage URIs rejected at config load. |
| PKG-004 | `backup/verify.py` | Restore preflight, `plan_restore`, `restore_backup`, staged in-place swap. | SUCCESS | feature_implementation | orchestrator | `tests/test_backup_restore_pg.py` (48 tests). | Failure at either injection point leaves the original schema bit-for-bit unchanged, parametrized. |
| PKG-005 | `backup/postrestore.py`, `snapshots.py` | Post-restore verification suite, `rehearse_restore`, provider-snapshot receipts. | SUCCESS | feature_implementation | orchestrator | `tests/test_backup_postrestore_pg.py` (22 tests), `test_backup_snapshots.py`. | Every check proven to fail when the thing it names is broken, not only to pass. |
| CLI-001 | `cli/backup.py` | Seven-command group; exit codes 0/1/2; registry wiring. | SUCCESS | feature_implementation | orchestrator | `tests/test_backup_cli.py`, `test_backup_cli_pg.py`. | `EXIT_FINDINGS` proven from real commands (corrupt artifact, unknown backup, missing confirmation, stale fingerprint). |
| API-001 | `admin/backups.py`, `admin/main.py` | Eight admin routes; typed error → HTTP status mapping. | SUCCESS | feature_implementation | orchestrator | `tests/test_backup_api_admin.py`, `test_backup_api_routes.py`, `test_backup_api_http_pg.py`. | 400/403/404/409/422 each asserted; 422 raised before any mutation. |
| GUI-001 | `gui/router.py`, `backups.html`, `restore_review.html` | Nine admin-only routes, status page, staged-restore review. | SUCCESS | feature_implementation | orchestrator | `tests/test_backup_gui_embedded.py` (33), `test_backup_gui_live.py` (7). | Non-admin refused 403 across all nine, parametrized. |
| GUI-002 | `restore_review.html` | Typed-label control rendered only when the service will check it. | SUCCESS | bug_fix | orchestrator | `test_backup_gui_live.py::test_isolated_review_does_not_ask_for_a_label_it_will_not_check`. | **Defect found in review.** The form demanded a label for isolated restores that `restore_backup` ignores — a page asserting a control that did not exist. Fixed by adding `verify.confirmation_required` as the single definition, consumed by the enforcement point, the plan payload, both surfaces. |
| TEST-001 | Surfaces contract | CLI + API + GUI reach identical service functions. | SUCCESS | contract_test | orchestrator | `tests/test_backup_surfaces_contract.py` (14). | Runtime proof via a shared spy, not static review. Mutation-tested: diverting the GUI's create route fails the test. |
| TEST-002 | Lifecycle | Scope, schema isolation, read-only, refusal-before-mutation, sequence continuity, no residue. | SUCCESS | contract_test | orchestrator | `tests/test_backup_pg_lifecycle.py` (16). | Version-mismatch wiring mutation-tested: unwiring the preflight check fails the test. |
| TEST-003 | Package contract | Service layer free of surface frameworks; `subprocess` only in `engine.py`. | SUCCESS | contract_test | orchestrator | `test_backup_package_contract.py::test_no_module_imports_a_surface_framework`, `::test_only_the_engine_shells_out`. | AST-based, not grep — the modules discuss typer in docstrings precisely because they avoid it. Both mutation-tested. |
| TEST-004 | Fixture drift | Fast GUI tests' hand-written payloads pinned to real `views` output. | SUCCESS | contract_test | orchestrator | `test_backup_gui_live.py::test_embedded_test_fakes_match_real_view_output`. | Added after a fake drifted during GUI-002; fails now with a readable diff instead of a baffling template error. |
| DOC-001 | `docs/backup-and-recovery.md` | Operator runbook — an acceptance criterion. | SUCCESS | documentation | orchestrator | The file; commands verified by `test_docs_contracts.py`. | Includes the included/excluded state inventory, the recovery-method decision table, staged-restore procedure with the exact typed label, rehearsal cadence, receipt-chain verification, status semantics, and the surface parity table. |
| DOC-002 | `tests/test_docs_contracts.py` | Every `tapdb` invocation in the runbook validated against the real CLI. | SUCCESS | contract_test | orchestrator | `::test_every_runbook_command_exists`, `::test_every_runbook_flag_is_accepted_where_it_is_written`. | **The first draft of the runbook shipped `tapdb backup list --json`, which the CLI rejects** — `--json` is global and must precede the subcommand. Flags are checked at the position written, not against a union of levels; the union version missed exactly this case. |
| DOC-003 | `AI_DIRECTIVE.md`, `AGENTS.md`, `README.md`, `docs/runtime-and-cli.md`, `docs/integration-and-embedding.md` | Command map, route reality, destructive guardrails. | SUCCESS | documentation | orchestrator | Diffs in those files. | `tapdb backup` added to the command map; in-place restore added to destructive guardrails; embedded GUI backup routes listed. |
| DEP-001 | `cli/db.py` | Legacy `db data backup`/`restore` deprecated, not removed. | SUCCESS | plan_amendment | orchestrator | `test_backup_cli.py::test_legacy_backup_warns_and_names_its_replacement`. | Decision recorded as plan §12 item 4 (2026-07-28). Behavior unchanged; stderr notice names the replacement. Removal is a later major. |
| FIX-001 | `daylily_tapdb/connection.py` | `echo_sql` bug found incidentally. | SUCCESS | bug_fix | orchestrator | `tests/test_db_safety.py`. | Folded into this branch at the operator's instruction rather than split out. |
| RISK-001 | GUI, repo-wide | No CSRF protection on any GUI POST route. | OPEN | risk_accepted | orchestrator | Plan §12 item 5, raised 2026-07-29. | Pre-existing and repo-wide: 23 mutating POST routes carry no token. The four backup routes follow that posture rather than diverging. Restore-apply is incidentally protected (needs an unguessable `plan_fingerprint` plus the typed label). Fix belongs in GUI middleware as its own issue. |
| RISK-002 | Retention | `keep_last` default of 30 is a placeholder. | OPEN | open_question | orchestrator | Plan §12 item 2. | Confirmed with the operator: keep 30. Enforcement moves into TapDB via `backup prune` (pass 2); Object Lock and lifecycle rules stay infrastructure-side. |
| BUG-001 | CLI exit codes | Non-zero exit codes were discarded by the real entry point. | SUCCESS | bug_fix | orchestrator | `tests/test_backup_cli_exit_codes.py` (9, subprocess-based). | **Found by running the CLI from a shell, not by a test.** `cli_core_yo.app.run` invokes the app with `standalone_mode=False`, where click *returns* a `typer.Exit` code instead of raising; `run` discards it and reports 0. `tapdb backup verify` on a missing backup exited 0 — monitoring would never page. Four `CliRunner` tests asserted exit 1 and passed, because `CliRunner` normalises both mechanisms. Fixed locally by exiting via `SystemExit`, which `run` propagates correctly; no dependency change. **125 `typer.Exit` sites remain in 7 other CLI modules and are still affected** — out of scope for this branch. |
| BUG-002 | In-place restore | An in-place restore reissued an EUID minted after the backup. | SUCCESS | bug_fix | orchestrator | `test_backup_pg_lifecycle.py::test_an_in_place_restore_never_reissues_an_euid_minted_after_the_backup`, `::test_the_in_place_restore_reports_which_sequences_it_advanced`. | **The most serious defect in this work.** A row minted `Z-GVR-5R`; an in-place restore rolled it back; the next insert was issued `Z-GVR-5R` again, pointing at a different object — the exact silent corruption the design claims to prevent. Root cause: `capture_sequences` read `pg_sequences.last_value`, which is NULL whenever `is_called` is false, so a sequence poised to issue 5 was recorded identically to a fresh one poised to issue 1. `sequences.high_water` then skipped it and reported all sequences healthy. Fixed by reading the sequence relation directly, comparing `next_value` rather than `last_value`, and reconciling sequences against the **pre-restore safety backup** before verification. |
| BUG-003 | Docs | Runbook documented a `jq` path and a flag position that do not work. | SUCCESS | bug_fix | orchestrator | `test_docs_contracts.py::test_every_runbook_flag_is_accepted_where_it_is_written`, `test_backup_pg_lifecycle.py::test_json_paths_the_runbook_tells_operators_to_use_actually_exist`. | `tapdb backup list --json` is rejected (`--json` is global and must precede the subcommand), and `.status.receipt_chain` did not exist in the CLI payload at all. The CLI now carries the same shared `status_context` block as the API and GUI. Both classes of error are now caught by tests. |
| VER-001 | Full suite | Suite, lint, format, security scan green. | SUCCESS | contract_test | orchestrator | See Verification below. | |
| VER-002 | Plan §8 | End-to-end Aurora verification. | OPEN | contract_test | orchestrator | Not run. | Deferred by agreement: Option B, **us-east-1 only**, read-mostly (`plan`/`create`/`verify`/`rehearse`), requires explicit approval at the time. |

## Verification

| Check | Command | Result |
|---|---|---|
| Full suite | `pytest -q` | 1399 passed, 14 skipped |
| Backup subsystem | `pytest tests/test_backup_*.py tests/test_docs_contracts.py -q` | 633 tests |
| Lint | `ruff check daylily_tapdb/ admin/ tests/` | clean |
| Format | `ruff format --check daylily_tapdb/ admin/ tests/` | 198 files, clean |
| Security | `bandit -c pyproject.toml -r daylily_tapdb/ admin/` | 0 issues (High/Medium/Low) |
| Coverage | backup package + `admin/backups.py` | 94% |
| Coverage | `daylily_tapdb/cli/backup.py` | 88% (measured with the repo's `cli/*` omit overridden) |

Coverage note: the repo's coverage config omits `daylily_tapdb/cli/*`, so the
CLI figure above required overriding it and is not visible in the default run.

Environment note: a full-suite run during this work reported 246 errors, all
`pg_ctl start` failures. The cause was a leftover PostgreSQL from an
interrupted earlier run still holding port 15438 — not a code regression. If
the `pg_instance` fixture fails to start for a whole file at a time, check
`lsof -nP -iTCP:15438` before looking at the code.

## Notes for the reviewer

Three defects in this work surfaced only as **test-ordering flakiness** —
passing in isolation, failing in the full suite — and each would have been easy
to dismiss as a flaky test:

1. a `check_prefix_sequences_ahead` false positive that would have blocked real
   restores;
2. a plan fingerprint that embedded a per-second timestamp, so any stage/apply
   pair more than one second apart failed as `stale_stage`. **The default
   restore mode was broken for any human operator**; the tests passed only
   because they ran within one second;
3. a test-isolation leak that produced 24 errors in an unrelated module.

The fourth was found only by **running the CLI from a shell**: every non-zero
exit code was discarded by the real entry point while `CliRunner` reported them
correctly. No amount of additional `CliRunner` testing could have caught it.
The fifth was found by **watching what the database actually did** after an
in-place restore -- the verification suite reported every sequence healthy
while the next insert reissued a live EUID.

The lesson both share: a contract asserted only through a test harness is a
contract verified against the harness. Exercise the real entry point and
inspect the real end state.

The recurring failure mode on the test side was assertions that could not fail
— vacuous loops, an inverted condition, and several `or True` tautologies,
including one written as late as step 8. An AST sweep for these is worth
re-running on any change to this subsystem.

## Final Report

All rows terminal: no
Objective complete: no — implementation and documentation complete; `RISK-001`
and `VER-002` remain open by decision, and the branch is unpushed pending
operator review. **`RISK-002` is closed** (see the item-10 addendum below).

Status counts:
- SUCCESS: 21
- OPEN: 3
- FAIL: 0
- BLOCKED: 0

---

## Addendum — item 10, pass 1 (2026-08-01)

Item 10's remaining scope was reassigned: the dayhoff CDK companion issue is
withdrawn, and everything now lands in TapDB or Kahlo. Pass 1 delivers the
signal; pass 2 delivers `backup prune`; pass 3 delivers Kahlo's scheduler and
alert consumer over HTTP.

| ID | Area | Outcome |
|---|---|---|
| P1-01 | Storage | Fixed a live S3 bug: `list_keys` built `Prefix` with no trailing delimiter, so listing `acme/orders` also returned `acme/orders-staging`. Local-backend tests cannot see it; a stubbed-client test pins the literal argument. |
| P1-02 | Manifest | Added structured `provenance`, set before signing. A pre-restore safety backup was previously identifiable only by an English note. |
| P1-03 | Restore | The failed-restore receipt now names its safety backup. It was recorded only on the success path, so the one case where the safety backup matters most had the weakest evidence. |
| P1-04 | Health | `service.health_report` + `tapdb backup health` + `GET /api/backups/health`, sharing one implementation. |
| P1-05 | Receipts | Receipt mirroring is executed, bounded by a circuit breaker, and observable via `health.receipt_mirror`. |
| P1-06 | Config | `expected_rehearsal_interval_days`, plus an `invalid_fields` signal so a present-but-unparseable number fails rather than silently defaulting. |

### Release note — staged restore fingerprints

`compute_plan_fingerprint` hashes `manifest.checksum()`, which is a
`to_payload()` round-trip. Adding `provenance` therefore changes every
manifest's computed checksum, and **a restore staged before this upgrade and
applied after it will be refused** with a stale-fingerprint error. Re-stage and
re-apply; nothing is lost. Stored backups are unaffected — verification hashes
raw stored bytes and parses raw JSON, never a round-trip, which is pinned by
`test_a_manifest_written_before_provenance_existed_still_verifies`.

No deployment is currently affected: `daylily_tapdb/backup/` does not exist in
any released tag that any service pins (bloom, atlas, ursa `6.0.9`; dewey
`6.0.8`; zebra_day `9.0.6`; kahlo `9.0.9`), so no manifest exists anywhere yet.

### Two findings that changed the design

**`health.never_run` cannot fire alone.** The plan graded it a warning so fresh
targets would not page. `health.inventory` overrides that: a target with no
backups cannot be recovered, and grading it as noise is the failure the command
exists to prevent. Every route to `never_run` is already failed by inventory,
receipt coverage, or last-attempt, so it is now documented as informational.

**A wiped receipt store was not caught by inventory.** The review expected
`health.inventory` to cover it, but if backups still exist inventory passes and
the chain verifies vacuously at count 0 — so corrupting one receipt failed
while deleting all of them passed. `health.receipt_coverage` closes it.

### Post-implementation review (2026-08-01)

An independent adversarial pass over pass 1 found seven defects, all fixed and
mutation-verified. Three were mine and would have shipped:

1. **`health.inventory` counted unrestorable backups.** `restore_backup` refuses
   every class but `full`, so a store of nothing but template packs listed as
   healthy and could not be restored — and the health *fixture* defaulted to
   `template-pack`, so `test_a_healthy_target_exits_zero` was pinning exactly
   the failure the command exists to prevent. Added `health.recovery_point`;
   `newest_verifies` now targets the newest recovery-point backup rather than
   the newest row.
2. **`except Exception: continue` in the hollow-backup loop.** If every
   per-prefix read failed — S3 throttling at scale, or a policy allowing LIST
   at the root but denying it per-prefix — the check reported "every listed
   backup's artifacts are present" having inspected none. Unreadable entries
   are now recorded and downgrade the verdict.
3. **`.inf` and `.nan` passed cadence validation.** Both parse as floats, so
   `expected_interval_hours: .inf` reported the alarm as armed while
   `age_hours > inf` is unsatisfiable. `keep_last` had the mirror-image bug:
   validated with `float` but resolved with `int`, so `3.7` silently fell back
   to 30 on the setting that governs deletion.

Also fixed: health downloaded the full artifact on every poll (now capped, with
`backup verify` as the audit path); the 503 body had a different shape from the
200 body; an unavailable receipts source emitted one check row where the
healthy path emits seven; concurrent mirror writes could move the mirror anchor
*backwards*; and `LocalStorageBackend.put_bytes` used a deterministic temp name
that collides between concurrent writers of the same key.

Confirmed correct under review and left unchanged: the `_delimited_prefix` fix,
the `progress` out-parameter, "health never opens a database connection", and
"adding `provenance` cannot invalidate stored manifests".

**Carried to pass 2:** `health.interrupted_prune` has no remediation path — once
a dangling prune intent exists there is no command that writes the matching
outcome, so it would page permanently. Unreachable today (nothing writes prune
receipts); prune must ship its own clearing path.

## Addendum — item 10, pass 2 (2026-08-01)

`tapdb backup prune` and the storage-safety probes it depends on.

| ID | Area | Outcome |
|---|---|---|
| P2-01 | Storage | `deletion_capability()` on both backends. Tested with `botocore.stub.Stubber` against the real S3 model rather than the hand-rolled fake, which is what surfaced that `get_bucket_versioning` returns `{}` with no `Status` key and that `ObjectLockConfigurationNotFoundError` is not a modelled exception. |
| P2-02 | Retention | `daylily_tapdb/backup/prune.py` — 14 holds in a registry the implementation iterates, 7 gates, oldest-first per-key deletion, two-receipt intent/outcome pair, reconciliation of dangling intents. |
| P2-03 | CLI | `backup prune`, dry-run by default, typed target label, delete ceiling, `--release` restricted to four holds. Registered `JSON`/`MUTATING`/`DRY_RUN`/`LONG_RUNNING`, deliberately **not** `INTERACTIVE`. |

### What the mutation sweep found

19 mutations, run twice. The first pass caught 14 and left 5 standing; **two of
those five were badly-built mutations that were no-ops** (`x = None or f()`
still calls `f`), which is worth recording because a defective mutation reads
exactly like a passing test. Of the three real gaps:

- The safety-backup **note regex** was never exercised, because every fixture
  carried structured `provenance`. That regex is the only thing protecting
  safety backups written before the field shipped.
- The **pre-delete receipt-head re-read** was masked by always passing a plan
  fingerprint, so the fingerprint check fired first. A scheduled prune passes
  no fingerprint — that is the path it guards.
- **`prefix_integrity`** had no reachable failure case in the suite.

After closing them: 19 of 19 caught, zero survivors.

### Two rules are defence in depth, and the tests say so

`only_copy_of_target` and `damaged` cannot be the *sole* protection for any
store, by construction — a lone backup is necessarily also held by `keep_last`,
and a prefix with no readable manifest is necessarily also `undated` and
`checksum_mismatch`. The isolation test would have quietly failed to isolate
them, so they are named in `DEFENCE_IN_DEPTH_HOLDS` and tested differently:
every *other* rule is disabled and the hold must still fire. That proves they
work without claiming a uniqueness they do not have.

### A real bug the tests caught

Future-dated manifests were given a hold but **left in the ranking**, so they
still consumed `keep_last` slots — reintroducing the exact trap the ranking
rewrite exists to prevent, one level down. An hourly cron through a skew window
with `keep_last: 7` would have filled every slot with 2099-dated manifests and
left every real backup with an empty hold set. They are now dropped from the
ranking entirely.

### Post-implementation review of passes 1 and 2 (2026-08-01)

Three independent reviews — prune data-loss paths, cross-pass integration, and a
test-suite audit for unfalsifiable assertions. Between them they found a
whole-store wipe that had shipped.

**Critical, all fixed and mutation-verified:**

1. **`reconcile_interrupted` deleted unvalidated paths taken from a receipt
   field, before any gate ran.** An intent recording
   `prefixes: ["acme/prod"]` erased an entire store of backups that no rule
   made deletable — `_prefix_state` saw no manifest at that level and called it
   half-deleted. Reconciliation also had no target scoping (two schemas on one
   database legitimately share a receipts directory) and no chain verification,
   so it could act on tampered receipt content and then *append* to the chain,
   laundering the tamper past the gate that would have caught it. Receipt
   content is now treated as evidence, not instruction: prefixes must recompute
   to `backup_prefix` under this target, the intent's `target_label` must match,
   and the chain is verified first.
2. **`apply_holds` keyed a dict on `backup_id`.** Two prefixes sharing an id —
   a hand-copied directory is enough — collapsed to one entry, so every rule's
   output landed on one candidate and the other kept an **empty hold set**:
   deletable with no flags and every gate green. The safety model's central
   premise was defeated by a dict comprehension. Holds now apply to every
   candidate sharing an id, plus a new `duplicate_backup_id` hold.
3. **`--ignore-damaged` silently became `--release keep_last`.** Subtracting
   damaged prefixes from the window removed protection from healthy backups
   without giving any to the damaged ones, because a damaged candidate has no
   date and cannot occupy a slot. With `keep_last: 7` and eight damaged
   prefixes the window protected nothing.
4. **A transient manifest read failure deleted intact backups during
   reconciliation** — the exact three cases (`throttle`, 5xx, expiring
   credential) that `_hold_damaged` names as benign. Unreadable is now
   `_PREFIX_UNKNOWN` and never deleted.

**Also fixed:** the abort path wrote an outcome receipt, which removed the run
from `dangling_intents` and made the half-deleted state permanent — the exact
outcome the abort exists to prevent; a refused *plan* exited 0, so a scheduled
prune could be blocked every night in silence; a failed reconcile cleared
`health.interrupted_prune`; `health.storage_safety` was dropped on the
oversized-artifact path, breaking the complete-rows guarantee on most real
targets; `keep_last`/`newest_successful` fired zero times in an all-undated
store with `--release undated`, falsifying the "no combination of releases can
empty a target" promise in the shape the flag exists for; and health spelled
prune's receipt vocabulary as literals on both sides *and* in both test suites,
so a rename would have left everything green with the detector dead.

**Test defects fixed:** four "refused, nothing happened" assertions were blind
to the receipt store, so an intent written before the confirmation check would
have passed them — and the next run's reconciliation would then have finished
exactly the deletions the refusal prevented. The mirror-ordering test could not
observe ordering (final state is identical either way on a local mirror) and
now injects a partial outage. `test_docs_contracts` used `!=` where `==` was
meant, which passes for any value. The prune fixtures compared against wall
clock while the data sat at a fixed instant, so the suite's correctness
depended on when it ran.

**A note on method.** Across both passes, **six mutations were defective rather
than survived** — `x = None or f()` still calls `f()`, and two anchors stopped
matching after reformatting. A broken mutation is indistinguishable from a
passing test unless the mutation is verified to change behaviour first.

## Item 10 — closed (2026-08-01)

Item 10 asks for *"scheduled backup status, retention enforcement, failure
alerts, immutable audit receipts, and a documented rehearsal/runbook cadence."*
All five are delivered.

| Part | Delivered by |
|---|---|
| Scheduled backup status | `ok`/`stale`/`failing`/`never_run`, shipped in the original branch work |
| Immutable audit receipts | hash-chained receipts with head-anchor truncation detection, shipped in the original branch work |
| Documented rehearsal/runbook cadence | `docs/backup-and-recovery.md` §6, shipped in the original branch work |
| **Retention enforcement** | **`tapdb backup prune`** (pass 2) |
| **Failure alerts** | **`tapdb backup health`** (pass 1) — the contract, documented with wiring examples |

**On alerting, and why this is complete rather than deferred.** TapDB is a
library embedded in several host services. It has no daemon and no notification
channel, and reaching out to SNS or Slack from inside another application's
process is a surprise, not a feature. What it owes a consumer is one stable,
machine-first verdict: `backup health` exits `0` when the target is
recoverable, `1` when it is not, and `2` when the check itself could not run,
with parseable JSON on stdout on every code. It needs no database, so it still
answers when the database is down; it writes nothing, so it is safe to poll from
anywhere. Wiring that to cron, a systemd timer, or any polling monitor is a
single line, documented in the runbook.

**On scheduling, and why it was never required.** Item 10 asks for *scheduled
backup status* — the status of a declared cadence, which ships — not for
anything here to execute one. A scheduler entered this work through the
"dayhoff must not schedule" conversation, not through the issue. It is tracked
as future work, not as a gap.

**Scope revision.** The original plan deferred retention enforcement, the
scheduler, and SNS to a dayhoff CDK companion issue. The operator withdrew that
deferral (dayhoff does not run backups on a schedule), which is what pulled
retention enforcement into TapDB. Bucket provisioning — creation, Object Lock,
lifecycle rules — remains infrastructure-side; TapDB verifies those properties
via `deletion_capability()` and refuses to prune where a delete would not free
bytes.

**Deliberately not built**, and recorded so they are decisions rather than
oversights: a Kahlo scheduler executing backups on a cadence; a Kahlo alert
consumer pushing to SNS; multi-target orchestration across the fleet; and
mounting the TapDB admin API in bloom, atlas, dewey and ursa. Each is viable
and none is required to close #89.
