# TapDB Backup and Recovery

Operator runbook for the TapDB backup lifecycle: what is captured, how to
recover, and how to prove a backup would actually work before you need it.

Every operation is available from three surfaces — the `tapdb backup` CLI, the
admin API, and the embedded GUI — and all three call the same service code, so
the guarantees below hold regardless of which one you use.

---

## 1. What is in a backup, and what is not

A TapDB backup is **schema-scoped**. It captures one schema in one database,
using `pg_dump --schema <name> --format custom --no-owner --no-acl`.

### Captured

| | |
|---|---|
| Tables and their rows | all base tables in the schema, including `_tapdb_migrations` |
| Sequences | including per-prefix `*_instance_seq` and IDENTITY-backed `*_uid_seq`, with their current values |
| Functions and triggers | the Meridian EUID machinery among them |
| Views, indexes, constraints | including the `euid` uniqueness constraint |
| Row-level security policies | recorded and re-applied |
| A manifest | table list with row counts, sequence high-water marks, an object inventory derived from `pg_restore --list`, the source server version, and a SHA-256 checksum of the archive |

### Not captured — and why it matters

| | |
|---|---|
| **Other schemas in the same database** | TapDB usually shares a database. A backup of `tapdb_prod` says nothing about a neighbouring schema, and restoring it will not touch one. |
| **Roles, users, and grants** | `--no-owner --no-acl` deliberately keep cluster-scoped state out. A restored schema needs its roles to already exist on the target. |
| **Anything cluster-level** | other databases, tablespaces, extensions installed outside the schema, `postgresql.conf`. |
| **The TapDB config file** | it holds connection details and must be backed up by whatever manages your configuration. A backup archive is not a substitute. |
| **Secrets of any kind** | manifests travel to shared storage, so credentials are rejected at config load and stripped from anything recorded. |

> **The practical consequence.** Restoring a TapDB backup onto a fresh cluster
> will not give you a working system on its own: you need the roles and the
> config too. Restoring onto an existing, correctly-provisioned cluster is the
> supported path, and it is what `rehearse` exercises.

---

## 2. Choosing a recovery method

Three different problems, three different answers. Picking the wrong one is the
most common way a recovery goes badly.

| Situation | Use | What you get | What you lose |
|---|---|---|---|
| You need a database-derived template snapshot for an operator backup workflow | **Database backup template pack** — `tapdb backup create --class template-pack` | Templates and their configuration, portable across databases | Instance data. This is a configuration export, not a recovery tool. |
| Data was lost, corrupted, or wrongly modified inside TapDB's configured schema | **Logical restore** — `--class full`, then `backup restore` | Every row in the configured schema, independent of runtime tenant scope, into an isolated database or through the gated in-place workflow | Anything written after the backup was taken |
| The whole cluster is gone, or you need a point far back in time and your provider holds it | **Provider snapshot cutover** — `--class provider-snapshot` records the snapshot; the cutover itself is an Aurora/RDS operation | Whole-cluster recovery including roles and other schemas | Granularity — you move the entire cluster, not one schema. TapDB records the receipt; it does not perform the cutover. |

**Start with `full` into an isolated target.** A full capture never uses the
runtime `NOBYPASSRLS` credential. It requires the separately configured
`target.operator` identity, verifies that identity is a distinct PostgreSQL
`SUPERUSER` or `BYPASSRLS` role, and fails hard when it is absent or invalid.
The signed manifest records `mode: physical_schema`, `row_security: bypassed`,
and `physical_schema_complete: true`; that claim means every tenant's rows in
the configured schema were visible to both the inventory transaction and
`pg_dump`. The ordinary runtime role remains RLS-bound and is never promoted.

A database `template-pack` uses that ordinary runtime role and therefore
contains only active templates visible under its configured tenant/global RLS
scope. Its manifest records that scope, omits sequences and non-template row
counts, and never claims physical-schema completeness. A `provider-snapshot`
is created through the RDS API without opening a PostgreSQL session; TapDB
signs a provider receipt but treats its database contents as opaque until a
provider restore is independently inspected.

The database backup class above is distinct from the deterministic,
source-controlled repository packs managed by `tapdb templates
export|import|inventory`. Repository packs have an adjacent portable provenance
receipt and intentionally exclude database identity, timestamps, secrets, and
sequence state; see [Template Authoring](template-authoring.md#repository-owned-packs).

---

## 3. Reading the status

`tapdb backup list`, `GET /api/backups/status`, and the GUI's Backups page all
report the same four states, derived from receipts:

| Status | Meaning | What to do |
|---|---|---|
| `ok` | a recent create succeeded, within the configured cadence | nothing |
| `stale` | the last success is older than `expected_interval_hours` | find out why the schedule stopped; take one now |
| `failing` | the most recent create *attempt* failed | read the receipt's error; a previous good backup may still exist |
| `never_run` | no successful create has ever been recorded | take one |

**A cadence of `0` means none is configured.** `stale` is then unreachable, and
the status will say so rather than implying a schedule that does not exist. If
you rely on scheduled backups, set `expected_interval_hours` — otherwise
nothing will ever tell you the schedule has stopped.

---

## 3.1 The alerting contract — `backup health`

One command answers the only question monitoring cares about: **can this target
be recovered?** Exit `0` means yes.

```bash
tapdb backup health          # JSON on stdout, always
tapdb backup health --human  # readable summary instead
```

**JSON is the default and there is no `--json` flag.** `--json` is a *global*
option, so it must precede the subcommand (`tapdb --json backup health`). A
caller that appends arguments after the subcommand would produce
`backup health --json` and get `Error: No such option`, so health emits machine
output unconditionally and `--human` opts out.

**It needs no database.** Everything it reads is receipts, storage and config,
so health still answers when the database is down — which is exactly when
someone is asking. Exit `2` therefore means config or storage could not be
consulted; it never means "the database is down".

| Exit | Meaning | What to do |
|---|---|---|
| `0` | Answered; nothing failing. Warnings may be present. | Nothing. |
| `1` | Answered; something is wrong. | Page. Read `failing` in the JSON. |
| `2` | Could not reach a verdict at all. | Fix the monitoring path, not the backups. |

A failure always outranks an unreachable source: a tampered receipt chain
*plus* an unreachable bucket exits `1`, not `2`. Burying a real finding under
"could not answer" would suppress the alert exactly when someone has broken the
audit trail.

### What fails, and what only warns

| Check | Fails when | Warns when |
|---|---|---|
| `health.inventory` | no backups exist at all | — |
| `health.recovery_point` | backups exist but none is `full` or `provider-snapshot` | — |
| `health.newest_verifies` | the newest **recovery-point** backup fails checksum verification | `pg_restore` is absent, or the artifact is above the health read limit |
| `health.hollow_backup` | a manifest lists artifacts that are missing **or the wrong size** | some backups could not be inspected |
| `health.damaged` | a backup prefix has an unreadable manifest | — |
| `health.receipt_coverage` | backups exist but no create receipt does | — |
| `health.receipt_chain` | hashes or the head anchor do not match — tampering | a sequence gap explained by an unreadable file — local corruption |
| `health.last_attempt` | the most recent backup attempt failed | — |
| `health.cadence` | the last success is older than `expected_interval_hours` | — |
| `health.cadence_configured` | the value is present but not a number | no cadence configured |
| `health.rehearsal_age` | the newest rehearsal failed, or the newest *successful* one is older than the interval | no rehearsal cadence, or none ever run |
| `health.interrupted_prune` | — | a prune recorded an intent with no matching outcome |
| `health.receipt_mirror` | — | the mirror is behind the local chain, or unreadable |
| `health.storage_safety` | — | no Object Lock or versioning on the bucket |
| `health.never_run` | — | no backup has ever succeeded (informational) |

**`health.recovery_point` is the one that makes `inventory` mean something.**
A target backed up nightly as `template-pack` produces a long, healthy-looking
listing and cannot be restored at all — `backup restore` refuses every class
but `full`, and §2 above is explicit that a template pack is a configuration
export, not a recovery tool. Counting rows is not the contract; being able to
get the data back is.

**Health does not checksum very large artifacts.** Above
`backup.health_verify_max_bytes` (default 1 GiB), `health.newest_verifies`
reports `skip` and points at `backup verify`. Health is polled — hashing a
50 GB dump every five minutes would move terabytes a day for a liveness check.
Set it to `0` to always checksum regardless of size.

What the cap does *not* skip: `health.hollow_backup` compares every artifact's
stored size against the size its manifest records, on every backup rather than
just the newest, at any size. That catches the realistic corruption modes — a
truncated upload, a partial sync, a zero-byte object — without reading a byte.
A full checksum additionally catches silent bit-rot in the middle of a file,
which is what `backup verify` and the quarterly rehearsal are for.

**`health.interrupted_prune` warns rather than fails**, because receipts are
immutable: a dangling intent cannot be cleared by writing anything, so failing
would page forever and the only cure would be deleting the audit trail. The
damage an interrupted prune actually causes is caught by
`health.hollow_backup` and `health.damaged`, which do fail.

Three of those warn-not-fail calls are deliberate and worth stating:

- **An unlocked, unversioned bucket warns.** It is the normal state today and
  is infrastructure's to change; failing would page on a condition nobody in
  this repo can fix.
- **A stale receipt mirror warns.** It is an evidence gap, not lost
  recoverability — the backups are still there and still restorable.
- **Local receipt corruption warns while tampering fails.** A partial write
  leaves a permanent sequence gap with no repair command, so failing hard would
  page forever, and the only way to silence it is deleting the audit trail. The
  check names the unreadable file instead.

`health.never_run` never changes the exit code on its own: every state that
reaches it is already failed by `inventory`, `receipt_coverage`, or
`last_attempt`. It is reported because "this target has never had a successful
backup" is worth reading next to whichever check did fail.

### Wiring it to an alert

This command **is** the failure-alert deliverable. TapDB is a library embedded
in other services — it has no daemon, no notification channel, and no opinion
about where your alerts go. What it owes you is a single, stable, machine-first
verdict, and that is what `backup health` is. Delivery is one line of whatever
you already run.

The whole contract is: **exit 0 means recoverable.** Anything that can read an
exit code can alert on it.

**cron, mailing on failure only:**

```bash
0 * * * * tapdb backup health > /var/log/tapdb-health.json || \
  mail -s "TapDB backup unhealthy on $(hostname)" ops@example.com \
    < /var/log/tapdb-health.json
```

**systemd timer** — let the unit fail and use whatever already watches unit
state (`OnFailure=`, Prometheus node-exporter, etc.):

```ini
[Service]
Type=oneshot
ExecStart=/usr/local/bin/tapdb backup health
StandardOutput=file:/var/log/tapdb-health.json
```

**Any polling monitor** — CloudWatch agent, Nagios, Zabbix, Sensu: run the
command, alert on non-zero, attach stdout. Distinguish the two failure codes if
your tool can: `1` means backups are in trouble, `2` means the *check* could not
run and is itself the outage.

Two properties make this safe to schedule anywhere:

- **It needs no database.** Receipts, storage and config only — so it still
  answers when the database is down, which is exactly when you are asking.
- **It writes nothing.** Safe to run every minute, from as many places as you
  like, with no effect on the audit trail.

If you would rather have TapDB push the alert itself, it cannot: an embedded
library that reaches out to SNS or Slack on its own is a surprise in someone
else's process. The consumer decides.

### Over HTTP

`GET /api/backups/health` runs the same implementation. Because HTTP has no
exit code, the mapping is:

| CLI exit | HTTP | Body |
|---|---|---|
| `0` | `200` | `status: "ok"` or `"warn"` |
| `1` | `200` | `status: "failing"` |
| `2` | `503` | `error: "health_unavailable"` |

**A failing backup is a successful health report, so it is `200`.** Returning
`5xx` would put a working detector behind every proxy and uptime monitor
between the caller and the service, each of which reads `5xx` as "retry" —
the finding would be retried, rate-limited, and eventually reported as a TapDB
outage rather than as the backup problem it is. Read `status` from the body,
and treat a non-200 as "the health signal itself is down".

---

## 4. Taking a backup

```bash
tapdb backup plan            # read-only: what would be captured
tapdb backup create          # capture, verify, and record a receipt
tapdb backup list            # inventory plus the status block

tapdb --json backup create   # machine-readable
tapdb --dry-run backup create
```

> `--json` and `--dry-run` are **global** flags and must come *before* the
> subcommand. `tapdb backup create --json` is rejected with
> `Error: No such option: --json`.

`plan` never writes anything and is safe against production. `create` reads the
database inside a `REPEATABLE READ` snapshot, so the dump is internally
consistent even under concurrent writes.

**Schema drift.** `create` compares the live schema against the expected
inventory. If it finds TapDB-namespaced objects that should not be there, it
refuses, because a backup of an unexplained schema is a backup you cannot
reason about later. Override with `--allow-drift` once you know what the extra
objects are. Use `plan --strict` to see drift as a blocking finding before you
commit to a backup.

### Exit codes

Automation depends on the distinction:

| Code | Meaning |
|---|---|
| `0` | succeeded |
| `1` | ran and found a problem — a corrupt backup, a failed check, a refused restore |
| `2` | could not run — missing config, unreachable database, bad arguments |

A monitoring job should page on `1`. It should treat `2` as a broken runner,
not a broken backup.

---

## 5. Verifying a backup

```bash
tapdb backup verify --backup-id <id>              # deep by default
tapdb backup verify --backup-id <id> --level quick
```

- **quick** — checksum and `pg_restore --list`; proves the archive is intact
  and readable.
- **deep** — additionally reads every block through `pg_restore -f /dev/null`;
  proves the archive decompresses end to end.

Neither touches the database. Verification failure exits `1` and, over the API,
returns **422** — the artifact is unprocessable, not a server fault.

---

## 6. Rehearsing — the part most teams skip

A backup that has never been restored is a hypothesis.

```bash
tapdb backup rehearse --backup-id <id>
```

This restores into a scratch database, runs the full post-restore verification
suite, tears the database down, and writes durable evidence to storage. It does
not touch live data, and it is **recorded as a rehearsal, not a restore**, so
drills never pollute the restore trail an auditor reads.

**Recommended cadence: quarterly, and after every schema migration.** Migrations
are what break restores, because an old archive and a new schema can disagree
in ways nothing notices until you need the archive.

Use `--keep` to leave the rehearsal database in place for inspection.

---

## 7. Restoring

### 7.1 Isolated (the default, and non-destructive)

```bash
tapdb backup restore-plan --backup-id <id>     # read-only preview
tapdb backup restore --backup-id <id>          # restores into a separate database
```

Creates a new database named from the backup, restores into it in a single
transaction, and runs post-restore verification. **Live data is never touched.**
No typed confirmation is required, because nothing destructive can happen.

If the restore fails, the created database is dropped. If verification fails,
it is *kept* and flagged `quarantined: true` so you can investigate — and the
command exits `1`.

### 7.2 In-place (destructive, and gated)

In-place replaces the live schema and accepts only a signed `full` archive
whose data-scope claim is exactly the operator-authenticated physical-schema
contract described in §2. Legacy or runtime-scoped archives fail the
`identity.data_scope` gate before mutation. The restore itself also requires
the distinct `target.operator` credential; there is no fallback to the runtime
credential and the runtime role is never granted an RLS bypass.

For a physically complete archive, in-place additionally requires the typed
target label:

```
<client_id>/<database_name>/<schema_name>@<database>
```

for example `acme/prod/tapdb_prod@tapdb_shared`. `restore-plan` prints the exact
string to type.

```bash
tapdb backup restore-plan --backup-id <id> --mode in-place
tapdb backup restore --backup-id <id> --mode in-place \
    --confirm-target 'acme/prod/tapdb_prod@tapdb_shared'
```

**What actually happens** — nothing is dropped until a verified replacement
exists:

1. a **safety backup** of the current schema is taken first;
2. the current schema is **renamed aside**, not dropped;
3. the archive is restored under the original schema name;
4. post-restore verification runs **while the previous schema is still on disk**;
5. only then is the superseded schema dropped — unless you pass
   `--keep-superseded`.

A failure at any point before step 5 leaves the original schema exactly as it
was: same rows, same sequence values, same name. The safety backup survives
regardless.

### 7.3 The staged-restore handshake

Restores are stateless across surfaces. `restore-plan` returns a
`plan_fingerprint` covering the manifest checksum, target label, table list,
mode, and backup reference. Apply **re-stages the plan and compares** — if
anything changed between preview and apply, the restore is refused as
`stale_stage` rather than quietly applying something you never reviewed.

This is why the GUI hands back a *fresh* fingerprint when it refuses one: you
should be able to re-read what changed and try again, not be stuck.

The typed label is re-checked inside the service, not just at the surface. A
script calling `restore_backup` directly gets the same gate.

### 7.4 The policy switch

`safety.destructive_operations` in the TapDB config:

| Value | Effect |
|---|---|
| `confirm_required` (default) | in-place needs the typed label |
| `allowed` | in-place proceeds without one — for automated environments only |
| `blocked` | in-place is refused outright; over the API, **403** |

---

## 8. EUIDs and why a restore is safe to consumers

A restore rewinds rows. The danger is not the rewind — it is minting an
identifier a consumer already holds, which would then resolve to a *different*
object.

**The hard case is an in-place restore.** Identifiers issued *between* the
backup and the restore are rolled back with everything else, so `max(euid_seq)`
drops and the archive's sequence positions come back with it. The archive
cannot help here: it predates those identifiers by construction.

Three mechanisms close that gap:

1. **Sequence reconciliation against the safety backup.** The safety backup is
   taken immediately before anything is touched, so its sequence positions
   cover everything the target ever issued. After restoring the archive and
   *before* verification, every sequence is advanced to at least that position.
   The restore reports what it moved as `sequences_advanced`, and the receipt
   records it — advancing past the archive is a deliberate divergence and is
   auditable.
2. **`sequences.high_water`**, which blocks if any sequence is behind what the
   backup recorded. It compares the *next* value, not `last_value`: after
   `setval(s, 5, false)` a sequence issues `5`, while after
   `setval(s, 5, true)` it issues `6`. Same `last_value`, different identifier.
3. **The `euid` unique constraint**, which rejects a collision outright if
   anything upstream is wrong.

So the outcome is a fresh identifier, or a loud failure — never a silently
reused one. References to objects created after the backup fail cleanly as
"not found" rather than resolving to something else.

> This was not always true. An earlier build reissued a rolled-back EUID to a
> different object while reporting all sequences healthy, because the manifest
> could not record a sequence that had been `setval`'d but never called.
> `tests/test_backup_pg_lifecycle.py::test_an_in_place_restore_never_reissues_an_euid_minted_after_the_backup`
> reproduces that exact scenario.

---

## 9. Receipts and the hash chain

Every create, verify, restore, and rehearsal writes an immutable receipt
(mode `0400`) into the receipts directory. Each receipt chains to its
predecessor by hash.

```bash
tapdb --json backup list | jq '.status.receipt_chain'
```

`receipt_chain.ok == false` means the audit trail has been edited or truncated.
The status page surfaces this prominently, because a status that reads
"healthy" while its own audit trail is broken is worse than no status at all.

> **What the chain can and cannot prove.** It detects edits to any receipt that
> something later chains to. Editing the newest receipt before the next one is
> written cannot be detected — inherent to hash chaining, not a defect. Mirror
> receipts off-host if you need stronger guarantees.

---

## 9.1 Retention — `backup prune`

The only command here that destroys recoverability, and the only one whose
default is to do nothing.

```bash
tapdb backup prune                                       # plan only; deletes nothing
tapdb backup prune --apply --confirm-target "<label>"    # actually delete
```

**Dry run is the default**, and a plan writes no receipt — a plan is a read.
Deleting needs `--apply` *and* the typed target label, the same one an in-place
restore demands. The global `--dry-run` **vetoes** `--apply`, so a scheduled
invocation wrapped in it cannot delete whatever else is on the command line.

### Nothing is selected for deletion

A backup is removed only when **no rule protects it**. That inversion is the
whole safety model: a bug in "choose what to delete" quietly adds something to
a kill list, while a bug here has to produce a positively-empty hold set.

Human output lists every **retained** backup with the holds keeping it. "Why is
this still here" is as operationally important as "what would go", and it is
the only way to watch a rule actually working.

| Hold | Protects | Releasable |
|---|---|---|
| `keep_last` | the newest N by date, N from `backup.retention.keep_last` | no |
| `newest_successful` | the most recent backup reporting itself complete | no |
| `only_copy_of_target` | the last backup of a target, whatever else is released | no |
| `only_copy_of_class` | the newest of each class | no |
| `safety_backup` | pre-restore safety backups — the last copy of production if a restore degraded | no |
| `recently_restored_from` | anything a restore has read from | no |
| `provider_snapshot_reference` | `provider-snapshot` manifests, the only index of a real cluster snapshot | no |
| `damaged` | prefixes whose manifest cannot be read | no |
| `future_dated` | timestamps far enough ahead to be a bad clock | no |
| `checksum_mismatch` | manifests whose bytes do not match `manifest.sha256` | no |
| `undated` | manifests with no `started_at` | **yes** |
| `unparseable_created_at` | manifests whose `started_at` is not a timestamp | **yes** |
| `rehearsal_evidence` | backups with rehearsal evidence stored against them | **yes** |
| `provenance_unknown` | manifests older than the receipt store can account for | **yes** |

`--release <hold>` accepts only the four marked releasable. `keep_last` and the
only-copy floors have **no flag at all**: `--release keep_last` would collapse a
90-backup history to roughly one per class in a single command.

Releasing `rehearsal_evidence` deletes the evidence along with the backup it
describes. Left behind it would be unreachable forever — `rehearsals/<id>/` has
no manifest, so no listing surface would ever show it again.

### Gates — conditions under which no deletion decision is trustworthy

A gate aborts the whole run before a single byte is deleted.

| Gate | Refuses when | Escape |
|---|---|---|
| `retention_sane` | `keep_last` is below 1 | none |
| `receipt_chain` | the receipt chain does not verify | none |
| `no_damaged` | any manifest is unreadable | `--ignore-damaged` |
| `storage_reclaims` | deleting would not free the bytes | see below |
| `policy` | the target's safety policy forbids destructive operations | none |
| `prefix_integrity` | a candidate prefix does not recompute from this target's identity | none |
| `delete_ceiling` | more than 25 backups, or more than half the store, would go | `--allow-bulk` |

On S3, `storage_reclaims` probes the bucket. **Versioning `Enabled` or
`Suspended`** means a delete writes a marker and frees nothing — use an S3
lifecycle `NoncurrentVersionExpiration` rule instead, or pass
`--allow-delete-markers` to proceed with `reclaimed_bytes: null`. **Object Lock
configured refuses with no escape**: the bucket carries a retention policy
declared outside TapDB. A probe that cannot be read is *unknown*, not absent,
and refuses unless `--allow-unknown-reclaim`.

**The delete ceiling is the control that survives automation.** The typed label
becomes a constant in a config file the moment this is scheduled, and the
`policy` gate lets `confirm_required` straight through — so a hand-edited
`keep_last: 1` would otherwise delete an entire history unattended with every
other gate satisfied.

### Interruption

Deletion goes oldest prefix first, so an interruption leaves a superset of the
intended survivors. Within a backup: **artifacts, then `manifest.sha256`, then
`manifest.json`**. The manifest goes last because deleting it first would leave
artifact bytes no listing can see at all.

An applied prune writes **two** receipts — an `intent` before the first delete
and an `outcome` after, linked by `prune_id`. An intent with no outcome is
precisely how an interrupted prune is detected, and `backup health` reports it
as `health.interrupted_prune`. The next `backup prune` reconciles it: it
finishes any half-deleted prefix and writes the missing outcome. That is the
only thing that clears the warning, because receipts are immutable.

---

## 10. Configuration

Under `backup:` in the TapDB config (`config_version: 4`). Every field has a
working default, so a config written before this section existed keeps working.

The authentication used by a `full` backup is not a `backup:` setting. Configure
the distinct privileged identity under the selected `target`:

```yaml
target:
  user: tapdb_runtime
  operator:
    user: tapdb_operator
    password: ""       # explicit empty value is valid for local trust auth
    secret_arn: ""
    iam_auth: false
```

The operator user must differ from `target.user` and must authenticate as a
PostgreSQL `SUPERUSER` or `BYPASSRLS` role. Aurora targets select an explicit
operator authentication path through `password`, `secret_arn`, or `iam_auth`.
Missing operator configuration makes full plan/create/restore fail; TapDB never
substitutes the runtime credential or ambient libpq target variables.

**Keys are nested.** Unrecognised keys are silently ignored — so a flat
`storage_uri:` does not fail, it just never takes effect and your backups go to
the local config directory. Copy this shape exactly:

```yaml
backup:
  storage:
    uri: ""                          # file:// or s3://; blank = <config_dir>/backups
  retention:
    keep_last: 30
  encryption:
    mode: none
  signing:
    mode: none                       # none | kms
    kms_key_arn: ""
  provider_snapshots:
    enabled: false
    cluster_identifier: ""
  rehearsal:
    database_prefix: tapdb_rehearsal
  expected_interval_hours: 0         # top-level under `backup:`, not nested
  receipt_mirror: {}                 # e.g. {uri: "s3://..."}
```

| YAML path | Default | Notes |
|---|---|---|
| `backup.storage.uri` | `<config_dir>/backups` | `file://` or `s3://`. **Credentials in the URI are rejected at load** — a bad config fails immediately rather than part-way through a backup. |
| `backup.retention.keep_last` | `30` | The retention window `tapdb backup prune` enforces — see §9.1. Also recorded in every manifest. Object Lock and lifecycle rules remain infrastructure-side. A value below 1 refuses the run rather than deleting everything. |
| `backup.expected_interval_hours` | `0` | `0` disables the `stale` status entirely. Set it if you rely on a schedule. A value that is present but not a number **fails** `backup health` rather than silently defaulting to `0` — a typo here would otherwise disarm the only scheduler-stopped detector there is. |
| `backup.health_verify_max_bytes` | `1073741824` | Artifact size above which `backup health` skips the full checksum read and defers to `backup verify`. `0` means no limit. Size *comparison* is unaffected and always runs. |
| `backup.expected_rehearsal_interval_days` | `0` | `0` disables the rehearsal-age check. The runbook recommends quarterly (`90`); it is opt-in so upgrading does not start failing every existing deployment for a cadence nobody chose. |
| `backup.encryption.mode` | `none` | must be `none` in this release |
| `backup.signing.mode` / `.kms_key_arn` | `none` / `""` | `kms` is designed but not implemented |
| `backup.provider_snapshots.enabled` / `.cluster_identifier` | `false` / `""` | Aurora only |
| `backup.rehearsal.database_prefix` | `tapdb_rehearsal` | names the scratch database rehearsals create |
| `backup.receipt_mirror` | `{}` | a second location for receipts, written after each receipt is published locally. Best-effort and **write-only**: verification always reads the local chain, so the mirror is evidence for a human or an auditor, not a recovery path the code falls back to. A mirror that falls behind is surfaced by `backup health`, not by a failed backup. |

To confirm what the loader actually resolved:

```bash
python -c "from daylily_tapdb.cli.db_config import get_backup_settings; \
import json; print(json.dumps(get_backup_settings(config_path='PATH'), indent=2))"
```

---

## 11. Surface parity

All three surfaces call the same service functions — enforced at runtime by
`tests/test_backup_surfaces_contract.py`, which drives all three at one spy and
fails if any reaches different code.

| Operation | CLI | Admin API | GUI |
|---|---|---|---|
| Plan | `backup plan` | `GET /api/backups/plan` | — |
| List + status | `backup list` | `GET /api/backups`, `GET /api/backups/status` | `/admin/backups` |
| Create | `backup create` | `POST /api/backups` → 201 | `POST /admin/backups/create` |
| Verify | `backup verify` | `POST /api/backups/{ref}/verify` | `POST /admin/backups/{ref}/verify` |
| Stage a restore | `backup restore-plan` | `POST /api/backups/{ref}/restore/stage` | `GET /admin/backups/{ref}/restore` |
| Apply a restore | `backup restore` | `POST /api/backups/{ref}/restore/apply` | `POST /admin/backups/{ref}/restore` |
| Rehearse | `backup rehearse` | `POST /api/backups/{ref}/rehearse` | `POST /admin/backups/{ref}/rehearse` |

Admin API routes require an authenticated admin. GUI backup routes are
admin-only and refuse everyone else with **403**.

### API status codes

| Code | Meaning |
|---|---|
| 400 | malformed request — bad reference, unknown class, missing required field |
| 403 | policy forbids it outright; retrying will not help |
| 409 | conflicts with current state — wrong label, stale stage, version mismatch. Re-stage and retry. |
| 422 | the artifact failed verification. For restores this is raised **before** the target is touched. |
| 404 | no such backup |

---

## 12. Troubleshooting

| Symptom | Cause | Action |
|---|---|---|
| `create` refuses with a drift finding | schema objects outside the expected inventory | identify them; re-run with `--allow-drift` once understood |
| Restore refused as `stale_stage` | state changed between staging and applying | re-run `restore-plan` and review the new plan before applying |
| Restore refused with a confirmation error | typed label does not match | copy it exactly from `restore-plan`; it is `client/db/schema@database` |
| `version.compatible` fails preflight | the backup came from a newer PostgreSQL than the target | PostgreSQL restores forward only. Restore onto an equal or newer server. |
| Isolated restore left a database behind | verification failed; it is kept deliberately | inspect it, then drop it manually. Look for `quarantined: true`. |
| Exit code `2` from a scheduled job | could not run — config or connectivity | check config path and database reachability; this is not a backup failure |
| `receipt_chain.ok == false` | receipts edited or truncated | treat the audit trail as untrustworthy; investigate host access |

---

## 13. Legacy commands

`tapdb db data backup` and `tapdb db data restore` are **deprecated**. They keep
their current behavior and emit a deprecation notice pointing here; removal is a
later major release. They cover only five tables, with no compatibility proof,
no sequence reconciliation, and no verification — do not use them for recovery.

Use `tapdb backup create` and `tapdb backup restore`.
