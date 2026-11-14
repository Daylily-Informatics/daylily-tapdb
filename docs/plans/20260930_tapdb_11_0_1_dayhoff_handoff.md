# TapDB 11.0.1: Dayhoff adoption handoff

## Scope and result

The user approved the bounded backup fix and package release and intends to
migrate the Dayhoff services to this version. Package publication is separate
from service adoption. This handoff does not claim a live fleet inventory,
successful production backup, database adoption or service cutover.

11.0.1 lets native backup operations select an existing retained receipt journal
using `backup.receipts_directory`. It preserves the sealed family, history and
recovery-floor checks. It adds no schema change beyond 11.0.0 and changes no
identifier allocation or attribution contract. See the
[operator instructions](../backup-and-recovery.md#retained-receipt-journals-1101)
and [TapDB 11 adoption contract](../tapdb-11-integrity.md).

**Will a common version work?** Yes, this is a coherent package target. Consumer
compatibility must still be established for each service. Installing 11.0.1 in
an operator environment can unblock backup planning without rebuilding an
existing service image. Embedding it in every service requires new dependency
locks and final tagged service images. A 10.x consumer also needs the major
11.x attribution, revision and database-adoption work; a pin alone is insufficient.

## Source observations (2026-09-30, not production assertions)

Paths below are under `/Users/jmajor/projects/mega_dayhoff/repos_work/`.
These are existing migration checkouts; reconcile them with the current owning
branch and deployed revision before edits. Dirty checkouts contain unrelated
work that must be preserved. Pins describe the inspected working files.

| Service / checkout | Exact HEAD | Declared TapDB | Working tree | Next requirement |
|---|---|---|---|---|
| Atlas / `atlas-tapdb-globaldag-20260926` | `3e6d50b07b8fb2724bed0c020c31551bea0acd86` | 11.0.0 | dirty | Existing 7.1.0 consumer source/image prepared; backup, adoption and runtime binding pending. Update pin/lock for a later 11.0.1 image. |
| Bloom / `bloom-tapdb-globaldag-20260926` | `9a9128661c539635504195a3b37307a8aebaf305` | 11.0.0 | clean | Existing 10.1.0 consumer source/image prepared; same operational gates. |
| Ursa / `ursa-tapdb-globaldag-20260926` | `847494d144776c8afed037695605287218bbb48c` | 10.1.2 | dirty | Coordinate with active Ursa work; inventory all request/background writers before 11.x adoption. |
| Dewey / `dewey-tapdb-globaldag-20260926` | `92a3c80ccc6a5edd56cc623a4248732cea5f5d4f` | 10.1.1rc1 | clean | Reconcile newer source/deployment, implement and qualify the 11.x write contract. |
| Kahlo / `kahlo-tapdb-globaldag-20260926` | `a47a6cda9d56e9b83af93976ab04b49be165c441` | Git tag 10.1.7 | dirty | Reconcile newer source/deployment, implement and qualify the 11.x write contract. |
| QEO / `qeo-tapdb-globaldag-20260926` | `b6d22d2c0657328a08b56d4b5cbb62cbb898e8fa` | 10.0.0 | dirty | Reconcile current suspended/active operating state and writers before any cutover. |
| Zebra / `zebra-day-tapdb-globaldag-20260926` | `63edd9c5a7723fa358d43c8c250229f9a5a53309` | Git tag 10.1.7 | clean | Inventory native writes and embedded management operations; adopt 11.x. |
| Login / `lsmc-oauth-login` | `3da58721b9ad9d68acb504b333519aae9071b5d6` | none in pyproject | dirty | Confirm deployment dependency inventory; do not invent a TapDB dependency. |

Dayhoff's inspected `services/services.yaml` identifies these eight deployable
services. OWY is not in that deploy-default catalog. Include it only if the
live inventory establishes a native TapDB dependency; an API-only integration
does not need an embedded TapDB pin just to match the fleet.

## Bounded operational sequence

1. Freeze current service revisions, images, runtime configs, operator config
   identities, databases/schemas, recovery families and journals. Coordinate
   with other active tasks; never apply the stale checkout table as live truth.
2. Install published 11.0.1 in the existing native backup operator environment.
   Configure each **original** operator config with its canonical retained
   journal through `tapdb config update --backup-receipts-directory PATH`.
   Do not copy receipts, alter a family descriptor or use a new config identity
   to escape retained floors. Preserve file ownership and access restrictions.
3. Rerun native backup planning with the exact historical source contract and
   sealed family; then obtain and verify the required backup. The 19 isolated
   patch checks do not establish that the live backup will pass every later gate.
4. For every 10.x consumer, reconcile all human/service writers with mandatory
   structured attribution, expected revisions, native reference operations,
   owner validators and pooled/background-session behavior. Update dependency
   and lock once on the final reviewed source. Do not add inferred compatibility.
5. Use native fencing/adoption/runtime-role controls for each database. Observe
   mandatory history-baseline and recovery safeguards. Keep authoritative
   objects, earlier audit evidence, namespaces and allocator floors intact.
6. Release each approved consumer once and build its final tagged image on EC2.
   Deploy only selected services; preserve unrelated Compose changes. Do not
   invoke broad bootstrap, template reseeding or database initialization.
7. Check actual image/package identity, readiness and approved behavior. Record
   per-service backup/adoption/binding/cutover receipts. Update Dayhoff pins only
   to the deliberately selected service releases. Package or image publication
   is not deployment or behavioral acceptance.

Atlas/Bloom's original journals and blocked backup receipts are recorded in
Atlas `docs/plans/20260930_native_backup_receipt_directory_addendum.md` and its
controlling `20260929T192741Z_atlas_accession_revision_ledger.md`. That ledger
continues to own the accession work and live acceptance boundaries.

Stop and report missing native operations, family/identity conflicts, unsafe
concurrent work, missing exact credential authorization or an unqualified
consumer contract. No direct SQL repair, destructive recovery or broad test
campaign is implied by this handoff.
