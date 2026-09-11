# Proposed TapDB 10.1.2: lineage within explicit service scopes

**User approved TapDB 10.1.2 tag and pip release at 2026-09-11 17:16 UTC.**
No PR or broad tests. TapDB 10.1.1 is already tagged and pip-installable.
Production remains unchanged; this release completes the approved allowlist.

## Concrete incompatibility

The Ursa read-only census found 536 active links with active endpoints whose
tenant values are not all identical. Ten relationship types include shared
catalog entries connected to tenant submission requests, profiles connected to
controller leases, and worksets connected to manifests. These are existing
persisted relationships; their original objects, IDs, tenant values and edges
must remain intact.

TapDB 10.1.1 RLS can read all six approved scopes, but
`schema/rls.sql:tapdb_validate_lineage_endpoint_scope` still requires matching
tenant values except for its specific canonical global-XRF case. Its canonical
DAG builder enforces the same restriction. Catalog-to-submission and
profile-to-lease links are ordinary local lineage, outside that XRF exception.
Thus retaining the source data does not by itself preserve functional writes
or canonical DAG views.

## Bounded proposed change

1. After both endpoints pass ordinary caller RLS, active-row lookup, and exact
   domain/owner checks, permit a link for a runtime principal with an explicitly
   bound additional tenant. The lineage row still passes its own RLS WITH CHECK.
   Ordinary single-tenant principals retain their current rules.
2. Give the public DAG builders an explicit service tenant scope and pass the
   already verified runtime configuration through the native GUI/DAG routes.
   Reject endpoints outside that finite scope and any different domain/owner.
3. Install the updated canonical RLS through one native migration. No original
   data rewrites, custom application RLS, operator runtime, or resource changes.

This completes the allowlist feature for services that connect shared catalogs
or system objects to tenant-owned work. Other Dayhoff services using that
pattern can use the same substrate contract; their need has not been claimed
or separately audited in this Ursa task.

## Alternatives considered

- Reassigning tenants or duplicating shared catalogs would change the identity
  model and enlarge the migration, contrary to the approved preservation plan.
- Replacing all local cross-scope links with self-service XRF objects would
  require widespread Ursa query changes while old links must remain preserved;
  canonical native views would still encounter the old links.
- Hiding old links or forcing shallow graphs removes requested content.
- Custom RLS/monkeypatching or operator runtime fails the approved native
  privilege contract. Multiple runtime principals and cross-tenant routing are
  a larger redesign and do not solve the canonical graph restriction directly.

No small supported Ursa-only workaround preserves all approved behavior.

If release is declined, retain 10.1.1 and leave migration pending. Proposed
TapDB issue title: **Support lineage between explicitly allowlisted runtime
scopes**. This document contains its reproducible case and requested behavior;
no issue has been posted yet.

## Validation scope

Only changed-path tests: canonical PostgreSQL security catalog, service-scope
lineage writes and hidden-endpoint rejection, unchanged single-tenant/global-XRF
behavior, and bounded DAG scope/deleted-record behavior. First selected run:
8 passed, 1 test-fixture failure (missing required lineage `bstatus`); the
fixture was corrected and only that failed test was rerun: 1 passed in 1.21s.
All nine selected checks passed after that fixture correction. Logs:
`/tmp/tapdb-allowlisted-lineage-01.log` and `-02.log`.
No exhaustive tests, coverage campaign, CI, PR, merge, or production image.

Final route checks: canonical rooted GUI forwards the finite tenant scope,
visible GUI graph routes still render, and authenticated DAG-v2 data routes
work: 3 passed in 0.78s (`/tmp/tapdb-allowlisted-lineage-routes.log`).
Changed-file Ruff and formatting plus `git diff --check` are the release checks.
The release will tag the clean feature commit without pushing the branch;
the existing CI workflow triggers only branch pushes and PRs.
