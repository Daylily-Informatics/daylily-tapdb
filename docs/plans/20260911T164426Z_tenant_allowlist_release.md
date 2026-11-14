# TapDB 10.1.1: explicit runtime tenant allowlist

The Ursa migration requires one constrained runtime principal to access its
existing application, system-queue and historical tenant scopes. The user
approved this bounded substrate change on 2026-09-11, then directed a tagged,
pip-installable feature release without a PR or exhaustive tests. Merge remains
deferred until the migrations finish and no further small releases are needed.

Source starts at `10.1.1rc1`, commit
`02ab7c9d0325dfcbf9dc47a03be66b62e0a22f2c`, in feature branch
`codex/tapdb-tenant-allowlist-20260911`. The ordinary dirty checkout is untouched.

## Contract

- `target.additional_tenant_ids` is an optional finite list of canonical UUID
  strings. The public connection keyword has the same name. Empty means no
  additional tenants; the explicitly configured primary `tenant_id` retains
  its existing meaning. Duplicate values and malformed/non-list values fail.
- Bind the complete sorted list through the existing runtime-principal
  preflight/apply receipt. The binding remains immutable. Changing it requires
  a separately configured principal and a new explicit binding.
- RLS resolves the primary tenant plus this allowlist from `session_user`.
  Request GUCs only assert the configured scope and cannot grant new access.
  Domain, owner, global-row-write policy, endpoint-scope constraints, denied
  TEMP/DDL, and ownership/membership restrictions are retained.
- Migration `20260911_164426_runtime_tenant_allowlist.sql` adds an empty array to
  existing scope rows and installs the canonical RLS asset. Existing bindings
  receive no additional tenant access. Native identity verification permits
  the declared added column while retaining every original column value.
- Active rooted DAG traversal omits deleted endpoints and their connecting
  edges. No historical object or lineage is modified or undeleted.

The application still owns per-user authorization. An allowlist grants the
service its configured data scope; it does not grant every application user
access to every tenant.

## Focused validation and release

At 2026-09-11 16:44 UTC, **29 targeted tests passed in 3.59 seconds**, including
four actual PostgreSQL 16.14 tests. Those cover the canonical policy/routine
catalog, primary/additional/global read and write behavior, owner isolation,
scope assertion, immutable/replayed binding, forbidden TEMP/DDL, attempted GUC
scope widening, config/connection forwarding, and retained deleted DAG records.
Changed Python files passed Ruff and `git diff --check`.

No broad suite, coverage campaign, CI run, PR, merge, production database change,
or production image build is part of this TapDB release. Publish an annotated
non-v tag `10.1.1` from the checked source and the corresponding wheel/sdist via
the existing `twup` function. The controlling Ursa ledger records publication
receipts and the resulting exact Ursa dependency pin.
