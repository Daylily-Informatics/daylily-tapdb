# TapDB Runtime DDL Guard Ledger

## Objective

Prevent a configured TapDB runtime principal from creating objects in its
managed TapDB schema while leaving runtime DML unchanged.

This hardening does not attempt to intercept arbitrary clients using an
operator credential. Operator credentials remain migration-only secrets. No
UID, EUID, sequence, instance, lineage, audit row, or other persisted identity
may be rewritten or regenerated.

## Gate 0 Baseline

- Base: annotated `9.2.1` tag and `origin/main`, both at
  `8f704b50ee63f7258b218a84e4286f2a33dd22fd`.
- Branch: `codex/tapdb-runtime-ddl-guard`.
- Existing local/bootstrap behavior creates a distinct
  `NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION` runtime role
  and grants DML plus sequence usage, but schema apply did not explicitly
  revoke `CREATE` on an externally provisioned managed schema.
- Approved scope is deliberately narrow: one managed-schema privilege only.
- Out of scope: database-wide privilege changes, per-transaction privilege
  scans, event triggers, drift-mode expansion, consumer-repository code,
  operator credential misuse interception, and any identity or data change.

## Control Ledger

| ID | Requirement | Status | Evidence / terminal note |
|---|---|---|---|
| BASE-001 | Start from the exact released 9.2.1 commit | SUCCESS | Branch and tag peel to `8f704b50ee63f7258b218a84e4286f2a33dd22fd`. |
| SCOPE-001 | Remove the unapproved broader runtime scans and exclusive drift work | SUCCESS | Diff contains neither per-transaction DDL inspection nor a new drift mode. |
| DDL-001 | Revoke `CREATE` from `PUBLIC` and the configured runtime role only on the managed TapDB schema | SUCCESS | `_runtime_schema_create_guard_sql` runs after binding during fresh apply and migration. |
| DDL-002 | Fail if the configured runtime still has effective `CREATE` on that schema | SUCCESS | Guard verifies `pg_catalog.has_schema_privilege`; no database or unrelated schema is inspected or changed. |
| MIG-001 | Add an append-only privilege-only migration for 9.2.1 targets | SUCCESS | `20260903_031820_runtime_ddl_guard.sql`; no row, identity, or sequence allow markers. |
| DOC-001 | Document no runtime DDL and migration-only operator credentials | SUCCESS | README, AI directive, runtime guide, and consumer guide updated concisely. |
| TEST-001 | Prove runtime DML remains usable and raw `CREATE TABLE IF NOT EXISTS` fails | SUCCESS | Focused unit: 41 passed, 2 skipped; focused PostgreSQL: 2 passed. |
| TEST-002 | Pass complete local suite, quality/security/build gates, and PostgreSQL 16/17 CI | IN_PROGRESS | Local: 2,215 passed and 14 skipped with 95.23% branch coverage; changed production module 93.59%; Ruff, format, mypy, Bandit, verified-secret scan, release-contract tests, build, wheel-asset verification, Twine check, and installed-wheel CLI smoke passed. PostgreSQL 16.13/17 CI receipts remain pending. |
| PR-001 | Commit, push, and open a reviewed PR to `main` | OPEN | Complete local gates passed; commit and PR pending. |
| RELEASE-001 | After green merge, publish annotated `9.2.2`, GitHub release, and PyPI artifacts | OPEN | User approved 9.2.2 release; no tag or publication exists yet. |

## Final Report

All rows terminal: no.

Objective complete: no.
