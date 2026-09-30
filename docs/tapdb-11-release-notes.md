# TapDB 11.0.0 release candidate notes

Status: source implemented and locally qualified; **not published**. `11.0.0`
was absent from remote tags on 2026-09-29; recheck before tagging. Baseline:
10.1.11 at `8fb344ecac341dcf29fc84faf9d1e0bd0af2bd4c`. This is one major
package release; no consumer deployment follows automatically.

## Breaking contracts

- Every supported domain write now requires explicit transaction-local
  attribution v1. Old actor strings are not converted. Missing identity context
  fails rather than silently producing incomplete audit history.
- Native object update/delete and external association changes require expected
  revisions. Callers must handle conflicts and reread explicitly.
- Existing installations require the native, fenced `integrity-adopt` plan/apply
  process, followed by reviewed runtime principal admission. Ordinary schema
  apply cannot silently upgrade existing data.
- Runtime roles lose audit mutation privileges. Dedicated non-login writer
  routines append audit in the same transaction as domain changes.
- SQLAlchemy is constrained to `>=2.0,<2.1`, the qualified psycopg2 runtime.
- Embedded authenticated writers must supply stable issuer/subject identity;
  an unrelated host user UID cannot stand in for a native TapDB actor.

## New native capabilities

Identity-safe audit queries separate audit-entry identity from changed-object
identity. Full before/after revisions retain human/service assertions, actual
DB principal, database timestamps, top-level transaction identity and template
identity. Read-only object/graph history uses PostgreSQL visibility boundaries.
Guarded forward corrections preserve intervening history and return durable
retry receipts; application-owned corrections require registered validators.

External references can be independently registered/resolved without another
service or fabricated source object. Typed annotations and associations preserve
the original reference identity. Shared Python services back CLI and authenticated
HTTP interfaces. Reads create no objects, snapshots, leases or operation records.

Adoption preserves original rows/audit and observes a labeled completeness
baseline; only `reference/annotation/generic/1.0` and
`governance/correction_receipt/generic/1.0` are added. Current-contract restores
start a new history epoch, retain earlier evidence, and reject old boundary
tokens. Old-schema restores retain their source contract and require separate
explicit adoption before TapDB 11 writes.

## Qualification and limits

**38 focused checks passed on isolated PostgreSQL 16.13**, Python 3.13.13 and
SQLAlchemy 2.0/psycopg2. Qualification includes current and exact-10.1.11 restore,
separate adoption, audit denial/atomicity, identity overlap, context isolation,
MVCC history, correction concurrency/retry, reference concurrency, scope and
Python/CLI/authenticated-HTTP parity. Three third-party deprecation warnings
remain (Typer/Click and Starlette/httpx). No broad suite, service CI campaign,
production database, AWS resource, consumer service, or live identity provider
was exercised.

Known pre-existing gap: standalone Cognito uses an import incompatible with its
pinned SDK. Host-session API tests do not prove standalone login works. No
Cognito/service configuration change is included.

Database owners/superusers remain able to defeat local database controls;
independent immutable preservation is tracked in [issue #117](https://github.com/Daylily-Informatics/daylily-tapdb/issues/117).
EUID issuance, namespaces, sequences and recovery rules are preserved, not
redesigned. Administrators remain responsible for nonoverlapping namespaces.
No PG17 qualification, Aurora cutover, performance/capacity claim, operating
backup schedule, historical backfill, physical laboratory rollback, or remote
object rollback is included.

## Consumer adoption

Use the [full native contract and operator sequence](tapdb-11-integrity.md).
Atlas/Bloom and other writers must supply verified initiating identities and
service envelopes, expected revisions and owner validators as applicable.
Operators must review backups, fenced adoption, runtime grants/bindings and pins.
Atlas sessions/search/tube editor and Bloom owner operations remain separate.
No PR, merge, image build, deployment or production acceptance is implicit in
publishing this package.
