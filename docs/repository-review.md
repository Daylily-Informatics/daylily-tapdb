# TapDB 10.0 Repository Reconciliation

This is a current-code reconciliation for the TapDB 10.0 release line. It
records what was reviewed, the contracts the repository now exposes, the
deliberate hard cuts, and the work that remains outside TapDB. It is not a
future documentation plan or a historical migration memo.

## Reviewed Areas

The reconciliation covered:

- package metadata, installed CLI registration, and the canonical v4 config;
- template storage, repository-owned packs, database backup classes, and
  governed object operations;
- Meridian identity, domain/owner/tenant scope, lineage, and external
  references;
- the shared sanitized runtime-information payload across CLI and the canonical
  GUI/API;
- authenticated DAG v2 mounting, exact external-reference search, and bounded
  federation;
- backup, restore, receipt, and release-documentation contracts.

The primary operator and adopter references are the root
[README](../README.md), [Runtime and CLI](runtime-and-cli.md),
[Template Authoring](template-authoring.md),
[Identity and Scoping](identity-and-scoping.md),
[Integration and Embedding](integration-and-embedding.md), the
[Consumer Discoverability Guide](consumer-discoverability-guide.md), and
[Backup and Recovery](backup-and-recovery.md).

## Current Architecture

TapDB is a reusable typed-object substrate. Templates define allowed object
shape, generic instances persist objects, `generic_instance_lineage` is the
authority for durable relationships, and audit records preserve
actor-attributed change evidence. Host services retain responsibility for
business meaning, authorization policy, and external orchestration.

Runtime operation is explicit-target and config-first. A v4 config contains one
`target` plus `meta`, `admin`, and `safety` sections; there is no environment
selector or service-side target discovery. The canonical example is
[`config/tapdb-config-example.yaml`](../config/tapdb-config-example.yaml).

TapDB pins `meridian-euid==0.4.8`. Domain identity comes from the explicit
config and registry paths. Prefix ownership is separately governed by the
prefix ownership registry. The effective owner-scoped template identity is
`(domain_code, issuer_app_code, category, type, subtype, version)`.

## Safety and Identity Findings

- Repository-owned template packs are deterministic and database-identity-free.
  Their adjacent export receipts retain source EUIDs and timestamps as required
  provenance, are written read-only, use the pack basename for cross-checkout
  portability, and bind content, domain, owner, and prefix-registry evidence by
  checksum.
- `tapdb templates import` is validation-only unless `--apply` is explicit.
  Export refuses overwrite, and all repository-pack commands require one
  explicit absolute `.json` path.
- Repository-owned template packs are distinct from the database-derived
  `tapdb backup create --class template-pack` artifact. Neither is a substitute
  for a full recovery point.
- Governed object mutation uses narrow allowlists, exact selectors, audit
  evidence, dry-run defaults where applicable, and soft deletion rather than a
  physical-delete shortcut.
- The runtime-info contract exposes package/Python/Meridian, Git, config
  identity, database reachability, scope, storage, UI, and DAG status without
  raw config, environment dumps, Cognito fields, passwords, or tokens.
- DAG v2 routes are mounted atomically behind an explicit host authentication
  dependency. There is no v1 router, outbound proxy, alias resolution, or
  compatibility endpoint.
- Canonical cross-service relationships are typed XRF objects connected by
  persisted lineage. The public lifecycle owns exact identity, replay,
  detach/reactivation, authority-scoped reconciliation, and reverse lookup.
- `daylily_tapdb.gui` is the sole web implementation and retains every valid
  former `admin.main` feature, including its rich graph interactions. The
  duplicate external-link writer was intentionally removed.
- Meridian-shaped EUIDs are accepted only from persisted TapDB rows or another
  owning service. Display labels, external IDs, hashes, and idempotency keys do
  not become synthetic EUIDs.

## Deliberate Hard Cuts

TapDB 10.0 intentionally does not provide ambient config fallback, `--env`
selection, implicit localhost/public-schema behavior, prefix derivation,
domainless or ownerless template lookup, offset pagination, hard object delete,
anonymous legacy DAG routes, DAG v1, outbound graph proxying, generic XRF
writes, or inferred graph edges from copied metadata. Missing config,
governance evidence, authentication, or an exact repository-pack path fails
clearly.

## Remaining Downstream Work and Non-Goals

Consuming services must adopt the v4 config, supply their real auth dependency,
register their exact DAG v2 service identity, adopt canonical external
references, own their domain templates and prefix claims, and test their
mounted integration. Infrastructure owners remain
responsible for PostgreSQL roles, TLS, backup scheduling and durable storage,
provider snapshot cutover, and alert delivery.

TapDB does not define domain workflows, create organization-specific templates,
choose tenant policy, discover a fleet, own transport credentials, validate
remote business state, or perform a deployment on behalf of consumers. The
federation client calls only the exact services and authenticated transport an
application supplies. Those are downstream integration concerns, not missing
fallback behavior in this repository.
