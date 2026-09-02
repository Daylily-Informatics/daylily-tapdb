# TapDB 9.2 Repository Reconciliation

This is a current-code reconciliation for the TapDB 9.2 release line. It
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
- the shared sanitized runtime-information payload across CLI, API, embedded
  GUI, and legacy admin;
- authenticated DAG v2 mounting and the isolated legacy DAG v1 boundary;
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
  dependency. The separate legacy v1 router also requires an explicit callable
  authentication dependency; its outbound proxy remains disabled unless an
  exact restrictive policy is provided.
- Meridian-shaped EUIDs are accepted only from persisted TapDB rows or another
  owning service. Display labels, external IDs, hashes, and idempotency keys do
  not become synthetic EUIDs.

## Deliberate Hard Cuts

TapDB 9.2 intentionally does not provide ambient config fallback, `--env`
selection, implicit localhost/public-schema behavior, prefix derivation,
domainless or ownerless template lookup, offset pagination, hard object delete,
anonymous legacy DAG routes, DAG v1 fallback for v2, or inferred graph edges
from copied metadata. Missing config, governance evidence, authentication, or
an exact repository-pack path fails clearly.

## Remaining Downstream Work and Non-Goals

Consuming services must adopt the v4 config, supply their real auth dependency,
register their exact DAG v2 service identity, own their domain templates and
prefix claims, and test their mounted integration. Infrastructure owners remain
responsible for PostgreSQL roles, TLS, backup scheduling and durable storage,
provider snapshot cutover, and alert delivery.

TapDB does not define domain workflows, create organization-specific templates,
choose tenant policy, fetch remote DAG v2 services, synthesize external object
identity, or perform a deployment or release on behalf of consumers. Those are
downstream integration concerns, not missing fallback behavior in this
repository.
