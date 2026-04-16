# TapDB 6.0.x Refactor Overview: What Was Done

## Purpose

This document is a handoff summary for downstream orchestration and deployment work.
It captures the TapDB 6.0.x hard-cut refactor, the client migrations that were completed
against it, the release/tag state that was produced, and the specific areas a Dayhoff
follow-up agent should exercise when running full local and AWS-target deployment flows.

This is not the planning document. It is the "what actually changed" overview.

Historical note: the version matrix below records the original migration train.
The current package line can advance beyond those values; use `pyproject.toml`
and Git tags for the live release state.

## Final released baseline

The refactor landed as a breaking cut and was then stabilized with a TapDB patch release:

- TapDB: `6.0.0` initial hard cut, then `6.0.1` stabilization release
- Current stable TapDB package for clients: `daylily-tapdb==6.0.1`
- Meridian dependency in TapDB release: `meridian-euid>=0.4.1,<0.4.3`

The intent of `6.0.x` is:

- no backward compatibility
- no data migration work
- no compatibility shims
- no domainless template resolution
- no app-code identity model

This was treated as greenfield behavior.

## Core TapDB refactor

### 1. Meridian EUID model was hard-cut to canonical 0.4.1 behavior

TapDB no longer uses older local EUID assumptions. It now follows canonical Meridian
v0.4.1 shape:

- `DOMAIN-PREFIX-BODYCHECKSUM`
- domain is required
- prefix is required
- domainless forms are rejected
- `:` forms are rejected
- prefix validation is Meridian-safe and strict

TapDB uses Meridian terminology consistently:

- `domain_code`
- `owner_repo_name`
- `prefix`
- `category` in TapDB template taxonomy now means the Meridian prefix token

### 2. Runtime ownership moved from app-code semantics to repo ownership semantics

Old concepts were removed from active behavior:

- `TAPDB_APP_CODE`
- `issuer_app_code` as a runtime input
- `session.current_app_code`
- `euid_client_code`
- passive client-core prefix rewriting behavior

New runtime ownership input:

- `TAPDB_OWNER_REPO`
- `owner_repo_name`

Database/session scope now keys on:

- `session.current_domain_code`
- `session.current_owner_repo_name`

### 3. Governance moved to shared registries

TapDB now validates domain and prefix ownership using shared registry files instead of
passive library inheritance.

Expected shared files:

- `~/.config/tapdb/domain_code_registry.json`
- `~/.config/tapdb/prefix_ownership_registry.json`

Operational behavior:

- domain must exist in the domain registry
- prefix must exist for that domain in the prefix ownership registry
- prefix ownership must match the calling repo name
- if the prefix is absent, fail
- if the prefix exists but belongs to a different repo, fail

This was done to stop multiple apps in the same domain from passively inheriting the
same generic TapDB prefix and minting conflicting EUIDs.

### 4. Template identity stayed the same shape, but lookup semantics changed

Template taxonomy remains:

- `category/type/subtype/version`

But the meaning is now:

- `category == Meridian prefix`
- domain is a separate required selector

Effective identity is now:

- `(domain_code, category, type, subtype, version)`

Hard rule:

- all template reads, writes, uniqueness checks, registration, and lookup paths must include domain
- there is no valid domainless query path for `category/type/subtype/version`

This was an explicit non-regression requirement from the old behavior that needed to die.

### 5. Passive inheritance of TapDB generic objects was removed

Clients should not receive reusable generic TapDB objects unless they explicitly seed and
own approved prefixes for them in the correct domain.

TapDB no longer passively solves client prefixing by rewriting bundled generic templates.

TapDB-owned fixed internal prefixes were normalized around:

- `TPX` for template rows
- `EDG` for lineage rows
- `ADT` for audit rows
- `SYS` for `system_user`
- `MSG` for system messages

Bundled default template surface was reduced to TapDB-internal operational templates only.

### 6. Config and CLI were updated to the new contract

TapDB config/runtime flows now require repo-owner and registry-aware context:

- `meta.owner_repo_name`
- `meta.domain_registry_path`
- `meta.prefix_ownership_registry_path`
- explicit `domain_code`

Representative public contract changes:

- `TAPDBConnection(..., owner_repo_name=...)`
- `TAPDB_OWNER_REPO`
- explicit `--domain-code`
- explicit domain and registry-aware seed/init/bootstrap flows

## Documentation and test cleanup done in TapDB

Stale docs/tests referring to removed identity concepts were cleaned up, especially:

- `docs/identity-and-scoping.md`
- `docs/template-authoring.md`

The active TapDB docs now describe:

- repo-name ownership
- required domain scoping
- canonical Meridian EUID behavior
- no domainless template lookups

## Client migration pattern used downstream

The downstream migrations followed the same pattern used first in Atlas:

1. Pin `daylily-tapdb==6.0.1`
2. Keep exactly one live TapDB version source per repo
3. Remove local TapDB fallback or sibling-checkout default behavior
4. Replace `TAPDB_APP_CODE` style runtime inputs with `TAPDB_OWNER_REPO`
5. Require explicit domain code in TapDB flows
6. Remove domainless template lookup behavior
7. Remove app-minted TapDB object identifiers where they duplicate EUIDs

Single-source version rules used:

- direct client repos: `pyproject.toml` only
- Dayhoff: `services/pins.toml` only

## Atlas-specific cutover

Atlas was the largest TapDB-facing client cleanup and set the migration pattern for the
other repos.

Main Atlas changes:

- moved to `daylily-tapdb==6.0.1`
- removed local-checkout/default-path TapDB wiring
- made `pyproject.toml` the only live TapDB version source
- made all TapDB template lookups domain-explicit
- removed generated TapDB-owned display identifiers

The Atlas EUID-only cut removed user-facing TapDB-owned identifiers such as:

- `MAN-*`
- `REL-*`
- `EXC-*`
- similar shipment/ticket/business-number style identifiers where the object already had an EUID

Rule after the cut:

- if a TapDB-owned object has an EUID, that EUID is the identifier shown to users
- do not mint a second app-level synonym identifier for the same TapDB object
- genuine external identifiers may remain if they represent outside-world identity

## Other client repo outcomes

### Bloom

- migrated to `daylily-tapdb==6.0.1`
- single-source TapDB pin in `pyproject.toml`
- no local TapDB fallback
- targeted env/runtime proof passed

### Dewey

- migrated to `daylily-tapdb==6.0.1`
- single-source TapDB pin in `pyproject.toml`
- old fallback/version duplication removed
- targeted validation passed

### Kahlo

- migrated to `daylily-tapdb==6.0.1`
- app-code/runtime semantics removed
- single-source TapDB pin in `pyproject.toml`
- targeted validation passed

### Zebra Day

- migrated to `daylily-tapdb==6.0.1`
- single-source TapDB pin in `pyproject.toml`
- fresh-env proof against published packages passed
- printer/print-job surfaces were moved toward EUID-first handling for TapDB-owned identity

### Ursa

- migrated to `daylily-tapdb==6.0.1`
- single-source TapDB pin in `pyproject.toml`
- no sibling-checkout TapDB fallback
- fresh-env TapDB/Meridian proof was completed

Post-release Ursa follow-up on branch:

- `daylily-ephemeral-cluster` moved to `2.0.3`
- workset-monitor configs now pin `daylily-omics-analysis` repo tag `0.7.641`
- Ursa activation no longer treats `daylily-omics-analysis` as a required pip runtime import
- targeted fresh-env validation for this follow-up passed locally

Important: this Ursa follow-up was done after the main release chain and may not yet be
present in the currently pinned Dayhoff service tag.

## Release/tag matrix produced by the migration

- TapDB: `6.0.1`
- Zebra Day: `v0.6.2` (package `0.6.2`)
- Atlas: `2.0.1`
- Bloom: `v0.11.17` (package `0.11.17`)
- Dewey: `1.2.4`
- Kahlo: `0.1.13`
- Ursa: `0.6.16`
- Dayhoff: `2.1.1`, then `2.1.2`

## Dayhoff pin cascade state

Dayhoff was updated so `services/pins.toml` is the single source of truth for service refs.

Pinned values in the current Dayhoff release line:

- TapDB: `6.0.1`
- Zebra Day: `v0.6.2`
- Atlas: `2.0.1`
- Bloom: `v0.11.17`
- Dewey: `1.2.4`
- Kahlo: `0.1.13`
- Ursa: `0.6.16`

Important Dayhoff nuance:

- the latest Dayhoff tag is `2.1.2`
- inside that release line, `services.dayhoff.tag` was intentionally updated to `2.1.1`
  as part of the two-step Dayhoff release pattern
- this is expected for the current tagged state and should not be "fixed" casually

## What is intentionally unsupported now

The following are not supposed to work anymore:

- `TAPDB_APP_CODE`
- `issuer_app_code` as a live runtime concept
- `session.current_app_code`
- `euid_client_code`
- domainless template lookup
- passive inheritance of generic client-usable TapDB prefixes
- app-level synonym identifiers for TapDB-owned objects when an EUID already exists
- local TapDB fallback logic in normal client activation flows
- local Meridian fallback logic in client repos

## What the next Dayhoff-focused agent should validate

The next agent working from the current tagged Dayhoff release should try to:

1. Run the full Dayhoff local build end to end.
2. Run the full Dayhoff test suite end to end.
3. Run both AWS target deployment workflows end to end.
4. Confirm that every service deployed through Dayhoff still boots and resolves the
   TapDB 6.0.1 contract correctly.
5. Use these full deploy/build runs to identify lingering TapDB migration issues.
6. Use the same run to finish stabilizing the deploy/build flow in both target-region workflows.

## Specific things that Dayhoff validation should look for

- stale references to:
  - `TAPDB_APP_CODE`
  - `issuer_app_code`
  - `current_app_code`
  - `euid_client_code`
- any service still assuming domainless template lookup
- any service script still relying on local TapDB fallback behavior
- any service still using removed Atlas number-based routes or old Atlas object identifiers
- any deployment bootstrap path that fails to provide:
  - `TAPDB_OWNER_REPO`
  - explicit `domain_code`
  - registry file paths
- any service showing user-facing TapDB object identifiers other than EUID where the
  object is TapDB-owned

## Likely hotspots for lingering issues

- Dayhoff deploy/runtime generators and manifests
- Atlas integration assumptions in service workflows
- Ursa cluster/workset orchestration, especially if Dayhoff deploys surface the newer
  `daylily-ephemeral-cluster` or analysis-repo follow-up needs
- any scripts or templates that still assume old number-based Atlas URLs
- any bootstrap/config logic that still expects old app-code terminology

## Short summary

TapDB was hard-cut from the old client/app-code model to repo-owned, domain-scoped,
registry-governed Meridian behavior. Clients were moved to `daylily-tapdb==6.0.1`,
domainless template behavior was removed, app-minted TapDB object identifiers were removed
where they duplicated EUIDs, and Dayhoff was updated to pin the new service release chain.

The next useful step is not more planning. It is running the full Dayhoff local build,
full test suite, and both AWS target deployment workflows against the current tagged
Dayhoff state to flush out anything still assuming pre-6.0 TapDB behavior.
