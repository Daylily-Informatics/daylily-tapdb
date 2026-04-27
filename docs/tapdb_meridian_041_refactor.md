# Meridian 0.4.1 Hard-Cut Branch Plan

## Summary
- Create feature branch `codex/meridian-euid-0-4-1-hard-cut`.
- Make TapDB a hard cut to canonical Meridian `DOMAIN-PREFIX-BODYCHECKSUM`.
- Replace passive core-prefix inheritance with repo-owned prefix governance checked against the shared registries in `~/.config/tapdb/`.
- Use repo name as the sole ownership token for prefix claims. Atlas becomes `lsmc-atlas`; TapDB internal ownership becomes `daylily-tapdb`.
- Keep templates as `category/type/subtype/version`, but treat `category` as the Meridian prefix token and require domain on every template lookup.
- Remove Atlas-generated user-facing business numbers for TapDB-owned objects. If an object has an EUID, the EUID is the displayed and routable identifier.

## Multi-Agent Execution
1. Agent 1 owns Meridian compliance and runtime scope: replace TapDB’s local EUID logic with a thin adapter over `meridian-euid==0.4.1`, rename runtime/session/schema scope to `owner_repo_name` and `TAPDB_OWNER_REPO`, and update all domain/prefix validation to Meridian 0.4.x rules: required domain, required prefix, 1-4 Crockford Base32 chars, digits allowed, `:` and domainless forms rejected.
2. Agent 2 owns template governance and the TapDB core-pack hard cut: remove old client-code-derived prefix behavior, sandbox/production validation mode, and any placeholder prefix rewrite; make template seeding fail unless the domain exists, the prefix claim exists, the claim owner matches `owner_repo_name`, and the registries are mutually consistent; stop auto-seeding client-usable bundled templates and keep only TapDB-internal operational templates bundled.
3. Agent 3 owns CLI/config and Atlas migration: change TapDB config to require `meta.owner_repo_name`, `meta.domain_registry_path`, `meta.prefix_ownership_registry_path`, and env `domain_code`; remove environment prefix overrides and client-code-derived core prefixes; migrate Atlas activation, settings, TapDB runtime wiring, template seeding, and tests to `lsmc-atlas` / `TAPDB_OWNER_REPO`, update all old-shape EUID expectations to canonical `Z-AGX-*`, and remove generated identifiers like `MAN-*`, `REL-*`, `EXC-*`, and shipment/ticket-style Atlas numbers where the record already has an EUID.

## Key Changes
- Keep TapDB template taxonomy (`category/type/subtype/version`), but make `category` the Meridian prefix token and require `domain + category/type/subtype/version` for every template query.
- Treat the Meridian registry leaf field `issuer_app_code` as the repo-name token value. The field name stays upstream-compatible; the stored value becomes `lsmc-atlas` or `daylily-tapdb`.
- Give TapDB-owned operational prefixes fixed values and register them to `daylily-tapdb`: `TPX` for template rows, `EDG` for lineage rows, `ADT` for audit rows, `SYS` for `system_user` instances, and `MSG` for system messages.
- Keep only these bundled templates: `SYS/actor/system_user/1.0/` and `MSG/message/webhook_event/1.0/`. Do not auto-seed the old generic bundled templates.
- Create shared local registries in `~/.config/tapdb/` during migration and verification:
  - `domain_code_registry.json`: register `Z` as `localhost`
  - `prefix_ownership_registry.json`: under `Z`, claim `AGX -> lsmc-atlas` and `TPX/EDG/ADT/SYS/MSG -> daylily-tapdb`
- Atlas EUID-only cut:
  - Remove generators and lookups for `manifest_number`, `package_number`, `exception_number`, `shipment_number`, `ticket_number`, and similar TapDB-owned business-number fields.
  - Route params, response models, graph labels, and templates must use EUID for TapDB-owned Atlas records.
  - Keep genuine external identifiers such as carrier tracking numbers or physical kit barcodes only when they model real external data.

## Public Interface Changes
- `TAPDBConnection(..., owner_repo_name=...)` replaces `issuer_app_code=...`.
- `TAPDB_OWNER_REPO` is the runtime ownership input.
- `daylily_tapdb.euid.format_euid(prefix, seq_val, *, domain_code)` always requires `domain_code` and emits `DOMAIN-PREFIX-BODYCHECKSUM`.
- `validate_euid` becomes strict canonical validation only; remove environment-mode args and legacy-shape support.
- Template seed/validate entrypoints require `domain_code`, `owner_repo_name`, and explicit registry paths; `core_instance_prefix` disappears.
- Atlas routes and APIs that currently use generated business numbers move to EUID route params and EUID response fields.

## Test Plan
- TapDB unit and integration coverage for canonical encode/parse/validate, digit-bearing domain/prefix tokens, rejection of colon and missing-domain forms, registry load and consistency failures, wrong-owner prefix failures, and hard-fail core-template inheritance rules.
- Schema and runtime coverage for `session.current_owner_repo`, renamed columns/functions, RLS and row-scope consistency, TapDB-owned operational prefixes, and client-owned instance prefixes like `AGX`.
- Atlas coverage for activation exports, settings/runtime propagation, template seeding, registry enforcement, canonical `Z-AGX-*` EUID helpers, EUID-only repository APIs for manifests/releases/exceptions/shipments/support tickets, and all TapDB-backed repository tests that currently assume `A` or old EUID shapes.
- End-to-end acceptance: local TapDB bootstrap plus Atlas seed succeeds only when the shared registry files exist and claim `AGX` for `lsmc-atlas`, and hard-fails when the claim is missing or owned by another repo.

## Assumptions
- No backward compatibility, no shims, no colon-form parsing, no client-code fallback, and no automatic fallback to bundled registry data for issuance.
- The Meridian repo remains the source of truth for canonical format and registry structure; TapDB consumes it and does not modify it.
- Atlas currently has one real template prefix, `AGX`, and all 70 Atlas templates continue to use that prefix after the cut.
