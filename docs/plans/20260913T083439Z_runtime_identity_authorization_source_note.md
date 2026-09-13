# Native runtime identity authorization — source handoff

## Implemented scope

Source only, based on TapDB 10.1.3 commit `7eccfe1b4e0dc440ed08b97f8decf214648152a5`, isolated branch `codex/ursa-bloom-identity-20260913`. The companion consumer is in Bloom's existing gap branch at 10.0.5 commit `240c67d0711ff2ad0e0d899a0df19d3ad7418be5`. The preceding source plan is `20260913T082300Z_bloom_identity_projection_source_plan.md`.

The existing Ursa token owner remains UID `3364`, EUID `M-SYS-9SX2`, issuer `daylily-tapdb`. The runtime remains `bloom_runtime_10`, owner scope `bloom`, domain `M`, database `tapdb_bloom_prod`, schema `tapdb_bloom_lsmcok1_local`. No owner choice remains unresolved: the existing persisted identity is authoritative. No code embeds a deployment UID, EUID, principal or credential.

## Changed files and contracts

- `schema/runtime_identity_authorization.sql`: adds private `tapdb_runtime_identity_access` policy rows keyed by runtime role and exact canonical user UID, preserving EUID, enabled state, approving operator, timestamp and reviewed plan SHA-256. Forced RLS remains enabled, only the exact schema operator receives its access policy, and runtime receives no table privileges. These rows are native authorization policy, not application relationships or copied users. Existing `schema/rls.sql` is unchanged.
- The same canonical asset adds `tapdb_resolve_user_authorization(BIGINT) RETURNS JSONB`. It asserts the authenticated `session_user` against the immutable runtime binding, uses schema-qualified dynamic relation/function references with the exact operator-owned schema pinned first and `pg_catalog, pg_temp` last, and returns only `uid`, `euid`, `role`, `is_active`. No login/email, arbitrary properties, credentials, password hashes, tokens, or enumeration endpoint. Invalid/ungranted identities return null. Same-issuer canonical identity reads respect existing tenant scope; a cross-issuer identity requires an enabled exact UID/EUID grant, the same domain, canonical `actor/user/system/1.0`, issuer `daylily-tapdb`, and tenant null. Current active state is calculated on every read. Ordinary object RLS predicates and the runtime's immutable issuer/tenant scope are unchanged.
- `schema/migrations/20260913_082300_runtime_identity_authorization.sql`: includes only the new additive identity asset. It does not update historical audit rows or reapply existing RLS. `pyproject.toml` packages the new asset; `daylily_tapdb/schema_inventory.py` inventories it; `daylily_tapdb/cli/db.py` includes the same asset after RLS for fresh schema application. Existing migrations are unchanged. Native migration application is a separate required operator step; installing a wheel does not apply it.
- `daylily_tapdb/user_authorization.py`: public exact positive BIGINT lookup and a frozen minimal `UserAuthorization` result. Rejects invalid/mismatched response shapes and unknown fields. No less-restricted query or alternate lookup if capability is unavailable.
- `daylily_tapdb/runtime_principal.py`: native `set_runtime_identity_access` creates a read-only plan, then applies only the unchanged reviewed target/catalog/identity/current-grant plan in a serializable operator transaction. Granting requires an active canonical global identity with an explicit role. It changes only the exact grant and, when granting, EXECUTE on this function for the selected runtime. Revocation marks that grant disabled; no user or token deletion. Application records the plan digest and operator in the DB and writes a separate result receipt. Existing result, stale state, changed catalog/role/identity or replay is rejected. New private tables are included in native catalog ownership/forced-RLS/permission checks and never receive ordinary runtime table grants.
- `daylily_tapdb/runtime_catalog_contract.py`: recognizes JSONB return metadata, verifies canonical function source/owner/search path through the existing packaged contract, and rejects PUBLIC EXECUTE on the identity function. Schema SQL explicitly revokes PUBLIC EXECUTE.
- `daylily_tapdb/cli/runtime_principal.py`: thin `db runtime-principal identity-access` plan/apply interface with exact UID/EUID, explicit grant/revoke and reason, and protected receipt path. No user-list or credential surface added.

## Lead-owned production sequence

Grant-time and read-time identity checks also bind the joined template to the actor's domain/issuer, canonical `actor_template` discriminator and `actor_instance` instance identity, active/undeleted status and version. Cross-issuer grant requires both actor and template global; same-issuer reads allow a global or actor-aligned template tenant. Imported malformed or differently scoped templates are rejected rather than treated as canonical from their coordinate strings alone.

1. Recheck the next unused numeric TapDB version (10.1.4 was absent from local tags during this source task), publish the final reviewed source, and install that exact release into the existing operator tooling. No release or build was performed here.
2. Through the existing supported TapDB migration plan/apply workflow and its required production fencing, apply only this pending additive migration to Bloom's exact database/schema. The new function must be owned by the configured native schema operator; no runtime superuser/BYPASSRLS/DDL authority is needed. This migration contains no historical audit UPDATE and does not reapply existing object RLS. Inspect the native plan rather than treating package adoption as schema application.
3. Plan the one authorized runtime→identity grant through the new native CLI using the existing protected operator context. Confirm the receipt identifies `bloom_runtime_10`, Bloom's exact database/schema and unchanged binding, and UID `3364` / EUID `M-SYS-9SX2`. Example command contract (choose a new durable absolute receipt path):

```sh
tapdb --config /home/ubuntu/bloom_ops/tapdb101-20260911/operator.yaml \
  --client-id bloom --database-name bloom-day \
  db runtime-principal identity-access \
  --user-uid 3364 --user-euid M-SYS-9SX2 --grant \
  --reason 'Restore authorization lookup for the existing Ursa token owner' \
  --receipt /absolute/reviewed/identity-access-plan.json
```

Apply uses that same command and receipt plus `--apply`. This is a new native capability command contract, not an already executed production command. It grants no user enumeration and leaves ordinary `generic_instance` reads of the global owner hidden. No general rebind or issuer change is required.

The first-grant precheck permits the new routine to lack EXECUTE: `_verify_bound_permissions` checks forbidden effective object privileges and EXECUTE WITH GRANT OPTION for canonical routines; it does not require every allowed privilege to be present. Postcheck uses the approved routine signature set and still rejects grant-option/table leaks, so ordinary EXECUTE added by this operation is permitted. Plan/catalog equality is checked before the grant, not against the changed post-grant ACL. The exact identity grant's changed state and separate result receipt reject replay.

4. Publish/adopt the companion Bloom source with an exact pin to the real published TapDB release and a regenerated `uv.lock` from its real artifact. The existing Bloom Dockerfile uses `uv sync --frozen`; this source task intentionally does not invent unpublished package hashes. Lead must complete the pin/lock after publication before the final EC2 build. No other service dependency is changed.
5. Read back the real authenticated Ursa→Bloom sequencing request and its existing token identity. A successful source review, wheel publication or function grant is not a live search acceptance receipt.

## Companion Bloom behavior and review limits

Bloom resolves only the validated token's exact numeric owner UID through the public native API, rejects missing/inactive/roleless identity, and keeps the generic 403 denial. It preserves group resolution, then intersects actual owner permissions with existing token-scope permissions. ADMIN + internal_ro yields only the existing READ_ONLY permission set; there is no role/token mutation or empty-role default. APIUser preserves an explicitly empty permission set, intersects supplied permissions with role authority, and enforces that set in has_permission/is_admin/can_write; only None means derive role permissions. The token remains `M-BBX-AXBJ`, owner `3364`, scope `internal_ro`.

Source was reviewed with targeted reads and diffs. No tests, lint, coverage, CI, imports/executable checks, package publication, container builds, deployments, credentials, DB mutations, production calls or delegation were performed during this source implementation. SQL execution and native migration acceptance remain lead-owned production evidence, not claimed here.
