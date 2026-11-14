# Bounded canonical identity authorization projection

Base: TapDB 10.1.3, commit `7eccfe1b4e0dc440ed08b97f8decf214648152a5`. Bloom consumer base: 10.0.5, commit `240c67d0711ff2ad0e0d899a0df19d3ad7418be5`. User explicitly authorized fixing the Bloom identity integration; source implementation is authorized, while release and production application remain lead-owned.

The established owner is UID 3364 / EUID M-SYS-9SX2, canonical actor/user/system, issuer daylily-tapdb, tenant null, active, with persisted ADMIN role. Existing Ursa token M-BBX-AXBJ retains that owner and internal_ro scope. No missing owner decision remains. Do not remap, duplicate or alter that identity/token.

## Native capability

Add a narrow authorization projection, accessible only through the existing authenticated runtime principal. Return UID, EUID, raw explicit role and active state; never return arbitrary json_addl, password/token fields, login/email or user listings. Resolve only an exact token-derived numeric UID. Same-issuer canonical users follow existing scope; cross-issuer users additionally require a reviewed operator-owned access grant for that exact runtime role and persisted UID/EUID. Initial cross-issuer support is limited to canonical global daylily-tapdb users in the same domain. Ordinary generic_instance SELECT/RLS and immutable runtime issuer/tenant binding remain unchanged.

The allowlist is native security control-plane state, analogous to runtime-principal binding, not an application object relationship or a copied identity. Runtime has no table privileges. A canonical SECURITY DEFINER routine, exact schema-owned and pinned search_path, reads the allowlist using session_user and rechecks current runtime binding. PostgreSQL function EXECUTE is no authority without the exact grant. Public grants are revoked. Missing/ungranted/malformed identities fail closed. Revocation takes effect on subsequent transactions without replacing credentials.

Expose native receipt-bound plan/apply grant and revoke through the existing runtime-principal CLI, with exact target/schema/catalog checks and selected nonsecret identity fields only. The existing migration workflow owns schema application; no ad hoc SQL or application-startup schema changes. The new package's canonical catalog validator must recognize and validate the new private table/function. Lead must apply the native migration, review/apply the exact UID/EUID grant, then adopt the Bloom consumer release; none of these production actions are performed by this source task.

## Bloom consumption

Use the public TapDB exact authorization resolver after existing token validation. Require active owner and explicit canonical Bloom role. Preserve Bloom group resolution and intersect effective authority with the declared token scope. Fix the current rank-filter bug that removes ADMIN entirely for internal_ro; the result must be READ_ONLY authority, never ADMIN. Keep truthful 403 for missing authorization and deny empty permission sets; no role default manufactures permissions.

## Boundaries

No tests, lint, coverage, CI, publication, build, deployment, credential changes, DB mutation, or delegation in this task. Source review and a dated final source note document the implementation and remaining production steps.
