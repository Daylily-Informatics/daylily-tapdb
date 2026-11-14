# Reserved-domain authorization

TapDB reads explicit Meridian authorization from the canonical v4 config's
`meta.governance_authorization` object. This is separate from
`meta.owner_repo_name`, which identifies the repository that owns prefix claims.
Neither the registry's owner nor the runtime identity or `safety.safety_tier`
supplies authorization on behalf of the caller.

For an explicitly authorized LSMC production consumer using domain `M`, retain
the existing physical target, namespace, owner, domain and registry paths, and
add this metadata:

```yaml
meta:
  governance_authorization:
    reserved_for: LSMC
    deployment_environment: production
    approval_tokens: []
```

The object accepts only these fields:

| Field | Input | Meaning |
| --- | --- | --- |
| `reserved_for` | Nonempty string when supplied | Explicit reserved-domain owner authorization |
| `deployment_environment` | Nonempty string when supplied | Caller-declared deployment environment |
| `approval_tokens` | Array of nonempty strings | Explicit approval evidence required by Meridian policy |

Missing owner/environment values remain missing. An absent token array means
no approval tokens. Strings are not split into tokens, and unknown fields or
malformed values fail. No environment-variable override or identity discovery
is used. The SDK also accepts an immutable `GovernanceAuthorization` object;
its token collection is copied to a tuple.

Meridian remains the policy authority. Reserved domains require the matching
`reserved_for` value. Under the existing LSMC `M` registry policy,
`deployment_environment` is required; non-production use requires exactly three
distinct explicit approval tokens. Production does not require those tokens.
TapDB does not alter the registry, substitute an owner, or relax prefix claims.
Unrestricted domains can continue without an authorization object.

## Native config controls

For an existing canonical config, including an operator config without admin UI
sections, use the dedicated native operation:

```sh
tapdb --config /absolute/path/tapdb.yaml db-config set-governance-authorization \
  --authorization-json '{"reserved_for":"LSMC","deployment_environment":"production","approval_tokens":[]}'
```

This replaces only the complete authorization object. It preserves all other
file bytes, values and file permissions, validates the resulting DB/governance
contract before atomic replacement, and rejects an observed concurrent config
change. YAML anchors, aliases and ambiguous metadata keys fail explicitly.
It never fills in unrelated admin sections.

The operation reads the canonical config and namespace metadata without first
requiring successful runtime governance resolution. It can therefore add
explicit authorization to an otherwise valid config that currently fails with
`reserved_for is required`. Adding this metadata does not change target host,
database, schema, namespace, owner, domain or registry paths. It does not connect
to the database or change a runtime principal.

The general `db-config init` and `db-config update` commands also accept
`--governance-authorization-json` for full native configurations. That option
replaces the complete authorization object; omission preserves it, including
when re-running init. These general commands retain their existing config
serialization and required admin mappings. They validate the object's shape;
runtime config resolution evaluates Meridian policy. An empty object does not
authorize a reserved domain.

## Native consumers and template APIs

`get_db_config()` returns the validated envelope as `governance_authorization`
(or `None` when absent). The CLI, GUI readiness and Meridian validation,
template seed/import, and integrity template installation forward this value
to their native governance checks.

Direct SDK callers of `GovernanceContext.load()`, `assert_registered_domain()`,
`seed_templates()` and `import_repository_pack()` supply the same explicit
keyword:

```python
governance_authorization=cfg["governance_authorization"]
```

Direct template seed/import checks enforce reserved-domain authorization before
using the database session, including repository-import dry runs. Prefix
ownership, repository receipts, immutable template identities and exact bundled
core-template delegation remain separate checks. An authorization object is
never stored in template definitions or copied into a template pack.
