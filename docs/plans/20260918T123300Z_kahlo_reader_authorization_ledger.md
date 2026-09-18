# Kahlo reader authorization: direct TapDB owner correction

Created 2026-09-18T12:33:00Z. Owner: Agent A; coordinator owns release, production application and the master ledger. This child ledger records source work and a read-only native preview. It does not authorize production mutation.

## Gate 0 and exact scope

- The reported Bloom403 was observed with Kahlo's existing configured native reader. No alternative service principal was substituted.
- Last established live Bloom release:10.0.6, commit`a67dee5dab4a2466f4534535b720f9560f261b6e`, image`sha256:590a8af40c0412c5e8d24c390597be253f12ac20642429ec4e338cd0b8ff2df2`, installed TapDB10.1.4. These deployment facts came from the coordinator and Agent C before the pause; they require a fresh deployment read before adoption.
- At12:31Z the existing operator environment independently reported TapDB10.1.5 and the exact configured target: database`tapdb_bloom_prod`, schema`tapdb_bloom_lsmcok1_local`, domain`M`, owner`bloom`, runtime`bloom_runtime_10`, operator`dayhoff`, no tenant, global claims enabled. No target configuration was changed.
- Read-only owner transactions at06:49–07:02Z verified`transaction_read_only=on`, zero pending ORM writes, and rollback/close. Native public owner helpers were explicitly approved by the coordinator because HTTP token authentication performs group/usage writes.
- Existing native token identity`M-BBX-DZPK`, ownerUID`9148`/EUID`M-SYS-9T5J`, scope`internal_ro`, ACTIVE revision30, expiry`2026-12-11T04:45:17.375690+00:00`, revoked_at null. No credential value, prefix or hash was retained. Public owner read returned active`READ_ONLY`; native authorization resolver returned null.
- The exact linked template isUID246/EUID`M-TPX-A05N`, Bloom-owned`actor/user/system/1.0`, active, undeleted, discriminator`actor_template`, tenant null. Its optional`instance_polymorphic_identity` is null. The installed packaged core template also omits this optional field. The actor's actual stored discriminator is`actor_instance`.
- Public exact actor/template audit contains only Sep12 provisioning: template INSERT and actor INSERT/initial role assignment. No later actor/template role, status, type or identity mutation was observed; the token has no revocation. This rules out a later revocation of the selected reader in the inspected owner evidence.
- The exact template has two bound active Bloom-owned instances:UID9148/`M-SYS-9T5J` (`READ_ONLY`) andUID9182/`M-SYS-9T6G` (`READ_WRITE`, created Sep16). Both store`actor_instance` and currently resolve to null. The fix also restores the second actor's already persisted authority; it does not create a grant or change its role.

Durable detailed diagnostics and the credential-free public read helper are in the companion Bloom worktree`/Users/jmajor/projects/mega_dayhoff/.codex-worktrees/bloom-kahlo-reader-auth-20260918/docs/plans/`: `20260918T064948Z_kahlo_owner_receipt.json`, `20260918T065220Z_kahlo_owner_receipt.json`, `20260918T065746Z_kahlo_identity_structure_receipt.json`, `20260918T070138Z_kahlo_template_census_receipt.json`, and`20260918T064925Z_read_kahlo_owner.py`. The census file retains its actual source times06:59:59Z; filename time is not an observation timestamp.

## Cause and supported correction

`daylily_tapdb/user_store.py:create_or_get` resolves the exact canonical system-user template and explicitly creates an`actor_instance`. The packaged`daylily_tapdb/core_config/actor/actor.json` omits the optional instance-creation hint. Generic factory source also supports this omission. Native authorization incorrectly made that optional hint an additional prerequisite despite separately requiring the actor's actual discriminator and exact active canonical template.

The approved change removes only the predicate`template.instance_polymorphic_identity = 'actor_instance'` from the native resolver asset and the matching native identity-access preflight. All actor type, exact active template, runtime binding, domain/issuer/tenant and explicit cross-issuer grant checks remain. There is no null substitution, inferred value, compatibility reader or new grant. No existing template, actor, token, role, scope, expiry or lineage is edited.

Native`objects update` and`objects repair` reject template mutations. The loader treats the field as part of the immutable loaded definition. Those guards were preserved; no template repair bypass was attempted.

## Execution ledger

| Row | Work | State | Completion evidence or remaining gate |
| --- | --- | --- | --- |
| B0 | Exact reader, template, active authority, revocation history and affected actor census | SUCCESS | Public owner-helper receipts and authenticated coordinator GUI; read-only rollback |
| B1 | Direct owning resolver/preflight correction | SUCCESS | Two predicate removals; coordinator source review accepted before preview |
| B2 | Additive native function migration | SUCCESS | `schema/migrations/20260918_070400_system_user_authorization_contract.sql`; replaces only function body and pins existing search path; existing function ACL/owner and all data stay unchanged |
| B3 | Read-only native preview on exact Bloom target | SUCCESS | Completed13:15:08.482516Z after42m07.68s; exactly one pending function migration, no allocator recovery pending; transaction read-only on and rollback. Native evidence SHA`04178a66f0779f99863ac70758891fe6540380edf8945933db4f9c56e0da3c6e`. |
| B4 | Final owner package10.1.6 | BLOCKED | Root review of exact historical fence and bounded performance proposal before final source freeze/tag. Exact tagged wheel installation in the operator venv is sufficient; PyPI publication and a Bloom image build are not required by the unchanged consumer contract. |
| B5 | Bloom-only native function migration | BLOCKED | Separate writer-fence epoch`000033-20260913T090834Z` remains unresolved despite allocator pending=0. Fresh13:32Z native read finds original ACL restored, normal connections open, clear instantaneous census; existing takeover/quarantine promotion cannot silently treat this as a native release. No fence/apply. |
| B6 | Real existing reader acceptance | OPEN | Fresh native authorization plus actual Kahlo/Bloom read and GUI health; no substituted token/principal |

## Candidate source and release identity

Worktree`/Users/jmajor/projects/mega_dayhoff/.codex-worktrees/tapdb-kahlo-reader-resolver-20260918`, branch`codex/kahlo-reader-resolver-20260918`. Base is latest published TapDB10.1.5, commit`1566aa345f5d85826b0637660fbd4dac21242e49`, annotated tag object`4ab0fa74caae42e9e78a7b101dae8c84c3c427ee`. Fresh Git remote and PyPI reads after12:28Z found10.1.5 latest and10.1.6 absent.10.1.6 is a proposed release only; no tag was created.

The initial isolated worktree was made at deployed10.1.4, then this exclusively owned, unmodified branch was advanced to inspected latest10.1.5 before the source edits. No existing/published tag was moved. The prescribed`source ./activate` created an ignored local`.venv` and began bootstrap; it was interrupted at the editable-install step. No tests, package publication, container build or production operation ran through that activation.

Source allowlist:

| File | SHA256 |
| --- | --- |
| `daylily_tapdb/runtime_principal.py` | `e52d04f1f894473fe1a5ee489c4b0b1af0a6b474671f21d20a21efa6f0c91c95` |
| `schema/runtime_identity_authorization.sql` | `4d1c9ee33c1bffdc0be264d1c68a3e60a74799dd17f95ed13c0031d98d3d72f1` |
| `schema/migrations/20260918_070400_system_user_authorization_contract.sql` | `e3b7313fd91777c86d3d7393c1e461f043b077b094c6dbbd340b02b70d74baf2` |

The new migration inlines the released resolver function and search-path pin. It does not re-run the original table/policy installation asset. Historical migration files remain untouched. The package's canonical runtime asset changes so strict native operator catalog verification can validate the corrected deployed function. All later operator binding/catalog operations must use the matching10.1.6 release; the running Bloom consumer does not need an image change for this unchanged SQL interface.

## Native preview and production command contract

The installed CLI has no migration-directory option. The coordinator explicitly approved the existing public`build_migration_preflight` API with an exact staged candidate directory and`operator_connection(read_only=True)`. The reviewed helper is`20260918T123300Z_bloom_native_migration_preview.py`; the source asset manifest is`20260918T123300Z_candidate_assets.json`. It checks every staged asset hash, verifies native operator version10.1.5 and exact target, proves transaction read-only, rolls back explicitly, retains existing mappings/recovery family/journal, and writes create-exclusive receipts. It exposes no apply operation. Targeted sudo from the interactive Ubuntu session is required to read the existing protected registry; the first non-sudo config read failed without accessing a database.

Staging path: `/home/ubuntu/bloom_ops/kahlo-resolver-20260918T123300Z`. Existing installed10.1.5 SQL asset hashes were compared with source before copying. Only candidate copies were changed. The helper was invoked as:

```bash
sudo /home/ubuntu/bloom_ops/tapdb101-20260911/venv/bin/python \
  /home/ubuntu/bloom_ops/kahlo-resolver-20260918T123300Z/preview.py
```

The supported release CLI preview shape is shown below for a changed target requiring new evidence. The accepted native preview may be passed directly to`--preflight-receipt` after the matching final operator package is installed, provided the target, catalog, journal authority and inventories are unchanged. Do not repeat this standalone preview merely because the operator package changed. Native apply still recomputes and compares all required evidence; it cannot be bypassed.

```bash
sudo /home/ubuntu/bloom_ops/tapdb101-20260911/venv/bin/tapdb \
  --config /home/ubuntu/bloom_ops/tapdb101-20260911/operator.yaml \
  --client-id bloom --database-name bloom-day db schema migrate --dry-run \
  --receipt /home/ubuntu/bloom_ops/tapdb101-20260911/receipts/kahlo-resolver-1016-plan.json \
  --sequence-mappings /home/ubuntu/bloom_ops/tapdb101-20260911/receipts/source-sequence-mappings.json \
  --recovery-family /home/ubuntu/bloom_ops/tapdb101-20260911/receipts/recovery-family.json \
  --receipts-dir /home/ubuntu/bloom_ops/tapdb101-20260911/journal
```

An apply command may be prepared only from the actual accepted unchanged native preflight; it uses`--apply --preflight-receipt` plus a new result path, the same mappings/family/journal, and the existing explicit writer fence or`--establish-writer-fence --control-config .../control-operator.yaml --provider-contract .../receipts/aurora-provider-contract.json`. Root owns the Bloom-only maintenance/admission boundary. No apply command was run. The Sep13 record reports a manually restored but unreconciled native recovery epoch; this preview must disclose its current status. It must not omit recovery authority, invent a fresh journal or apply unrelated pending migrations.

Preview progress at12:57Z: the one native process had elapsed1456s, CPU1450s, with no terminal receipt. The existing journal has38 JSON files totaling508,553,329bytes, including two245.9MB receipts. An earlier process I/O observation showed32,568,914,528bytes read and zero filesystem bytes written. Native`validate_recovery_family` reads and verifies the complete hash chain; `recovery_family_state` repeats that validation for embedded family declarations, and the preflight calls those public validations repeatedly. This finite repeated validation explains the sustained CPU; it is not evidence of a completed preview. Root instructed allowing this existing read-only invocation to complete. No duplicate invocation, journal pruning, private reviewed-state injection or validation bypass was used.

At13:03Z, a single public`read_receipts` metadata count (not a new integrity result) found38 receipts and36 embedded family declarations in the sole declared root. One`recovery_family_state` call therefore performs38 full history reads/chain validations. The success path contains four such calls and three standalone history validations:155 full scans, approximately78.83GB of journal bytes before in-memory checksum work. The original preview's process read counter had reached54.44GB, with no filesystem writes. This supports a finite remaining-work estimate only if the journal is unchanged; no terminal success or exact remaining duration is inferred.

## Bloom consumer reconciliation and acceptance gates

Fresh remote Bloom10.0.7 exists at`a70ed8e9c553a5e7f97a52e5b8f76e6bc3d071b3`. Its entire delta from live10.0.6 is the existing TapDB10.1.5 pin and real artifact entries in`pyproject.toml`/`uv.lock`; no app source changed. Bloom10.0.8 was absent at the fresh remote query. Those release facts do not establish a need to build or deploy another Bloom image.

The coordinator reviewed and accepted retaining Bloom10.0.6 for this SQL-only repair. Its exact auth path`bloom_lims/auth/services/user_api_tokens.py:constrained_roles_for_token_owner` calls`daylily_tapdb.user_authorization.resolve_user_authorization`. That wrapper is byte-identical between10.1.4 and10.1.5 and invokes the same native BIGINT function with the unchanged exact response keys`uid,euid,role,is_active`. Runtime connection installation calls the unchanged`tapdb_assert_runtime_role`, not the native operator body/catalog comparator. Bloom imports`runtime_principal` only in its owning CLI DB operations. Thus the scope is TapDB10.1.6 owner package and exact native function migration; retain the current Bloom image, credential, runtime binding and request protocol. There is no compatibility shim or old-tool rebinding. Actual existing-reader acceptance remains required after the migration.

Acceptance requires: deployed native routine/catalog match the reviewed release; existing readerUID9148/EUID`M-SYS-9T5J` resolves active`READ_ONLY`; same token identity/owner/scope/expiry and existing grants; template and actor identity/semantic fields and all lineage unchanged; no unrelated pending migration applied; actual authenticated Bloom observability/DAG read succeeds through Kahlo with its existing configured credential. Capture native owner read before the HTTP read so ordinary token usage bookkeeping is not confused with credential rotation. A tag, build or successful native preview is not GUI/reader acceptance.

No tests, lint, CI, package build/publication, container build, DB application, deployment, new grant, token rotation or notification was performed by this child task. Objective remains incomplete untilB4–B6 succeed.

## Completed native preview and independent fence state

The exact native preview ran from12:33:00.804906Z through13:15:08.482516Z in the existing10.1.5 operator package against the reviewed candidate SQL directory. It retained the original family and mappings. Only`20260918_070400_system_user_authorization_contract.sql` is pending, with SHA`e3b7313fd91777c86d3d7393c1e461f043b077b094c6dbbd340b02b70d74baf2`;17 migrations are already recorded. No unrelated migration or pending allocator recovery was found. The retained native receipt has66 sequences.

Full native receipt:`/home/ubuntu/bloom_ops/kahlo-resolver-20260918T123300Z/native-preview.json`,505,668,660bytes, file SHA`d3ddd3ab24c97a5410bbcebcfc3f876dfe33a11b88b34f3f48981a5fe2920211`. Its internal evidence SHA is`04178a66f0779f99863ac70758891fe6540380edf8945933db4f9c56e0da3c6e`. It stays on the protected EC2 evidence path; the credential-free local compact result is`20260918T131508Z_native_preview_status.json`.

A separate public native journal read completed13:21:10Z. Family`d869afb8-47a7-41dd-813a-f24d0ca96fa8` still has writer-fence takeover intent`000033-20260913T090834Z`. The unchanged head is`000038-20260913T093102Z`, SHA`058c4ec4e3027c32808ba2795acf4967f52d2a9898efb6faaa2fdda0d6344733`. Local evidence:`20260918T132110Z_native_fence_epoch.json`. Allocator pending and writer-fence epoch are distinct native states.

The protected public owner-helper read`20260918T132800Z_read_native_fence_state.py` then inspected only native control state, ACL, census and PostgreSQL timeout settings in an explicitly read-only transaction, rolled back, and reverified the unchanged journal. Receipt:`20260918T133201Z_native_fence_owner_read.json`, observed13:31:28.139896Z–13:32:01.110456Z. DatabaseOID17040 is open; its ACL exactly equals the original epoch ACL and grants CONNECT to PUBLIC and`bloom_runtime_10`. The instantaneous native census is clear. No connection was terminated or privilege changed. Control-session statement/lock timeouts are0 and idle-in-transaction timeout is1d; the native fence code has no automatic elapsed-time reopening.

The epoch reached native`quarantined` at receipt`000036-20260913T091549Z`, but there is no later native release event. The current restored normal ACL differs from that quarantine. Current source therefore prevents both a fresh acquire against this unresolved epoch and promotion using the old quarantine receipt. A takeover plan for an open old takeover requires operator-only access, which the fresh original-ACL observation does not satisfy. The source route accepts no evidence-only completion of a manually restored quarantine. Root must review an exact owning recovery proposal; do not write a fabricated release event, edit the journal, relabel an old receipt, restore a fallback ACL or bypass the guard.

Native apply also verifies the writer fence, locks target tables ACCESS EXCLUSIVE, and repeats the full preflight before executing any SQL. The accepted standalone preview avoids one redundant preview but does not waive that required comparison. Root rejected entering the observed42-minute validation window under a fence and requested a source-level performance proposal. No performance code has been edited. See the dated rollout proposal for exact locations and guard semantics.

## 13:52Z source candidate update

After reviewing the concrete restored-ACL evidence, the coordinator authorized the explicit native observation mode and the narrow journal-validation optimization. Both are now source candidates in this isolated worktree, without a live invocation. The earlier statement that performance code was not edited describes the13:32Z evidence stage. Current implementation and exact prepared native command are recorded in`20260918T135200Z_restoration_performance_candidate.md`; the refreshed allowlist contains eight source files and the owned child documents. Final coordinator review, release, install, native plan, journal-only observation, independent fresh fence/migration and existing-reader acceptance remain pending. No tests or builds ran.
