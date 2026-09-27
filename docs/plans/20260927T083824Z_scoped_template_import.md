# Scoped repository template import repair

## Current continuation state

Explicitly approved 2026-09-27: bounded native import fix, one new package release and Atlas-only dependency/lock update. Baseline is latest native release 10.1.10, `c26d07b03a0ff7d18f6b513affe8dac31ae97f75`. New branch `codex/tapdb-scoped-template-import-20260927`. Source fix complete; publication pending. Tests and final review WONT DO by user instruction. No native schema migration, database repair, template deletion, PR or merge.

Atlas's exact two-definition import on installed 10.1.7 failed before insertion with `json_addl.properties contains sensitive key: password_hash`. The same code exists in 10.1.10. Existing-template lookup serialized every template owned by Atlas, including unrelated authentication definitions.

The change filters existing rows by the exact requested category/type/subtype/version tuples before serialization, in addition to domain/issuer scope. Sensitive-field checks, prefix/ownership validation, selected-identity conflicts and non-overwrite behavior are unchanged. Existing authentication definitions remain untouched. Source-only evidence is not a successful runtime import.

This isolated checkout was created with Git because the desktop managed-worktree tool targets this chat's OWY repository and cannot select the separate TapDB repository. Existing TapDB worktrees and dirty changes are preserved.

The Dayhoff controlling ledger is `docs/plans/20260926T141228Z_accession_object_specification_ledger.md` in `repos_work/dayhoff-service-pins-20260924`. Runtime receipts and exact resulting versions belong there.
