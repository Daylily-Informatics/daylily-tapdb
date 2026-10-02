# Native audit UID sequence denial — local source correction

## Current continuation state

Source only, authored 2026-10-02. No tests, imports, lint, package installation,
version change, commit, tag, publication or production execution performed for
this correction. Published 11.0.3 remains immutable. Coordinator owns release
authority, production execution and the Dayhoff fleet ledger.

Confirmed Kahlo and Dewey runtime binding plans reject effective SELECT/USAGE
on `audit_log_uid_seq`. Ursa's observed pre-adoption catalog retains the same
owner-issued direct runtime grants; the earlier failed transaction's exact
exception is unavailable, and its current read-only planner fails the expected
pre-adoption table inventory check. Equal quarantine row counts do not prove
content equality or a historical transaction outcome.

## Exact correction

`audit_uid_sequence_denial.plan_audit_uid_sequence_denial(connection, cfg,
roles=...)` is read-only. Its sealed `tapdb-audit-uid-sequence-denial/v1` plan
contains authenticated operator/database identity, exact `audit_log.uid` bigint
identity and its one internal sequence dependency, table/sequence ownership,
complete sequence ACL, principal identities/effective privileges, and exact
owner-issued direct revocations. The dependency and ownership establish the
allocator; its canonical name is an additional check, not discovery by name.

Planning rejects missing/ambiguous identity ownership, non-operator grantors,
runtime-issued downstream grants, PUBLIC or inherited effective privileges,
operator-owner membership, superuser and broad predefined-role authority.
It does not revoke memberships/PUBLIC privileges or alter any other allocator.

`apply_audit_uid_sequence_denial(connection, cfg, plan=...)` requires an exact
fresh comparison with the sealed plan, then executes only its listed direct
sequence privileges with `REVOKE ... RESTRICT`. It proves effective denial and
exact preservation of every unrelated ACL entry. Its receipt explicitly says
`applied_in_transaction`; the outer lifecycle owns commit/failure semantics.
No sequence value, definition, dependency, identity, scope or audit-writer grant
is changed by this operation.

Binding adds `audit_uid_sequence_denial` to its existing sealed plan/result.
The read-only planner permits the exact proven planned-denial object only when
an actual selected-runtime revocation is present. Apply performs that operation
before existing grants; the final full effective permission verifier still runs
without an exception. Fresh-schema sequence provisioning rejects a needed
denial before granting anything and requires the receipt-bound bind lifecycle.

Adoption seals a denial plan for **all** existing scope-bound runtime roles plus
the selected adoption principal. The complete snapshot is compared under the
existing table locks and closed physical fence. Denial runs before allocator
advancement and before schema DDL grants the audit writer canonical access.
The existing all-runtime audit table/column enforcement remains unchanged.
The resulting native adoption receipt carries the denial receipt; existing
allocator intent/reservations/finalization, outcome, quarantine and fence
release checks are unchanged. Failure rolls back the caller's transaction and
does not manufacture a terminal historical outcome or reopen the fence.

## Compatibility and limits

Only operator lifecycle code and authored checks/documentation change. Packaged
SQL, templates, models, connection/security context, audit writer installation,
runtime CAS/history interfaces and consumer dependencies are byte-identical to
11.0.3 (and the preserved 11.0.2 runtime contract). Existing six prepared 11.0.2
consumer images use SECURITY DEFINER audit routines owned by the audit writer;
they do not require direct access to this sequence. No consumer rebuild follows
from this correction. Publication/installation of any new operator release
requires the coordinator's concrete approved amendment.

Old adoption/binding plans must be regenerated: new source requires the sealed
denial input and exact fresh catalog comparison. No compatibility default is
provided. Already-adopted databases use native binding only; adoption replay is
still prohibited. Ursa's quarantine must first follow a separately reviewed
supported native promotion/release route; the combined planner still rejects
active quarantine. No raw SQL recovery, forced opening, journal edit, backup,
floor reduction or scope substitution is introduced.

Authored but **unrun** checks cover read-only planning, all-bound role coverage,
direct-only revocation and audit-writer preservation, unsafe authority/grantor/
downstream rejection, ownership and stale ACL rejection, final permission
failure, exact planner exception, pre-advance ordering, and an isolated native
binding/trigger-audit scenario. They provide no execution or production proof.
