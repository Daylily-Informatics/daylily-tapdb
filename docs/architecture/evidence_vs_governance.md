# Evidence Versus Governance

TapDB is a polymorphic evidence substrate. It stores evidence first and lets
governance evolve around that evidence without rewriting history.

## Core Doctrine

Evidence persists. Governance evolves. Validators observe. Repairs add new
evidence. Validators do not rewrite historical objects, relationships, lineage,
claims, events, metadata, or terminology values.

Evidence includes:
- object JSON and metadata stored on `generic_instance`
- template identity and governance references stored on `generic_template`
- relationships stored in `generic_instance_lineage`
- claims, events, audit rows, repair records, and explicit supersession or
  retraction records represented as TapDB evidence objects

Governance includes:
- validators and validator pipelines
- terminology sets and term lifecycle metadata
- relationship constraints
- containment rules
- position schemes
- template validator references
- editor hints
- search and display mappings

Governance determines how future actions are assessed. It is not historical
truth, and it is not a license to rewrite old evidence.

## Physical DDL Versus Evidence Migration

TapDB preserves existing database DDL migration tooling only for physical schema
changes, such as adding `generic_template.validator_ref`. This does not permit
validator-driven object, relationship, lineage, claim, event, metadata,
terminology, or evidence migration.

Forbidden governance states and UI/API terms are:
- `migration_available`
- `migration_required`
- `migration_pending`
- `migration_in_progress`
- `migration_before_edit`

Governance changes are handled by publishing new governance objects, assigning
new validator references, assessing evidence, and creating explicit repair
evidence when a human or approved system chooses to repair something.

## Template Validator Reference

Every template resolves to a validator reference. The default is:

```text
UNIVERSAL_PASS@1
```

A missing validator reference is not "no validation"; it resolves to Universal
Pass. Universal Pass is explicit governance that accepts the evidence shape it
observes and produces a successful assessment.

Template identity remains taxonomy-first:

```text
category/type/subtype/version
```

Instance prefixes are EUID issuance configuration. They are not lookup identity
and may change over time without changing the template taxonomy.

## Validator Types

TapDB recognizes these validator behavior families:
- Universal Pass
- JSON Shape
- Terminology
- Relationship
- Containment
- Position
- Claim
- Composite
- Custom

A validator is governed TapDB evidence describing behavior. It is not substrate
schema authority. Composite validators run a pipeline of validator components;
each component produces observations, findings, and recommendations. The
composite assessment still does not mutate the subject.

## Assessment States

Assessment output may use these states:
- `valid_current`
- `valid_historical`
- `nonconforming_current`
- `not_evaluated_current`

Validation assessments are ephemeral by default. They may be recalculated,
dropped, or projected for UI/search speed. Only explicitly published regulated
assessment reports may become durable evidence.

An explicitly published regulated assessment report must record:
- subject evidence reference, version, and hash
- validator reference, version, and hash
- assessment context
- actor or system
- timestamp
- findings
- statement that the subject evidence was not mutated by the assessment

## Revalidation

Revalidation means assessing existing evidence under current or selected
governance. It does not mutate the subject.

When governance changes:
1. publish a new validator or validator version
2. assign the new template validator reference for future governed actions
3. preserve existing evidence
4. optionally revalidate evidence and return assessment output
5. create explicit repair evidence only through a repair action

Historical evidence can become `valid_historical` or `nonconforming_current`
without being changed.

## Repairs

A repair is an explicit auditable action that adds evidence. A repair is not a
validator migration.

A repair may add:
- a correction claim
- a superseding relationship
- a new object revision
- a retraction or supersession event
- a normalized value while preserving the original value

Every repair record must include:
- actor or system
- timestamp
- reason
- prior evidence reference
- new evidence reference
- governance context
- approval or review metadata when required
- audit trail

Soft delete, direct JSON overwrite, or status mutation is not repair semantics.

## Relationships And Containment

Relationship constraints govern creation of new relationship assertions. They do
not make historical relationships disappear.

Historical invalid relationships remain queryable as evidence. Current views may
project active or superseded state, but the underlying evidence remains present.

Containment and position governance must be assessment behavior, repair
recommendation behavior, or explicit repair evidence. It must not be encoded as
hard substrate truth that makes old evidence impossible to preserve.

## Terminology

Controlled terminology is governed TapDB evidence. Terms may be active,
deprecated, retired, aliases, or replacement suggestions.

Terminology validators assess values; they do not automatically replace stored
values. A replacement is a repair action and must preserve the original evidence.

## GUI And Editor Language

Governance UI must use these concepts:
- assess
- revalidate
- current governance
- historical governance
- `valid_current`
- `valid_historical`
- `nonconforming_current`
- `not_evaluated_current`
- repair
- derived projection

Governance UI must not say an object needs migration, upgrade, schema
conversion, automatic fix, or migration before edit.

Repair buttons must say `Create repair`.

Revalidation screens must state that revalidation does not mutate objects,
relationships, lineage, claims, events, metadata, or terminology values.

## JSON Editor V2

The canonical TapDB GUI editor supports:
- raw JSON editing with parse validation, formatting, path search, text search,
  diff, jump to validation finding, and large nested JSON handling
- structured view driven by validator behavior and rendering hints
- terminology dropdowns
- object and relationship pickers
- containment and position controls
- inline findings and repair recommendations
- split raw and structured view

When raw JSON is invalid, the structured view remains frozen at the last valid
state and displays parse errors.

## Template Editor

The template editor assigns governance; it does not make a template the owner of
business truth.

Required editor sections:
- template identity
- validator reference and composition
- terminology references
- relationship constraints
- containment and position rules
- rendering hints
- assessment preview
- repair recommendation preview
- publish governance version

Template editor UI must not contain migration workflow, before-edit migration,
auto-rewrite, or schema-conversion states.

## Proposal Gate

Any future TapDB change that introduces validation, repair, terminology,
relationship constraints, editor hints, projections, or governance persistence
must answer these questions before implementation:

1. What evidence is being preserved?
2. What governance is being applied?
3. Does this assessment mutate the subject?
4. If evidence changes, what explicit repair record is created?
5. Is any DDL migration strictly physical schema evolution?
6. Are historical objects, relationships, lineage, claims, events, metadata, and
   terminology values still queryable?
