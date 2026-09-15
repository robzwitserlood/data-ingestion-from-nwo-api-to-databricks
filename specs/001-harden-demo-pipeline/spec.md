# Feature Specification: Harden Ingestion Pipeline for Free-Edition Demo

**Feature Branch**: `001-harden-demo-pipeline`

**Created**: 2026-09-14

**Status**: Draft

**Input**: User description: "Harden the existing NWO grant-data ingestion pipeline so it runs reliably as a public, personal demo project on Databricks Free Edition, bringing its current behavior and its known gaps into compliance with the project constitution."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - A broken change can never silently reach production (Priority: P1)

The project owner proposes a change to the pipeline. Before that change can run against
pre-production or production data, the project's automated checks must actually execute and
pass — today they exist but are never invoked, so a broken change can reach either environment
untested.

**Why this priority**: This is the single highest-priority gap named in the background: an
untested deployment path makes every other guarantee in this spec (data correctness, freshness,
trustworthiness) unverifiable. Nothing else in this feature matters if a broken change can still
reach production silently.

**Independent Test**: Introduce a change that would fail an existing automated check, propose it
the normal way, and confirm the change is stopped before it reaches the pre-production or
production environment — verifiable purely from the deployment history, without reading any code.

**Acceptance Scenarios**:

1. **Given** a proposed change that fails an existing automated check, **When** the change is
   proposed for pre-production, **Then** it is not deployed or run against pre-production data.
2. **Given** a proposed change that passes all automated checks, **When** it is proposed for
   pre-production and later merged, **Then** it deploys and runs against pre-production, and then
   production, exactly as before.
3. **Given** a change is deploying, **When** its automated checks have not yet completed,
   **Then** the deployment does not proceed ahead of the check result.

---

### User Story 2 - The data can be trusted, not just assumed correct (Priority: P2)

A demo visitor queries the tables and needs to trust that every record has a real, unique
identifier and that the data reflects what the source actually returned — not a silent
misinterpretation of a source response that changed shape unexpectedly.

**Why this priority**: Without this, the pipeline can keep running and reporting success while
quietly serving wrong or corrupted data — the worst failure mode for a demo meant to showcase
correctness.

**Independent Test**: Feed the pipeline a source response with an unexpected shape (e.g. missing
the field that lists records) and confirm the run fails visibly rather than producing an empty or
partial table; separately, confirm the reconciled and cleaned tables reject a duplicate or missing
record identifier.

**Acceptance Scenarios**:

1. **Given** the external source returns a response missing an expected top-level field,
   **When** the pipeline processes it, **Then** the run fails with a clear, specific error instead
   of producing silently empty or truncated output.
2. **Given** two source records share the same identifier, **When** they are reconciled into the
   record store, **Then** the conflict is detectable rather than silently collapsed or duplicated.
3. **Given** a source record has no identifier, **When** it reaches the reconciled or cleaned
   table, **Then** it is rejected rather than stored with a missing identifier.

---

### User Story 3 - Anyone can tell whether the data is current and the pipeline is healthy (Priority: P3)

A demo visitor or the project owner wants to know, at a glance, whether the last scheduled run
succeeded and how recent the data is — without opening the job's run history or asking the owner.

**Why this priority**: A pipeline that can fail silently for weeks without anyone noticing
undermines the "trustworthy, monitored" impression the demo is meant to give, even if the
underlying logic is otherwise correct.

**Independent Test**: Force a scheduled run to fail and confirm a notification is raised without
anyone checking manually; separately, confirm that the recency of the last successful refresh can
be determined in a single lookup.

**Acceptance Scenarios**:

1. **Given** a scheduled run fails, **When** the failure occurs, **Then** a notification is sent
   without requiring anyone to open the run history to discover it.
2. **Given** the pipeline has completed at least one successful run, **When** anyone wants to know
   how current the data is, **Then** the timestamp of the last successful refresh is available in
   a single lookup.

---

### User Story 4 - Environments stay isolated on a single free account (Priority: P4)

The project owner promotes a change from a pre-production space to a production space, exactly as
the original three-environment design intended, even though only one Databricks environment is
available on the free personal account.

**Why this priority**: This preserves the promotion workflow's safety property (nothing reaches
production data untested) under a real infrastructure constraint; it's a structural requirement
for Story 1 to mean anything, but ranks below the testing and correctness stories because it's a
reorganization of existing behavior rather than new protection.

**Independent Test**: Run the pipeline against the pre-production space and confirm it does not
read, write, or otherwise affect the production space's data, and vice versa.

**Acceptance Scenarios**:

1. **Given** a run is triggered for the pre-production space, **When** it executes, **Then** only
   pre-production data is read or written — production data is untouched.
2. **Given** a change is merged, **When** it promotes to the production space, **Then** it runs
   against production's own independent data, separate from pre-production's.

---

### User Story 5 - Automated deployment works without an organizational identity system (Priority: P5)

The project owner's automated deployment (triggered by proposing or merging a change) authenticates
to the Databricks account safely, without relying on the organization-managed identity system the
pipeline originally assumed, which is unavailable on the free personal account.

**Why this priority**: This is an enabling requirement — without it, Stories 1 and 4 can't run
unattended at all — but it ranks last because it's a substitution of mechanism, not a new
capability or protection.

**Independent Test**: Trigger an automated deployment from a fresh checkout with no manual login
step and confirm it authenticates and completes successfully, and confirm no long-lived credential
is ever visible in the repository's tracked files or logs.

**Acceptance Scenarios**:

1. **Given** a change is proposed or merged, **When** the automated deployment runs, **Then** it
   authenticates to the Databricks account without any manual, interactive step.
2. **Given** the repository's tracked files and CI logs, **When** inspected, **Then** no
   credential value is ever visible in them.

---

### Edge Cases

- What happens when the external source has zero pages of data (e.g. temporarily empty)?
- What happens when the number of pages changes between the "count pages" step and the "fetch
  each page" step of the same run?
- What happens when the external source returns a response that is not valid JSON at all?
- What happens when two records in the same source response share an identifier?
- What happens when a record present in a previous run is absent from the current run, and then
  reappears in a later run — is it removed and then correctly re-added, without residual
  inconsistency?
- What happens when a scheduled run overlaps with another still-running run for the same
  environment?
- What happens when the credential used for automated deployment expires or is revoked?
- What happens when the pre-production and production promotion happens in rapid succession
  (e.g. a merge immediately after a pull request)?

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST run its full automated check suite as part of proposing a change,
  before that change is deployed or run against the pre-production space.
- **FR-002**: The system MUST NOT deploy or run a change against the pre-production or production
  space if the automated check suite for that change has failed.
- **FR-003**: The system MUST run its full automated check suite again on merge to the primary
  branch, before that change is deployed or run against the production space.
- **FR-004**: The system MUST validate the shape of each response received from the external data
  source before treating its contents as trustworthy, and MUST fail the run visibly, with a
  specific and actionable error, when the shape does not match what is expected.
- **FR-005**: The system MUST guarantee that every record identifier in the reconciled record
  store and in the cleaned, query-ready table is both present (not missing) and unique.
- **FR-006**: The system MUST make the timestamp of the last successful data refresh available
  through a single lookup, without requiring access to job run history or source code.
- **FR-007**: The system MUST notify the project owner when a scheduled run fails, without
  requiring anyone to check manually.
- **FR-008**: The system MUST keep the pre-production and production spaces' data logically
  isolated from each other, even while both run on the same underlying account and infrastructure.
- **FR-009**: The system MUST preserve the existing promotion behavior: a proposed change runs
  against the pre-production space, and only a merge to the primary branch promotes a change to
  run against the production space.
- **FR-010**: The system MUST authenticate its automated deployment and run steps without any
  manual, interactive login step and without depending on an organization-managed identity
  system.
- **FR-011**: The system MUST NOT expose any credential value in the repository's tracked files,
  configuration, or logs.
- **FR-012**: The system MUST preserve its existing landing behavior: a given page of source data
  for a given run is fetched and stored at most once, never re-fetched or overwritten once stored.
- **FR-013**: The system MUST preserve its existing reconciliation behavior: the record store
  reflects exactly the records currently present in the source — new records added, changed
  records updated, and records no longer present removed — on every run.
- **FR-014**: The system MUST preserve its existing cleaning behavior: placeholder "empty" values
  in the source data are normalized to true nulls, and every field is presented in its proper
  type in the cleaned, query-ready table.

### Key Entities

- **Source Page**: One page of the external source's paginated response, landed and stored
  exactly once per run; identified by the run and the page number.
- **Reconciled Record**: One grant/project record as currently known to the pipeline, kept in
  sync with the source on every run (added, updated, or removed); identified by a unique record
  identifier.
- **Clean Record**: The query-ready version of a Reconciled Record, with fields extracted,
  placeholder values normalized to null, and each field typed.
- **Pipeline Run**: One execution of the scheduled pipeline, with a recorded outcome (success or
  failure) and, on success, a completion timestamp.
- **Environment Space**: A logically isolated space for data (pre-production or production) that
  a given run and its records belong to.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of changes that reach the pre-production or production space over any period
  have a recorded, passing automated check run associated with them — zero untested deployments.
- **SC-002**: When the external source's response shape is unexpectedly wrong, 100% of affected
  runs fail visibly with a specific error, and 0% produce silently empty, partial, or incorrect
  output.
- **SC-003**: The recency of the data (time since last successful refresh) can be determined by
  anyone in a single lookup, in under one minute, without contacting the project owner.
- **SC-004**: A failed scheduled run results in a notification within the same day it fails,
  without any manual monitoring step.
- **SC-005**: Pre-production and production data remain fully isolated: zero instances of one
  space's run affecting the other's data.
- **SC-006**: The project owner can operate and redeploy the pipeline solo, with zero dependency
  on an organization-managed identity system and zero manual login steps in the automated path.

## Assumptions

- "Automated checks" refers to the test suite that already exists in this repository; this
  feature is about ensuring it always runs and gates deployment, not about writing an entirely
  new test suite from scratch (individual new checks called for in FR-004/FR-005 are additive to
  it).
- The external data source (NWOpen API) remains publicly accessible without requiring
  authentication; if that ever changes, credential handling is covered by the constitution's
  least-privilege principle but is out of scope for this feature.
- Exactly one Databricks account/environment is available to run pre-production and production
  spaces on, per the constraints of a free personal account; this feature does not assume any
  additional infrastructure becomes available.
- The project owner is the sole operator; there is no separate reviewer/approver role beyond the
  existing pull-request-based promotion flow.
- Demo data volume is small enough that a single account's usage limits are not a concern for this
  feature; capacity planning beyond current volumes is out of scope.
- Out of scope for this feature: a new aggregated/business-facing reporting layer beyond the
  existing cleaned table, a change of ingestion/transformation framework, new external data
  sources, and any user-facing application or dashboard.
