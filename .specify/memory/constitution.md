<!--
Sync Impact Report
==================
Version change: [TEMPLATE] → 1.0.0 (initial ratification)
Modified principles: n/a (first version)
Added sections:
  - Core Principles I–VIII (all new)
  - Technology & Environment Constraints (new, replaces [SECTION_2_NAME])
  - Development Workflow & Quality Gates (new, replaces [SECTION_3_NAME])
  - Governance (filled in)
Removed sections: none
Deferred items / TODOs: none — all placeholders resolved from user-supplied input.
Templates requiring follow-up: none checked yet in this session (no plan/spec/tasks
templates have been generated against this constitution); re-validate them the first
time /speckit-plan or /speckit-tasks runs against this constitution.
-->

# data-ingestion-from-nwo-api-to-databricks Constitution

## Core Principles

### I. Immutable Landing, Reconciled Snapshot

The landing volume (raw JSON files written per job run and page number) is an
append-only bronze zone: a file for a given `run_id`/page is written at most once
and MUST NOT be overwritten or deleted by pipeline code once it exists. The `raw`
catalog schema is a fully-reconciled silver snapshot, built via a Delta `MERGE`
that updates rows whose content hash changed, inserts rows new to the source, and
deletes rows no longer present in the source (`whenNotMatchedBySourceDelete`). The
`base` schema is the cleaned, typed, query-ready table rebuilt from `raw`. No
aggregated/business "gold" layer is in scope for this project.

**Rationale**: this mirrors the standard medallion pattern's core invariant —
bronze is never mutated, so any downstream mistake is fixed by rebuilding from
bronze rather than re-ingesting from the external API. Treating the landing
volume as the immutable source of truth means the reconciled `raw`/`base` tables
can always be regenerated from what was actually received, even if transform
logic later has a bug.

**Verification**: a test MUST assert that writing a JSON page whose file already
exists is a no-op (no overwrite), and that re-running the `raw` MERGE against an
unchanged landing set leaves `raw` byte-for-byte unchanged.

### II. Idempotent, Full-Snapshot Reconciliation (NON-NEGOTIABLE)

Re-running any task for the same run, or running a later run against unchanged
source data, MUST NOT duplicate rows or leave the tables in a divergent state.
`raw` MUST be maintained as a full-snapshot mirror of the source via the
merge/insert/delete pattern described in Principle I. `base` MUST be fully
rebuilt (`overwrite`) from `raw` on every run, never incrementally patched. Any
new transformation logic added to this pipeline MUST preserve this idempotency
property.

**Rationale**: idempotency is what makes it safe to retry a failed run, replay a
backfill, or change `max_pages` without manual cleanup — a pipeline that isn't
idempotent silently accumulates duplicates or drift the moment a retry happens,
which is exactly when operators are least likely to notice.

**Verification**: every merge/overwrite transformation MUST have a test that runs
the transformation twice on the same input and asserts identical output
(row count and content), per standard idempotency-validation testing practice.

### III. Tests Gate Deployment (NON-NEGOTIABLE)

The pytest suite MUST run as a required CI check on every pull request and on
every push to `main`, and a failing suite MUST block `databricks bundle deploy`
and `databricks bundle run` from executing. Tests intentionally run against real
serverless Spark compute via Databricks Connect rather than mocks, to catch real
Delta/Spark behavior that a mocked test would miss.

**Rationale**: standard DataOps CI/CD practice is that every commit triggers
automated tests inside CI and deployment is gated on them passing. A pipeline
that deploys and runs before any test executes has, in effect, no working test
gate at all — regardless of how good the tests themselves are.

**Verification**: the CI workflow definition MUST contain a test step that runs
before the deploy step and MUST be configured so the deploy step does not run
(or the job fails) when the test step fails.

### IV. Validate the Data Contract at the Boundary

The external NWOpen API's JSON shape (a top-level `projects` array and a
`meta.pages` field) is an external data contract owned by a third party outside
this project's control. The pipeline MUST fail loudly and visibly — not silently
drop, null out, or coerce — when the API's response shape doesn't match what the
pipeline expects. `project_id` MUST be enforced as unique and not-null in both
`raw` and `base`.

**Rationale**: this project has no influence over NWOpen's API and no schema
registry to negotiate compatibility with, so the only defense against a silent
upstream change is an explicit shape check at the boundary plus primary-key
constraints downstream — the same discipline dbt-style projects apply by testing
every model's primary key for uniqueness and non-null, adapted here for a
non-dbt Spark pipeline.

**Verification**: a test MUST assert that malformed/unexpected API responses
(missing `projects`, missing `meta.pages`) raise a clear, typed error rather than
producing empty or partial output; a test MUST assert `raw` and `base` reject
duplicate or null `project_id` values.

### V. Environment Parity via Catalogs, Not Workspaces

`dev`, `stage`, and `prod` are three Unity Catalog catalogs within a single
Databricks Free Edition workspace, not three separate workspaces. All bundle
targets point at the same workspace host. Promotion stays git-driven: a pull
request deploys and runs against the `stage` catalog, and a merge to `main`
deploys and runs against the `prod` catalog.

**Rationale**: Databricks Free Edition provisions exactly one workspace and one
metastore per account, with no account console and no account-level APIs — the
three-workspace topology this pipeline was originally built for is unavailable
by construction. Catalog-level separation inside one workspace is the closest
available approximation that preserves the original promotion workflow's intent
(isolated schemas per environment, git-triggered promotion) without requiring
infrastructure Free Edition doesn't offer.

**Verification**: `databricks.yml` MUST define `dev`, `stage`, and `prod` targets
that share one `workspace.host` value and differ only in bundle variables and the
target-derived catalog name.

### VI. Least-Privilege, Secret-Free by Default

The pipeline requires no credentials today because the NWOpen API is public, and
it MUST stay that way unless a future data source makes a credential unavoidable.
Any future credential MUST live in a Databricks secret scope — never in bundle
YAML, source code, logs, or CI configuration. Because Free Edition has no
account-level API and therefore cannot support GitHub OIDC / service-principal
federation (confirmed by hands-on testing against a live Free Edition
workspace), CI/CD MUST instead authenticate using a personal access token
belonging to a dedicated automation identity (not a personal admin account),
stored only as an encrypted GitHub Actions secret, never committed to the
repository, and rotated periodically.

**Rationale**: the project's original CI/CD design assumed workload-identity
federation via an account-level service principal, which Free Edition's
architecture rules out. A PAT is a deliberate, documented downgrade in
auth posture, not an oversight, so it must be scoped to a dedicated identity and
handled with the same discipline OIDC would have provided by default.

**Verification**: a repository secret scan (or manual review) MUST confirm no
token, key, or credential appears in tracked files; the CI workflow MUST read the
Databricks token exclusively from a GitHub Actions secret.

### VII. Observability by Default, No Extra Tooling Required

The pipeline relies on Unity Catalog's automatic, built-in table lineage for
auditability rather than adopting a separate lineage tool. Every scheduled job
run MUST have failure notifications configured (email or webhook) so a broken
run is never silent. The pipeline MUST expose at least a basic freshness signal
— a queryable last-successful-run timestamp, sourced from job run history or a
table property — so staleness can be checked without opening the job UI.

**Rationale**: data observability practice names freshness, quality, volume,
schema, and lineage as the pillars worth tracking; a pipeline this size doesn't
justify standing up a dedicated observability stack, but it can get lineage for
free from Unity Catalog and freshness/failure-alerting for near-zero cost from
native job features, so there's no excuse to skip them.

**Verification**: the job resource definition MUST configure an `on_failure`
notification target; a documented query or job-history check MUST exist for
retrieving the last successful run's timestamp.

### VIII. Pure Logic, Isolated I/O

Every module in `src/` separates pure, unit-testable functions (parsing,
transforming, validating) from Spark/`dbutils`/CLI wiring, which stays confined
to the `if __name__ == "__main__":` block. Every new pure function added to
`src/` MUST ship with a corresponding unit test in `tests/` in the same change.

**Rationale**: this is the existing convention in this codebase and the reason
its unit tests can run fast and without a live Spark session for the parsing/
transformation logic, while the Databricks-Connect-backed tests separately cover
the Spark-dependent paths. Codifying it prevents the two concerns from
re-merging as the codebase grows.

**Verification**: code review MUST reject a new pure function in `src/` that has
no matching test in `tests/`.

## Technology & Environment Constraints

- **Databricks Free Edition, serverless-only**: no classic clusters; a maximum
  of 5 concurrent job tasks per account; one workspace and one metastore per
  account; no account console or account-level APIs. All job/pipeline
  definitions MUST target serverless compute (`environment_version` job
  environments), never a classic cluster spec.
- **Databricks Asset Bundles (DABs)** are the sole deployment mechanism for
  jobs, permissions, and environment configuration. No resource is created or
  changed by hand through the workspace UI outside of initial account setup.
- **Databricks Connect** version pinned in `pyproject.toml` MUST stay compatible
  with the workspace's current serverless runtime (client version at or below
  the server's supported range); bumping the workspace runtime requires
  re-validating this pin.
- **CI/CD**: GitHub Actions is the CI/CD platform; authentication to Databricks
  uses a PAT-backed GitHub Actions secret per Principle VI, not OIDC federation.

## Development Workflow & Quality Gates

- Work happens on topic branches; a pull request into `main` triggers a `stage`
  deployment and run (Principle V), gated by the test suite (Principle III).
- A merge to `main` triggers a `prod` deployment and run, gated the same way.
- Every PR touching `src/` or `resources/` MUST be checked against the Core
  Principles above before merge; a PR that violates a non-negotiable principle
  (II, III) MUST NOT be merged regardless of other review approval.
- New logic follows Principle VIII: pure functions plus a matching unit test,
  added in the same PR.

## Governance

This constitution supersedes ad hoc practice for this repository. Amendments
require a documented rationale (why the change is needed, what it replaces) and
a version bump following semantic versioning: MAJOR for a backward-incompatible
principle removal or redefinition, MINOR for a new principle or materially
expanded guidance, PATCH for clarifications and wording fixes. Every pull
request touching `src/` or `resources/` must be checked against this
constitution as part of review; complexity or deviation from a principle must be
justified in the PR description, not silently introduced.

**Version**: 1.0.0 | **Ratified**: 2026-09-14 | **Last Amended**: 2026-09-14
