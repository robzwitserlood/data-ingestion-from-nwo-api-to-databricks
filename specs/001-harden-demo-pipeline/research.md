# Phase 0 Research: Harden Ingestion Pipeline for Free-Edition Demo

Each item below resolves one implementation-level unknown left open by the spec and
constitution. Format: Decision / Rationale / Alternatives considered.

## 1. How CI enforces the test gate (FR-001, FR-002, FR-003)

**Decision**: Add a `test` job to both `stage_deployment.yaml` and `prod_deployment.yaml` that:
runs `actions/checkout`, sets up Python matching `pyproject.toml`'s `requires-python`, installs
the `dev` dependency group (`uv sync` or equivalent), authenticates to Databricks using the same
`DATABRICKS_HOST`/`DATABRICKS_TOKEN` env already defined at workflow level, sets
`DATABRICKS_SERVERLESS_COMPUTE_ID: auto` (matching `tests/conftest.py`'s own fallback), and runs
`pytest`. The existing `deploy` job gains `needs: [test]`; `pipeline_update` keeps `needs: [deploy]`
unchanged, so the chain becomes `test → deploy → pipeline_update`. A failing `test` job stops the
workflow before `deploy` runs (GitHub Actions' default `needs` behavior — a job is skipped if any
of its `needs` fails), which satisfies FR-002 ("MUST NOT deploy... if the automated check suite...
has failed") without any extra conditional logic.

**Rationale**: This is the minimal change that makes Principle III's existing verification
criterion true ("the CI workflow definition MUST contain a test step that runs before the deploy
step and MUST be configured so the deploy step does not run... when the test step fails") without
inventing a new CI mechanism. Reusing the same PAT-based auth the deploy/run jobs already use
avoids a second credential.

**Alternatives considered**:
- *A single combined job (test+deploy+run in one job, sequential steps)*: rejected — a step
  failure partway through a job still lets the workflow report per-step status, but GitHub
  Actions' clean gating idiom for "don't run B if A fails" is job-level `needs`, and separate jobs
  also give the PR a clearly separate "test" check in the UI, useful given FR-001's independent
  test ("Introduce a change that would fail an existing automated check... confirm it's stopped
  before it reaches pre-production... verifiable purely from the deployment history").
- *Branch-protection required-status-check instead of `needs`*: rejected as the sole mechanism —
  it depends on repo settings outside version control, which the constitution's verification
  criterion ("CI workflow definition MUST...") implies should be self-contained in the workflow
  file itself. (Adding branch protection as a *belt-and-suspenders* is a reasonable operational
  follow-up but is not a code change and is out of scope for this plan's artifacts.)

## 2. How the API boundary validates response shape (FR-004)

**Decision**: Add a pure function (e.g. `validate_response_shape(data: dict) -> None`) in
`nwo_projects_api.py` that checks `data` has a `"projects"` key whose value is a list, and a
`"meta"` key whose value is a dict containing a `"pages"` key whose value is an int. On any
mismatch it raises a specific, purpose-built exception (e.g. `NWOpenAPIShapeError`, a `ValueError`
subclass) naming exactly which field was missing or wrong-shaped. `fetch_data` calls this
immediately after `response.json()`, before the data is returned to any caller — so a malformed
response is rejected before `write_json_file` ever lands it, and before `set_page_numbers.py`'s
`first_page['meta']['pages']` access (today's only incidental check, which raises an opaque
`KeyError` rather than a clear one).

**Rationale**: Principle IV requires failing "loudly and visibly... not silently drop, null out,
or coerce" and requires the failure be a "clear, specific and actionable error." Validating at the
single point where the untrusted response first enters the system (immediately after JSON
decoding, before landing) is the literal "boundary" the principle names, and it means every
downstream consumer (landing, `update_raw`, `set_page_numbers`) can assume shape correctness
already holds for anything actually landed. This makes the later schema-on-read in
`read_json_projects` (`update_raw.py`) a defense-in-depth detail rather than the primary
validation point — it is left as-is (Spark's schema-on-read will simply produce a well-typed
`DataFrame` from data already known-valid), and no second full validation pass is added there for
this feature.

**Alternatives considered**:
- *Validate only at `update_raw.py` read time (schema-on-read + explicit null-column check)*:
  rejected — this lands malformed data first, which violates Principle I's landing-volume
  semantics only in spirit (landing itself is still append-only) but means a bad response
  produces a permanently-stored bad file before anyone notices, and the failure surfaces several
  pipeline stages away from its cause, contradicting "specific and actionable."
- *Use a JSON Schema library (e.g. `jsonschema`) for validation*: rejected as unnecessary
  dependency weight for two field checks; a small hand-written pure function is more in keeping
  with Principle VIII's "pure, unit-testable functions" and needs no new dependency.

## 3. How `project_id` non-null/uniqueness is enforced (FR-005)

**Decision**: Add an explicit, pure validation step (e.g. `assert_unique_not_null_ids(df, id_col)`
or equivalent) that runs on the freshly-parsed snapshot DataFrame in both `update_raw.py` (before
`merge_into_delta_table`) and `update_base.py` (before the final `saveAsTable(..., mode=
'overwrite')`), and raises a specific, typed error (naming the count of null/duplicate values
found) if any `project_id` is null or repeated. In `update_raw.py`, this replaces the current
`.dropDuplicates(subset=['project_id'])` call in `create_full_snapshot_from_df` — today's
behavior silently keeps one of two conflicting records, which spec User Story 2 explicitly calls
out as the failure mode to avoid ("the conflict is detectable rather than silently collapsed or
duplicated").

**Rationale**: Unity Catalog's declarative `PRIMARY KEY`/`NOT NULL` table constraints are
informational only for managed Delta tables in this context (not enforced at write time the way a
traditional RDBMS constraint is), so relying on them alone would not actually satisfy "the system
MUST guarantee" (FR-005) — a MERGE or overwrite that inserts a null or duplicate `project_id`
would silently succeed at the storage layer either way. An explicit, application-level check that
fails the run is the only mechanism that actually guarantees the property, and it is trivially
unit-testable without a live Spark session's need for real constraint-enforcement behavior beyond
what `tests/conftest.py` already provides.

**Alternatives considered**:
- *Rely on declared Delta/UC table constraints only*: rejected for the reason above — informational
  constraints don't block a bad write, so they can't be the sole guarantee FR-005 requires. They
  remain an optional, low-cost *documentation* addition on top of the code-level check, but adding
  them is not required to satisfy the requirement and is left out of this plan's required scope to
  keep the change minimal.
- *Silently keep dedup but log a warning*: rejected — explicitly contradicted by spec User Story 2
  Acceptance Scenario 2, which requires the conflict be "detectable," and a log line that nobody
  is required to read does not meet that bar the way a failed run does.

**Consequence for existing tests**: `tests/update_raw_test.py::test_create_full_snapshot_from_df`
currently asserts `result.count() == 5` for a fixture containing a duplicate `project_id`
("001" appears twice) — i.e. it currently asserts the silent-collapse behavior this feature
removes. That test's expectation changes from "returns 5 deduplicated rows" to "raises the new
duplicate-identifier error" as part of implementing this decision (tracked in data-model.md and
left for `/speckit-tasks` to schedule as a concrete task).

## 4. How the freshness signal is exposed (FR-006)

**Decision**: At the end of `update_base.py`, after the `base.nwo_projects` table write succeeds,
set a Delta table property (e.g. `pipeline.last_successful_refresh_utc`) to the current UTC
timestamp via `ALTER TABLE ... SET TBLPROPERTIES (...)` (or the equivalent
`DataFrameWriter`/SQL call). Anyone can retrieve it with a single `SHOW TBLPROPERTIES
<catalog>.base.nwo_projects` (or `DESCRIBE TABLE EXTENDED`) — no job-run-history lookup or source
access required.

**Rationale**: The constitution's own Principle VII verification criterion offers this exact
option ("a documented query or job-history check... for retrieving the last successful run's
timestamp") and a table property is strictly simpler than a job-run-history API call: it needs no
job ID, no separate auth scope beyond table read access the demo visitor already needs to query
the data anyway, and it is naturally colocated with the data whose freshness it describes. It only
updates when `update_base.py` completes successfully, which is exactly the "last successful
refresh" semantics FR-006 asks for (a failed run never reaches this line).

**Alternatives considered**:
- *Databricks Jobs API run-history query*: rejected as the primary mechanism — requires the
  querier to know the job ID and have Jobs API permissions, which is a heavier ask for "anyone" (a
  demo visitor) than a SQL table-property lookup they can run the same way they query the data
  itself (SC-003: "under one minute, without contacting the project owner").
- *A dedicated one-row "pipeline_status" table*: rejected as unnecessary extra state to keep in
  sync — a table property piggybacks on a write the pipeline already performs and cannot drift out
  of sync with the table it describes the way a separate status table could (e.g. if the status
  write and the data write aren't atomic together).

## 5. Job failure notification target (FR-007)

**Decision**: Add `email_notifications.on_failure` to the job resource
(`resources/data_nwo_ingestion_pipeline.job.yml`), pointed at an email address supplied via a new
bundle variable (e.g. `owner_email`) rather than hardcoded, defaulting to the Databricks account's
own user address already visible in `databricks.yml`'s `workspace.root_path`
(`ikdeelgeengegevensmet@gmail.com`) so the default "just works" for the current single owner while
staying overridable per target/person without a code change.

**Rationale**: Databricks Jobs' native `email_notifications` field is exactly what Principle VII's
verification criterion names ("the job resource definition MUST configure an `on_failure`
notification target") and needs no extra infrastructure (webhook receiver, etc.) — appropriate for
a personal demo project. Using a variable instead of a literal keeps the email address easy to
change without touching job logic, and avoids assuming which address the *next* owner (if the repo
is ever forked/reused) would want.

**Alternatives considered**:
- *Webhook notification*: rejected as unnecessary infrastructure for a solo demo project; email
  requires no receiving service to stand up.
- *Hardcode the email address directly in the job YAML with no variable*: rejected — the address
  is already effectively public (it's in a tracked file's `root_path`), so this isn't a secrecy
  concern, but a variable is a near-zero-cost improvement that avoids a second hardcoded copy of
  an identity value and matches how `max_pages`/`trigger_periodic_unit` are already handled as
  bundle variables in this file.

## 6. Whether `stage`'s `mode: development` setting should change (FR-008, FR-009)

**Decision**: Change the `stage` target in `databricks.yml` to drop `mode: development` (i.e. let
it default to standard/production-style deployment behavior — real resource names, no automatic
trigger-pausing), keeping only the `stage`-specific `workspace.root_path` and the target-derived
catalog name (`${bundle.target}` = `stage`) as what distinguishes it from `prod`.

**Rationale**: `mode: development` is designed for a human developer's personal iteration copy —
it prefixes deployed resource names with `[dev <username>]` and pauses schedule triggers by
default. `stage` here is not that: it's a CI-triggered pre-production target invoked
non-interactively by GitHub Actions on every PR (`databricks bundle run ... --refresh-all`), and
its whole purpose (per constitution Principle V and spec FR-009) is to behave like a faithful
rehearsal of what will happen in `prod`, differing only by catalog. Name-prefixing and
schedule-pausing are cosmetic/safety behaviors meant for a human's throwaway sandbox, not
properties this pipeline's promotion workflow relies on — and leaving them in place risks the
prefixed resource name depending on *which* CI-runner identity happens to be evaluated at deploy
time, which is exactly the kind of surprise Principle V's "environment parity" is meant to
prevent. `dev` (the actual interactive-developer target, `default: true`) correctly keeps `mode:
development` — only `stage` changes.

**Alternatives considered**:
- *Leave `stage` as `mode: development`, accept the prefixing*: rejected — while functionally
  survivable (CI always deploys+runs explicitly, so a paused trigger doesn't block it), a
  prefixed/mangled resource name for a target the CI treats as "the" `stage` deployment is exactly
  the kind of avoidable inconsistency that undermines FR-009's "runs against pre-production...
  exactly as before" guarantee if the prefix ever changes across CI runs.
- *Introduce a fourth explicit `mode` value or custom tagging scheme*: rejected — DABs' `mode`
  field only supports `development`/`production`; there's no need to invent anything custom when
  simply omitting `mode` (its default) already gives `stage` production-style behavior while
  `stage`'s catalog variable keeps it isolated from `prod`'s data, per Principle V.

## 7. Databricks Connect / serverless compute wiring for the new CI `test` job

**Decision**: The new CI `test` job needs no code beyond environment variables — reuse
`tests/conftest.py`'s existing `enable_fallback_compute()` logic (already present, unmodified in
this feature) which sets `DATABRICKS_SERVERLESS_COMPUTE_ID=auto` automatically when no compute is
otherwise configured. The workflow only needs `DATABRICKS_HOST`/`DATABRICKS_TOKEN` set (already
defined at workflow `env:` level) for `WorkspaceClient()` inside `conftest.py` to authenticate and
for `enable_fallback_compute()` to detect "no compute specified" correctly.

**Rationale**: Avoids duplicating compute-selection logic between local developer runs and CI —
the same fallback that already lets a developer run `uv run pytest` with zero manual serverless
configuration works unmodified in GitHub Actions once the same two env vars are present, which
they already are in both workflow files.

**Alternatives considered**: *Explicitly set `DATABRICKS_SERVERLESS_COMPUTE_ID: auto` at the CI
workflow's `env:` level too*: not rejected outright — it's a harmless, slightly more explicit
belt-and-suspenders option left as an implementation detail for `/speckit-tasks`/
`/speckit-implement` to decide, since `conftest.py` already covers it either way.
