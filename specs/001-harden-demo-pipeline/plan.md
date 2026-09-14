# Implementation Plan: Harden Ingestion Pipeline for Free-Edition Demo

**Branch**: `001-harden-demo-pipeline` | **Date**: 2026-09-14 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/001-harden-demo-pipeline/spec.md`

**Note**: This template is filled in by the `/speckit-plan` command; its definition describes the execution workflow.

## Summary

The existing NWO grant-data pipeline (land → reconcile → clean, on Databricks Free Edition) has
five compliance gaps relative to the project constitution, all confirmed by reading the current
code and CI workflows: (1) `stage_deployment.yaml` and `prod_deployment.yaml` deploy and run the
bundle without ever invoking `pytest`, so the test suite gates nothing; (2) the API boundary
(`nwo_projects_api.py`) has no shape validation — a malformed NWOpen response is only caught
incidentally (or not at all) downstream; (3) `create_full_snapshot_from_df` silently
`dropDuplicates`s on `project_id`, which collapses rather than surfaces a source conflict, and
nothing rejects a null `project_id`; (4) the job resource has no `on_failure` notification and no
freshness signal is exposed anywhere; (5) `stage`'s `mode: development` target setting is a
holdover from solo-developer iteration, not a CI-triggered pre-production target, and interacts
awkwardly with catalog-based isolation. The technical approach is additive and conservative:
close each gap with the smallest change that satisfies its Functional Requirement and stays
inside the existing module boundaries (Principle VIII), without altering the pipeline's landing,
reconciliation, or cleaning *logic* beyond what FR-004/FR-005 require.

## Technical Context

**Language/Version**: Python (per `pyproject.toml`: `>=3.10,<=3.13`); no version change needed.

**Primary Dependencies**: PySpark via `databricks-connect` (pinned `>=15.4,<15.5`), `delta-spark`
(via Databricks Connect), `databricks-sdk` (`dbutils` runtime access), `requests` (NWOpen API
calls), `pytest` + `pytest-cov` (test suite). `databricks-dlt` is present in `pyproject.toml` but
unused by any current script — out of scope to remove/add to here.

**Storage**: Unity Catalog managed Delta tables (`raw.nwo_projects`, `base.nwo_projects`) plus a
Unity Catalog Volume (`/Volumes/<catalog>/raw/landing/nwo_projects/`) for landed JSON pages. No
new storage technology is introduced.

**Testing**: `pytest`, with Spark-backed tests running against real Free Edition serverless
compute via Databricks Connect (`tests/conftest.py` already wires this up) — per constitution
Principle III, mocks are intentionally not used for Spark/Delta behavior. This feature's central
task is making this suite a *required, blocking* CI gate; it does not change the test runner.

**Target Platform**: Databricks Free Edition, serverless-only compute (`environment_version: "4"`
job environments), one workspace/one metastore per account, no account console or account-level
API.

**Project Type**: Single project — a Databricks Asset Bundle (DAB) with Python job tasks. No
frontend/backend split.

**Performance Goals**: None specific; this is a small, low-volume personal demo (see spec
Assumptions). No new performance targets are introduced by this feature.

**Constraints**:
- Serverless-only, max 5 concurrent job tasks per account (existing `land_response` fan-out
  already caps `concurrency: 4`; a new CI test step also consumes serverless compute but runs as
  a separate Databricks Connect client session, not a job task, so it does not count against that
  job-task limit).
- No account-level API → no GitHub OIDC / workload-identity federation; CI auth is PAT-based
  (already decided, Principle VI) and this feature does not change that mechanism, only adds the
  test step that must authenticate the same way.
- Exactly one workspace for `dev`/`stage`/`prod`, distinguished by Unity Catalog catalog, not by
  workspace (Principle V, already decided).

**Scale/Scope**: Demo scale (see spec Assumptions: current volume is not a capacity concern).
Touches 4 existing source files, 2 CI workflow files, 1 bundle config file, 1 job resource file,
and adds/extends a handful of unit tests — no new services or modules.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

This feature's entire purpose is closing pre-existing gaps between the current code and the
constitution ratified 2026-09-14. The gaps below are the *current* state, not something this
plan's design introduces — the check below confirms the planned approach closes each one without
violating any other principle or adding undue complexity.

| Principle | Current state | Plan's approach | Gate |
|---|---|---|---|
| I. Immutable Landing, Reconciled Snapshot | Landing write is already at-most-once (file-exists check); `raw` MERGE already follows the merge/insert/delete pattern | No change to this behavior; FR-012/FR-013 explicitly preserve it | PASS |
| II. Idempotent, Full-Snapshot Reconciliation | `raw` MERGE and `base` overwrite are already idempotent | No change; new validation (FR-005) runs *before* merge/write, so it doesn't affect idempotency | PASS |
| III. Tests Gate Deployment (NON-NEGOTIABLE) | **Gap**: neither `stage_deployment.yaml` nor `prod_deployment.yaml` runs `pytest`; deploy runs unconditionally | Add a `test` job to both workflows that runs before `deploy`/`pipeline_update` and blocks them on failure, using the existing Databricks-Connect-backed suite | PASS (closes gap) |
| IV. Validate the Data Contract at the Boundary | **Gap**: `fetch_data` returns `response.json()` unchecked; `read_json_projects` uses a lenient schema-on-read that nulls out rather than fails on shape mismatch | Add explicit shape validation in `nwo_projects_api.py` immediately after parsing the response, raising a specific, typed error before the response is ever landed | PASS (closes gap) |
| IV (cont'd) / FR-005 | **Gap**: `create_full_snapshot_from_df` silently `dropDuplicates`s on `project_id`; nothing rejects null `project_id` | Replace silent dedup with an explicit validation step that fails the run on a duplicate or null `project_id`, in both `raw`'s snapshot-build step and `base`'s build step | PASS (closes gap) |
| V. Environment Parity via Catalogs, Not Workspaces | `databricks.yml` already defines `dev`/`stage`/`prod` sharing one `workspace.host`; `stage` uses `mode: development`, a mismatch for a CI-triggered pre-prod target | Reassess `stage`'s `mode` setting so it behaves predictably under unattended CI while catalog-level isolation (already correct) remains the isolation mechanism; documented as a research decision (see research.md) | PASS |
| VI. Least-Privilege, Secret-Free by Default | PAT-based CI auth already implemented and secret-scoped | New CI test job reuses the same `DATABRICKS_TOKEN`/`DATABRICKS_HOST` secrets/env, no new credential | PASS |
| VII. Observability by Default | **Gap**: no `on_failure` notification on the job resource; no freshness signal exposed anywhere | Add `on_failure` notification to the job resource; expose last-successful-refresh via a Delta table property set at the end of `update_base.py`, queryable in one `SHOW TBLPROPERTIES`/`DESCRIBE` call | PASS (closes gap) |
| VIII. Pure Logic, Isolated I/O | Existing modules already separate pure functions from `__main__` I/O wiring; `update_base.py`'s pure functions currently have **no** unit tests | New pure functions (shape validation, identifier validation) ship with unit tests in the same change; add missing coverage for `update_base.py`'s existing pure functions as part of this hardening work | PASS (closes gap) |

No new complexity, dependency, or architectural layer is introduced. **Complexity Tracking is not
needed.**

## Project Structure

### Documentation (this feature)

```text
specs/001-harden-demo-pipeline/
├── plan.md              # This file (/speckit-plan command output)
├── research.md          # Phase 0 output (/speckit-plan command)
├── data-model.md         # Phase 1 output (/speckit-plan command)
├── quickstart.md        # Phase 1 output (/speckit-plan command)
└── tasks.md             # Phase 2 output (/speckit-tasks command - NOT created by /speckit-plan)
```

No `contracts/` directory is generated for this feature — see Structure Decision below.

### Source Code (repository root)

```text
# Existing single-project layout (Option 1), retained as-is — no new top-level directories.
src/
├── nwo_projects_api.py     # fetch_data() + write_json_file(); gains response-shape validation
├── set_page_numbers.py     # unchanged
├── update_raw.py           # gains project_id null/duplicate validation before merge
└── update_base.py          # gains project_id null/duplicate validation; gains freshness
                             # table-property write; pure functions gain unit tests

tests/
├── conftest.py                 # unchanged (Databricks Connect session fixture)
├── nwo_projects_api_test.py    # gains shape-validation tests
├── set_page_numbers_test.py    # unchanged
├── update_raw_test.py          # existing dedup test updated to expect a raised error
│                                # instead of silent collapse (see data-model.md)
└── update_base_test.py         # NEW — first unit test coverage for update_base.py's
                                 # pure functions, including new validation

resources/
└── data_nwo_ingestion_pipeline.job.yml   # gains on_failure notification target

.github/workflows/
├── stage_deployment.yaml   # gains a blocking `test` job before `deploy`
└── prod_deployment.yaml    # gains a blocking `test` job before `deploy`

databricks.yml              # `stage` target's `mode` setting reassessed (see research.md);
                             # gains an owner-notification-email variable
```

**Structure Decision**: The existing single-project layout (`src/` + `tests/` + `resources/` +
bundle config) is retained unchanged — this feature is a hardening pass on an existing pipeline,
not a restructuring. No `contracts/` directory is produced: this project has no external interface
it exposes to other systems or users (it's a scheduled batch job consuming one external, publicly
documented third-party API and writing to its own Unity Catalog tables). The one existing external
contract — the NWOpen API's JSON response shape — is already documented in the constitution
(Principle IV) and is handled as runtime boundary validation (captured in data-model.md), not as a
contract this project defines and publishes.

## Complexity Tracking

*No Constitution Check violations — this section is intentionally empty.*
