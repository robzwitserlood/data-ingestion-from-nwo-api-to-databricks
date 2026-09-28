---

description: "Task list for hardening the NWO ingestion pipeline for Free-Edition demo"
---

# Tasks: Harden Ingestion Pipeline for Free-Edition Demo

**Input**: Design documents from `/specs/001-harden-demo-pipeline/`

**Prerequisites**: [plan.md](./plan.md), [spec.md](./spec.md), [research.md](./research.md), [data-model.md](./data-model.md), [quickstart.md](./quickstart.md)

**Tests**: Test changes are explicitly called for by plan.md/data-model.md (an existing test's
expectation must change, and `update_base.py` gains its first unit tests) — they are included
below as concrete tasks, scoped to User Story 2 where the underlying behavior changes.

**Organization**: Tasks are grouped by user story (from spec.md, priority order P1→P5) to enable
independent implementation and testing of each story.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: Can run in parallel (different files, no dependencies)
- **[Story]**: Which user story this task belongs to (US1–US5)
- Every task includes its exact file path

## Path Conventions

Existing single-project layout, unchanged by this feature: `src/`, `tests/`, `resources/`,
`.github/workflows/`, and `databricks.yml` at the repository root.

---

## Phase 1: Setup

No dedicated setup tasks. This feature is a hardening pass on an existing, fully-scaffolded
project (per plan.md's Project Structure): no new dependency, top-level directory, or module is
introduced, and no project initialization is required. Proceed directly to Phase 3.

---

## Phase 2: Foundational

No blocking cross-story prerequisites. Each user story below touches its own distinct set of
files (CI workflows for US1; `nwo_projects_api.py`/`update_raw.py`/`update_base.py` and their
tests for US2; `update_base.py`/the job resource/`databricks.yml` for US3; `databricks.yml` for
US4; verification only for US5) with no shared infrastructure that must be built first. Proceed
directly to Phase 3.

---

## Phase 3: User Story 1 - A broken change can never silently reach production (Priority: P1) 🎯 MVP

**Goal**: The existing `pytest` suite actually gates deployment: a failing check stops a proposed
change before it reaches the `stage` (pre-production) or `prod` space; a passing change deploys
and runs exactly as before.

**Independent Test**: Introduce a change that fails an existing test, propose it the normal way
(PR), and confirm — purely from GitHub Actions run history — that it is stopped before reaching
`stage` or `prod`.

### Implementation for User Story 1

- [X] T001 [P] [US1] In [.github/workflows/stage_deployment.yaml](.github/workflows/stage_deployment.yaml), add a `test` job that: checks out the repo (`actions/checkout@v4`), installs `uv`, sets up Python matching `pyproject.toml`'s `requires-python = ">=3.10,<=3.13"`, runs `uv sync` for the `dev` dependency group, reuses the workflow's existing `env:`-level `DATABRICKS_HOST`/`DATABRICKS_TOKEN`, and runs `uv run pytest`; then add `needs: [test]` to the `deploy` job so a failing `test` job causes `deploy` (and therefore `pipeline_update`) to be skipped rather than run (FR-001, FR-002).
- [X] T002 [P] [US1] In [.github/workflows/prod_deployment.yaml](.github/workflows/prod_deployment.yaml), add the equivalent `test` job (same steps as T001, reusing this workflow's own `DATABRICKS_HOST`/`DATABRICKS_TOKEN` `env:` block) and add `needs: [test]` to its `deploy` job, so the `main`-branch promotion path is gated the same way as `stage` (FR-003).
- [ ] T003 [US1] Validate the Independent Test for User Story 1 per [quickstart.md](./quickstart.md) "User Story 1" section: on a topic branch, break an existing test, open a PR, confirm `test` fails and `deploy`/`pipeline_update` show as **skipped** (not failed-after-running) in the Actions history for [.github/workflows/stage_deployment.yaml](.github/workflows/stage_deployment.yaml); revert, confirm `test → deploy → pipeline_update` all succeed in order; merge and confirm the same ordering in [.github/workflows/prod_deployment.yaml](.github/workflows/prod_deployment.yaml).

**Checkpoint**: A broken change can no longer reach `stage` or `prod` — verifiable purely from
deployment history. This alone is the MVP for this feature.

---

## Phase 4: User Story 2 - The data can be trusted, not just assumed correct (Priority: P2)

**Goal**: A malformed NWOpen API response fails the run visibly and specifically instead of
landing; a duplicate or missing `project_id` is rejected rather than silently collapsed or stored,
in both the reconciled (`raw`) and cleaned (`base`) tables.

**Independent Test**: Feed the pipeline a source response missing its `projects` field and confirm
the run fails with a specific error rather than producing empty/partial output; separately,
confirm the `raw` and `base` tables reject a duplicate or missing `project_id`.

### Tests for User Story 2 (write/update first, confirm they fail against current code)

- [X] T004 [P] [US2] In [tests/nwo_projects_api_test.py](tests/nwo_projects_api_test.py), fix `test_fetch_data`'s mocked response to a shape-valid payload (`projects` as a list, `meta` as a dict containing an int `pages`, since the current mock's `{'projects': 'test_data', 'meta': {'page': 2}}` would now fail validation), and add new tests asserting `fetch_data`/`validate_response_shape` raises `NWOpenAPIShapeError`, naming the specific offending field, for each of: missing `projects`, `projects` not a list, missing `meta`, `meta` not a dict, `meta` missing `pages`, and `meta.pages` not an int — per data-model.md's rule that a response must have "`projects` present and a list, `meta.pages` present and an int" (FR-004).
- [X] T005 [P] [US2] In [tests/update_raw_test.py](tests/update_raw_test.py), change `test_create_full_snapshot_from_df`'s expectation from `result.count() == 5` to asserting `create_full_snapshot_from_df(dataframe_raw)` raises `DuplicateOrNullIdentifierError` (the fixture's duplicated `project_id` `"001"` must now fail the run, not be silently collapsed to one row), and add a new test asserting a fixture row with a null `project_id` is rejected the same way (FR-005, spec User Story 2 Acceptance Scenarios 2 and 3).
- [X] T006 [P] [US2] Create [tests/update_base_test.py](tests/update_base_test.py) — the first unit test coverage for `update_base.py`'s pure functions — with tests for `parse_project_columns`, `cleanse_missing_values` (covering empty/whitespace-only strings, one-or-more hyphen/en-dash/em-dash sequences, and case-insensitive `n/a`/`na`/`none`/`null` tokens all normalizing to `NULL`, per the function's existing docstring), `apply_types`, and the new duplicate/null-`project_id` validation path (both cases must raise `DuplicateOrNullIdentifierError`), using the `spark` fixture from [tests/conftest.py](tests/conftest.py).

### Implementation for User Story 2

- [X] T007 [P] [US2] In [src/nwo_projects_api.py](src/nwo_projects_api.py), add `NWOpenAPIShapeError(ValueError)` and a pure function `validate_response_shape(data: dict) -> None` that raises it, naming the exact missing/invalid field, when: `data` lacks a `"projects"` key or its value is not a list; `data` lacks a `"meta"` key or its value is not a dict; or `data["meta"]` lacks a `"pages"` key or its value is not an int. Call `validate_response_shape(data)` inside `fetch_data` immediately after `response.json()` and before returning, so a malformed response is rejected before `write_json_file` ever lands it (FR-004; research.md §2).
- [X] T008 [P] [US2] In [src/update_raw.py](src/update_raw.py), add `DuplicateOrNullIdentifierError(ValueError)` and a pure function `assert_unique_not_null_ids(df: DataFrame, id_col: str = 'project_id') -> DataFrame` that raises it — naming the count of null values and the count of duplicated values found in `id_col` — and otherwise returns `df` unchanged; replace the existing `.dropDuplicates(subset=['project_id'])` call inside `create_full_snapshot_from_df` with a call to `assert_unique_not_null_ids(..., 'project_id')` so a conflicting or missing identifier fails the run instead of being silently collapsed (FR-005; research.md §3).
- [X] T009 [US2] In [src/update_base.py](src/update_base.py), import `assert_unique_not_null_ids` and `DuplicateOrNullIdentifierError` from `update_raw` and call `assert_unique_not_null_ids(typed, 'project_id')` immediately before the final `typed.write.mode('overwrite').saveAsTable(...)` call, so `base.nwo_projects` is protected by the same null/duplicate guarantee as `raw.nwo_projects` (FR-005; data-model.md's Clean Record "New validation rule"). Depends on T008 (the shared helper must exist in `update_raw.py` first).
- [X] T010 [US2] Validate the Independent Test for User Story 2 per [quickstart.md](./quickstart.md) "User Story 2" section: run `uv run pytest tests/nwo_projects_api_test.py tests/update_raw_test.py tests/update_base_test.py` and confirm the shape-validation and duplicate/null-identifier cases fail with the new specific error types rather than producing empty, partial, or duplicated output.

**Checkpoint**: A malformed API response or a duplicate/missing `project_id` now fails the run
visibly, in both `raw` and `base` — independently testable via `uv run pytest`.

---

## Phase 5: User Story 3 - Anyone can tell whether the data is current and the pipeline is healthy (Priority: P3)

**Goal**: The timestamp of the last successful refresh is retrievable in a single lookup, and a
failed scheduled run triggers an email notification without anyone checking manually.

**Independent Test**: Force a scheduled run to fail and confirm a notification is raised
unprompted; separately, confirm the recency of the last successful refresh is available in a
single lookup.

### Implementation for User Story 3

- [X] T011 [US3] In [src/update_base.py](src/update_base.py), after the `typed.write.mode('overwrite').saveAsTable(...)` call succeeds, set the Delta table property `pipeline.last_successful_refresh_utc` to the current UTC timestamp (e.g. via `ALTER TABLE {catalog}.{schema_to}.nwo_projects SET TBLPROPERTIES (...)` or the equivalent writer option), so it is retrievable via `SHOW TBLPROPERTIES`/`DESCRIBE TABLE EXTENDED` with no job-run-history lookup or source access (FR-006; research.md §4). A failed run must leave the property at its previous value — only place this call after the write has already succeeded.
- [X] T012 [P] [US3] In [tests/update_base_test.py](tests/update_base_test.py) (created in T006), add a unit test asserting the freshness-timestamp function set-up in T011 is only invoked after a successful write and is passed the correct fully-qualified table name and a UTC timestamp, keeping the function itself a pure, isolated unit per Principle VIII.
- [X] T013 [P] [US3] In [resources/data_nwo_ingestion_pipeline.job.yml](resources/data_nwo_ingestion_pipeline.job.yml), add an `email_notifications` block with `on_failure: [${var.owner_email}]` to the `data_nwo_ingestion_pipeline_job` job resource, so a failed scheduled run notifies the owner without any manual monitoring step (FR-007; research.md §5).
- [X] T014 [P] [US3] In [databricks.yml](databricks.yml), add a new bundle variable `owner_email` (description: "Email address to notify on job failure", default: `ikdeelgeengegevensmet@gmail.com`, matching the account's existing address already visible in `workspace.root_path`) so the job's `on_failure` target in T013 is configurable per target/person without a code change (research.md §5).
- [ ] T015 [US3] Validate the Independent Test for User Story 3 per [quickstart.md](./quickstart.md) "User Story 3" section: after a successful run against any target, run `SHOW TBLPROPERTIES <catalog>.base.nwo_projects` and confirm `pipeline.last_successful_refresh_utc` shows a recent UTC timestamp (under one minute, no job-history UI); separately, force a task failure and confirm an email notification arrives at the configured `owner_email` without opening the Databricks Jobs UI.

**Checkpoint**: Freshness is a one-query fact and a failed scheduled run produces an unprompted
notification — independently testable against a deployed target.

---

## Phase 6: User Story 4 - Environments stay isolated on a single free account (Priority: P4)

**Goal**: `stage` behaves as a faithful, CI-triggered pre-production rehearsal of `prod` — not a
human developer's throwaway sandbox — while catalog-based data isolation (already correct)
continues to keep the two spaces' data separate.

**Independent Test**: Run the pipeline against `stage` and confirm it does not read, write, or
otherwise affect `prod`'s data, and vice versa.

### Implementation for User Story 4

- [X] T016 [US4] In [databricks.yml](databricks.yml), remove `mode: development` from the `stage` target, leaving only its `workspace.root_path` and the target-derived catalog (`${bundle.target}` = `stage`) as what distinguishes it from `prod` — so `stage`'s deployed resource names are no longer `[dev <username>]`-prefixed and its trigger is no longer auto-paused, matching its actual role as a CI-triggered pre-production target (FR-008, FR-009; research.md §6). Leave `dev`'s `mode: development` unchanged.
- [ ] T017 [US4] Validate the Independent Test for User Story 4 per [quickstart.md](./quickstart.md) "User Story 4" section: run `databricks bundle run data_nwo_ingestion_pipeline_job --target stage` and separately `--target prod`; query `stage.raw.nwo_projects`/`stage.base.nwo_projects` and `prod.raw.nwo_projects`/`prod.base.nwo_projects` and confirm each run only wrote to its own catalog; confirm `stage`'s deployed job/resource names are now plain (no `[dev ...]` prefix) and do not collide with `prod`'s.

**Checkpoint**: `stage` and `prod` remain fully isolated purely via catalog separation, with
`stage` now behaving as a faithful pre-production rehearsal.

---

## Phase 7: User Story 5 - Automated deployment works without an organizational identity system (Priority: P5)

**Goal**: Confirm the automated deployment path (including the new `test` job from User Story 1)
authenticates unattended via the existing PAT-based secret, with no credential value ever exposed.

**Independent Test**: Trigger an automated deployment from a fresh checkout with no manual login
step and confirm it authenticates and completes successfully, with no credential value visible in
tracked files or logs.

This story requires no new production code — PAT-based CI authentication (`DATABRICKS_HOST`/
`DATABRICKS_TOKEN` secrets, reused unchanged by User Story 1's new `test` job) is already in place
per plan.md's Constitution Check (Principle VI: PASS, no gap). The tasks below are verification.

### Verification for User Story 5

- [ ] T018 [US5] Validate the Independent Test for User Story 5 per [quickstart.md](./quickstart.md) "User Story 5" section: from a fresh clone with no local Databricks CLI login, trigger [.github/workflows/stage_deployment.yaml](.github/workflows/stage_deployment.yaml) and [.github/workflows/prod_deployment.yaml](.github/workflows/prod_deployment.yaml) (including the `test` job added in T001/T002) and confirm every job authenticates solely via the `DATABRICKS_TOKEN`/`DATABRICKS_HOST` GitHub Actions secrets/env, with no interactive step anywhere in the run logs (FR-010).
- [X] T019 [P] [US5] Grep this repository's tracked files and a completed workflow run's logs for the literal `DATABRICKS_TOKEN` secret value (or any credential-shaped string) and confirm no match — only the secret reference `${{ secrets.DATABRICKS_TOKEN }}` appears in tracked files (FR-011).

**Checkpoint**: Automated deployment is confirmed to run unattended with zero exposed credential
values — the project owner can operate and redeploy solo.

---

## Phase 8: Polish & Cross-Cutting Concerns

**Purpose**: Final sign-off across all five stories together.

- [X] T020 [P] Run `uv run pytest` locally (full suite) to confirm every test across [tests/nwo_projects_api_test.py](tests/nwo_projects_api_test.py), [tests/update_raw_test.py](tests/update_raw_test.py), [tests/update_base_test.py](tests/update_base_test.py), and [tests/set_page_numbers_test.py](tests/set_page_numbers_test.py) passes together after all changes from Phases 3–7.
- [ ] T021 Run the full [quickstart.md](./quickstart.md) validation guide end-to-end (all five user stories in order) as the final acceptance sign-off before merging this feature.

---

## Dependencies & Execution Order

### Phase Dependencies

- **Setup (Phase 1)** and **Foundational (Phase 2)**: No tasks — proceed directly to Phase 3.
- **User Story 1 (Phase 3, P1)**: No dependencies — can start immediately. This is the MVP.
- **User Story 2 (Phase 4, P2)**: No dependency on US1; touches entirely different files. Can run
  in parallel with US1 if staffed, or sequentially after it.
- **User Story 3 (Phase 5, P3)**: T011 edits `update_base.py`, the same file US2's T009 changes —
  do T009 before T011 to avoid a merge conflict, even though the two stories are logically
  independent.
- **User Story 4 (Phase 6, P4)**: T016 edits `databricks.yml`, the same file US3's T014 changes —
  do T014 before T016 to avoid a merge conflict.
- **User Story 5 (Phase 7, P5)**: T018 exercises the CI workflows built in US1 (T001/T002) — do
  those first.
- **Polish (Phase 8)**: Depends on all five user stories being complete.

### Within Each User Story

- User Story 2: tests (T004–T006) before implementation (T007–T009); T009 depends on T008 (shared
  helper import).
- User Story 3: T011 (impl) before T012 (its test); T013/T014 (job resource + bundle variable) are
  independent of T011/T012 and of each other.

### Parallel Opportunities

- T001 and T002 (the two CI workflow files) can run in parallel.
- T004, T005, T006 (three independent test files) can run in parallel.
- T007 and T008 (two independent source files) can run in parallel with each other and with T004–T006.
- T012, T013, T014 can run in parallel with each other.
- T019 can run any time after T001/T002 exist.
- T020 can run in parallel with T021 only in the sense that both are read-only validation; run
  T020 first since T021 subsumes it.

---

## Parallel Example: User Story 2

```bash
# Launch all three test-file updates for User Story 2 together:
Task: "Fix test_fetch_data mock + add NWOpenAPIShapeError negative cases in tests/nwo_projects_api_test.py"
Task: "Change duplicate-id expectation + add null-id case in tests/update_raw_test.py"
Task: "Create tests/update_base_test.py with pure-function + validation coverage"

# Then launch the two independent implementation tasks together:
Task: "Add NWOpenAPIShapeError + validate_response_shape in src/nwo_projects_api.py"
Task: "Add DuplicateOrNullIdentifierError + assert_unique_not_null_ids in src/update_raw.py"

# T009 (src/update_base.py) waits for the update_raw.py task above to finish.
```

---

## Implementation Strategy

### MVP First (User Story 1 Only)

1. Complete Phase 3 (User Story 1): CI actually gates deployment on `pytest`.
2. **STOP and VALIDATE**: run T003's manual PR check.
3. This alone satisfies the spec's highest-priority gap and can be merged/demoed on its own.

### Incremental Delivery

1. Phase 3 (US1) → the automated-check gate → demo-ready on its own.
2. Phase 4 (US2) → data correctness guarantees → demo-ready increment.
3. Phase 5 (US3) → freshness + failure notification → demo-ready increment.
4. Phase 6 (US4) → `stage` mode correction → demo-ready increment.
5. Phase 7 (US5) → verification only, confirms the solo-operator authentication story holds.
6. Phase 8 → final full-suite + quickstart sign-off.

Each phase leaves the pipeline in a fully working, deployable state — none of the five stories
requires a later one to function.
