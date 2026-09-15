# Quickstart: Validating the Hardened Pipeline

This is a validation guide, not an implementation guide — it documents how to prove each user
story in `spec.md` actually holds once this feature is implemented. Run these checks against a
real checkout with Databricks CLI access (`databricks auth login` or an existing profile) and,
where noted, GitHub Actions history for this repository.

## Prerequisites

- Databricks CLI installed and authenticated to the Free Edition workspace referenced in
  `databricks.yml` (`https://dbc-122f0c0d-dc6f.cloud.databricks.com`).
- `uv` installed (dependency/test runner already used by this repo — see `pyproject.toml`).
- Push access to a branch of this repo and the ability to open a pull request against `main`, to
  exercise the CI-gated stories end to end.

## User Story 1 — A broken change can never silently reach production

1. On a topic branch, introduce a change that fails an existing test (e.g. temporarily change an
   assertion in `tests/update_raw_test.py` to an impossible value).
2. Open a pull request against `main`.
3. In the PR's checks, confirm the new `test` job in `stage_deployment.yaml` fails, and confirm
   the `deploy`/`pipeline_update` jobs show as **skipped**, not failed-after-running — i.e. the
   bundle was never deployed or run against `stage`. This is checkable purely from the GitHub
   Actions run history, without reading any code (matches the spec's Independent Test).
4. Revert the failing change, push again, and confirm `test → deploy → pipeline_update` all run
   and succeed in order.
5. Merge the PR and confirm `prod_deployment.yaml` shows the same `test → deploy →
   pipeline_update` ordering on `push` to `main`.

**Expected outcome**: a failing check suite is visible in deployment history as a stopped
pipeline, never as a completed-then-broken deployment.

## User Story 2 — The data can be trusted, not just assumed correct

**Shape validation**:
1. Locally, call `nwo_projects_api.fetch_data` (or the new `validate_response_shape` helper
   directly) against a hand-built dict missing the `projects` key.
2. Confirm it raises `NWOpenAPIShapeError` with a message naming the missing field — this is
   exactly what `tests/nwo_projects_api_test.py`'s new tests assert; running `uv run pytest
   tests/nwo_projects_api_test.py` is the automated form of this check.

**Duplicate/missing identifier**:
1. Run `uv run pytest tests/update_raw_test.py tests/update_base_test.py`.
2. Confirm the duplicate-`project_id` fixture case (two records sharing `"001"`) now raises
   `DuplicateOrNullIdentifierError` instead of asserting a deduplicated row count.
3. Confirm a fixture row with a null `project_id` is rejected the same way, for both the `raw`-
   bound and `base`-bound validation paths.

**Expected outcome**: a malformed API response or a duplicate/missing identifier fails the run
with a specific, readable error — never a silently short or duplicated table.

## User Story 3 — Anyone can tell whether the data is current and the pipeline is healthy

**Freshness lookup** (after at least one successful run against any target):
```bash
databricks bundle run data_nwo_ingestion_pipeline_job --target stage
databricks query-something-or-sql "SHOW TBLPROPERTIES stage.base.nwo_projects" # or via a SQL warehouse / notebook
```
Confirm `pipeline.last_successful_refresh_utc` appears with a recent UTC timestamp, retrievable in
one lookup — no job-run-history UI, no source code required (matches SC-003's "under one minute").

**Failure notification**:
1. Temporarily point the job at an invalid catalog (or otherwise force a task to fail) and trigger
   a run.
2. Confirm an email arrives at the configured `owner_email` address without opening the Databricks
   Jobs UI.
3. Revert the induced failure.

**Expected outcome**: freshness is a one-query fact; a failed scheduled run produces a
notification with no manual monitoring step.

## User Story 4 — Environments stay isolated on a single free account

1. `databricks bundle run data_nwo_ingestion_pipeline_job --target stage` and, separately, `...
   --target prod`.
2. Query `stage.raw.nwo_projects` / `stage.base.nwo_projects` and `prod.raw.nwo_projects` /
   `prod.base.nwo_projects` and confirm each target only ever wrote to its own catalog — e.g.
   compare row counts or a run-identifying value landed by each run.
3. Confirm the `stage` target's deployed job/resource names in the workspace are the plain,
   undecorated names (post research.md §6 change) rather than `[dev ...]`-prefixed, and that this
   doesn't cause it to collide with or read `prod`'s resources.

**Expected outcome**: a `stage` run never touches `prod` data or vice versa, purely via catalog
separation.

## User Story 5 — Automated deployment works without an organizational identity system

1. From a fresh clone (no local Databricks CLI login), confirm `stage_deployment.yaml`/
   `prod_deployment.yaml` authenticate entirely via the `DATABRICKS_TOKEN` GitHub Actions secret —
   no interactive step appears anywhere in the run logs.
2. `grep` the repository's tracked files and the workflow run logs for the literal token value (or
   any credential-shaped string) and confirm no match — only the secret *reference*
   (`${{ secrets.DATABRICKS_TOKEN }}`) appears in tracked files.

**Expected outcome**: deployment runs unattended end to end, and no credential value is ever
visible in tracked files or logs.
