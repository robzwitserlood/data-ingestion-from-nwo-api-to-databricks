# Phase 1 Data Model: Harden Ingestion Pipeline for Free-Edition Demo

This feature does not introduce new tables or storage technology. It adds validation rules,
error types, and one new piece of metadata (a table property) to the existing entities named in
the spec's Key Entities section. Each entity below states what already exists, what this feature
changes, and the validation rules that now apply.

## Source Page

**Storage**: JSON file in a Unity Catalog Volume, `/Volumes/<catalog>/raw/landing/nwo_projects/
<run_id>_page<page_nr>.json`.

**Identity**: `(run_id, page_nr)` — one file per run per page number.

**Fields** (as received from the NWOpen API, landed verbatim): `projects` (array of JSON-encoded
project strings), `meta` (object, including `pages`: total page count at fetch time).

**Existing invariant (unchanged by this feature)**: a file for a given `(run_id, page_nr)` is
written at most once; if it already exists, the write is skipped (Principle I).

**New validation rule (FR-004)**: before a Source Page is ever written to the Volume, the raw
response it's built from MUST pass shape validation — `projects` present and a list, `meta.pages`
present and an int. A response that fails this check is never landed; `fetch_data` raises
`NWOpenAPIShapeError` (a new, specific exception type) instead of returning the malformed data to
its caller.

## Reconciled Record

**Storage**: Unity Catalog managed Delta table `<catalog>.raw.nwo_projects`.

**Identity**: `project_id` (string) — MUST be unique and non-null (constitution Principle IV;
spec FR-005).

**Fields**: `project_id`, `project` (raw JSON string of the full record), `row_hash` (MD5 of
`project`, used to detect content changes across runs).

**Existing invariant (unchanged by this feature)**: full-snapshot MERGE against the source on
every run — update rows whose `row_hash` changed, insert new rows, delete rows no longer present
(Principles I & II).

**Changed validation rule (FR-005)**: the pre-MERGE snapshot DataFrame (built by
`create_full_snapshot_from_df`) MUST be checked for null or duplicate `project_id` values *before*
column-level dedup is applied. Today's `.dropDuplicates(subset=['project_id'])` call is replaced
by an explicit check that raises a new, specific exception (e.g. `DuplicateOrNullIdentifierError`)
naming the offending count, rather than silently keeping one row per duplicate group. A run that
hits this condition MUST fail — no partial or best-effort MERGE occurs.

## Clean Record

**Storage**: Unity Catalog managed Delta table `<catalog>.base.nwo_projects`.

**Identity**: `project_id` (string) — MUST be unique and non-null, same as Reconciled Record
(FR-005 applies to *both* `raw` and `base`).

**Fields**: `project_id` plus one column per `COLUMN_SPECS` entry in `update_base.py` (`title`,
`department`, `sub_department`, `reporting_year`, `start_date`, `end_date`, `award_amount`,
`summary_nl`, `summary_en`), each cast to its declared type, with placeholder "empty" sentinels
normalized to true `NULL` (Principle I/FR-014, unchanged).

**Existing invariant (unchanged by this feature)**: fully rebuilt (`overwrite`) from `raw.
nwo_projects` on every run, never incrementally patched (Principle II).

**New validation rule (FR-005)**: the same null/duplicate `project_id` check applied to the
Reconciled Record snapshot is also applied here, immediately before the final
`typed.write.mode('overwrite').saveAsTable(...)` call, since `raw` and `base` are independently
in scope for this guarantee per FR-005's wording ("in the reconciled record store and in the
cleaned, query-ready table").

**New metadata field**: `base.nwo_projects` gains a Delta table property,
`pipeline.last_successful_refresh_utc`, set to the current UTC timestamp immediately after the
`overwrite` write succeeds. This is the freshness signal required by FR-006/SC-003. It has no
effect on the table's row data or schema — it is metadata only, retrievable via `SHOW
TBLPROPERTIES` or `DESCRIBE TABLE EXTENDED`.

## Pipeline Run

**Storage**: Databricks job run (native platform concept — no new table). Outcome (success/
failure) and completion timestamp are already recorded by the Databricks Jobs runtime; this
feature does not duplicate that state anywhere.

**New behavior (FR-007)**: a Pipeline Run whose outcome is failure MUST trigger an email
notification (see research.md §5) — configured declaratively on the job resource, not something
the pipeline code itself needs to detect or act on.

**Relationship to Clean Record's new freshness field**: a *successful* Pipeline Run is exactly
what advances `pipeline.last_successful_refresh_utc` on `base.nwo_projects` (see above) — a failed
run leaves that property at its previous value, which is the correct "last **successful**
refresh" semantics.

## Environment Space

**Storage**: Unity Catalog catalog (`dev`, `stage`, or `prod`) within the single Free Edition
workspace — already modeled this way in `databricks.yml` (Principle V, unchanged by this
feature).

**Changed configuration (FR-008, FR-009)**: the `stage` target's `mode: development` setting is
removed (see research.md §6) so that `stage`'s deployed resource *names* and trigger behavior are
no longer subject to development-mode name-prefixing/auto-pausing — isolation itself continues to
come entirely from the catalog variable (`${bundle.target}`), which already differs correctly
across `dev`/`stage`/`prod` and is unaffected by this change.

## New Error Types Introduced

| Type | Raised by | When | Satisfies |
|---|---|---|---|
| `NWOpenAPIShapeError` (`ValueError` subclass) | `nwo_projects_api.fetch_data` (via a new `validate_response_shape` helper) | API response missing/mis-shaped `projects` or `meta.pages` | FR-004 |
| `DuplicateOrNullIdentifierError` (`ValueError` subclass, or equivalent) | new shared validation helper used by both `update_raw.py` and `update_base.py` | snapshot DataFrame has a null or repeated `project_id` | FR-005 |

Both are plain Python exceptions raised from pure functions (Principle VIII) — no new dependency
is needed to define or test them; a `spark_python_task` that raises any exception already fails
its job task and thus the run, which is what FR-004/FR-005's "fail the run visibly" requires.
