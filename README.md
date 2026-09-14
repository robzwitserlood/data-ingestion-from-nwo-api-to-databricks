# data-ingestion-from-nwo-api-to-databricks

[![Stage deployment](https://github.com/robzwitserlood/data-ingestion-from-nwo-api-to-databricks/actions/workflows/stage_deployment.yaml/badge.svg)](https://github.com/robzwitserlood/data-ingestion-from-nwo-api-to-databricks/actions/workflows/stage_deployment.yaml)
[![Production deployment](https://github.com/robzwitserlood/data-ingestion-from-nwo-api-to-databricks/actions/workflows/prod_deployment.yaml/badge.svg)](https://github.com/robzwitserlood/data-ingestion-from-nwo-api-to-databricks/actions/workflows/prod_deployment.yaml)

This is a personal portfolio project that downloads grant/project data from the [NWOpen API](https://www.nwo.nl) and loads it into Databricks so it can be queried as tables, showcasing production-grade data engineering practices on free-tier infrastructure (Databricks Free Edition). It's built as a **Databricks Asset Bundle (DAB)** — Databricks' way of defining a data pipeline as code (YAML + Python files in this repo) instead of clicking things together in the workspace UI. That means the whole pipeline — including which tables it creates and how often it runs — is version-controlled here and deployed automatically by CI/CD.

Project governance (principles the pipeline is held to, and why) lives in [.specify/memory/constitution.md](.specify/memory/constitution.md); feature work is specified before it's built under [specs/](specs/), following the [Spec-Driven Development](https://github.com/github/spec-kit) workflow.

If you're new to Databricks, the [Quick glossary](#quick-glossary) section below explains the terms used throughout this document.

## Table of contents

- [data-ingestion-from-nwo-api-to-databricks](#data-ingestion-from-nwo-api-to-databricks)
  - [Table of contents](#table-of-contents)
  - [Quick glossary](#quick-glossary)
  - [How the pipeline works](#how-the-pipeline-works)
  - [Environments](#environments)
  - [Repository layout](#repository-layout)
  - [Getting started](#getting-started)
  - [Running the tests](#running-the-tests)
  - [CI / CD](#ci--cd)
  - [Contributing](#contributing)
  - [Ownership](#ownership)

## Quick glossary

A few Databricks/Spark terms come up a lot in this repo — here's what they mean in plain language:

| Term | What it means here |
| --- | --- |
| **Bundle** | A folder of YAML + code (this repo) that describes everything to deploy: jobs, tables, permissions. Deployed with the `databricks` CLI or by CI. |
| **Job** | A scheduled pipeline in Databricks, made up of one or more **tasks**. Ours is called `data_nwo_ingestion_pipeline_job`, defined in [resources/data_nwo_ingestion_pipeline.job.yml](resources/data_nwo_ingestion_pipeline.job.yml). |
| **Task** | One step in a job (e.g. "call the API", "merge into a table"). Tasks can depend on each other, forming a small pipeline (a DAG). |
| **Catalog / schema / table** | Databricks organizes data like `catalog.schema.table` — similar to `database.folder.file`. Our catalog is the environment name (`dev`, `stage`, or `prod`); schemas are `raw` and `base`. |
| **Volume** | A managed storage location for plain files (here: the JSON pages we download from the API), addressed like `/Volumes/<catalog>/<schema>/...`. |
| **Delta table** | A regular table, but one that supports reliable inserts/updates/deletes (a "merge", i.e. an upsert) instead of only appending data. |
| **Serverless compute** | Databricks-managed compute that starts on demand — we don't provision or manage clusters ourselves. |
| **Databricks Connect** | A library that lets Python code (e.g. our tests, running on your laptop or in CI) talk to a real Databricks cluster/serverless compute as if it were a local Spark session. |

## How the pipeline works

The job runs four tasks in sequence, each a Python script in [src/](src/):

1. **[set_page_numbers.py](src/set_page_numbers.py)** — asks the NWOpen API how many pages of results exist, then decides which page numbers to fetch (all of them, or just the most recent `max_pages`, a setting per environment).
2. **[nwo_projects_api.py](src/nwo_projects_api.py)** — runs once per page (in parallel, up to 4 at a time). Downloads that page from the API and writes the raw JSON to a volume.
3. **[update_raw.py](src/update_raw.py)** — reads all the downloaded JSON pages, flattens the list of projects, computes a hash per project (to detect changes), removes duplicates, and **merges** the result into the `raw.nwo_projects` Delta table: changed projects are updated, new ones inserted, and projects no longer in the API response are removed.
4. **[update_base.py](src/update_base.py)** — reads `raw.nwo_projects`, pulls out individual fields from the raw JSON (title, department, dates, amounts, summaries, …), cleans up placeholder "empty" values (e.g. `"n/a"`, `"-"`), casts everything to the right type, and writes the result to `base.nwo_projects` — a clean, ready-to-query table.

In short: **API → JSON files in a volume → raw Delta table → clean, typed Delta table.**

## Environments

The bundle defines three targets in [databricks.yml](databricks.yml). Databricks Free Edition
provides exactly one workspace and one metastore per account, so all three targets share that one
workspace; `dev`/`stage`/`prod` are separated by Unity Catalog **catalog**, not by workspace (see
the constitution's "Environment Parity via Catalogs" principle):

| Target | Purpose | How it's triggered |
| --- | --- | --- |
| `dev` | Personal development catalog. Resources are prefixed with your username and schedules are paused by default — safe to experiment in. Fetches only the last 8 pages, refreshed daily, so iteration is fast. | Deployed manually from your own machine. |
| `stage` | Pre-production catalog. | Deployed and run automatically on every pull request into `main` (see [CI / CD](#ci--cd)). |
| `prod` | Production catalog. Refreshes on a weekly schedule (configurable via the `trigger_periodic_unit` bundle variable). | Deployed and run automatically whenever a PR is merged into `main`. |

## Repository layout

```text
src/
  set_page_numbers.py    # task 1: decide which API pages to fetch
  nwo_projects_api.py     # task 2: download one page from the API
  update_raw.py           # task 3: merge downloaded pages into the raw Delta table
  update_base.py          # task 4: clean/type the raw table into the base table
resources/
  data_nwo_ingestion_pipeline.job.yml  # the job definition (tasks, schedule, retries)
tests/                     # unit tests, one file per src/ module
fixtures/                  # sample data used by tests
databricks.yml             # bundle definition: targets (dev/stage/prod), variables
.github/workflows/
  stage_deployment.yaml    # CI: deploy + run in stage on every PR to main
  prod_deployment.yaml     # CI: deploy + run in prod on every merge to main
pyproject.toml, uv.lock    # Python dependencies (managed with uv)
```

## Getting started

1. Install the [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html) (bundle support included) and [uv](https://docs.astral.sh/uv/) for dependency management.
2. Authenticate the CLI against your workspace: `databricks auth login`.
3. Install the Python dependencies: `uv sync`.
4. Deploy the pipeline to your personal `dev` environment:

   ```bash
   databricks bundle deploy --target dev
   ```

5. Run it once to try it out:

   ```bash
   databricks bundle run data_nwo_ingestion_pipeline_job --target dev
   ```

You can also open the [databricks.yml](databricks.yml) targets and job resource file to see or tweak settings like the schedule or how many pages `dev` fetches.

## Running the tests

```bash
uv run pytest
```

Note that these tests don't mock Spark — they use Databricks Connect to run against real (serverless) compute in your Databricks workspace, so you need to be authenticated (`databricks auth login`) before running them. This catches real Spark/Delta behavior that a mocked test would miss, at the cost of needing a live connection.

When adding new logic, keep the pattern already used in `src/`: separate pure/testable functions (parsing, transforming) from the `if __name__ == "__main__"` block that wires them together with Spark and CLI arguments, and add a matching test in `tests/`.

## CI / CD

Two GitHub Actions workflows deploy the bundle automatically — no manual deployment is needed for `stage` or `prod`:

- **[stage_deployment.yaml](.github/workflows/stage_deployment.yaml)** — on every pull request into `main`, deploys the bundle to `stage` and runs the job once.
- **[prod_deployment.yaml](.github/workflows/prod_deployment.yaml)** — on every push (merge) to `main`, deploys the bundle to `prod` and runs the job once.

Both authenticate to Databricks using a personal access token stored as a GitHub Actions secret
(`DATABRICKS_TOKEN`) — Databricks Free Edition has no account console or account-level APIs, so
GitHub OIDC / service-principal federation isn't available here. Each workflow has a concurrency
guard so overlapping deployments to the same target can't run at once.

> **Note**: as of this baseline, these workflows deploy and run the job but do not yet run the
> test suite as a gate beforehand. Closing this gap is tracked as a specified feature (see the
> [constitution](.specify/memory/constitution.md), Principle III) developed on its own branch.

## Contributing

- Open an issue for bugs or feature requests.
- Work on a topic branch and open a pull request into `main` — this automatically deploys your change to `stage` so it can be reviewed running for real, not just read as a diff.
- Add or update tests for any new logic.

## Ownership

See [CODEOWNERS](CODEOWNERS). This is a personal portfolio project; there is no public license.
