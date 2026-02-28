# Empty Directories Policy

Date: 2026-02-28

## Policy

Empty directories are not tracked unless they carry clear ownership and implementation intent.
For this repository, each empty directory discovered during hardening was assigned one decision:

- `A` = remove from git scope (preferred for unused placeholders)
- `B` = keep and document purpose

## Decisions From This Hardening Pass

| Directory | Decision | Rationale |
| --- | --- | --- |
| `dbt/lakehouse_dbt` | B | Converted from empty placeholder to a real optional dbt project with models/tests/docs/macros. |
| `.ipynb_checkpoints` | A | Local notebook artifact; should never be tracked. |
| `ci/github-actions` | A | Empty placeholder not used by current CI layout. |
| `ci/test-data` | A | No active test fixtures in this folder; keep out of tracked scope until needed. |
| `great_expectations` | A | Current quality framework is contract + custom validators; no active GE project files. |
| `orchestration/airflow` | A | Airflow is not wired in the current local-supported flow. |
| `orchestration/schedules` | A | No active scheduler manifests used by current run commands. |
| `pipelines/bronze_ingest` | A | Bronze logic currently lives in `orchestration/local` and pipeline modules used by run commands. |
| `quality/expectations` | A | Expectations are currently encoded in contract YAMLs and validators. |
| `reports` | A | Runtime-generated output directory; keep untracked. |
| `src/bronze` | A | Empty placeholder; no active module imports from `src/`. |
| `src/common` | A | Empty placeholder; no active module imports from `src/`. |
| `src/gold` | A | Empty placeholder; no active module imports from `src/`. |
| `src/ingestion` | A | Empty placeholder; no active module imports from `src/`. |
| `src/silver` | A | Empty placeholder; no active module imports from `src/`. |

## Enforcement Notes

- Keep runtime artifacts under ignore rules (`.local/`, `lakehouse/`, `metastore_db/`, `spark-warehouse/`, `data/raw/`, `reports/`).
- Add a README only when a directory is intentionally tracked before full implementation.
