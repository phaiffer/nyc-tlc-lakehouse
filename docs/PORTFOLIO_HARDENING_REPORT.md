# Portfolio Hardening Report

Date: 2026-02-28
Repository: `nyc-tlc-lakehouse`
Mode: Enterprise portfolio hardening

## Phase 0 Inventory (Tree Summary)

```text
./
├── .github/workflows
├── analytics
├── ci/{schemas,scripts}
├── config
├── contracts/{bronze,silver,gold}
├── data/raw                    # ignored local downloads
├── dbt/lakehouse_dbt
├── docs/{adr,...}
├── orchestration/local
├── pipelines/{common,silver_transform,gold_marts}
├── quality/{validators,observability,reconciliation}
├── scripts
├── tests/{unit,integration,contract_regression}
├── notebooks                   # includes optional exploration notebooks
└── runtime-local (ignored): .local/, lakehouse/, metastore_db/, spark-warehouse/
```

## Empty Directories and Skeleton Signals

Observed in workspace during this pass (excluding `.git/`, `.venv/`, caches):

- `metastore_db/tmp` (runtime/local artifact path)
- `spark-warehouse` (runtime/local artifact path)

Tracked repository scope:

- No tracked empty directories were found.

Skeleton-only signals:

- `config/.gitkeep` is no longer needed because `config/` has active config files.

## Phase 1 Findings (Paths and Actions)

- `metastore_db/tmp` -> keep (runtime-only empty directory under ignored metastore path).
- `spark-warehouse` -> keep untracked, explicitly ignored in `.gitignore` as a local Spark artifact.
- `.gitkeep` placeholders -> none found; no placeholder-only tracked directories detected.
- `docs/REQUIREMENTS_BACKLOG.md` and `docs/IMPROVEMENTS_BACKLOG.md` -> keep both; clarify scopes in the [Documentation Index](README.md) as release-scope backlog vs post-release roadmap.
- `dbt/lakehouse_dbt/models/marts/mart_daily_revenue.sql` -> keep as trips-daily mart equivalent (`mart_trips_daily`) and document this mapping in dbt README.

## Local Artifact Directories That Must Stay Untracked

- `.local/` (embedded metastore, local Spark temp, local managed-table warehouse)
- `lakehouse/` (filesystem Delta snapshots for local experimentation)
- `metastore_db/` (Derby metastore state)
- `spark-warehouse/` and `notebooks/spark-warehouse/` (Spark warehouse outputs)
- `data/raw/` (downloaded parquet inputs)
- `reports/` (generated outputs)
- `derby.log`

## Current State Summary (What Works End-to-End)

- Local orchestration supports full flow: `make setup`, `make download`, `make reset`, `make run`, `make inspect`.
- `make check` covers format, lint, and tests (`fmt-check`, `lint`, `test`).
- Lightweight quality extras exist: `make docs-check` (markdown links) and `make compile` (fast compile sanity).
- Data contracts and quality governance are active (quarantine, violations summary, drift metrics).
- Optional dbt analytics engineering layer exists with sources/models/tests/exposure/macros.
- ADR set documents metastore topology, contract/quality gate policy, and drift metrics by grain.

## Gaps, Risks, and Confusing Optics

- dbt execution remains intentionally out of CI because adapter/runtime provisioning is environment-specific.
- Two backlog documents remain by design (`REQUIREMENTS_BACKLOG`, `IMPROVEMENTS_BACKLOG`) and require ongoing scope discipline to avoid overlap.
- Local Spark/Hive runtime artifacts can accumulate quickly; cleanup guidance must stay explicit in root README.

## Prioritized Backlog

### P0 (Implement Now)

- Refresh hardening report with current-state inventory, risks, and backlog.
- Refresh empty-directory policy based on actual current directory state.
- Align ignore rules for all known local Spark artifacts (`spark-warehouse/`).
- Remove obsolete placeholder marker files where directories are already active.

### P1 (Implemented, Keep Improving)

- Keep optional dbt layer in place as portfolio analytics engineering evidence.
- Expand dbt model/test coverage as new marts are added.

### P2 (Roadmap)

- Add optional CI stage for `dbt parse`/`dbt test` once adapter/runtime strategy is standardized.
- Add ownership metadata (`CODEOWNERS` and docs ownership matrix) if team scope expands.
- Consider lightweight markdown lint style checks in CI if doc volume grows.

## Scope Commitment for This Pass

### Implementing Now

- Phase 0 inventory and requirements refresh report.
- Empty-directory policy refresh.
- Repo hygiene cleanup for local artifact ignore paths.
- Validation runs and final report update.

### Remaining as Roadmap

- dbt execution in CI with adapter-specific infrastructure.
- Expanded repository governance metadata (owners/escalation matrices).

## Validation Commands (Phase 5)

```bash
make check
make reset && make run YEAR=2024 MONTH=1 && make inspect
```

## Implementation Log (Completed)

Implemented in this pass:

- Added Mermaid architecture visibility in root `README.md` for Medallion + contracts + quality + drift + optional dbt.
- Added troubleshooting link coverage in docs index and clarified backlog-doc scopes.
- Added optional dbt Make targets (`dbt-parse`, `dbt-run`, `dbt-test`, `dbt-docs`) that no-op when dbt CLI is absent.
- Added Phase 1 findings list with concrete path/action decisions for empty dirs and roadmap optics.
- Re-ran full validation and captured fresh evidence below.

## Validation Results

Execution date: 2026-02-28 (latest refresh)

`make check`:

- `ruff format --check`: passed (`30 files already formatted`)
- `ruff check`: passed (`All checks passed!`)
- `pytest -q`: passed (`13 passed`)
- Optional dbt target probe: `make dbt-parse` passed with expected no-op (`dbt CLI not found; skipping optional target`)

`make reset && make run YEAR=2024 MONTH=1 && make inspect`:

- Reset dropped managed Bronze/Silver/Gold/Quality tables successfully.
- Full run completed with expected local Spark/Hive warnings only (SerDe/native Hadoop/loopback warnings).
- Key pipeline outputs:
  - Bronze rows: `2,964,606`
  - Silver rows: `2,926,167`
  - Gold rows: `85`
  - Quality summary rows: `6`
  - Drift events emitted: `0` (Silver), `0` (Gold)
- Inspect confirmed expected databases and tables:
  - Databases: `bronze`, `silver`, `gold`, `quality`, `default`
  - Core tables: `bronze.events_raw`, `silver.trips_clean`, `gold.fct_trips_daily`, `gold.dim_vendor`, `gold.dim_payment_type`, `gold.dim_rate_code`, `quality.pipeline_metrics`, `quality.violations_summary`, `quality.drift_events`, `quality.drift_baseline_metrics`.
