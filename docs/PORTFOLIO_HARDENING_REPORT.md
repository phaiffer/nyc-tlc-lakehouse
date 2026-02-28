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

- Hardening report and empty-directory policy documents were stale and not aligned with current repository state.
- `spark-warehouse/` existed locally but was not explicitly ignored in `.gitignore`, which could create accidental tracking noise.
- `config/.gitkeep` remained from a prior skeleton phase and can read as unfinished scaffolding.
- dbt execution remains intentionally out of CI because adapter/runtime provisioning is environment-specific.

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

## Implementation Log (In Progress)

- Updated report to reflect current state and concrete backlog.
- Pending: run validation commands and append execution summary.
