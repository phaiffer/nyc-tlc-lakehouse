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
├── data/raw
├── dbt/lakehouse_dbt
├── docs/adr
├── notebooks
├── orchestration/local
├── pipelines/{common,silver_transform,gold_marts}
├── quality/{validators,observability,reconciliation}
├── scripts
├── tests/{unit,integration,contract_regression}
└── runtime-local (ignored): .local, lakehouse, metastore_db, spark-warehouse
```

## Empty Directories and Skeleton Signals

Empty directories detected in workspace at the start of this hardening pass:

- `.ipynb_checkpoints`
- `ci/github-actions`
- `ci/test-data`
- `dbt/lakehouse_dbt`
- `great_expectations`
- `orchestration/airflow`
- `orchestration/schedules`
- `pipelines/bronze_ingest`
- `quality/expectations`
- `reports`
- `src/bronze`
- `src/common`
- `src/gold`
- `src/ingestion`
- `src/silver`

Initial policy decision:

- `dbt/lakehouse_dbt`: implement as optional but real dbt analytics engineering layer (P1 high-value).
- Runtime-only / local-only folders: keep untracked and explicitly documented as generated artifacts.
- Placeholder directories with no immediate runtime value: either remove from workspace scope or document ownership/purpose through concise README notes where keeping the folder helps future contributors.

## Local Artifact Directories That Must Stay Untracked

- `.local/` (embedded metastore, spark local temp, local warehouse)
- `lakehouse/` (filesystem output snapshots)
- `metastore_db/` (Derby metastore state)
- `spark-warehouse/` and `notebooks/spark-warehouse/`
- `data/raw/` (downloaded raw parquet)
- `reports/` (generated outputs)
- `derby.log`

Status at inventory time:

- These paths exist locally and are already configured to be ignored in `.gitignore`.
- Optics/documentation still need a clearer explanation of lifecycle and cleanup commands.

## Current State Summary (What Works End-to-End)

- Local orchestration supports full flow: `setup`, `download`, `reset`, `run`, `inspect`.
- Quality gates are integrated in code and CI (`contracts`, schema checks, drift events, quarantine, metrics).
- `make check` baseline execution passes (`fmt-check`, `lint`, `pytest`).
- ADR baseline already exists for metastore constraints, contract governance, and incremental merge.

## Gaps, Risks, and Portfolio Optics Issues

- No `docs/README.md` index; discoverability is weaker than expected for portfolio review.
- Root `README.md` does not present a single explicit supported run flow including `check` and cleanup in one place.
- Empty/skeleton directories create unclear ownership and can look unfinished.
- `dbt/lakehouse_dbt` is fully ignored and empty; this hides an opportunity for analytics engineering demonstration.
- ADR set does not explicitly document Silver-vs-Gold drift grain decision in a dedicated record.
- Makefile lacks a fast compile sanity target and a docs-focused check target.

## Prioritized Backlog

### P0 (Implement Now)

- Add `docs/README.md` documentation index with stable relative links.
- Tighten root `README.md` supported local flow, expected warnings, outputs location, and cleanup steps.
- Add ADRs explicitly covering:
  - local Hive metastore + Delta tables,
  - contracts + quality gate,
  - drift metrics by grain (Silver vs Gold).
- Establish empty-directory handling policy (remove or document purpose).
- Add lightweight Makefile quality targets (`docs-check` if lightweight, compile sanity).

### P1 (Implement Now if lightweight and no heavy infra)

- Create a real optional dbt project in `dbt/lakehouse_dbt`:
  - models (`staging`, `marts`), sources, tests, exposures, and helper macros,
  - clear README describing adapter expectations and mapping from Silver/Gold outputs.

### P2 (Roadmap)

- CI job for dbt parsing/tests once a stable local adapter strategy is selected.
- Optional markdown lint standardization across docs.
- Ownership metadata expansion (`CODEOWNERS`, docs owners matrix) if repository governance broadens.

## Scope Commitment for This Pass

### Will Implement Now

- Phase 1 docs optics and ADR additions.
- Phase 2 empty-directory policy with concrete handling for `dbt/lakehouse_dbt`.
- Phase 3 optional dbt layer as a real, documented skeleton (no fake CI execution).
- Phase 4 Makefile hardening for docs-check and compile sanity.
- Phase 5 validation run results and final update of this report.

### Will Remain Roadmap

- Full dbt execution in CI tied to live Spark/Databricks adapter infrastructure.
- Broader governance expansion beyond lightweight portfolio-focused hardening.

## Validation Commands (To Execute in Phase 5)

```bash
make check
make reset && make run YEAR=2024 MONTH=1 && make inspect
```

## Implementation Log (To Update in Phase 5)

- Pending implementation.
