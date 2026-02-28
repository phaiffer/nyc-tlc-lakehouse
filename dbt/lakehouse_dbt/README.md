# lakehouse_dbt (Optional Analytics Engineering Layer)

This directory contains an optional dbt project that sits on top of the local Spark + Delta pipeline outputs.

The core pipeline remains the source of truth for ingestion, contracts, quality enforcement, and managed table writes.
This dbt layer demonstrates analytics engineering modeling patterns without introducing mandatory runtime dependencies for `make check`.

## Intended Adapter Targets

- `dbt-spark`: default local profile target (`local`) uses Spark `session` mode.
- `dbt-databricks`: optional target path when promoting the project to Databricks.

Why these adapters:

- Existing pipeline datasets are Spark/Delta-native.
- SQL in this project is written to remain Spark-compatible.

## Mapping to Existing Pipeline Outputs

- Silver managed table `silver.trips_clean` -> dbt `staging` model `stg_silver_trips_clean`
- Gold managed table `gold.fct_trips_daily` -> conceptual source for downstream semantic consumption
- dbt `marts` models -> optional analytics-serving layer for dashboards and BI
- `models/marts/mart_daily_revenue.sql` is the trips-daily mart equivalent (`mart_trips_daily`) for this project

## What Is Included

- `dbt_project.yml`
- `profiles/profiles.yml` with repo-local `local` target (no dependency on `~/.dbt`)
- `models/sources.yml` with Silver and Gold source definitions
- `models/staging` and `models/marts` SQL models
- schema tests (`not_null`, `unique`, `accepted_values`)
- `models/exposures.yml` with `portfolio_dashboard`
- `macros/` for safe casting and standardization

## Prerequisites

- Run the core pipeline first so source tables exist:

```bash
make reset && make run YEAR=2024 MONTH=1 && make inspect
```

- Install dbt and an adapter (example local path):

```bash
pip install dbt-core dbt-spark
```

## Local Usage (Optional)

Use repository Make targets (recommended):

```bash
make dbt-parse
make dbt-run
make dbt-test
make dbt-docs
```

Direct `dbt` commands with explicit project/profile dirs:

```bash
dbt parse --project-dir dbt/lakehouse_dbt --profiles-dir dbt/lakehouse_dbt/profiles --target local
dbt run --project-dir dbt/lakehouse_dbt --profiles-dir dbt/lakehouse_dbt/profiles --target local
dbt test --project-dir dbt/lakehouse_dbt --profiles-dir dbt/lakehouse_dbt/profiles --target local
dbt docs generate --project-dir dbt/lakehouse_dbt --profiles-dir dbt/lakehouse_dbt/profiles --target local
```

Override target or profiles directory from Make:

```bash
make dbt-parse DBT_TARGET=local
make dbt-run DBT_PROFILES_DIR=/absolute/path/to/profiles_dir
```

## Built Models and Artifacts

- Staging:
  - `stg_silver_trips_clean` (view)
- Marts:
  - `mart_daily_revenue` (table)
  - `mart_vendor_profile` (view)
- Metadata:
  - tests in `models/staging/staging.yml` and `models/marts/marts.yml`
  - exposure in `models/exposures.yml` (`portfolio_dashboard`)

## Troubleshooting

- `dbt CLI not found`:
  - Install `dbt-core` plus an adapter, for example: `pip install dbt-core dbt-spark`.
- Adapter missing (`Could not find adapter type spark`):
  - Install `dbt-spark` and re-run `make dbt-parse`.
- Profile path errors:
  - Use repo-local profile path: `dbt/lakehouse_dbt/profiles/profiles.yml`.
  - Override with `DBT_PROFILES_DIR=...` if you need a custom profile location.
- Relation/source not found during `dbt run` or `dbt test`:
  - Re-run pipeline first (`make reset && make run YEAR=2024 MONTH=1 && make inspect`).
- `dbt docs generate` failures:
  - Confirm parse and run succeed first for the selected target.

## Notes

- CI does not execute dbt because adapter/runtime provisioning is environment-specific.
- `make check` intentionally does not call any dbt target.
- dbt targets in Make now fail fast with guidance when prerequisites are missing.

## Optional Advanced Usage

If you use a non-local adapter target, update `profiles/profiles.yml` with the adapter-specific connection settings and run with `DBT_TARGET=<target_name>`.

Example:

```bash
make dbt-parse DBT_TARGET=databricks_dev
```
