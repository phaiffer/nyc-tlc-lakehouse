# lakehouse_dbt (Optional Analytics Engineering Layer)

This directory contains an optional dbt project that sits on top of the local Spark + Delta pipeline outputs.

The core pipeline remains the source of truth for ingestion, contracts, quality enforcement, and managed table writes.
This dbt layer demonstrates analytics engineering modeling patterns without introducing mandatory runtime dependencies for `make check`.

## Intended Adapter Targets

- `dbt-spark` via Spark Thrift Server or Livy: aligns with Spark SQL semantics used by the repository.
- `dbt-databricks` for managed Databricks workspaces: suitable when promoting the portfolio project to a cloud runtime.

Why these adapters:

- Existing pipeline datasets are Spark/Delta-native.
- SQL in this project is written to remain Spark-compatible.

## Mapping to Existing Pipeline Outputs

- Silver managed table `silver.trips_clean` -> dbt `staging` model `stg_silver_trips_clean`
- Gold managed table `gold.fct_trips_daily` -> conceptual source for downstream semantic consumption
- dbt `marts` models -> optional analytics-serving layer for dashboards and BI

## What Is Included

- `dbt_project.yml`
- `models/sources.yml` with Silver and Gold source definitions
- `models/staging` and `models/marts` SQL models
- schema tests (`not_null`, `unique`, `accepted_values`)
- `models/exposures.yml` with `portfolio_dashboard`
- `macros/` for safe casting and standardization

## Local Usage (Optional)

Install a dbt adapter only when you want to run this layer (not required for repository CI):

```bash
cd dbt/lakehouse_dbt
# Example only; choose the adapter for your environment.
pip install dbt-spark

dbt debug
# dbt deps  # only if packages.yml is later introduced
# dbt seed  # only if seeds are added

dbt parse
dbt run
dbt test
dbt docs generate
```

## Notes

- This project is intentionally adapter-agnostic at repository level.
- CI does not execute dbt because adapter/runtime provisioning is environment-specific.
