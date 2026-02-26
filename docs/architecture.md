# Architecture

## Runtime Context

Local-first execution with:

- Spark SQL + Delta catalog extensions
- Embedded Hive metastore (Derby) for managed table metadata
- Warehouse path under `.local/spark-warehouse`

The orchestration entrypoint is:

- `orchestration/local/run_pipeline.py`

## Data Layers

### Bronze

- Table: `bronze.events_raw`
- Input: NYC TLC monthly parquet
- Guarantees:
  - deterministic type normalization (`timestamp_ntz -> timestamp`, stable numeric casts)
  - deterministic month-window filtering
  - controlled overwrite with recreate support to avoid metastore schema drift

### Silver

- Table: `silver.trips_clean`
- Transform: canonical trip model + deterministic `trip_id`
- Governance:
  - contract enforcement (schema + expectations)
  - quarantine writes with rule metadata
  - incremental merge (`primary_key=trip_id`, `watermark=updated_at`)

### Gold

- Fact: `gold.fct_trips_daily`
- Dimensions:
  - `gold.dim_vendor`
  - `gold.dim_payment_type`
  - `gold.dim_rate_code`
- Governance:
  - contract enforcement
  - incremental merge (`primary_key=[trip_date, vendor_id]`, `watermark=trip_date`)

## Quality and Observability

- `quality.pipeline_metrics`: run metrics payloads (`run_ts` timestamp)
- `quality.quarantine_records`: row-level invalid records with rule metadata
- `quality.violations_summary`: severity-aware quality gate summary
- `quality.drift_events`: threshold-based drift events
- `quality.drift_baseline_metrics`: baseline profiles used for drift comparisons

## Governance Controls

- Contract definitions in `contracts/{bronze,silver,gold}/*.yml`
- CI validation:
  - contract schema and semantic checks
  - breaking-change detection requires version bump
- Schema evolution safety:
  - `mergeSchema=false`
  - Delta auto-merge disabled for merges
  - controlled drop/recreate fallback for managed quality tables
