# NYC TLC Lakehouse (Spark + Delta + Local Hive Metastore)

Enterprise-style local Lakehouse project for portfolio use, built on:

- PySpark 3.5 + Delta Lake
- Embedded Hive metastore (Derby) for local catalog persistence
- Contract-driven Bronze/Silver/Gold pipelines
- Quality gates, quarantine, observability metrics, and drift detection

## Architecture Overview

Data flow:

1. `bronze.events_raw`: deterministic raw ingestion from TLC parquet
2. `silver.trips_clean`: canonical trip model + contract enforcement + incremental merge
3. `gold.fct_trips_daily`: business KPIs (daily grain) + incremental merge
4. Quality & observability:
   - `quality.quarantine_records`
   - `quality.violations_summary`
   - `quality.pipeline_metrics`
   - `quality.drift_events`
   - `quality.drift_baseline_metrics`
5. Semantic dimensions:
   - `gold.dim_vendor`
   - `gold.dim_payment_type`
   - `gold.dim_rate_code`

## Quickstart (Make Entrypoint)

```bash
make setup
make download YEAR=2024 MONTH=1
make run YEAR=2024 MONTH=1
make inspect
```

Core developer targets:

```bash
make fmt
make fmt-check
make lint
make test
make check
make reset
make smoke
make run YEAR=2024 MONTH=1
make inspect
```

## Local Execution (Release-Gate Path)

```bash
python orchestration/local/run_pipeline.py reset
python orchestration/local/run_pipeline.py run-all --year 2024 --month 1
python orchestration/local/run_pipeline.py run-all --year 2024 --month 1
python orchestration/local/run_pipeline.py inspect
```

## Expected WARN Messages (Local Hive + Delta)

With Spark + Delta + embedded Hive metastore, warnings like this are expected:

- `Couldn't find corresponding Hive SerDe for data source provider delta`

These are informational in local mode. They do not indicate pipeline failure. Actual failures are
treated as errors (for example: incompatible schema/alter-table exceptions).

## Incremental Strategy (Watermark + Late Arrivals)

- Silver contract:
  - watermark: `updated_at`
  - late-arrival window: 7 days
- Gold contract:
  - watermark: `trip_date`
  - late-arrival window: 7 days

Merge behavior:

- deterministic source dedup by PK + watermark + stable tie-break hash
- implicit schema evolution disabled (`mergeSchema=false`, `autoMerge=false`)
- explicit schema mismatch handling with controlled table recreate when needed

## Data Contracts

Contracts are defined in `contracts/{bronze,silver,gold}` and validated in CI.

- `version` is mandatory and used for schema governance.
- Breaking changes require version bump (enforced by `ci/scripts/detect_contract_breaking_changes.py`).
- Silver/Gold contracts define `primary_key`, `watermark`, and `late_arrival_days`.
- Expectation rules support severity (`error`, `warn`, `info`) for quality behavior.

## Quality Gates

Quality gates write severity-aware rule summaries to `quality.violations_summary`.

- `severity=error`: can fail strict gate
- `severity=warn/info`: recorded but does not fail strict gate

Quarantine records include:

- `reason_code`, `rule_id`, `rule_name`, `severity`, `run_id`, `run_ts`

Drift detection is configurable at:

- `config/drift_thresholds.yml`

## Example Spark SQL Queries

```sql
-- Bronze: monthly volume by source file
SELECT source_file, COUNT(*) AS rows
FROM bronze.events_raw
GROUP BY source_file
ORDER BY rows DESC;

-- Silver: daily clean trip volume
SELECT pickup_date, COUNT(*) AS trips
FROM silver.trips_clean
GROUP BY pickup_date
ORDER BY pickup_date;

-- Gold: top revenue days
SELECT trip_date, vendor_id, trips, total_fare
FROM gold.fct_trips_daily
ORDER BY total_fare DESC
LIMIT 20;

-- Quality: failed rules by severity
SELECT severity, rule_name, failed_count, run_ts
FROM quality.violations_summary
WHERE passed = false
ORDER BY run_ts DESC, severity;
```

## Documentation

- [Architecture](docs/architecture.md)
- [Operations](docs/operations.md)
- [Data Quality](docs/data_quality.md)
- [Semantic Model](docs/semantic_model.md)
- [Contracts](docs/contracts.md)
- [Incremental](docs/incremental.md)

ADRs:

- [ADR-0001 Embedded Hive Metastore Constraints](docs/adr/0001-embedded-hive-metastore-constraints.md)
- [ADR-0002 Contract-Driven Schema Governance](docs/adr/0002-contract-driven-schema-governance.md)
- [ADR-0003 Incremental Merge and Reconciliation](docs/adr/0003-incremental-merge-reconciliation.md)
