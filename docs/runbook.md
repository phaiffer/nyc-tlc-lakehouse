# Runbook: Local Enterprise Pipeline

## Prerequisites

- Linux environment
- Java 8+ (required by Spark)
- Python 3.11+ (project uses `.venv`)
- Dependencies installed:
  - `python3 -m venv .venv`
  - `./.venv/bin/pip install -r requirements.txt`

## Local Storage Layout

Deterministic local runtime paths are configured in `orchestration/local/run_pipeline.py`:

- Spark warehouse: `.local/spark-warehouse`
- Derby metastore: `.local/metastore_db`
- Spark local dir: `.local/spark-local`

Managed Delta tables are created automatically under warehouse namespaces:

- `bronze.events_raw`
- `silver.trips_clean`
- `gold.fct_trips_daily`
- `quality.quarantine_records`
- `quality.violations_summary`
- `quality.pipeline_metrics`

## Pipeline Commands (CLI)

All commands are idempotent and safe to re-run.

```bash
python orchestration/local/run_pipeline.py run-bronze \
  --input-parquet data/raw/yellow_tripdata_2024-01.parquet \
  --year 2024 --month 1

python orchestration/local/run_pipeline.py run-silver --year 2024 --month 1

python orchestration/local/run_pipeline.py run-gold --year 2024 --month 1

python orchestration/local/run_pipeline.py run-quality --strict-quality

python orchestration/local/run_pipeline.py run-all \
  --input-parquet data/raw/yellow_tripdata_2024-01.parquet \
  --year 2024 --month 1 \
  --strict-quality
```

Optional controls:

- `--max-invalid-ratio 0.001` (default)
- `--warehouse-dir <path>`
- `--reset` (drop stage tables before running command)

## Makefile Shortcuts

```bash
make lint
make contracts
make smoke

make bronze INPUT_PARQUET=data/raw/yellow_tripdata_2024-01.parquet YEAR=2024 MONTH=1
make silver YEAR=2024 MONTH=1
make gold YEAR=2024 MONTH=1
make quality STRICT_QUALITY=1

make run-all INPUT_PARQUET=data/raw/yellow_tripdata_2024-01.parquet YEAR=2024 MONTH=1 STRICT_QUALITY=1
```

## Reset and Cleanup

Drop all pipeline tables:

```bash
python orchestration/local/run_pipeline.py reset
# or
make reset
```

Drop quality tables only and clean quality warehouse dirs:

```bash
python orchestration/local/run_pipeline.py clean --remove-quality-warehouse
# or
make clean
```

## Troubleshooting

- `ModuleNotFoundError: delta`:
  - install dependencies into `.venv` and run commands with `./.venv/bin/python`.
- Spark cannot create warehouse/metastore:
  - ensure write permissions for `.local/`.
- `Missing required columns in Bronze input`:
  - verify the parquet matches NYC TLC Yellow Taxi schema.
- `Invalid ratio exceeded` / `Quality gate failed`:
  - inspect `quality.quarantine_records` and `quality.violations_summary` for failed rules.
- Contract/type mismatch errors:
  - compare produced schema with `contracts/*/*.yml` and run `python ci/scripts/validate_contracts.py`.
