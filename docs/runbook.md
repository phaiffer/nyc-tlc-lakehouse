# Runbook: Local Enterprise Pipeline

## Quickstart

```bash
# 1) Create and install venv
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip
./.venv/bin/pip install -r requirements.txt

# 2) Download TLC parquet (January 2024 example)
./.venv/bin/python orchestration/local/run_pipeline.py download --year 2024 --month 1

# 3) Run end-to-end (no --input-parquet needed when default file exists)
./.venv/bin/python orchestration/local/run_pipeline.py run-all --year 2024 --month 1

# 4) Inspect databases/tables in the same local metastore used by the CLI
./.venv/bin/python orchestration/local/run_pipeline.py inspect

# 5) Reset tables
./.venv/bin/python orchestration/local/run_pipeline.py reset
```

## Local Storage Layout

Deterministic local runtime paths are configured by the CLI:

- Spark warehouse: `.local/spark-warehouse`
- Derby metastore: `.local/metastore_db`
- Spark local dir: `.local/spark-local`
- Structured pipeline logs: `.local/logs/pipeline_<run_id>.jsonl`
- Backfill checkpoints: `.local/checkpoints/backfill_<run_id>.json`
- Demo outputs: `.local/demo/demo_<run_id>.txt`

Managed Delta tables:

- `bronze.events_raw`
- `silver.trips_clean`
- `gold.fct_trips_daily`
- `gold.dim_vendor`
- `gold.dim_payment_type`
- `gold.dim_rate_code`
- `quality.quarantine_records`
- `quality.pipeline_metrics`
- `quality.violations_summary`
- `quality.drift_events`
- `quality.drift_baseline_metrics`

## CLI Commands

```bash
# Download source file
./.venv/bin/python orchestration/local/run_pipeline.py download --year 2024 --month 1

# Stage-by-stage runs
./.venv/bin/python orchestration/local/run_pipeline.py run-bronze --year 2024 --month 1
./.venv/bin/python orchestration/local/run_pipeline.py run-silver --year 2024 --month 1
./.venv/bin/python orchestration/local/run_pipeline.py run-gold --year 2024 --month 1
./.venv/bin/python orchestration/local/run_pipeline.py run-quality --strict-quality

# End-to-end
./.venv/bin/python orchestration/local/run_pipeline.py run-all --year 2024 --month 1 --strict-quality

# Month-range backfill (tuple form)
./.venv/bin/python orchestration/local/run_pipeline.py run-backfill \
  --from-year 2024 --from-month 1 \
  --to-year 2024 --to-month 6

# Month-range backfill (YYYY-MM form)
./.venv/bin/python orchestration/local/run_pipeline.py run-backfill \
  --from 2024-01 --to 2024-06

# Force a fresh backfill run without resume
./.venv/bin/python orchestration/local/run_pipeline.py run-backfill \
  --from 2024-01 --to 2024-06 --no-resume

# Inspect local catalog
./.venv/bin/python orchestration/local/run_pipeline.py inspect

# Open a matching local Spark session and inspect catalog
./.venv/bin/python ci/scripts/open_local_spark.py

# Reset (drop tables only)
./.venv/bin/python orchestration/local/run_pipeline.py reset

# Reset with schema drop
./.venv/bin/python orchestration/local/run_pipeline.py reset --drop-schemas

# Purge backfill checkpoints explicitly
./.venv/bin/python orchestration/local/run_pipeline.py reset --purge-checkpoints
```

## Makefile Shortcuts

```bash
make doctor
make verify
make download YEAR=2024 MONTH=1
make run YEAR=2024 MONTH=1
make inspect
make reset
make demo YEAR=2024 MONTH=1
```

## Backfill Resume Behavior

- Checkpoints are created under `.local/checkpoints/backfill_<run_id>.json`.
- `run-backfill` resumes automatically from the first incomplete month when a matching incomplete checkpoint is found.
- Use `--no-resume` to force a new backfill run and checkpoint.
- `reset` does not remove checkpoint files by default.
- Use `reset --purge-checkpoints` only when you intentionally want to remove checkpoint state.

## Troubleshooting

- `FileNotFoundError` for parquet input:
  - run `./.venv/bin/python orchestration/local/run_pipeline.py download --year 2024 --month 1`
  - or locate files manually: `find ~/ -name "yellow_tripdata_2024-01.parquet"`
- SparkUI port warning (`Service 'SparkUI' could not bind on port 4040`): normal in local runs.
- Native Hadoop warning (`Unable to load native-hadoop library`): normal for local Linux usage.
- Metastore mismatch when using a separate `pyspark` shell:
  - use `run_pipeline.py inspect` or `ci/scripts/open_local_spark.py` instead of a raw shell.
- Spark SQL catalog inspection syntax:
  - use `spark.sql("SHOW DATABASES").show()` (plural `DATABASES`)
  - use `spark.sql("SHOW TABLES IN silver").show()`
  - run `make doctor` first to confirm local metastore/warehouse paths.
- Quality table type conflict after schema drift:
  - rerun with `run-quality --reset`; quality tables auto-drop/recreate on compatible drift errors.
- Backfill range errors:
  - `--from` / `--to` must be in `YYYY-MM` format.
  - start month must be earlier than or equal to end month.
- Checkpoint resume confusion:
  - run with `--no-resume` to create a fresh checkpoint.
  - run `reset --purge-checkpoints` only when you want to discard all local backfill progress files.
