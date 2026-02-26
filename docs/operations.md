# Operations Runbook

## Standard Local Run

```bash
make venv
make download YEAR=2024 MONTH=1
make run-all YEAR=2024 MONTH=1
make inspect
```

## Release-Gate Validation Run

```bash
python orchestration/local/run_pipeline.py reset
python orchestration/local/run_pipeline.py run-all --year 2024 --month 1
python orchestration/local/run_pipeline.py run-all --year 2024 --month 1
python orchestration/local/run_pipeline.py inspect
```

## Deterministic Rerun Expectations

- No Hive alter-table incompatibility errors on rerun
- Stable schemas across Bronze/Silver/Gold/Quality tables
- Incremental merge keeps counts stable for repeated identical windows

## Troubleshooting

### Missing parquet input

- `python orchestration/local/run_pipeline.py download --year 2024 --month 1`
- or run with explicit path:
  - `python orchestration/local/run_pipeline.py run-all --input-parquet <path> --year 2024 --month 1`

### Expected local warnings

Warnings about Hive SerDe mapping for Delta tables are expected in this local topology:

- `Couldn't find corresponding Hive SerDe for data source provider delta`

### Metastore startup issues

The CLI retries once after recreating `.local/metastore_db` if Derby metastore startup fails.

### Schema mismatch in quality tables

Quality tables use controlled recreate behavior on schema conflict to keep reruns stable.

## Useful Commands

```bash
make fmt
make lint
make test
make smoke
make run-local YEAR=2024 MONTH=1
```
