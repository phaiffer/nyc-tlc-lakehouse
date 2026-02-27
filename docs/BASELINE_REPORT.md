# Baseline Report (Phase 0)

Run date: 2026-02-27  
Environment: local Linux, Python virtual environment, Spark + Delta + embedded Hive metastore.

## Commands Executed

1. `ruff format --check .`
2. `ruff check .`
3. `pytest -q`
4. `python orchestration/local/run_pipeline.py reset`
5. `python orchestration/local/run_pipeline.py run-all --year 2024 --month 1`
6. `python orchestration/local/run_pipeline.py inspect`

## Results Summary

- `ruff format --check .`: passed (`25 files already formatted`)
- `ruff check .`: passed (`All checks passed!`)
- `pytest -q`: passed (exit code `0`)
- `reset`: passed and dropped managed Bronze/Silver/Gold/Quality tables
- `run-all --year 2024 --month 1`: passed end-to-end
  - Bronze rows: `2964606`
  - Silver rows: `2926167`
  - Gold rows: `85`
  - Quality rows (`quality.violations_summary`): `4`
- `inspect`: passed, catalog includes `bronze`, `silver`, `gold`, `quality` schemas and expected managed tables

## Observations

- Expected local warnings were observed and are informational in this setup:
  - Delta/Hive SerDe compatibility warnings
  - native Hadoop library warning
  - hostname loopback warning
- No harmful stack traces or fatal metastore errors were observed in the baseline run.
