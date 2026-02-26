#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN="${PYTHON_BIN:-./.venv/bin/python}"
YEAR="${YEAR:-2024}"
MONTH="${MONTH:-1}"
MAX_INVALID_RATIO="${MAX_INVALID_RATIO:-0.001}"
WAREHOUSE_DIR="${WAREHOUSE_DIR:-}"
INPUT_PARQUET="${INPUT_PARQUET:-}"

run_all_args=(--year "$YEAR" --month "$MONTH" --max-invalid-ratio "$MAX_INVALID_RATIO")
inspect_args=()

if [[ -n "$WAREHOUSE_DIR" ]]; then
  run_all_args+=(--warehouse-dir "$WAREHOUSE_DIR")
  inspect_args+=(--warehouse-dir "$WAREHOUSE_DIR")
fi

if [[ -n "$INPUT_PARQUET" ]]; then
  run_all_args+=(--input-parquet "$INPUT_PARQUET")
fi

echo "[local-smoke] reset"
"$PYTHON_BIN" orchestration/local/run_pipeline.py reset "${run_all_args[@]}"

echo "[local-smoke] run-all #1"
"$PYTHON_BIN" orchestration/local/run_pipeline.py run-all "${run_all_args[@]}"

echo "[local-smoke] run-all #2 (rerun)"
"$PYTHON_BIN" orchestration/local/run_pipeline.py run-all "${run_all_args[@]}"

echo "[local-smoke] inspect catalog"
"$PYTHON_BIN" orchestration/local/run_pipeline.py inspect "${inspect_args[@]}"

echo "[local-smoke] validating table existence and positive row counts"
WAREHOUSE_DIR="$WAREHOUSE_DIR" "$PYTHON_BIN" - <<'PY'
from __future__ import annotations

import os

from orchestration.local.run_pipeline import BRONZE_TABLE
from orchestration.local.run_pipeline import GOLD_TABLE
from orchestration.local.run_pipeline import METRICS_TABLE
from orchestration.local.run_pipeline import QUARANTINE_TABLE
from orchestration.local.run_pipeline import SILVER_TABLE
from orchestration.local.run_pipeline import VIOLATIONS_TABLE
from orchestration.local.run_pipeline import _build_spark_session
from orchestration.local.run_pipeline import _resolve_spark_paths

warehouse_dir = os.environ.get("WAREHOUSE_DIR") or None
paths = _resolve_spark_paths(warehouse_dir=warehouse_dir)
spark = _build_spark_session(paths=paths)

required_positive_tables = [
    BRONZE_TABLE,
    SILVER_TABLE,
    GOLD_TABLE,
    METRICS_TABLE,
    VIOLATIONS_TABLE,
]

try:
    for table_name in required_positive_tables:
        if not spark.catalog.tableExists(table_name):
            raise RuntimeError(f"Required table not found: {table_name}")
        row_count = spark.table(table_name).count()
        if row_count <= 0:
            raise RuntimeError(f"Required table has no rows: {table_name}")
        print(f"[local-smoke] {table_name}: rows={row_count}")

    if spark.catalog.tableExists(QUARANTINE_TABLE):
        quarantine_count = spark.table(QUARANTINE_TABLE).count()
        print(f"[local-smoke] {QUARANTINE_TABLE}: rows={quarantine_count}")
    else:
        print(f"[local-smoke] {QUARANTINE_TABLE}: table not created (no quarantined rows)")
finally:
    spark.stop()

print("[local-smoke] verification passed")
PY

echo "[local-smoke] completed"
