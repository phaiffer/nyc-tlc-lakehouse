# Validation Report

Run date: 2026-02-27  
Mode: Enterprise Data Platform Release Gate + Hardening

## 1) Commands Executed

### Mandatory validation

1. `make check`
2. `make reset && make run YEAR=2024 MONTH=1 && make inspect`

### Targeted validation

3. `make doctor`
4. `pytest -q tests/integration/test_pipeline_smoke_wiring.py tests/unit/test_drift_detector_gold_profile.py tests/unit/test_quality_gate_gold_sanity.py`

## 2) Results

- `make check`: **PASS**
  - `ruff format --check`: pass
  - `ruff check`: pass
  - `pytest -q`: pass
- `make reset && make run YEAR=2024 MONTH=1 && make inspect`: **PASS**
  - Bronze rows: `2964606`
  - Silver rows: `2926167`
  - Gold rows: `85`
  - Quality rows (`quality.violations_summary`): `6`
  - Expected tables present in inspect output:
    - `bronze.events_raw`
    - `silver.trips_clean`
    - `gold.fct_trips_daily`
    - `gold.dim_vendor`, `gold.dim_payment_type`, `gold.dim_rate_code`
    - `quality.pipeline_metrics`, `quality.violations_summary`
    - `quality.drift_baseline_metrics`, `quality.drift_events`
- `make doctor`: **PASS**
  - Prints Python/Java/runtime variables and local Spark path diagnostics.
- Targeted strict/drift tests: **PASS**
  - strict smoke wiring test
  - Gold drift profile sanity test
  - Gold sanity quality rules strict/warn behavior tests

No blocking failures were observed.

## 3) Functional Validation Findings

### Schema stability and rerun safety

- Deterministic Bronze normalization is active and logged (`timestamp_ntz` and fixed numeric casts).
- Managed table recreation logic for schema conflicts remains intact.
- No Hive metastore drift failure observed during reset/run/rerun path.

### Incremental merge and watermark semantics

- Merge logic validates schema compatibility, deduplicates deterministically, and enforces late-arrival lower-bound windowing.
- Integration tests confirm idempotency and late-arrival filtering behavior.

### Quality gate strict vs warn behavior

- Strict mode behavior is validated by unit tests:
  - error-severity failures raise and fail strict mode
  - warn-severity failures are recorded and do not fail strict mode
- Gold KPI sanity checks are now covered (volume and average fare per trip bounds).

### Drift profile grain correctness (Silver vs Gold)

- Silver profile metrics remain row-level and complete.
- Gold profile now uses grain-appropriate metrics:
  - `volume_ratio`
  - `avg_fare_per_trip_ratio`
- Gold drift no longer relies on non-existent Gold columns (`trip_distance`, `passenger_count`).

## 4) CI Workflow Validation

- `.github/workflows/ci-contracts.yml` triggers on both `push` and `pull_request`.
- Workflow includes:
  - Ruff format check
  - Ruff lint
  - Pytest suite
  - strict-mode pipeline smoke test path
- Runtime guardrails remain reasonable (`timeout-minutes: 30`).
- Manual full pipeline workflow aligned to Python `3.12`.

## 5) Hardening Changes Applied In This Validation Pass

- Added `make doctor` and `make verify` targets for reproducible environment diagnostics + local quality gate checks.
- Upgraded docs:
  - architecture text diagram
  - runbook updates with doctor/verify usage
  - troubleshooting additions
- Aligned manual workflow Python version to `3.12`.

## 6) Expected Local Warnings

Observed warnings about Hive SerDe/embedded metastore/native-hadoop remain expected in this local topology and are non-fatal in this validation run.
