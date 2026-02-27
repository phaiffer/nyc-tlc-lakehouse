# Drift Audit (Gold Profile)

## Findings

### Root Cause
- `detect_and_record_drift` was designed for row-level trip data and was reused for both Silver and Gold without grain-specific adaptation.
- Gold drift was called with `fare_column="total_fare"`, so the metric labeled `avg_fare_amount` became `AVG(total_fare)` across daily aggregated rows (`trip_date`, `vendor_id`) instead of trip-level average fare.
- Gold table does not contain `trip_distance` or `passenger_count`, so Gold drift produced:
  - `avg_trip_distance = None`
  - `passenger_distribution = {}`

### Why Metrics Looked Wrong
- `total_fare` in Gold is a daily aggregate. Averaging it by row yields a value at the daily aggregate scale (hundreds of thousands), not a per-trip fare KPI.
- Missing columns at Gold grain made distance/distribution metrics non-applicable and effectively empty.

## Chosen Fix Approach

Selected Option A (Gold drift with only applicable KPI signals):
- Keep Silver drift unchanged (volume, avg fare amount, avg trip distance, passenger distribution).
- For Gold, evaluate only:
  - `volume_ratio`
  - `avg_fare_per_trip_ratio` computed as `SUM(total_fare) / SUM(trips)`
- Remove non-applicable Gold metrics from active drift evaluation (`avg_trip_distance_ratio`, `passenger_distribution_l1`).

Implementation notes:
- Added drift metric support for weighted fare profile (`fare_numerator_column` / `fare_denominator_column`).
- Gold drift call now uses:
  - `fare_metric_name="avg_fare_per_trip_ratio"`
  - `fare_profile_key="avg_fare_per_trip"`
  - `enabled_metrics=("volume_ratio", "avg_fare_per_trip_ratio")`
- Gold thresholds in `config/drift_thresholds.yml` now align to active metrics only.

## Risk Assessment

- Low functional risk: Silver path remains unchanged; Gold changes are isolated to drift metric selection and computation.
- Low operational risk: Drift baseline/event schemas are unchanged; metric names are explicit and thresholds are config-driven.
- Medium behavioral risk: Historical Gold drift comparisons that depended on old `avg_fare_amount_ratio` semantics are intentionally replaced by a more meaningful KPI (`avg_fare_per_trip_ratio`).

## Backward Compatibility Impact

- No table deletions or pipeline stage removals.
- Drift events for Gold now use `avg_fare_per_trip_ratio` instead of `avg_fare_amount_ratio`.
- Non-applicable Gold metrics are no longer emitted by design.
- This is backward-compatible for pipeline execution, with intentional metric-semantics correction for observability consumers.
