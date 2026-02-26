-- Top vendor revenue days
SELECT
  f.trip_date,
  f.vendor_id,
  v.vendor_name,
  f.trips,
  f.total_fare
FROM gold.fct_trips_daily AS f
LEFT JOIN gold.dim_vendor AS v
  ON f.vendor_id = v.vendor_id
ORDER BY f.total_fare DESC
LIMIT 20;

-- Monthly rollup from daily fact table
SELECT
  DATE_TRUNC('month', f.trip_date) AS trip_month,
  SUM(f.trips) AS trips,
  SUM(f.total_fare) AS total_fare,
  CASE WHEN SUM(f.trips) = 0 THEN 0 ELSE SUM(f.total_fare) / SUM(f.trips) END AS avg_fare_per_trip
FROM gold.fct_trips_daily AS f
GROUP BY DATE_TRUNC('month', f.trip_date)
ORDER BY trip_month;

-- Quality gate outcomes by severity
SELECT
  severity,
  COUNT(*) AS rules_checked,
  SUM(CASE WHEN passed THEN 0 ELSE 1 END) AS rules_failed
FROM quality.violations_summary
GROUP BY severity
ORDER BY severity;

-- Drift events observed across pipeline runs
SELECT
  dataset,
  metric_name,
  severity,
  run_ts,
  delta_ratio,
  baseline_value,
  observed_value
FROM quality.drift_events
ORDER BY run_ts DESC, severity DESC, dataset, metric_name;
