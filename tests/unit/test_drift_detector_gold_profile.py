from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from quality.observability.drift_detector import detect_and_record_drift


def test_gold_drift_profile_uses_avg_fare_per_trip_metric(spark, tmp_path) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS quality")
    baseline_table = "quality.drift_baseline_metrics_gold_profile"
    drift_events_table = "quality.drift_events_gold_profile"
    spark.sql(f"DROP TABLE IF EXISTS {baseline_table}")
    spark.sql(f"DROP TABLE IF EXISTS {drift_events_table}")

    config_path = Path(tmp_path) / "drift_thresholds.yml"
    config_path.write_text(
        """
datasets:
  gold.fct_trips_daily:
    volume_ratio:
      warn: 0.01
      error: 0.02
    avg_fare_per_trip_ratio:
      warn: 0.01
      error: 0.02
""".strip(),
        encoding="utf-8",
    )

    first_df = spark.createDataFrame(
        [
            ("2024-01-01", "1", 100, 2000.0),
            ("2024-01-01", "2", 80, 1680.0),
        ],
        schema=["trip_date", "vendor_id", "trips", "total_fare"],
    ).withColumn("trip_date", F.to_date(F.col("trip_date")))

    first_summary = detect_and_record_drift(
        spark,
        source_df=first_df,
        dataset="gold.fct_trips_daily",
        run_id="gold-drift-1",
        run_ts=datetime(2024, 1, 1, 0, 0, 0),
        window_year=2024,
        window_month=1,
        config_path=config_path,
        drift_events_table=drift_events_table,
        baseline_table=baseline_table,
        fare_metric_name="avg_fare_per_trip_ratio",
        fare_profile_key="avg_fare_per_trip",
        fare_numerator_column="total_fare",
        fare_denominator_column="trips",
        enabled_metrics=("volume_ratio", "avg_fare_per_trip_ratio"),
    )

    assert set(first_summary["current_profile"].keys()) == {"volume", "avg_fare_per_trip"}
    assert first_summary["current_profile"]["avg_fare_per_trip"] == pytest.approx(
        20.444444, rel=1e-6
    )
    assert first_summary["current_profile"]["avg_fare_per_trip"] < 100.0

    second_df = spark.createDataFrame(
        [
            ("2024-01-02", "1", 120, 2520.0),
            ("2024-01-02", "2", 90, 1980.0),
            ("2024-01-02", "3", 60, 1200.0),
        ],
        schema=["trip_date", "vendor_id", "trips", "total_fare"],
    ).withColumn("trip_date", F.to_date(F.col("trip_date")))

    second_summary = detect_and_record_drift(
        spark,
        source_df=second_df,
        dataset="gold.fct_trips_daily",
        run_id="gold-drift-2",
        run_ts=datetime(2024, 1, 2, 0, 0, 0),
        window_year=2024,
        window_month=1,
        config_path=config_path,
        drift_events_table=drift_events_table,
        baseline_table=baseline_table,
        fare_metric_name="avg_fare_per_trip_ratio",
        fare_profile_key="avg_fare_per_trip",
        fare_numerator_column="total_fare",
        fare_denominator_column="trips",
        enabled_metrics=("volume_ratio", "avg_fare_per_trip_ratio"),
    )

    assert set(second_summary["current_profile"].keys()) == {"volume", "avg_fare_per_trip"}
    assert second_summary["current_profile"]["avg_fare_per_trip"] == pytest.approx(
        21.111111, rel=1e-6
    )
    assert second_summary["current_profile"]["avg_fare_per_trip"] < 100.0

    metric_names = {
        row["metric_name"]
        for row in spark.table(drift_events_table).select("metric_name").collect()
    }
    assert "avg_trip_distance_ratio" not in metric_names
    assert "passenger_distribution_l1" not in metric_names
    assert {"baseline_bootstrap", "volume_ratio", "avg_fare_per_trip_ratio"} <= metric_names
