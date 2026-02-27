from __future__ import annotations

from datetime import datetime

import pytest

from quality.validators.quality_gate import run_quality_gate


def _create_sanity_input_tables(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS unit_quality")

    silver_df = spark.sql(
        """
        SELECT
          'trip_1' AS trip_id,
          CAST('2024-01-01' AS DATE) AS pickup_date,
          CAST('2024-01-01 10:00:00' AS TIMESTAMP) AS pickup_ts,
          CAST('2024-01-01 10:15:00' AS TIMESTAMP) AS dropoff_ts,
          CAST(20.0 AS DECIMAL(18,2)) AS fare_amount,
          CAST(3.5 AS DOUBLE) AS trip_distance
        """
    )
    silver_df.write.mode("overwrite").saveAsTable("unit_quality.silver_gold_sanity_input")

    gold_df = spark.sql(
        """
        SELECT
          CAST('2024-01-01' AS DATE) AS trip_date,
          '1' AS vendor_id,
          CAST(1 AS BIGINT) AS trips,
          CAST(1000000.0 AS DECIMAL(18,2)) AS total_fare
        """
    )
    gold_df.write.mode("overwrite").saveAsTable("unit_quality.gold_sanity_input")


def test_gold_sanity_rule_fails_in_strict_mode_when_error(spark) -> None:
    _create_sanity_input_tables(spark)

    with pytest.raises(ValueError, match="Quality gate failed"):
        run_quality_gate(
            spark,
            silver_table="unit_quality.silver_gold_sanity_input",
            gold_table="unit_quality.gold_sanity_input",
            output_table="unit_quality.violations_gold_sanity_error",
            thresholds={
                "max_invalid_ratio": 0.0,
                "gold_min_avg_fare_per_trip": 1.0,
                "gold_max_avg_fare_per_trip": 500.0,
            },
            strict=True,
            dataset="silver.trips_clean",
            run_id="test-run-gold-sanity-error",
            run_ts=datetime(2024, 1, 1, 0, 0, 0),
            window_year=2024,
            window_month=1,
            contract_version=2,
        )


def test_gold_sanity_warn_rule_does_not_fail_strict_mode(spark) -> None:
    _create_sanity_input_tables(spark)

    result = run_quality_gate(
        spark,
        silver_table="unit_quality.silver_gold_sanity_input",
        gold_table="unit_quality.gold_sanity_input",
        output_table="unit_quality.violations_gold_sanity_warn",
        thresholds={
            "max_invalid_ratio": 0.0,
            "gold_min_avg_fare_per_trip": 1.0,
            "gold_max_avg_fare_per_trip": 500.0,
        },
        strict=True,
        dataset="silver.trips_clean",
        run_id="test-run-gold-sanity-warn",
        run_ts=datetime(2024, 1, 1, 0, 0, 0),
        window_year=2024,
        window_month=1,
        contract_version=2,
        rule_configs={
            "gold_avg_fare_per_trip_out_of_range": {"rule_id": "QG006", "severity": "warn"},
        },
    )

    assert "gold_avg_fare_per_trip_out_of_range" in ";".join(result["failed_rules"])
    assert result["failed_error_rules"] == []
