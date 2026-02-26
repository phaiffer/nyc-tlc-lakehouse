from __future__ import annotations

from datetime import datetime

import pytest

from quality.validators.quality_gate import run_quality_gate


def _create_quality_input_tables(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS unit_quality")

    silver_df = spark.sql(
        """
        SELECT
          'trip_1' AS trip_id,
          CAST('2024-01-01' AS DATE) AS pickup_date,
          CAST('2024-01-01 10:00:00' AS TIMESTAMP) AS pickup_ts,
          CAST('2024-01-01 09:59:00' AS TIMESTAMP) AS dropoff_ts,
          CAST(12.5 AS DECIMAL(18,2)) AS fare_amount,
          CAST(2.1 AS DOUBLE) AS trip_distance
        """
    )
    silver_df.write.mode("overwrite").saveAsTable("unit_quality.silver_input")

    gold_df = spark.sql(
        """
        SELECT
          CAST('2024-01-01' AS DATE) AS trip_date,
          '1' AS vendor_id
        """
    )
    gold_df.write.mode("overwrite").saveAsTable("unit_quality.gold_input")


def test_quality_gate_warn_rule_does_not_fail_strict_mode(spark) -> None:
    _create_quality_input_tables(spark)

    result = run_quality_gate(
        spark,
        silver_table="unit_quality.silver_input",
        gold_table="unit_quality.gold_input",
        output_table="unit_quality.violations_warn",
        thresholds={"max_invalid_ratio": 0.0},
        strict=True,
        dataset="silver.trips_clean",
        run_id="test-run-warn",
        run_ts=datetime(2024, 1, 1, 0, 0, 0),
        window_year=2024,
        window_month=1,
        contract_version=2,
        rule_configs={
            "non_positive_trip_duration": {"rule_id": "QG002", "severity": "warn"},
        },
    )

    assert "non_positive_trip_duration" in ";".join(result["failed_rules"])
    assert result["failed_error_rules"] == []


def test_quality_gate_error_rule_fails_strict_mode(spark) -> None:
    _create_quality_input_tables(spark)

    with pytest.raises(ValueError, match="Quality gate failed"):
        run_quality_gate(
            spark,
            silver_table="unit_quality.silver_input",
            gold_table="unit_quality.gold_input",
            output_table="unit_quality.violations_error",
            thresholds={"max_invalid_ratio": 0.0},
            strict=True,
            dataset="silver.trips_clean",
            run_id="test-run-error",
            run_ts=datetime(2024, 1, 1, 0, 0, 0),
            window_year=2024,
            window_month=1,
            contract_version=2,
            rule_configs={
                "non_positive_trip_duration": {"rule_id": "QG002", "severity": "error"},
            },
        )
