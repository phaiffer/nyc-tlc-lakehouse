from __future__ import annotations

from datetime import datetime

from quality.validators.quality_gate import run_quality_gate


def _schema_signature(table_df) -> list[tuple[str, str]]:
    return [(field.name, field.dataType.simpleString()) for field in table_df.schema.fields]


def test_quality_gate_output_schema_is_stable_across_reruns(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS unit_quality")
    silver_table = "unit_quality.silver_schema_stability"
    gold_table = "unit_quality.gold_schema_stability"
    output_table = "quality.violations_schema_stability"
    spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
    spark.sql(f"DROP TABLE IF EXISTS {gold_table}")
    spark.sql(f"DROP TABLE IF EXISTS {output_table}")

    silver_df = spark.sql(
        """
        SELECT
          'trip_1' AS trip_id,
          CAST('2024-01-01' AS DATE) AS pickup_date,
          CAST('2024-01-01 10:00:00' AS TIMESTAMP) AS pickup_ts,
          CAST('2024-01-01 10:10:00' AS TIMESTAMP) AS dropoff_ts,
          CAST(15.5 AS DECIMAL(18,2)) AS fare_amount,
          CAST(3.1 AS DOUBLE) AS trip_distance
        """
    )
    silver_df.write.mode("overwrite").saveAsTable(silver_table)

    gold_df = spark.sql(
        """
        SELECT
          CAST('2024-01-01' AS DATE) AS trip_date,
          '1' AS vendor_id,
          CAST(10 AS BIGINT) AS trips,
          CAST(155.0 AS DECIMAL(18,2)) AS total_fare
        """
    )
    gold_df.write.mode("overwrite").saveAsTable(gold_table)

    run_quality_gate(
        spark,
        silver_table=silver_table,
        gold_table=gold_table,
        output_table=output_table,
        thresholds={"max_invalid_ratio": 0.0},
        strict=False,
        dataset="silver.trips_clean",
        run_id="schema-stability-1",
        run_ts=datetime(2024, 1, 1, 0, 0, 0),
        window_year=2024,
        window_month=1,
        contract_version=2,
    )
    first_signature = _schema_signature(spark.table(output_table))

    run_quality_gate(
        spark,
        silver_table=silver_table,
        gold_table=gold_table,
        output_table=output_table,
        thresholds={"max_invalid_ratio": 0.0},
        strict=False,
        dataset="silver.trips_clean",
        run_id="schema-stability-2",
        run_ts=datetime(2024, 1, 1, 1, 0, 0),
        window_year=2024,
        window_month=1,
        contract_version=2,
    )
    second_signature = _schema_signature(spark.table(output_table))

    expected_signature = [
        ("dataset", "string"),
        ("run_id", "string"),
        ("run_ts", "timestamp"),
        ("window_year", "int"),
        ("window_month", "int"),
        ("contract_version", "int"),
        ("rule_id", "string"),
        ("rule_name", "string"),
        ("severity", "string"),
        ("passed", "boolean"),
        ("failed_count", "bigint"),
        ("sample_values", "string"),
        ("threshold", "bigint"),
        ("details_json", "string"),
    ]

    assert first_signature == expected_signature
    assert second_signature == expected_signature
    assert spark.table(output_table).count() == 6
