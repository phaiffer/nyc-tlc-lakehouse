from __future__ import annotations

import json
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

_DEFAULT_THRESHOLDS = {
    "invalid_pickup_date_window": 0,
    "non_positive_trip_duration": 0,
    "negative_fare_or_distance": 0,
    "pk_duplicates_detected": 0,
}


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _quote_table_name(table_name: str) -> str:
    if "." not in table_name:
        return _quote_identifier(table_name)
    namespace, table = table_name.split(".", 1)
    return f"{_quote_identifier(namespace)}.{_quote_identifier(table)}"


def _ensure_table_namespace(spark: SparkSession, table_name: str) -> None:
    if "." not in table_name:
        return
    namespace, _ = table_name.split(".", 1)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(namespace)}")


def _drop_table_if_exists(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {_quote_table_name(table_name)}")


def _is_schema_conflict_error(exc: Exception) -> bool:
    error_text = str(exc).lower()
    markers = [
        "incompatible",
        "cannot cast",
        "invalidoperationexception",
        "failed to alter table",
        "analysisexception",
        "schema",
    ]
    return any(marker in error_text for marker in markers)


def _validate_columns(df: DataFrame, columns: list[str], *, label: str) -> None:
    missing_columns = sorted([column for column in columns if column not in df.columns])
    if missing_columns:
        raise ValueError(f"Missing required columns in {label}: {missing_columns}")


def _count_duplicate_rows(df: DataFrame, *, key_columns: list[str]) -> int:
    duplicate_row = (
        df.groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicate_rows"))
        .collect()[0]
    )
    return int(duplicate_row["duplicate_rows"] or 0)


def _resolve_threshold(rule_name: str, *, thresholds: dict[str, Any], total_rows: int) -> int:
    if rule_name in thresholds:
        return int(thresholds[rule_name])

    ratio_key = f"{rule_name}_ratio"
    if ratio_key in thresholds:
        return int(max(0.0, float(thresholds[ratio_key])) * max(total_rows, 1))

    if "max_invalid_ratio" in thresholds:
        return int(max(0.0, float(thresholds["max_invalid_ratio"])) * max(total_rows, 1))

    return _DEFAULT_THRESHOLDS[rule_name]


def run_quality_gate(
    spark: SparkSession,
    *,
    silver_table: str,
    gold_table: str,
    output_table: str,
    thresholds: dict[str, Any] | None = None,
    strict: bool = False,
) -> dict[str, Any]:
    """
    Execute auditable quality gate rules and persist a violations summary Delta table.
    """
    effective_thresholds = dict(_DEFAULT_THRESHOLDS)
    if thresholds:
        effective_thresholds.update(thresholds)

    silver_df = spark.table(silver_table)
    gold_df = spark.table(gold_table)

    _validate_columns(
        silver_df,
        ["trip_id", "pickup_date", "pickup_ts", "dropoff_ts", "fare_amount", "trip_distance"],
        label=f"silver_table='{silver_table}'",
    )
    _validate_columns(
        gold_df,
        ["trip_date", "vendor_id"],
        label=f"gold_table='{gold_table}'",
    )

    silver_rows = silver_df.count()

    gold_bounds = gold_df.select(
        F.min(F.col("trip_date")).alias("min_trip_date"),
        F.max(F.col("trip_date")).alias("max_trip_date"),
    ).collect()[0]
    min_trip_date = gold_bounds["min_trip_date"]
    max_trip_date = gold_bounds["max_trip_date"]

    if min_trip_date is None or max_trip_date is None:
        invalid_pickup_date_window = 0
    else:
        invalid_pickup_date_window = silver_df.filter(
            (F.col("pickup_date").isNull())
            | (F.col("pickup_date") < F.lit(min_trip_date))
            | (F.col("pickup_date") > F.lit(max_trip_date))
        ).count()

    non_positive_trip_duration = silver_df.filter(
        (F.col("pickup_ts").isNull())
        | (F.col("dropoff_ts").isNull())
        | (F.col("dropoff_ts") <= F.col("pickup_ts"))
    ).count()

    negative_fare_or_distance = silver_df.filter(
        (F.col("fare_amount") < F.lit(0)) | (F.col("trip_distance") < F.lit(0.0))
    ).count()

    silver_pk_duplicates = _count_duplicate_rows(silver_df, key_columns=["trip_id"])
    gold_pk_duplicates = _count_duplicate_rows(gold_df, key_columns=["trip_date", "vendor_id"])
    pk_duplicates_detected = silver_pk_duplicates + gold_pk_duplicates

    evaluations = [
        {
            "rule_name": "invalid_pickup_date_window",
            "violation_count": int(invalid_pickup_date_window),
            "details": {
                "silver_min_expected": str(min_trip_date) if min_trip_date is not None else None,
                "silver_max_expected": str(max_trip_date) if max_trip_date is not None else None,
            },
        },
        {
            "rule_name": "non_positive_trip_duration",
            "violation_count": int(non_positive_trip_duration),
            "details": {},
        },
        {
            "rule_name": "negative_fare_or_distance",
            "violation_count": int(negative_fare_or_distance),
            "details": {},
        },
        {
            "rule_name": "pk_duplicates_detected",
            "violation_count": int(pk_duplicates_detected),
            "details": {
                "silver_trip_id_duplicates": int(silver_pk_duplicates),
                "gold_trip_date_vendor_duplicates": int(gold_pk_duplicates),
            },
        },
    ]

    summary_rows: list[tuple[str, str, str, str, int, int, bool, str]] = []
    failing_rules: list[str] = []

    for evaluation in evaluations:
        rule_name = evaluation["rule_name"]
        violation_count = int(evaluation["violation_count"])
        threshold = _resolve_threshold(
            rule_name,
            thresholds=effective_thresholds,
            total_rows=silver_rows,
        )
        passed = violation_count <= threshold
        if not passed:
            failing_rules.append(
                f"{rule_name}(violations={violation_count}, threshold={threshold})"
            )

        summary_rows.append(
            (
                silver_table,
                gold_table,
                output_table,
                rule_name,
                violation_count,
                threshold,
                passed,
                json.dumps(evaluation["details"], sort_keys=True),
            )
        )

    summary_df = spark.createDataFrame(
        summary_rows,
        schema=[
            "silver_table",
            "gold_table",
            "output_table",
            "rule_name",
            "violation_count",
            "threshold",
            "passed",
            "details_json",
        ],
    ).withColumn("run_ts", F.current_timestamp())

    ordered_columns = [
        "run_ts",
        "silver_table",
        "gold_table",
        "output_table",
        "rule_name",
        "violation_count",
        "threshold",
        "passed",
        "details_json",
    ]
    summary_df = summary_df.select(*ordered_columns)

    _ensure_table_namespace(spark, output_table)
    try:
        (
            summary_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(output_table)
        )
    except Exception as exc:
        if not output_table.startswith("quality.") or not _is_schema_conflict_error(exc):
            raise

        _drop_table_if_exists(spark, output_table)
        (
            summary_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(output_table)
        )

    result = {
        "silver_rows": silver_rows,
        "rules_evaluated": len(evaluations),
        "failed_rules": failing_rules,
        "output_table": output_table,
    }

    if strict and failing_rules:
        raise ValueError(
            "Quality gate failed: " + "; ".join(failing_rules)
        )

    return result
