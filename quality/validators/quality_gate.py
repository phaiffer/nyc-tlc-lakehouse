from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pipelines.common.local_delta_writer import (
    drop_table_if_exists,
    ensure_table_namespace,
    is_schema_conflict_error,
    write_delta_table_safe,
)

_DEFAULT_THRESHOLDS = {
    "invalid_pickup_date_window": 0,
    "non_positive_trip_duration": 0,
    "negative_fare_or_distance": 0,
    "pk_duplicates_detected": 0,
    "gold_non_positive_volume": 0,
    "gold_avg_fare_per_trip_out_of_range": 0,
    "gold_min_avg_fare_per_trip": 0.5,
    "gold_max_avg_fare_per_trip": 500.0,
}

_DEFAULT_RULE_CONFIGS = {
    "invalid_pickup_date_window": {"rule_id": "QG001", "severity": "error"},
    "non_positive_trip_duration": {"rule_id": "QG002", "severity": "error"},
    "negative_fare_or_distance": {"rule_id": "QG003", "severity": "error"},
    "pk_duplicates_detected": {"rule_id": "QG004", "severity": "error"},
    "gold_non_positive_volume": {"rule_id": "QG005", "severity": "error"},
    "gold_avg_fare_per_trip_out_of_range": {"rule_id": "QG006", "severity": "error"},
}


def _validate_columns(df: DataFrame, columns: list[str], *, label: str) -> None:
    missing_columns = sorted([column for column in columns if column not in df.columns])
    if missing_columns:
        raise ValueError(f"Missing required columns in {label}: {missing_columns}")


def _normalize_severity(value: Any) -> str:
    severity = str(value or "error").strip().lower()
    if severity not in {"error", "warn", "info"}:
        return "error"
    return severity


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


def _collect_sample_values(df: DataFrame, *, sample_column: str, limit: int = 5) -> str | None:
    rows = (
        df.select(F.col(sample_column))
        .where(F.col(sample_column).isNotNull())
        .limit(int(limit))
        .collect()
    )
    values = [row[sample_column] for row in rows if row[sample_column] is not None]
    if not values:
        return None
    return json.dumps(values, default=str)


def _resolve_rule_config(
    *,
    rule_name: str,
    rule_configs: dict[str, dict[str, Any]],
) -> tuple[str, str]:
    config = rule_configs.get(rule_name, {})
    rule_id = str(config.get("rule_id", rule_name)).strip() or rule_name
    severity = _normalize_severity(config.get("severity", "error"))
    return rule_id, severity


def run_quality_gate(
    spark: SparkSession,
    *,
    silver_table: str,
    gold_table: str,
    output_table: str,
    thresholds: dict[str, Any] | None = None,
    strict: bool = False,
    dataset: str | None = None,
    run_id: str,
    run_ts: datetime,
    window_year: int | None = None,
    window_month: int | None = None,
    contract_version: int | None = None,
    rule_configs: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """
    Execute auditable quality gate rules and persist a violations summary Delta table.
    """
    effective_thresholds = dict(_DEFAULT_THRESHOLDS)
    if thresholds:
        effective_thresholds.update(thresholds)

    effective_rule_configs = dict(_DEFAULT_RULE_CONFIGS)
    if rule_configs:
        for rule_name, config in rule_configs.items():
            existing = dict(effective_rule_configs.get(rule_name, {}))
            existing.update(config)
            effective_rule_configs[rule_name] = existing

    resolved_dataset = dataset or silver_table

    silver_df = spark.table(silver_table)
    gold_df = spark.table(gold_table)

    _validate_columns(
        silver_df,
        ["trip_id", "pickup_date", "pickup_ts", "dropoff_ts", "fare_amount", "trip_distance"],
        label=f"silver_table='{silver_table}'",
    )
    _validate_columns(
        gold_df,
        ["trip_date", "vendor_id", "trips", "total_fare"],
        label=f"gold_table='{gold_table}'",
    )

    silver_rows = silver_df.count()

    gold_bounds = gold_df.select(
        F.min(F.col("trip_date")).alias("min_trip_date"),
        F.max(F.col("trip_date")).alias("max_trip_date"),
        F.count(F.lit(1)).cast("bigint").alias("gold_rows"),
        F.sum(F.col("total_fare").cast("double")).alias("gold_total_fare"),
        F.sum(F.col("trips").cast("double")).alias("gold_total_trips"),
    ).collect()[0]
    min_trip_date = gold_bounds["min_trip_date"]
    max_trip_date = gold_bounds["max_trip_date"]
    gold_rows = int(gold_bounds["gold_rows"] or 0)
    gold_total_fare = float(gold_bounds["gold_total_fare"] or 0.0)
    gold_total_trips = float(gold_bounds["gold_total_trips"] or 0.0)
    avg_fare_per_trip = None if gold_total_trips <= 0 else gold_total_fare / gold_total_trips
    min_avg_fare_per_trip = float(effective_thresholds["gold_min_avg_fare_per_trip"])
    max_avg_fare_per_trip = float(effective_thresholds["gold_max_avg_fare_per_trip"])
    if min_avg_fare_per_trip > max_avg_fare_per_trip:
        raise ValueError(
            "Invalid quality thresholds: gold_min_avg_fare_per_trip cannot exceed "
            "gold_max_avg_fare_per_trip"
        )

    if min_trip_date is None or max_trip_date is None:
        invalid_pickup_date_df = silver_df.limit(0)
    else:
        invalid_pickup_date_df = silver_df.filter(
            (F.col("pickup_date").isNull())
            | (F.col("pickup_date") < F.lit(min_trip_date))
            | (F.col("pickup_date") > F.lit(max_trip_date))
        )
    invalid_pickup_date_window = invalid_pickup_date_df.count()

    non_positive_trip_duration_df = silver_df.filter(
        (F.col("pickup_ts").isNull())
        | (F.col("dropoff_ts").isNull())
        | (F.col("dropoff_ts") <= F.col("pickup_ts"))
    )
    non_positive_trip_duration = non_positive_trip_duration_df.count()

    negative_fare_or_distance_df = silver_df.filter(
        (F.col("fare_amount") < F.lit(0)) | (F.col("trip_distance") < F.lit(0.0))
    )
    negative_fare_or_distance = negative_fare_or_distance_df.count()

    silver_pk_duplicates = _count_duplicate_rows(silver_df, key_columns=["trip_id"])
    gold_pk_duplicates = _count_duplicate_rows(gold_df, key_columns=["trip_date", "vendor_id"])
    pk_duplicates_detected = silver_pk_duplicates + gold_pk_duplicates
    gold_non_positive_volume = 0 if gold_rows > 0 else 1
    gold_avg_fare_per_trip_out_of_range = (
        0
        if avg_fare_per_trip is not None
        and min_avg_fare_per_trip <= avg_fare_per_trip <= max_avg_fare_per_trip
        else 1
    )

    duplicate_trip_ids_df = (
        silver_df.groupBy("trip_id")
        .count()
        .filter(F.col("count") > 1)
        .select(F.col("trip_id").cast("string").alias("sample_value"))
    )

    evaluations = [
        {
            "rule_name": "invalid_pickup_date_window",
            "failed_count": int(invalid_pickup_date_window),
            "sample_values": _collect_sample_values(
                invalid_pickup_date_df.select(
                    F.col("pickup_date").cast("string").alias("sample_value")
                ),
                sample_column="sample_value",
            ),
            "details": {
                "silver_min_expected": str(min_trip_date) if min_trip_date is not None else None,
                "silver_max_expected": str(max_trip_date) if max_trip_date is not None else None,
            },
        },
        {
            "rule_name": "non_positive_trip_duration",
            "failed_count": int(non_positive_trip_duration),
            "sample_values": _collect_sample_values(
                non_positive_trip_duration_df.select(
                    F.col("trip_id").cast("string").alias("sample_value")
                ),
                sample_column="sample_value",
            ),
            "details": {},
        },
        {
            "rule_name": "negative_fare_or_distance",
            "failed_count": int(negative_fare_or_distance),
            "sample_values": _collect_sample_values(
                negative_fare_or_distance_df.select(
                    F.concat_ws(
                        "|",
                        F.col("trip_id").cast("string"),
                        F.col("fare_amount").cast("string"),
                        F.col("trip_distance").cast("string"),
                    ).alias("sample_value")
                ),
                sample_column="sample_value",
            ),
            "details": {},
        },
        {
            "rule_name": "pk_duplicates_detected",
            "failed_count": int(pk_duplicates_detected),
            "sample_values": _collect_sample_values(
                duplicate_trip_ids_df,
                sample_column="sample_value",
            ),
            "details": {
                "silver_trip_id_duplicates": int(silver_pk_duplicates),
                "gold_trip_date_vendor_duplicates": int(gold_pk_duplicates),
            },
        },
        {
            "rule_name": "gold_non_positive_volume",
            "failed_count": int(gold_non_positive_volume),
            "sample_values": json.dumps([gold_rows]),
            "details": {"gold_rows": int(gold_rows)},
        },
        {
            "rule_name": "gold_avg_fare_per_trip_out_of_range",
            "failed_count": int(gold_avg_fare_per_trip_out_of_range),
            "sample_values": (
                json.dumps([round(float(avg_fare_per_trip), 6)])
                if avg_fare_per_trip is not None
                else None
            ),
            "details": {
                "observed_avg_fare_per_trip": avg_fare_per_trip,
                "min_avg_fare_per_trip": min_avg_fare_per_trip,
                "max_avg_fare_per_trip": max_avg_fare_per_trip,
                "gold_total_fare": gold_total_fare,
                "gold_total_trips": gold_total_trips,
            },
        },
    ]

    summary_rows: list[tuple[Any, ...]] = []
    failed_rules: list[str] = []
    failed_error_rules: list[str] = []

    for evaluation in evaluations:
        rule_name = evaluation["rule_name"]
        failed_count = int(evaluation["failed_count"])
        threshold = _resolve_threshold(
            rule_name,
            thresholds=effective_thresholds,
            total_rows=silver_rows,
        )
        rule_id, severity = _resolve_rule_config(
            rule_name=rule_name,
            rule_configs=effective_rule_configs,
        )
        passed = failed_count <= threshold
        if not passed:
            message = f"{rule_name}(failed_count={failed_count}, threshold={threshold})"
            failed_rules.append(message)
            if severity == "error":
                failed_error_rules.append(message)

        summary_rows.append(
            (
                resolved_dataset,
                run_id,
                int(window_year) if window_year is not None else None,
                int(window_month) if window_month is not None else None,
                int(contract_version) if contract_version is not None else None,
                rule_id,
                rule_name,
                severity,
                bool(passed),
                int(failed_count),
                evaluation["sample_values"],
                int(threshold),
                json.dumps(evaluation["details"], sort_keys=True),
            )
        )

    summary_schema = StructType(
        [
            StructField("dataset", StringType(), nullable=False),
            StructField("run_id", StringType(), nullable=False),
            StructField("window_year", IntegerType(), nullable=True),
            StructField("window_month", IntegerType(), nullable=True),
            StructField("contract_version", IntegerType(), nullable=True),
            StructField("rule_id", StringType(), nullable=False),
            StructField("rule_name", StringType(), nullable=False),
            StructField("severity", StringType(), nullable=False),
            StructField("passed", BooleanType(), nullable=False),
            StructField("failed_count", LongType(), nullable=False),
            StructField("sample_values", StringType(), nullable=True),
            StructField("threshold", LongType(), nullable=False),
            StructField("details_json", StringType(), nullable=False),
        ]
    )
    summary_df = spark.createDataFrame(summary_rows, schema=summary_schema).withColumn(
        "run_ts",
        F.lit(run_ts).cast("timestamp"),
    )

    ordered_columns = [
        "dataset",
        "run_id",
        "run_ts",
        "window_year",
        "window_month",
        "contract_version",
        "rule_id",
        "rule_name",
        "severity",
        "passed",
        "failed_count",
        "sample_values",
        "threshold",
        "details_json",
    ]
    summary_df = summary_df.select(*ordered_columns)

    ensure_table_namespace(spark, output_table)
    if output_table.startswith("quality.") and spark.catalog.tableExists(output_table):
        # Drop/recreate avoids Hive alterTableDataSchema attempts during overwrite reruns.
        drop_table_if_exists(spark, output_table)

    try:
        write_delta_table_safe(
            spark,
            table_name=output_table,
            source_df=summary_df,
            mode="overwrite",
            overwrite_schema=True,
            recreate_on_schema_conflict=output_table.startswith("quality."),
        )
    except Exception as exc:
        if not output_table.startswith("quality.") or not is_schema_conflict_error(exc):
            raise

        drop_table_if_exists(spark, output_table)
        write_delta_table_safe(
            spark,
            table_name=output_table,
            source_df=summary_df,
            mode="overwrite",
            overwrite_schema=True,
            recreate_on_schema_conflict=True,
        )

    result = {
        "dataset": resolved_dataset,
        "run_id": run_id,
        "run_ts": run_ts.isoformat(timespec="seconds"),
        "silver_rows": silver_rows,
        "rules_evaluated": len(evaluations),
        "failed_rules": failed_rules,
        "failed_error_rules": failed_error_rules,
        "output_table": output_table,
    }

    if strict and failed_error_rules:
        raise ValueError("Quality gate failed: " + "; ".join(failed_error_rules))

    return result
