from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pipelines.common.local_delta_writer import write_delta_table_safe

DEFAULT_DRIFT_THRESHOLDS = {
    "volume_ratio": {"warn": 0.20, "error": 0.35},
    "avg_fare_amount_ratio": {"warn": 0.20, "error": 0.35},
    "avg_trip_distance_ratio": {"warn": 0.20, "error": 0.35},
    "passenger_distribution_l1": {"warn": 0.15, "error": 0.25},
}


def _to_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


def _load_drift_thresholds(config_path: Path, *, dataset: str) -> dict[str, dict[str, float]]:
    if not config_path.exists():
        return dict(DEFAULT_DRIFT_THRESHOLDS)

    payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return dict(DEFAULT_DRIFT_THRESHOLDS)

    datasets = payload.get("datasets", {})
    if not isinstance(datasets, dict):
        return dict(DEFAULT_DRIFT_THRESHOLDS)

    selected = datasets.get(dataset, {})
    if not isinstance(selected, dict):
        return dict(DEFAULT_DRIFT_THRESHOLDS)

    resolved: dict[str, dict[str, float]] = dict(DEFAULT_DRIFT_THRESHOLDS)
    for metric_name, threshold_block in selected.items():
        if metric_name not in resolved or not isinstance(threshold_block, dict):
            continue
        warn_value = threshold_block.get("warn", resolved[metric_name]["warn"])
        error_value = threshold_block.get("error", resolved[metric_name]["error"])
        resolved[metric_name] = {
            "warn": float(warn_value),
            "error": float(error_value),
        }
    return resolved


def _relative_delta_ratio(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None:
        return None
    if baseline == 0:
        return 0.0 if current == 0 else 1.0
    return abs(current - baseline) / abs(baseline)


def _resolve_severity(
    *,
    delta_ratio: float | None,
    warn_threshold: float,
    error_threshold: float,
) -> str | None:
    if delta_ratio is None:
        return None
    if delta_ratio >= error_threshold:
        return "error"
    if delta_ratio >= warn_threshold:
        return "warn"
    return None


def _passenger_distribution(df: DataFrame, *, column_name: str | None) -> dict[str, float]:
    if not column_name or column_name not in df.columns:
        return {}

    total_rows = df.count()
    if total_rows <= 0:
        return {}

    rows = (
        df.groupBy(F.coalesce(F.col(column_name).cast("string"), F.lit("__null__")).alias("key"))
        .count()
        .collect()
    )
    return {str(row["key"]): float(row["count"]) / float(total_rows) for row in rows}


def _distribution_l1_distance(current: dict[str, float], baseline: dict[str, float]) -> float:
    keys = set(current) | set(baseline)
    if not keys:
        return 0.0
    return float(sum(abs(float(current.get(k, 0.0)) - float(baseline.get(k, 0.0))) for k in keys))


def _read_latest_profile(
    spark: SparkSession,
    *,
    baseline_table: str,
    dataset: str,
) -> dict[str, Any] | None:
    if not spark.catalog.tableExists(baseline_table):
        return None

    rows = (
        spark.table(baseline_table)
        .filter(F.col("dataset") == F.lit(dataset))
        .orderBy(F.col("run_ts").desc())
        .limit(1)
        .collect()
    )
    if not rows:
        return None
    return rows[0].asDict(recursive=True)


def _write_baseline_profile(
    spark: SparkSession,
    *,
    baseline_table: str,
    dataset: str,
    run_id: str,
    run_ts: Any,
    window_year: int | None,
    window_month: int | None,
    volume: int,
    avg_fare_amount: float | None,
    avg_trip_distance: float | None,
    passenger_distribution: dict[str, float],
) -> None:
    schema = StructType(
        [
            StructField("dataset", StringType(), nullable=False),
            StructField("run_id", StringType(), nullable=False),
            StructField("window_year", IntegerType(), nullable=True),
            StructField("window_month", IntegerType(), nullable=True),
            StructField("volume", LongType(), nullable=False),
            StructField("avg_fare_amount", DoubleType(), nullable=True),
            StructField("avg_trip_distance", DoubleType(), nullable=True),
            StructField("passenger_distribution_json", StringType(), nullable=False),
        ]
    )

    baseline_df = spark.createDataFrame(
        [
            (
                dataset,
                run_id,
                int(window_year) if window_year is not None else None,
                int(window_month) if window_month is not None else None,
                int(volume),
                float(avg_fare_amount) if avg_fare_amount is not None else None,
                float(avg_trip_distance) if avg_trip_distance is not None else None,
                _to_json(passenger_distribution),
            )
        ],
        schema=schema,
    ).withColumn("run_ts", F.lit(run_ts).cast("timestamp"))

    write_mode = "append" if spark.catalog.tableExists(baseline_table) else "overwrite"
    write_delta_table_safe(
        spark,
        table_name=baseline_table,
        source_df=baseline_df.select(
            "dataset",
            "run_id",
            "run_ts",
            "window_year",
            "window_month",
            "volume",
            "avg_fare_amount",
            "avg_trip_distance",
            "passenger_distribution_json",
        ),
        mode=write_mode,
        overwrite_schema=(write_mode == "overwrite"),
        recreate_on_schema_conflict=baseline_table.startswith("quality."),
    )


def _write_drift_events(
    spark: SparkSession,
    *,
    drift_events_table: str,
    events: list[tuple[Any, ...]],
) -> None:
    if not events:
        return

    schema = StructType(
        [
            StructField("dataset", StringType(), nullable=False),
            StructField("run_id", StringType(), nullable=False),
            StructField("window_year", IntegerType(), nullable=True),
            StructField("window_month", IntegerType(), nullable=True),
            StructField("metric_name", StringType(), nullable=False),
            StructField("severity", StringType(), nullable=False),
            StructField("baseline_value", StringType(), nullable=True),
            StructField("observed_value", StringType(), nullable=True),
            StructField("delta_ratio", DoubleType(), nullable=True),
            StructField("warn_threshold", DoubleType(), nullable=False),
            StructField("error_threshold", DoubleType(), nullable=False),
            StructField("details_json", StringType(), nullable=False),
        ]
    )
    events_df = spark.createDataFrame(events, schema=schema)
    events_df = events_df.withColumn("run_ts", F.current_timestamp().cast("timestamp"))

    ordered = [
        "dataset",
        "run_id",
        "run_ts",
        "window_year",
        "window_month",
        "metric_name",
        "severity",
        "baseline_value",
        "observed_value",
        "delta_ratio",
        "warn_threshold",
        "error_threshold",
        "details_json",
    ]
    final_df = events_df.select(*ordered)

    write_mode = "append" if spark.catalog.tableExists(drift_events_table) else "overwrite"
    write_delta_table_safe(
        spark,
        table_name=drift_events_table,
        source_df=final_df,
        mode=write_mode,
        overwrite_schema=(write_mode == "overwrite"),
        recreate_on_schema_conflict=drift_events_table.startswith("quality."),
    )


def detect_and_record_drift(
    spark: SparkSession,
    *,
    source_df: DataFrame,
    dataset: str,
    run_id: str,
    run_ts: Any,
    window_year: int | None,
    window_month: int | None,
    config_path: Path,
    drift_events_table: str = "quality.drift_events",
    baseline_table: str = "quality.drift_baseline_metrics",
    fare_column: str = "fare_amount",
    distance_column: str = "trip_distance",
    passenger_count_column: str = "passenger_count",
) -> dict[str, Any]:
    thresholds = _load_drift_thresholds(config_path, dataset=dataset)

    aggregate_row = source_df.agg(
        F.count(F.lit(1)).cast("bigint").alias("volume"),
        F.avg(F.col(fare_column)).cast("double").alias("avg_fare_amount")
        if fare_column in source_df.columns
        else F.lit(None).cast("double").alias("avg_fare_amount"),
        F.avg(F.col(distance_column)).cast("double").alias("avg_trip_distance")
        if distance_column in source_df.columns
        else F.lit(None).cast("double").alias("avg_trip_distance"),
    ).collect()[0]
    current_volume = int(aggregate_row["volume"] or 0)
    current_avg_fare = (
        float(aggregate_row["avg_fare_amount"])
        if aggregate_row["avg_fare_amount"] is not None
        else None
    )
    current_avg_distance = (
        float(aggregate_row["avg_trip_distance"])
        if aggregate_row["avg_trip_distance"] is not None
        else None
    )
    current_distribution = _passenger_distribution(
        source_df,
        column_name=passenger_count_column,
    )

    baseline = _read_latest_profile(
        spark,
        baseline_table=baseline_table,
        dataset=dataset,
    )

    events: list[tuple[Any, ...]] = []
    if baseline is not None:
        baseline_distribution = json.loads(baseline.get("passenger_distribution_json") or "{}")

        metric_pairs = [
            (
                "volume_ratio",
                float(current_volume),
                float(baseline.get("volume")) if baseline.get("volume") is not None else None,
            ),
            (
                "avg_fare_amount_ratio",
                current_avg_fare,
                float(baseline.get("avg_fare_amount"))
                if baseline.get("avg_fare_amount") is not None
                else None,
            ),
            (
                "avg_trip_distance_ratio",
                current_avg_distance,
                float(baseline.get("avg_trip_distance"))
                if baseline.get("avg_trip_distance") is not None
                else None,
            ),
        ]

        for metric_name, current_value, baseline_value in metric_pairs:
            warn_threshold = float(thresholds[metric_name]["warn"])
            error_threshold = float(thresholds[metric_name]["error"])
            delta_ratio = _relative_delta_ratio(current_value, baseline_value)
            severity = _resolve_severity(
                delta_ratio=delta_ratio,
                warn_threshold=warn_threshold,
                error_threshold=error_threshold,
            )
            if severity is None:
                continue

            events.append(
                (
                    dataset,
                    run_id,
                    int(window_year) if window_year is not None else None,
                    int(window_month) if window_month is not None else None,
                    metric_name,
                    severity,
                    str(baseline_value) if baseline_value is not None else None,
                    str(current_value) if current_value is not None else None,
                    float(delta_ratio) if delta_ratio is not None else None,
                    warn_threshold,
                    error_threshold,
                    _to_json({"baseline_run_id": baseline.get("run_id")}),
                )
            )

        distribution_metric = "passenger_distribution_l1"
        distribution_warn = float(thresholds[distribution_metric]["warn"])
        distribution_error = float(thresholds[distribution_metric]["error"])
        distribution_delta = _distribution_l1_distance(current_distribution, baseline_distribution)
        distribution_severity = _resolve_severity(
            delta_ratio=distribution_delta,
            warn_threshold=distribution_warn,
            error_threshold=distribution_error,
        )
        if distribution_severity is not None:
            events.append(
                (
                    dataset,
                    run_id,
                    int(window_year) if window_year is not None else None,
                    int(window_month) if window_month is not None else None,
                    distribution_metric,
                    distribution_severity,
                    _to_json(baseline_distribution),
                    _to_json(current_distribution),
                    float(distribution_delta),
                    distribution_warn,
                    distribution_error,
                    _to_json({"baseline_run_id": baseline.get("run_id")}),
                )
            )
    else:
        events.append(
            (
                dataset,
                run_id,
                int(window_year) if window_year is not None else None,
                int(window_month) if window_month is not None else None,
                "baseline_bootstrap",
                "info",
                None,
                None,
                None,
                0.0,
                0.0,
                _to_json(
                    {
                        "note": (
                            "No prior baseline profile found; drift evaluation starts next run."
                        ),
                    }
                ),
            )
        )

    _write_drift_events(
        spark,
        drift_events_table=drift_events_table,
        events=events,
    )

    _write_baseline_profile(
        spark,
        baseline_table=baseline_table,
        dataset=dataset,
        run_id=run_id,
        run_ts=run_ts,
        window_year=window_year,
        window_month=window_month,
        volume=current_volume,
        avg_fare_amount=current_avg_fare,
        avg_trip_distance=current_avg_distance,
        passenger_distribution=current_distribution,
    )

    return {
        "dataset": dataset,
        "events_emitted": len(events),
        "baseline_table": baseline_table,
        "drift_events_table": drift_events_table,
        "thresholds": thresholds,
        "current_profile": {
            "volume": current_volume,
            "avg_fare_amount": current_avg_fare,
            "avg_trip_distance": current_avg_distance,
            "passenger_distribution": current_distribution,
        },
    }
