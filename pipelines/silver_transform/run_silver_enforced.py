from __future__ import annotations

import uuid
from dataclasses import asdict
from decimal import Decimal
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

from pipelines.common.delta_incremental_merge import incremental_merge
from quality.observability.metrics_writer import write_pipeline_metrics
from quality.reconciliation.reconciliation_checks import reconcile_row_counts
from quality.validators.contract_loader import load_contract_by_dataset
from quality.validators.quarantine_writer import write_quarantine
from quality.validators.spark_contract_enforcer import enforce_contract

_DECIMAL_18_2 = DecimalType(18, 2)


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _ensure_table_namespace(spark: SparkSession, table_name: str) -> None:
    if "." not in table_name:
        return
    namespace, _ = table_name.split(".", 1)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(namespace)}")


def _stable_text(column_name: str):
    return F.coalesce(F.col(column_name).cast("string"), F.lit("__null__"))


def _stable_timestamp_text(column_name: str):
    return F.coalesce(
        F.date_format(F.col(column_name), "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        F.lit("__null__"),
    )


def _stable_float_text(column_name: str, decimals: int):
    pattern = f"%.{decimals}f"
    return F.coalesce(
        F.format_string(pattern, F.col(column_name).cast("double")),
        F.lit("__null__"),
    )


def apply_silver_rules(df: DataFrame) -> DataFrame:
    """
    Build a deterministic canonical Silver model from TLC Bronze rows.
    """
    required_columns = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "fare_amount",
        "passenger_count",
    ]
    missing_columns = sorted([column for column in required_columns if column not in df.columns])
    if missing_columns:
        raise ValueError(f"Missing required columns in Bronze input: {missing_columns}")

    canonical_df = (
        df.withColumn("vendor_id", F.col("VendorID").cast("string"))
        .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
        .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("fare_amount", F.col("fare_amount").cast(_DECIMAL_18_2))
        .withColumn("passenger_count", F.col("passenger_count").cast("bigint"))
        .withColumn("updated_at", F.col("tpep_pickup_datetime").cast("timestamp"))
    )

    trip_id_expr = F.sha2(
        F.concat_ws(
            "||",
            _stable_text("vendor_id"),
            _stable_timestamp_text("pickup_ts"),
            _stable_timestamp_text("dropoff_ts"),
            _stable_float_text("trip_distance", 6),
            _stable_float_text("fare_amount", 2),
            _stable_text("passenger_count"),
        ),
        256,
    )

    canonical_filtered_df = (
        canonical_df.withColumn("trip_id", trip_id_expr)
        .filter(F.col("pickup_ts").isNotNull())
        .filter(F.col("dropoff_ts").isNotNull())
        .filter(F.col("dropoff_ts") > F.col("pickup_ts"))
        .filter(F.col("pickup_date").isNotNull())
        .filter(F.col("fare_amount") >= F.lit(Decimal("0.00")).cast(_DECIMAL_18_2))
        .filter(F.col("trip_distance") >= F.lit(0.0))
        .select(
            "trip_id",
            "vendor_id",
            "pickup_ts",
            "dropoff_ts",
            "pickup_date",
            "trip_distance",
            "fare_amount",
            "passenger_count",
            "updated_at",
        )
    )

    stable_columns = [F.col(column) for column in sorted(canonical_filtered_df.columns)]
    with_tie_breaker = canonical_filtered_df.withColumn(
        "__stable_tiebreaker",
        F.sha2(F.to_json(F.struct(*stable_columns)), 256),
    )
    dedup_window = Window.partitionBy(F.col("trip_id")).orderBy(
        F.col("updated_at").desc_nulls_last(),
        F.col("__stable_tiebreaker").desc(),
    )

    return (
        with_tie_breaker.withColumn("__row_number", F.row_number().over(dedup_window))
        .filter(F.col("__row_number") == 1)
        .drop("__row_number", "__stable_tiebreaker")
    )


def _apply_month_window(df: DataFrame, *, year: int | None, month: int | None) -> DataFrame:
    if year is None and month is None:
        return df

    if year is None or month is None:
        raise ValueError("Both year and month must be provided together for month window filtering")
    if month < 1 or month > 12:
        raise ValueError("month must be between 1 and 12")

    window_start = F.to_date(F.lit(f"{year:04d}-{month:02d}-01"))
    window_end = F.add_months(window_start, 1)

    return df.filter((F.col("pickup_date") >= window_start) & (F.col("pickup_date") < window_end))


def run_silver_enforced(
    spark: SparkSession,
    *,
    repo_root: str,
    contract_dataset: str,
    input_table: str,
    output_table: str,
    quarantine_table: str,
    max_invalid_ratio: float = 0.001,
    reconciliation_max_diff_ratio: float = 0.02,
    strict_reconciliation: bool = False,
    metrics_table: str = "quality.pipeline_metrics",
    year: int | None = None,
    month: int | None = None,
) -> None:
    run_id = str(uuid.uuid4())

    contract = load_contract_by_dataset(Path(repo_root), contract_dataset)
    if not contract.primary_key:
        raise ValueError("Contract must define primary_key for deterministic incremental merge.")
    if not contract.watermark:
        raise ValueError("Contract must define watermark for incremental merge windowing.")
    if contract.late_arrival_days is None:
        raise ValueError("Contract must define late_arrival_days for incremental merge windowing.")

    source_df = spark.table(input_table)
    silver_candidate_df = apply_silver_rules(source_df)
    silver_candidate_df = _apply_month_window(silver_candidate_df, year=year, month=month)

    enforcement = enforce_contract(
        silver_candidate_df,
        contract,
        max_invalid_ratio=max_invalid_ratio,
    )

    _ensure_table_namespace(spark, output_table)
    _ensure_table_namespace(spark, quarantine_table)
    _ensure_table_namespace(spark, metrics_table)

    quarantine_summary = write_quarantine(
        enforcement.quarantine_df,
        quarantine_table=quarantine_table,
        dataset=contract.dataset,
        contract_version=contract.version,
        run_id=run_id,
    )

    merge_metrics = incremental_merge(
        spark,
        source_df=enforcement.valid_df,
        target_table=output_table,
        primary_key=contract.primary_key,
        watermark_column=contract.watermark,
        late_arrival_days=contract.late_arrival_days,
    )

    reconciliation_source_view = "__silver_reconciliation_source"
    enforcement.valid_df.createOrReplaceTempView(reconciliation_source_view)
    try:
        reconciliation_report = reconcile_row_counts(
            spark,
            source_table=reconciliation_source_view,
            target_table=output_table,
            max_diff_ratio=reconciliation_max_diff_ratio,
            run_id=run_id,
            strict=strict_reconciliation,
        )
    finally:
        if spark.catalog.tableExists(reconciliation_source_view):
            spark.catalog.dropTempView(reconciliation_source_view)

    run_metrics = {
        "enforcement_summary": enforcement.summary,
        "quarantine_summary": quarantine_summary,
        "merge_metrics": merge_metrics,
        "reconciliation_report": asdict(reconciliation_report),
        "window": {
            "year": year,
            "month": month,
        },
    }
    write_pipeline_metrics(
        spark,
        metrics_table=metrics_table,
        run_id=run_id,
        pipeline_name="silver_transform_enforced",
        dataset=contract.dataset,
        contract_version=contract.version,
        metrics=run_metrics,
    )

    print("Silver enforcement summary:", enforcement.summary)
    print("Silver quarantine summary:", quarantine_summary)
    print("Silver merge metrics:", merge_metrics)
    print("Silver reconciliation report:", asdict(reconciliation_report))
    print(
        "Silver final run summary:",
        {
            "run_id": run_id,
            "dataset": contract.dataset,
            "contract_version": contract.version,
            "output_table": output_table,
            "metrics_table": metrics_table,
            "window": {"year": year, "month": month},
            "merge_metrics": merge_metrics,
            "enforcement_summary": enforcement.summary,
        },
    )
