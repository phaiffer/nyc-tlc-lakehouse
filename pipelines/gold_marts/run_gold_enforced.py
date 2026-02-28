from __future__ import annotations

import calendar
import uuid
from dataclasses import asdict
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from pipelines.common.delta_incremental_merge import incremental_merge
from pipelines.common.local_delta_writer import write_delta_table_safe
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


def _build_gold_daily_kpis(silver_df: DataFrame) -> DataFrame:
    canonical_base = (
        silver_df.withColumn("trip_date", F.col("pickup_date").cast("date"))
        .withColumn("vendor_id", F.coalesce(F.col("vendor_id"), F.lit("UNKNOWN")))
        .withColumn("fare_amount", F.col("fare_amount").cast(_DECIMAL_18_2))
        .select("trip_date", "vendor_id", "fare_amount")
    )

    return canonical_base.groupBy("trip_date", "vendor_id").agg(
        F.count(F.lit(1)).cast("bigint").alias("trips"),
        F.sum(F.col("fare_amount")).cast(_DECIMAL_18_2).alias("total_fare"),
    )


def _build_dim_vendor(silver_df: DataFrame) -> DataFrame:
    return (
        silver_df.select(F.col("vendor_id").cast("string").alias("vendor_id"))
        .distinct()
        .filter(F.col("vendor_id").isNotNull())
        .withColumn(
            "vendor_name",
            F.when(F.col("vendor_id") == F.lit("1"), F.lit("Creative Mobile Technologies"))
            .when(F.col("vendor_id") == F.lit("2"), F.lit("VeriFone Inc."))
            .otherwise(F.lit("Unknown/Other")),
        )
        .withColumn(
            "is_known_vendor",
            F.col("vendor_id").isin(["1", "2"]).cast("boolean"),
        )
        .select("vendor_id", "vendor_name", "is_known_vendor")
    )


def _build_dim_payment_type(spark: SparkSession) -> DataFrame:
    rows = [
        (0, "Flex Fare trip"),
        (1, "Credit card"),
        (2, "Cash"),
        (3, "No charge"),
        (4, "Dispute"),
        (5, "Unknown"),
        (6, "Voided trip"),
    ]
    return spark.createDataFrame(
        rows,
        schema=["payment_type_id", "payment_type_name"],
    )


def _build_dim_rate_code(spark: SparkSession) -> DataFrame:
    rows = [
        (1, "Standard rate"),
        (2, "JFK"),
        (3, "Newark"),
        (4, "Nassau or Westchester"),
        (5, "Negotiated fare"),
        (6, "Group ride"),
        (99, "Unknown"),
    ]
    return spark.createDataFrame(
        rows,
        schema=["rate_code_id", "rate_code_name"],
    )


def _write_dimension_table(
    spark: SparkSession,
    *,
    table_name: str,
    source_df: DataFrame,
) -> None:
    _ensure_table_namespace(spark, table_name)
    write_delta_table_safe(
        spark,
        table_name=table_name,
        source_df=source_df,
        mode="overwrite",
        overwrite_schema=True,
        force_recreate=True,
        recreate_on_schema_conflict=True,
    )


def _assert_month_window_day_range(df: DataFrame, *, year: int | None, month: int | None) -> None:
    if year is None and month is None:
        return

    if year is None or month is None:
        raise ValueError(
            "Both year and month must be provided together for month window validation"
        )

    expected_days_in_month = calendar.monthrange(year, month)[1]
    observed_days = df.select("trip_date").distinct().count()

    if observed_days < 28 or observed_days > expected_days_in_month:
        raise ValueError(
            "Unexpected number of distinct trip_date values for filtered month: "
            f"observed_days={observed_days}, expected_range=28..{expected_days_in_month}"
        )


def run_gold_enforced(
    spark: SparkSession,
    *,
    repo_root: str,
    contract_dataset: str,
    input_table: str,
    output_table: str,
    quarantine_table: str,
    metrics_table: str = "quality.pipeline_metrics",
    max_invalid_ratio: float = 0.001,
    reconciliation_max_diff_ratio: float = 0.05,
    strict_reconciliation: bool = False,
    year: int | None = None,
    month: int | None = None,
    dim_vendor_table: str = "gold.dim_vendor",
    dim_payment_type_table: str = "gold.dim_payment_type",
    dim_rate_code_table: str = "gold.dim_rate_code",
    run_id: str | None = None,
) -> None:
    effective_run_id = run_id or str(uuid.uuid4())

    contract = load_contract_by_dataset(Path(repo_root), contract_dataset)
    if not contract.primary_key:
        raise ValueError("Contract must define primary_key for deterministic incremental merge.")

    watermark_column = contract.watermark or "trip_date"
    late_arrival_days = contract.late_arrival_days if contract.late_arrival_days is not None else 7

    silver_df = spark.table(input_table)
    windowed_silver_df = _apply_month_window(silver_df, year=year, month=month)

    gold_candidate_df = _build_gold_daily_kpis(windowed_silver_df)
    _assert_month_window_day_range(gold_candidate_df, year=year, month=month)

    enforcement = enforce_contract(
        gold_candidate_df,
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
        run_id=effective_run_id,
    )

    merge_metrics = incremental_merge(
        spark,
        source_df=enforcement.valid_df,
        target_table=output_table,
        primary_key=contract.primary_key,
        watermark_column=watermark_column,
        late_arrival_days=late_arrival_days,
    )

    reconciliation_source_view = "__gold_reconciliation_source"
    reconciliation_target_view = "__gold_reconciliation_target"

    enforcement.valid_df.createOrReplaceTempView(reconciliation_source_view)

    if year is not None and month is not None:
        target_window_start = F.to_date(F.lit(f"{year:04d}-{month:02d}-01"))
        target_window_end = F.add_months(target_window_start, 1)
        target_df = spark.table(output_table).filter(
            (F.col("trip_date") >= target_window_start) & (F.col("trip_date") < target_window_end)
        )
    else:
        target_df = spark.table(output_table)

    target_df.createOrReplaceTempView(reconciliation_target_view)

    try:
        reconciliation_report = reconcile_row_counts(
            spark,
            source_table=reconciliation_source_view,
            target_table=reconciliation_target_view,
            max_diff_ratio=reconciliation_max_diff_ratio,
            run_id=effective_run_id,
            strict=strict_reconciliation,
        )
    finally:
        spark.catalog.dropTempView(reconciliation_source_view)
        spark.catalog.dropTempView(reconciliation_target_view)

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
        run_id=effective_run_id,
        pipeline_name="gold_marts_enforced",
        dataset=contract.dataset,
        contract_version=contract.version,
        metrics=run_metrics,
    )

    dim_vendor_df = _build_dim_vendor(windowed_silver_df)
    dim_payment_type_df = _build_dim_payment_type(spark)
    dim_rate_code_df = _build_dim_rate_code(spark)

    _write_dimension_table(
        spark,
        table_name=dim_vendor_table,
        source_df=dim_vendor_df,
    )
    _write_dimension_table(
        spark,
        table_name=dim_payment_type_table,
        source_df=dim_payment_type_df,
    )
    _write_dimension_table(
        spark,
        table_name=dim_rate_code_table,
        source_df=dim_rate_code_df,
    )

    print("Gold enforcement summary:", enforcement.summary)
    print("Gold quarantine summary:", quarantine_summary)
    print("Gold merge metrics:", merge_metrics)
    print("Gold reconciliation report:", asdict(reconciliation_report))
    print(
        "Gold final run summary:",
        {
            "run_id": effective_run_id,
            "dataset": contract.dataset,
            "contract_version": contract.version,
            "output_table": output_table,
            "metrics_table": metrics_table,
            "window": {"year": year, "month": month},
            "watermark_column": watermark_column,
            "late_arrival_days": late_arrival_days,
            "enforcement_summary": enforcement.summary,
            "merge_metrics": merge_metrics,
            "reconciliation_report": asdict(reconciliation_report),
            "dimension_tables": {
                "vendor": dim_vendor_table,
                "payment_type": dim_payment_type_table,
                "rate_code": dim_rate_code_table,
            },
        },
    )
