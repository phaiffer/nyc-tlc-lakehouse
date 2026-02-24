from __future__ import annotations

import uuid
from dataclasses import asdict
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pipelines.common.delta_incremental_merge import incremental_merge
from quality.observability.metrics_writer import write_pipeline_metrics
from quality.reconciliation.reconciliation_checks import reconcile_row_counts
from quality.validators.contract_loader import load_contract_by_dataset
from quality.validators.quarantine_writer import write_quarantine
from quality.validators.spark_contract_enforcer import enforce_contract


def apply_silver_rules(df: DataFrame) -> DataFrame:
    """
    Apply deterministic Silver cleansing rules.

    Keep this function:
    - deterministic
    - idempotent
    - free from side effects
    """
    # Example: derive partition column deterministically.
    return df.withColumn("pickup_date", F.to_date(F.col("tpep_pickup_datetime")))


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

    enforcement = enforce_contract(
        silver_candidate_df,
        contract,
        max_invalid_ratio=max_invalid_ratio,
    )

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

    reconciliation_report = reconcile_row_counts(
        spark,
        source_table=input_table,
        target_table=output_table,
        max_diff_ratio=reconciliation_max_diff_ratio,
        run_id=run_id,
        strict=strict_reconciliation,
    )

    run_metrics = {
        "enforcement_summary": enforcement.summary,
        "quarantine_summary": quarantine_summary,
        "merge_metrics": merge_metrics,
        "reconciliation_report": asdict(reconciliation_report),
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
            "merge_metrics": merge_metrics,
            "enforcement_summary": enforcement.summary,
        },
    )
