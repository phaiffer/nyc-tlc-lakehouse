from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import SparkSession


@dataclass(frozen=True)
class ReconciliationReport:
    source_table: str
    target_table: str
    run_id: str
    source_count: int
    target_count: int
    count_diff: int
    count_diff_ratio: float
    passed: bool
    notes: str


def reconcile_row_counts(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    *,
    max_diff_ratio: float,
    run_id: str = "N/A",
    strict: bool = False,
) -> ReconciliationReport:
    if max_diff_ratio < 0:
        raise ValueError("max_diff_ratio must be >= 0")

    source_count = spark.table(source_table).count()
    target_count = spark.table(target_table).count()
    count_diff = abs(source_count - target_count)
    denominator = max(source_count, 1)
    count_diff_ratio = count_diff / denominator
    passed = count_diff_ratio <= max_diff_ratio

    if passed:
        notes = (
            "Row count reconciliation passed: "
            f"diff_ratio={count_diff_ratio:.6f} threshold={max_diff_ratio:.6f}"
        )
    else:
        notes = (
            "Row count reconciliation failed: "
            f"diff_ratio={count_diff_ratio:.6f} threshold={max_diff_ratio:.6f}"
        )

    report = ReconciliationReport(
        source_table=source_table,
        target_table=target_table,
        run_id=run_id,
        source_count=source_count,
        target_count=target_count,
        count_diff=count_diff,
        count_diff_ratio=count_diff_ratio,
        passed=passed,
        notes=notes,
    )

    if strict and not report.passed:
        raise ValueError(report.notes)

    return report
