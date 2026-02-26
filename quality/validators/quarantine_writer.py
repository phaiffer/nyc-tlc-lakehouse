from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipelines.common.local_delta_writer import (
    is_schema_conflict_error,
    table_schema_differs,
    write_delta_table_safe,
)


def write_quarantine(
    quarantine_df: DataFrame,
    *,
    quarantine_table: str,
    dataset: str,
    contract_version: int,
    run_id: str,
) -> dict[str, Any]:
    """
    Append quarantined records into a Delta table with audit metadata columns.
    """
    if quarantine_df.rdd.isEmpty():
        return {"quarantined_rows": 0, "table": quarantine_table}

    enriched = (
        quarantine_df
        .withColumn("_quarantine_dataset", F.lit(dataset))
        .withColumn("_quarantine_contract_version", F.lit(contract_version))
        .withColumn("_quarantine_run_id", F.lit(run_id))
        .withColumn("_quarantine_ts", F.current_timestamp().cast("timestamp"))
    )

    spark = enriched.sparkSession
    table_exists = spark.catalog.tableExists(quarantine_table)
    recreate_on_mismatch = quarantine_table.startswith("quality.") and table_schema_differs(
        spark,
        table_name=quarantine_table,
        expected_df=enriched,
    )

    try:
        write_mode = "append" if table_exists else "overwrite"
        write_delta_table_safe(
            spark,
            table_name=quarantine_table,
            source_df=enriched,
            mode=write_mode,
            overwrite_schema=(write_mode == "overwrite"),
            recreate_on_schema_mismatch=recreate_on_mismatch,
            recreate_on_schema_conflict=quarantine_table.startswith("quality."),
        )
    except Exception as exc:
        if not quarantine_table.startswith("quality.") or not is_schema_conflict_error(exc):
            raise

        write_delta_table_safe(
            spark,
            table_name=quarantine_table,
            source_df=enriched,
            mode="overwrite",
            overwrite_schema=True,
            force_recreate=True,
            recreate_on_schema_conflict=True,
        )

    return {"quarantined_rows": enriched.count(), "table": quarantine_table}
