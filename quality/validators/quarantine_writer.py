from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _ensure_table_namespace(df: DataFrame, table_name: str) -> None:
    if "." not in table_name:
        return
    namespace, _ = table_name.split(".", 1)
    df.sparkSession.sql(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(namespace)}")


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

    _ensure_table_namespace(quarantine_df, quarantine_table)

    enriched = (
        quarantine_df
        .withColumn("_quarantine_dataset", F.lit(dataset))
        .withColumn("_quarantine_contract_version", F.lit(contract_version))
        .withColumn("_quarantine_run_id", F.lit(run_id))
        .withColumn("_quarantine_ts", F.current_timestamp())
    )

    enriched.write.format("delta").mode("append").saveAsTable(quarantine_table)

    return {"quarantined_rows": enriched.count(), "table": quarantine_table}
