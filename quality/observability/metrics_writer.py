from __future__ import annotations

import json
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _ensure_table_namespace(spark: SparkSession, table_name: str) -> None:
    if "." not in table_name:
        return
    namespace, _ = table_name.split(".", 1)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(namespace)}")


def write_pipeline_metrics(
    spark: SparkSession,
    metrics_table: str,
    run_id: str,
    pipeline_name: str,
    dataset: str,
    contract_version: int,
    metrics: dict[str, Any],
) -> None:
    metrics_json = json.dumps(metrics, sort_keys=True, default=str)
    metrics_df = spark.createDataFrame(
        [(run_id, pipeline_name, dataset, int(contract_version), metrics_json)],
        schema=["run_id", "pipeline_name", "dataset", "contract_version", "metrics_json"],
    ).withColumn("_ts", F.current_timestamp())

    ordered_columns = [
        "_ts",
        "run_id",
        "pipeline_name",
        "dataset",
        "contract_version",
        "metrics_json",
    ]
    metrics_df = metrics_df.select(*ordered_columns)

    _ensure_table_namespace(spark, metrics_table)

    write_mode = "append" if spark.catalog.tableExists(metrics_table) else "overwrite"
    metrics_df.write.format("delta").mode(write_mode).saveAsTable(metrics_table)
