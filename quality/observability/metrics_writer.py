from __future__ import annotations

import json
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
    try:
        metrics_df.write.format("delta").mode(write_mode).saveAsTable(metrics_table)
    except Exception as exc:
        if not metrics_table.startswith("quality.") or not _is_schema_conflict_error(exc):
            raise

        _drop_table_if_exists(spark, metrics_table)
        (
            metrics_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(metrics_table)
        )
