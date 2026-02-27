from __future__ import annotations

import json
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from pipelines.common.local_delta_writer import (
    is_schema_conflict_error,
    table_schema_differs,
    write_delta_table_safe,
)


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
    metrics_schema = StructType(
        [
            StructField("run_id", StringType(), nullable=False),
            StructField("pipeline_name", StringType(), nullable=False),
            StructField("dataset", StringType(), nullable=False),
            StructField("contract_version", IntegerType(), nullable=False),
            StructField("metrics_json", StringType(), nullable=False),
        ]
    )
    metrics_df = spark.createDataFrame(
        [(run_id, pipeline_name, dataset, int(contract_version), metrics_json)],
        schema=metrics_schema,
    ).withColumn("run_ts", F.current_timestamp().cast("timestamp"))

    ordered_columns = [
        "run_ts",
        "run_id",
        "pipeline_name",
        "dataset",
        "contract_version",
        "metrics_json",
    ]
    metrics_df = metrics_df.select(*ordered_columns)

    recreate_on_conflict = metrics_table.startswith("quality.")
    recreate_on_mismatch = metrics_table.startswith("quality.") and table_schema_differs(
        spark,
        table_name=metrics_table,
        expected_df=metrics_df,
    )

    write_mode = "append" if spark.catalog.tableExists(metrics_table) else "overwrite"
    try:
        write_delta_table_safe(
            spark,
            table_name=metrics_table,
            source_df=metrics_df,
            mode=write_mode,
            overwrite_schema=(write_mode == "overwrite"),
            recreate_on_schema_mismatch=recreate_on_mismatch,
            recreate_on_schema_conflict=recreate_on_conflict,
        )
    except Exception as exc:
        if not metrics_table.startswith("quality.") or not is_schema_conflict_error(exc):
            raise

        write_delta_table_safe(
            spark,
            table_name=metrics_table,
            source_df=metrics_df,
            mode="overwrite",
            overwrite_schema=True,
            force_recreate=True,
            recreate_on_schema_conflict=True,
        )
