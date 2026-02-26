from __future__ import annotations

from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from pipelines.common.local_delta_writer import write_delta_table_safe

try:
    from delta.tables import DeltaTable
except ImportError:  # pragma: no cover - fallback path is tested by runtime.
    DeltaTable = None


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _validate_columns(df: DataFrame, columns: list[str], *, label: str) -> None:
    missing_columns = sorted([column for column in columns if column not in df.columns])
    if missing_columns:
        raise ValueError(f"Missing required columns in {label}: {missing_columns}")


def _deduplicate_latest(
    source_df: DataFrame,
    *,
    primary_key: list[str],
    watermark_column: str,
) -> DataFrame:
    stable_columns = [F.col(column) for column in sorted(source_df.columns)]
    with_tie_breaker = source_df.withColumn(
        "__stable_tiebreaker",
        F.sha2(F.to_json(F.struct(*stable_columns)), 256),
    )

    window = Window.partitionBy(*[F.col(column) for column in primary_key]).orderBy(
        F.col(watermark_column).desc_nulls_last(),
        F.col("__stable_tiebreaker").desc(),
    )

    return (
        with_tie_breaker.withColumn("__row_number", F.row_number().over(window))
        .filter(F.col("__row_number") == 1)
        .drop("__row_number", "__stable_tiebreaker")
    )


def _compute_lower_bound(
    spark: SparkSession,
    *,
    max_watermark_target: Any,
    late_arrival_days: int,
) -> Any:
    row = (
        spark.range(1)
        .select(
            F.date_sub(F.to_date(F.lit(max_watermark_target)), late_arrival_days).alias(
                "lower_bound"
            )
        )
        .collect()[0]
    )
    return row["lower_bound"]


def _build_merge_condition(primary_key: list[str]) -> str:
    return " AND ".join(
        [
            f"target.{_quote_identifier(column)} <=> source.{_quote_identifier(column)}"
            for column in primary_key
        ]
    )


def _add_missing_target_columns(
    spark: SparkSession,
    *,
    target_table: str,
    source_df: DataFrame,
) -> None:
    target_schema = spark.table(target_table).schema
    target_columns = {field.name for field in target_schema.fields}
    missing_columns = [
        field for field in source_df.schema.fields if field.name not in target_columns
    ]

    for field in missing_columns:
        spark.sql(
            f"ALTER TABLE {target_table} "
            f"ADD COLUMNS ({_quote_identifier(field.name)} {field.dataType.simpleString()})"
        )


def _merge_with_delta_api(
    spark: SparkSession,
    *,
    source_df: DataFrame,
    target_table: str,
    merge_condition: str,
) -> None:
    delta_table = DeltaTable.forName(spark, target_table)
    (
        delta_table.alias("target")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _merge_with_sql(
    spark: SparkSession,
    *,
    source_df: DataFrame,
    target_table: str,
    merge_condition: str,
) -> None:
    temp_view = "__incremental_merge_source"
    source_df.createOrReplaceTempView(temp_view)
    source_columns = source_df.columns

    update_set = ", ".join(
        [
            f"target.{_quote_identifier(column)} = source.{_quote_identifier(column)}"
            for column in source_columns
        ]
    )
    insert_columns = ", ".join([_quote_identifier(column) for column in source_columns])
    insert_values = ", ".join([f"source.{_quote_identifier(column)}" for column in source_columns])

    spark.sql(
        f"""
        MERGE INTO {target_table} AS target
        USING {temp_view} AS source
        ON {merge_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
        """
    )
    spark.catalog.dropTempView(temp_view)


def incremental_merge(
    spark: SparkSession,
    source_df: DataFrame,
    target_table: str,
    primary_key: list[str],
    watermark_column: str,
    late_arrival_days: int,
) -> dict[str, Any]:
    if not primary_key:
        raise ValueError("primary_key must contain at least one column")
    if not watermark_column:
        raise ValueError("watermark_column must be provided")
    if late_arrival_days < 0:
        raise ValueError("late_arrival_days must be >= 0")

    _validate_columns(source_df, primary_key + [watermark_column], label="source_df")

    table_exists = spark.catalog.tableExists(target_table)
    max_watermark_target = None
    lower_bound = None
    filtered_source_df = source_df

    if table_exists:
        target_df = spark.table(target_table)
        _validate_columns(
            target_df,
            primary_key + [watermark_column],
            label=f"target_table='{target_table}'",
        )
        max_watermark_target = target_df.select(
            F.max(F.col(watermark_column)).alias("max_watermark")
        ).collect()[0]["max_watermark"]
        if max_watermark_target is not None:
            lower_bound = _compute_lower_bound(
                spark,
                max_watermark_target=max_watermark_target,
                late_arrival_days=late_arrival_days,
            )
            filtered_source_df = source_df.filter(
                F.to_date(F.col(watermark_column)) >= F.lit(lower_bound)
            )

    rows_source_filtered = filtered_source_df.count()
    deduped_source_df = _deduplicate_latest(
        filtered_source_df,
        primary_key=primary_key,
        watermark_column=watermark_column,
    )
    rows_deduped = deduped_source_df.count()

    if not table_exists:
        write_delta_table_safe(
            spark,
            table_name=target_table,
            source_df=deduped_source_df,
            mode="overwrite",
            overwrite_schema=True,
        )
        merged_estimate = rows_deduped
    elif rows_deduped == 0:
        merged_estimate = 0
    else:
        _add_missing_target_columns(
            spark,
            target_table=target_table,
            source_df=deduped_source_df,
        )
        merge_condition = _build_merge_condition(primary_key)
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        if DeltaTable is not None:
            _merge_with_delta_api(
                spark,
                source_df=deduped_source_df,
                target_table=target_table,
                merge_condition=merge_condition,
            )
        else:
            _merge_with_sql(
                spark,
                source_df=deduped_source_df,
                target_table=target_table,
                merge_condition=merge_condition,
            )
        merged_estimate = rows_deduped

    return {
        "rows_source_filtered": rows_source_filtered,
        "rows_deduped": rows_deduped,
        "max_watermark_target": str(max_watermark_target)
        if max_watermark_target is not None
        else None,
        "lower_bound": str(lower_bound) if lower_bound is not None else None,
        "rows_merged_estimate": merged_estimate,
        "target_table_exists_before_merge": table_exists,
    }
