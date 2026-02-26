from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _quote_table_name(table_name: str) -> str:
    if "." not in table_name:
        return _quote_identifier(table_name)
    namespace, table = table_name.split(".", 1)
    return f"{_quote_identifier(namespace)}.{_quote_identifier(table)}"


def ensure_table_namespace(spark: SparkSession, table_name: str) -> None:
    if "." not in table_name:
        return
    namespace, _ = table_name.split(".", 1)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(namespace)}")


def drop_table_if_exists(spark: SparkSession, table_name: str) -> None:
    ensure_table_namespace(spark, table_name)
    spark.sql(f"DROP TABLE IF EXISTS {_quote_table_name(table_name)}")


def is_schema_conflict_error(exc: Exception) -> bool:
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


def _schema_signature(df: DataFrame) -> list[tuple[str, str]]:
    return [(field.name, field.dataType.simpleString()) for field in df.schema.fields]


def table_schema_differs(
    spark: SparkSession,
    *,
    table_name: str,
    expected_df: DataFrame,
) -> bool:
    if not spark.catalog.tableExists(table_name):
        return False

    table_signature = _schema_signature(spark.table(table_name))
    expected_signature = _schema_signature(expected_df)
    return table_signature != expected_signature


def write_delta_table_safe(
    spark: SparkSession,
    *,
    table_name: str,
    source_df: DataFrame,
    mode: str = "overwrite",
    overwrite_schema: bool = False,
    force_recreate: bool = False,
    recreate_on_schema_mismatch: bool = False,
    recreate_on_schema_conflict: bool = False,
) -> None:
    if mode not in {"overwrite", "append"}:
        raise ValueError(f"Unsupported write mode: {mode}")

    ensure_table_namespace(spark, table_name)
    table_exists = spark.catalog.tableExists(table_name)

    if force_recreate and table_exists:
        print(f"Recreating {table_name} before write to avoid metastore schema drift")
        drop_table_if_exists(spark, table_name)
        table_exists = False
    elif recreate_on_schema_mismatch and table_exists and table_schema_differs(
        spark,
        table_name=table_name,
        expected_df=source_df,
    ):
        print(f"Schema mismatch detected for {table_name}; recreating table")
        drop_table_if_exists(spark, table_name)
        table_exists = False

    effective_mode = mode
    if effective_mode == "append" and not table_exists:
        effective_mode = "overwrite"

    writer = source_df.write.format("delta").mode(effective_mode)
    if overwrite_schema and effective_mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")

    try:
        writer.saveAsTable(table_name)
    except Exception as exc:
        if not recreate_on_schema_conflict or not is_schema_conflict_error(exc):
            raise

        print(f"Schema conflict detected for {table_name}; dropping and recreating table")
        drop_table_if_exists(spark, table_name)
        (
            source_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(table_name)
        )
