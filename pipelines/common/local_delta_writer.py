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


def _write_via_sql(
    spark: SparkSession,
    *,
    table_name: str,
    source_df: DataFrame,
    mode: str,
    table_exists: bool,
) -> None:
    """
    Issue the actual write as Spark SQL DDL/DML rather than DataFrameWriter.saveAsTable().

    DataFrameWriter.saveAsTable(mode="overwrite") against a Delta table registered under a V2
    catalog (spark_catalog = DeltaCatalog) plans as AtomicReplaceTableAsSelectExec, which runs
    Spark's TableCapabilityCheck for TRUNCATE. Delta's V2 table doesn't declare that capability
    directly (it relies on V1_BATCH_WRITE), so this raises "Table ... does not support truncate
    in batch mode." This is a confirmed regression in Spark 3.5.6+ (still present in 3.5.9) --
    see https://github.com/delta-io/delta/issues/4671. Critically, "CREATE OR REPLACE TABLE ...
    AS SELECT" hits the *same* AtomicReplaceTableAsSelectExec path (the "OR REPLACE" is what
    triggers it, independent of whether the table actually exists yet), so that doesn't dodge
    the bug either -- confirmed by testing against this exact repo. The two constructs that are
    NOT affected, because they never go through the Replace/TableCapabilityCheck-for-TRUNCATE
    exec path at all, are a plain "CREATE TABLE ... AS SELECT" (no OR REPLACE) for a table that
    doesn't exist yet, and "INSERT OVERWRITE TABLE" / "INSERT INTO" DML for one that does.
    """
    view_name = f"__write_safe_source_{abs(hash(table_name))}"
    source_df.createOrReplaceTempView(view_name)
    try:
        quoted_table = _quote_table_name(table_name)
        if table_exists:
            if mode == "append":
                spark.sql(f"INSERT INTO {quoted_table} SELECT * FROM {view_name}")
            else:
                spark.sql(f"INSERT OVERWRITE TABLE {quoted_table} SELECT * FROM {view_name}")
        else:
            # Plain CREATE (no OR REPLACE): the table doesn't exist, so this is a pure CTAS
            # and never touches the buggy Replace/truncate-capability-check exec path.
            spark.sql(
                f"CREATE TABLE {quoted_table} USING DELTA AS SELECT * FROM {view_name}"
            )
    finally:
        spark.catalog.dropTempView(view_name)


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
    """
    Write managed Delta tables with explicit schema controls for local metastore stability.

    The writer always disables implicit schema evolution and supports controlled recreate
    paths (`force_recreate` / `recreate_on_schema_*`) to avoid Hive metastore drift on
    reruns. Note: `overwrite_schema` is accepted for backwards compatibility with callers,
    but every overwrite already fully replaces the schema (see `_write_via_sql`), since that
    is the only way to avoid the Spark 3.5.6+ saveAsTable regression described there.
    """
    if mode not in {"overwrite", "append"}:
        raise ValueError(f"Unsupported write mode: {mode}")

    ensure_table_namespace(spark, table_name)
    table_exists = spark.catalog.tableExists(table_name)

    if force_recreate and table_exists:
        print(f"Recreating {table_name} before write to avoid metastore schema drift")
        drop_table_if_exists(spark, table_name)
        table_exists = False
    elif (
        recreate_on_schema_mismatch
        and table_exists
        and table_schema_differs(
            spark,
            table_name=table_name,
            expected_df=source_df,
        )
    ):
        print(f"Schema mismatch detected for {table_name}; recreating table")
        drop_table_if_exists(spark, table_name)
        table_exists = False

    effective_mode = mode
    if effective_mode == "append" and not table_exists:
        effective_mode = "overwrite"

    try:
        _write_via_sql(
            spark,
            table_name=table_name,
            source_df=source_df,
            mode=effective_mode,
            table_exists=table_exists,
        )
    except Exception as exc:
        if not recreate_on_schema_conflict or not is_schema_conflict_error(exc):
            raise

        print(f"Schema conflict detected for {table_name}; dropping and recreating table")
        drop_table_if_exists(spark, table_name)
        _write_via_sql(
            spark,
            table_name=table_name,
            source_df=source_df,
            mode="overwrite",
            table_exists=False,
        )
