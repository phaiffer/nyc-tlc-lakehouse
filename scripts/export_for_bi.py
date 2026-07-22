"""Export gold and quality tables to single-file Parquet for BI tools (Power BI, etc.).

Reuses the same local Spark + embedded Hive metastore configuration as the
rest of the pipeline, so it reads directly from the existing .local/ warehouse
without re-running anything.

Usage:
    python scripts/export_for_bi.py
    python scripts/export_for_bi.py --warehouse-dir .local/spark-warehouse --output-dir exports
"""
import argparse
import os
import sys

# See orchestration/local/run_pipeline.py for why this is needed: on Windows,
# "python"/"python3" on PATH can resolve to the Microsoft Store stub, which
# silently kills any Spark Python worker (e.g. spawned by toPandas()/collect
# fallbacks) with no Python traceback. Pin workers to this interpreter.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

DEFAULT_WAREHOUSE = ".local/spark-warehouse"
DEFAULT_METASTORE = ".local/metastore_db"

# Tables read by the observability + business dashboards.
TABLES = [
    "gold.fct_trips_daily",
    "gold.dim_vendor",
    "gold.dim_payment_type",
    "gold.dim_rate_code",
    "quality.violations_summary",
    "quality.pipeline_metrics",
    "quality.drift_events",
    "quality.drift_baseline_metrics",
    "quality.quarantine_records",
]


def build_spark(warehouse_dir: str) -> SparkSession:
    metastore_dir = os.environ.get("SPARK_METASTORE_PATH", DEFAULT_METASTORE)
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    builder = (
        SparkSession.builder.appName("export-for-bi")
        # See orchestration/local/run_pipeline.py's _build_spark_session for
        # why this is set explicitly (default 1g heap in local mode is too
        # small once tables have real data and easily causes GC-thrashing
        # crashes, including phantom "Python worker exited unexpectedly"
        # errors during unrelated small operations; the IPv4 pin works
        # around a Windows localhost IPv4/IPv6 mismatch that breaks the
        # Python worker's callback socket to the JVM).
        .config("spark.driver.host", os.environ.get("SPARK_DRIVER_HOST", "127.0.0.1"))
        .config(
            "spark.driver.bindAddress", os.environ.get("SPARK_DRIVER_BIND_ADDRESS", "127.0.0.1")
        )
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "4g"))
        # This session reads Delta tables written by run_pipeline.py, so it needs the same
        # Delta wiring as _build_spark_session there: the SQL extension + catalog configs,
        # plus configure_spark_with_delta_pip(...) below to actually put the delta-spark jar
        # on the classpath. Without this, spark.table("gold.fct_trips_daily") etc. fail with
        # "[DATA_SOURCE_NOT_FOUND] Failed to find the data source: delta".
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        # Must be prefixed with "spark.hadoop." -- Spark silently ignores (and warns on) any
        # config key that doesn't start with "spark.", so the old bare "javax.jdo.option.
        # ConnectionURL" key here never actually took effect.
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_dir};create=true",
        )
    )
    return configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()


def export_tables(spark: SparkSession, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    for table in TABLES:
        try:
            df = spark.table(table)
        except Exception as exc:
            # Some tables (e.g. drift_baseline_metrics) only exist after
            # a second run once a baseline has been established.
            print(f"[export] skipping {table}: table not found ({exc})")
            continue

        row_count = df.count()
        target = os.path.join(output_dir, f"{table.replace('.', '_')}.parquet")
        print(f"[export] {table} ({row_count} rows) -> {target}")

        # Single-file parquet via pandas so Power BI's "Get Data > Parquet"
        # can point at one predictable filename instead of a Spark part-file
        # folder with a generated name.
        df.toPandas().to_parquet(target, index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--warehouse-dir", default=DEFAULT_WAREHOUSE)
    parser.add_argument("--output-dir", default="exports")
    args = parser.parse_args()

    spark = build_spark(args.warehouse_dir)
    try:
        export_tables(spark, args.output_dir)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()