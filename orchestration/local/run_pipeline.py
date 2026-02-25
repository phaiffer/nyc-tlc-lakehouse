from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.gold_marts.run_gold_enforced import run_gold_enforced  # noqa: E402
from pipelines.silver_transform.run_silver_enforced import run_silver_enforced  # noqa: E402
from quality.validators.quality_gate import run_quality_gate  # noqa: E402

BRONZE_TABLE = "bronze.events_raw"
SILVER_TABLE = "silver.trips_clean"
GOLD_TABLE = "gold.fct_trips_daily"
QUARANTINE_TABLE = "quality.quarantine_records"
METRICS_TABLE = "quality.pipeline_metrics"
VIOLATIONS_TABLE = "quality.violations_summary"

QUALITY_TABLES = [QUARANTINE_TABLE, METRICS_TABLE, VIOLATIONS_TABLE]
ALL_TABLES = [
    BRONZE_TABLE,
    SILVER_TABLE,
    GOLD_TABLE,
    QUARANTINE_TABLE,
    METRICS_TABLE,
    VIOLATIONS_TABLE,
]


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _ensure_database_for_table(spark: SparkSession, table_name: str) -> None:
    if "." not in table_name:
        return
    namespace, _ = table_name.split(".", 1)
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(namespace)}")


def _build_spark_session(*, warehouse_dir: str | None) -> SparkSession:
    local_root = REPO_ROOT / ".local"
    warehouse_path = (
        Path(warehouse_dir).expanduser() if warehouse_dir else local_root / "spark-warehouse"
    )
    metastore_path = local_root / "metastore_db"
    spark_local_dir = local_root / "spark-local"

    warehouse_path.mkdir(parents=True, exist_ok=True)
    spark_local_dir.mkdir(parents=True, exist_ok=True)

    # If a plain directory was created previously, Derby cannot initialize.
    if metastore_path.exists() and not (metastore_path / "service.properties").exists():
        shutil.rmtree(metastore_path, ignore_errors=True)

    builder = (
        SparkSession.builder.appName("nyc-tlc-lakehouse-local")
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .config("spark.sql.warehouse.dir", str(warehouse_path.resolve()))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_path.resolve()};create=true",
        )
        .config("spark.local.dir", str(spark_local_dir.resolve()))
        .config("spark.sql.shuffle.partitions", "8")
    )

    spark = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _drop_tables(spark: SparkSession, tables: Iterable[str]) -> None:
    for table in tables:
        _ensure_database_for_table(spark, table)
        spark.sql(f"DROP TABLE IF EXISTS {table}")


def _cleanup_quality_warehouse_dirs(*, warehouse_dir: str | None) -> None:
    local_root = REPO_ROOT / ".local"
    warehouse_path = (
        Path(warehouse_dir).expanduser() if warehouse_dir else local_root / "spark-warehouse"
    )
    for quality_path in warehouse_path.glob("quality.db*"):
        if quality_path.is_dir():
            shutil.rmtree(quality_path, ignore_errors=True)


def _validate_month_args(*, year: int | None, month: int | None) -> None:
    if year is None and month is None:
        return
    if year is None or month is None:
        raise ValueError("Both --year and --month must be provided together")
    if month < 1 or month > 12:
        raise ValueError("--month must be between 1 and 12")


def _apply_optional_month_filter(df, *, year: int | None, month: int | None):
    _validate_month_args(year=year, month=month)
    if year is None or month is None:
        return df

    window_start = F.to_timestamp(F.lit(f"{year:04d}-{month:02d}-01 00:00:00"))
    window_end = F.add_months(window_start, 1)

    if "tpep_pickup_datetime" not in df.columns:
        raise ValueError(
            "Input parquet does not contain 'tpep_pickup_datetime' required for "
            "month window filtering"
        )

    return df.filter(
        (F.col("tpep_pickup_datetime") >= window_start)
        & (F.col("tpep_pickup_datetime") < window_end)
    )


def _run_bronze(
    spark: SparkSession,
    *,
    input_parquet: str,
    year: int | None,
    month: int | None,
    reset: bool,
) -> None:
    input_path = Path(input_parquet).expanduser()
    if not input_path.exists():
        raise FileNotFoundError(f"Input parquet not found: {input_path}")

    if reset:
        _drop_tables(spark, [BRONZE_TABLE])

    raw_df = spark.read.parquet(str(input_path.resolve()))
    filtered_df = _apply_optional_month_filter(raw_df, year=year, month=month)

    mtime_utc = datetime.fromtimestamp(input_path.stat().st_mtime, tz=timezone.utc).replace(
        tzinfo=None
    )
    bronze_df = (
        filtered_df.withColumn("ingest_ts", F.lit(mtime_utc).cast("timestamp"))
        .withColumn("source_file", F.lit(input_path.name))
    )

    _ensure_database_for_table(spark, BRONZE_TABLE)
    bronze_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        BRONZE_TABLE
    )

    print("Bronze table ready:", BRONZE_TABLE)
    print("Bronze rows:", spark.table(BRONZE_TABLE).count())


def _run_silver(
    spark: SparkSession,
    *,
    max_invalid_ratio: float,
    year: int | None,
    month: int | None,
    reset: bool,
) -> None:
    if reset:
        _drop_tables(spark, [SILVER_TABLE, QUARANTINE_TABLE, METRICS_TABLE])

    run_silver_enforced(
        spark=spark,
        repo_root=str(REPO_ROOT),
        contract_dataset=SILVER_TABLE,
        input_table=BRONZE_TABLE,
        output_table=SILVER_TABLE,
        quarantine_table=QUARANTINE_TABLE,
        max_invalid_ratio=max_invalid_ratio,
        metrics_table=METRICS_TABLE,
        year=year,
        month=month,
    )

    print("Silver table ready:", SILVER_TABLE)
    print("Silver rows:", spark.table(SILVER_TABLE).count())


def _run_gold(
    spark: SparkSession,
    *,
    max_invalid_ratio: float,
    year: int | None,
    month: int | None,
    reset: bool,
) -> None:
    if reset:
        _drop_tables(spark, [GOLD_TABLE, QUARANTINE_TABLE, METRICS_TABLE])

    run_gold_enforced(
        spark=spark,
        repo_root=str(REPO_ROOT),
        contract_dataset=GOLD_TABLE,
        input_table=SILVER_TABLE,
        output_table=GOLD_TABLE,
        quarantine_table=QUARANTINE_TABLE,
        metrics_table=METRICS_TABLE,
        max_invalid_ratio=max_invalid_ratio,
        year=year,
        month=month,
    )

    print("Gold table ready:", GOLD_TABLE)
    print("Gold rows:", spark.table(GOLD_TABLE).count())


def _run_quality(
    spark: SparkSession,
    *,
    max_invalid_ratio: float,
    strict_quality: bool,
    reset: bool,
) -> None:
    if reset:
        _drop_tables(spark, [VIOLATIONS_TABLE])

    result = run_quality_gate(
        spark,
        silver_table=SILVER_TABLE,
        gold_table=GOLD_TABLE,
        output_table=VIOLATIONS_TABLE,
        thresholds={"max_invalid_ratio": max_invalid_ratio},
        strict=strict_quality,
    )

    print("Quality table ready:", VIOLATIONS_TABLE)
    print("Quality rows:", spark.table(VIOLATIONS_TABLE).count())
    print("Quality summary:", result)


def _add_common_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Optional year for month-window filtering",
    )
    parser.add_argument(
        "--month",
        type=int,
        default=None,
        help="Optional month (1-12) for month-window filtering",
    )
    parser.add_argument(
        "--max-invalid-ratio",
        type=float,
        default=0.001,
        help="Maximum allowed invalid ratio for contract enforcement / quality thresholds",
    )
    parser.add_argument(
        "--warehouse-dir",
        type=str,
        default=None,
        help="Optional warehouse directory (default: .local/spark-warehouse)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop stage tables before running the selected command",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local orchestration CLI for NYC TLC Lakehouse")
    subparsers = parser.add_subparsers(dest="command", required=True)

    bronze_parser = subparsers.add_parser("run-bronze", help="Load parquet into bronze.events_raw")
    bronze_parser.add_argument(
        "--input-parquet",
        type=str,
        required=True,
        help="Local parquet path for TLC source data",
    )
    _add_common_options(bronze_parser)

    silver_parser = subparsers.add_parser("run-silver", help="Run Silver enforcement pipeline")
    _add_common_options(silver_parser)

    gold_parser = subparsers.add_parser("run-gold", help="Run Gold KPI incremental pipeline")
    _add_common_options(gold_parser)

    quality_parser = subparsers.add_parser(
        "run-quality",
        help="Run quality gate and persist violations",
    )
    _add_common_options(quality_parser)
    quality_parser.add_argument(
        "--strict-quality",
        action="store_true",
        help="Fail when any quality-gate threshold is exceeded",
    )

    run_all_parser = subparsers.add_parser(
        "run-all",
        help="Execute bronze -> silver -> gold -> quality",
    )
    run_all_parser.add_argument(
        "--input-parquet",
        type=str,
        required=True,
        help="Local parquet path for TLC source data",
    )
    _add_common_options(run_all_parser)
    run_all_parser.add_argument(
        "--strict-quality",
        action="store_true",
        help="Fail when any quality-gate threshold is exceeded",
    )

    reset_parser = subparsers.add_parser("reset", help="Drop bronze/silver/gold/quality tables")
    _add_common_options(reset_parser)

    clean_parser = subparsers.add_parser(
        "clean",
        help="Drop quality tables and optionally clean quality warehouse directories",
    )
    _add_common_options(clean_parser)
    clean_parser.add_argument(
        "--remove-quality-warehouse",
        action="store_true",
        help="Remove quality warehouse directories under the configured spark warehouse path",
    )

    return parser


def main() -> None:
    args = _build_parser().parse_args()
    _validate_month_args(year=args.year, month=args.month)

    spark = _build_spark_session(warehouse_dir=args.warehouse_dir)

    try:
        if args.command == "reset":
            _drop_tables(spark, ALL_TABLES)
            print("Dropped tables:", ", ".join(ALL_TABLES))
            return

        if args.command == "clean":
            _drop_tables(spark, QUALITY_TABLES)
            if args.remove_quality_warehouse:
                _cleanup_quality_warehouse_dirs(warehouse_dir=args.warehouse_dir)
            print("Dropped tables:", ", ".join(QUALITY_TABLES))
            print("remove_quality_warehouse:", bool(args.remove_quality_warehouse))
            return

        if args.command == "run-bronze":
            _run_bronze(
                spark,
                input_parquet=args.input_parquet,
                year=args.year,
                month=args.month,
                reset=args.reset,
            )
            return

        if args.command == "run-silver":
            _run_silver(
                spark,
                max_invalid_ratio=args.max_invalid_ratio,
                year=args.year,
                month=args.month,
                reset=args.reset,
            )
            return

        if args.command == "run-gold":
            _run_gold(
                spark,
                max_invalid_ratio=args.max_invalid_ratio,
                year=args.year,
                month=args.month,
                reset=args.reset,
            )
            return

        if args.command == "run-quality":
            _run_quality(
                spark,
                max_invalid_ratio=args.max_invalid_ratio,
                strict_quality=args.strict_quality,
                reset=args.reset,
            )
            return

        if args.command == "run-all":
            if args.reset:
                _drop_tables(spark, ALL_TABLES)

            _run_bronze(
                spark,
                input_parquet=args.input_parquet,
                year=args.year,
                month=args.month,
                reset=False,
            )
            _run_silver(
                spark,
                max_invalid_ratio=args.max_invalid_ratio,
                year=args.year,
                month=args.month,
                reset=False,
            )
            _run_gold(
                spark,
                max_invalid_ratio=args.max_invalid_ratio,
                year=args.year,
                month=args.month,
                reset=False,
            )
            _run_quality(
                spark,
                max_invalid_ratio=args.max_invalid_ratio,
                strict_quality=args.strict_quality,
                reset=False,
            )
            return

        raise ValueError(f"Unsupported command: {args.command}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
