from __future__ import annotations

import argparse
import shutil
import sys
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pipelines.common.local_delta_writer import (  # noqa: E402
    is_schema_conflict_error,
    write_delta_table_safe,
)
from pipelines.common.schema_normalizer import normalize_bronze_schema  # noqa: E402
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
ALL_SCHEMAS = ["bronze", "silver", "gold", "quality"]

DEFAULT_WAREHOUSE_DIR = REPO_ROOT / ".local" / "spark-warehouse"
DEFAULT_METASTORE_DIR = REPO_ROOT / ".local" / "metastore_db"
DEFAULT_SPARK_LOCAL_DIR = REPO_ROOT / ".local" / "spark-local"
DEFAULT_DOWNLOAD_DIR = REPO_ROOT / "data" / "raw"
LOCAL_METASTORE_WARN_NOTE = (
    "Local note: WARN messages about Hive SerDe compatibility for Delta tables are expected when "
    "using Spark + Delta + embedded Hive metastore."
)


@dataclass(frozen=True)
class LocalSparkPaths:
    warehouse_dir: Path
    metastore_dir: Path
    spark_local_dir: Path


def _quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def _resolve_relative_path(path_value: str) -> Path:
    candidate = Path(path_value).expanduser()
    if candidate.is_absolute():
        return candidate
    return (REPO_ROOT / candidate).resolve()


def _resolve_spark_paths(*, warehouse_dir: str | None) -> LocalSparkPaths:
    resolved_warehouse = (
        _resolve_relative_path(warehouse_dir) if warehouse_dir else DEFAULT_WAREHOUSE_DIR
    )
    return LocalSparkPaths(
        warehouse_dir=resolved_warehouse,
        metastore_dir=DEFAULT_METASTORE_DIR,
        spark_local_dir=DEFAULT_SPARK_LOCAL_DIR,
    )


def _prepare_spark_paths(paths: LocalSparkPaths) -> None:
    paths.warehouse_dir.mkdir(parents=True, exist_ok=True)
    paths.spark_local_dir.mkdir(parents=True, exist_ok=True)
    paths.metastore_dir.parent.mkdir(parents=True, exist_ok=True)

    # Derby creates metastore_db. If a plain directory was left behind, recreate it safely.
    if paths.metastore_dir.exists() and not (paths.metastore_dir / "service.properties").exists():
        shutil.rmtree(paths.metastore_dir, ignore_errors=True)


def _is_metastore_startup_error(exc: Exception) -> bool:
    error_text = str(exc).lower()
    markers = [
        "failed to start database",
        "failed initialising database",
        "unable to instantiate",
        "sessionhivemetastoreclient",
        "xbm0u",
        "xj040",
    ]
    return any(marker in error_text for marker in markers)


def _stop_spark_session(spark: SparkSession) -> None:
    try:
        spark.stop()
    except Exception:
        pass

    clear_active = getattr(SparkSession, "clearActiveSession", None)
    if callable(clear_active):
        try:
            clear_active()
        except Exception:
            pass

    clear_default = getattr(SparkSession, "clearDefaultSession", None)
    if callable(clear_default):
        try:
            clear_default()
        except Exception:
            pass


def _validate_metastore_access(spark: SparkSession) -> None:
    spark.sql("SHOW DATABASES").limit(1).collect()


def _build_spark_session(*, paths: LocalSparkPaths) -> SparkSession:
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
        .config("spark.sql.warehouse.dir", str(paths.warehouse_dir.resolve()))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={paths.metastore_dir.resolve()};create=true",
        )
        .config("spark.local.dir", str(paths.spark_local_dir.resolve()))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )

    def _create_session() -> SparkSession:
        return configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()

    for attempt in range(2):
        _prepare_spark_paths(paths)
        spark: SparkSession | None = None

        try:
            spark = _create_session()
            _validate_metastore_access(spark)
            spark.sparkContext.setLogLevel("WARN")
            return spark
        except Exception as exc:
            if spark is not None:
                _stop_spark_session(spark)

            if not _is_metastore_startup_error(exc) or attempt == 1:
                raise

            print(
                "Metastore startup error detected; recreating .local/metastore_db and retrying once"
            )
            shutil.rmtree(paths.metastore_dir, ignore_errors=True)

    raise RuntimeError("Spark session initialization failed")


def _quote_table_name(full_table_name: str) -> str:
    if "." not in full_table_name:
        return _quote_identifier(full_table_name)
    namespace, table = full_table_name.split(".", 1)
    return f"{_quote_identifier(namespace)}.{_quote_identifier(table)}"


def _ensure_schema_exists(spark: SparkSession, schema_name: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {_quote_identifier(schema_name)}")


def _drop_schema_if_exists(spark: SparkSession, schema_name: str, *, cascade: bool) -> None:
    cascade_clause = " CASCADE" if cascade else ""
    spark.sql(f"DROP DATABASE IF EXISTS {_quote_identifier(schema_name)}{cascade_clause}")


def _drop_table_if_exists(spark: SparkSession, full_table_name: str) -> None:
    if "." in full_table_name:
        namespace, _ = full_table_name.split(".", 1)
        _ensure_schema_exists(spark, namespace)
    spark.sql(f"DROP TABLE IF EXISTS {_quote_table_name(full_table_name)}")


def _drop_tables(spark: SparkSession, tables: Iterable[str]) -> None:
    for table in tables:
        _drop_table_if_exists(spark, table)


def _cleanup_quality_warehouse_dirs(*, paths: LocalSparkPaths) -> None:
    for quality_path in paths.warehouse_dir.glob("quality.db*"):
        if quality_path.is_dir():
            shutil.rmtree(quality_path, ignore_errors=True)


def _validate_month_args(*, year: int | None, month: int | None) -> None:
    if year is None and month is None:
        return
    if year is None or month is None:
        raise ValueError("Both --year and --month must be provided together")
    if month < 1 or month > 12:
        raise ValueError("--month must be between 1 and 12")


def _build_tlc_filename(*, year: int, month: int) -> str:
    return f"yellow_tripdata_{year:04d}-{month:02d}.parquet"


def _build_tlc_url(*, year: int, month: int) -> str:
    filename = _build_tlc_filename(year=year, month=month)
    return f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"


def _download_parquet(*, year: int, month: int, output_dir: str) -> Path:
    _validate_month_args(year=year, month=month)
    target_dir = _resolve_relative_path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    filename = _build_tlc_filename(year=year, month=month)
    target_file = target_dir / filename

    if target_file.exists() and target_file.stat().st_size > 0:
        print("Parquet already exists:", target_file)
        return target_file

    url = _build_tlc_url(year=year, month=month)
    print("Downloading:", url)
    print("Target file:", target_file)

    try:
        urllib.request.urlretrieve(url, target_file)
    except Exception:
        if target_file.exists():
            target_file.unlink(missing_ok=True)
        raise

    if not target_file.exists() or target_file.stat().st_size <= 0:
        raise ValueError(f"Download failed or empty file: {target_file}")

    print("Download completed:", target_file)
    print("File size (bytes):", target_file.stat().st_size)
    return target_file


def _default_parquet_candidates(*, year: int, month: int) -> list[Path]:
    filename = _build_tlc_filename(year=year, month=month)
    return [REPO_ROOT / filename, REPO_ROOT / "data" / "raw" / filename]


def _build_parquet_error_message(
    *,
    input_parquet: str | None,
    year: int | None,
    month: int | None,
    searched_candidates: list[Path],
) -> str:
    lines = ["Unable to locate a valid NYC TLC parquet input file."]
    rerun_step = 3

    if input_parquet:
        lines.append(f"Provided --input-parquet was not found or is empty: {input_parquet}")

    if searched_candidates:
        lines.append("Checked default locations:")
        for candidate in searched_candidates:
            lines.append(f"- {candidate}")

    lines.append("Next steps:")
    if year is not None and month is not None:
        lines.append(
            "1) Download automatically: "
            f"python orchestration/local/run_pipeline.py download --year {year} --month {month}"
        )
        lines.append(
            f'2) Find manually: find ~/ -name "yellow_tripdata_{year:04d}-{month:02d}.parquet"'
        )
    else:
        lines.append("1) Provide both --year and --month for auto-discovery/download")
        lines.append(
            "2) Download automatically: "
            "python orchestration/local/run_pipeline.py download --year <YEAR> --month <MONTH>"
        )
        lines.append('3) Find manually: find ~/ -name "yellow_tripdata_*.parquet"')
        rerun_step = 4

    lines.append(
        f"{rerun_step}) Re-run with explicit path: "
        "python orchestration/local/run_pipeline.py run-all "
        "--input-parquet <path> --year <YEAR> --month <MONTH>"
    )

    return "\n".join(lines)


def _resolve_input_parquet(
    *,
    input_parquet: str | None,
    year: int | None,
    month: int | None,
) -> Path:
    searched_candidates: list[Path] = []

    if input_parquet:
        provided = _resolve_relative_path(input_parquet)
        if provided.exists() and provided.is_file() and provided.stat().st_size > 0:
            return provided
        message = _build_parquet_error_message(
            input_parquet=input_parquet,
            year=year,
            month=month,
            searched_candidates=searched_candidates,
        )
        raise FileNotFoundError(message)

    _validate_month_args(year=year, month=month)
    if year is None or month is None:
        message = _build_parquet_error_message(
            input_parquet=None,
            year=year,
            month=month,
            searched_candidates=searched_candidates,
        )
        raise FileNotFoundError(message)

    for candidate in _default_parquet_candidates(year=year, month=month):
        resolved_candidate = candidate.resolve()
        searched_candidates.append(resolved_candidate)
        if (
            resolved_candidate.exists()
            and resolved_candidate.is_file()
            and resolved_candidate.stat().st_size > 0
        ):
            print("Using discovered parquet:", resolved_candidate)
            return resolved_candidate

    message = _build_parquet_error_message(
        input_parquet=None,
        year=year,
        month=month,
        searched_candidates=searched_candidates,
    )
    raise FileNotFoundError(message)


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
    input_parquet: Path,
    year: int | None,
    month: int | None,
    reset: bool,
) -> None:
    if reset:
        _drop_tables(spark, [BRONZE_TABLE])

    raw_df = spark.read.parquet(str(input_parquet.resolve()))
    canonical_df, schema_report = normalize_bronze_schema(raw_df)
    casted_timestamp_ntz_columns = schema_report.casted_timestamp_ntz_columns
    casted_fixed_columns = schema_report.casted_fixed_columns
    if casted_timestamp_ntz_columns:
        print(
            "Cast timestamp_ntz columns to timestamp for local Hive metastore compatibility:"
            f" {len(casted_timestamp_ntz_columns)} columns -> "
            f"{', '.join(casted_timestamp_ntz_columns)}"
        )

    if casted_fixed_columns:
        print(
            "Applied deterministic Bronze column casts for rerun stability:"
            f" {len(casted_fixed_columns)} columns -> "
            f"{', '.join(casted_fixed_columns)}"
        )

    filtered_df = _apply_optional_month_filter(canonical_df, year=year, month=month)

    mtime_utc = datetime.fromtimestamp(input_parquet.stat().st_mtime, tz=timezone.utc).replace(
        tzinfo=None
    )
    bronze_df = filtered_df.withColumn("ingest_ts", F.lit(mtime_utc).cast("timestamp")).withColumn(
        "source_file", F.lit(input_parquet.name)
    )

    write_delta_table_safe(
        spark,
        table_name=BRONZE_TABLE,
        source_df=bronze_df,
        mode="overwrite",
        overwrite_schema=True,
        force_recreate=True,
        recreate_on_schema_conflict=True,
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

    _ensure_schema_exists(spark, "quality")

    try:
        result = run_quality_gate(
            spark,
            silver_table=SILVER_TABLE,
            gold_table=GOLD_TABLE,
            output_table=VIOLATIONS_TABLE,
            thresholds={"max_invalid_ratio": max_invalid_ratio},
            strict=strict_quality,
        )
    except Exception as exc:
        if not is_schema_conflict_error(exc):
            raise

        print("Quality write conflict detected; dropping and recreating quality.violations_summary")
        _drop_table_if_exists(spark, VIOLATIONS_TABLE)
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


def _first_column_value(row) -> str:
    data = row.asDict(recursive=True)
    if data:
        return str(next(iter(data.values())))
    return str(row[0])


def inspect_catalog(spark: SparkSession, *, paths: LocalSparkPaths) -> None:
    print("Spark warehouse:", paths.warehouse_dir.resolve())
    print("Spark metastore:", paths.metastore_dir.resolve())
    print("Spark local dir:", paths.spark_local_dir.resolve())

    print("\nDatabases:")
    for row in spark.sql("SHOW DATABASES").collect():
        print("-", _first_column_value(row))

    for schema_name in ALL_SCHEMAS:
        print(f"\nTables in {schema_name}:")
        if not spark.catalog.databaseExists(schema_name):
            print("- <schema not found>")
            continue

        rows = spark.sql(f"SHOW TABLES IN {_quote_identifier(schema_name)}").collect()
        if not rows:
            print("- <no tables>")
            continue

        for row in rows:
            payload = row.asDict(recursive=True)
            table_name = payload.get("tableName") or payload.get("tablename")
            if table_name is None:
                table_name = row[1]
            is_temp = bool(payload.get("isTemporary", False))
            suffix = " (temp)" if is_temp else ""
            print(f"- {schema_name}.{table_name}{suffix}")


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
        default=str(DEFAULT_WAREHOUSE_DIR),
        help="Warehouse directory (default: .local/spark-warehouse)",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop stage tables before running the selected command",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Local orchestration CLI for NYC TLC Lakehouse")
    subparsers = parser.add_subparsers(dest="command", required=True)

    download_parser = subparsers.add_parser("download", help="Download NYC TLC yellow parquet")
    download_parser.add_argument("--year", type=int, required=True, help="Data year")
    download_parser.add_argument("--month", type=int, required=True, help="Data month (1-12)")
    download_parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_DOWNLOAD_DIR),
        help="Output directory (default: data/raw)",
    )

    inspect_parser = subparsers.add_parser(
        "inspect",
        help="Inspect databases/tables in local metastore",
    )
    inspect_parser.add_argument(
        "--warehouse-dir",
        type=str,
        default=str(DEFAULT_WAREHOUSE_DIR),
        help="Warehouse directory (default: .local/spark-warehouse)",
    )

    bronze_parser = subparsers.add_parser("run-bronze", help="Load parquet into bronze.events_raw")
    bronze_parser.add_argument(
        "--input-parquet",
        type=str,
        required=False,
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
        required=False,
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
    reset_parser.add_argument(
        "--drop-schemas",
        action="store_true",
        help="Drop bronze/silver/gold/quality schemas with CASCADE",
    )

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

    if args.command == "download":
        _download_parquet(year=args.year, month=args.month, output_dir=args.output_dir)
        return

    year = getattr(args, "year", None)
    month = getattr(args, "month", None)
    _validate_month_args(year=year, month=month)

    resolved_input_parquet: Path | None = None
    if args.command in {"run-bronze", "run-all"}:
        resolved_input_parquet = _resolve_input_parquet(
            input_parquet=getattr(args, "input_parquet", None),
            year=year,
            month=month,
        )

    paths = _resolve_spark_paths(warehouse_dir=getattr(args, "warehouse_dir", None))
    spark = _build_spark_session(paths=paths)
    print(LOCAL_METASTORE_WARN_NOTE)

    try:
        if args.command == "inspect":
            inspect_catalog(spark, paths=paths)
            return

        if args.command == "reset":
            if args.drop_schemas:
                for schema_name in ALL_SCHEMAS:
                    _drop_schema_if_exists(spark, schema_name, cascade=True)
                print("Dropped schemas with CASCADE:", ", ".join(ALL_SCHEMAS))
            else:
                _drop_tables(spark, ALL_TABLES)
                print("Dropped tables:", ", ".join(ALL_TABLES))
            return

        if args.command == "clean":
            _drop_tables(spark, QUALITY_TABLES)
            if args.remove_quality_warehouse:
                _cleanup_quality_warehouse_dirs(paths=paths)
            print("Dropped tables:", ", ".join(QUALITY_TABLES))
            print("remove_quality_warehouse:", bool(args.remove_quality_warehouse))
            return

        if args.command == "run-bronze":
            if resolved_input_parquet is None:
                raise ValueError("Resolved parquet path is required for run-bronze")
            _run_bronze(
                spark,
                input_parquet=resolved_input_parquet,
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
            if resolved_input_parquet is None:
                raise ValueError("Resolved parquet path is required for run-all")
            if args.reset:
                _drop_tables(spark, ALL_TABLES)

            _run_bronze(
                spark,
                input_parquet=resolved_input_parquet,
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
    try:
        main()
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc
