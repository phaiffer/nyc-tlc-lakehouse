from __future__ import annotations

import argparse
import shutil
import sys
import time
import urllib.request
import uuid
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

from orchestration.local.backfill_utils import (  # noqa: E402
    checkpoint_path,
    find_resume_checkpoint,
    initialize_checkpoint_payload,
    iter_month_range,
    load_checkpoint,
    month_token,
    next_incomplete_month,
    parse_month_token,
    purge_backfill_checkpoints,
    resolve_backfill_range,
    utc_now_iso,
    write_checkpoint,
)
from pipelines.common.local_delta_writer import (  # noqa: E402
    is_schema_conflict_error,
    write_delta_table_safe,
)
from pipelines.common.schema_normalizer import normalize_bronze_schema  # noqa: E402
from pipelines.common.structured_logging import JsonPipelineLogger  # noqa: E402
from pipelines.gold_marts.run_gold_enforced import run_gold_enforced  # noqa: E402
from pipelines.silver_transform.run_silver_enforced import run_silver_enforced  # noqa: E402
from quality.observability.drift_detector import detect_and_record_drift  # noqa: E402
from quality.validators.contract_loader import load_contract_by_dataset  # noqa: E402
from quality.validators.quality_gate import run_quality_gate  # noqa: E402

BRONZE_TABLE = "bronze.events_raw"
SILVER_TABLE = "silver.trips_clean"
GOLD_TABLE = "gold.fct_trips_daily"
DIM_VENDOR_TABLE = "gold.dim_vendor"
DIM_PAYMENT_TYPE_TABLE = "gold.dim_payment_type"
DIM_RATE_CODE_TABLE = "gold.dim_rate_code"
QUARANTINE_TABLE = "quality.quarantine_records"
METRICS_TABLE = "quality.pipeline_metrics"
VIOLATIONS_TABLE = "quality.violations_summary"
DRIFT_EVENTS_TABLE = "quality.drift_events"
DRIFT_BASELINE_TABLE = "quality.drift_baseline_metrics"

QUALITY_TABLES = [
    QUARANTINE_TABLE,
    METRICS_TABLE,
    VIOLATIONS_TABLE,
    DRIFT_EVENTS_TABLE,
    DRIFT_BASELINE_TABLE,
]
ALL_TABLES = [
    BRONZE_TABLE,
    SILVER_TABLE,
    GOLD_TABLE,
    DIM_VENDOR_TABLE,
    DIM_PAYMENT_TYPE_TABLE,
    DIM_RATE_CODE_TABLE,
    QUARANTINE_TABLE,
    METRICS_TABLE,
    VIOLATIONS_TABLE,
    DRIFT_EVENTS_TABLE,
    DRIFT_BASELINE_TABLE,
]
ALL_SCHEMAS = ["bronze", "silver", "gold", "quality"]

DEFAULT_WAREHOUSE_DIR = REPO_ROOT / ".local" / "spark-warehouse"
DEFAULT_METASTORE_DIR = REPO_ROOT / ".local" / "metastore_db"
DEFAULT_SPARK_LOCAL_DIR = REPO_ROOT / ".local" / "spark-local"
DEFAULT_DOWNLOAD_DIR = REPO_ROOT / "data" / "raw"
DEFAULT_CHECKPOINT_DIR = REPO_ROOT / ".local" / "checkpoints"
DEFAULT_LOG_DIR = REPO_ROOT / ".local" / "logs"
LOCAL_METASTORE_WARN_NOTE = (
    "Local note: WARN messages about Hive SerDe compatibility for Delta tables are expected when "
    "using Spark + Delta + embedded Hive metastore."
)
STRICT_MODE_NOTE = (
    "Quality strict mode enabled: quality rules with severity=error fail the pipeline."
)
WARN_MODE_NOTE = (
    "Quality warn mode enabled: quality rules with severity=error are recorded without failing the "
    "run."
)


@dataclass(frozen=True)
class LocalSparkPaths:
    warehouse_dir: Path
    metastore_dir: Path
    spark_local_dir: Path


def _build_json_logger(*, run_id: str) -> JsonPipelineLogger:
    return JsonPipelineLogger(
        run_id=run_id,
        log_file=DEFAULT_LOG_DIR / f"pipeline_{run_id}.jsonl",
    )


def _log_event(
    logger: JsonPipelineLogger | None,
    *,
    level: str,
    stage: str,
    step: str,
    message: str,
    year: int | None = None,
    month: int | None = None,
    duration_ms: int | None = None,
    row_counts: dict[str, int] | None = None,
    invalid_ratio: float | None = None,
    exception: str | None = None,
) -> None:
    if logger is None:
        return
    logger.event(
        level=level,
        stage=stage,
        step=step,
        message=message,
        year=year,
        month=month,
        duration_ms=duration_ms,
        row_counts=row_counts,
        invalid_ratio=invalid_ratio,
        exception=exception,
    )


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
    run_id: str | None = None,
    logger: JsonPipelineLogger | None = None,
) -> None:
    stage_start = time.perf_counter()
    _log_event(
        logger,
        level="INFO",
        stage="bronze",
        step="start",
        message="Starting Bronze stage",
        year=year,
        month=month,
    )
    if reset:
        _drop_tables(spark, [BRONZE_TABLE])

    try:
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
        bronze_df = filtered_df.withColumn(
            "ingest_ts", F.lit(mtime_utc).cast("timestamp")
        ).withColumn("source_file", F.lit(input_parquet.name))

        write_delta_table_safe(
            spark,
            table_name=BRONZE_TABLE,
            source_df=bronze_df,
            mode="overwrite",
            overwrite_schema=True,
            force_recreate=True,
            recreate_on_schema_conflict=True,
        )

        bronze_rows = spark.table(BRONZE_TABLE).count()
        print("Bronze table ready:", BRONZE_TABLE)
        print("Bronze rows:", bronze_rows)
        _log_event(
            logger,
            level="INFO",
            stage="bronze",
            step="complete",
            message="Completed Bronze stage",
            year=year,
            month=month,
            duration_ms=int((time.perf_counter() - stage_start) * 1000),
            row_counts={"bronze_rows": int(bronze_rows)},
        )
    except Exception as exc:
        _log_event(
            logger,
            level="ERROR",
            stage="bronze",
            step="error",
            message="Bronze stage failed",
            year=year,
            month=month,
            duration_ms=int((time.perf_counter() - stage_start) * 1000),
            exception=str(exc),
        )
        raise


def _run_silver(
    spark: SparkSession,
    *,
    max_invalid_ratio: float,
    year: int | None,
    month: int | None,
    reset: bool,
    run_id: str | None = None,
    logger: JsonPipelineLogger | None = None,
) -> None:
    stage_start = time.perf_counter()
    _log_event(
        logger,
        level="INFO",
        stage="silver",
        step="start",
        message="Starting Silver stage",
        year=year,
        month=month,
        invalid_ratio=max_invalid_ratio,
    )
    if reset:
        _drop_tables(spark, [SILVER_TABLE, QUARANTINE_TABLE, METRICS_TABLE])

    try:
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
            run_id=run_id,
        )

        silver_rows = spark.table(SILVER_TABLE).count()
        print("Silver table ready:", SILVER_TABLE)
        print("Silver rows:", silver_rows)
        _log_event(
            logger,
            level="INFO",
            stage="silver",
            step="complete",
            message="Completed Silver stage",
            year=year,
            month=month,
            duration_ms=int((time.perf_counter() - stage_start) * 1000),
            row_counts={"silver_rows": int(silver_rows)},
            invalid_ratio=max_invalid_ratio,
        )
    except Exception as exc:
        _log_event(
            logger,
            level="ERROR",
            stage="silver",
            step="error",
            message="Silver stage failed",
            year=year,
            month=month,
            duration_ms=int((time.perf_counter() - stage_start) * 1000),
            invalid_ratio=max_invalid_ratio,
            exception=str(exc),
        )
        raise


def _run_gold(
    spark: SparkSession,
    *,
    max_invalid_ratio: float,
    year: int | None,
    month: int | None,
    reset: bool,
    run_id: str | None = None,
    logger: JsonPipelineLogger | None = None,
) -> None:
    stage_start = time.perf_counter()
    _log_event(
        logger,
        level="INFO",
        stage="gold",
        step="start",
        message="Starting Gold stage",
        year=year,
        month=month,
        invalid_ratio=max_invalid_ratio,
    )
    if reset:
        _drop_tables(
            spark,
            [
                GOLD_TABLE,
                DIM_VENDOR_TABLE,
                DIM_PAYMENT_TYPE_TABLE,
                DIM_RATE_CODE_TABLE,
                QUARANTINE_TABLE,
                METRICS_TABLE,
            ],
        )

    try:
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
            dim_vendor_table=DIM_VENDOR_TABLE,
            dim_payment_type_table=DIM_PAYMENT_TYPE_TABLE,
            dim_rate_code_table=DIM_RATE_CODE_TABLE,
            run_id=run_id,
        )

        gold_rows = spark.table(GOLD_TABLE).count()
        print("Gold table ready:", GOLD_TABLE)
        print("Gold rows:", gold_rows)
        _log_event(
            logger,
            level="INFO",
            stage="gold",
            step="complete",
            message="Completed Gold stage",
            year=year,
            month=month,
            duration_ms=int((time.perf_counter() - stage_start) * 1000),
            row_counts={"gold_rows": int(gold_rows)},
            invalid_ratio=max_invalid_ratio,
        )
    except Exception as exc:
        _log_event(
            logger,
            level="ERROR",
            stage="gold",
            step="error",
            message="Gold stage failed",
            year=year,
            month=month,
            duration_ms=int((time.perf_counter() - stage_start) * 1000),
            invalid_ratio=max_invalid_ratio,
            exception=str(exc),
        )
        raise


def _run_quality(
    spark: SparkSession,
    *,
    max_invalid_ratio: float,
    strict_quality: bool,
    year: int | None,
    month: int | None,
    reset: bool,
    run_id: str | None = None,
    logger: JsonPipelineLogger | None = None,
) -> None:
    stage_start = time.perf_counter()
    _log_event(
        logger,
        level="INFO",
        stage="quality",
        step="start",
        message="Starting Quality stage",
        year=year,
        month=month,
        invalid_ratio=max_invalid_ratio,
    )
    if reset:
        _drop_tables(spark, [VIOLATIONS_TABLE])

    print(STRICT_MODE_NOTE if strict_quality else WARN_MODE_NOTE)

    _ensure_schema_exists(spark, "quality")
    contract = load_contract_by_dataset(REPO_ROOT, SILVER_TABLE)
    quality_rule_configs = {
        str(rule.get("name")): {
            "rule_id": str(rule.get("rule_id", rule.get("name"))),
            "severity": str(rule.get("severity", "error")).lower(),
        }
        for rule in contract.expectations
        if str(rule.get("type", "")).strip().lower() in {"quality_gate", "quality_rule"}
    }
    effective_run_id = run_id or str(uuid.uuid4())
    run_ts = datetime.now(tz=timezone.utc).replace(tzinfo=None)

    try:
        result = run_quality_gate(
            spark,
            silver_table=SILVER_TABLE,
            gold_table=GOLD_TABLE,
            output_table=VIOLATIONS_TABLE,
            thresholds={"max_invalid_ratio": max_invalid_ratio},
            strict=strict_quality,
            dataset=contract.dataset,
            run_id=effective_run_id,
            run_ts=run_ts,
            window_year=year,
            window_month=month,
            contract_version=contract.version,
            rule_configs=quality_rule_configs,
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
            dataset=contract.dataset,
            run_id=effective_run_id,
            run_ts=run_ts,
            window_year=year,
            window_month=month,
            contract_version=contract.version,
            rule_configs=quality_rule_configs,
        )

    silver_drift_summary = detect_and_record_drift(
        spark,
        source_df=spark.table(SILVER_TABLE),
        dataset=SILVER_TABLE,
        run_id=effective_run_id,
        run_ts=run_ts,
        window_year=year,
        window_month=month,
        config_path=REPO_ROOT / "config" / "drift_thresholds.yml",
        drift_events_table=DRIFT_EVENTS_TABLE,
        baseline_table=DRIFT_BASELINE_TABLE,
        fare_column="fare_amount",
        distance_column="trip_distance",
        passenger_count_column="passenger_count",
    )
    gold_drift_summary = detect_and_record_drift(
        spark,
        source_df=spark.table(GOLD_TABLE),
        dataset=GOLD_TABLE,
        run_id=effective_run_id,
        run_ts=run_ts,
        window_year=year,
        window_month=month,
        config_path=REPO_ROOT / "config" / "drift_thresholds.yml",
        drift_events_table=DRIFT_EVENTS_TABLE,
        baseline_table=DRIFT_BASELINE_TABLE,
        fare_column="total_fare",
        distance_column="trip_distance",
        passenger_count_column="passenger_count",
        fare_metric_name="avg_fare_per_trip_ratio",
        fare_profile_key="avg_fare_per_trip",
        fare_numerator_column="total_fare",
        fare_denominator_column="trips",
        enabled_metrics=("volume_ratio", "avg_fare_per_trip_ratio"),
    )

    print("Quality table ready:", VIOLATIONS_TABLE)
    quality_rows = spark.table(VIOLATIONS_TABLE).count()
    print("Quality rows:", quality_rows)
    print("Quality summary:", result)
    print("Drift summary (silver):", silver_drift_summary)
    print("Drift summary (gold):", gold_drift_summary)
    _log_event(
        logger,
        level="INFO",
        stage="quality",
        step="complete",
        message="Completed Quality stage",
        year=year,
        month=month,
        duration_ms=int((time.perf_counter() - stage_start) * 1000),
        row_counts={"quality_rows": int(quality_rows)},
        invalid_ratio=max_invalid_ratio,
    )


def _run_all_month(
    spark: SparkSession,
    *,
    year: int | None,
    month: int | None,
    max_invalid_ratio: float,
    strict_quality: bool,
    input_parquet: Path | None = None,
    run_id: str | None = None,
    logger: JsonPipelineLogger | None = None,
) -> None:
    if input_parquet is None:
        if year is None or month is None:
            raise ValueError("Both --year and --month are required when --input-parquet is not set")
        resolved_input = _resolve_input_parquet(input_parquet=None, year=year, month=month)
    else:
        resolved_input = input_parquet
    _run_bronze(
        spark,
        input_parquet=resolved_input,
        year=year,
        month=month,
        reset=False,
        run_id=run_id,
        logger=logger,
    )
    _run_silver(
        spark,
        max_invalid_ratio=max_invalid_ratio,
        year=year,
        month=month,
        reset=False,
        run_id=run_id,
        logger=logger,
    )
    _run_gold(
        spark,
        max_invalid_ratio=max_invalid_ratio,
        year=year,
        month=month,
        reset=False,
        run_id=run_id,
        logger=logger,
    )
    try:
        _run_quality(
            spark,
            max_invalid_ratio=max_invalid_ratio,
            strict_quality=strict_quality,
            year=year,
            month=month,
            reset=False,
            run_id=run_id,
            logger=logger,
        )
    except Exception as exc:
        _log_event(
            logger,
            level="ERROR",
            stage="quality",
            step="error",
            message="Quality stage failed",
            year=year,
            month=month,
            exception=str(exc),
        )
        raise


def _run_backfill(
    spark: SparkSession,
    *,
    from_year: int | None,
    from_month: int | None,
    to_year: int | None,
    to_month: int | None,
    from_token: str | None,
    to_token: str | None,
    input_parquet: str | None,
    max_invalid_ratio: float,
    strict_quality: bool,
    no_resume: bool,
    reset: bool,
    warehouse_dir: str | None,
) -> None:
    start, end = resolve_backfill_range(
        from_year=from_year,
        from_month=from_month,
        to_year=to_year,
        to_month=to_month,
        from_token=from_token,
        to_token=to_token,
    )
    months = iter_month_range(start=start, end=end)
    months_planned = [month_token(year, month) for year, month in months]

    if input_parquet and len(months_planned) > 1:
        raise ValueError(
            "--input-parquet is only supported for single-month backfill ranges. "
            "Use default month discovery/download for multi-month backfill."
        )

    settings_snapshot = {
        "range_from": month_token(*start),
        "range_to": month_token(*end),
        "max_invalid_ratio": float(max_invalid_ratio),
        "strict_quality": bool(strict_quality),
        "warehouse_dir": str(_resolve_relative_path(warehouse_dir).resolve())
        if warehouse_dir
        else str(DEFAULT_WAREHOUSE_DIR.resolve()),
        "input_parquet": str(_resolve_relative_path(input_parquet).resolve())
        if input_parquet
        else None,
    }

    checkpoint_file: Path
    checkpoint: dict
    if no_resume:
        run_id = str(uuid.uuid4())
        checkpoint_file = checkpoint_path(repo_root=REPO_ROOT, run_id=run_id)
        checkpoint = initialize_checkpoint_payload(
            run_id=run_id,
            settings_snapshot=settings_snapshot,
            months_planned=months_planned,
        )
        write_checkpoint(checkpoint_file=checkpoint_file, payload=checkpoint)
        print("Backfill resume disabled; created checkpoint:", checkpoint_file)
    else:
        matched = find_resume_checkpoint(repo_root=REPO_ROOT, settings_snapshot=settings_snapshot)
        if matched is None:
            run_id = str(uuid.uuid4())
            checkpoint_file = checkpoint_path(repo_root=REPO_ROOT, run_id=run_id)
            checkpoint = initialize_checkpoint_payload(
                run_id=run_id,
                settings_snapshot=settings_snapshot,
                months_planned=months_planned,
            )
            write_checkpoint(checkpoint_file=checkpoint_file, payload=checkpoint)
            print("Created new backfill checkpoint:", checkpoint_file)
        else:
            checkpoint_file, checkpoint = matched
            checkpoint = load_checkpoint(checkpoint_file=checkpoint_file)
            print("Resuming backfill from checkpoint:", checkpoint_file)

    checkpoint.setdefault("months_planned", months_planned)
    checkpoint.setdefault("months_completed", [])
    checkpoint.setdefault("current_month", None)
    backfill_run_id = str(checkpoint["run_id"])
    logger = _build_json_logger(run_id=backfill_run_id)
    _log_event(
        logger,
        level="INFO",
        stage="backfill",
        step="start",
        message="Starting backfill orchestration",
    )

    pending_month = next_incomplete_month(
        months_planned=checkpoint["months_planned"],
        months_completed=checkpoint["months_completed"],
    )
    if pending_month is None:
        checkpoint["finished_at"] = checkpoint.get("finished_at") or utc_now_iso()
        checkpoint["current_month"] = None
        write_checkpoint(checkpoint_file=checkpoint_file, payload=checkpoint)
        print(
            "Backfill already complete for range:",
            settings_snapshot["range_from"],
            "->",
            settings_snapshot["range_to"],
        )
        print("Checkpoint:", checkpoint_file)
        _log_event(
            logger,
            level="INFO",
            stage="backfill",
            step="complete",
            message="Backfill already complete",
        )
        return

    if reset:
        _drop_tables(spark, ALL_TABLES)
        print("Backfill reset applied before first pending month")
        _log_event(
            logger,
            level="INFO",
            stage="backfill",
            step="reset",
            message="Applied table reset before backfill",
        )

    resolved_single_input: Path | None = None
    if input_parquet:
        resolved_single_input = _resolve_input_parquet(
            input_parquet=input_parquet,
            year=start[0],
            month=start[1],
        )

    completed: list[str] = list(checkpoint["months_completed"])
    completed_set = set(completed)

    try:
        for month_value in checkpoint["months_planned"]:
            if month_value in completed_set:
                continue

            checkpoint["current_month"] = month_value
            write_checkpoint(checkpoint_file=checkpoint_file, payload=checkpoint)

            run_year, run_month = parse_month_token(month_value, flag_name="checkpoint month")
            print(f"Backfill processing month: {month_value}")
            _log_event(
                logger,
                level="INFO",
                stage="backfill",
                step="month_start",
                message=f"Processing backfill month {month_value}",
                year=run_year,
                month=run_month,
            )
            _run_all_month(
                spark,
                year=run_year,
                month=run_month,
                max_invalid_ratio=max_invalid_ratio,
                strict_quality=strict_quality,
                input_parquet=resolved_single_input,
                run_id=backfill_run_id,
                logger=logger,
            )

            completed.append(month_value)
            completed_set.add(month_value)
            checkpoint["months_completed"] = completed
            checkpoint["current_month"] = None
            write_checkpoint(checkpoint_file=checkpoint_file, payload=checkpoint)
            _log_event(
                logger,
                level="INFO",
                stage="backfill",
                step="month_complete",
                message=f"Completed backfill month {month_value}",
                year=run_year,
                month=run_month,
            )
    except Exception as exc:
        checkpoint["finished_at"] = None
        write_checkpoint(checkpoint_file=checkpoint_file, payload=checkpoint)
        _log_event(
            logger,
            level="ERROR",
            stage="backfill",
            step="error",
            message="Backfill failed",
            exception=str(exc),
        )
        raise

    checkpoint["finished_at"] = utc_now_iso()
    checkpoint["current_month"] = None
    checkpoint["months_completed"] = completed
    write_checkpoint(checkpoint_file=checkpoint_file, payload=checkpoint)
    print(
        "Backfill completed:",
        f"run_id={checkpoint['run_id']}",
        f"months_completed={len(completed)}",
        f"checkpoint={checkpoint_file}",
    )
    _log_event(
        logger,
        level="INFO",
        stage="backfill",
        step="complete",
        message="Backfill completed",
        row_counts={"months_completed": len(completed)},
    )


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
        "--strict",
        dest="strict_quality",
        action="store_true",
        help="Fail pipeline when any severity=error quality-gate threshold is exceeded",
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
        "--strict",
        dest="strict_quality",
        action="store_true",
        help="Fail pipeline when any severity=error quality-gate threshold is exceeded",
    )

    backfill_parser = subparsers.add_parser(
        "run-backfill",
        help="Run month-range backfill with local checkpoint/resume",
    )
    _add_common_options(backfill_parser)
    backfill_parser.add_argument(
        "--from-year",
        type=int,
        default=None,
        help="Backfill start year",
    )
    backfill_parser.add_argument(
        "--from-month",
        type=int,
        default=None,
        help="Backfill start month (1-12)",
    )
    backfill_parser.add_argument(
        "--to-year",
        type=int,
        default=None,
        help="Backfill end year",
    )
    backfill_parser.add_argument(
        "--to-month",
        type=int,
        default=None,
        help="Backfill end month (1-12)",
    )
    backfill_parser.add_argument(
        "--from",
        dest="from_token",
        type=str,
        default=None,
        help="Backfill start month in YYYY-MM format",
    )
    backfill_parser.add_argument(
        "--to",
        dest="to_token",
        type=str,
        default=None,
        help="Backfill end month in YYYY-MM format",
    )
    backfill_parser.add_argument(
        "--input-parquet",
        type=str,
        required=False,
        help="Optional explicit parquet path (single-month ranges only)",
    )
    backfill_parser.add_argument(
        "--strict-quality",
        "--strict",
        dest="strict_quality",
        action="store_true",
        help="Fail pipeline when any severity=error quality-gate threshold is exceeded",
    )
    backfill_parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Disable resume and create a new checkpoint run",
    )

    reset_parser = subparsers.add_parser("reset", help="Drop bronze/silver/gold/quality tables")
    _add_common_options(reset_parser)
    reset_parser.add_argument(
        "--drop-schemas",
        action="store_true",
        help="Drop bronze/silver/gold/quality schemas with CASCADE",
    )
    reset_parser.add_argument(
        "--purge-checkpoints",
        action="store_true",
        help="Delete local backfill checkpoint files under .local/checkpoints",
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

    logged_commands = {"run-bronze", "run-silver", "run-gold", "run-quality", "run-all"}
    orchestration_run_id = str(uuid.uuid4()) if args.command in logged_commands else None
    logger = (
        _build_json_logger(run_id=orchestration_run_id)
        if orchestration_run_id is not None
        else None
    )

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
    _log_event(
        logger,
        level="INFO",
        stage="orchestration",
        step="command_start",
        message=f"Starting command '{args.command}'",
        year=year,
        month=month,
    )

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
            if getattr(args, "purge_checkpoints", False):
                DEFAULT_CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
                purged_count = purge_backfill_checkpoints(repo_root=REPO_ROOT)
                print("Purged backfill checkpoints:", purged_count)
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
                run_id=orchestration_run_id,
                logger=logger,
            )
            _log_event(
                logger,
                level="INFO",
                stage="orchestration",
                step="command_complete",
                message="Completed command 'run-bronze'",
                year=year,
                month=month,
            )
            return

        if args.command == "run-silver":
            _run_silver(
                spark,
                max_invalid_ratio=args.max_invalid_ratio,
                year=args.year,
                month=args.month,
                reset=args.reset,
                run_id=orchestration_run_id,
                logger=logger,
            )
            _log_event(
                logger,
                level="INFO",
                stage="orchestration",
                step="command_complete",
                message="Completed command 'run-silver'",
                year=year,
                month=month,
            )
            return

        if args.command == "run-gold":
            _run_gold(
                spark,
                max_invalid_ratio=args.max_invalid_ratio,
                year=args.year,
                month=args.month,
                reset=args.reset,
                run_id=orchestration_run_id,
                logger=logger,
            )
            _log_event(
                logger,
                level="INFO",
                stage="orchestration",
                step="command_complete",
                message="Completed command 'run-gold'",
                year=year,
                month=month,
            )
            return

        if args.command == "run-quality":
            try:
                _run_quality(
                    spark,
                    max_invalid_ratio=args.max_invalid_ratio,
                    strict_quality=args.strict_quality,
                    year=args.year,
                    month=args.month,
                    reset=args.reset,
                    run_id=orchestration_run_id,
                    logger=logger,
                )
            except Exception as exc:
                _log_event(
                    logger,
                    level="ERROR",
                    stage="quality",
                    step="error",
                    message="Quality stage failed",
                    year=year,
                    month=month,
                    exception=str(exc),
                )
                raise
            _log_event(
                logger,
                level="INFO",
                stage="orchestration",
                step="command_complete",
                message="Completed command 'run-quality'",
                year=year,
                month=month,
            )
            return

        if args.command == "run-all":
            if resolved_input_parquet is None:
                raise ValueError("Resolved parquet path is required for run-all")
            if args.reset:
                _drop_tables(spark, ALL_TABLES)
            _run_all_month(
                spark,
                year=args.year,
                month=args.month,
                max_invalid_ratio=args.max_invalid_ratio,
                strict_quality=args.strict_quality,
                input_parquet=resolved_input_parquet,
                run_id=orchestration_run_id,
                logger=logger,
            )
            _log_event(
                logger,
                level="INFO",
                stage="orchestration",
                step="command_complete",
                message="Completed command 'run-all'",
                year=year,
                month=month,
            )
            return

        if args.command == "run-backfill":
            _run_backfill(
                spark,
                from_year=args.from_year,
                from_month=args.from_month,
                to_year=args.to_year,
                to_month=args.to_month,
                from_token=args.from_token,
                to_token=args.to_token,
                input_parquet=args.input_parquet,
                max_invalid_ratio=args.max_invalid_ratio,
                strict_quality=args.strict_quality,
                no_resume=args.no_resume,
                reset=args.reset,
                warehouse_dir=getattr(args, "warehouse_dir", None),
            )
            return

        raise ValueError(f"Unsupported command: {args.command}")
    except Exception as exc:
        _log_event(
            logger,
            level="ERROR",
            stage="orchestration",
            step="command_error",
            message=f"Command '{args.command}' failed",
            year=year,
            month=month,
            exception=str(exc),
        )
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc
