from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from pyspark.sql import functions as F  # noqa: E402

from orchestration.local.run_pipeline import (  # noqa: E402
    GOLD_TABLE,
    METRICS_TABLE,
    SILVER_TABLE,
    VIOLATIONS_TABLE,
    _build_spark_session,
    _resolve_spark_paths,
)


def _require_month(*, year: int, month: int) -> None:
    if year < 1:
        raise ValueError("--year must be >= 1")
    if month < 1 or month > 12:
        raise ValueError("--month must be between 1 and 12")


def _window_bounds(*, year: int, month: int) -> tuple[str, str]:
    start = f"{year:04d}-{month:02d}-01"
    if month == 12:
        end = f"{year + 1:04d}-01-01"
    else:
        end = f"{year:04d}-{month + 1:02d}-01"
    return start, end


def resolve_report_run(spark, *, year: int, month: int, run_id: str | None = None) -> dict:
    """Resolve the latest matching stage run; never label a report with a new UUID."""
    _require_month(year=year, month=month)
    metrics = spark.table(METRICS_TABLE).filter(
        (F.get_json_object("metrics_json", "$.window.year").cast("int") == year)
        & (F.get_json_object("metrics_json", "$.window.month").cast("int") == month)
    )
    latest = {}
    for dataset in (SILVER_TABLE, GOLD_TABLE):
        rows = (
            metrics.filter(F.col("dataset") == dataset)
            .orderBy(F.col("run_ts").desc())
            .limit(1)
            .collect()
        )
        if not rows:
            raise ValueError(f"No pipeline metrics for {dataset} in {year:04d}-{month:02d}")
        latest[dataset] = rows[0]
    effective_run_id = latest[GOLD_TABLE]["run_id"]
    if latest[SILVER_TABLE]["run_id"] != effective_run_id:
        raise ValueError("Latest Silver and Gold metrics belong to different runs")
    if run_id is not None and run_id != effective_run_id:
        raise ValueError("Requested run_id does not match the latest pipeline run for this month")
    quality_rows = spark.table(VIOLATIONS_TABLE).filter(
        (F.col("run_id") == effective_run_id)
        & (F.col("window_year") == year)
        & (F.col("window_month") == month)
    )
    if not quality_rows.limit(1).count():
        raise ValueError("No quality results for the selected pipeline run and month")
    return {
        "run_id": effective_run_id,
        "silver_metrics": json.loads(latest[SILVER_TABLE]["metrics_json"]),
    }


def _query_month_counts(
    *, year: int, month: int, warehouse_dir: str | None, run_id: str | None = None
) -> dict[str, str]:
    start, end = _window_bounds(year=year, month=month)
    paths = _resolve_spark_paths(warehouse_dir=warehouse_dir)
    spark = _build_spark_session(paths=paths)

    try:
        selected_run = resolve_report_run(spark, year=year, month=month, run_id=run_id)
        # Bronze is overwritten each month. Read its run snapshot, so an older
        # month's report remains correct after a subsequent month was ingested.
        bronze_count = selected_run["silver_metrics"]["row_accounting"]["input_count"]

        silver_count = spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {SILVER_TABLE}
            WHERE pickup_date >= DATE('{start}')
              AND pickup_date < DATE('{end}')
            """
        ).collect()[0]["cnt"]

        gold_count = spark.sql(
            f"""
            SELECT COUNT(*) AS cnt
            FROM {GOLD_TABLE}
            WHERE trip_date >= DATE('{start}')
              AND trip_date < DATE('{end}')
            """
        ).collect()[0]["cnt"]

        month_kpi = spark.sql(
            f"""
            SELECT
              CAST(SUM(total_fare) / NULLIF(SUM(trips), 0) AS DECIMAL(18, 2)) AS avg_fare_per_trip
            FROM {GOLD_TABLE}
            WHERE trip_date >= DATE('{start}')
              AND trip_date < DATE('{end}')
            """
        ).collect()[0]["avg_fare_per_trip"]

        top_days = spark.sql(
            f"""
            SELECT
              trip_date,
              CAST(SUM(total_fare) / NULLIF(SUM(trips), 0) AS DECIMAL(18, 2)) AS avg_fare_per_trip
            FROM {GOLD_TABLE}
            WHERE trip_date >= DATE('{start}')
              AND trip_date < DATE('{end}')
            GROUP BY trip_date
            ORDER BY trip_date
            LIMIT 5
            """
        ).collect()
    finally:
        spark.stop()

    return {
        "run_id": selected_run["run_id"],
        "bronze_count": str(int(bronze_count)),
        "silver_count": str(int(silver_count)),
        "gold_count": str(int(gold_count)),
        "month_avg_fare_per_trip": str(month_kpi),
        "top_days": "\n".join(
            [
                f"  - {row['trip_date']}: avg_fare_per_trip={row['avg_fare_per_trip']}"
                for row in top_days
            ]
        ),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate local demo KPI report")
    parser.add_argument("--year", type=int, required=True, help="Demo year")
    parser.add_argument("--month", type=int, required=True, help="Demo month (1-12)")
    parser.add_argument(
        "--warehouse-dir",
        type=str,
        default=None,
        help="Optional warehouse override",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(REPO_ROOT / ".local" / "demo"),
        help="Output directory for demo text reports",
    )
    parser.add_argument(
        "--run-id",
        type=str,
        default=None,
        help="Validate against this pipeline run_id (defaults to latest matching month)",
    )
    args = parser.parse_args()

    _require_month(year=args.year, month=args.month)
    output_dir = Path(args.output_dir).expanduser()
    output_dir.mkdir(parents=True, exist_ok=True)
    counts = _query_month_counts(
        year=args.year, month=args.month, warehouse_dir=args.warehouse_dir, run_id=args.run_id
    )
    run_id = counts["run_id"]
    output_file = output_dir / f"demo_{run_id}.txt"
    lines = [
        "NYC TLC Lakehouse Demo Report",
        f"generated_at_utc={datetime.now(tz=timezone.utc).isoformat()}",
        f"run_id={run_id}",
        f"window={args.year:04d}-{args.month:02d}",
        "",
        f"bronze_events_count={counts['bronze_count']}",
        f"silver_trips_count={counts['silver_count']}",
        f"gold_daily_fact_row_count={counts['gold_count']}",
        f"month_avg_fare_per_trip={counts['month_avg_fare_per_trip']}",
        "top_5_days_avg_fare_per_trip:",
        counts["top_days"] or "  - <no rows>",
    ]
    report_text = "\n".join(lines) + "\n"

    output_file.write_text(report_text, encoding="utf-8")
    print(report_text)
    print(f"Demo report saved: {output_file}")


if __name__ == "__main__":
    main()
