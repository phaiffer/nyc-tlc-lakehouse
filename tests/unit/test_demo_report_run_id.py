import json
from datetime import datetime

import pytest

from scripts import demo_report


@pytest.fixture
def report_tables(spark, monkeypatch):
    monkeypatch.setattr(demo_report, "METRICS_TABLE", "report_metrics")
    monkeypatch.setattr(demo_report, "VIOLATIONS_TABLE", "report_violations")
    rows = []
    for month, run_id, day in [(1, "old-january", 1), (1, "january", 2), (2, "february", 3)]:
        for dataset in (demo_report.SILVER_TABLE, demo_report.GOLD_TABLE):
            rows.append(
                (
                    run_id,
                    dataset,
                    datetime(2024, 3, day),
                    json.dumps({"window": {"year": 2024, "month": month}}),
                )
            )
    spark.createDataFrame(
        rows, "run_id string, dataset string, run_ts timestamp, metrics_json string"
    ).createOrReplaceTempView("report_metrics")
    spark.createDataFrame(
        [("january", 2024, 1), ("february", 2024, 2)],
        "run_id string, window_year int, window_month int",
    ).createOrReplaceTempView("report_violations")


@pytest.mark.parametrize("month,expected", [(1, "january"), (2, "february")])
def test_report_uses_latest_pipeline_run_for_its_month(spark, report_tables, month, expected):
    assert demo_report.resolve_report_run(spark, year=2024, month=month)["run_id"] == expected


def test_report_rejects_unrelated_explicit_run_id(spark, report_tables):
    with pytest.raises(ValueError, match="does not match"):
        demo_report.resolve_report_run(spark, year=2024, month=1, run_id="february")


def test_report_rejects_month_without_execution(spark, report_tables):
    with pytest.raises(ValueError, match="No pipeline metrics"):
        demo_report.resolve_report_run(spark, year=2024, month=3)


def test_report_rejects_quality_results_from_another_run(spark, report_tables):
    spark.sql("SELECT * FROM report_violations WHERE window_month = 2").createOrReplaceTempView(
        "report_violations"
    )
    with pytest.raises(ValueError, match="No quality results"):
        demo_report.resolve_report_run(spark, year=2024, month=1)
