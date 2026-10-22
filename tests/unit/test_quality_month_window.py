from datetime import datetime

from orchestration.local import run_pipeline
from quality.validators.quality_gate import run_quality_gate


def test_quality_and_drift_profile_only_the_selected_month(spark, monkeypatch, capsys):
    silver = spark.sql("""
        SELECT 'bad-january' trip_id, DATE'2024-01-31' pickup_date,
               TIMESTAMP'2024-01-31 10:00:00' pickup_ts,
               TIMESTAMP'2024-01-31 09:00:00' dropoff_ts,
               CAST(-5 AS DECIMAL(18,2)) fare_amount, 2D trip_distance, 1 passenger_count
        UNION ALL
        SELECT 'good-february', DATE'2024-02-01', TIMESTAMP'2024-02-01 10:00:00',
               TIMESTAMP'2024-02-01 11:00:00', CAST(10 AS DECIMAL(18,2)), 2D, 1
    """)
    gold = spark.sql("""
        SELECT DATE'2024-01-31' trip_date, '1' vendor_id, 1L trips,
               CAST(1000000 AS DECIMAL(18,2)) total_fare
        UNION ALL
        SELECT DATE'2024-02-01', '1', 1L, CAST(10 AS DECIMAL(18,2))
    """)
    silver.createOrReplaceTempView("window_silver")
    gold.createOrReplaceTempView("window_gold")
    result = run_quality_gate(
        spark,
        silver_table="window_silver",
        gold_table="window_gold",
        output_table="quality.window_violations",
        strict=True,
        run_id="window-run",
        run_ts=datetime(2024, 3, 1),
        window_year=2024,
        window_month=2,
    )
    assert result["silver_rows"] == 1
    assert result["failed_rules"] == []
    assert result["run_id"] == "window-run"

    monkeypatch.setattr(run_pipeline, "SILVER_TABLE", "window_silver")
    monkeypatch.setattr(run_pipeline, "GOLD_TABLE", "window_gold")
    monkeypatch.setattr(run_pipeline, "VIOLATIONS_TABLE", "quality.window_violations")
    # Contract lookup uses a canonical dataset name even when input views are renamed.
    real_loader = run_pipeline.load_contract_by_dataset
    monkeypatch.setattr(
        run_pipeline,
        "load_contract_by_dataset",
        lambda root, dataset: real_loader(root, "silver.trips_clean"),
    )
    observed = {}
    next_run_id = "window-run-next"

    def capture_drift(spark, **kwargs):
        observed[kwargs["dataset"]] = kwargs["source_df"].collect()
        assert kwargs["run_id"] == next_run_id
        return {}

    monkeypatch.setattr(run_pipeline, "detect_and_record_drift", capture_drift)
    run_pipeline._run_quality(
        spark,
        max_invalid_ratio=0.001,
        strict_quality=True,
        year=2024,
        month=2,
        reset=False,
        run_id=next_run_id,
    )
    assert len(observed["window_silver"]) == len(observed["window_gold"]) == 1
    assert str(observed["window_silver"][0].pickup_date) == "2024-02-01"
    assert str(observed["window_gold"][0].trip_date) == "2024-02-01"
    assert spark.table("quality.window_violations").count() == 12
    assert "Quality rows for this run/window: 6" in capsys.readouterr().out
