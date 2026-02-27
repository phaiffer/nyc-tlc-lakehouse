from __future__ import annotations

from datetime import datetime

from pipelines.common.delta_incremental_merge import incremental_merge


def _build_source_df(spark):
    return spark.createDataFrame(
        [
            ("trip_1", "1", datetime(2024, 1, 1, 10, 0, 0), datetime(2024, 1, 1, 10, 5, 0)),
            ("trip_2", "2", datetime(2024, 1, 1, 11, 0, 0), datetime(2024, 1, 1, 11, 10, 0)),
        ],
        schema=["trip_id", "vendor_id", "pickup_ts", "updated_at"],
    )


def test_incremental_merge_is_idempotent_for_same_window(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS integration")
    target_table = "integration.silver_incremental_idempotent"
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    source_df = _build_source_df(spark)

    first = incremental_merge(
        spark,
        source_df=source_df,
        target_table=target_table,
        primary_key=["trip_id"],
        watermark_column="updated_at",
        late_arrival_days=7,
    )
    count_after_first = spark.table(target_table).count()

    second = incremental_merge(
        spark,
        source_df=source_df,
        target_table=target_table,
        primary_key=["trip_id"],
        watermark_column="updated_at",
        late_arrival_days=7,
    )
    count_after_second = spark.table(target_table).count()

    assert first["rows_deduped"] == 2
    assert second["rows_deduped"] == 2
    assert count_after_first == 2
    assert count_after_second == 2


def test_incremental_merge_applies_late_arrival_window(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS integration")
    target_table = "integration.silver_incremental_window"
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

    baseline_df = spark.createDataFrame(
        [
            ("trip_a", "1", datetime(2024, 1, 10, 10, 0, 0), datetime(2024, 1, 10, 10, 0, 0)),
            ("trip_b", "2", datetime(2024, 1, 10, 11, 0, 0), datetime(2024, 1, 10, 11, 0, 0)),
        ],
        schema=["trip_id", "vendor_id", "pickup_ts", "updated_at"],
    )
    incremental_merge(
        spark,
        source_df=baseline_df,
        target_table=target_table,
        primary_key=["trip_id"],
        watermark_column="updated_at",
        late_arrival_days=2,
    )

    candidate_df = spark.createDataFrame(
        [
            ("trip_old", "1", datetime(2024, 1, 7, 9, 0, 0), datetime(2024, 1, 7, 9, 0, 0)),
            (
                "trip_in_window",
                "1",
                datetime(2024, 1, 8, 10, 30, 0),
                datetime(2024, 1, 8, 10, 30, 0),
            ),
            ("trip_new", "2", datetime(2024, 1, 12, 8, 0, 0), datetime(2024, 1, 12, 8, 0, 0)),
        ],
        schema=["trip_id", "vendor_id", "pickup_ts", "updated_at"],
    )

    result = incremental_merge(
        spark,
        source_df=candidate_df,
        target_table=target_table,
        primary_key=["trip_id"],
        watermark_column="updated_at",
        late_arrival_days=2,
    )

    target_df = spark.table(target_table)
    target_trip_ids = {row["trip_id"] for row in target_df.select("trip_id").collect()}

    assert result["max_watermark_target"] == "2024-01-10 11:00:00"
    assert result["lower_bound"] == "2024-01-08"
    assert result["rows_source_filtered"] == 2
    assert "trip_old" not in target_trip_ids
    assert "trip_in_window" in target_trip_ids
    assert "trip_new" in target_trip_ids
