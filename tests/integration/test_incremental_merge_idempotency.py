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
