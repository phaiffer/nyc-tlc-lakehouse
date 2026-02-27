from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from orchestration.local.run_pipeline import (
    ALL_TABLES,
    BRONZE_TABLE,
    GOLD_TABLE,
    METRICS_TABLE,
    SILVER_TABLE,
    VIOLATIONS_TABLE,
    _drop_tables,
    _run_bronze,
    _run_gold,
    _run_quality,
    _run_silver,
)


def _write_sample_input_parquet(*, spark, output_path: Path) -> None:
    rows = [
        (
            1,
            datetime(2024, 1, 1, 8, 0, 0),
            datetime(2024, 1, 1, 8, 15, 0),
            2,
            1,
            101,
            202,
            1.8,
            Decimal("12.50"),
            1,
        ),
        (
            2,
            datetime(2024, 1, 1, 9, 0, 0),
            datetime(2024, 1, 1, 9, 20, 0),
            1,
            2,
            103,
            204,
            4.1,
            Decimal("21.00"),
            2,
        ),
        (
            1,
            datetime(2024, 1, 2, 10, 0, 0),
            datetime(2024, 1, 2, 10, 18, 0),
            5,
            1,
            105,
            206,
            3.2,
            Decimal("18.20"),
            1,
        ),
    ]
    sample_df = spark.createDataFrame(
        rows,
        schema=[
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "RatecodeID",
            "payment_type",
            "PULocationID",
            "DOLocationID",
            "trip_distance",
            "fare_amount",
            "passenger_count",
        ],
    )
    sample_df.write.mode("overwrite").parquet(str(output_path.resolve()))


def test_pipeline_smoke_wiring_uses_local_sample_parquet(spark, tmp_path) -> None:
    input_parquet = tmp_path / "yellow_tripdata_sample.parquet"
    _write_sample_input_parquet(spark=spark, output_path=input_parquet)

    _drop_tables(spark, ALL_TABLES)
    try:
        _run_bronze(
            spark,
            input_parquet=input_parquet,
            year=None,
            month=None,
            reset=False,
        )
        _run_silver(
            spark,
            max_invalid_ratio=0.001,
            year=None,
            month=None,
            reset=False,
        )
        _run_gold(
            spark,
            max_invalid_ratio=0.001,
            year=None,
            month=None,
            reset=False,
        )
        _run_quality(
            spark,
            max_invalid_ratio=0.001,
            strict_quality=True,
            year=None,
            month=None,
            reset=False,
        )

        required_tables = [BRONZE_TABLE, SILVER_TABLE, GOLD_TABLE, METRICS_TABLE, VIOLATIONS_TABLE]
        for table_name in required_tables:
            assert spark.catalog.tableExists(table_name), f"Expected table to exist: {table_name}"
            assert spark.table(table_name).count() > 0, f"Expected positive rows in: {table_name}"
    finally:
        _drop_tables(spark, ALL_TABLES)
