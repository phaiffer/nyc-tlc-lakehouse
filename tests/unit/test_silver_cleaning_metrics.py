from datetime import datetime
from decimal import Decimal

from pipelines.silver_transform.run_silver_enforced import apply_silver_rules


def test_cleaning_removals_balance_with_nulls_overlaps_and_duplicates(spark):
    pickup = datetime(2024, 1, 1, 10)
    dropoff = datetime(2024, 1, 1, 11)
    good = (1, pickup, dropoff, 2.0, Decimal("10.00"), 1)
    rows = [
        good,
        good,
        (2, pickup, dropoff, 2.0, Decimal("10.00"), 1),
        (1, None, None, -1.0, Decimal("-1.00"), 1),
        (1, pickup, None, -1.0, Decimal("-1.00"), 1),
        (1, pickup, pickup, -1.0, Decimal("-1.00"), 1),
        (1, pickup, dropoff, -1.0, Decimal("-1.00"), 1),
        (1, pickup, dropoff, -1.0, None, 1),
        (1, pickup, dropoff, -1.0, Decimal("10.00"), 1),
        (1, pickup, dropoff, None, Decimal("10.00"), 1),
    ]
    source = spark.createDataFrame(
        rows,
        "VendorID int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, "
        "trip_distance double, fare_amount decimal(18,2), passenger_count bigint",
    )
    metrics = {}
    result = apply_silver_rules(source, cleaning_metrics=metrics)
    assert metrics["removed_by_rule"] == {
        "missing_pickup_ts": 1,
        "missing_dropoff_ts": 1,
        "non_positive_duration": 1,
        "missing_pickup_date": 0,
        "negative_or_null_fare": 2,
        "negative_or_null_distance": 2,
        "duplicate_trip_id": 1,
    }
    assert metrics["input_count"] == source.count() == 10
    assert metrics["output_count"] == result.count() == 2
    assert sum(metrics["removed_by_rule"].values()) == source.count() - result.count()
    assert {row.vendor_id for row in result.collect()} == {"1", "2"}


def test_empty_cleaning_input_has_zero_removals(spark):
    source = spark.createDataFrame(
        [],
        "VendorID int, tpep_pickup_datetime timestamp, tpep_dropoff_datetime timestamp, "
        "trip_distance double, fare_amount decimal(18,2), passenger_count bigint",
    )
    metrics = {}
    assert apply_silver_rules(source, cleaning_metrics=metrics).count() == 0
    assert metrics["input_count"] == metrics["output_count"] == metrics["removed_count"] == 0
    assert all(count == 0 for count in metrics["removed_by_rule"].values())
