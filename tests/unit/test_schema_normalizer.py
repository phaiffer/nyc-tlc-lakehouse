from __future__ import annotations

from pipelines.common.schema_normalizer import normalize_bronze_schema


def _schema_as_dict(df) -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in df.schema.fields}


def test_normalize_bronze_schema_casts_expected_columns(spark) -> None:
    raw_df = spark.sql(
        """
        SELECT
          CAST(1 AS INT) AS VendorID,
          CAST(100 AS INT) AS PULocationID,
          CAST(200 AS INT) AS DOLocationID,
          CAST(2 AS INT) AS payment_type,
          CAST('2024-01-01 08:00:00' AS TIMESTAMP_NTZ) AS tpep_pickup_datetime,
          CAST('2024-01-01 08:05:00' AS TIMESTAMP_NTZ) AS tpep_dropoff_datetime,
          CAST(1.5 AS DOUBLE) AS trip_distance
        """
    )

    normalized_df, report = normalize_bronze_schema(raw_df)
    schema_map = _schema_as_dict(normalized_df)

    assert schema_map["VendorID"] == "int"
    assert schema_map["PULocationID"] == "int"
    assert schema_map["DOLocationID"] == "int"
    assert schema_map["payment_type"] == "bigint"
    assert schema_map["tpep_pickup_datetime"] == "timestamp"
    assert schema_map["tpep_dropoff_datetime"] == "timestamp"
    assert "tpep_pickup_datetime" in report.casted_timestamp_ntz_columns
    assert "tpep_dropoff_datetime" in report.casted_timestamp_ntz_columns
    assert "payment_type:bigint" in report.casted_fixed_columns
