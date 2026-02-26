from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

BRONZE_FIXED_COLUMN_TYPES = {
    "VendorID": "int",
    "passenger_count": "bigint",
    "RatecodeID": "bigint",
    "payment_type": "bigint",
    "PULocationID": "int",
    "DOLocationID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
}


@dataclass(frozen=True)
class SchemaNormalizationReport:
    casted_timestamp_ntz_columns: list[str]
    casted_fixed_columns: list[str]


def _cast_timestamp_ntz_columns_to_timestamp(df: DataFrame) -> tuple[DataFrame, list[str]]:
    timestamp_ntz_columns = [
        field.name for field in df.schema.fields if field.dataType.simpleString() == "timestamp_ntz"
    ]
    if not timestamp_ntz_columns:
        return df, []

    timestamp_ntz_column_set = set(timestamp_ntz_columns)
    normalized_df = df.select(
        *[
            F.col(column_name).cast("timestamp").alias(column_name)
            if column_name in timestamp_ntz_column_set
            else F.col(column_name)
            for column_name in df.columns
        ]
    )
    return normalized_df, timestamp_ntz_columns


def _cast_columns_to_fixed_types(
    df: DataFrame,
    *,
    column_types: dict[str, str],
) -> tuple[DataFrame, list[str]]:
    casted_columns: list[str] = []
    projected_columns = []

    for column_name in df.columns:
        target_type = column_types.get(column_name)
        if target_type is None:
            projected_columns.append(F.col(column_name))
            continue

        casted_columns.append(f"{column_name}:{target_type}")
        projected_columns.append(F.col(column_name).cast(target_type).alias(column_name))

    if not casted_columns:
        return df, []

    return df.select(*projected_columns), casted_columns


def normalize_bronze_schema(df: DataFrame) -> tuple[DataFrame, SchemaNormalizationReport]:
    """
    Apply deterministic casting for Bronze stability across reruns.
    """
    normalized_df, casted_timestamp_ntz_columns = _cast_timestamp_ntz_columns_to_timestamp(df)
    canonical_df, casted_fixed_columns = _cast_columns_to_fixed_types(
        normalized_df,
        column_types=BRONZE_FIXED_COLUMN_TYPES,
    )
    return canonical_df, SchemaNormalizationReport(
        casted_timestamp_ntz_columns=casted_timestamp_ntz_columns,
        casted_fixed_columns=casted_fixed_columns,
    )
