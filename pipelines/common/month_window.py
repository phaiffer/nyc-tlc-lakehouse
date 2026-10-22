from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def filter_month_window(
    df: DataFrame, *, date_column: str, year: int | None, month: int | None
) -> DataFrame:
    """Select the observation window before profiling accumulated Silver/Gold tables."""
    if year is None and month is None:
        return df
    if year is None or month is None or not 1 <= month <= 12:
        raise ValueError("Provide both year and month (1-12) for month window filtering")
    start = F.to_date(F.lit(f"{year:04d}-{month:02d}-01"))
    return df.filter((F.col(date_column) >= start) & (F.col(date_column) < F.add_months(start, 1)))
