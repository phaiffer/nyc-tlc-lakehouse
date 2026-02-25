from __future__ import annotations

from dataclasses import dataclass
from functools import reduce
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from quality.validators.contract_loader import DataContract


@dataclass(frozen=True)
class EnforcementResult:
    valid_df: DataFrame
    quarantine_df: DataFrame
    summary: dict[str, Any]


def _spark_schema_types(schema: StructType) -> dict[str, str]:
    """
    Convert Spark schema into {column_name: spark_simple_type_string}.
    """
    return {field.name: field.dataType.simpleString() for field in schema.fields}


def enforce_contract(
    df: DataFrame,
    contract: DataContract,
    *,
    max_invalid_ratio: float = 0.001,
) -> EnforcementResult:
    """
    Enforce a DataContract against a Spark DataFrame.

    Steps:
    1) Validate required columns and types
    2) Apply row-level expectations (not_null, range)
    3) Validate primary key uniqueness (if defined)
    4) Fail if invalid ratio exceeds threshold
    """

    expected_cols = {c.name: c for c in contract.schema}
    actual_types = _spark_schema_types(df.schema)

    # 1) Columns presence
    missing_cols = sorted([name for name in expected_cols if name not in actual_types])
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # 1) Type matching (strict string compare on Spark simpleString)
    mismatches: list[str] = []
    for name, col_def in expected_cols.items():
        actual = actual_types.get(name)
        expected = col_def.dtype.strip().lower()
        if actual is None:
            continue
        if actual.strip().lower() != expected:
            mismatches.append(f"{name}: expected={expected} actual={actual}")

    if mismatches:
        raise ValueError(f"Type mismatches found: {mismatches}")

    # 2) Row-level expectations -> build boolean predicate list
    predicates = []
    for rule in contract.expectations:
        rule_type = str(rule.get("type", "")).strip().lower()

        if rule_type == "not_null":
            for col_name in rule.get("columns", []):
                predicates.append(F.col(col_name).isNotNull())

        elif rule_type == "range":
            col_name = str(rule.get("column"))
            min_value = rule.get("min")
            max_value = rule.get("max")

            p = F.col(col_name).isNotNull()
            if min_value is not None:
                p = p & (F.col(col_name) >= F.lit(min_value))
            if max_value is not None:
                p = p & (F.col(col_name) <= F.lit(max_value))
            predicates.append(p)

        elif rule_type == "unique":
            # Uniqueness is checked separately for PK (below).
            continue

        else:
            # Unknown rules are ignored (best-effort). You can tighten this later.
            continue

    if predicates:
        is_valid_expr = reduce(lambda a, b: a & b, predicates)
        staged = df.withColumn("__is_valid", is_valid_expr)
        valid_df = staged.filter(F.col("__is_valid")).drop("__is_valid")
        quarantine_df = staged.filter(~F.col("__is_valid")).drop("__is_valid")
    else:
        valid_df = df
        quarantine_df = df.limit(0)

    # 3) Primary key uniqueness
    pk_cols = contract.primary_key or []
    pk_violations = 0
    if pk_cols:
        pk_violations = (
            valid_df.groupBy(*pk_cols)
            .count()
            .filter(F.col("count") > 1)
            .limit(1)
            .count()
        )

        if pk_violations > 0:
            raise ValueError(f"Primary key uniqueness violated for PK={pk_cols}")

    # 4) Threshold gate
    total_count = df.count()
    invalid_count = quarantine_df.count()
    invalid_ratio = (invalid_count / total_count) if total_count else 0.0

    if invalid_ratio > max_invalid_ratio:
        raise ValueError(
            "Invalid ratio exceeded: "
            f"invalid_ratio={invalid_ratio:.6f} "
            f"threshold={max_invalid_ratio:.6f}"
        )

    summary = {
        "dataset": contract.dataset,
        "contract_version": contract.version,
        "total_count": total_count,
        "invalid_count": invalid_count,
        "invalid_ratio": invalid_ratio,
        "pk_checked": bool(pk_cols),
        "pk_violations": pk_violations,
    }

    return EnforcementResult(valid_df=valid_df, quarantine_df=quarantine_df, summary=summary)
