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


def _normalize_severity(value: Any) -> str:
    severity = str(value or "error").strip().lower()
    if severity not in {"error", "warn", "info"}:
        return "error"
    return severity


def _build_first_failure_value(rules: list[dict[str, Any]], *, key: str):
    return F.coalesce(*[F.when(rule["failed_expr"], F.lit(str(rule[key]))) for rule in rules])


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

    # 2) Row-level expectations (severity-aware)
    expectation_rules: list[dict[str, Any]] = []
    for rule in contract.expectations:
        rule_type = str(rule.get("type", "")).strip().lower()
        rule_name = str(rule.get("name", "")).strip()
        rule_id = str(rule.get("rule_id", "")).strip() or rule_name
        severity = _normalize_severity(rule.get("severity"))

        if rule_type == "not_null":
            not_null_columns = [str(col_name) for col_name in rule.get("columns", [])]
            if not not_null_columns:
                continue

            failed_expr = reduce(
                lambda left, right: left | right,
                [F.col(col_name).isNull() for col_name in not_null_columns],
            )
            expectation_rules.append(
                {
                    "rule_id": rule_id,
                    "rule_name": rule_name,
                    "severity": severity,
                    "reason_code": "not_null_violation",
                    "failed_expr": failed_expr,
                }
            )

        elif rule_type == "range":
            col_name = str(rule.get("column"))
            min_value = rule.get("min")
            max_value = rule.get("max")

            failed_expr = F.col(col_name).isNull()
            if min_value is not None:
                failed_expr = failed_expr | (F.col(col_name) < F.lit(min_value))
            if max_value is not None:
                failed_expr = failed_expr | (F.col(col_name) > F.lit(max_value))
            expectation_rules.append(
                {
                    "rule_id": rule_id,
                    "rule_name": rule_name,
                    "severity": severity,
                    "reason_code": "range_violation",
                    "failed_expr": failed_expr,
                }
            )

        elif rule_type == "unique":
            # Uniqueness is checked separately for PK (below).
            continue

        else:
            # Unknown rules are ignored (best-effort). You can tighten this later.
            continue

    error_rules = [rule for rule in expectation_rules if rule["severity"] == "error"]
    non_error_rules = [rule for rule in expectation_rules if rule["severity"] != "error"]

    if error_rules:
        failed_rule_id = _build_first_failure_value(error_rules, key="rule_id")
        failed_rule_name = _build_first_failure_value(error_rules, key="rule_name")
        failed_reason_code = _build_first_failure_value(error_rules, key="reason_code")
        failed_severity = _build_first_failure_value(error_rules, key="severity")
        is_error_invalid_expr = reduce(
            lambda left, right: left | right,
            [rule["failed_expr"] for rule in error_rules],
        )

        staged = (
            df.withColumn("rule_id", failed_rule_id)
            .withColumn("rule_name", failed_rule_name)
            .withColumn("reason_code", failed_reason_code)
            .withColumn("severity", failed_severity)
            .withColumn("__is_error_invalid", is_error_invalid_expr)
        )
        valid_df = staged.filter(~F.col("__is_error_invalid")).drop(
            "rule_id",
            "rule_name",
            "reason_code",
            "severity",
            "__is_error_invalid",
        )
        quarantine_df = staged.filter(F.col("__is_error_invalid")).drop("__is_error_invalid")
    else:
        valid_df = df
        quarantine_df = df.limit(0)

    # 3) Primary key uniqueness
    pk_cols = contract.primary_key or []
    pk_violations = 0
    if pk_cols:
        pk_violations = (
            valid_df.groupBy(*pk_cols).count().filter(F.col("count") > 1).limit(1).count()
        )

        if pk_violations > 0:
            raise ValueError(f"Primary key uniqueness violated for PK={pk_cols}")

    # 4) Threshold gate
    total_count = df.count()
    invalid_count = quarantine_df.count()
    invalid_ratio = (invalid_count / total_count) if total_count else 0.0

    warn_count = 0
    info_count = 0
    for severity_name in ("warn", "info"):
        severity_rules = [
            rule["failed_expr"] for rule in non_error_rules if rule["severity"] == severity_name
        ]
        if severity_rules:
            combined_expr = reduce(lambda left, right: left | right, severity_rules)
            severity_count = df.filter(combined_expr).count()
        else:
            severity_count = 0
        if severity_name == "warn":
            warn_count = severity_count
        else:
            info_count = severity_count

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
        "warn_count": warn_count,
        "info_count": info_count,
    }

    return EnforcementResult(valid_df=valid_df, quarantine_df=quarantine_df, summary=summary)
