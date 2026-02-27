from __future__ import annotations

from quality.validators.quarantine_writer import write_quarantine


def _schema_as_dict(df) -> dict[str, str]:
    return {field.name: field.dataType.simpleString() for field in df.schema.fields}


def test_write_quarantine_adds_metadata_and_persists_rows(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS quality")
    quarantine_table = "quality.quarantine_writer_test"
    spark.sql(f"DROP TABLE IF EXISTS {quarantine_table}")

    source_df = spark.createDataFrame(
        [("trip_1",), ("trip_2",)],
        schema=["trip_id"],
    )
    summary = write_quarantine(
        source_df,
        quarantine_table=quarantine_table,
        dataset="silver.trips_clean",
        contract_version=2,
        run_id="run-1",
    )

    assert summary["quarantined_rows"] == 2
    written_df = spark.table(quarantine_table)
    assert written_df.count() == 2

    schema_map = _schema_as_dict(written_df)
    required_columns = [
        "reason_code",
        "rule_id",
        "rule_name",
        "severity",
        "dataset",
        "contract_version",
        "run_id",
        "run_ts",
    ]
    for column_name in required_columns:
        assert column_name in schema_map

    assert schema_map["dataset"] == "string"
    assert schema_map["run_id"] == "string"
    assert schema_map["run_ts"] == "timestamp"
    assert schema_map["contract_version"] in {"int", "bigint"}

    observed_reason_codes = {
        row["reason_code"] for row in written_df.select("reason_code").distinct().collect()
    }
    observed_rule_ids = {
        row["rule_id"] for row in written_df.select("rule_id").distinct().collect()
    }
    observed_severities = {
        row["severity"] for row in written_df.select("severity").distinct().collect()
    }
    assert observed_reason_codes == {"contract_violation"}
    assert observed_rule_ids == {"contract_violation"}
    assert observed_severities == {"error"}

    empty_summary = write_quarantine(
        source_df.limit(0),
        quarantine_table=quarantine_table,
        dataset="silver.trips_clean",
        contract_version=2,
        run_id="run-2",
    )
    assert empty_summary["quarantined_rows"] == 0
    assert spark.table(quarantine_table).count() == 2
