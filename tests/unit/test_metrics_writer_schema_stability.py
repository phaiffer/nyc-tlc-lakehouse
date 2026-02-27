from __future__ import annotations

from quality.observability.metrics_writer import write_pipeline_metrics


def _schema_signature(table_df) -> list[tuple[str, str]]:
    return [(field.name, field.dataType.simpleString()) for field in table_df.schema.fields]


def test_metrics_writer_output_schema_is_stable_across_reruns(spark) -> None:
    spark.sql("CREATE DATABASE IF NOT EXISTS quality")
    metrics_table = "quality.pipeline_metrics_schema_stability"
    spark.sql(f"DROP TABLE IF EXISTS {metrics_table}")

    write_pipeline_metrics(
        spark,
        metrics_table=metrics_table,
        run_id="metrics-schema-1",
        pipeline_name="silver_transform_enforced",
        dataset="silver.trips_clean",
        contract_version=2,
        metrics={"rows_source_filtered": 2, "rows_deduped": 2},
    )
    first_signature = _schema_signature(spark.table(metrics_table))

    write_pipeline_metrics(
        spark,
        metrics_table=metrics_table,
        run_id="metrics-schema-2",
        pipeline_name="gold_marts_enforced",
        dataset="gold.fct_trips_daily",
        contract_version=2,
        metrics={"rows_source_filtered": 3, "rows_deduped": 3},
    )
    second_signature = _schema_signature(spark.table(metrics_table))

    expected_signature = [
        ("run_ts", "timestamp"),
        ("run_id", "string"),
        ("pipeline_name", "string"),
        ("dataset", "string"),
        ("contract_version", "int"),
        ("metrics_json", "string"),
    ]

    assert first_signature == expected_signature
    assert second_signature == expected_signature
    assert spark.table(metrics_table).count() == 2
