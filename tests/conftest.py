from __future__ import annotations

import os
import sys
from pathlib import Path

# See orchestration/local/run_pipeline.py for why this is needed: on Windows,
# "python"/"python3" on PATH can resolve to the Microsoft Store stub, which
# silently kills any Spark Python worker with no Python traceback. Pin
# workers to this interpreter so tests don't depend on global PATH state.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

import pytest  # noqa: E402
from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


@pytest.fixture(scope="session")
def spark(tmp_path_factory: pytest.TempPathFactory) -> SparkSession:
    workspace = tmp_path_factory.mktemp("spark-tests")
    warehouse_dir = workspace / "warehouse"
    metastore_dir = workspace / "metastore_db"
    spark_local_dir = workspace / "spark-local"

    warehouse_dir.mkdir(parents=True, exist_ok=True)
    spark_local_dir.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName("nyc-tlc-lakehouse-tests")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(warehouse_dir.resolve()))
        .config(
            "spark.hadoop.javax.jdo.option.ConnectionURL",
            f"jdbc:derby:;databaseName={metastore_dir.resolve()};create=true",
        )
        .config("spark.local.dir", str(spark_local_dir.resolve()))
        .config("spark.ui.showConsoleProgress", "false")
    )
    session = configure_spark_with_delta_pip(builder).enableHiveSupport().getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
