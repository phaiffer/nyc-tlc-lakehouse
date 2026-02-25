from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from orchestration.local.run_pipeline import (  # noqa: E402
    DEFAULT_WAREHOUSE_DIR,
    _build_spark_session,
    _resolve_spark_paths,
    inspect_catalog,
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Open local Spark session with project metastore/warehouse and list tables"
    )
    parser.add_argument(
        "--warehouse-dir",
        type=str,
        default=str(DEFAULT_WAREHOUSE_DIR),
        help="Warehouse directory (default: .local/spark-warehouse)",
    )
    args = parser.parse_args()

    paths = _resolve_spark_paths(warehouse_dir=args.warehouse_dir)
    spark = _build_spark_session(paths=paths)
    try:
        inspect_catalog(spark, paths=paths)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
