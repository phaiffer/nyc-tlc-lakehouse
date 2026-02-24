from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from pipelines.common.delta_incremental_merge import incremental_merge
    from quality.observability.metrics_writer import write_pipeline_metrics
    from quality.reconciliation.reconciliation_checks import reconcile_row_counts

    _ = (incremental_merge, reconcile_row_counts, write_pipeline_metrics)
    print("Smoke imports succeeded.")


if __name__ == "__main__":
    main()
