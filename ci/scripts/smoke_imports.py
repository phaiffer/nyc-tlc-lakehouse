from __future__ import annotations

import sys
from pathlib import Path


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))

    from orchestration.local.run_pipeline import main as run_pipeline_main
    from pipelines.common.delta_incremental_merge import incremental_merge
    from pipelines.gold_marts.run_gold_enforced import run_gold_enforced
    from pipelines.silver_transform.run_silver_enforced import run_silver_enforced
    from quality.observability.metrics_writer import write_pipeline_metrics
    from quality.reconciliation.reconciliation_checks import reconcile_row_counts
    from quality.validators.contract_loader import DataContract, load_contract_by_dataset
    from quality.validators.quality_gate import run_quality_gate
    from quality.validators.quarantine_writer import write_quarantine
    from quality.validators.spark_contract_enforcer import enforce_contract

    _ = (
        incremental_merge,
        reconcile_row_counts,
        write_pipeline_metrics,
        enforce_contract,
        write_quarantine,
        load_contract_by_dataset,
        DataContract,
        run_silver_enforced,
        run_gold_enforced,
        run_quality_gate,
        run_pipeline_main,
    )
    print("Smoke imports succeeded.")


if __name__ == "__main__":
    main()
