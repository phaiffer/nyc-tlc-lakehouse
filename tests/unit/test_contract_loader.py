from __future__ import annotations

from pathlib import Path

import pytest

from quality.validators.contract_loader import load_contract_by_dataset


def test_load_contract_applies_default_severity_and_rule_id(tmp_path: Path) -> None:
    contracts_dir = tmp_path / "contracts" / "silver"
    contracts_dir.mkdir(parents=True, exist_ok=True)
    (contracts_dir / "sample.yml").write_text(
        """
dataset: silver.sample
owner: data-platform
version: 1
primary_key: [trip_id]
watermark: updated_at
late_arrival_days: 1
schema:
  - name: trip_id
    type: string
    nullable: false
  - name: updated_at
    type: timestamp
    nullable: false
expectations:
  - name: required_not_null
    type: not_null
    columns: [trip_id]
""".strip(),
        encoding="utf-8",
    )

    contract = load_contract_by_dataset(tmp_path, "silver.sample")
    expectation = contract.expectations[0]
    assert expectation["severity"] == "error"
    assert expectation["rule_id"] == "required_not_null"


def test_load_contract_rejects_invalid_severity(tmp_path: Path) -> None:
    contracts_dir = tmp_path / "contracts" / "silver"
    contracts_dir.mkdir(parents=True, exist_ok=True)
    (contracts_dir / "sample.yml").write_text(
        """
dataset: silver.sample
owner: data-platform
version: 1
primary_key: [trip_id]
watermark: updated_at
late_arrival_days: 1
schema:
  - name: trip_id
    type: string
    nullable: false
  - name: updated_at
    type: timestamp
    nullable: false
expectations:
  - name: required_not_null
    rule_id: RULE_1
    severity: critical
    type: not_null
    columns: [trip_id]
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unsupported severity"):
        load_contract_by_dataset(tmp_path, "silver.sample")
