from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ALLOWED_RULE_SEVERITIES = {"error", "warn", "info"}


@dataclass(frozen=True)
class ContractColumn:
    name: str
    dtype: str
    nullable: bool


@dataclass(frozen=True)
class DataContract:
    dataset: str
    owner: str
    version: int
    schema: list[ContractColumn]
    primary_key: list[str] | None
    watermark: str | None
    late_arrival_days: int | None
    expectations: list[dict[str, Any]]


def _normalize_expectation(raw_rule: dict[str, Any]) -> dict[str, Any]:
    normalized_rule = dict(raw_rule)

    rule_name = str(normalized_rule.get("name", "")).strip()
    if not rule_name:
        raise ValueError("Expectation rule must include a non-empty 'name'")

    severity = str(normalized_rule.get("severity", "error")).strip().lower()
    if not severity:
        severity = "error"
    if severity not in ALLOWED_RULE_SEVERITIES:
        raise ValueError(
            f"Expectation '{rule_name}' has unsupported severity='{severity}'. "
            f"Allowed values: {sorted(ALLOWED_RULE_SEVERITIES)}"
        )

    rule_id = str(normalized_rule.get("rule_id", "")).strip() or rule_name

    normalized_rule["name"] = rule_name
    normalized_rule["severity"] = severity
    normalized_rule["rule_id"] = rule_id
    return normalized_rule


def load_contract_by_dataset(repo_root: Path, dataset: str) -> DataContract:
    """
    Load a contract YAML from contracts/{bronze|silver|gold} by matching the 'dataset' field.
    """
    contracts_root = repo_root / "contracts"
    if not contracts_root.exists():
        raise FileNotFoundError(f"Contracts directory not found: {contracts_root}")

    contract_files = list(contracts_root.rglob("*.yml"))
    if not contract_files:
        raise FileNotFoundError(f"No contract YAML files found under: {contracts_root}")

    for path in contract_files:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            continue

        if str(payload.get("dataset", "")).strip() != dataset:
            continue

        schema_raw = payload.get("schema", [])
        schema: list[ContractColumn] = []
        for col in schema_raw:
            schema.append(
                ContractColumn(
                    name=str(col["name"]),
                    dtype=str(col["type"]),
                    nullable=bool(col["nullable"]),
                )
            )

        raw_expectations = list(payload.get("expectations", []))
        expectations: list[dict[str, Any]] = []
        for raw_expectation in raw_expectations:
            if not isinstance(raw_expectation, dict):
                raise ValueError(
                    f"Contract '{dataset}' contains a non-mapping expectation entry: "
                    f"{raw_expectation!r}"
                )
            expectations.append(_normalize_expectation(raw_expectation))

        return DataContract(
            dataset=str(payload["dataset"]),
            owner=str(payload["owner"]),
            version=int(payload["version"]),
            schema=schema,
            primary_key=list(payload["primary_key"]) if payload.get("primary_key") else None,
            watermark=str(payload["watermark"]) if payload.get("watermark") else None,
            late_arrival_days=int(payload["late_arrival_days"])
            if payload.get("late_arrival_days") is not None
            else None,
            expectations=expectations,
        )

    raise FileNotFoundError(f"Contract not found for dataset='{dataset}'")
