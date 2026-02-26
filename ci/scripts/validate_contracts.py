from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator

ALLOWED_LAYERS = {"bronze", "silver", "gold"}
ALLOWED_SEVERITIES = {"error", "warn", "info"}


@dataclass(frozen=True)
class ContractFile:
    path: Path
    layer: str
    payload: dict[str, Any]


def _fail(message: str) -> None:
    print(f"[ERROR] {message}", file=sys.stderr)
    raise SystemExit(1)


def _load_json_schema(schema_path: Path) -> Draft202012Validator:
    if not schema_path.exists():
        _fail(f"Schema file not found: {schema_path}")

    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    validator = Draft202012Validator(schema)
    return validator


def _load_contracts(contracts_root: Path) -> list[ContractFile]:
    if not contracts_root.exists():
        _fail(f"Contracts directory not found: {contracts_root}")

    contracts: list[ContractFile] = []
    for layer_dir in contracts_root.iterdir():
        if not layer_dir.is_dir():
            continue

        layer = layer_dir.name
        if layer not in ALLOWED_LAYERS:
            continue

        for yml in sorted(layer_dir.glob("*.yml")):
            payload = yaml.safe_load(yml.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                _fail(f"Contract must be a YAML mapping/object: {yml}")
            contracts.append(ContractFile(path=yml, layer=layer, payload=payload))

    if not contracts:
        _fail("No contracts found under contracts/{bronze,silver,gold}/*.yml")

    return contracts


def _semantic_checks(cf: ContractFile) -> list[str]:
    """
    Semantic rules beyond JSON Schema validation.
    """
    errors: list[str] = []
    c = cf.payload

    # Ensure dataset is aligned with directory layer (soft rule but useful)
    dataset = str(c.get("dataset", ""))
    if dataset and not dataset.startswith(f"{cf.layer}."):
        errors.append(f"dataset '{dataset}' should start with '{cf.layer}.' for consistency")

    schema_cols = c.get("schema", [])
    col_names = [x.get("name") for x in schema_cols if isinstance(x, dict)]
    if len(col_names) != len(set(col_names)):
        errors.append("schema has duplicated column names")

    # PK rules
    pk = c.get("primary_key")
    if pk is not None:
        if not isinstance(pk, list) or not pk:
            errors.append("primary_key must be a non-empty list of column names")
        else:
            for k in pk:
                if k not in col_names:
                    errors.append(f"primary_key column '{k}' not found in schema")

    # Watermark rules (only if provided)
    watermark = c.get("watermark")
    if watermark is not None:
        if not isinstance(watermark, str) or not watermark.strip():
            errors.append("watermark must be a non-empty string")
        elif watermark not in col_names:
            errors.append(f"watermark column '{watermark}' not found in schema")

    # Late arrival days requires watermark
    if c.get("late_arrival_days") is not None and watermark is None:
        errors.append("late_arrival_days provided but watermark is missing")

    # Layer minimum expectations
    if cf.layer in {"silver", "gold"} and pk is None:
        errors.append(f"{cf.layer} contract should define primary_key")

    if cf.layer == "silver" and watermark is None:
        errors.append("silver contract should define watermark for incremental processing")

    expectations = c.get("expectations", [])
    if isinstance(expectations, list):
        for index, rule in enumerate(expectations):
            if not isinstance(rule, dict):
                errors.append(f"expectations[{index}] must be an object")
                continue

            rule_name = str(rule.get("name", "")).strip()
            if not rule_name:
                errors.append(f"expectations[{index}].name must be a non-empty string")

            severity = rule.get("severity")
            if severity is not None:
                normalized = str(severity).strip().lower()
                if normalized not in ALLOWED_SEVERITIES:
                    errors.append(
                        f"expectations[{index}].severity must be one of "
                        f"{sorted(ALLOWED_SEVERITIES)}"
                    )

            rule_id = rule.get("rule_id")
            if rule_id is not None and not str(rule_id).strip():
                errors.append(f"expectations[{index}].rule_id must not be empty when provided")

    return errors


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]  # ci/scripts -> repo root
    contracts_root = repo_root / "contracts"
    schema_path = repo_root / "ci" / "schemas" / "data_contract.schema.json"

    validator = _load_json_schema(schema_path)
    contracts = _load_contracts(contracts_root)

    total_errors = 0

    for cf in contracts:
        # JSON Schema validation
        schema_errors = sorted(validator.iter_errors(cf.payload), key=lambda e: e.path)
        if schema_errors:
            total_errors += 1
            print(f"\n[FAIL] {cf.path}")
            for e in schema_errors:
                loc = ".".join([str(x) for x in e.path]) if e.path else "<root>"
                print(f"  - schema error at {loc}: {e.message}")
            continue

        # Semantic validation
        sem_errors = _semantic_checks(cf)
        if sem_errors:
            total_errors += 1
            print(f"\n[FAIL] {cf.path}")
            for msg in sem_errors:
                print(f"  - semantic error: {msg}")
            continue

        print(f"[OK] {cf.path}")

    if total_errors > 0:
        _fail(f"Validation failed for {total_errors} contract file(s).")

    print("\nAll contracts validated successfully.")


if __name__ == "__main__":
    main()
