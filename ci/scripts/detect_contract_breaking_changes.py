from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

CONTRACTS_DIR = "contracts"
DEFAULT_BASELINE_REF = "HEAD~1"
SUPPORTED_SUFFIXES = (".yml", ".yaml")


@dataclass(frozen=True)
class ColumnDef:
    name: str
    dtype: str
    nullable: bool


@dataclass(frozen=True)
class Contract:
    dataset: str
    version_text: str
    version_key: tuple[int, ...]
    schema: dict[str, ColumnDef]
    primary_key: tuple[str, ...] | None
    watermark: str | None
    source: str


def _run_git(repo_root: Path, args: list[str]) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or "unknown git error"
        raise RuntimeError(f"git {' '.join(args)} failed: {stderr}")
    return result.stdout


def _normalize_version_parts(parts: list[int]) -> tuple[int, ...]:
    while len(parts) > 1 and parts[-1] == 0:
        parts.pop()
    return tuple(parts)


def _parse_version(value: Any, source: str) -> tuple[str, tuple[int, ...]]:
    if isinstance(value, bool):
        raise ValueError(f"{source}: version must be an integer or dotted numeric string")
    if isinstance(value, int):
        if value < 0:
            raise ValueError(f"{source}: version must be >= 0")
        return str(value), (value,)
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            raise ValueError(f"{source}: version must not be empty")
        parts = raw.split(".")
        if any(not part.isdigit() for part in parts):
            raise ValueError(
                f"{source}: version '{value}' must contain digits only or dotted digits "
                "(for example: 2 or 2.1)"
            )
        numeric_parts = [int(part) for part in parts]
        return raw, _normalize_version_parts(numeric_parts)
    raise ValueError(f"{source}: version must be an integer or dotted numeric string")


def _parse_schema(payload: dict[str, Any], source: str) -> dict[str, ColumnDef]:
    raw_schema = payload.get("schema", [])
    if raw_schema is None:
        raw_schema = []
    if not isinstance(raw_schema, list):
        raise ValueError(f"{source}: schema must be a list")

    columns: dict[str, ColumnDef] = {}
    for index, raw_col in enumerate(raw_schema):
        pointer = f"{source}: schema[{index}]"
        if not isinstance(raw_col, dict):
            raise ValueError(f"{pointer} must be a mapping/object")

        name = raw_col.get("name")
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"{pointer}.name must be a non-empty string")
        name = name.strip()

        col_type = raw_col.get("type")
        if not isinstance(col_type, str) or not col_type.strip():
            raise ValueError(f"{pointer}.type must be a non-empty string")
        col_type = col_type.strip()

        nullable = raw_col.get("nullable", True)
        if not isinstance(nullable, bool):
            raise ValueError(f"{pointer}.nullable must be a boolean")

        if name in columns:
            raise ValueError(f"{source}: duplicated column name in schema: '{name}'")

        columns[name] = ColumnDef(name=name, dtype=col_type, nullable=nullable)

    return columns


def _parse_primary_key(payload: dict[str, Any], source: str) -> tuple[str, ...] | None:
    raw_pk = payload.get("primary_key")
    if raw_pk is None:
        return None
    if not isinstance(raw_pk, list):
        raise ValueError(f"{source}: primary_key must be a list of column names")

    keys: list[str] = []
    for index, value in enumerate(raw_pk):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{source}: primary_key[{index}] must be a non-empty string")
        keys.append(value.strip())

    return tuple(keys)


def _parse_watermark(payload: dict[str, Any], source: str) -> str | None:
    raw_watermark = payload.get("watermark")
    if raw_watermark is None:
        return None
    if not isinstance(raw_watermark, str) or not raw_watermark.strip():
        raise ValueError(f"{source}: watermark must be a non-empty string when provided")
    return raw_watermark.strip()


def _parse_contract(payload: Any, source: str) -> Contract:
    if not isinstance(payload, dict):
        raise ValueError(f"{source}: contract YAML must be a mapping/object")

    dataset = payload.get("dataset")
    if not isinstance(dataset, str) or not dataset.strip():
        raise ValueError(f"{source}: dataset must be a non-empty string")
    dataset = dataset.strip()

    if "version" not in payload:
        raise ValueError(f"{source}: missing required field 'version'")
    version_text, version_key = _parse_version(payload.get("version"), source)

    schema = _parse_schema(payload, source)
    primary_key = _parse_primary_key(payload, source)
    watermark = _parse_watermark(payload, source)

    return Contract(
        dataset=dataset,
        version_text=version_text,
        version_key=version_key,
        schema=schema,
        primary_key=primary_key,
        watermark=watermark,
        source=source,
    )


def _load_contracts_from_worktree(repo_root: Path) -> tuple[dict[str, Contract], list[str]]:
    contracts_root = repo_root / CONTRACTS_DIR
    if not contracts_root.exists():
        return {}, [f"Contracts directory not found: {contracts_root}"]

    paths = sorted(
        path
        for path in contracts_root.rglob("*")
        if path.is_file() and path.suffix.lower() in SUPPORTED_SUFFIXES
    )
    if not paths:
        return {}, [f"No contract files found under {contracts_root}"]

    contracts: dict[str, Contract] = {}
    errors: list[str] = []

    for path in paths:
        source = str(path.relative_to(repo_root))
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
        except (OSError, yaml.YAMLError) as exc:
            errors.append(f"{source}: failed to load YAML ({exc})")
            continue

        try:
            contract = _parse_contract(raw, source)
        except ValueError as exc:
            errors.append(str(exc))
            continue

        if contract.dataset in contracts:
            first_source = contracts[contract.dataset].source
            errors.append(
                f"{source}: duplicate dataset '{contract.dataset}' "
                f"already defined in {first_source}"
            )
            continue

        contracts[contract.dataset] = contract

    return contracts, errors


def _load_contracts_from_ref(repo_root: Path, ref: str) -> tuple[dict[str, Contract], list[str]]:
    try:
        listed = _run_git(repo_root, ["ls-tree", "-r", "--name-only", ref, CONTRACTS_DIR])
    except RuntimeError as exc:
        return {}, [f"Failed to list baseline contracts at ref '{ref}': {exc}"]

    paths = sorted(
        line.strip() for line in listed.splitlines() if line.strip().endswith(SUPPORTED_SUFFIXES)
    )

    contracts: dict[str, Contract] = {}
    errors: list[str] = []

    for rel_path in paths:
        source = f"{ref}:{rel_path}"
        try:
            raw_text = _run_git(repo_root, ["show", f"{ref}:{rel_path}"])
        except RuntimeError as exc:
            errors.append(f"{source}: failed to read baseline file ({exc})")
            continue

        try:
            raw = yaml.safe_load(raw_text)
        except yaml.YAMLError as exc:
            errors.append(f"{source}: failed to parse YAML ({exc})")
            continue

        try:
            contract = _parse_contract(raw, source)
        except ValueError as exc:
            errors.append(str(exc))
            continue

        if contract.dataset in contracts:
            first_source = contracts[contract.dataset].source
            errors.append(
                f"{source}: duplicate dataset '{contract.dataset}' "
                f"already defined in {first_source}"
            )
            continue

        contracts[contract.dataset] = contract

    return contracts, errors


def _format_pk(pk: tuple[str, ...] | None) -> str:
    if pk is None:
        return "null"
    return "[" + ", ".join(pk) + "]"


def _detect_breaking_reasons(baseline: Contract, current: Contract) -> list[str]:
    reasons: list[str] = []

    for name, base_col in baseline.schema.items():
        current_col = current.schema.get(name)
        if current_col is None:
            reasons.append(f"column removed: {name}")
            continue

        if current_col.dtype != base_col.dtype:
            reasons.append(
                f"column type changed for '{name}': {base_col.dtype} -> {current_col.dtype}"
            )

        if base_col.nullable and not current_col.nullable:
            reasons.append(f"column nullability tightened for '{name}': true -> false")

    if baseline.primary_key != current.primary_key:
        reasons.append(
            "primary_key changed: "
            f"{_format_pk(baseline.primary_key)} -> {_format_pk(current.primary_key)}"
        )

    if baseline.watermark is not None:
        if current.watermark is None:
            reasons.append(f"watermark removed: {baseline.watermark}")
        elif baseline.watermark != current.watermark:
            reasons.append(f"watermark changed: {baseline.watermark} -> {current.watermark}")

    return reasons


def main() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    baseline_ref = os.getenv("BASE_REF", DEFAULT_BASELINE_REF)
    head_ref = os.getenv("HEAD_REF", "WORKTREE")

    print("Contract breaking-change detection")
    print(f"Baseline ref: {baseline_ref}")
    print(f"Head ref: {head_ref}")

    current_contracts, current_errors = _load_contracts_from_worktree(repo_root)
    baseline_contracts, baseline_errors = _load_contracts_from_ref(repo_root, baseline_ref)

    all_errors = [*current_errors, *baseline_errors]
    if all_errors:
        print("\nInput validation errors:")
        for err in all_errors:
            print(f"- {err}")
        raise SystemExit(1)

    baseline_datasets = set(baseline_contracts)
    current_datasets = set(current_contracts)

    new_datasets = sorted(current_datasets - baseline_datasets)
    removed_datasets = sorted(baseline_datasets - current_datasets)
    shared_datasets = sorted(current_datasets & baseline_datasets)

    print(f"\nBaseline datasets: {len(baseline_datasets)}")
    print(f"Current datasets: {len(current_datasets)}")

    if new_datasets:
        print("\nNew datasets (non-breaking by default):")
        for dataset in new_datasets:
            print(f"- {dataset}")

    warnings: list[str] = []
    failures: list[str] = []

    for dataset in removed_datasets:
        baseline = baseline_contracts[dataset]
        failures.append(
            f"{dataset}: dataset removed from PR branch (baseline source: {baseline.source}). "
            "Dataset removals are always breaking in this gate."
        )

    for dataset in shared_datasets:
        baseline = baseline_contracts[dataset]
        current = current_contracts[dataset]
        reasons = _detect_breaking_reasons(baseline, current)
        if not reasons:
            continue

        reason_text = "; ".join(reasons)
        version_text = f"baseline={baseline.version_text}, current={current.version_text}"

        if current.version_key > baseline.version_key:
            warnings.append(
                f"{dataset}: breaking changes allowed with version bump ({version_text}). "
                f"Reasons: {reason_text}"
            )
        else:
            failures.append(
                f"{dataset}: breaking changes require version bump ({version_text}). "
                f"Reasons: {reason_text}"
            )

    if warnings:
        print("\nWarnings (breaking changes with version bump):")
        for item in warnings:
            print(f"- {item}")

    if failures:
        print("\nErrors (breaking changes without required version bump):")
        for item in failures:
            print(f"- {item}")
        raise SystemExit(1)

    print("\nNo unversioned breaking changes detected.")


if __name__ == "__main__":
    main()
