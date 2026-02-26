from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_detector_module():
    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / "ci" / "scripts" / "detect_contract_breaking_changes.py"
    module_name = "detect_contract_breaking_changes_under_test"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from path: {module_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def test_breaking_schema_change_requires_version_bump() -> None:
    detector = _load_detector_module()
    baseline = detector.Contract(
        dataset="silver.trips_clean",
        version_text="2",
        version_key=(2,),
        schema={
            "trip_id": detector.ColumnDef("trip_id", "string", False),
            "fare_amount": detector.ColumnDef("fare_amount", "double", True),
        },
        primary_key=("trip_id",),
        watermark="updated_at",
        source="baseline",
    )
    current_same_version = detector.Contract(
        dataset="silver.trips_clean",
        version_text="2",
        version_key=(2,),
        schema={
            "trip_id": detector.ColumnDef("trip_id", "string", False),
            "fare_amount": detector.ColumnDef("fare_amount", "decimal(18,2)", True),
        },
        primary_key=("trip_id",),
        watermark="updated_at",
        source="current",
    )

    reasons = detector._detect_breaking_reasons(baseline, current_same_version)
    assert reasons
    assert current_same_version.version_key <= baseline.version_key


def test_breaking_schema_change_with_bump_is_allowed_path() -> None:
    detector = _load_detector_module()
    baseline = detector.Contract(
        dataset="silver.trips_clean",
        version_text="2",
        version_key=(2,),
        schema={
            "trip_id": detector.ColumnDef("trip_id", "string", False),
            "fare_amount": detector.ColumnDef("fare_amount", "double", True),
        },
        primary_key=("trip_id",),
        watermark="updated_at",
        source="baseline",
    )
    current_bumped_version = detector.Contract(
        dataset="silver.trips_clean",
        version_text="3",
        version_key=(3,),
        schema={
            "trip_id": detector.ColumnDef("trip_id", "string", False),
            "fare_amount": detector.ColumnDef("fare_amount", "decimal(18,2)", True),
        },
        primary_key=("trip_id",),
        watermark="updated_at",
        source="current",
    )

    reasons = detector._detect_breaking_reasons(baseline, current_bumped_version)
    assert reasons
    assert current_bumped_version.version_key > baseline.version_key
