from __future__ import annotations

from pathlib import Path

import pytest

from orchestration.local.backfill_utils import (
    checkpoint_path,
    find_resume_checkpoint,
    initialize_checkpoint_payload,
    iter_month_range,
    month_token,
    next_incomplete_month,
    parse_month_token,
    resolve_backfill_range,
    write_checkpoint,
)


def test_parse_month_token_valid() -> None:
    assert parse_month_token("2024-01", flag_name="--from") == (2024, 1)


@pytest.mark.parametrize("token", ["2024-13", "2024-1", "24-01", "2024/01", "invalid"])
def test_parse_month_token_invalid(token: str) -> None:
    with pytest.raises(ValueError):
        parse_month_token(token, flag_name="--from")


def test_resolve_backfill_range_with_month_tokens() -> None:
    start, end = resolve_backfill_range(
        from_year=None,
        from_month=None,
        to_year=None,
        to_month=None,
        from_token="2024-01",
        to_token="2024-03",
    )
    assert start == (2024, 1)
    assert end == (2024, 3)
    assert iter_month_range(start=start, end=end) == [(2024, 1), (2024, 2), (2024, 3)]


def test_resolve_backfill_range_rejects_mixed_formats() -> None:
    with pytest.raises(ValueError):
        resolve_backfill_range(
            from_year=2024,
            from_month=1,
            to_year=2024,
            to_month=3,
            from_token="2024-01",
            to_token="2024-03",
        )


def test_checkpoint_resume_and_next_incomplete_month(tmp_path: Path) -> None:
    repo_root = tmp_path
    run_id = "resume-test-run"
    settings_snapshot = {
        "range_from": "2024-01",
        "range_to": "2024-03",
        "max_invalid_ratio": 0.001,
        "strict_quality": False,
        "warehouse_dir": "/tmp/warehouse",
        "input_parquet": None,
    }
    months_planned = ["2024-01", "2024-02", "2024-03"]
    payload = initialize_checkpoint_payload(
        run_id=run_id,
        settings_snapshot=settings_snapshot,
        months_planned=months_planned,
    )
    payload["months_completed"] = ["2024-01"]

    checkpoint_file = checkpoint_path(repo_root=repo_root, run_id=run_id)
    write_checkpoint(checkpoint_file=checkpoint_file, payload=payload)

    match = find_resume_checkpoint(repo_root=repo_root, settings_snapshot=settings_snapshot)
    assert match is not None

    matched_path, matched_payload = match
    assert matched_path == checkpoint_file
    assert matched_payload["run_id"] == run_id
    assert (
        next_incomplete_month(
            months_planned=matched_payload["months_planned"],
            months_completed=matched_payload["months_completed"],
        )
        == "2024-02"
    )


def test_find_resume_checkpoint_ignores_finished_runs(tmp_path: Path) -> None:
    repo_root = tmp_path
    settings_snapshot = {
        "range_from": "2024-01",
        "range_to": "2024-02",
        "max_invalid_ratio": 0.001,
        "strict_quality": False,
        "warehouse_dir": "/tmp/warehouse",
        "input_parquet": None,
    }
    payload = initialize_checkpoint_payload(
        run_id="finished-run",
        settings_snapshot=settings_snapshot,
        months_planned=[month_token(2024, 1), month_token(2024, 2)],
    )
    payload["finished_at"] = "2026-02-28T00:00:00+00:00"

    write_checkpoint(
        checkpoint_file=checkpoint_path(repo_root=repo_root, run_id="finished-run"),
        payload=payload,
    )
    assert find_resume_checkpoint(repo_root=repo_root, settings_snapshot=settings_snapshot) is None
