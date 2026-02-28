from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MONTH_TOKEN_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")


def utc_now_iso() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def month_token(year: int, month: int) -> str:
    validate_year_month(year=year, month=month, flag_prefix="month")
    return f"{year:04d}-{month:02d}"


def parse_month_token(value: str, *, flag_name: str) -> tuple[int, int]:
    token = str(value).strip()
    if not MONTH_TOKEN_PATTERN.fullmatch(token):
        raise ValueError(f"{flag_name} must be in YYYY-MM format with month between 01 and 12")
    year_str, month_str = token.split("-", 1)
    return int(year_str), int(month_str)


def validate_year_month(*, year: int, month: int, flag_prefix: str) -> None:
    if year < 1:
        raise ValueError(f"--{flag_prefix}-year must be >= 1")
    if month < 1 or month > 12:
        raise ValueError(f"--{flag_prefix}-month must be between 1 and 12")


def resolve_backfill_range(
    *,
    from_year: int | None,
    from_month: int | None,
    to_year: int | None,
    to_month: int | None,
    from_token: str | None,
    to_token: str | None,
) -> tuple[tuple[int, int], tuple[int, int]]:
    has_tuple_range = any(value is not None for value in [from_year, from_month, to_year, to_month])
    has_token_range = bool(from_token or to_token)

    if has_tuple_range and has_token_range:
        raise ValueError(
            "Use either --from-year/--from-month/--to-year/--to-month or --from/--to, not both"
        )

    if has_token_range:
        if not from_token or not to_token:
            raise ValueError("Both --from and --to are required when using YYYY-MM range format")
        from_ym = parse_month_token(from_token, flag_name="--from")
        to_ym = parse_month_token(to_token, flag_name="--to")
    else:
        if not has_tuple_range:
            raise ValueError(
                "Backfill range required: provide --from-year/--from-month/--to-year/--to-month "
                "or --from/--to"
            )
        missing = [
            name
            for name, value in [
                ("--from-year", from_year),
                ("--from-month", from_month),
                ("--to-year", to_year),
                ("--to-month", to_month),
            ]
            if value is None
        ]
        if missing:
            raise ValueError(f"Missing required range options: {', '.join(missing)}")
        assert from_year is not None
        assert from_month is not None
        assert to_year is not None
        assert to_month is not None
        validate_year_month(year=from_year, month=from_month, flag_prefix="from")
        validate_year_month(year=to_year, month=to_month, flag_prefix="to")
        from_ym = (from_year, from_month)
        to_ym = (to_year, to_month)

    if from_ym > to_ym:
        raise ValueError(
            "Invalid month range: start month must be earlier than or equal to end month"
        )

    return from_ym, to_ym


def iter_month_range(*, start: tuple[int, int], end: tuple[int, int]) -> list[tuple[int, int]]:
    if start > end:
        raise ValueError("Invalid month range: start must be <= end")
    year, month = start
    months: list[tuple[int, int]] = []
    while (year, month) <= end:
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


def checkpoint_dir(repo_root: Path) -> Path:
    return repo_root / ".local" / "checkpoints"


def checkpoint_path(*, repo_root: Path, run_id: str) -> Path:
    return checkpoint_dir(repo_root) / f"backfill_{run_id}.json"


def initialize_checkpoint_payload(
    *,
    run_id: str,
    settings_snapshot: dict[str, Any],
    months_planned: list[str],
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "started_at": utc_now_iso(),
        "finished_at": None,
        "months_completed": [],
        "current_month": None,
        "months_planned": months_planned,
        "settings_snapshot": settings_snapshot,
    }


def write_checkpoint(*, checkpoint_file: Path, payload: dict[str, Any]) -> None:
    checkpoint_file.parent.mkdir(parents=True, exist_ok=True)
    temp_file = checkpoint_file.with_suffix(".tmp")
    temp_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_file.replace(checkpoint_file)


def load_checkpoint(*, checkpoint_file: Path) -> dict[str, Any]:
    payload = json.loads(checkpoint_file.read_text(encoding="utf-8"))
    required_fields = {
        "run_id",
        "started_at",
        "finished_at",
        "months_completed",
        "current_month",
        "months_planned",
        "settings_snapshot",
    }
    missing = sorted(required_fields - set(payload))
    if missing:
        raise ValueError(
            f"Checkpoint file '{checkpoint_file}' missing required fields: {', '.join(missing)}"
        )
    return payload


def find_resume_checkpoint(
    *,
    repo_root: Path,
    settings_snapshot: dict[str, Any],
) -> tuple[Path, dict[str, Any]] | None:
    root = checkpoint_dir(repo_root)
    if not root.exists():
        return None

    candidates = sorted(root.glob("backfill_*.json"), key=lambda path: path.stat().st_mtime)
    for checkpoint_file in reversed(candidates):
        try:
            payload = load_checkpoint(checkpoint_file=checkpoint_file)
        except Exception:
            continue
        if payload.get("finished_at") is not None:
            continue
        if payload.get("settings_snapshot") != settings_snapshot:
            continue
        return checkpoint_file, payload
    return None


def next_incomplete_month(
    *,
    months_planned: list[str],
    months_completed: list[str],
) -> str | None:
    completed = set(months_completed)
    for token in months_planned:
        if token not in completed:
            return token
    return None


def purge_backfill_checkpoints(*, repo_root: Path) -> int:
    root = checkpoint_dir(repo_root)
    if not root.exists():
        return 0

    purged_count = 0
    for checkpoint_file in root.glob("backfill_*.json"):
        checkpoint_file.unlink(missing_ok=True)
        purged_count += 1
    return purged_count
