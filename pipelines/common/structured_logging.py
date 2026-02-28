from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class JsonPipelineLogger:
    def __init__(self, *, run_id: str, log_file: Path) -> None:
        self.run_id = run_id
        self.log_file = log_file
        self.log_file.parent.mkdir(parents=True, exist_ok=True)

    def event(
        self,
        *,
        level: str,
        stage: str,
        step: str,
        message: str,
        year: int | None = None,
        month: int | None = None,
        duration_ms: int | None = None,
        row_counts: dict[str, int] | None = None,
        invalid_ratio: float | None = None,
        exception: str | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "level": level.upper(),
            "run_id": self.run_id,
            "stage": stage,
            "step": step,
            "message": message,
            "year": year,
            "month": month,
            "duration_ms": duration_ms,
            "row_counts": row_counts,
            "invalid_ratio": invalid_ratio,
            "exception": exception,
        }
        line = json.dumps(payload, default=str, sort_keys=True)
        print(line, flush=True)
        with self.log_file.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
