"""Lightweight local markdown link checker for repository docs."""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MARKDOWN_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
SKIP_SCHEMES = ("http://", "https://", "mailto:", "tel:", "data:", "#")
SKIP_PATH_PREFIXES = (
    ".git/",
    ".venv/",
    ".local/",
    "lakehouse/",
    "metastore_db/",
    "spark-warehouse/",
    "notebooks/spark-warehouse/",
    "dbt/lakehouse_dbt/target/",
    "dbt/lakehouse_dbt/dbt_packages/",
)


def in_skipped_path(path: Path) -> bool:
    rel = path.relative_to(ROOT).as_posix()
    return any(rel.startswith(prefix) for prefix in SKIP_PATH_PREFIXES)


def resolve_target(markdown_file: Path, link_target: str) -> Path:
    if link_target.startswith("/"):
        return (ROOT / link_target.lstrip("/")).resolve()
    return (markdown_file.parent / link_target).resolve()


def main() -> int:
    failures: list[str] = []

    for markdown_file in ROOT.rglob("*.md"):
        if in_skipped_path(markdown_file):
            continue

        text = markdown_file.read_text(encoding="utf-8")

        for raw_target in MARKDOWN_LINK.findall(text):
            target = raw_target.strip().strip("<>")
            if not target or target.startswith(SKIP_SCHEMES):
                continue

            target = target.split("#", 1)[0].split("?", 1)[0].strip()
            if not target:
                continue

            resolved = resolve_target(markdown_file, target)
            if not resolved.exists():
                rel_markdown = markdown_file.relative_to(ROOT).as_posix()
                failures.append(f"{rel_markdown}: missing link target '{raw_target}'")

    if failures:
        print("[docs-check] broken markdown links detected:")
        for failure in sorted(failures):
            print(f"- {failure}")
        return 1

    print("[docs-check] all markdown links resolved")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
