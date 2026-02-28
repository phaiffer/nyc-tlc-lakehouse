# Empty Directories Policy

Date: 2026-02-28

## Policy

Empty directories are not tracked unless they carry clear ownership and implementation intent.
For this repository, each empty directory discovered during hardening is assigned one decision:

- `A` = remove from git scope (preferred for unused placeholders)
- `B` = keep and document purpose

## Decisions From This Hardening Pass

| Directory | Decision | Rationale |
| --- | --- | --- |
| `metastore_db/tmp` | B | Runtime directory created by local embedded metastore; lives under ignored `metastore_db/`. |
| `spark-warehouse` | A | Local Spark artifact folder in repository root; explicitly ignored to avoid accidental tracking. |
| `config/.gitkeep` | A | Removed because `config/` contains real files and no longer needs a placeholder marker. |

## Enforcement Notes

- Keep runtime artifacts under ignore rules (`.local/`, `lakehouse/`, `metastore_db/`, `spark-warehouse/`, `notebooks/spark-warehouse/`, `data/raw/`, `reports/`).
- Tracked empty directories are not allowed unless accompanied by a purpose-specific `README.md`.
- Optional dbt layer remains tracked and populated (`dbt/lakehouse_dbt` is not an empty placeholder).
