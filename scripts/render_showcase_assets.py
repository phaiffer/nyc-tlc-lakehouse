"""Render showcase SVGs from real BI exports, pipeline logs and matching demo reports.

Example:
    python scripts/render_showcase_assets.py --months 2024-01 2024-02
    python scripts/render_showcase_assets.py --snapshot docs/showcase_results.json

The small JSON snapshot contains aggregate evidence only, never raw trip records.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates  # noqa: E402
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
import yaml  # noqa: E402
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
INK = "#172b46"
MUTED = "#52647a"
BLUE = "#245fbd"
TEAL = "#087f8c"
BG = "#f6f8fc"
RULE_LABELS = {
    "missing_pickup_ts": "Embarque ausente",
    "missing_dropoff_ts": "Desembarque ausente",
    "non_positive_duration": "Duração não positiva",
    "missing_pickup_date": "Data de embarque ausente",
    "negative_or_null_fare": "Tarifa negativa ou ausente",
    "negative_or_null_distance": "Distância negativa ou ausente",
    "duplicate_trip_id": "Trip ID duplicado",
}
QUALITY_LABELS = {
    "QG001": "Datas dentro da janela",
    "QG002": "Duração positiva",
    "QG003": "Tarifa / distância não negativas",
    "QG004": "Chaves sem duplicatas",
    "QG005": "Gold com volume positivo",
    "QG006": "Tarifa média dentro dos limites",
}


def number(value: int) -> str:
    return f"{int(value):,}".replace(",", ".")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def read_export(directory: Path, table: str) -> pd.DataFrame:
    return pd.read_parquet(directory / f"{table.replace('.', '_')}.parquet")


def summarize_gold_vendors(evidence: dict) -> list[dict]:
    """Measure actual Gold groups and trips, including absent vendor/month pairs."""
    daily = evidence["gold_daily"]
    vendors = sorted({row["vendor_id"] for row in daily})
    summary = []
    for run in evidence["runs"]:
        for vendor in vendors:
            rows = [
                row
                for row in daily
                if row["trip_date"][:7] == run["month"] and row["vendor_id"] == vendor
            ]
            summary.append(
                {
                    "month": run["month"],
                    "vendor_id": vendor,
                    "gold_rows": len(rows),
                    "days": len({row["trip_date"] for row in rows}),
                    "trips": sum(row["trips"] for row in rows),
                }
            )
    return summary


def gold_grain_notes(evidence: dict) -> tuple[str, str]:
    """Explain the measured row-volume change using day coverage and vendor presence."""
    summary = evidence["gold_vendor_summary"]
    months = [run["month"] for run in evidence["runs"]]
    days = {
        month: len(
            {row["trip_date"] for row in evidence["gold_daily"] if row["trip_date"][:7] == month}
        )
        for month in months
    }
    full_vendors = {
        row["vendor_id"]
        for row in summary
        if all(
            item["gold_rows"] == days[item["month"]]
            for item in summary
            if item["vendor_id"] == row["vendor_id"]
        )
    }
    partial_vendors = sorted({row["vendor_id"] for row in summary} - full_vendors)
    vendor_label = "VendorID " + ", ".join(partial_vendors) if partial_vendors else "outros"
    expressions = []
    totals = []
    for month in months:
        rows = [row for row in summary if row["month"] == month]
        partial_groups = sum(
            row["gold_rows"] for row in rows if row["vendor_id"] not in full_vendors
        )
        total_groups = sum(row["gold_rows"] for row in rows)
        expressions.append(
            f"{month}: {days[month]} × {len(full_vendors)} + {partial_groups} = {total_groups}"
        )
        totals.append(sum(row["trips"] for row in rows))
    trip_change = f"{totals[-1] - totals[0]:+,}".replace(",", ".")
    return (
        "Grupos: " + "; ".join(expressions) + f". Parcela adicional: {vendor_label}.",
        "O sinal reflete calendário e presença de fornecedores. Viagens: "
        f"{number(totals[0])} → {number(totals[-1])} ({trip_change}).",
    )


def compare_profiles(previous: dict, current: dict, thresholds: dict) -> list[dict]:
    """Expose below-threshold comparisons too, using the detector's stored profiles."""
    baseline = {item["dataset"]: item for item in previous["drift_profiles"]}
    comparisons = []
    columns = {
        "volume_ratio": "volume",
        "avg_fare_amount_ratio": "avg_fare_amount",
        "avg_fare_per_trip_ratio": "avg_fare_amount",
        "avg_trip_distance_ratio": "avg_trip_distance",
    }
    for profile in current["drift_profiles"]:
        dataset = profile["dataset"]
        prior = baseline[dataset]
        for metric, limits in thresholds[dataset].items():
            if metric == "passenger_distribution_l1":
                before = json.loads(prior["passenger_distribution_json"])
                after = json.loads(profile["passenger_distribution_json"])
                delta = sum(abs(after.get(key, 0) - before.get(key, 0)) for key in before | after)
            else:
                before, after = prior[columns[metric]], profile[columns[metric]]
                require(before is not None and after is not None, f"Missing drift input: {metric}")
                delta = abs(after - before) / abs(before) if before else float(after != 0)
            severity = (
                "error" if delta >= limits["error"] else "warn" if delta >= limits["warn"] else None
            )
            comparisons.append(
                {
                    "dataset": dataset,
                    "metric": metric,
                    "baseline_run_id": previous["run_id"],
                    "baseline_value": before,
                    "observed_value": after,
                    "delta_ratio": delta,
                    "thresholds": limits,
                    "severity": severity,
                }
            )
    expected = {
        (item["dataset"], item["metric"], item["severity"])
        for item in comparisons
        if item["severity"]
    }
    recorded = {
        (item["dataset"], item["metric_name"], item["severity"])
        for item in current["drift_events"]
        if item["event_type"] == "drift_detected"
    }
    require(expected == recorded, "Computed drift comparisons differ from persisted events")
    return comparisons


def collect_evidence(args) -> dict:
    metrics = read_export(args.exports, "quality.pipeline_metrics")
    quality = read_export(args.exports, "quality.violations_summary")
    gold = read_export(args.exports, "gold.fct_trips_daily")
    profiles = read_export(args.exports, "quality.drift_baseline_metrics")
    events = read_export(args.exports, "quality.drift_events")
    thresholds = yaml.safe_load((ROOT / "config/drift_thresholds.yml").read_text())["datasets"]
    metrics = metrics.assign(parsed=metrics.metrics_json.map(json.loads))
    runs = []
    for token in args.months:
        year, month = map(int, token.split("-"))
        window = {"year": year, "month": month}
        scoped = metrics[metrics.parsed.map(lambda item, window=window: item["window"] == window)]
        stages = {}
        ids = []
        for dataset in ("silver.trips_clean", "gold.fct_trips_daily"):
            selected = scoped[scoped.dataset == dataset].sort_values("run_ts")
            require(not selected.empty, f"Missing metrics: {dataset}, {token}")
            row = selected.iloc[-1]
            stages[dataset] = row.parsed
            ids.append(row.run_id)
        require(len(set(ids)) == 1, f"Stage run_id mismatch: {token}")
        run_id = ids[0]
        log_path = args.logs / f"pipeline_{run_id}.jsonl"
        log = [json.loads(line) for line in log_path.read_text().splitlines()]
        log = [item for item in log if item["year"] == year and item["month"] == month]
        require(
            any(item["step"] == "command_complete" for item in log),
            f"Pipeline did not complete: {token}",
        )
        bronze = next(
            item["row_counts"]["bronze_rows"]
            for item in log
            if item["stage"] == "bronze" and item["step"] == "complete"
        )
        silver = stages["silver.trips_clean"]
        accounting = silver["row_accounting"]
        removals = silver["cleaning_summary"]["removed_by_rule"]
        require(accounting["input_count"] == bronze, f"Bronze count mismatch: {token}")
        require(accounting["balanced"], f"Unbalanced accounting: {token}")
        require(
            bronze - accounting["output_count"]
            == sum(removals.values())
            + accounting["month_window_excluded_count"]
            + accounting["contract_rejected_count"],
            f"Unexplained removals: {token}",
        )
        for stage in stages.values():
            report = stage["reconciliation_report"]
            require(report["passed"] and report["count_diff"] == 0, f"Reconciliation: {token}")
        month_gold = gold[pd.to_datetime(gold.trip_date).dt.strftime("%Y-%m") == token]
        require(int(month_gold.trips.sum()) == accounting["output_count"], f"Gold trips: {token}")
        require(
            len(month_gold) == stages["gold.fct_trips_daily"]["enforcement_summary"]["total_count"],
            f"Gold row count mismatch: {token}",
        )
        rules = quality[
            (quality.run_id == run_id)
            & (quality.window_year == year)
            & (quality.window_month == month)
        ].sort_values("rule_id")
        require(not rules.empty and not rules.rule_id.duplicated().any(), f"Quality rules: {token}")
        month_profiles = profiles[
            (profiles.run_id == run_id)
            & (profiles.window_year == year)
            & (profiles.window_month == month)
        ].sort_values("dataset")
        require(set(month_profiles.dataset) == set(stages), f"Drift profiles missing: {token}")
        require(
            int(month_profiles[month_profiles.dataset == "silver.trips_clean"].iloc[0].volume)
            == accounting["output_count"],
            f"Drift used accumulated Silver instead of monthly data: {token}",
        )
        month_events = events[
            (events.run_id == run_id)
            & (events.window_year == year)
            & (events.window_month == month)
        ]
        report_path = args.reports / f"demo_{run_id}.txt"
        report_fields = dict(
            line.split("=", 1) for line in report_path.read_text().splitlines() if "=" in line
        )
        require(report_fields["run_id"] == run_id, f"Report run_id mismatch: {token}")
        require(report_fields["window"] == token, f"Report window mismatch: {token}")
        for key, expected in (
            ("bronze_events_count", bronze),
            ("silver_trips_count", accounting["output_count"]),
            ("gold_daily_fact_row_count", len(month_gold)),
        ):
            require(int(report_fields[key]) == expected, f"Report {key} mismatch: {token}")
        source = ROOT / "data/raw" / f"yellow_tripdata_{token}.parquet"
        with source.open("rb") as source_file:
            source_sha256 = hashlib.file_digest(source_file, "sha256").hexdigest()
        runs.append(
            {
                "month": token,
                "run_id": run_id,
                "report_run_id": report_fields["run_id"],
                "source_file": source.name,
                "source_sha256": source_sha256,
                "pipeline_command": f"make run YEAR={year} MONTH={month}",
                "report_command": (
                    f".venv/bin/python scripts/demo_report.py --year {year} --month {month}"
                ),
                "bronze_rows": bronze,
                "silver_rows": accounting["output_count"],
                "gold_rows": len(month_gold),
                "removed_by_rule": removals,
                "row_accounting": accounting,
                "quality_rules": json.loads(
                    rules[
                        ["rule_id", "rule_name", "severity", "failed_count", "threshold", "passed"]
                    ].to_json(orient="records")
                ),
                "drift_profiles": json.loads(month_profiles.to_json(orient="records")),
                "drift_events": json.loads(month_events.to_json(orient="records")),
            }
        )
    selected_gold = gold[pd.to_datetime(gold.trip_date).dt.strftime("%Y-%m").isin(args.months)]
    selected_gold = selected_gold.sort_values(["trip_date", "vendor_id"]).copy()
    selected_gold["trip_date"] = pd.to_datetime(selected_gold.trip_date).dt.strftime("%Y-%m-%d")
    selected_gold["total_fare"] = selected_gold.total_fare.map(str)
    for previous, current in zip(runs[:-1], runs[1:], strict=True):
        current["drift_comparisons"] = compare_profiles(previous, current, thresholds)
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "commands": {
            "export": "make export-bi EXPORT_DIR=.local/showcase/exports",
            "render": ".venv/bin/python scripts/render_showcase_assets.py --months "
            + " ".join(args.months),
        },
        "versions": {
            package: version(package)
            for package in ("pyspark", "delta-spark", "pandas", "matplotlib", "ruff", "pytest")
        },
        "drift_thresholds": thresholds,
        "runs": runs,
        "gold_daily": json.loads(selected_gold.to_json(orient="records")),
    }


def save_figure(fig, name: str, args, description: str) -> None:
    args.output_dir.mkdir(parents=True, exist_ok=True)
    svg_path = args.output_dir / f"{name}.svg"
    fig.savefig(
        svg_path,
        facecolor=fig.get_facecolor(),
        metadata={"Date": None, "Title": description, "Description": description},
    )
    # Matplotlib leaves spaces at the ends of SVG path-data lines.
    svg_path.write_text(
        "\n".join(line.rstrip() for line in svg_path.read_text(encoding="utf-8").splitlines())
        + "\n",
        encoding="utf-8",
    )
    if args.preview_dir:
        args.preview_dir.mkdir(parents=True, exist_ok=True)
        fig.savefig(args.preview_dir / f"{name}.png", dpi=160, facecolor=fig.get_facecolor())
    plt.close(fig)


def canvas(title: str, subtitle: str, height: float):
    fig = plt.figure(figsize=(14, height), facecolor=BG)
    ax = fig.add_axes((0, 0, 1, 1))
    ax.set(xlim=(0, 1), ylim=(0, 1))
    ax.axis("off")
    ax.text(0.045, 0.94, title, fontsize=23, color=INK, weight="bold", va="top")
    ax.text(0.045, 0.885, subtitle, fontsize=11, color=MUTED, va="top")
    return fig, ax


def architecture(args):
    fig, ax = canvas(
        "NYC TLC Lakehouse | arquitetura local",
        "Spark + Delta Lake • catálogo Hive embarcado • contratos versionados",
        6,
    )

    def box(x, y, w, title, text, accent=BLUE):
        ax.add_patch(
            FancyBboxPatch(
                (x, y),
                w,
                0.23,
                boxstyle="round,pad=0.008,rounding_size=0.012",
                facecolor="white",
                edgecolor="#dbe3ee",
                linewidth=1.3,
            )
        )
        ax.text(x + 0.015, y + 0.185, title, color=accent, fontsize=14, weight="bold")
        ax.text(x + 0.015, y + 0.13, text, color=INK, fontsize=10, linespacing=1.8, va="top")

    def arrow(start, end):
        ax.add_patch(FancyArrowPatch(start, end, arrowstyle="-|>", mutation_scale=14, color=MUTED))

    box(0.045, 0.53, 0.19, "Fonte pública", "NYC TLC • yellow\nParquet mensal")
    box(0.28, 0.53, 0.19, "Bronze", "events_raw\nNormalização + janela\nSobrescrita mensal")
    box(0.515, 0.53, 0.19, "Silver", "trips_clean\nLimpeza + deduplicação\nContrato + merge Delta")
    box(
        0.75,
        0.53,
        0.205,
        "Gold",
        "fct_trips_daily + dimensões\nData × fornecedor\nKPIs + merge Delta",
    )
    for left in (0.235, 0.47, 0.705):
        arrow((left, 0.65), (left + 0.045, 0.65))
    box(
        0.045,
        0.13,
        0.25,
        "Rastreabilidade",
        "Métricas de limpeza por regra\nQuarentena de contratos\nLogs com run_id",
        TEAL,
    )
    box(
        0.355,
        0.13,
        0.29,
        "Qualidade + drift",
        "Gate sobre Silver / Gold do mês\n"
        "Perfis mensais + base anterior\nViolações e eventos persistidos",
        TEAL,
    )
    box(
        0.705,
        0.13,
        0.25,
        "Exportação BI",
        "Gold + tabelas de qualidade\nParquet por tabela\nscripts/export_for_bi.py",
        TEAL,
    )
    arrow((0.61, 0.53), (0.53, 0.36))
    arrow((0.85, 0.53), (0.60, 0.36))
    arrow((0.55, 0.53), (0.19, 0.36))
    arrow((0.295, 0.245), (0.355, 0.245))
    arrow((0.644, 0.245), (0.705, 0.245))
    arrow((0.92, 0.53), (0.92, 0.36))
    ax.text(
        0.045,
        0.055,
        "Execução local; dbt é uma camada opcional fora deste fluxo.",
        color=MUTED,
        fontsize=10,
    )
    save_figure(fig, "architecture", args, "Arquitetura do fluxo local NYC TLC Lakehouse")


def daily_chart(evidence: dict, args):
    runs = evidence["runs"]
    fig, heading = canvas(
        "Viagens por dia e fornecedor",
        f"NYC TLC yellow • {runs[0]['month']} a {runs[-1]['month']} • Gold real após limpeza",
        6.7,
    )
    ax = fig.add_axes((0.085, 0.20, 0.87, 0.58), facecolor=BG)
    daily = pd.DataFrame(evidence["gold_daily"])
    daily["trip_date"] = pd.to_datetime(daily.trip_date)
    plotted = daily[daily.vendor_id != "6"]
    omitted_trips = int(daily.loc[daily.vendor_id == "6", "trips"].sum())
    colors = [BLUE, TEAL, "#ac6300", "#8056ad"]
    for idx, (vendor, group) in enumerate(plotted.groupby("vendor_id", sort=True)):
        # Missing vendor/day combinations have no Gold row and therefore no trips.
        series = group.set_index("trip_date").trips.reindex(
            pd.date_range(daily.trip_date.min(), daily.trip_date.max()), fill_value=0
        )
        ax.plot(
            series.index,
            series.values,
            label=f"VendorID {vendor}",
            color=colors[idx % len(colors)],
            linewidth=2,
        )
    ax.spines[["top", "right", "left"]].set_visible(False)
    ax.spines["bottom"].set_color("#cbd5e1")
    ax.grid(axis="y", color="#dbe3ee", linewidth=0.8)
    ax.set_axisbelow(True)
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, pos: number(value)))
    ax.xaxis.set_major_locator(mdates.WeekdayLocator(byweekday=mdates.MO, interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m"))
    ax.tick_params(axis="both", colors=MUTED, labelsize=10, length=0, pad=9)
    ax.set_ylabel("Viagens / dia", fontsize=11, color=MUTED, labelpad=12)
    ax.set_ylim(bottom=0)
    ax.legend(loc="upper left", frameon=False, ncols=plotted.vendor_id.nunique(), fontsize=10)
    heading.text(
        0.085,
        0.10,
        f"{number(daily.trips.sum())} viagens limpas no período, incluindo todos os fornecedores.",
        fontsize=11,
        color=INK,
    )
    heading.text(
        0.085,
        0.055,
        f"VendorID 6: {number(omitted_trips)} viagens no período; "
        "série omitida para preservar a escala.",
        fontsize=11,
        color=INK,
    )
    save_figure(fig, "trips_daily", args, "Viagens diárias por VendorID a partir da Gold real")


def execution_summary(evidence: dict, args):
    runs = evidence["runs"]
    fig, ax = canvas(
        "Execução observada | contagens e controles",
        "NYC TLC yellow • execução local • remoções contadas na primeira regra que rejeita a linha",
        13,
    )
    positions = [0.69 + i * 0.23 for i in range(len(runs))]

    def line(y, label, values, *, bold=False, color=INK):
        ax.text(0.055, y, label, fontsize=11, color=color, weight="bold" if bold else "normal")
        for x, value in zip(positions, values, strict=True):
            ax.text(
                x,
                y,
                str(value),
                ha="right",
                fontsize=11,
                color=color,
                weight="bold" if bold else "normal",
            )

    line(0.84, "Camadas e reconciliação mensal", [run["month"] for run in runs], bold=True)
    for y, key, label in (
        (0.805, "bronze_rows", "Bronze • linhas na janela"),
        (0.775, "silver_rows", "Silver • viagens limpas"),
        (0.745, "gold_rows", "Gold • linhas data × fornecedor"),
    ):
        line(y, label, [number(run[key]) for run in runs])
    line(
        0.710,
        "Bronze − Silver",
        [number(run["bronze_rows"] - run["silver_rows"]) for run in runs],
        bold=True,
    )
    line(0.66, "Remoções por filtro, em ordem", ["Linhas" for _ in runs], bold=True)
    for i, (key, label) in enumerate(RULE_LABELS.items()):
        line(0.625 - i * 0.027, label, [number(run["removed_by_rule"][key]) for run in runs])
    line(
        0.425,
        "Exclusões de janela / rejeições de contrato",
        [
            f"{number(run['row_accounting']['month_window_excluded_count'])} / "
            f"{number(run['row_accounting']['contract_rejected_count'])}"
            for run in runs
        ],
    )
    line(0.385, "Gate de qualidade", ["Falhas / limite" for _ in runs], bold=True)
    for i, rule_id in enumerate(sorted(QUALITY_LABELS)):
        values = []
        for run in runs:
            rule = next(item for item in run["quality_rules"] if item["rule_id"] == rule_id)
            values.append(f"{number(rule['failed_count'])} / {number(rule['threshold'])}")
        line(0.35 - i * 0.027, f"{rule_id} • {QUALITY_LABELS[rule_id]}", values)
    drift_status = []
    for run in runs:
        if any(event["event_type"] == "baseline_initialized" for event in run["drift_events"]):
            drift_status.append("Base inicializada")
        else:
            levels = [
                event["severity"]
                for event in run["drift_events"]
                if event["event_type"] == "drift_detected"
            ]
            drift_status.append(f"{levels.count('warn')} warn / {levels.count('error')} error")
    line(0.175, "Drift • Silver e Gold", drift_status, bold=True)
    alerts = [item for item in runs[-1]["drift_comparisons"] if item["severity"]]
    if alerts:
        alert = alerts[0]
        delta = f"{alert['delta_ratio']:.2%}".replace(".", ",")
        ax.text(
            0.055,
            0.146,
            f"{alert['dataset']} • {alert['metric']}: "
            f"{number(alert['baseline_value'])} → {number(alert['observed_value'])} grupos • "
            f"|Δ| {delta} • warn ≥ {alert['thresholds']['warn']:.0%} • "
            f"error ≥ {alert['thresholds']['error']:.0%}",
            fontsize=10,
            color=MUTED,
        )
    group_note, trip_note = gold_grain_notes(evidence)
    ax.text(0.055, 0.123, group_note, fontsize=10, color=MUTED)
    ax.text(0.055, 0.101, trip_note, fontsize=10, color=MUTED)
    ax.text(
        0.055,
        0.077,
        "warn: falhas do gate são registradas sem interromper; "
        "limites de contrato continuam ativos.",
        fontsize=10,
        color=MUTED,
    )
    for i, run in enumerate(runs):
        ax.text(
            0.055,
            0.055 - i * 0.022,
            f"{run['month']}  run_id = relatório = {run['run_id']}",
            fontsize=9,
            color=MUTED,
        )
    save_figure(
        fig, "execution_summary", args, "Contagens, remoções, qualidade e drift das execuções reais"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exports", type=Path, default=ROOT / ".local/showcase/exports")
    parser.add_argument("--logs", type=Path, default=ROOT / ".local/logs")
    parser.add_argument("--reports", type=Path, default=ROOT / ".local/demo")
    parser.add_argument("--months", nargs="+", default=["2024-01", "2024-02"])
    parser.add_argument("--snapshot", type=Path, help="Re-render the versioned aggregate snapshot")
    parser.add_argument("--evidence-output", type=Path, default=ROOT / "docs/showcase_results.json")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "docs/images")
    parser.add_argument("--preview-dir", type=Path)
    args = parser.parse_args()
    plt.rcParams.update(
        {"font.family": "DejaVu Sans", "svg.fonttype": "none", "svg.hashsalt": "nyc-tlc-showcase"}
    )
    evidence = json.loads(args.snapshot.read_text()) if args.snapshot else collect_evidence(args)
    require(len(evidence["runs"]) == 2, "The summary layout requires two monthly runs")
    evidence["gold_vendor_summary"] = summarize_gold_vendors(evidence)
    architecture(args)
    daily_chart(evidence, args)
    execution_summary(evidence, args)
    if not args.snapshot:
        args.evidence_output.parent.mkdir(parents=True, exist_ok=True)
        args.evidence_output.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n")
    for run in evidence["runs"]:
        print(
            f"{run['month']}: Bronze={run['bronze_rows']} Silver={run['silver_rows']} "
            f"Gold={run['gold_rows']} removed={sum(run['removed_by_rule'].values())} "
            f"run_id={run['run_id']}"
        )
    for row in evidence["gold_vendor_summary"]:
        print(
            f"{row['month']} VendorID={row['vendor_id']} "
            f"Gold rows={row['gold_rows']} days={row['days']} trips={row['trips']}"
        )
    for note in gold_grain_notes(evidence):
        print(note)
    print(f"SVGs saved: {args.output_dir}")


if __name__ == "__main__":
    main()
