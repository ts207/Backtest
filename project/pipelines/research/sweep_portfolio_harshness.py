from __future__ import annotations

import argparse
import itertools
import json
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir


def _parse_grid(raw: str, *, cast=float) -> List[float]:
    values = [x.strip() for x in str(raw).split(",") if x.strip()]
    return [cast(v) for v in values]


def _yearly_sign_consistency(portfolio_path: Path) -> float:
    if not portfolio_path.exists():
        return 0.0
    df = pd.read_csv(portfolio_path)
    if df.empty:
        return 0.0
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, format="mixed")
    df["year"] = df["timestamp"].dt.year
    yearly = df.groupby("year", as_index=False)["portfolio_pnl"].sum()
    if yearly.empty:
        return 0.0
    signs = yearly["portfolio_pnl"].apply(lambda x: 1 if x > 0 else -1 if x < 0 else 0)
    non_zero = signs[signs != 0]
    if non_zero.empty:
        return 0.0
    dominant = non_zero.mode().iloc[0]
    return float((non_zero == dominant).mean())


def _symbol_positive_ratio(symbol_contrib_path: Path) -> float:
    if not symbol_contrib_path.exists():
        return 0.0
    df = pd.read_csv(symbol_contrib_path)
    if df.empty or "total_pnl" not in df.columns:
        return 0.0
    return float((pd.to_numeric(df["total_pnl"], errors="coerce").fillna(0.0) > 0).mean())


def _evaluate_run(run_id: str) -> Dict[str, float]:
    baseline_summary_path = DATA_ROOT / "reports" / "vol_compression_expansion_v1" / run_id / "summary.json"
    multi_metrics_path = DATA_ROOT / "lake" / "trades" / "backtests" / "multi_edge_portfolio" / run_id / "metrics.json"

    if not baseline_summary_path.exists() or not multi_metrics_path.exists():
        return {"run_id": run_id, "available": 0}

    baseline = json.loads(baseline_summary_path.read_text(encoding="utf-8"))
    multi = json.loads(multi_metrics_path.read_text(encoding="utf-8"))

    selected_mode = str(multi.get("selected_mode", "")).strip()
    selected_payload = multi.get("modes", {}).get(selected_mode, {}) if selected_mode else {}

    selected_return = float(selected_payload.get("net_total_return", 0.0) or 0.0)
    baseline_return = float(baseline.get("net_total_return", 0.0) or 0.0)
    max_drawdown = float(selected_payload.get("max_drawdown", 0.0) or 0.0)

    portfolio_path = Path(str(selected_payload.get("paths", {}).get("portfolio", "")))
    symbol_contrib_path = Path(str(selected_payload.get("paths", {}).get("symbol_contribution", "")))

    regime_consistency = _yearly_sign_consistency(portfolio_path)
    symbol_ratio = _symbol_positive_ratio(symbol_contrib_path)

    estimated_cost_drag = float(selected_payload.get("estimated_cost_drag", 0.0) or 0.0)
    friction_excess = selected_return - estimated_cost_drag

    return {
        "run_id": run_id,
        "available": 1,
        "constraints_pass": int(bool(selected_payload.get("constraints_pass", False))),
        "selected_return": selected_return,
        "baseline_return": baseline_return,
        "uplift_delta": selected_return - baseline_return,
        "max_drawdown": max_drawdown,
        "regime_consistency": regime_consistency,
        "symbol_ratio": symbol_ratio,
        "friction_excess": friction_excess,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Sweep multi-edge validation harshness thresholds")
    parser.add_argument("--run_ids", required=True, help="Comma-separated run IDs")
    parser.add_argument("--regime_sign_consistency_grid", default="0.7,0.75,0.8")
    parser.add_argument("--symbol_positive_ratio_grid", default="0.5,0.55,0.6")
    parser.add_argument("--friction_floor_grid", default="0.0")
    parser.add_argument("--uplift_min_grid", default="0.0")
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    run_ids = [x.strip() for x in args.run_ids.split(",") if x.strip()]
    if not run_ids:
        return 1

    run_rows = [_evaluate_run(run_id) for run_id in run_ids]
    available = [row for row in run_rows if int(row.get("available", 0)) == 1]

    out_dir = (
        Path(args.out_dir)
        if args.out_dir
        else DATA_ROOT / "reports" / "multi_edge_validation" / "harshness_sweep"
    )
    ensure_dir(out_dir)

    run_df = pd.DataFrame(run_rows)
    run_df.to_csv(out_dir / "run_level_metrics.csv", index=False)

    combos = list(
        itertools.product(
            _parse_grid(args.regime_sign_consistency_grid),
            _parse_grid(args.symbol_positive_ratio_grid),
            _parse_grid(args.friction_floor_grid),
            _parse_grid(args.uplift_min_grid),
        )
    )

    frontier_rows = []
    for regime_min, symbol_min, friction_min, uplift_min in combos:
        survivors = [
            row
            for row in available
            if int(row["constraints_pass"]) == 1
            and float(row["regime_consistency"]) >= regime_min
            and float(row["symbol_ratio"]) >= symbol_min
            and float(row["friction_excess"]) >= friction_min
            and float(row["uplift_delta"]) >= uplift_min
        ]
        total = len(available)
        surv = len(survivors)
        survival_rate = float(surv / total) if total else 0.0
        avg_net_return = float(pd.Series([x["selected_return"] for x in survivors]).mean()) if survivors else 0.0
        avg_max_drawdown = float(pd.Series([x["max_drawdown"] for x in survivors]).mean()) if survivors else 0.0
        frontier_rows.append(
            {
                "regime_sign_consistency_min": regime_min,
                "symbol_positive_pnl_ratio_min": symbol_min,
                "friction_floor_excess_min": friction_min,
                "uplift_min_vs_baseline": uplift_min,
                "available_runs": total,
                "survivor_count": surv,
                "survival_rate": survival_rate,
                "avg_survivor_net_return": avg_net_return,
                "avg_survivor_max_drawdown": avg_max_drawdown,
            }
        )

    frontier_df = pd.DataFrame(frontier_rows).sort_values(
        ["survival_rate", "avg_survivor_net_return"], ascending=[False, False]
    )
    frontier_csv = out_dir / "frontier.csv"
    frontier_df.to_csv(frontier_csv, index=False)

    summary = {
        "run_count": len(run_ids),
        "available_run_count": len(available),
        "grid_size": len(combos),
        "best_tradeoff": frontier_df.head(1).to_dict(orient="records")[0] if not frontier_df.empty else {},
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")

    md_lines = [
        "# Multi-Edge Harshness Sweep",
        "",
        f"Run count: `{len(run_ids)}`",
        f"Available run count: `{len(available)}`",
        f"Grid size: `{len(combos)}`",
        "",
        "## Top frontier points",
        "",
    ]
    top = frontier_df.head(10)
    if top.empty:
        md_lines.append("No available runs with matching artifacts.")
    else:
        md_lines.append(top.to_string(index=False))
    (out_dir / "report.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
