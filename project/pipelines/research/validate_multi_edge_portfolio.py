from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest


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
    consistency = float((non_zero == dominant).mean())
    return consistency


def _symbol_positive_ratio(symbol_contrib_path: Path) -> float:
    if not symbol_contrib_path.exists():
        return 0.0
    df = pd.read_csv(symbol_contrib_path)
    if df.empty or "total_pnl" not in df.columns:
        return 0.0
    return float((pd.to_numeric(df["total_pnl"], errors="coerce").fillna(0.0) > 0).mean())


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate multi-edge portfolio promotion gates")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--require_pass", type=int, default=1)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    params = {"run_id": args.run_id, "require_pass": int(args.require_pass)}
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("validate_multi_edge_portfolio", args.run_id, params, inputs, outputs)

    config = load_configs([str(PROJECT_ROOT / "configs" / "portfolio.yaml")])
    gates_cfg = config.get("multi_edge_portfolio", {}).get("gates", {})

    try:
        baseline_summary_path = DATA_ROOT / "reports" / "vol_compression_expansion_v1" / args.run_id / "summary.json"
        multi_metrics_path = DATA_ROOT / "lake" / "trades" / "backtests" / "multi_edge_portfolio" / args.run_id / "metrics.json"

        if not baseline_summary_path.exists():
            raise FileNotFoundError(f"baseline summary missing: {baseline_summary_path}")
        if not multi_metrics_path.exists():
            raise FileNotFoundError(f"multi-edge metrics missing: {multi_metrics_path}")

        baseline = json.loads(baseline_summary_path.read_text(encoding="utf-8"))
        multi = json.loads(multi_metrics_path.read_text(encoding="utf-8"))

        selected_mode = str(multi.get("selected_mode", "")).strip()
        selected_payload = multi.get("modes", {}).get(selected_mode, {}) if selected_mode else {}

        baseline_return = float(baseline.get("net_total_return", 0.0) or 0.0)
        selected_return = float(selected_payload.get("net_total_return", 0.0) or 0.0)

        portfolio_path = Path(str(selected_payload.get("paths", {}).get("portfolio", "")))
        symbol_contrib_path = Path(str(selected_payload.get("paths", {}).get("symbol_contribution", "")))

        regime_consistency = _yearly_sign_consistency(portfolio_path)
        symbol_ratio = _symbol_positive_ratio(symbol_contrib_path)

        estimated_cost_drag = float(selected_payload.get("estimated_cost_drag", 0.0) or 0.0)
        friction_excess = selected_return - estimated_cost_drag

        gate_uplift = (selected_return - baseline_return) >= float(gates_cfg.get("uplift_min_vs_baseline", 0.0))
        gate_regime = regime_consistency >= float(gates_cfg.get("regime_sign_consistency_min", 0.8))
        gate_symbol = symbol_ratio >= float(gates_cfg.get("symbol_positive_pnl_ratio_min", 0.6))
        gate_friction = friction_excess >= float(gates_cfg.get("friction_floor_excess_min", 0.0))
        gate_constraints = bool(selected_payload.get("constraints_pass", False))

        gates = {
            "uplift_vs_baseline": {
                "pass": gate_uplift,
                "baseline_return": baseline_return,
                "selected_return": selected_return,
                "delta": selected_return - baseline_return,
            },
            "regime_stability": {
                "pass": gate_regime,
                "sign_consistency": regime_consistency,
            },
            "symbol_stability": {
                "pass": gate_symbol,
                "positive_symbol_ratio": symbol_ratio,
            },
            "friction_floor": {
                "pass": gate_friction,
                "estimated_cost_drag": estimated_cost_drag,
                "friction_excess": friction_excess,
            },
            "constraints": {
                "pass": gate_constraints,
                "constraint_breaches": selected_payload.get("constraint_breaches", []),
            },
        }

        verdict = "PASS_PROMOTION" if all(v.get("pass", False) for v in gates.values()) else "FAIL_PROMOTION"
        payload = {
            "run_id": args.run_id,
            "selected_mode": selected_mode,
            "verdict": verdict,
            "gates": gates,
        }

        out_dir = DATA_ROOT / "reports" / "multi_edge_validation" / args.run_id
        ensure_dir(out_dir)
        verdict_json = out_dir / "verdict.json"
        verdict_md = out_dir / "verdict.md"

        verdict_json.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        lines = [
            "# Multi-Edge Validation Verdict",
            "",
            f"Run ID: `{args.run_id}`",
            f"Selected mode: `{selected_mode}`",
            f"Verdict: **{verdict}**",
            "",
            "## Gates",
        ]
        for gate_name, gate_payload in gates.items():
            lines.append(f"- {gate_name}: {'PASS' if gate_payload.get('pass') else 'FAIL'}")
        verdict_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

        outputs.append({"path": str(verdict_json), "rows": 1, "start_ts": None, "end_ts": None})
        outputs.append({"path": str(verdict_md), "rows": len(lines), "start_ts": None, "end_ts": None})

        finalize_manifest(manifest, "success", stats={"verdict": verdict})
        if verdict != "PASS_PROMOTION" and int(args.require_pass):
            return 1
        return 0
    except Exception as exc:  # pragma: no cover
        logging.exception("Multi-edge validation failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats={})
        return 1


if __name__ == "__main__":
    sys.exit(main())
