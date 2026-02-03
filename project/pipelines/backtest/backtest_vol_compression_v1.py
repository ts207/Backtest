from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from engine.pnl import compute_pnl
from engine.runner import run_engine

INITIAL_EQUITY = 1_000_000.0


def _compute_drawdown(equity_curve: pd.Series) -> float:
    peak = equity_curve.cummax()
    drawdown = (equity_curve - peak) / peak
    return float(drawdown.min()) if not drawdown.empty else 0.0


def _empty_trades_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "symbol",
            "direction",
            "entry_time",
            "exit_time",
            "entry_price",
            "exit_price",
            "qty",
            "pnl",
            "r_multiple",
            "exit_reason",
            "stop_price",
            "target_price",
        ]
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest volatility compression -> expansion")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)
    parser.add_argument("--strategies", default=None)
    parser.add_argument("--force", type=int, default=0)
    parser.add_argument("--config", action="append", default=[])
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]

    log_handlers = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        log_handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=log_handlers, format="%(asctime)s %(levelname)s %(message)s")

    config_paths = [str(PROJECT_ROOT / "configs" / "pipeline.yaml"), str(PROJECT_ROOT / "configs" / "fees.yaml")]
    config_paths.extend(args.config)
    config = load_configs(config_paths)

    fee_bps = float(args.fees_bps) if args.fees_bps is not None else float(config.get("fee_bps_per_side", 4))
    slippage_bps = (
        float(args.slippage_bps) if args.slippage_bps is not None else float(config.get("slippage_bps_per_fill", 2))
    )
    trade_day_timezone = str(config.get("trade_day_timezone", "UTC"))
    cost_bps = float(args.cost_bps) if args.cost_bps is not None else float(fee_bps + slippage_bps)
    strategies = (
        [s.strip() for s in args.strategies.split(",") if s.strip()]
        if args.strategies
        else ["vol_compression_v1"]
    )

    params = {
        "fee_bps_per_side": fee_bps,
        "slippage_bps_per_fill": slippage_bps,
        "trade_day_timezone": trade_day_timezone,
        "symbols": symbols,
        "force": int(args.force),
        "cost_bps": cost_bps,
        "strategies": strategies,
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("backtest_vol_compression_v1", run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        trades_dir = PROJECT_ROOT / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
        trades_dir.mkdir(parents=True, exist_ok=True)

        if not args.force and (trades_dir / "metrics.json").exists():
            finalize_manifest(manifest, "success", stats={"skipped": True})
            return 0

        engine_results = run_engine(
            run_id=run_id,
            symbols=symbols,
            strategies=strategies,
            params={
                "trade_day_timezone": trade_day_timezone,
                "one_trade_per_day": True,
            },
            cost_bps=cost_bps,
            project_root=PROJECT_ROOT,
        )

        for strategy_name in strategies:
            strategy_path = engine_results["engine_dir"] / f"strategy_returns_{strategy_name}.csv"
            strategy_rows = len(engine_results["strategy_frames"].get(strategy_name, pd.DataFrame()))
            outputs.append({"path": str(strategy_path), "rows": strategy_rows, "start_ts": None, "end_ts": None})

        portfolio = engine_results["portfolio"]
        portfolio_path = engine_results["engine_dir"] / "portfolio_returns.csv"
        outputs.append({"path": str(portfolio_path), "rows": len(portfolio), "start_ts": None, "end_ts": None})

        for symbol in symbols:
            trades_path = trades_dir / f"trades_{symbol}.csv"
            _empty_trades_frame().to_csv(trades_path, index=False)
            outputs.append({"path": str(trades_path), "rows": 0, "start_ts": None, "end_ts": None})

        metrics_path = trades_dir / "metrics.json"
        ending_equity = float(INITIAL_EQUITY + portfolio["portfolio_pnl"].sum()) if not portfolio.empty else INITIAL_EQUITY
        max_drawdown = _compute_drawdown(INITIAL_EQUITY + portfolio["portfolio_pnl"].cumsum()) if not portfolio.empty else 0.0
        metrics_payload = {
            "total_trades": int(sum(v.get("entries", 0) for v in engine_results["metrics"]["strategies"].values())),
            "win_rate": 0.0,
            "avg_r": 0.0,
            "max_drawdown": max_drawdown,
            "ending_equity": ending_equity,
        }
        metrics_path.write_text(json.dumps(metrics_payload, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(metrics_path), "rows": 1, "start_ts": None, "end_ts": None})

        if portfolio.empty:
            equity_curve = pd.DataFrame(columns=["timestamp", "equity"])
        else:
            equity_curve = pd.DataFrame(
                {
                    "timestamp": portfolio["timestamp"],
                    "equity": INITIAL_EQUITY + portfolio["portfolio_pnl"].cumsum(),
                }
            )
        equity_curve_path = trades_dir / "equity_curve.csv"
        equity_curve.to_csv(equity_curve_path, index=False)
        outputs.append({"path": str(equity_curve_path), "rows": len(equity_curve), "start_ts": None, "end_ts": None})

        fee_scenarios = [0, 2, 5, 10]
        if cost_bps not in fee_scenarios:
            fee_scenarios.insert(0, cost_bps)

        fee_sensitivity: Dict[str, Dict[str, object]] = {}
        for scenario_cost in fee_scenarios:
            scenario_pnl_frames: List[pd.Series] = []
            entry_count = 0
            for _, frame in engine_results["strategy_frames"].items():
                for _, symbol_frame in frame.groupby("symbol", sort=True):
                    pos = symbol_frame.set_index("timestamp")["pos"]
                    ret = symbol_frame.set_index("timestamp")["ret"]
                    pnl = compute_pnl(pos, ret, scenario_cost)
                    scenario_pnl_frames.append(pnl)
                    prior = pos.shift(1).fillna(0)
                    entry_count += int(((prior == 0) & (pos != 0)).sum())
            scenario_pnl = (
                pd.concat(scenario_pnl_frames, axis=1).sum(axis=1) if scenario_pnl_frames else pd.Series(dtype=float)
            )
            scenario_equity = INITIAL_EQUITY + scenario_pnl.cumsum()
            fee_sensitivity[str(scenario_cost)] = {
                "fee_bps_per_side": scenario_cost,
                "net_return": float(scenario_pnl.sum()),
                "avg_r": 0.0,
                "max_drawdown": _compute_drawdown(scenario_equity) if not scenario_equity.empty else 0.0,
                "trades": entry_count,
            }

        fee_path = trades_dir / "fee_sensitivity.json"
        fee_path.write_text(json.dumps(fee_sensitivity, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(fee_path), "rows": len(fee_sensitivity), "start_ts": None, "end_ts": None})

        stats["symbols"] = {symbol: {"entries": 0} for symbol in symbols}
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Backtest failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
