from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from engine.pnl import compute_pnl
from engine.runner import run_engine
from strategies.overlay_registry import apply_overlay

INITIAL_EQUITY = 1_000_000.0
BARS_PER_YEAR_15M = 365 * 24 * 4


def _compute_drawdown(equity_curve: pd.Series) -> float:
    peak = equity_curve.cummax()
    drawdown = (equity_curve - peak) / peak
    return float(drawdown.min()) if not drawdown.empty else 0.0


def _annualized_sharpe(returns: pd.Series, periods_per_year: int = BARS_PER_YEAR_15M) -> float:
    returns = returns.dropna()
    if returns.empty:
        return 0.0
    std = float(returns.std())
    if std == 0.0:
        return 0.0
    return float((returns.mean() / std) * np.sqrt(periods_per_year))


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


def _extract_trades(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return _empty_trades_frame()
    if "close" not in frame.columns:
        logging.warning("Strategy frame missing close prices; skipping trade extraction.")
        return _empty_trades_frame()

    frame = frame.sort_values("timestamp").reset_index(drop=True)
    pos = frame["pos"].fillna(0).astype(int)
    pnl = frame["pnl"].fillna(0.0).astype(float)
    close = frame["close"].astype(float)
    high = frame["high"].astype(float) if "high" in frame.columns else pd.Series([float("nan")] * len(frame))
    low = frame["low"].astype(float) if "low" in frame.columns else pd.Series([float("nan")] * len(frame))
    high_96 = frame["high_96"].astype(float) if "high_96" in frame.columns else pd.Series([float("nan")] * len(frame))
    low_96 = frame["low_96"].astype(float) if "low_96" in frame.columns else pd.Series([float("nan")] * len(frame))
    ts = frame["timestamp"]

    trades: List[Dict[str, object]] = []
    current_pos = 0
    entry_idx: int | None = None
    entry_time: object | None = None
    entry_price: float | None = None
    direction: str | None = None
    stop_price: float | None = None
    target_price: float | None = None
    risk_return: float | None = None

    for i in range(len(frame)):
        pos_i = int(pos.iat[i])
        if current_pos == 0:
            if pos_i != 0:
                current_pos = pos_i
                entry_idx = i
                entry_time = ts.iat[i]
                entry_price = float(close.iat[i]) if pd.notna(close.iat[i]) else float("nan")
                direction = "long" if pos_i > 0 else "short"
                stop_price = float(low_96.iat[i]) if direction == "long" else float(high_96.iat[i])
                if pd.notna(entry_price) and pd.notna(stop_price) and entry_price > 0:
                    risk_per_unit = entry_price - stop_price if direction == "long" else stop_price - entry_price
                    if risk_per_unit > 0:
                        target_price = (
                            entry_price + 2 * risk_per_unit if direction == "long" else entry_price - 2 * risk_per_unit
                        )
                        risk_return = risk_per_unit / entry_price
                    else:
                        target_price = float("nan")
                        risk_return = None
                else:
                    stop_price = float("nan")
                    target_price = float("nan")
                    risk_return = None
            continue

        if pos_i == current_pos:
            continue

        exit_idx = i
        exit_time = ts.iat[i]
        exit_price = float(close.iat[i]) if pd.notna(close.iat[i]) else float("nan")
        trade_pnl = float(pnl.iloc[entry_idx : exit_idx + 1].sum()) if entry_idx is not None else 0.0
        r_multiple = trade_pnl / risk_return if risk_return and risk_return > 0 else 0.0
        exit_reason = "position_flip" if pos_i != 0 else "position_exit"
        bars_held = int(exit_idx - entry_idx) if entry_idx is not None else 0
        if pd.notna(stop_price) and pd.notna(target_price):
            high_exit = float(high.iat[exit_idx]) if pd.notna(high.iat[exit_idx]) else float("nan")
            low_exit = float(low.iat[exit_idx]) if pd.notna(low.iat[exit_idx]) else float("nan")
            if direction == "long":
                if pd.notna(low_exit) and low_exit <= stop_price:
                    exit_reason = "stop"
                elif pd.notna(high_exit) and high_exit >= target_price:
                    exit_reason = "target"
            elif direction == "short":
                if pd.notna(high_exit) and high_exit >= stop_price:
                    exit_reason = "stop"
                elif pd.notna(low_exit) and low_exit <= target_price:
                    exit_reason = "target"
        if exit_reason == "position_exit" and bars_held >= 48:
            exit_reason = "time"
        trades.append(
            {
                "symbol": frame["symbol"].iat[i],
                "direction": direction,
                "entry_time": entry_time,
                "exit_time": exit_time,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "qty": 1.0,
                "pnl": trade_pnl,
                "r_multiple": r_multiple,
                "exit_reason": exit_reason,
                "stop_price": stop_price,
                "target_price": target_price,
            }
        )

        if pos_i == 0:
            current_pos = 0
            entry_idx = None
            entry_time = None
            entry_price = None
            direction = None
        else:
            current_pos = pos_i
            entry_idx = i
            entry_time = ts.iat[i]
            entry_price = float(close.iat[i]) if pd.notna(close.iat[i]) else float("nan")
            direction = "long" if pos_i > 0 else "short"
            stop_price = float(low_96.iat[i]) if direction == "long" else float(high_96.iat[i])
            if pd.notna(entry_price) and pd.notna(stop_price) and entry_price > 0:
                risk_per_unit = entry_price - stop_price if direction == "long" else stop_price - entry_price
                if risk_per_unit > 0:
                    target_price = (
                        entry_price + 2 * risk_per_unit if direction == "long" else entry_price - 2 * risk_per_unit
                    )
                    risk_return = risk_per_unit / entry_price
                else:
                    target_price = float("nan")
                    risk_return = None
            else:
                stop_price = float("nan")
                target_price = float("nan")
                risk_return = None

    if current_pos != 0 and entry_idx is not None:
        exit_idx = len(frame) - 1
        exit_time = ts.iat[exit_idx]
        exit_price = float(close.iat[exit_idx]) if pd.notna(close.iat[exit_idx]) else float("nan")
        trade_pnl = float(pnl.iloc[entry_idx : exit_idx + 1].sum())
        r_multiple = trade_pnl / risk_return if risk_return and risk_return > 0 else 0.0
        trades.append(
            {
                "symbol": frame["symbol"].iat[exit_idx],
                "direction": direction,
                "entry_time": entry_time,
                "exit_time": exit_time,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "qty": 1.0,
                "pnl": trade_pnl,
                "r_multiple": r_multiple,
                "exit_reason": "eod",
                "stop_price": stop_price,
                "target_price": target_price,
            }
        )

    if not trades:
        return _empty_trades_frame()
    return pd.DataFrame(trades, columns=_empty_trades_frame().columns)


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest volatility compression -> expansion")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
    parser.add_argument("--cost_bps", type=float, default=None)
    parser.add_argument("--strategies", default=None)
    parser.add_argument("--overlays", default="")
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

    overlays = [o.strip() for o in str(args.overlays).split(",") if o.strip()]
    params = {
        "fee_bps_per_side": fee_bps,
        "slippage_bps_per_fill": slippage_bps,
        "trade_day_timezone": trade_day_timezone,
        "symbols": symbols,
        "force": int(args.force),
        "cost_bps": cost_bps,
        "strategies": strategies,
        "overlays": overlays,
    }
    inputs: List[Dict[str, object]] = []
    outputs: List[Dict[str, object]] = []
    manifest = start_manifest("backtest_vol_compression_v1", run_id, params, inputs, outputs)
    stats: Dict[str, object] = {"symbols": {}}

    try:
        trades_dir = DATA_ROOT / "lake" / "trades" / "backtests" / "vol_compression_expansion_v1" / run_id
        trades_dir.mkdir(parents=True, exist_ok=True)

        if not args.force and (trades_dir / "metrics.json").exists():
            finalize_manifest(manifest, "success", stats={"skipped": True})
            return 0

        strategy_params = {
            "trade_day_timezone": trade_day_timezone,
            "one_trade_per_day": True,
        }
        for overlay_name in overlays:
            strategy_params = apply_overlay(overlay_name, strategy_params)

        engine_results = run_engine(
            run_id=run_id,
            symbols=symbols,
            strategies=strategies,
            params=strategy_params,
            cost_bps=cost_bps,
            data_root=DATA_ROOT,
        )

        for strategy_name in strategies:
            strategy_path = engine_results["engine_dir"] / f"strategy_returns_{strategy_name}.csv"
            strategy_rows = len(engine_results["strategy_frames"].get(strategy_name, pd.DataFrame()))
            outputs.append({"path": str(strategy_path), "rows": strategy_rows, "start_ts": None, "end_ts": None})

        portfolio = engine_results["portfolio"]
        portfolio_path = engine_results["engine_dir"] / "portfolio_returns.csv"
        outputs.append({"path": str(portfolio_path), "rows": len(portfolio), "start_ts": None, "end_ts": None})

        if len(strategies) > 1:
            logging.warning("Multiple strategies detected; trades will be combined without strategy attribution.")

        trades_by_symbol: Dict[str, List[pd.DataFrame]] = {symbol: [] for symbol in symbols}
        for _, frame in engine_results["strategy_frames"].items():
            for symbol, symbol_frame in frame.groupby("symbol", sort=True):
                trades_by_symbol.setdefault(symbol, []).append(_extract_trades(symbol_frame))

        all_symbol_trades: List[pd.DataFrame] = []
        symbol_entry_counts: Dict[str, int] = {}
        for symbol in symbols:
            trades_frames = [frame for frame in trades_by_symbol.get(symbol, []) if not frame.empty]
            symbol_trades = pd.concat(trades_frames, ignore_index=True) if trades_frames else _empty_trades_frame()
            symbol_entry_counts[symbol] = int(len(symbol_trades))
            trades_path = trades_dir / f"trades_{symbol}.csv"
            symbol_trades.to_csv(trades_path, index=False)
            if not symbol_trades.empty:
                all_symbol_trades.append(symbol_trades)
            outputs.append({"path": str(trades_path), "rows": len(symbol_trades), "start_ts": None, "end_ts": None})

        all_trades = pd.concat(all_symbol_trades, ignore_index=True) if all_symbol_trades else _empty_trades_frame()
        total_trades = int(len(all_trades))
        if total_trades:
            win_rate = float((all_trades["pnl"] > 0).mean())
            avg_r = float(all_trades["r_multiple"].mean())
        else:
            win_rate = 0.0
            avg_r = 0.0

        metrics_path = trades_dir / "metrics.json"
        if portfolio.empty:
            ending_equity = INITIAL_EQUITY
            max_drawdown = 0.0
            sharpe_annualized = 0.0
        else:
            cumulative_return = portfolio["portfolio_pnl"].cumsum()
            equity_series = INITIAL_EQUITY * (1.0 + cumulative_return)
            ending_equity = float(equity_series.iloc[-1])
            max_drawdown = _compute_drawdown(equity_series)
            sharpe_annualized = _annualized_sharpe(portfolio["portfolio_pnl"])
        metrics_payload = {
            "total_trades": total_trades,
            "win_rate": win_rate,
            "avg_r": avg_r,
            "max_drawdown": max_drawdown,
            "ending_equity": ending_equity,
            "sharpe_annualized": sharpe_annualized,
        }
        metrics_path.write_text(json.dumps(metrics_payload, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(metrics_path), "rows": 1, "start_ts": None, "end_ts": None})

        if portfolio.empty:
            equity_curve = pd.DataFrame(columns=["timestamp", "equity"])
        else:
            equity_curve = pd.DataFrame(
                {
                    "timestamp": portfolio["timestamp"],
                    "equity": INITIAL_EQUITY * (1.0 + portfolio["portfolio_pnl"].cumsum()),
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
            scenario_trade_frames: List[pd.DataFrame] = []
            entry_count = 0
            for _, frame in engine_results["strategy_frames"].items():
                for _, symbol_frame in frame.groupby("symbol", sort=True):
                    indexed = symbol_frame.set_index("timestamp")
                    pos = indexed["pos"]
                    ret = indexed["ret"]
                    pnl = compute_pnl(pos, ret, scenario_cost)
                    scenario_pnl_frames.append(pnl)
                    prior = pos.shift(1).fillna(0)
                    entry_count += int(((prior == 0) & (pos != 0)).sum())
                    temp_frame = symbol_frame.copy()
                    temp_frame = temp_frame.set_index("timestamp")
                    temp_frame["pnl"] = pnl
                    temp_frame = temp_frame.reset_index()
                    scenario_trade_frames.append(_extract_trades(temp_frame))
            scenario_pnl = (
                pd.concat(scenario_pnl_frames, axis=1).sum(axis=1) if scenario_pnl_frames else pd.Series(dtype=float)
            )
            scenario_equity = INITIAL_EQUITY * (1.0 + scenario_pnl.cumsum())
            if scenario_trade_frames:
                scenario_trades = pd.concat(scenario_trade_frames, ignore_index=True)
                avg_r = float(scenario_trades["r_multiple"].mean()) if not scenario_trades.empty else 0.0
            else:
                avg_r = 0.0
            fee_sensitivity[str(scenario_cost)] = {
                "fee_bps_per_side": scenario_cost,
                "net_return": float(scenario_pnl.sum()),
                "avg_r": avg_r,
                "max_drawdown": _compute_drawdown(scenario_equity) if not scenario_equity.empty else 0.0,
                "trades": entry_count,
            }

        fee_path = trades_dir / "fee_sensitivity.json"
        fee_path.write_text(json.dumps(fee_sensitivity, indent=2, sort_keys=True), encoding="utf-8")
        outputs.append({"path": str(fee_path), "rows": len(fee_sensitivity), "start_ts": None, "end_ts": None})

        stats["symbols"] = {symbol: {"entries": int(symbol_entry_counts.get(symbol, 0))} for symbol in symbols}
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Backtest failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
