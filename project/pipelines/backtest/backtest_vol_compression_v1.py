from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.config import load_configs
from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp

# Default constants; will be overridden by config.
FEE_RATE = 0.0004
SLIP_RATE = 0.0002
RISK_PCT = 0.005
INITIAL_EQUITY = 1_000_000.0


@dataclass
class Trade:
    symbol: str
    direction: str
    entry_time: pd.Timestamp
    exit_time: pd.Timestamp
    entry_price: float
    exit_price: float
    qty: float
    pnl: float
    r_multiple: float
    exit_reason: str
    stop_price: float
    target_price: float


def _collect_stats(df: pd.DataFrame) -> Dict[str, object]:
    if df.empty:
        return {"rows": 0, "start_ts": None, "end_ts": None}
    return {
        "rows": int(len(df)),
        "start_ts": df["timestamp"].min().isoformat(),
        "end_ts": df["timestamp"].max().isoformat(),
    }


def _apply_slippage(price: float, direction: str, side: str) -> float:
    """
    Apply slippage to an entry or exit price based on the trade direction.
    """
    if direction == "long":
        return price * (1 + SLIP_RATE) if side == "entry" else price * (1 - SLIP_RATE)
    return price * (1 - SLIP_RATE) if side == "entry" else price * (1 + SLIP_RATE)


def _calculate_fees(entry_price: float, exit_price: float, qty: float) -> float:
    """
    Calculate total fees for a trade, based on entry and exit prices.
    """
    return (entry_price + exit_price) * qty * FEE_RATE


def _compute_drawdown(equity_curve: pd.Series) -> float:
    """
    Compute the maximum drawdown of an equity curve.
    """
    peak = equity_curve.cummax()
    drawdown = (equity_curve - peak) / peak
    return float(drawdown.min()) if not drawdown.empty else 0.0


def _exit_trade(
    direction: str,
    entry_price: float,
    exit_price: float,
    qty: float,
    risk_dollars: float,
) -> Dict[str, float]:
    """
    Compute PnL and R-multiple on exit.
    """
    pnl = (exit_price - entry_price) * qty if direction == "long" else (entry_price - exit_price) * qty
    fees = _calculate_fees(entry_price, exit_price, qty)
    pnl -= fees
    r_multiple = pnl / risk_dollars if risk_dollars != 0 else 0.0
    return {"pnl": pnl, "r_multiple": r_multiple}


def _prepare_data(symbol: str) -> pd.DataFrame:
    """
    Load and merge features and cleaned bars for a symbol.
    """
    features_dir = PROJECT_ROOT / "lake" / "features" / "perp" / symbol / "15m" / "features_v1"
    bars_dir = PROJECT_ROOT / "lake" / "cleaned" / "perp" / symbol / "bars_15m"
    feature_files = list_parquet_files(features_dir)
    bars_files = list_parquet_files(bars_dir)
    features = read_parquet(feature_files)
    bars = read_parquet(bars_files)
    if features.empty or bars.empty:
        raise ValueError(f"Missing data for {symbol}")

    features["timestamp"] = pd.to_datetime(features["timestamp"], utc=True)
    bars["timestamp"] = pd.to_datetime(bars["timestamp"], utc=True)
    ensure_utc_timestamp(features["timestamp"], "timestamp")
    ensure_utc_timestamp(bars["timestamp"], "timestamp")

    merged = pd.merge(
        features,
        bars[["timestamp", "close", "high", "low", "is_gap", "gap_len"]],
        on="timestamp",
        suffixes=("", "_bar"),
    )
    merged = merged.sort_values("timestamp").reset_index(drop=True)
    return merged


def _run_backtest(symbol: str, data: pd.DataFrame, trade_day_timezone: str) -> List[Trade]:
    """
    Execute the backtest for a single symbol.
    """
    trades: List[Trade] = []
    equity = INITIAL_EQUITY
    in_position = False
    position: Dict[str, object] = {}
    last_trade_day: Optional[pd.Timestamp] = None

    data = data.copy()
    data["prior_high_96"] = data["high_96"].shift(1)
    data["prior_low_96"] = data["low_96"].shift(1)

    for idx, row in data.iterrows():
        ts = row["timestamp"]
        # Normalize to configured time zone for one-trade-per-day rule.
        day = ts.tz_convert(trade_day_timezone).normalize()

        required_fields = [
            "rv_pct_17280",
            "range_96",
            "range_med_2880",
            "prior_high_96",
            "prior_low_96",
            "low_96",
            "high_96",
            "close",
            "is_gap",
            "gap_len",
        ]
        if row[required_fields].isna().any() or row["is_gap"] or row["gap_len"] > 0:
            continue

        if in_position:
            bars_held = int(idx - position["entry_index"])
            exit_price: Optional[float] = None
            exit_reason: Optional[str] = None

            if position["direction"] == "long" and row["low"] <= position["stop_price"]:
                exit_price = position["stop_price"]
                exit_reason = "stop"
            elif position["direction"] == "short" and row["high"] >= position["stop_price"]:
                exit_price = position["stop_price"]
                exit_reason = "stop"
            else:
                if position["direction"] == "long" and row["high"] >= position["target_price"]:
                    exit_price = position["target_price"]
                    exit_reason = "target_2r"
                elif position["direction"] == "short" and row["low"] <= position["target_price"]:
                    exit_price = position["target_price"]
                    exit_reason = "target_2r"
                elif bars_held >= 48:
                    exit_price = row["close"]
                    exit_reason = "time_stop"
                elif row["rv_pct_17280"] > 40:
                    exit_price = row["close"]
                    exit_reason = "rv_exit"

            if exit_price is not None:
                exit_price = _apply_slippage(exit_price, position["direction"], "exit")
                results = _exit_trade(
                    position["direction"],
                    position["entry_price"],
                    exit_price,
                    position["qty"],
                    position["risk_dollars"],
                )
                equity += results["pnl"]
                trades.append(
                    Trade(
                        symbol=symbol,
                        direction=position["direction"],
                        entry_time=position["entry_time"],
                        exit_time=ts,
                        entry_price=position["entry_price"],
                        exit_price=exit_price,
                        qty=position["qty"],
                        pnl=results["pnl"],
                        r_multiple=results["r_multiple"],
                        exit_reason=exit_reason,
                        stop_price=position["stop_price"],
                        target_price=position["target_price"],
                    )
                )
                in_position = False
                position = {}
                continue

        # Only one trade per day.
        if in_position or (last_trade_day is not None and day == last_trade_day):
            continue

        compression = row["rv_pct_17280"] <= 10 and row["range_96"] <= 0.8 * row["range_med_2880"]
        if not compression:
            continue

        if row["close"] > row["prior_high_96"]:
            direction = "long"
        elif row["close"] < row["prior_low_96"]:
            direction = "short"
        else:
            continue

        if pd.isna(row["low_96"]) or pd.isna(row["high_96"]):
            continue

        entry_price = _apply_slippage(row["close"], direction, "entry")
        stop_price = row["low_96"] if direction == "long" else row["high_96"]
        risk_per_unit = entry_price - stop_price if direction == "long" else stop_price - entry_price
        if risk_per_unit <= 0:
            continue
        risk_dollars = equity * RISK_PCT
        qty = risk_dollars / risk_per_unit
        target_price = entry_price + 2 * risk_per_unit if direction == "long" else entry_price - 2 * risk_per_unit

        position = {
            "direction": direction,
            "entry_time": ts,
            "entry_price": entry_price,
            "stop_price": stop_price,
            "target_price": target_price,
            "qty": qty,
            "risk_dollars": risk_dollars,
            "entry_index": idx,
        }
        in_position = True
        last_trade_day = day

    return trades


def _metrics_from_trades(trades: List[Trade]) -> Dict[str, object]:
    """
    Compute summary metrics from a list of trades.
    """
    if not trades:
        return {
            "total_trades": 0,
            "win_rate": 0.0,
            "avg_r": 0.0,
            "max_drawdown": 0.0,
            "ending_equity": INITIAL_EQUITY,
        }

    df = pd.DataFrame([t.__dict__ for t in trades])
    wins = df[df["pnl"] > 0]
    win_rate = float(len(wins) / len(df))
    avg_r = float(df["r_multiple"].mean())

    equity = INITIAL_EQUITY + df["pnl"].cumsum()
    max_dd = _compute_drawdown(equity)

    return {
        "total_trades": int(len(df)),
        "win_rate": win_rate,
        "avg_r": avg_r,
        "max_drawdown": max_dd,
        "ending_equity": float(equity.iloc[-1]),
    }


def _run_scenario(
    symbols: List[str],
    data_map: Dict[str, pd.DataFrame],
    trade_day_timezone: str,
) -> List[Trade]:
    trades: List[Trade] = []
    for symbol in symbols:
        symbol_trades = _run_backtest(symbol, data_map[symbol], trade_day_timezone)
        trades.extend(symbol_trades)
    return trades


def main() -> int:
    parser = argparse.ArgumentParser(description="Backtest volatility compression -> expansion")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--fees_bps", type=float, default=None)
    parser.add_argument("--slippage_bps", type=float, default=None)
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
    risk_pct = float(config.get("risk_per_trade_pct", 0.5))
    trade_day_timezone = str(config.get("trade_day_timezone", "UTC"))

    params = {
        "fee_bps_per_side": fee_bps,
        "slippage_bps_per_fill": slippage_bps,
        "risk_per_trade_pct": risk_pct,
        "trade_day_timezone": trade_day_timezone,
        "symbols": symbols,
        "force": int(args.force),
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

        data_map: Dict[str, pd.DataFrame] = {}
        for symbol in symbols:
            data = _prepare_data(symbol)
            data_map[symbol] = data
            inputs.append({"path": f"features+bars:{symbol}", **_collect_stats(data)})

        global FEE_RATE, SLIP_RATE, RISK_PCT
        FEE_RATE = fee_bps / 10_000
        SLIP_RATE = slippage_bps / 10_000
        RISK_PCT = risk_pct / 100

        base_trades = _run_scenario(symbols, data_map, trade_day_timezone)
        all_trades: List[Trade] = []

        for symbol in symbols:
            symbol_trades = [t for t in base_trades if t.symbol == symbol]
            all_trades.extend(symbol_trades)
            trades_df = pd.DataFrame([t.__dict__ for t in symbol_trades])
            trades_path = trades_dir / f"trades_{symbol}.csv"
            trades_df.to_csv(trades_path, index=False)
            outputs.append({"path": str(trades_path), "rows": len(trades_df), "start_ts": None, "end_ts": None})

        sorted_trades = sorted(all_trades, key=lambda trade: (trade.exit_time, trade.entry_time))
        metrics = _metrics_from_trades(sorted_trades)
        metrics_path = trades_dir / "metrics.json"
        with metrics_path.open("w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2, sort_keys=True)
        outputs.append({"path": str(metrics_path), "rows": 1, "start_ts": None, "end_ts": None})

        equity_curve = pd.DataFrame(
            {
                "timestamp": [t.exit_time for t in sorted_trades],
                "equity": INITIAL_EQUITY + pd.Series([t.pnl for t in sorted_trades]).cumsum(),
            }
        )
        equity_curve_path = trades_dir / "equity_curve.csv"
        equity_curve.to_csv(equity_curve_path, index=False)
        outputs.append({"path": str(equity_curve_path), "rows": len(equity_curve), "start_ts": None, "end_ts": None})

        fee_scenarios = [0, 2, 5, 10]
        if fee_bps not in fee_scenarios:
            fee_scenarios.insert(0, fee_bps)
        fee_sensitivity: Dict[str, Dict[str, object]] = {}

        for scenario_fee in fee_scenarios:
            FEE_RATE = float(scenario_fee) / 10_000
            scenario_trades = _run_scenario(symbols, data_map, trade_day_timezone)
            scenario_sorted = sorted(scenario_trades, key=lambda trade: (trade.exit_time, trade.entry_time))
            scenario_metrics = _metrics_from_trades(scenario_sorted)
            fee_sensitivity[str(scenario_fee)] = {
                "fee_bps_per_side": scenario_fee,
                "net_return": scenario_metrics["ending_equity"] - INITIAL_EQUITY,
                "avg_r": scenario_metrics["avg_r"],
                "max_drawdown": scenario_metrics["max_drawdown"],
                "trades": scenario_metrics["total_trades"],
            }

        fee_path = trades_dir / "fee_sensitivity.json"
        with fee_path.open("w", encoding="utf-8") as f:
            json.dump(fee_sensitivity, f, indent=2, sort_keys=True)
        outputs.append({"path": str(fee_path), "rows": len(fee_sensitivity), "start_ts": None, "end_ts": None})

        stats["symbols"] = {symbol: {"trades": len([t for t in base_trades if t.symbol == symbol])} for symbol in symbols}
        finalize_manifest(manifest, "success", stats=stats)
        return 0
    except Exception as exc:
        logging.exception("Backtest failed")
        finalize_manifest(manifest, "failed", error=str(exc), stats=stats)
        return 1


if __name__ == "__main__":
    sys.exit(main())
