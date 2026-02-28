"""
kill_switch.py
==============
Automated kill switch: evaluates recent realized slippage, equity drawdown,
and data freshness to issue a HALT or OK status artifact.

Exits:
  0 = OK
  1 = HALT triggered (dangerous conditions detected)
  2 = WARNING (degraded but not halting)

Usage:
    python project/scripts/kill_switch.py \\
        --run_id my_run --symbol BTCUSDT \\
        [--max_slippage_ratio 2.0] \\
        [--max_drawdown 0.15] \\
        [--max_staleness_bars 3] \\
        [--dry_run]
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

def _check_data_freshness(symbol: str, timeframe: str, max_staleness_bars: int) -> tuple[str, str]:
    bar_dir = DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}"
    files = sorted(bar_dir.rglob("*.parquet")) if bar_dir.exists() else []
    if not files:
        return "HALT", f"no cleaned bar data found for {symbol}"
    try:
        df = pd.read_parquet(files[-1], columns=["timestamp"])
        latest_ts = pd.to_datetime(df["timestamp"], utc=True).max()
    except Exception as exc:
        return "HALT", f"cannot read latest bars for {symbol}: {exc}"
    interval_min = 5  # default to 5m
    staleness = int((pd.Timestamp.utcnow() - latest_ts).total_seconds() / 60 / interval_min)
    if staleness > max_staleness_bars:
        return "HALT", f"data stale: {staleness} bars since last bar for {symbol} (max={max_staleness_bars})"
    return "OK", f"data fresh: {staleness} bars stale for {symbol}"


def _check_drawdown(run_id: str, strategy: str, max_drawdown: float) -> tuple[str, str]:
    """Read most recent strategy trace and compute realized drawdown."""
    trace_candidates = [
        DATA_ROOT / "reports" / "strategy_builder" / run_id / f"{strategy}_trace.parquet",
        DATA_ROOT / "runs" / run_id / f"{strategy}_trace.parquet",
    ]
    trace_path = next((p for p in trace_candidates if p.exists()), None)
    if trace_path is None:
        return "OK", f"no trace found for {strategy}; skipping drawdown check"
    try:
        trace = pd.read_parquet(trace_path)
        if "pnl" not in trace.columns:
            return "OK", "trace missing pnl column; skipping drawdown check"
        equity = (1.0 + trace["pnl"].fillna(0.0)).cumprod()
        peak = equity.cummax().replace(0.0, np.nan)
        drawdown = float(((peak - equity) / peak).fillna(0.0).max())
    except Exception as exc:
        return "WARN", f"drawdown check failed: {exc}"
    if drawdown >= max_drawdown:
        return "HALT", f"drawdown={drawdown:.3f} exceeds max={max_drawdown} for {strategy}"
    return "OK", f"drawdown={drawdown:.3f} within limit={max_drawdown} for {strategy}"


def _check_slippage_ratio(run_id: str, max_ratio: float) -> tuple[str, str]:
    """Compare realized slippage (from trace) to modeled cost."""
    manifest_path = DATA_ROOT / "runs" / run_id / "run_manifest.json"
    if not manifest_path.exists():
        return "OK", "no manifest; skipping slippage check"
    # Placeholder: actual realized slippage requires live trade log integration.
    # Currently returns WARN to signal this check isn't yet fully active.
    return "WARN", f"slippage ratio check not yet wired to live trade log; max_ratio={max_ratio}"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Automated kill switch evaluator.")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--strategy", default="", help="Strategy name for drawdown check")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--max_slippage_ratio", type=float, default=2.0)
    parser.add_argument("--max_drawdown", type=float, default=0.15)
    parser.add_argument("--max_staleness_bars", type=int, default=3)
    parser.add_argument("--dry_run", action="store_true", help="Print status without writing artifact")
    args = parser.parse_args()

    checks = [
        _check_data_freshness(args.symbol.upper(), args.timeframe, args.max_staleness_bars),
        _check_slippage_ratio(args.run_id, args.max_slippage_ratio),
    ]
    if args.strategy:
        checks.append(_check_drawdown(args.run_id, args.strategy, args.max_drawdown))

    all_statuses = [s for s, _ in checks]
    if "HALT" in all_statuses:
        overall = "HALT"
        exit_code = 1
    elif "WARN" in all_statuses:
        overall = "WARN"
        exit_code = 2
    else:
        overall = "OK"
        exit_code = 0

    artifact = {
        "run_id": args.run_id,
        "symbol": args.symbol.upper(),
        "overall": overall,
        "checks": [{"status": s, "message": m} for s, m in checks],
    }

    for s, m in checks:
        level = "ERROR" if s == "HALT" else ("WARN" if s == "WARN" else "INFO")
        print(f"[kill_switch][{level}] {s}: {m}", file=sys.stderr if s == "HALT" else sys.stdout)

    print(f"[kill_switch] OVERALL: {overall}", file=sys.stdout)

    if not args.dry_run:
        out_dir = DATA_ROOT / "runs" / args.run_id
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "kill_switch_status.json"
        out_path.write_text(json.dumps(artifact, indent=2), encoding="utf-8")
        print(f"[kill_switch] Status written to {out_path}", file=sys.stdout)

    return exit_code


if __name__ == "__main__":
    sys.exit(main())
