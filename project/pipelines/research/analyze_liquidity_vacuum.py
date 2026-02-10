"""
Phase‑1 analyzer for liquidity vacuum events.

This module wires the liquidity vacuum feature definitions into a simple
command‑line tool that scans cleaned bar data, calibrates a shock
threshold, detects events, and writes a summary report.  It is
analogous to the existing vol‑shock→relaxation analyzer but uses
volume and range proxies for thin order book conditions instead of
realised volatility.

The analyzer performs the following steps per symbol:

* Load cleaned OHLCV bars for the given run and timeframe.
* Calibrate a shock threshold by sweeping over candidate quantiles of
  absolute returns until a minimum number of events are detected.
* Detect liquidity vacuum events using the selected threshold and
  configuration parameters (e.g. minimum/maximum vacuum length,
  post‑event horizons).
* Persist the resulting event table to CSV and emit a lightweight
  JSON summary containing counts and the selected threshold.

This script does not implement full matched baseline analysis or
hazard/phase diagnostics.  It is intended as a foundation for
iterative research; downstream analysts can extend the output or run
additional tooling as needed.
"""

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

# Resolve project and data roots.  The project root is two levels up
# from this file (project/pipelines/research), while the data root
# defaults to ``data`` under the parent of the project root.  An
# environment variable ``BACKTEST_DATA_ROOT`` can override the data
# location.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

# Import the liquidity vacuum feature utilities.  These provide a
# configuration dataclass, a threshold calibration helper, and the
# event detector.  See features/liquidity_vacuum.py for details.
from features.liquidity_vacuum import (
    DEFAULT_LV_CONFIG,
    LiquidityVacuumConfig,
    calibrate_shock_threshold,
    detect_liquidity_vacuum_events,
)

from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)


def _load_bars(run_id: str, symbol: str, timeframe: str = "15m") -> pd.DataFrame:
    """Load cleaned bar data for a symbol and run.

    This helper looks for cleaned OHLCV bars in the per‑run lake
    partition first and falls back to the global lake if necessary.
    It expects columns ``timestamp``, ``close``, ``high``, ``low`` and
    ``volume`` to be present.  The resulting frame is sorted by
    timestamp with a UTC timezone.  If bars cannot be found, an
    empty DataFrame is returned.

    Parameters
    ----------
    run_id : str
        Identifier of the run scope.
    symbol : str
        Trading symbol (e.g. ``BTCUSDT``).
    timeframe : str, optional
        Bars timeframe (e.g. ``15m``).  Defaults to ``15m``.

    Returns
    -------
    pd.DataFrame
        Sorted bar DataFrame or empty DataFrame if not found.
    """
    # Determine candidate directories for the cleaned bars.  The
    # ``run_scoped_lake_path`` helper resolves per‑run partitions under
    # ``data/runs/<run_id>/lake``.  If the per‑run data is missing,
    # fallback to the global cleaned lake under ``data/lake/cleaned``.
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "cleaned", "perp", symbol, f"bars_{timeframe}"),
        DATA_ROOT / "lake" / "cleaned" / "perp" / symbol / f"bars_{timeframe}",
    ]
    bars_dir = choose_partition_dir(candidates)
    if bars_dir is None:
        return pd.DataFrame()
    files = list_parquet_files(bars_dir)
    if not files:
        return pd.DataFrame()
    frame = read_parquet(files)
    if frame.empty:
        return pd.DataFrame()
    # Ensure timestamps are timezone‑aware and sorted
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.sort_values("timestamp").reset_index(drop=True)
    # Filter to required columns if available
    req_cols = {"timestamp", "close", "high", "low", "volume"}
    missing = req_cols - set(frame.columns)
    if missing:
        logging.warning("Missing columns in bars for %s: %s", symbol, sorted(missing))
    return frame


def _parse_quantiles(raw: str) -> List[float]:
    """Parse a comma‑separated list of quantiles into floats.

    Only values between 0 and 1 (exclusive) are retained.  Duplicate
    entries are deduplicated and the result is sorted ascending.  A
    ``ValueError`` is raised if no valid quantiles are supplied.

    Parameters
    ----------
    raw : str
        Comma‑separated string of quantile values (e.g. ``"0.95,0.99"``).

    Returns
    -------
    List[float]
        Sorted unique quantile values.
    """
    vals: List[float] = []
    for tok in [x.strip() for x in raw.split(",") if x.strip()]:
        try:
            q = float(tok)
        except ValueError:
            continue
        if 0.0 < q < 1.0:
            vals.append(q)
    uniq = sorted(set(vals))
    if not uniq:
        raise ValueError("No valid shock quantiles supplied")
    return uniq


def main() -> int:
    """Run the liquidity vacuum analyzer from the command line."""
    parser = argparse.ArgumentParser(description="Phase‑1 analyzer for liquidity vacuum events")
    parser.add_argument("--run_id", required=True, help="Run identifier")
    parser.add_argument("--symbols", required=True, help="Comma‑separated list of symbols")
    parser.add_argument("--timeframe", default="15m", help="Bars timeframe (default: 15m)")
    parser.add_argument(
        "--shock_quantiles",
        default="0.95,0.97,0.98,0.99,0.995",
        help="Comma‑separated quantiles to sweep when calibrating the shock threshold",
    )
    parser.add_argument(
        "--min_events",
        type=int,
        default=10,
        help="Minimum number of events required when selecting a shock threshold",
    )
    parser.add_argument(
        "--out_dir",
        default=None,
        help="Output directory.  Defaults to data/reports/liquidity_vacuum/<run_id>",
    )
    parser.add_argument(
        "--log_path",
        default=None,
        help="Optional path to write debug logs.  If provided, logging is configured accordingly.",
    )
    args = parser.parse_args()

    # Configure logging.  If a log_path is provided, logs are written
    # there in addition to stderr.  Otherwise, only stderr is used.
    handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if args.log_path:
        ensure_dir(Path(args.log_path).parent)
        handlers.append(logging.FileHandler(args.log_path))
    logging.basicConfig(level=logging.INFO, handlers=handlers, format="%(asctime)s %(levelname)s %(message)s")

    run_id: str = args.run_id
    symbols: List[str] = [s.strip() for s in args.symbols.split(",") if s.strip()]
    timeframe: str = str(args.timeframe or "15m")
    quantiles: List[float] = _parse_quantiles(str(args.shock_quantiles))
    min_events: int = int(args.min_events)

    # Resolve output directory.  Per default, reports are written
    # under ``data/reports/liquidity_vacuum/<run_id>``.  Use
    # ``ensure_dir`` to create it if necessary.
    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "liquidity_vacuum" / run_id
    ensure_dir(out_dir)

    # Iterate over symbols, calibrate threshold and detect events.  We
    # accumulate per‑symbol event frames into a list and record summary
    # statistics for the run.  If calibration fails or no events are
    # detected for a symbol, that symbol is skipped.
    all_events: List[pd.DataFrame] = []
    summary_rows: List[Dict[str, object]] = []
    for sym in symbols:
        bars = _load_bars(run_id, sym, timeframe=timeframe)
        if bars.empty:
            logging.warning("No bars found for %s; skipping", sym)
            continue
        # Use the default configuration unless overridden via
        # environment variables or other mechanisms.  Copy to avoid
        # mutating the global default.
        cfg: LiquidityVacuumConfig = DEFAULT_LV_CONFIG
        # Calibrate shock threshold by sweeping the quantiles.  The
        # calibration helper returns a DataFrame of candidate
        # thresholds and a dict with the selected entry.
        df_q, sel = calibrate_shock_threshold(bars, sym, cfg, quantiles=quantiles, min_events=min_events)
        if sel.get("selected_t_shock") is None or sel.get("selected_event_count", 0) < min_events:
            logging.warning(
                "Calibration did not yield enough events for %s; threshold may be unreliable (selected=%s, count=%s)",
                sym,
                sel.get("selected_t_shock"),
                sel.get("selected_event_count"),
            )
        t_shock = float(sel.get("selected_t_shock")) if sel.get("selected_t_shock") is not None else None
        events = pd.DataFrame()
        if t_shock is not None and np.isfinite(t_shock):
            events = detect_liquidity_vacuum_events(bars, sym, cfg=cfg, t_shock=t_shock)
        if events.empty:
            logging.info("No liquidity vacuum events detected for %s", sym)
            continue
        events["symbol"] = sym
        all_events.append(events)
        summary_rows.append(
            {
                "symbol": sym,
                "selected_t_shock": t_shock,
                "event_count": int(len(events)),
                "max_duration_bars": int(events["duration_bars"].max()),
                "median_duration_bars": float(events["duration_bars"].median()),
            }
        )

    # Concatenate all event tables and persist if any events were found.
    events_df = pd.concat(all_events, ignore_index=True) if all_events else pd.DataFrame()
    if not events_df.empty:
        csv_path = out_dir / "liquidity_vacuum_events.csv"
        events_df.to_csv(csv_path, index=False)
        logging.info("Wrote events to %s", csv_path)

    # Write a JSON summary capturing calibration and event counts per symbol
    summary_path = out_dir / "liquidity_vacuum_summary.json"
    with summary_path.open("w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "symbols": symbols, "summaries": summary_rows}, f, indent=2, default=str)
    logging.info("Wrote summary to %s", summary_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())