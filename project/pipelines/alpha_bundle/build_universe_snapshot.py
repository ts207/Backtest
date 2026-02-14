from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, list_parquet_files, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp

def _stable_symbol_sort(symbols: List[str]) -> List[str]:
    # Deterministic ordering. If you maintain a canonical ID mapping, apply it here.
    return sorted(symbols)

def main() -> int:
    p = argparse.ArgumentParser(description="Build UniverseSnapshot (deterministic, PIT-safe)")
    p.add_argument("--run_id", required=True)
    p.add_argument("--universe_id", default="default_universe_v1")
    p.add_argument("--symbols", required=True, help="Comma-separated symbols")
    p.add_argument(
        "--cleaned_root",
        default=None,
        help="Root of cleaned lake (defaults to $BACKTEST_DATA_ROOT/lake/cleaned). Expected: cleaned/perp/<symbol>/bars_<bar_interval>/*.parquet",
    )
    p.add_argument("--bar_interval", default="15m")
    p.add_argument("--start", default=None, help="ISO start (inclusive), e.g. 2021-01-01T00:00:00Z")
    p.add_argument("--end", default=None, help="ISO end (exclusive), e.g. 2022-01-01T00:00:00Z")
    p.add_argument("--adv_window_bars", type=int, default=96, help="Rolling window (bars) for ADV proxy (default 96 = 1 day at 15m)")
    p.add_argument("--adv_min_usd", type=float, default=1_000_000.0, help="Minimum rolling USD ADV proxy")
    p.add_argument("--out_dir", default=None, help="Defaults to data/feature_store/universe_snapshots")
    args = p.parse_args()

    run_id = args.run_id
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    symbols_sorted = _stable_symbol_sort(symbols)

    project_root = PROJECT_ROOT
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", project_root.parent / "data"))
    out_dir = Path(args.out_dir) if args.out_dir else data_root / "feature_store" / "universe_snapshots"
    ensure_dir(out_dir)

    stage = "alpha_universe_snapshot"
    manifest = start_manifest(stage, run_id, params={"universe_id": args.universe_id}, inputs=[], outputs=[{"path": str(out_dir)}])

    cleaned_root = Path(args.cleaned_root) if args.cleaned_root else (data_root / "lake" / "cleaned")

    # Load bars for each symbol and build included_flags per ts_event.
    # This produces a PIT-consistent membership snapshot at each bar timestamp.
    panels: Dict[str, pd.DataFrame] = {}
    all_ts: Optional[pd.DatetimeIndex] = None
    for sym in symbols_sorted:
        bdir = cleaned_root / "perp" / sym / f"bars_{args.bar_interval}"
        files = sorted(list(bdir.glob("**/*.parquet"))) if bdir.exists() else []
        if not files:
            raise FileNotFoundError(f"No cleaned bars for {sym} at {bdir}")
        dfb = read_parquet(files)
        tcol = "ts_event" if "ts_event" in dfb.columns else "timestamp"
        dfb[tcol] = ensure_utc_timestamp(dfb[tcol], tcol)
        dfb = dfb.sort_values(tcol).reset_index(drop=True)
        # Filter date range
        if args.start:
            start_ts = pd.to_datetime(args.start, utc=True)
            dfb = dfb[dfb[tcol] >= start_ts]
        if args.end:
            end_ts = pd.to_datetime(args.end, utc=True)
            dfb = dfb[dfb[tcol] < end_ts]

        price_col = "mid" if "mid" in dfb.columns else ("close" if "close" in dfb.columns else None)
        vol_col = "volume" if "volume" in dfb.columns else ("quote_volume" if "quote_volume" in dfb.columns else None)
        if price_col is None or vol_col is None:
            raise ValueError(f"Bars for {sym} must include price (mid/close) and volume")

        # USD ADV proxy from close*volume (or quote_volume if already USD-like)
        if vol_col == "quote_volume":
            usd_vol = dfb[vol_col].astype(float)
        else:
            usd_vol = dfb[price_col].astype(float) * dfb[vol_col].astype(float)
        adv = usd_vol.rolling(args.adv_window_bars, min_periods=args.adv_window_bars).mean()

        is_gap = dfb["is_gap"].astype(bool) if "is_gap" in dfb.columns else pd.Series(False, index=dfb.index)
        ok = (
            np.isfinite(dfb[price_col].astype(float))
            & np.isfinite(usd_vol.astype(float))
            & (~is_gap)
            & (adv >= float(args.adv_min_usd))
        )

        out = pd.DataFrame({"ts_event": dfb[tcol], "included": ok.astype(bool)})
        panels[sym] = out
        idx = pd.DatetimeIndex(out["ts_event"])
        all_ts = idx if all_ts is None else all_ts.union(idx)

    if all_ts is None or len(all_ts) == 0:
        raise ValueError("No timestamps found to build universe snapshots")

    all_ts = all_ts.sort_values()

    # Build snapshots per ts_event
    rows = []
    for ts in all_ts:
        flags = []
        for sym in symbols_sorted:
            dfp = panels[sym]
            # exact timestamp match for bar clocks; if missing, treat as excluded
            m = dfp[dfp["ts_event"] == ts]
            flags.append(bool(m.iloc[-1]["included"]) if not m.empty else False)
        payload = json.dumps({"universe_id": args.universe_id, "ts_event": ts.isoformat(), "symbols_sorted": symbols_sorted, "included_flags": flags}, sort_keys=True).encode("utf-8")
        import hashlib
        rows.append({
            "ts_event": ts,
            "universe_id": args.universe_id,
            "symbols_sorted": symbols_sorted,
            "included_flags": flags,
            "snapshot_hash": hashlib.sha256(payload).hexdigest(),
            "universe_snapshot_version": 1,
        })

    df = pd.DataFrame(rows)
    df["ts_event"] = ensure_utc_timestamp(df["ts_event"], "ts_event")
    out_path = out_dir / f"universe_snapshot_{args.universe_id}.parquet"
    write_parquet(df, out_path)

    finalize_manifest(manifest, status="success", stats={"rows": int(len(df)), "out": str(out_path), "symbols": len(symbols_sorted)})
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
