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

def robust_z(series: pd.Series, window: int, eps: float = 1e-12) -> pd.Series:
    # Winsorize to 1%/99% then MAD-z over rolling window ending at t (PIT).
    def _rz(x: np.ndarray) -> float:
        x = x.copy()
        lo, hi = np.quantile(x, 0.01), np.quantile(x, 0.99)
        x = np.clip(x, lo, hi)
        med = np.median(x)
        mad = np.median(np.abs(x - med))
        return float((x[-1] - med) / (1.4826 * mad + eps))
    return series.rolling(window=window, min_periods=window).apply(_rz, raw=True)

def main() -> int:
    p = argparse.ArgumentParser(description="Build alpha signals v2 (Model1 depth, PIT-safe)")
    p.add_argument("--run_id", required=True)
    # Backwards compatible single-symbol mode
    p.add_argument("--symbol", required=False, default=None)
    p.add_argument("--bars_path", required=False, default=None, help="Parquet with timestamp + mid/close")
    p.add_argument("--funding_path", required=False, default=None, help="Parquet with timestamp + funding_rate")
    p.add_argument("--oi_path", required=False, default=None, help="Parquet with timestamp + oi_usd")

    # Multi-universe mode
    p.add_argument("--symbols", required=False, default=None, help="Comma-separated symbols (multi-universe)")
    p.add_argument(
        "--cleaned_root",
        required=False,
        default=None,
        help="Root of cleaned lake (defaults to project/lake/cleaned). Expected layout: cleaned/perp/<symbol>/{bars_15m,funding_15m,open_interest}/*.parquet",
    )
    p.add_argument("--bar_interval", default="15m", help="Bar interval used in cleaned lake (default: 15m)")
    p.add_argument("--oi_subdir", default="open_interest", help="Subdir under cleaned/perp/<symbol>/ for open interest parquet")
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    project_root = PROJECT_ROOT
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", project_root.parent / "data"))
    out_dir = Path(args.out_dir) if args.out_dir else data_root / "feature_store" / "signals"
    ensure_dir(out_dir)

    stage = "alpha_signals_v2"

    cleaned_root = Path(args.cleaned_root) if args.cleaned_root else (PROJECT_ROOT / "lake" / "cleaned")

    # Resolve symbols
    if args.symbols:
        symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    elif args.symbol:
        symbols = [args.symbol]
    else:
        raise ValueError("Provide --symbols for multi-universe or --symbol for single-asset mode")

    outputs_written: List[str] = []
    total_rows = 0
    inputs = []

    for symbol in symbols:
        # Resolve input paths
        bars_path = Path(args.bars_path) if (args.bars_path and args.symbol and symbol == args.symbol) else None
        funding_path = Path(args.funding_path) if (args.funding_path and args.symbol and symbol == args.symbol) else None
        oi_path = Path(args.oi_path) if (args.oi_path and args.symbol and symbol == args.symbol) else None

        if bars_path is None:
            bdir = cleaned_root / "perp" / symbol / f"bars_{args.bar_interval}"
            bars_files = sorted(list(bdir.glob("**/*.parquet"))) if bdir.exists() else []
            if not bars_files:
                raise FileNotFoundError(
                    f"No cleaned bars found for {symbol} at {cleaned_root}/perp/{symbol}/bars_{args.bar_interval}"
                )
        else:
            bars_files = [bars_path]

        if funding_path is None:
            fdir = cleaned_root / "perp" / symbol / f"funding_{args.bar_interval}"
            funding_files = sorted(list(fdir.glob("**/*.parquet"))) if fdir.exists() else []
        else:
            funding_files = [funding_path] if funding_path.exists() else []

        if oi_path is None:
            odir = cleaned_root / "perp" / symbol / args.oi_subdir
            oi_files = sorted(list(odir.glob("**/*.parquet"))) if odir.exists() else []
        else:
            oi_files = [oi_path] if oi_path.exists() else []

        inputs.append({"symbol": symbol, "bars_dir": str(bars_files[0].parent)})

        bars = read_parquet([Path(p) for p in bars_files])
        tcol = "ts_event" if "ts_event" in bars.columns else "timestamp"
        bars[tcol] = ensure_utc_timestamp(bars[tcol])
        price_col = "mid" if "mid" in bars.columns else ("close" if "close" in bars.columns else None)
        if price_col is None:
            raise ValueError("bars must contain 'mid' or 'close'")
        bars = bars.sort_values(tcol).reset_index(drop=True)
        pser = bars[price_col].astype(float)

        # Returns
        bars["logret"] = np.log(pser / pser.shift(1))

        # Vol proxy (EWMA var)
        lam = 0.97
        r2 = bars["logret"].fillna(0.0).to_numpy(dtype=np.float64)
        var = np.zeros_like(r2)
        for i in range(1, len(r2)):
            var[i] = lam * var[i - 1] + (1 - lam) * (r2[i] ** 2)
        bars["ewma_vol"] = np.sqrt(var)

        # Multi-lookback TSMOM (example lookbacks in bars)
        lookbacks = [20, 60, 120]
        cap = 3.0
        mom_parts = []
        for L in lookbacks:
            tr = np.log(pser / pser.shift(L))
            m = tr / (bars["ewma_vol"] + 1e-12)
            mom_parts.append(m.clip(-cap, cap))
        bars["z_tsmom_multi"] = sum(mom_parts) / len(mom_parts)

        # Mean reversion z-score
        L = 20
        mu = pser.rolling(L, min_periods=L).mean()
        sd = pser.rolling(L, min_periods=L).std()
        Z = (pser - mu) / (sd + 1e-12)
        bars["z_mr"] = (-Z).clip(-4.0, 4.0)

        # Funding carry (optional)
        if funding_files:
            fund = read_parquet([Path(p) for p in funding_files])
            ftcol = "ts_event" if "ts_event" in fund.columns else "timestamp"
            fund[ftcol] = ensure_utc_timestamp(fund[ftcol])
            fund = fund.sort_values(ftcol)
            fcol = "funding_rate" if "funding_rate" in fund.columns else ("funding_rate_scaled" if "funding_rate_scaled" in fund.columns else "rate")
            fund = fund[[ftcol, fcol]].rename(columns={ftcol: tcol, fcol: "funding_rate"})
            merged = pd.merge_asof(bars[[tcol]].sort_values(tcol), fund.sort_values(tcol), on=tcol, direction="backward")
            bars["funding_rate"] = merged["funding_rate"].astype(float)
            bars["z_funding"] = robust_z(bars["funding_rate"], window=60)
            bars["z_fund_carry"] = -bars["z_funding"].clip(-6.0, 6.0)
        else:
            bars["z_fund_carry"] = np.nan

        # OI change (optional)
        if oi_files:
            oi = read_parquet([Path(p) for p in oi_files])
            otcol = "ts_event" if "ts_event" in oi.columns else "timestamp"
            oi[otcol] = ensure_utc_timestamp(oi[otcol])
            oi = oi.sort_values(otcol)
            ocol = "oi_usd" if "oi_usd" in oi.columns else ("oi" if "oi" in oi.columns else ("open_interest" if "open_interest" in oi.columns else None))
            if ocol:
                oi = oi[[otcol, ocol]].rename(columns={otcol: tcol, ocol: "oi_usd"})
                merged = pd.merge_asof(bars[[tcol]].sort_values(tcol), oi.sort_values(tcol), on=tcol, direction="backward")
                bars["oi_usd"] = merged["oi_usd"].astype(float)
                bars["dOI"] = np.log(bars["oi_usd"] / (bars["oi_usd"].shift(1) + 1e-12))
            else:
                bars["dOI"] = np.nan
        else:
            bars["dOI"] = np.nan

        out_cols = [tcol, "z_tsmom_multi", "z_mr", "z_fund_carry", "dOI", "ewma_vol"]
        out = bars[out_cols].copy()
        out.insert(1, "symbol", symbol)
        out_path = out_dir / f"signals_{symbol}.parquet"
        write_parquet(out, out_path)
        outputs_written.append(str(out_path))
        total_rows += int(len(out))

    manifest = start_manifest(stage, run_id, params={"symbols": symbols}, inputs=inputs, outputs=[{"path": str(out_dir)}])
    finalize_manifest(manifest, status="success", stats={"rows": total_rows, "outs": outputs_written})
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
