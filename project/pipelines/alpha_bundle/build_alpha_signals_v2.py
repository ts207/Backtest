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


def _safe_log_ratio(a: pd.Series, b: pd.Series, eps: float = 1e-12) -> pd.Series:
    """PIT-safe log(a/b) with small eps handling."""
    return np.log((a.astype(float) + eps) / (b.astype(float) + eps))

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
        help="Root of cleaned lake (defaults to $BACKTEST_DATA_ROOT/lake/cleaned). Expected layout: cleaned/perp/<symbol>/{bars_15m,funding_15m,open_interest}/*.parquet",
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

    cleaned_root = Path(args.cleaned_root) if args.cleaned_root else (data_root / "lake" / "cleaned")

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
        bars[tcol] = ensure_utc_timestamp(bars[tcol], tcol)
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

        # ---- Signal 1: Time-series momentum (multi-lookback, risk-adjusted)
        # NOTE: lookbacks are in bars, not days. Keep deterministic defaults.
        lookbacks = [20, 60, 120]
        mom_cap = 3.0
        mom_parts = []
        for L in lookbacks:
            tr = _safe_log_ratio(pser, pser.shift(L))
            m = tr / (bars["ewma_vol"] + 1e-12)
            mom_parts.append(m.clip(-mom_cap, mom_cap))
        bars["ts_momentum_multi"] = (sum(mom_parts) / len(mom_parts)).astype(float)
        # Back-compat
        bars["z_tsmom_multi"] = bars["ts_momentum_multi"]

        # ---- Signal 2: Mean reversion (continuous z-score; state-machine can be built downstream)
        mr_L = 20
        mu = pser.rolling(mr_L, min_periods=mr_L).mean()
        sd = pser.rolling(mr_L, min_periods=mr_L).std()
        Z = (pser - mu) / (sd + 1e-12)
        bars["mean_reversion_state"] = (-Z).clip(-4.0, 4.0).astype(float)
        # Back-compat
        bars["z_mr"] = bars["mean_reversion_state"]

        # ---- Signal 3: Funding crowding fade (+ optional basis + OI adjustment)
        if funding_files:
            fund = read_parquet([Path(p) for p in funding_files])
            ftcol = "ts_event" if "ts_event" in fund.columns else "timestamp"
            fund[ftcol] = ensure_utc_timestamp(fund[ftcol], ftcol)
            fund = fund.sort_values(ftcol)
            fcol = "funding_rate" if "funding_rate" in fund.columns else ("funding_rate_scaled" if "funding_rate_scaled" in fund.columns else "rate")
            fund = fund[[ftcol, fcol]].rename(columns={ftcol: tcol, fcol: "funding_rate"})
            merged = pd.merge_asof(bars[[tcol]].sort_values(tcol), fund.sort_values(tcol), on=tcol, direction="backward")
            bars["funding_rate"] = merged["funding_rate"].astype(float)
            bars["funding_z"] = robust_z(bars["funding_rate"], window=60)
        else:
            bars["funding_rate"] = np.nan
            bars["funding_z"] = np.nan

        # OI change (optional)
        if oi_files:
            oi = read_parquet([Path(p) for p in oi_files])
            otcol = "ts_event" if "ts_event" in oi.columns else "timestamp"
            oi[otcol] = ensure_utc_timestamp(oi[otcol], otcol)
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

        # Optional: Basis z-score if spot series exists
        # Expected spot layout: cleaned_root/spot/<asset_or_symbol>/bars_<interval>/*.parquet
        # We attempt two mappings:
        #   1) exact symbol match
        #   2) strip common quote suffixes to get base asset (e.g., BTCUSDT -> BTC)
        basis = pd.Series(np.nan, index=bars.index)
        basis_z = pd.Series(np.nan, index=bars.index)
        try:
            spot_candidates = [symbol]
            for suf in ["USDT", "USD", "PERP"]:
                if symbol.endswith(suf):
                    spot_candidates.append(symbol[: -len(suf)])
            spot_files = []
            for cand in spot_candidates:
                sdir = cleaned_root / "spot" / cand / f"bars_{args.bar_interval}"
                if sdir.exists():
                    spot_files = sorted(list(sdir.glob("**/*.parquet")))
                    if spot_files:
                        break
            if spot_files:
                spot = read_parquet([Path(p) for p in spot_files])
                stcol = "ts_event" if "ts_event" in spot.columns else "timestamp"
                spot[stcol] = ensure_utc_timestamp(spot[stcol], stcol)
                spot = spot.sort_values(stcol).reset_index(drop=True)
                sp_col = "mid" if "mid" in spot.columns else ("close" if "close" in spot.columns else None)
                if sp_col is not None:
                    spot = spot[[stcol, sp_col]].rename(columns={stcol: tcol, sp_col: "spot_price"})
                    merged_spot = pd.merge_asof(bars[[tcol]].sort_values(tcol), spot.sort_values(tcol), on=tcol, direction="backward")
                    basis = _safe_log_ratio(pser, merged_spot["spot_price"].astype(float))
                    basis_z = robust_z(basis, window=60)
        except Exception:
            # Basis is optional; keep NaNs if loading fails
            pass
        bars["basis"] = basis.astype(float)
        bars["basis_z"] = basis_z.astype(float)

        # Funding carry adjusted score
        k_b = 0.5
        k_oi = 0.25
        dOI_cap = 2.0
        fund_cap = 6.0
        dOI_clip = bars["dOI"].clip(-dOI_cap, dOI_cap)
        zf = bars["funding_z"].clip(-fund_cap, fund_cap)
        zb = bars["basis_z"].clip(-fund_cap, fund_cap)
        bars["funding_carry_adjusted"] = (-zf - k_b * zb - k_oi * dOI_clip).clip(-fund_cap, fund_cap)
        # Back-compat
        bars["z_fund_carry"] = bars["funding_carry_adjusted"]

        # ---- Signal 6 proxy: Orderflow imbalance (PIT-safe proxy using signed volume)
        # If true L2/L3 data is available, replace this in a dedicated microstructure pipeline.
        signed_vol = np.sign(bars["logret"].fillna(0.0)) * (pser * bars.get("volume", 0.0).astype(float))
        bars["orderflow_imbalance"] = robust_z(pd.Series(signed_vol), window=60).clip(-4.0, 4.0)

        out_cols = [
            tcol,
            "ts_momentum_multi",
            "mean_reversion_state",
            "funding_carry_adjusted",
            "orderflow_imbalance",
            "funding_rate",
            "funding_z",
            "basis",
            "basis_z",
            "dOI",
            "ewma_vol",
            # legacy names (keep for downstream compatibility)
            "z_tsmom_multi",
            "z_mr",
            "z_fund_carry",
        ]
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
