from __future__ import annotations

"""Build on-chain exchange netflow signal standardized by market cap.

Input contract (parquet/csv supported via pandas):
  columns: ts_event, asset, provider, netflow_coin, mcap_usd

Signal:
  flow_usd_t = netflow_coin_t * price_t
  f_t = flow_usd_t / (mcap_usd_t + eps)
  Zflow_t = MAD-z over rolling window (default 30)
  onchain_flow_mc = -clip(Zflow_t, -5, +5)

Output:
  ts_event, symbol, onchain_flow_mc

Notes:
  - `asset` (e.g., BTC) is mapped to trading `symbol` using a simple rule:
      symbol = asset + quote_suffix (default: USDT)
    Override mapping via --asset_to_symbol_json if needed.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp


def robust_z(series: pd.Series, window: int, eps: float = 1e-12) -> pd.Series:
    def _rz(x: np.ndarray) -> float:
        x = x.copy()
        lo, hi = np.quantile(x, 0.01), np.quantile(x, 0.99)
        x = np.clip(x, lo, hi)
        med = np.median(x)
        mad = np.median(np.abs(x - med))
        return float((x[-1] - med) / (1.4826 * mad + eps))
    return series.rolling(window=window, min_periods=window).apply(_rz, raw=True)


def main() -> int:
    p = argparse.ArgumentParser(description="Build on-chain flow signal (netflow standardized by mcap)")
    p.add_argument("--run_id", required=True)
    p.add_argument("--onchain_path", required=True, help="Parquet/CSV with OnChainFlow schema")
    p.add_argument("--cleaned_root", required=False, default=None)
    p.add_argument("--bar_interval", default="15m")
    p.add_argument("--quote_suffix", default="USDT")
    p.add_argument("--asset_to_symbol_json", default=None, help="Optional JSON mapping asset->symbol")
    p.add_argument("--window", type=int, default=30)
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
    cleaned_root = Path(args.cleaned_root) if args.cleaned_root else (data_root / "lake" / "cleaned")

    out_dir = Path(args.out_dir) if args.out_dir else (data_root / "feature_store" / "signals")
    ensure_dir(out_dir)

    stage = "alpha_onchain_flow"
    manifest = start_manifest(
        stage,
        run_id,
        params={"window": args.window, "bar_interval": args.bar_interval},
        inputs=[{"path": args.onchain_path}],
        outputs=[{"path": str(out_dir)}],
    )

    path = Path(args.onchain_path)
    if path.suffix.lower() in {".parquet"}:
        oc = read_parquet([path])
    else:
        oc = pd.read_csv(path)

    if oc.empty:
        finalize_manifest(manifest, status="success", stats={"rows": 0, "note": "empty onchain input"})
        return 0

    oc["ts_event"] = ensure_utc_timestamp(oc["ts_event"], "ts_event")
    required = {"ts_event", "asset", "netflow_coin", "mcap_usd"}
    missing = required - set(oc.columns)
    if missing:
        raise ValueError(f"onchain_path missing columns: {sorted(missing)}")

    asset_to_symbol: Dict[str, str] = {}
    if args.asset_to_symbol_json:
        asset_to_symbol.update(json.loads(args.asset_to_symbol_json))

    def map_symbol(asset: str) -> str:
        if asset in asset_to_symbol:
            return asset_to_symbol[asset]
        return f"{asset}{args.quote_suffix}"

    oc["symbol"] = oc["asset"].astype(str).map(map_symbol)

    # Attach price at ts_event for each symbol (from perp bars)
    out_rows = []
    for sym, g in oc.groupby("symbol", sort=True):
        bdir = cleaned_root / "perp" / sym / f"bars_{args.bar_interval}"
        files = sorted(list(bdir.glob("**/*.parquet"))) if bdir.exists() else []
        if not files:
            continue
        bars = read_parquet([Path(p) for p in files])
        tcol = "ts_event" if "ts_event" in bars.columns else "timestamp"
        bars[tcol] = ensure_utc_timestamp(bars[tcol], tcol)
        price_col = "mid" if "mid" in bars.columns else ("close" if "close" in bars.columns else None)
        if price_col is None:
            continue
        bars = bars.sort_values(tcol, kind="mergesort").reset_index(drop=True)
        gb = g.sort_values("ts_event", kind="mergesort").reset_index(drop=True)
        merged = pd.merge_asof(gb[["ts_event", "netflow_coin", "mcap_usd"]], bars[[tcol, price_col]].rename(columns={tcol: "ts_event"}), on="ts_event", direction="backward")
        px = merged[price_col].astype(float)
        flow_usd = merged["netflow_coin"].astype(float) * px
        f = flow_usd / (merged["mcap_usd"].astype(float) + 1e-12)
        z = robust_z(f, window=args.window).clip(-5.0, 5.0)
        sig = (-z).astype(float)
        tmp = pd.DataFrame({"ts_event": merged["ts_event"], "symbol": sym, "onchain_flow_mc": sig})
        out_rows.append(tmp)

    if not out_rows:
        finalize_manifest(manifest, status="success", stats={"rows": 0, "note": "no symbols matched bars"})
        return 0

    out = pd.concat(out_rows, ignore_index=True)
    out["ts_event"] = ensure_utc_timestamp(out["ts_event"], "ts_event")
    out = out.sort_values(["ts_event", "symbol"], kind="mergesort").reset_index(drop=True)
    out_path = out_dir / "onchain_flow_mc.parquet"
    write_parquet(out, out_path)

    finalize_manifest(manifest, status="success", stats={"rows": int(len(out)), "out": str(out_path)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
