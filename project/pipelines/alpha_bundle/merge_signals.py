from __future__ import annotations

"""Merge alpha signal components into a single training/apply table.

Inputs:
  - per-symbol signal parquet files produced by build_alpha_signals_v2.py
  - optional XS momentum parquet produced by build_xs_momentum.py
  - optional on-chain flow parquet produced by build_onchain_flow_signal.py

Output:
  - merged_signals.parquet with columns:
      ts_event, symbol,
      ts_momentum_multi, xs_momentum, mean_reversion_state,
      funding_carry_adjusted, onchain_flow_mc, orderflow_imbalance,
      ewma_vol, dOI, funding_z, basis_z, ... (aux)

This file exists to keep fit/apply stages simple and deterministic.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp


def _read_signals_dir(path: Path) -> pd.DataFrame:
    if path.is_dir():
        files = sorted(list(path.glob("signals_*.parquet")))
        if not files:
            files = sorted(list(path.glob("*.parquet")))
        if not files:
            raise FileNotFoundError(f"No parquet files in {path}")
        return read_parquet([Path(p) for p in files])
    return read_parquet([path])


def main() -> int:
    p = argparse.ArgumentParser(description="Merge signal components into a single table")
    p.add_argument("--run_id", required=True)
    p.add_argument("--signals_dir", required=True, help="Dir from build_alpha_signals_v2.py")
    p.add_argument("--xs_momentum_path", default=None)
    p.add_argument("--onchain_flow_path", default=None)
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
    out_dir = Path(args.out_dir) if args.out_dir else (data_root / "feature_store" / "alpha_bundle")
    ensure_dir(out_dir)

    stage = "alpha_merge_signals"
    inputs = [{"path": args.signals_dir}]
    if args.xs_momentum_path:
        inputs.append({"path": args.xs_momentum_path})
    if args.onchain_flow_path:
        inputs.append({"path": args.onchain_flow_path})
    manifest = start_manifest(stage, run_id, params={}, inputs=inputs, outputs=[{"path": str(out_dir)}])

    sig = _read_signals_dir(Path(args.signals_dir))
    tcol = "ts_event" if "ts_event" in sig.columns else "timestamp"
    sig[tcol] = ensure_utc_timestamp(sig[tcol])
    if "symbol" not in sig.columns:
        raise ValueError("signals must include symbol")
    sig = sig.sort_values([tcol, "symbol"], kind="mergesort").reset_index(drop=True)

    # Core columns expected by spec
    keep = [
        tcol,
        "symbol",
        "ts_momentum_multi",
        "mean_reversion_state",
        "funding_carry_adjusted",
        "orderflow_imbalance",
        # aux
        "ewma_vol",
        "dOI",
        "funding_z",
        "basis_z",
    ]
    existing_keep = [c for c in keep if c in sig.columns]
    df = sig[existing_keep].copy()

    # Optional: XS momentum
    if args.xs_momentum_path:
        xs = read_parquet([Path(args.xs_momentum_path)])
        xs["ts_event"] = ensure_utc_timestamp(xs["ts_event"])
        xs = xs[["ts_event", "symbol", "xs_momentum"]].dropna().sort_values(["ts_event", "symbol"], kind="mergesort")
        df = pd.merge(df, xs, left_on=[tcol, "symbol"], right_on=["ts_event", "symbol"], how="left")
        df = df.drop(columns=["ts_event"], errors="ignore")
    else:
        df["xs_momentum"] = np.nan

    # Optional: on-chain flow
    if args.onchain_flow_path:
        oc = read_parquet([Path(args.onchain_flow_path)])
        oc["ts_event"] = ensure_utc_timestamp(oc["ts_event"])
        oc = oc[["ts_event", "symbol", "onchain_flow_mc"]].dropna().sort_values(["ts_event", "symbol"], kind="mergesort")
        df = pd.merge(df, oc, left_on=[tcol, "symbol"], right_on=["ts_event", "symbol"], how="left")
        df = df.drop(columns=["ts_event"], errors="ignore")
    else:
        df["onchain_flow_mc"] = np.nan

    # Stable column order
    out_cols = [
        tcol,
        "symbol",
        "ts_momentum_multi",
        "xs_momentum",
        "mean_reversion_state",
        "funding_carry_adjusted",
        "onchain_flow_mc",
        "orderflow_imbalance",
        "ewma_vol",
        "dOI",
        "funding_z",
        "basis_z",
    ]
    for c in out_cols:
        if c not in df.columns:
            df[c] = np.nan
    df = df[out_cols].sort_values([tcol, "symbol"], kind="mergesort").reset_index(drop=True)

    out_path = out_dir / "merged_signals.parquet"
    write_parquet(df, out_path)

    finalize_manifest(manifest, status="success", stats={"rows": int(len(df)), "out": str(out_path)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
