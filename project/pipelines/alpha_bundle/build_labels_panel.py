from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, read_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp

def _load_bars_for_symbol(bars_root: Path, symbol: str) -> pd.DataFrame:
    # Accept either bars_root/<symbol>.parquet or bars_root/**/<symbol>*.parquet
    direct = bars_root / f"{symbol}.parquet"
    if direct.exists():
        return read_parquet([direct])
    matches = sorted(list(bars_root.rglob(f"*{symbol}*.parquet")))
    if not matches:
        raise FileNotFoundError(f"Could not find bars parquet for symbol={symbol} under {bars_root}")
    return read_parquet(matches)

def main() -> int:
    p = argparse.ArgumentParser(description="Build panel labels y(ts_event,symbol) = log(P_{t+h}/P_t)")
    p.add_argument("--run_id", required=True)
    p.add_argument("--bars_root", required=True, help="Directory containing per-symbol bar parquets (or nested)")
    p.add_argument("--symbols", required=True, help="Comma-separated symbols, e.g. BTCUSDT,ETHUSDT")
    p.add_argument("--horizon_bars", type=int, required=True, help="Forward horizon in bars")
    p.add_argument("--price_col", default=None, help="Price column to use (default: mid if present else close)")
    p.add_argument("--out_path", default=None, help="Output parquet path (default: data/labels/labels_h{h}.parquet)")
    args = p.parse_args()

    run_id = args.run_id
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
    out_path = Path(args.out_path) if args.out_path else data_root / "labels" / f"labels_h{args.horizon_bars}.parquet"
    ensure_dir(out_path.parent)

    stage = "build_labels_panel"
    manifest = start_manifest(stage, run_id, params={"horizon_bars": args.horizon_bars}, inputs=[{"path": args.bars_root}], outputs=[{"path": str(out_path)}])

    bars_root = Path(args.bars_root)
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    frames: List[pd.DataFrame] = []
    h = int(args.horizon_bars)

    for sym in symbols:
        df = _load_bars_for_symbol(bars_root, sym)
        tcol = "ts_event" if "ts_event" in df.columns else "timestamp"
        df[tcol] = ensure_utc_timestamp(df[tcol])
        df = df.sort_values(tcol, kind="mergesort").reset_index(drop=True)

        price_col = args.price_col
        if price_col is None:
            price_col = "mid" if "mid" in df.columns else ("close" if "close" in df.columns else None)
        if price_col is None or price_col not in df.columns:
            raise ValueError(f"No usable price column found for {sym}. Provide --price_col. Columns={list(df.columns)}")

        p0 = df[price_col].astype("float64")
        p1 = p0.shift(-h)
        y = np.log(p1 / p0)
        out = pd.DataFrame({
            "ts_event": df[tcol],
            "symbol": sym,
            "y": y,
        })
        out = out.dropna()
        frames.append(out)

    panel = pd.concat(frames, ignore_index=True)
    panel = panel.sort_values(["ts_event", "symbol"], kind="mergesort").reset_index(drop=True)
    panel.to_parquet(out_path, index=False)

    finalize_manifest(manifest, status="success", stats={"rows": int(len(panel)), "out": str(out_path)})
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
