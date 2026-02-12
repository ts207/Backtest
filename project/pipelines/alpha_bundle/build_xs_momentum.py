from __future__ import annotations

"""Build cross-sectional momentum signal.

This pipeline produces a per-(ts_event, symbol) signal named `xs_momentum`.

Design:
  1) For each symbol in the UniverseSnapshot, compute trailing return TR_i_t = ln(P_t / P_{t-L}).
  2) Feed TR as a base feature into `build_cross_section_features.py` to obtain cs_rank.
  3) Convert rank into a continuous [-1, +1] score: xs_momentum = 2*cs_rank - 1.

Notes:
  - PIT-safe: uses only data up to and including ts_event.
  - Deterministic: stable sorting + deterministic dense rank from CS pipeline.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, read_parquet, write_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp


def _safe_log_ratio(a: pd.Series, b: pd.Series, eps: float = 1e-12) -> pd.Series:
    return np.log((a.astype(float) + eps) / (b.astype(float) + eps))


def main() -> int:
    p = argparse.ArgumentParser(description="Build XS momentum (rank-based)")
    p.add_argument("--run_id", required=True)
    p.add_argument("--universe_snapshot_path", required=True)
    p.add_argument("--cleaned_root", required=False, default=None)
    p.add_argument("--bar_interval", default="15m")
    p.add_argument("--lookback_bars", type=int, default=60 * 24 * 4, help="Default ~60d on 15m bars")
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
    cleaned_root = Path(args.cleaned_root) if args.cleaned_root else (PROJECT_ROOT / "lake" / "cleaned")

    out_dir = Path(args.out_dir) if args.out_dir else (data_root / "feature_store" / "signals")
    ensure_dir(out_dir)

    stage = "alpha_xs_momentum"
    manifest = start_manifest(
        stage,
        run_id,
        params={"lookback_bars": args.lookback_bars, "bar_interval": args.bar_interval},
        inputs=[{"path": args.universe_snapshot_path}],
        outputs=[{"path": str(out_dir)}],
    )

    snap = read_parquet([Path(args.universe_snapshot_path)])
    snap["ts_event"] = ensure_utc_timestamp(snap["ts_event"])
    sample_row = snap.iloc[0]
    symbols_sorted = list(sample_row["symbols_sorted"])
    universe_id = str(sample_row["universe_id"])

    # Build per-symbol base feature parquet: trailing_return
    base_dir = data_root / "feature_store" / "base_features" / "trailing_return" / f"{universe_id}"
    ensure_dir(base_dir)

    written_base: List[str] = []
    for sym in symbols_sorted:
        bdir = cleaned_root / "perp" / sym / f"bars_{args.bar_interval}"
        files = sorted(list(bdir.glob("**/*.parquet"))) if bdir.exists() else []
        if not files:
            continue
        bars = read_parquet([Path(p) for p in files])
        tcol = "ts_event" if "ts_event" in bars.columns else "timestamp"
        bars[tcol] = ensure_utc_timestamp(bars[tcol])
        price_col = "mid" if "mid" in bars.columns else ("close" if "close" in bars.columns else None)
        if price_col is None:
            continue
        bars = bars.sort_values(tcol, kind="mergesort").reset_index(drop=True)
        P = bars[price_col].astype(float)
        TR = _safe_log_ratio(P, P.shift(args.lookback_bars))
        out = pd.DataFrame({"ts_event": bars[tcol], "trailing_return": TR.astype(float)})
        out_path = base_dir / f"{sym}.parquet"
        write_parquet(out, out_path)
        written_base.append(str(out_path))

    # Use the existing cross-section builder to compute cs_rank for trailing_return.
    # Import locally to avoid circular CLI invocation.
    from pipelines.alpha_bundle.build_cross_section_features import main as cs_main

    cs_out_dir = data_root / "feature_store" / "cross_section"
    ensure_dir(cs_out_dir)

    # Run CS builder by emulating argv
    argv_saved = sys.argv
    try:
        sys.argv = [
            "build_cross_section_features",
            "--run_id",
            run_id,
            "--universe_snapshot_path",
            str(Path(args.universe_snapshot_path)),
            "--base_feature_dir",
            str(base_dir),
            "--base_feature_name",
            "trailing_return",
            "--out_dir",
            str(cs_out_dir),
        ]
        cs_main()
    finally:
        sys.argv = argv_saved

    cs_path = cs_out_dir / f"cs_{universe_id}_trailing_return.parquet"
    cs = read_parquet([cs_path]) if cs_path.exists() else pd.DataFrame()
    if cs.empty:
        finalize_manifest(manifest, status="success", stats={"rows": 0, "note": "no CS rows"})
        return 0

    cs["ts_event"] = ensure_utc_timestamp(cs["ts_event"])
    cs = cs.sort_values(["ts_event", "symbol"], kind="mergesort").reset_index(drop=True)
    cs["xs_momentum"] = (2.0 * cs["cs_rank"].astype(float) - 1.0).astype(float)
    out = cs[["ts_event", "symbol", "xs_momentum"]].copy()
    out_path = out_dir / f"xs_momentum_{universe_id}.parquet"
    write_parquet(out, out_path)

    finalize_manifest(
        manifest,
        status="success",
        stats={"rows": int(len(out)), "out": str(out_path), "base_written": len(written_base)},
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
