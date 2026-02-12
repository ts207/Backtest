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


def _resolve_symbol_feature_path(base_feature_dir: Path, symbol: str) -> Optional[Path]:
    # Support both "<SYMBOL>.parquet" and "signals_<SYMBOL>.parquet".
    candidates = [
        base_feature_dir / f"{symbol}.parquet",
        base_feature_dir / f"signals_{symbol}.parquet",
    ]
    for path in candidates:
        if path.exists():
            return path
    return None


def _dense_rank(values: np.ndarray, symbols_sorted: List[str], ascending: bool) -> np.ndarray:
    # Deterministic dense rank with stable tie-breaker.
    order = np.argsort(values, kind="mergesort")
    if not ascending:
        order = order[::-1]
    sorted_vals = values[order]
    ranks = np.empty_like(order, dtype=np.int64)
    rank = 1
    ranks[order[0]] = rank
    for i in range(1, len(order)):
        if sorted_vals[i] != sorted_vals[i-1]:
            rank += 1
        ranks[order[i]] = rank
    return ranks

def main() -> int:
    p = argparse.ArgumentParser(description="Build CrossSectionAgg + derived CS features (PIT, deterministic)")
    p.add_argument("--run_id", required=True)
    p.add_argument("--universe_snapshot_path", required=True)
    p.add_argument("--base_feature_dir", required=True, help="Directory of per-symbol feature parquet files")
    p.add_argument("--base_feature_name", required=True, help="Column name in base feature files")
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    project_root = PROJECT_ROOT
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", project_root.parent / "data"))
    out_dir = Path(args.out_dir) if args.out_dir else data_root / "feature_store" / "cross_section"
    ensure_dir(out_dir)

    stage = "alpha_cross_section"
    manifest = start_manifest(stage, run_id, params={"base_feature_name": args.base_feature_name}, inputs=[{"path": args.universe_snapshot_path}], outputs=[{"path": str(out_dir)}])

    snap = read_parquet([Path(args.universe_snapshot_path)])
    snap["ts_event"] = ensure_utc_timestamp(snap["ts_event"])
    # Multi-snapshot mode: compute cross-sectional stats at each ts_event using the UniverseSnapshot at that same ts_event.
    # Base feature per symbol is expected as a parquet file at <base_feature_dir>/<symbol>.parquet
    # with columns: ts_event (or timestamp) and <base_feature_name>.

    # Preload base features into per-symbol series keyed by ts_event for fast lookups.
    sym_to_series: Dict[str, pd.Series] = {}
    # Determine symbol universe from snapshot file (symbols_sorted stored as array in each row)
    sample_row = snap.iloc[0]
    symbols_sorted = list(sample_row["symbols_sorted"])
    universe_id = str(sample_row["universe_id"])

    for sym in symbols_sorted:
        fpath = _resolve_symbol_feature_path(Path(args.base_feature_dir), sym)
        if not fpath:
            continue
        df = read_parquet([fpath])
        tcol = "ts_event" if "ts_event" in df.columns else "timestamp"
        df[tcol] = ensure_utc_timestamp(df[tcol])
        if args.base_feature_name not in df.columns:
            continue
        s = df.set_index(tcol)[args.base_feature_name].astype(float)
        sym_to_series[sym] = s

    eps = 1e-12
    rows_out: List[Dict[str, object]] = []

    for _, row in snap.iterrows():
        ts_event = row["ts_event"]
        inc_flags = list(row["included_flags"])
        used_symbols: List[str] = []
        values: List[float] = []

        for sym, inc in zip(symbols_sorted, inc_flags):
            if not inc:
                continue
            ser = sym_to_series.get(sym)
            if ser is None:
                continue
            if ts_event not in ser.index:
                continue
            v = float(ser.loc[ts_event])
            if np.isfinite(v):
                used_symbols.append(sym)
                values.append(v)

        if len(values) < 3:
            continue

        x = np.asarray(values, dtype=np.float64)
        cs_mean = float(np.mean(x))
        cs_std = float(np.std(x, ddof=1)) if len(x) > 1 else 0.0
        cs_median = float(np.median(x))
        cs_mad = float(np.median(np.abs(x - cs_median)))

        cs_z = (x - cs_mean) / (cs_std + eps)
        cs_rz = (x - cs_median) / (1.4826 * cs_mad + eps)
        ranks = _dense_rank(x, used_symbols, ascending=True)
        cs_rank = ranks / max(1, len(x))

        for sym, v, z, rz, rk in zip(used_symbols, values, cs_z, cs_rz, cs_rank):
            rows_out.append(
                {
                    "ts_event": ts_event,
                    "universe_id": universe_id,
                    "symbol": sym,
                    "base_feature_name": args.base_feature_name,
                    "base_feature_value": float(v),
                    "cs_mean": cs_mean,
                    "cs_std": cs_std,
                    "cs_median": cs_median,
                    "cs_mad": cs_mad,
                    "cs_zscore": float(z),
                    "cs_robust_z": float(rz),
                    "cs_rank": float(rk),
                    "cs_def_version": 1,
                }
            )

    out = pd.DataFrame(rows_out)
    if out.empty:
        finalize_manifest(manifest, status="success", stats={"rows": 0, "note": "no rows emitted"})
        return 0

    out["ts_event"] = ensure_utc_timestamp(out["ts_event"])
    out_path = out_dir / f"cs_{universe_id}_{args.base_feature_name}.parquet"
    write_parquet(out, out_path)

    finalize_manifest(manifest, status="success", stats={"rows": int(len(out)), "out": str(out_path), "ts_events": int(out["ts_event"].nunique())})
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
