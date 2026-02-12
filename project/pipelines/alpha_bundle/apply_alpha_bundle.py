from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, read_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp

EPS = 1e-12

def sequential_residualization_apply(X: np.ndarray, betas: List[Optional[List[float]]]) -> np.ndarray:
    Xo = X.copy()
    k = Xo.shape[1]
    for j in range(k):
        if j == 0:
            continue
        beta = betas[j]
        if beta is None:
            continue
        b = np.asarray(beta, dtype=np.float64)
        Xprev = Xo[:, :j]
        y = Xo[:, j]
        Xo[:, j] = y - Xprev @ b
    return Xo

def main() -> int:
    p = argparse.ArgumentParser(description="Apply AlphaBundle (standardize + orth + ridge)")
    p.add_argument("--run_id", required=True)
    p.add_argument("--signals_path", required=True, help="Parquet file OR directory of parquets")
    p.add_argument("--ridge_model_path", required=True, help="CombModelRidge_*.json")
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
    out_dir = Path(args.out_dir) if args.out_dir else data_root / "feature_store" / "alpha_bundle"
    ensure_dir(out_dir)

    stage = "alpha_apply_bundle"
    manifest = start_manifest(
        stage,
        run_id,
        params={},
        inputs=[{"path": args.signals_path}, {"path": args.ridge_model_path}],
        outputs=[{"path": str(out_dir)}],
    )

    sp = Path(args.signals_path)
    if sp.is_dir():
        sig_files = sorted(list(sp.glob("*.parquet")))
        if not sig_files:
            raise FileNotFoundError(f"No parquet files found in signals dir: {sp}")
        sig = read_parquet(sig_files)
    else:
        sig = read_parquet([sp])

    tcol = "ts_event" if "ts_event" in sig.columns else "timestamp"
    sig[tcol] = ensure_utc_timestamp(sig[tcol])

    with open(args.ridge_model_path, "r", encoding="utf-8") as f:
        model = json.load(f)

    if "symbol" not in sig.columns:
        raise ValueError("signals must include 'symbol' column for multi-universe apply")

    sig = sig.sort_values([tcol, "symbol"], kind="mergesort").reset_index(drop=True)

    cols = model["signal_cols"]
    X = sig[cols].to_numpy(dtype=np.float64)
    orth = model["orth_spec"]
    mean = np.asarray(orth["mean"], dtype=np.float64)
    std = np.asarray(orth["std"], dtype=np.float64)

    Xz = (X - mean) / (std + EPS)
    if orth.get("method", "") == "sequential_residualization_v1":
        betas = orth.get("betas")
        if not isinstance(betas, list):
            raise ValueError("OrthSpec betas missing or invalid")
        Xo = sequential_residualization_apply(Xz, betas)
    else:
        Xo = Xz

    beta = np.asarray(model["beta"], dtype=np.float64)
    intercept = float(model["intercept"])
    score = intercept + Xo @ beta

    out = sig[[tcol, "symbol"]].copy()
    out["score"] = score

    out_path = out_dir / "alpha_scores.parquet"
    out.to_parquet(out_path, index=False)

    finalize_manifest(manifest, status="success", stats={"rows": int(len(out)), "out": str(out_path)})
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
