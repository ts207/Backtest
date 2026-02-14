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


def sequential_residualize_apply(Xz: np.ndarray, resid_betas: list[list[float] | None]) -> np.ndarray:
    """Apply sequential residualization using stored betas.

    resid_betas[j] is the regression coefficient vector (length j) used to
    residualize column j on columns [0..j-1] after earlier residualizations.
    """
    Xo = Xz.copy()
    k = Xo.shape[1]
    if len(resid_betas) != k:
        raise ValueError(f"resid_betas length {len(resid_betas)} != k {k}")
    for j in range(1, k):
        b = resid_betas[j]
        if b is None:
            continue
        bvec = np.asarray(b, dtype=np.float64)
        Xo[:, j] = Xo[:, j] - Xo[:, :j] @ bvec
    return Xo

def main() -> int:
    p = argparse.ArgumentParser(description="Apply AlphaBundle (orth + ridge + post-processing scaffold)")
    p.add_argument("--run_id", required=True)
    p.add_argument("--signals_path", required=True)
    p.add_argument("--ridge_model_path", required=True)
    p.add_argument("--regime_path", default=None, help="Optional vol_regime.parquet with ts_event, gate_scalar, regime_label")
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    project_root = PROJECT_ROOT
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", project_root.parent / "data"))
    out_dir = Path(args.out_dir) if args.out_dir else data_root / "feature_store" / "alpha_bundle" / run_id
    ensure_dir(out_dir)

    stage = "alpha_apply_bundle"
    inputs = [{"path": args.signals_path}, {"path": args.ridge_model_path}]
    if args.regime_path:
        inputs.append({"path": args.regime_path})
    manifest = start_manifest(stage, run_id, params={"regime_path": args.regime_path}, inputs=inputs, outputs=[{"path": str(out_dir)}])

    sp = Path(args.signals_path)
    if sp.is_dir():
        sig_files = sorted(list(sp.glob("*.parquet")))
        if not sig_files:
            raise FileNotFoundError(f"No parquet files found in signals dir: {sp}")
        sig = read_parquet(sig_files)
    else:
        sig = read_parquet([sp])
    tcol = "ts_event" if "ts_event" in sig.columns else "timestamp"
    sig[tcol] = ensure_utc_timestamp(sig[tcol], tcol)

    with open(args.ridge_model_path, "r", encoding="utf-8") as f:
        model = json.load(f)

    if "symbol" not in sig.columns:
        raise ValueError("signals must include 'symbol' column for multi-universe apply")
    sig = sig.sort_values([tcol, "symbol"], kind="mergesort").reset_index(drop=True)

    cols = model["signal_cols"]
    X = sig[cols].to_numpy(dtype=np.float64)
    mean = np.asarray(model["orth_spec"]["mean"], dtype=np.float64)
    std = np.asarray(model["orth_spec"]["std"], dtype=np.float64)
    Xz = (X - mean) / (std + 1e-12)

    orth = model.get("orth_spec", {})
    method = orth.get("method", "standardize_only_v1")
    if method == "sequential_residualization_v1":
        X_used = sequential_residualize_apply(Xz, orth.get("resid_betas", []))
    else:
        X_used = Xz

    beta = np.asarray(model["beta"], dtype=np.float64)
    intercept = float(model["intercept"])
    score_pre = intercept + X_used @ beta
    score = score_pre

    # Optional regime gating
    regime_label = None
    gate_scalar = None
    if args.regime_path:
        reg = read_parquet([Path(args.regime_path)])
        rtcol = "ts_event" if "ts_event" in reg.columns else "timestamp"
        reg[rtcol] = ensure_utc_timestamp(reg[rtcol], rtcol)
        reg = reg.sort_values(rtcol, kind="mergesort")
        base = pd.merge_asof(sig[[tcol]].sort_values(tcol), reg[[rtcol, "gate_scalar", "regime_label"]].rename(columns={rtcol: tcol}), on=tcol, direction="backward")
        gate_scalar = base["gate_scalar"].astype(float).fillna(1.0).to_numpy(dtype=np.float64)
        regime_label = base["regime_label"].astype(object)
        score = score * gate_scalar

    out = sig[[tcol, "symbol"]].copy()
    out["score_pre_gate"] = score_pre
    if args.regime_path:
        out["regime_label"] = regime_label
        out["gate_scalar"] = gate_scalar
    out["score"] = score
    # Placeholder mu mapping: mu = beta_score * score * sigma * sqrt(h)
    # Requires sigma and h; integrate with risk model / label defs in future patch.
    out_path = out_dir / "alpha_bundle_scores.parquet"
    write_parquet(out, out_path)

    result = {"run_id": run_id, "rows": int(len(out)), "out": str(out_path)}
    finalize_manifest(manifest, status="success", stats=result)
    print(json.dumps(result, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
