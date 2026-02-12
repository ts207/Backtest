from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, read_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp

EPS = 1e-12

def sequential_residualization_fit(X: np.ndarray) -> Dict[str, object]:
    """
    Fit a deterministic sequential residualization transform on standardized features.

    Given standardized X (n,k), we compute for j=1..k:
      resid_j = x_j - X_prev @ beta_j
    using OLS. Store beta_j for online apply.

    Returns an OrthSpec dict containing betas (list of lists) and schema.
    """
    n, k = X.shape
    betas: List[List[float] | None] = []
    for j in range(k):
        if j == 0:
            betas.append(None)
            continue
        Xprev = X[:, :j]
        y = X[:, j]
        # OLS via lstsq (deterministic given fixed ordering + float64)
        beta, *_ = np.linalg.lstsq(Xprev, y, rcond=None)
        betas.append(beta.astype(np.float64).tolist())
        # replace column j with residual for subsequent steps
        X[:, j] = y - Xprev @ beta
    return {
        "method": "sequential_residualization_v1",
        "betas": betas,
        "k": k,
    }

def sequential_residualization_apply(X: np.ndarray, betas: List[List[float] | None]) -> np.ndarray:
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

def ridge_fit(X: np.ndarray, y: np.ndarray, lam: float) -> Tuple[np.ndarray, float]:
    """Ridge on already standardized/orthogonalized X."""
    XtX = X.T @ X
    Xty = X.T @ y
    beta = np.linalg.solve(XtX + lam * np.eye(XtX.shape[0], dtype=np.float64), Xty)
    intercept = float(y.mean() - (X @ beta).mean())
    return beta.astype(np.float64), intercept

def main() -> int:
    p = argparse.ArgumentParser(description="Fit OrthSpec (sequential residualization) + Ridge (offline)")
    p.add_argument("--run_id", required=True)
    p.add_argument("--signals_path", required=True, help="Parquet with ts_event, symbol, and signal columns")
    p.add_argument("--label_path", required=True, help="Parquet with ts_event, symbol, and y column")
    p.add_argument("--signal_cols", required=True, help="Comma-separated signal column names in deterministic order")
    p.add_argument("--label_col", default="y")
    p.add_argument("--lambda_", type=float, default=1e-3)
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
    out_dir = Path(args.out_dir) if args.out_dir else data_root / "model_registry"
    ensure_dir(out_dir)

    stage = "alpha_fit_orth_ridge"
    manifest = start_manifest(
        stage,
        run_id,
        params={"lambda": args.lambda_, "signal_cols": args.signal_cols, "label_col": args.label_col},
        inputs=[{"path": args.signals_path}, {"path": args.label_path}],
        outputs=[{"path": str(out_dir)}],
    )

    sig = read_parquet([Path(args.signals_path)])
    lab = read_parquet([Path(args.label_path)])
    tcol = "ts_event" if "ts_event" in sig.columns else "timestamp"
    sig[tcol] = ensure_utc_timestamp(sig[tcol])
    lab[tcol] = ensure_utc_timestamp(lab[tcol])

    if "symbol" not in sig.columns or "symbol" not in lab.columns:
        raise ValueError("signals_path and label_path must include a 'symbol' column for multi-universe fitting")

    cols = [c.strip() for c in args.signal_cols.split(",") if c.strip()]

    df = pd.merge(sig, lab[[tcol, "symbol", args.label_col]], on=[tcol, "symbol"], how="inner").dropna(subset=cols + [args.label_col])
    df = df.sort_values([tcol, "symbol"], kind="mergesort").reset_index(drop=True)

    X = df[cols].to_numpy(dtype=np.float64)
    y = df[args.label_col].to_numpy(dtype=np.float64)

    # Standardize (train statistics)
    mean = X.mean(axis=0)
    std = X.std(axis=0, ddof=1)
    Xz = (X - mean) / (std + EPS)

    # Orth fit+apply (residualization)
    Xz_for_fit = Xz.copy()
    orth_meta = sequential_residualization_fit(Xz_for_fit)
    Xo = sequential_residualization_apply(Xz, orth_meta["betas"])  # type: ignore[arg-type]

    lam = float(args.lambda_)
    beta, intercept = ridge_fit(Xo, y, lam)

    model = {
        "type": "ridge_linear",
        "lambda": lam,
        "beta": beta.tolist(),
        "intercept": intercept,
        "signal_cols": cols,
        "orth_spec": {
            "signal_cols": cols,
            "mean": mean.tolist(),
            "std": std.tolist(),
            **orth_meta,
        },
        "created_utc": datetime.now(timezone.utc).isoformat(),
    }

    blob = json.dumps(model, sort_keys=True).encode("utf-8")
    model["artifact_hash"] = hashlib.sha256(blob).hexdigest()

    out_path = out_dir / f"CombModelRidge_{model['artifact_hash']}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(model, f, indent=2, sort_keys=True)

    finalize_manifest(manifest, status="success", stats={"rows": int(len(df)), "out": str(out_path)})
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
