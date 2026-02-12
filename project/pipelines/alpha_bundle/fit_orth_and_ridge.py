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


def sequential_residualize(Xz: np.ndarray) -> tuple[np.ndarray, list[list[float] | None]]:
    """Sequentially residualize columns of Xz.

    For j=1..k-1:
      Xz[:,j] <- residual from regressing Xz[:,j] on Xz[:,0..j-1].

    Returns:
      Xo: residualized matrix
      betas: list where betas[j] is length-j regression beta (or None for j=0)
    """
    n, k = Xz.shape
    Xo = Xz.copy()
    betas: list[list[float] | None] = [None]
    for j in range(1, k):
        Xprev = Xo[:, :j]
        y = Xo[:, j]
        # OLS solve Xprev * b ≈ y
        b = np.linalg.lstsq(Xprev, y, rcond=None)[0]
        Xo[:, j] = y - Xprev @ b
        betas.append(b.astype(float).tolist())
    return Xo, betas


def time_block_splits(n: int, k_blocks: int) -> list[tuple[np.ndarray, np.ndarray]]:
    """Deterministic forward-chaining splits for walk-forward CV.

    Splits indices into k_blocks contiguous chunks.
    For block b>=1: train = [0:cut_b), val = chunk_b.
    """
    k_blocks = max(2, int(k_blocks))
    cuts = np.linspace(0, n, k_blocks + 1, dtype=int)
    splits = []
    for b in range(1, k_blocks):
        tr = np.arange(cuts[0], cuts[b])
        va = np.arange(cuts[b], cuts[b + 1])
        if len(tr) < 100 or len(va) < 50:
            continue
        splits.append((tr, va))
    return splits


def ridge_fit(X: np.ndarray, y: np.ndarray, lam: float) -> tuple[np.ndarray, float]:
    """Fit ridge regression with intercept on centered y."""
    XtX = X.T @ X
    Xty = X.T @ y
    beta = np.linalg.solve(XtX + float(lam) * np.eye(XtX.shape[0]), Xty)
    intercept = float(y.mean() - (X @ beta).mean())
    return beta, intercept


def corr_ic(a: np.ndarray, b: np.ndarray) -> float:
    a = np.asarray(a, dtype=np.float64)
    b = np.asarray(b, dtype=np.float64)
    mask = np.isfinite(a) & np.isfinite(b)
    if mask.sum() < 30:
        return 0.0
    aa = a[mask]
    bb = b[mask]
    if np.std(aa) == 0.0 or np.std(bb) == 0.0:
        return 0.0
    return float(np.corrcoef(aa, bb)[0, 1])

def main() -> int:
    p = argparse.ArgumentParser(description="Fit OrthSpec + Ridge (offline)")
    p.add_argument("--run_id", required=True)
    p.add_argument("--signals_path", required=True, help="Parquet with ts_event, symbol, and signal columns")
    p.add_argument("--label_path", required=True, help="Parquet with ts_event, symbol, and y column")
    p.add_argument("--signal_cols", required=True, help="Comma-separated signal column names in deterministic order")
    p.add_argument("--label_col", default="y")
    p.add_argument("--orth_method", default="residualization", choices=["residualization", "standardize_only"], help="Orthogonalization method")
    p.add_argument("--lambda_", type=float, default=1e-3, help="Used if --lambda_grid is not provided")
    p.add_argument("--lambda_grid", default=None, help="Optional comma-separated lambda grid for walk-forward CV")
    p.add_argument("--cv_blocks", type=int, default=6)
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    project_root = PROJECT_ROOT
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", project_root.parent / "data"))
    out_dir = Path(args.out_dir) if args.out_dir else data_root / "model_registry"
    ensure_dir(out_dir)

    stage = "alpha_fit_orth_ridge"
    manifest = start_manifest(
        stage,
        run_id,
        params={
            "orth_method": args.orth_method,
            "lambda": args.lambda_,
            "lambda_grid": args.lambda_grid,
            "cv_blocks": args.cv_blocks,
        },
        inputs=[{"path": args.signals_path}, {"path": args.label_path}],
        outputs=[{"path": str(out_dir)}],
    )

    sig = read_parquet([Path(args.signals_path)])
    lab = read_parquet([Path(args.label_path)])
    tcol = "ts_event" if "ts_event" in sig.columns else "timestamp"
    sig[tcol] = ensure_utc_timestamp(sig[tcol])
    lab[tcol] = ensure_utc_timestamp(lab[tcol])

    # Multi-universe: merge by (ts_event, symbol)
    if "symbol" not in sig.columns or "symbol" not in lab.columns:
        raise ValueError("signals_path and label_path must include a 'symbol' column for multi-universe fitting")
    df = pd.merge(sig, lab[[tcol, "symbol", args.label_col]], on=[tcol, "symbol"], how="inner").dropna()
    cols = [c.strip() for c in args.signal_cols.split(",") if c.strip()]
    # Deterministic row ordering
    df = df.sort_values([tcol, "symbol"], kind="mergesort").reset_index(drop=True)
    X = df[cols].to_numpy(dtype=np.float64)
    y = df[args.label_col].to_numpy(dtype=np.float64)

    # Standardize
    mean = X.mean(axis=0)
    std = X.std(axis=0, ddof=1)
    Xz = (X - mean) / (std + 1e-12)

    # Orthogonalize
    if args.orth_method == "residualization":
        Xo, resid_betas = sequential_residualize(Xz)
        orth_spec = {
            "signal_cols": cols,
            "mean": mean.tolist(),
            "std": std.tolist(),
            "method": "sequential_residualization_v1",
            "resid_betas": resid_betas,
        }
        X_used = Xo
    else:
        orth_spec = {"signal_cols": cols, "mean": mean.tolist(), "std": std.tolist(), "method": "standardize_only_v1"}
        X_used = Xz

    # Choose lambda
    if args.lambda_grid:
        grid = [float(x.strip()) for x in args.lambda_grid.split(",") if x.strip()]
        splits = time_block_splits(len(df), args.cv_blocks)
        best_lam = None
        best_ic = -1e18
        for lam in grid:
            ics = []
            for tr, va in splits:
                beta_tr, intercept_tr = ridge_fit(X_used[tr], y[tr], lam)
                pred = intercept_tr + X_used[va] @ beta_tr
                ics.append(corr_ic(pred, y[va]))
            mean_ic = float(np.mean(ics)) if ics else 0.0
            if mean_ic > best_ic:
                best_ic = mean_ic
                best_lam = lam
        lam = float(best_lam if best_lam is not None else args.lambda_)
        cv_stats = {"mean_ic": best_ic, "splits": len(splits), "grid": grid}
    else:
        lam = float(args.lambda_)
        cv_stats = None

    beta, intercept = ridge_fit(X_used, y, lam)

    model = {
        "type": "ridge_linear",
        "lambda": lam,
        "beta": beta.tolist(),
        "intercept": intercept,
        "signal_cols": cols,
        "orth_spec": orth_spec,
        "cv_stats": cv_stats,
    }

    import hashlib
    blob = json.dumps(model, sort_keys=True).encode("utf-8")
    model["artifact_hash"] = hashlib.sha256(blob).hexdigest()

    out_path = out_dir / f"CombModelRidge_{model['artifact_hash']}.json"
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(model, f, indent=2, sort_keys=True)

    finalize_manifest(manifest, status="success", stats={"rows": int(len(df)), "out": str(out_path)})
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
