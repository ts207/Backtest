from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import ensure_dir, read_parquet
from pipelines._lib.run_manifest import finalize_manifest, start_manifest
from pipelines._lib.validation import ensure_utc_timestamp

EPS = 1e-12

def sequential_residualization_fit_apply(X: np.ndarray) -> Tuple[np.ndarray, List[List[float] | None]]:
    Xo = X.copy()
    n, k = Xo.shape
    betas: List[List[float] | None] = []
    for j in range(k):
        if j == 0:
            betas.append(None)
            continue
        Xprev = Xo[:, :j]
        y = Xo[:, j]
        beta, *_ = np.linalg.lstsq(Xprev, y, rcond=None)
        betas.append(beta.astype(np.float64).tolist())
        Xo[:, j] = y - Xprev @ beta
    return Xo, betas

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
    XtX = X.T @ X
    Xty = X.T @ y
    beta = np.linalg.solve(XtX + lam * np.eye(XtX.shape[0], dtype=np.float64), Xty)
    intercept = float(y.mean() - (X @ beta).mean())
    return beta.astype(np.float64), intercept

def ridge_predict(X: np.ndarray, beta: np.ndarray, intercept: float) -> np.ndarray:
    return intercept + X @ beta

def spearman_ic(x: np.ndarray, y: np.ndarray) -> float:
    # simple deterministic Spearman via rankdata
    xr = pd.Series(x).rank(method="average").to_numpy()
    yr = pd.Series(y).rank(method="average").to_numpy()
    if xr.std() < EPS or yr.std() < EPS:
        return float("nan")
    return float(np.corrcoef(xr, yr)[0, 1])

def make_time_blocks(unique_ts: np.ndarray, n_splits: int, val_size: int) -> List[Tuple[int, int]]:
    # Returns list of (train_end_idx, val_end_idx) on unique_ts index
    # Expanding window: train=[0:train_end), val=[train_end:val_end)
    blocks = []
    total = len(unique_ts)
    step = max(1, (total - val_size) // n_splits)
    for s in range(n_splits):
        train_end = min(total - val_size, (s + 1) * step)
        val_end = min(total, train_end + val_size)
        if train_end <= 0 or val_end <= train_end:
            continue
        blocks.append((train_end, val_end))
    return blocks

def load_signals(signals_path: Path) -> pd.DataFrame:
    if signals_path.is_dir():
        files = sorted(list(signals_path.glob("*.parquet")))
        if not files:
            raise FileNotFoundError(f"No parquet files found in signals dir: {signals_path}")
        return read_parquet(files)
    return read_parquet([signals_path])

def main() -> int:
    p = argparse.ArgumentParser(description="Walk-forward CV to select ridge lambda; fit final OrthSpec+Ridge artifact")
    p.add_argument("--run_id", required=True)
    p.add_argument("--signals_path", required=True)
    p.add_argument("--label_path", required=True)
    p.add_argument("--signal_cols", required=True, help="Comma-separated column order")
    p.add_argument("--label_col", default="y")
    p.add_argument("--lambda_grid", default="1e-6,1e-5,1e-4,1e-3,1e-2,1e-1,1,10")
    p.add_argument("--n_splits", type=int, default=5)
    p.add_argument("--val_size", type=int, default=500, help="Validation size in unique timestamps")
    p.add_argument("--purge_bars", type=int, default=0)
    p.add_argument("--embargo_bars", type=int, default=0)
    p.add_argument("--out_dir", default=None)
    args = p.parse_args()

    run_id = args.run_id
    data_root = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
    out_dir = Path(args.out_dir) if args.out_dir else data_root / "model_registry"
    ensure_dir(out_dir)

    stage = "walk_forward_cv_ridge"
    manifest = start_manifest(
        stage,
        run_id,
        params={
            "lambda_grid": args.lambda_grid,
            "n_splits": args.n_splits,
            "val_size": args.val_size,
            "purge_bars": args.purge_bars,
            "embargo_bars": args.embargo_bars,
        },
        inputs=[{"path": args.signals_path}, {"path": args.label_path}],
        outputs=[{"path": str(out_dir)}],
    )

    sig = load_signals(Path(args.signals_path))
    lab = read_parquet([Path(args.label_path)])

    tcol = "ts_event" if "ts_event" in sig.columns else "timestamp"
    sig[tcol] = ensure_utc_timestamp(sig[tcol])
    lab[tcol] = ensure_utc_timestamp(lab[tcol])

    if "symbol" not in sig.columns or "symbol" not in lab.columns:
        raise ValueError("signals and labels must include symbol column")

    cols = [c.strip() for c in args.signal_cols.split(",") if c.strip()]
    df = pd.merge(sig, lab[[tcol, "symbol", args.label_col]], on=[tcol, "symbol"], how="inner").dropna(subset=cols + [args.label_col])
    df = df.sort_values([tcol, "symbol"], kind="mergesort").reset_index(drop=True)

    unique_ts = np.array(sorted(df[tcol].unique()))
    blocks = make_time_blocks(unique_ts, args.n_splits, args.val_size)

    lam_grid = [float(x) for x in args.lambda_grid.split(",") if x.strip()]
    results = []

    for lam in lam_grid:
        fold_metrics = []
        for train_end, val_end in blocks:
            train_ts_end = unique_ts[train_end - 1]
            val_ts_start = unique_ts[train_end]
            val_ts_end = unique_ts[val_end - 1]

            # Purge/embargo around boundary in timestamp index space
            purge = int(args.purge_bars)
            embargo = int(args.embargo_bars)
            # Map purge/embargo as counts of unique timestamps
            train_cut = max(0, train_end - purge)
            val_start = min(len(unique_ts), train_end + embargo)

            if val_start >= val_end or train_cut <= 10:
                continue

            train_ts_max = unique_ts[train_cut - 1]
            val_ts_min = unique_ts[val_start]
            val_ts_max = unique_ts[val_end - 1]

            dtrain = df[df[tcol] <= train_ts_max]
            dval = df[(df[tcol] >= val_ts_min) & (df[tcol] <= val_ts_max)]
            if len(dtrain) < 1000 or len(dval) < 200:
                continue

            Xtr = dtrain[cols].to_numpy(dtype=np.float64)
            ytr = dtrain[args.label_col].to_numpy(dtype=np.float64)

            mean = Xtr.mean(axis=0)
            std = Xtr.std(axis=0, ddof=1)
            Xtrz = (Xtr - mean) / (std + EPS)

            Xtro, betas = sequential_residualization_fit_apply(Xtrz)

            beta_hat, intercept = ridge_fit(Xtro, ytr, lam)

            Xv = dval[cols].to_numpy(dtype=np.float64)
            yv = dval[args.label_col].to_numpy(dtype=np.float64)
            Xvz = (Xv - mean) / (std + EPS)
            Xvo = sequential_residualization_apply(Xvz, betas)
            yhat = ridge_predict(Xvo, beta_hat, intercept)

            mse = float(np.mean((yv - yhat) ** 2))
            ic = spearman_ic(yhat, yv)
            fold_metrics.append({"mse": mse, "ic": ic})

        if not fold_metrics:
            continue
        mse_mean = float(np.nanmean([m["mse"] for m in fold_metrics]))
        ic_mean = float(np.nanmean([m["ic"] for m in fold_metrics]))
        results.append({"lambda": lam, "mse_mean": mse_mean, "ic_mean": ic_mean, "folds": fold_metrics})

    if not results:
        raise RuntimeError("No valid CV results. Check val_size, data coverage, or purge/embargo settings.")

    # Select lambda by lowest mse_mean (tie-break: highest ic_mean)
    results_sorted = sorted(results, key=lambda r: (r["mse_mean"], -np.nan_to_num(r["ic_mean"], nan=-1e9)))
    best = results_sorted[0]

    # Fit final on all data using best lambda
    X = df[cols].to_numpy(dtype=np.float64)
    y = df[args.label_col].to_numpy(dtype=np.float64)
    mean = X.mean(axis=0)
    std = X.std(axis=0, ddof=1)
    Xz = (X - mean) / (std + EPS)
    Xo, betas = sequential_residualization_fit_apply(Xz.copy())
    beta_hat, intercept = ridge_fit(Xo, y, float(best["lambda"]))

    model = {
        "type": "ridge_linear",
        "lambda": float(best["lambda"]),
        "beta": beta_hat.tolist(),
        "intercept": intercept,
        "signal_cols": cols,
        "orth_spec": {
            "method": "sequential_residualization_v1",
            "signal_cols": cols,
            "mean": mean.tolist(),
            "std": std.tolist(),
            "betas": betas,
            "k": len(cols),
        },
        "cv_summary": {
            "selection_metric": "mse_mean (tie-break ic_mean)",
            "best": {"lambda": float(best["lambda"]), "mse_mean": best["mse_mean"], "ic_mean": best["ic_mean"]},
            "all": results_sorted,
        },
        "created_utc": datetime.now(timezone.utc).isoformat(),
    }

    blob = json.dumps(model, sort_keys=True).encode("utf-8")
    import hashlib
    model["artifact_hash"] = hashlib.sha256(blob).hexdigest()

    model_path = out_dir / f"CombModelRidge_{model['artifact_hash']}.json"
    with model_path.open("w", encoding="utf-8") as f:
        json.dump(model, f, indent=2, sort_keys=True)

    report_path = out_dir / f"cv_report_{model['artifact_hash']}.json"
    with report_path.open("w", encoding="utf-8") as f:
        json.dump({"best": model["cv_summary"]["best"], "grid": results_sorted}, f, indent=2, sort_keys=True)

    finalize_manifest(manifest, status="success", stats={"rows": int(len(df)), "model": str(model_path), "report": str(report_path)})
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
