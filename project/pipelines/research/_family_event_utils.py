from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

from pipelines._lib.io_utils import choose_partition_dir, ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))

EVENT_COLUMNS = [
    "event_type",
    "event_id",
    "symbol",
    "anchor_ts",
    "enter_ts",
    "exit_ts",
    "event_idx",
    "year",
    "event_score",
    "basis_z",
    "spread_z",
    "funding_rate_scaled",
    "oi_notional",
    "liquidation_notional",
]


def safe_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce")
    return pd.Series(np.nan, index=df.index, dtype=float)


def rolling_z(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window=window, min_periods=max(24, window // 4)).mean()
    std = series.rolling(window=window, min_periods=max(24, window // 4)).std().replace(0.0, np.nan)
    return (series - mean) / std


def past_quantile(series: pd.Series, q: float, window: int = 576, min_periods: int = 96) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce")
    return s.rolling(window=window, min_periods=min_periods).quantile(q).shift(1)


def load_features(run_id: str, symbol: str, timeframe: str) -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, timeframe, "features_v1"),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / timeframe / "features_v1",
    ]
    features_dir = choose_partition_dir(candidates)
    files = list_parquet_files(features_dir) if features_dir else []
    if not files:
        return pd.DataFrame()
    frame = read_parquet(files)
    if frame.empty or "timestamp" not in frame.columns:
        return pd.DataFrame()
    frame = frame.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True, errors="coerce")
    frame = frame.dropna(subset=["timestamp"]).sort_values("timestamp").reset_index(drop=True)
    return frame


def sparsify(mask: pd.Series, min_spacing: int) -> List[int]:
    idxs = np.flatnonzero(mask.fillna(False).values)
    selected: List[int] = []
    last = -10**9
    for idx in idxs:
        i = int(idx)
        if i - last >= int(min_spacing):
            selected.append(i)
            last = i
    return selected


def rows_for_event(
    df: pd.DataFrame,
    *,
    symbol: str,
    event_type: str,
    mask: pd.Series,
    event_score: pd.Series | None = None,
    min_spacing: int = 6,
) -> pd.DataFrame:
    idxs = sparsify(mask, min_spacing=min_spacing)
    if not idxs:
        return pd.DataFrame(columns=EVENT_COLUMNS)

    score_series = event_score if event_score is not None else safe_series(df, "rv_96")
    basis_series = safe_series(df, "basis_zscore")
    if basis_series.isna().all():
        basis_series = safe_series(df, "cross_exchange_spread_z")

    rows: List[Dict[str, object]] = []
    for n, idx in enumerate(idxs):
        ts = pd.to_datetime(df.at[idx, "timestamp"], utc=True, errors="coerce")
        if pd.isna(ts):
            continue
        end_idx = min(len(df) - 1, idx + 12)
        exit_ts = pd.to_datetime(df.at[end_idx, "timestamp"], utc=True, errors="coerce")
        rows.append(
            {
                "event_type": event_type,
                "event_id": f"{event_type.lower()}_{symbol}_{idx:08d}_{n:03d}",
                "symbol": symbol,
                "anchor_ts": ts.isoformat(),
                "enter_ts": ts.isoformat(),
                "exit_ts": exit_ts.isoformat() if pd.notna(exit_ts) else ts.isoformat(),
                "event_idx": int(idx),
                "year": int(ts.year),
                "event_score": float(np.nan_to_num(score_series.iloc[idx], nan=0.0)),
                "basis_z": float(np.nan_to_num(basis_series.iloc[idx], nan=0.0)),
                "spread_z": float(np.nan_to_num(safe_series(df, "spread_zscore").iloc[idx], nan=0.0)),
                "funding_rate_scaled": float(np.nan_to_num(safe_series(df, "funding_rate_scaled").iloc[idx], nan=0.0)),
                "oi_notional": float(np.nan_to_num(safe_series(df, "oi_notional").iloc[idx], nan=0.0)),
                "liquidation_notional": float(np.nan_to_num(safe_series(df, "liquidation_notional").iloc[idx], nan=0.0)),
            }
        )
    return pd.DataFrame(rows, columns=EVENT_COLUMNS)


def merge_event_csv(out_path: Path, event_type: str, new_df: pd.DataFrame) -> pd.DataFrame:
    ensure_dir(out_path.parent)
    if out_path.exists():
        try:
            prior = pd.read_csv(out_path)
        except Exception:
            prior = pd.DataFrame()
        if not prior.empty and "event_type" in prior.columns:
            prior = prior[prior["event_type"].astype(str) != event_type].copy()
            new_df = pd.concat([prior, new_df], ignore_index=True)
    new_df.to_csv(out_path, index=False)
    return new_df
