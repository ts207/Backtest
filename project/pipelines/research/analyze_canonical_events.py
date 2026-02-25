from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from events.registry import EVENT_REGISTRY_SPECS
from pipelines._lib.io_utils import choose_partition_dir, ensure_dir, list_parquet_files, read_parquet, run_scoped_lake_path


def _safe_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce")
    return pd.Series(np.nan, index=df.index, dtype=float)


def _rolling_z(series: pd.Series, window: int) -> pd.Series:
    mean = series.rolling(window=window, min_periods=max(24, window // 4)).mean()
    std = series.rolling(window=window, min_periods=max(24, window // 4)).std().replace(0.0, np.nan)
    return (series - mean) / std


def _load_features(run_id: str, symbol: str, timeframe: str) -> pd.DataFrame:
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


def _event_mask(df: pd.DataFrame, event_type: str) -> pd.Series:
    close = _safe_series(df, "close")
    high = _safe_series(df, "high")
    low = _safe_series(df, "low")
    rv_96 = _safe_series(df, "rv_96")
    spread_z = _safe_series(df, "spread_zscore")
    basis_z = _safe_series(df, "basis_zscore")
    if basis_z.isna().all():
        basis_z = _safe_series(df, "cross_exchange_spread_z")
    funding = _safe_series(df, "funding_rate_scaled")
    oi = _safe_series(df, "oi_notional")
    oi_delta_1h = _safe_series(df, "oi_delta_1h")
    range_96 = _safe_series(df, "range_96")
    range_med_2880 = _safe_series(df, "range_med_2880")
    trend_96 = close.pct_change(96)
    ret_1 = close.pct_change(1)

    rv_z = _rolling_z(rv_96.ffill(), 288)
    funding_abs = funding.abs()
    oi_pct = oi.pct_change(1, fill_method=None)

    comp_ratio = (range_96 / range_med_2880.replace(0.0, np.nan)).astype(float)
    px_z = _rolling_z(close.pct_change(12).fillna(0.0), 288)

    q = lambda s, v: float(s.quantile(v)) if s.notna().any() else np.nan

    if event_type == "DEPTH_COLLAPSE":
        return ((spread_z >= q(spread_z, 0.90)).fillna(False) & (rv_z >= 1.0).fillna(False))
    if event_type == "SPREAD_BLOWOUT":
        return (spread_z >= max(2.0, q(spread_z, 0.97))).fillna(False)
    if event_type == "ORDERFLOW_IMBALANCE_SHOCK":
        return ((ret_1.abs() >= q(ret_1.abs(), 0.99)).fillna(False) & (rv_z >= 1.2).fillna(False))
    if event_type == "SWEEP_STOPRUN":
        wick = ((high - close).abs() + (close - low).abs()) / close.replace(0.0, np.nan)
        return ((wick >= q(wick, 0.97)).fillna(False) & (ret_1.abs() >= q(ret_1.abs(), 0.90)).fillna(False))
    if event_type == "ABSORPTION_EVENT":
        return ((ret_1.abs() <= q(ret_1.abs(), 0.35)).fillna(False) & (spread_z >= q(spread_z, 0.80)).fillna(False))
    if event_type == "LIQUIDITY_GAP_PRINT":
        return (ret_1.abs() >= q(ret_1.abs(), 0.995)).fillna(False)

    if event_type == "VOL_SPIKE":
        return (rv_z >= max(2.0, q(rv_z, 0.97))).fillna(False)
    if event_type == "VOL_RELAXATION_START":
        return ((rv_z.shift(1) >= q(rv_z, 0.95)).fillna(False) & (rv_z < q(rv_z, 0.70)).fillna(False) & (rv_z.diff() < 0).fillna(False))
    if event_type == "VOL_CLUSTER_SHIFT":
        return (rv_z.diff().abs() >= q(rv_z.diff().abs(), 0.98)).fillna(False)
    if event_type == "RANGE_COMPRESSION_END":
        return ((comp_ratio.shift(1) <= 0.80).fillna(False) & (comp_ratio >= 0.95).fillna(False))
    if event_type == "BREAKOUT_TRIGGER":
        rolling_hi = close.rolling(96, min_periods=24).max().shift(1)
        rolling_lo = close.rolling(96, min_periods=24).min().shift(1)
        return (((close > rolling_hi) | (close < rolling_lo)).fillna(False) & (comp_ratio.shift(1) <= 0.85).fillna(False))

    if event_type == "FUNDING_FLIP":
        return ((np.sign(funding) != np.sign(funding.shift(1))) & (funding_abs >= q(funding_abs, 0.60))).fillna(False)
    if event_type == "DELEVERAGING_WAVE":
        return ((oi_delta_1h <= q(oi_delta_1h, 0.05)).fillna(False) & (rv_z >= q(rv_z, 0.70)).fillna(False))

    if event_type == "TREND_EXHAUSTION_TRIGGER":
        return ((trend_96.abs() >= q(trend_96.abs(), 0.85)).fillna(False) & (ret_1 * trend_96 <= 0).fillna(False) & (rv_z >= q(rv_z, 0.50)).fillna(False))
    if event_type == "MOMENTUM_DIVERGENCE_TRIGGER":
        return ((trend_96.abs() >= q(trend_96.abs(), 0.75)).fillna(False) & (ret_1.abs() <= q(ret_1.abs(), 0.30)).fillna(False))
    if event_type == "CLIMAX_VOLUME_BAR":
        return ((ret_1.abs() >= q(ret_1.abs(), 0.99)).fillna(False) & (rv_z >= q(rv_z, 0.90)).fillna(False))
    if event_type == "FAILED_CONTINUATION":
        return ((ret_1.shift(1).abs() >= q(ret_1.abs(), 0.95)).fillna(False) & (ret_1 * ret_1.shift(1) < 0).fillna(False))

    if event_type == "RANGE_BREAKOUT":
        high_96 = _safe_series(df, "high_96").shift(1)
        low_96 = _safe_series(df, "low_96").shift(1)
        if high_96.isna().all() or low_96.isna().all():
            high_96 = close.rolling(96, min_periods=24).max().shift(1)
            low_96 = close.rolling(96, min_periods=24).min().shift(1)
        return ((close > high_96) | (close < low_96)).fillna(False)
    if event_type == "FALSE_BREAKOUT":
        breakout = _event_mask(df, "RANGE_BREAKOUT")
        return (breakout.shift(1).fillna(False) & (ret_1.abs() <= q(ret_1.abs(), 0.45)).fillna(False))
    if event_type == "TREND_ACCELERATION":
        d = trend_96.diff()
        return (d >= q(d, 0.95)).fillna(False)
    if event_type == "TREND_DECELERATION":
        d = trend_96.diff()
        return (d <= q(d, 0.05)).fillna(False)
    if event_type == "PULLBACK_PIVOT":
        return ((trend_96.shift(1).abs() >= q(trend_96.abs(), 0.75)).fillna(False) & (ret_1 * trend_96.shift(1) < 0).fillna(False) & (ret_1.abs() <= q(ret_1.abs(), 0.70)).fillna(False))
    if event_type == "SUPPORT_RESISTANCE_BREAK":
        rolling_hi = close.rolling(288, min_periods=96).max().shift(1)
        rolling_lo = close.rolling(288, min_periods=96).min().shift(1)
        return ((close > rolling_hi) | (close < rolling_lo)).fillna(False)

    if event_type == "ZSCORE_STRETCH":
        return (px_z.abs() >= max(2.0, q(px_z.abs(), 0.96))).fillna(False)
    if event_type == "BAND_BREAK":
        ma = close.rolling(96, min_periods=24).mean()
        sd = close.rolling(96, min_periods=24).std().replace(0.0, np.nan)
        return ((close > (ma + 2.0 * sd)) | (close < (ma - 2.0 * sd))).fillna(False)
    if event_type == "OVERSHOOT_AFTER_SHOCK":
        return ((rv_z.shift(1) >= q(rv_z, 0.95)).fillna(False) & (px_z.abs() >= q(px_z.abs(), 0.95)).fillna(False))
    if event_type == "GAP_OVERSHOOT":
        return (ret_1.abs() >= q(ret_1.abs(), 0.995)).fillna(False)

    if event_type == "VOL_REGIME_SHIFT_EVENT":
        regime = pd.cut(rv_96.rank(pct=True), bins=[-np.inf, 0.33, 0.66, np.inf], labels=[0, 1, 2])
        return (regime.astype(str) != regime.shift(1).astype(str)).fillna(False)
    if event_type == "TREND_TO_CHOP_SHIFT":
        return ((trend_96.shift(1).abs() >= q(trend_96.abs(), 0.75)).fillna(False) & (trend_96.abs() <= q(trend_96.abs(), 0.35)).fillna(False))
    if event_type == "CHOP_TO_TREND_SHIFT":
        return ((trend_96.shift(1).abs() <= q(trend_96.abs(), 0.35)).fillna(False) & (trend_96.abs() >= q(trend_96.abs(), 0.75)).fillna(False))
    if event_type == "CORRELATION_BREAKDOWN_EVENT":
        return ((basis_z.abs() >= q(basis_z.abs(), 0.95)).fillna(False) & (spread_z.abs() >= q(spread_z.abs(), 0.75)).fillna(False))
    if event_type == "BETA_SPIKE_EVENT":
        return ((ret_1.abs() >= q(ret_1.abs(), 0.99)).fillna(False) & (basis_z.abs() >= q(basis_z.abs(), 0.85)).fillna(False))

    if event_type == "INDEX_COMPONENT_DIVERGENCE":
        return ((basis_z.abs() >= q(basis_z.abs(), 0.93)).fillna(False) & (ret_1.abs() >= q(ret_1.abs(), 0.75)).fillna(False))
    if event_type == "SPOT_PERP_BASIS_SHOCK":
        return (basis_z.abs() >= max(2.0, q(basis_z.abs(), 0.97))).fillna(False)
    if event_type == "LEAD_LAG_BREAK":
        return (basis_z.diff().abs() >= q(basis_z.diff().abs(), 0.99)).fillna(False)

    ts = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
    if event_type == "SESSION_OPEN_EVENT":
        return ((ts.dt.minute == 0) & ts.dt.hour.isin([0, 8, 13])).fillna(False)
    if event_type == "SESSION_CLOSE_EVENT":
        return ((ts.dt.minute >= 55) & ts.dt.hour.isin([7, 12, 23])).fillna(False)
    if event_type == "FUNDING_TIMESTAMP_EVENT":
        return ((ts.dt.minute == 0) & ts.dt.hour.isin([0, 8, 16])).fillna(False)
    if event_type == "SCHEDULED_NEWS_WINDOW_EVENT":
        return (ts.dt.hour.isin([12, 13, 14, 18]) & ts.dt.minute.between(25, 35)).fillna(False)

    if event_type == "SPREAD_REGIME_WIDENING_EVENT":
        return (spread_z >= max(2.0, q(spread_z, 0.97))).fillna(False)
    if event_type == "SLIPPAGE_SPIKE_EVENT":
        return ((spread_z >= q(spread_z, 0.95)).fillna(False) & (ret_1.abs() >= q(ret_1.abs(), 0.90)).fillna(False))
    if event_type == "FEE_REGIME_CHANGE_EVENT":
        return ((ts.dt.day == 1) & (ts.dt.hour == 0) & (ts.dt.minute == 0)).fillna(False)

    return pd.Series(False, index=df.index, dtype=bool)


def _sparsify(mask: pd.Series, min_spacing: int) -> List[int]:
    idxs = np.flatnonzero(mask.fillna(False).values)
    selected: List[int] = []
    last = -10**9
    for idx in idxs:
        i = int(idx)
        if i - last >= int(min_spacing):
            selected.append(i)
            last = i
    return selected


def _rows_for_event(df: pd.DataFrame, symbol: str, event_type: str) -> pd.DataFrame:
    mask = _event_mask(df, event_type)
    idxs = _sparsify(mask, min_spacing=6)
    if not idxs:
        return pd.DataFrame()

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
                "event_score": float(np.nan_to_num(_safe_series(df, "rv_96").iloc[idx], nan=0.0)),
                "basis_z": float(np.nan_to_num(_safe_series(df, "basis_zscore").iloc[idx], nan=0.0)),
                "spread_z": float(np.nan_to_num(_safe_series(df, "spread_zscore").iloc[idx], nan=0.0)),
                "funding_rate_scaled": float(np.nan_to_num(_safe_series(df, "funding_rate_scaled").iloc[idx], nan=0.0)),
                "oi_notional": float(np.nan_to_num(_safe_series(df, "oi_notional").iloc[idx], nan=0.0)),
                "liquidation_notional": float(np.nan_to_num(_safe_series(df, "liquidation_notional").iloc[idx], nan=0.0)),
            }
        )
    return pd.DataFrame(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Canonical multi-family event detector for planned event types")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--event_type", required=True)
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--out_dir", default=None)
    parser.add_argument("--seed", type=int, default=13)
    parser.add_argument("--log_path", default=None)
    args = parser.parse_args()

    event_type = str(args.event_type).strip().upper()
    spec = EVENT_REGISTRY_SPECS.get(event_type)
    if spec is None:
        print(f"Unknown event_type: {event_type}", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / spec.reports_dir / args.run_id
    ensure_dir(out_dir)
    out_path = out_dir / spec.events_file

    events_parts: List[pd.DataFrame] = []
    symbols = [s.strip().upper() for s in str(args.symbols).split(",") if s.strip()]
    for symbol in symbols:
        features = _load_features(run_id=str(args.run_id), symbol=symbol, timeframe=str(args.timeframe))
        if features.empty:
            continue
        part = _rows_for_event(features, symbol=symbol, event_type=event_type)
        if not part.empty:
            events_parts.append(part)

    new_df = pd.concat(events_parts, ignore_index=True) if events_parts else pd.DataFrame(
        columns=[
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
    )

    if out_path.exists():
        try:
            prior = pd.read_csv(out_path)
        except Exception:
            prior = pd.DataFrame()
        if not prior.empty and "event_type" in prior.columns:
            prior = prior[prior["event_type"].astype(str) != event_type].copy()
            new_df = pd.concat([prior, new_df], ignore_index=True)

    new_df.to_csv(out_path, index=False)
    summary = {
        "run_id": str(args.run_id),
        "event_type": event_type,
        "rows": int(len(new_df[new_df["event_type"].astype(str) == event_type])) if not new_df.empty else 0,
        "events_file": str(out_path),
    }
    (out_dir / f"{event_type.lower()}_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Wrote {summary['rows']} rows for {event_type} to {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
