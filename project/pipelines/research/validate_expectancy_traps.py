from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_ROOT = Path(os.getenv("BACKTEST_DATA_ROOT", PROJECT_ROOT.parent / "data"))
sys.path.insert(0, str(PROJECT_ROOT))

from pipelines._lib.io_utils import (
    choose_partition_dir,
    ensure_dir,
    list_parquet_files,
    read_parquet,
    run_scoped_lake_path,
)


@dataclass
class CompressionEvent:
    symbol: str
    start_idx: int
    end_idx: int
    end_reason: str
    trend_state: int
    funding_bucket: str
    year: int
    vol_q: str
    bull_bear: str


EVENT_ROW_COLUMNS = [
    "symbol",
    "year",
    "vol_q",
    "bull_bear",
    "funding_bucket",
    "horizon",
    "end_reason",
    "trend_state",
    "breakout_dir",
    "breakout_aligns_htf",
    "time_to_expansion_bars",
    "mfe_post_end",
    "event_return",
    "event_directional_return",
]


def _parse_horizons(value: str) -> List[int]:
    parts = [x.strip() for x in value.split(",") if x.strip()]
    horizons = sorted({int(x) for x in parts if int(x) > 0})
    if not horizons:
        raise ValueError("At least one positive horizon is required")
    return horizons


def _pick_window_column(columns: Iterable[str], prefix: str) -> str:
    candidates: List[Tuple[int, str]] = []
    for col in columns:
        if not col.startswith(prefix):
            continue
        try:
            window = int(col.split("_")[-1])
        except ValueError:
            continue
        candidates.append((window, col))
    if not candidates:
        raise ValueError(f"Missing required feature prefix: {prefix}")
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def _rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    def pct_rank(values: np.ndarray) -> float:
        valid = values[~np.isnan(values)]
        if len(valid) == 0:
            return np.nan
        last = values[-1]
        if np.isnan(last):
            return np.nan
        return float(np.sum(valid <= last) / len(valid) * 100.0)

    return series.rolling(window=window, min_periods=window).apply(pct_rank, raw=True)


def _distribution_stats(returns: pd.Series) -> Dict[str, float]:
    clean = returns.dropna().astype(float)
    n = int(len(clean))
    if n == 0:
        return {
            "samples": 0,
            "mean_return": 0.0,
            "median_return": 0.0,
            "std_return": 0.0,
            "win_rate": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "t_stat": 0.0,
        }
    mean_val = float(clean.mean())
    median_val = float(clean.median())
    std_val = float(clean.std())
    t_stat = float(mean_val / (std_val / np.sqrt(n))) if std_val > 0.0 and n > 1 else 0.0
    return {
        "samples": n,
        "mean_return": mean_val,
        "median_return": median_val,
        "std_return": std_val,
        "win_rate": float((clean > 0).mean()),
        "p25": float(clean.quantile(0.25)),
        "p75": float(clean.quantile(0.75)),
        "t_stat": t_stat,
    }


def _tail_report(returns: pd.Series) -> Dict[str, float]:
    clean = returns.dropna().astype(float)
    if clean.empty:
        return {
            "median": 0.0,
            "p25": 0.0,
            "p75": 0.0,
            "top_1pct_contribution": 0.0,
            "top_5pct_contribution": 0.0,
        }
    total = float(clean.sum())
    sorted_desc = clean.sort_values(ascending=False)
    n = len(sorted_desc)
    n1 = max(1, int(np.ceil(n * 0.01)))
    n5 = max(1, int(np.ceil(n * 0.05)))
    top1 = float(sorted_desc.iloc[:n1].sum())
    top5 = float(sorted_desc.iloc[:n5].sum())
    denom = total if total != 0.0 else np.nan
    return {
        "median": float(clean.median()),
        "p25": float(clean.quantile(0.25)),
        "p75": float(clean.quantile(0.75)),
        "top_1pct_contribution": float(top1 / denom) if np.isfinite(denom) else 0.0,
        "top_5pct_contribution": float(top5 / denom) if np.isfinite(denom) else 0.0,
    }


def _load_symbol_features(symbol: str, run_id: str) -> pd.DataFrame:
    candidates = [
        run_scoped_lake_path(DATA_ROOT, run_id, "features", "perp", symbol, "15m", "features_v1"),
        DATA_ROOT / "lake" / "features" / "perp" / symbol / "15m" / "features_v1",
    ]
    features_dir = choose_partition_dir(candidates)
    files = list_parquet_files(features_dir) if features_dir else []
    if not files:
        raise ValueError(f"No features found for {symbol}: {candidates[0]}")
    df = read_parquet(files)
    if df.empty:
        raise ValueError(f"Empty features for {symbol}")
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df.sort_values("timestamp").reset_index(drop=True)


def _build_features(df: pd.DataFrame, htf_window: int, htf_lookback: int, funding_pct_window: int) -> pd.DataFrame:
    rv_pct_col = _pick_window_column(df.columns, "rv_pct_")
    range_med_col = _pick_window_column(df.columns, "range_med_")

    close = df["close"].astype(float)
    htf_ma = close.rolling(window=htf_window, min_periods=htf_window).mean()
    htf_delta = htf_ma - htf_ma.shift(htf_lookback)
    trend_state = pd.Series(np.where(htf_delta > 0, 1, np.where(htf_delta < 0, -1, 0)), index=df.index)

    funding_pct = _rolling_percentile(df["funding_rate_scaled"].astype(float), funding_pct_window)
    funding_bucket = pd.Series(
        np.select(
            [funding_pct <= 20, funding_pct >= 80],
            ["low", "high"],
            default="mid",
        ),
        index=df.index,
    ).where(funding_pct.notna())

    compression = ((df[rv_pct_col] <= 10.0) & (df["range_96"] <= 0.8 * df[range_med_col])).fillna(False)

    out = df.copy()
    out["trend_state"] = trend_state
    out["funding_bucket"] = funding_bucket
    out["compression"] = compression
    out["prior_high_96"] = out["high_96"].shift(1)
    out["prior_low_96"] = out["low_96"].shift(1)
    out["breakout_up"] = out["close"] > out["prior_high_96"]
    out["breakout_down"] = out["close"] < out["prior_low_96"]
    out["breakout_any"] = out["breakout_up"] | out["breakout_down"]
    out["vol_q"] = pd.qcut(out["rv_96"], q=4, labels=["Q1", "Q2", "Q3", "Q4"], duplicates="drop")
    out["bull_bear"] = np.where(close / close.shift(96) - 1.0 >= 0, "bull", "bear")
    return out


def _leakage_check(df: pd.DataFrame, htf_window: int, htf_lookback: int) -> Dict[str, object]:
    close = df["close"].astype(float)
    full_ma = close.rolling(window=htf_window, min_periods=htf_window).mean()
    full_delta = full_ma - full_ma.shift(htf_lookback)
    full_trend = pd.Series(np.where(full_delta > 0, 1, np.where(full_delta < 0, -1, 0)), index=df.index)

    rng = np.random.default_rng(7)
    candidates = np.arange(htf_window + htf_lookback, len(df))
    if len(candidates) == 0:
        return {"pass": False, "checked": 0, "mismatches": 0}
    sample = rng.choice(candidates, size=min(500, len(candidates)), replace=False)

    mismatches = 0
    for i in sample:
        partial = close.iloc[: i + 1]
        ma = partial.rolling(window=htf_window, min_periods=htf_window).mean()
        delta = ma - ma.shift(htf_lookback)
        trend_i = int(np.sign(delta.iloc[-1])) if pd.notna(delta.iloc[-1]) else 0
        if trend_i != int(full_trend.iloc[i]):
            mismatches += 1
    return {"pass": mismatches == 0, "checked": int(len(sample)), "mismatches": int(mismatches)}


def _extract_compression_events(df: pd.DataFrame, symbol: str, max_duration: int) -> List[CompressionEvent]:
    events: List[CompressionEvent] = []
    n = len(df)
    i = 1
    while i < n:
        if not bool(df.at[i, "compression"]) or bool(df.at[i - 1, "compression"]):
            i += 1
            continue

        start = i
        max_end = min(n - 1, start + max_duration - 1)
        end = start
        end_reason = "max_duration"

        j = start
        while j <= max_end:
            if bool(df.at[j, "breakout_any"]):
                end = j
                end_reason = "breakout"
                break
            if not bool(df.at[j, "compression"]):
                end = j
                end_reason = "compression_off"
                break
            end = j
            j += 1

        ts = df.at[start, "timestamp"]
        vol_q = df.at[start, "vol_q"]
        events.append(
            CompressionEvent(
                symbol=symbol,
                start_idx=start,
                end_idx=end,
                end_reason=end_reason,
                trend_state=int(df.at[start, "trend_state"]) if pd.notna(df.at[start, "trend_state"]) else 0,
                funding_bucket=str(df.at[start, "funding_bucket"]) if pd.notna(df.at[start, "funding_bucket"]) else "na",
                year=int(ts.year),
                vol_q=str(vol_q) if pd.notna(vol_q) else "na",
                bull_bear=str(df.at[start, "bull_bear"]),
            )
        )
        i = end + 1
    return events


def _first_expansion_after(df: pd.DataFrame, idx: int, lookahead: int) -> Tuple[int | None, int]:
    n = len(df)
    end = min(n - 1, idx + lookahead)
    for j in range(idx + 1, end + 1):
        if bool(df.at[j, "breakout_up"]):
            return j, 1
        if bool(df.at[j, "breakout_down"]):
            return j, -1
    return None, 0


def _event_rows(df: pd.DataFrame, events: List[CompressionEvent], horizons: List[int], expansion_lookahead: int, mfe_horizon: int) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    close = df["close"].to_numpy(dtype=float)
    high = df["high"].to_numpy(dtype=float)
    low = df["low"].to_numpy(dtype=float)
    n = len(df)

    for ev in events:
        expansion_idx = ev.end_idx if bool(df.at[ev.end_idx, "breakout_any"]) else None
        breakout_dir = 1 if bool(df.at[ev.end_idx, "breakout_up"]) else -1 if bool(df.at[ev.end_idx, "breakout_down"]) else 0
        if expansion_idx is None:
            expansion_idx, breakout_dir = _first_expansion_after(df, ev.end_idx, expansion_lookahead)

        time_to_expansion = (expansion_idx - ev.start_idx) if expansion_idx is not None else np.nan
        aligns = bool(breakout_dir == ev.trend_state) if breakout_dir != 0 and ev.trend_state != 0 else np.nan

        mfe = np.nan
        mfe_end = min(n - 1, ev.end_idx + mfe_horizon)
        if breakout_dir != 0 and ev.end_idx + 1 <= mfe_end:
            entry = close[ev.end_idx]
            if breakout_dir > 0:
                mfe = float(np.nanmax(high[ev.end_idx + 1 : mfe_end + 1]) / entry - 1.0)
            else:
                mfe = float(entry / np.nanmin(low[ev.end_idx + 1 : mfe_end + 1]) - 1.0)

        for h in horizons:
            if ev.end_idx + h >= n:
                continue
            ret = float(close[ev.end_idx + h] / close[ev.end_idx] - 1.0)
            directional_ret = float(ret * ev.trend_state) if ev.trend_state != 0 else np.nan
            rows.append(
                {
                    "symbol": ev.symbol,
                    "year": ev.year,
                    "vol_q": ev.vol_q,
                    "bull_bear": ev.bull_bear,
                    "funding_bucket": ev.funding_bucket,
                    "horizon": h,
                    "end_reason": ev.end_reason,
                    "trend_state": ev.trend_state,
                    "breakout_dir": breakout_dir,
                    "breakout_aligns_htf": aligns,
                    "time_to_expansion_bars": time_to_expansion,
                    "mfe_post_end": mfe,
                    "event_return": ret,
                    "event_directional_return": directional_ret,
                }
            )
    return rows


def _split_sign_report(events: pd.DataFrame, col: str, ret_col: str) -> Dict[str, object]:
    if events.empty:
        return {"stable_sign": False, "groups": {}}
    grouped = events.groupby(col, dropna=False)[ret_col].mean().dropna()
    groups = {str(k): float(v) for k, v in grouped.items()}
    if grouped.empty:
        return {"stable_sign": False, "groups": groups}
    positive = grouped > 0
    stable_sign = bool(positive.all() or (~positive).all())
    return {"stable_sign": stable_sign, "groups": groups}


def _bar_condition_stats(df: pd.DataFrame, condition: str, horizon: int) -> Dict[str, float]:
    close = df["close"].astype(float)
    fwd = close.shift(-horizon) / close - 1.0

    if condition == "compression":
        mask = df["compression"]
        ret = fwd.where(mask)
    elif condition == "compression_plus_htf_trend":
        mask = df["compression"] & (df["trend_state"] != 0)
        ret = (fwd * df["trend_state"]).where(mask)
    elif condition == "compression_plus_funding_low":
        mask = df["compression"] & (df["funding_bucket"] == "low")
        ret = fwd.where(mask)
    else:
        raise ValueError(f"Unknown condition: {condition}")

    return _distribution_stats(ret)


def _event_condition_frame(events_df: pd.DataFrame, condition: str, horizon: int) -> Tuple[pd.DataFrame, str]:
    ret_col = "event_directional_return" if condition == "compression_plus_htf_trend" else "event_return"

    if events_df.empty or "horizon" not in events_df.columns:
        return pd.DataFrame(columns=EVENT_ROW_COLUMNS), ret_col

    frame = events_df[events_df["horizon"] == horizon].copy()
    if condition == "compression":
        pass
    elif condition == "compression_plus_htf_trend":
        frame = frame[frame["trend_state"] != 0]
    elif condition == "compression_plus_funding_low":
        frame = frame[frame["funding_bucket"] == "low"]
    else:
        raise ValueError(f"Unknown condition: {condition}")

    return frame, ret_col


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate conditional expectancy against statistical traps (event-level)")
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--symbols", required=True)
    parser.add_argument("--horizons", default="4,16,96")
    parser.add_argument("--htf_window", type=int, default=384)
    parser.add_argument("--htf_lookback", type=int, default=96)
    parser.add_argument("--funding_pct_window", type=int, default=2880)
    parser.add_argument("--max_event_duration", type=int, default=96)
    parser.add_argument("--expansion_lookahead", type=int, default=192)
    parser.add_argument("--mfe_horizon", type=int, default=96)
    parser.add_argument("--tstat_threshold", type=float, default=2.0)
    parser.add_argument("--min_samples", type=int, default=100)
    parser.add_argument("--out_dir", default=None)
    args = parser.parse_args()

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    horizons = _parse_horizons(args.horizons)

    out_dir = Path(args.out_dir) if args.out_dir else DATA_ROOT / "reports" / "expectancy" / args.run_id
    ensure_dir(out_dir)

    leakage = {}
    all_bar_df = []
    all_event_rows: List[Dict[str, object]] = []
    event_summary_rows: List[Dict[str, object]] = []

    for symbol in symbols:
        df = _load_symbol_features(symbol, run_id=args.run_id)
        df = _build_features(df, args.htf_window, args.htf_lookback, args.funding_pct_window)
        leakage[symbol] = _leakage_check(df, args.htf_window, args.htf_lookback)
        events = _extract_compression_events(df, symbol=symbol, max_duration=args.max_event_duration)
        rows = _event_rows(
            df,
            events,
            horizons=horizons,
            expansion_lookahead=args.expansion_lookahead,
            mfe_horizon=args.mfe_horizon,
        )
        all_event_rows.extend(rows)
        all_bar_df.append(df)

        breakout_count = sum(1 for e in events if e.end_reason == "breakout")
        event_summary_rows.append(
            {
                "symbol": symbol,
                "event_count": len(events),
                "breakout_end_count": breakout_count,
                "breakout_end_rate": float(breakout_count / len(events)) if events else 0.0,
            }
        )

    master_bars = pd.concat(all_bar_df, ignore_index=True)
    events_df = pd.DataFrame(all_event_rows, columns=EVENT_ROW_COLUMNS)

    conditions = ["compression", "compression_plus_htf_trend", "compression_plus_funding_low"]
    trap_rows = []
    split_rows = []
    tail_rows = []
    symmetry_rows = []
    expansion_rows = []

    rng = np.random.default_rng(11)

    for condition in conditions:
        for horizon in horizons:
            bar_stats = _bar_condition_stats(master_bars, condition, horizon)
            event_frame, ret_col = _event_condition_frame(events_df, condition, horizon)
            event_stats = _distribution_stats(event_frame[ret_col] if ret_col in event_frame else pd.Series(dtype=float))

            trap_rows.append(
                {
                    "condition": condition,
                    "horizon": horizon,
                    "bar_samples": bar_stats["samples"],
                    "bar_mean": bar_stats["mean_return"],
                    "bar_t": bar_stats["t_stat"],
                    "event_samples": event_stats["samples"],
                    "event_mean": event_stats["mean_return"],
                    "event_t": event_stats["t_stat"],
                }
            )

            year_split = _split_sign_report(event_frame, "year", ret_col)
            vol_split = _split_sign_report(event_frame, "vol_q", ret_col)
            bull_split = _split_sign_report(event_frame, "bull_bear", ret_col)

            split_rows.append(
                {
                    "condition": condition,
                    "horizon": horizon,
                    "year_stable_sign": year_split["stable_sign"],
                    "vol_q_stable_sign": vol_split["stable_sign"],
                    "bull_bear_stable_sign": bull_split["stable_sign"],
                    "year_means": year_split["groups"],
                    "vol_q_means": vol_split["groups"],
                    "bull_bear_means": bull_split["groups"],
                }
            )

            tail = _tail_report(event_frame[ret_col] if ret_col in event_frame else pd.Series(dtype=float))
            tail_rows.append(
                {
                    "condition": condition,
                    "horizon": horizon,
                    "mean": event_stats["mean_return"],
                    "median": tail["median"],
                    "p25": tail["p25"],
                    "p75": tail["p75"],
                    "top_1pct_contribution": tail["top_1pct_contribution"],
                    "top_5pct_contribution": tail["top_5pct_contribution"],
                }
            )

            if condition == "compression_plus_htf_trend":
                base = event_frame[ret_col].dropna()
                opp = -base
                rand_sign = pd.Series(rng.choice([-1.0, 1.0], size=len(base)), index=base.index)
                rnd = base.abs() * rand_sign
                symmetry_rows.append(
                    {
                        "condition": condition,
                        "horizon": horizon,
                        "base_mean": float(base.mean()) if len(base) else 0.0,
                        "base_t": _distribution_stats(base)["t_stat"],
                        "opposite_mean": float(opp.mean()) if len(opp) else 0.0,
                        "opposite_t": _distribution_stats(opp)["t_stat"],
                        "random_mean": float(rnd.mean()) if len(rnd) else 0.0,
                        "random_t": _distribution_stats(rnd)["t_stat"],
                    }
                )

        cond_all, _ = _event_condition_frame(events_df, condition, horizons[0])
        cond_all = cond_all.drop_duplicates(subset=["symbol", "year", "vol_q", "bull_bear", "time_to_expansion_bars", "mfe_post_end", "trend_state", "funding_bucket", "end_reason", "breakout_dir"]) if not cond_all.empty else cond_all
        expansion_rows.append(
            {
                "condition": condition,
                "events": int(len(cond_all)),
                "time_to_expansion_median": float(cond_all["time_to_expansion_bars"].median()) if not cond_all.empty else np.nan,
                "time_to_expansion_p25": float(cond_all["time_to_expansion_bars"].quantile(0.25)) if not cond_all.empty else np.nan,
                "time_to_expansion_p75": float(cond_all["time_to_expansion_bars"].quantile(0.75)) if not cond_all.empty else np.nan,
                "mfe_median": float(cond_all["mfe_post_end"].median()) if not cond_all.empty else np.nan,
                "mfe_mean": float(cond_all["mfe_post_end"].mean()) if not cond_all.empty else np.nan,
                "breakout_align_rate": float(cond_all["breakout_aligns_htf"].dropna().mean()) if not cond_all.empty else np.nan,
            }
        )

    trap_df = pd.DataFrame(trap_rows)
    split_df = pd.DataFrame(split_rows)
    tail_df = pd.DataFrame(tail_rows)
    symmetry_df = pd.DataFrame(symmetry_rows)
    expansion_df = pd.DataFrame(expansion_rows)
    event_summary_df = pd.DataFrame(event_summary_rows)

    survivors = trap_df[
        (trap_df["event_samples"] >= args.min_samples)
        & (trap_df["event_mean"] > 0)
        & (trap_df["event_t"] >= args.tstat_threshold)
    ]

    payload = {
        "run_id": args.run_id,
        "symbols": symbols,
        "horizons": horizons,
        "config": {
            "max_event_duration": args.max_event_duration,
            "expansion_lookahead": args.expansion_lookahead,
            "mfe_horizon": args.mfe_horizon,
        },
        "event_summary": event_summary_df.to_dict(orient="records"),
        "trap_1_overlap_bar_vs_event": trap_df.to_dict(orient="records"),
        "trap_2_leakage": leakage,
        "trap_3_regimes": split_df.to_dict(orient="records"),
        "trap_4_tails": tail_df.to_dict(orient="records"),
        "trap_5_symmetry": symmetry_df.to_dict(orient="records"),
        "event_expansion_metrics": expansion_df.to_dict(orient="records"),
        "survivors": survivors.to_dict(orient="records"),
    }

    json_path = out_dir / "conditional_expectancy_robustness.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    lines = [
        "# Conditional Expectancy Robustness (Event-Level)",
        "",
        f"Run ID: `{args.run_id}`",
        "",
        "## Event Summary",
        event_summary_df.to_markdown(index=False) if not event_summary_df.empty else "No rows",
        "",
        "## Survivors (event-level)",
    ]
    if survivors.empty:
        lines.append("No condition survived event-level thresholds.")
    else:
        lines.append(survivors.to_markdown(index=False))

    lines.extend(["", "## Trap 2 Leakage", json.dumps(leakage, indent=2), ""])
    lines.extend(["## Trap 1 Overlap (Bar vs Event)", trap_df.to_markdown(index=False) if not trap_df.empty else "No rows", ""])
    lines.extend(["## Trap 3 Regime Stability", split_df.to_markdown(index=False) if not split_df.empty else "No rows", ""])
    lines.extend(["## Trap 4 Tail Dominance", tail_df.to_markdown(index=False) if not tail_df.empty else "No rows", ""])
    lines.extend(["## Trap 5 Symmetry", symmetry_df.to_markdown(index=False) if not symmetry_df.empty else "No rows", ""])
    lines.extend(["## Event Metrics", expansion_df.to_markdown(index=False) if not expansion_df.empty else "No rows", ""])

    md_path = out_dir / "conditional_expectancy_robustness.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"Wrote {json_path}")
    print(f"Wrote {md_path}")
    print(f"Survivors: {len(survivors)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
